"""
app.py — CandleLab Flask application.
Gunicorn: 1 worker, 4 threads. PORT env var, default 7860.
"""
import os
import json
import logging
import multiprocessing
import concurrent.futures
import numpy as np
import pandas as pd
from datetime import datetime, timezone
from flask import Flask, request, jsonify, render_template

import anthropic
from data import get_ohlc, INSTRUMENTS
from backtest import compute_atr
from patterns import detect_all, PATTERNS
from cache import cache_set, cache_get
from scheduler import start_scheduler
from strategy_store import (
    save_strategy, get_strategies_by_device,
    init_strategy_tables, load_strategy_trades, delete_strategy,
)
from indicators import check_ma_cross, check_rsi_extreme, check_ma_stable

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

ctx = multiprocessing.get_context("spawn")

app = Flask(__name__)

# ── Instrument normalisation ──────────────────────────────────────────────────

_INSTRUMENT_MAP = {
    "EURUSD":   "EUR/USD",
    "GBPUSD":   "GBP/USD",
    "USDJPY":   "USD/JPY",
    "USDCAD":   "USD/CAD",
    "AUDUSD":   "AUD/USD",
    "USDCHF":   "USD/CHF",
    "NZDUSD":   "NZD/USD",
    "XAUUSD":   "Gold",
    "XAGUSD":   "Silver",
    "WTICOUSD": "Oil",
    "SPX500USD": "S&P 500",
    "BTCUSD":   "Bitcoin",
}

_INSTRUMENT_DISPLAY = {
    "EURUSD":   "EUR/USD — euro vs dollar",
    "GBPUSD":   "GBP/USD — pound vs dollar",
    "USDJPY":   "USD/JPY — dollar vs yen",
    "USDCAD":   "USD/CAD — dollar vs loonie",
    "AUDUSD":   "AUD/USD — aussie vs dollar",
    "USDCHF":   "USD/CHF — dollar vs franc",
    "NZDUSD":   "NZD/USD — kiwi vs dollar",
    "XAUUSD":   "Gold",
    "XAGUSD":   "Silver",
    "WTICOUSD": "Crude Oil",
    "SPX500USD": "S&P 500",
    "BTCUSD":   "Bitcoin",
}


def _norm_instrument(s: str) -> str:
    """
    Normalize user-facing instrument strings to the canonical keys used by `data.INSTRUMENTS`.

    The UI and API sometimes supply either:
    - canonical names (e.g. "EUR/USD", "Gold"), or
    - compact codes (e.g. "EURUSD", "XAUUSD"), or
    - mixed-case variants.

    This helper keeps the API tolerant without changing internal naming.
    """
    s = s.strip()
    if s in INSTRUMENTS:
        return s
    return _INSTRUMENT_MAP.get(s.upper(), s)


# ── Pattern direction classification ─────────────────────────────────────────

_REVERSAL_PATTERNS = {
    "Doji",
    "Hammer/Hanging Man",
    "Shooting Star/Inv. Hammer",
    "Engulfing",
    "Morning/Evening Star",
    "Harami",
    "Piercing/Dark Cloud",
    "Spinning Top",
    "Long-legged Doji",
}

_TREND_PATTERNS = {
    "Three Soldiers/Crows",
    "Rising/Falling Three Methods",
    "Upside/Downside Tasuki Gap",
}

ALL_PATTERN_NAMES = list(PATTERNS.keys())


def _patterns_for_direction(direction: str) -> list:
    """
    Return the subset of pattern names appropriate for the selected strategy direction.

    - "reversal" → patterns that are typically interpreted as reversals
    - "trend"    → continuation/trend patterns
    - anything else (including "both") → all patterns
    """
    if direction == "reversal":
        return [p for p in ALL_PATTERN_NAMES if p in _REVERSAL_PATTERNS]
    if direction == "trend":
        return [p for p in ALL_PATTERN_NAMES if p in _TREND_PATTERNS]
    return ALL_PATTERN_NAMES


# ── Session filter ────────────────────────────────────────────────────────────

_SESSION_HOURS = {
    "London":   (8, 17),
    "New York": (13, 22),
    "Asian":    (0, 9),
    "Overlap":  (13, 17),
}


def _session_mask(index: pd.DatetimeIndex, session: str) -> np.ndarray:
    """
    Build a boolean mask selecting bars inside a named trading session window.

    Notes:
    - Input index is assumed to be timezone-aware UTC (as returned by `data.get_ohlc`).
    - Sessions are configured as UTC hour ranges in `_SESSION_HOURS`.
    - For ranges that wrap midnight (not currently used), the mask is OR-ed.
    """
    if not session or session in ("All", "All Sessions"):
        return np.ones(len(index), dtype=bool)
    hours = _SESSION_HOURS.get(session)
    if not hours:
        return np.ones(len(index), dtype=bool)
    h = index.hour
    start_h, end_h = hours
    if start_h < end_h:
        return (h >= start_h) & (h < end_h)
    return (h >= start_h) | (h < end_h)


# ── Custom backtest engine ────────────────────────────────────────────────────

RISK_DOLLARS = 100.0
PIP_VALUES = {
    "EUR/USD": 10.0, "GBP/USD": 10.0, "GBP/JPY": 9.30,
    "USD/JPY": 9.30, "USD/CAD": 7.70, "AUD/USD": 10.0,
    "USD/CHF": 10.0, "NZD/USD": 10.0, "EUR/GBP": 10.0,
    "EUR/JPY": 9.30, "Gold": 1.0, "Silver": 1.0,
    "Oil": 1.0, "S&P 500": 1.0, "Bitcoin": 1.0,
}
MIN_SL_PIPS = 5.0
ATR_PERIOD   = 14
TIMEOUT      = 1000


def _sl_distance_price(atr: float, pip: float) -> float:
    """Price distance for SL: max(ATR, MIN_SL_PIPS × pip)."""
    base = float(atr) if np.isfinite(atr) else 0.0
    return max(base, MIN_SL_PIPS * pip) if pip > 0 else base


def _simulate_trades(
    df: pd.DataFrame,
    signals_series: pd.Series,
    atr: pd.Series,
    pip: float,
    sl_mult: float,
    tp_mult: float,
    timeout: int,
    session: str = None,
    indicator_fn=None,
    direction_val: int = 0,
    instrument: str = "EUR/USD",
) -> list:
    """
    Simulate all trades for a single pattern.
    direction_val: 0=both, 1=longs only, -1=shorts only.
    indicator_fn: callable(df, signal_idx, direction_str) -> bool
    """
    # Session filtering is applied at signal time (not entry time) to keep the rule simple.
    sess_mask = _session_mask(df.index, session)
    trades = []
    n = len(df)

    for i in range(n - timeout - 1):
        sig = signals_series.iloc[i]
        if sig == 0:
            continue
        if direction_val != 0 and int(sig) != direction_val:
            continue
        if not sess_mask[i]:
            continue

        direction = int(sig)
        if indicator_fn is not None:
            dir_str = "long" if direction == 1 else "short"
            if not indicator_fn(df, i, dir_str):
                continue

        trade_atr = float(atr.iloc[i])
        if trade_atr == 0 or np.isnan(trade_atr):
            continue

        pip_val  = PIP_VALUES.get(instrument, 10.0)
        sl_dist  = _sl_distance_price(trade_atr, pip)
        sl_pips  = sl_dist / pip if pip > 0 else 0.0
        tp_pips  = sl_pips * (tp_mult / sl_mult)
        lot_size = (RISK_DOLLARS / (sl_pips * pip_val)) if sl_pips > 0 and pip_val > 0 else 0.0

        entry = float(df["open"].iloc[i + 1]) + direction * 0.5 * pip
        tp = entry + direction * tp_mult * trade_atr
        sl = entry - direction * _sl_distance_price(trade_atr, pip)

        future_hi      = df["high"].iloc[i + 1: i + 1 + timeout].to_numpy()
        future_lo      = df["low"].iloc[i + 1: i + 1 + timeout].to_numpy()
        future_cl      = df["close"].iloc[i + 1: i + 1 + timeout].to_numpy()
        future_op_next = df["open"].iloc[i + 2: i + 2 + timeout].to_numpy()
        if len(future_op_next) < len(future_cl):
            future_op_next = np.append(future_op_next, future_cl[-1])

        if len(future_hi) == 0:
            continue

        outcome    = "timeout_loss"
        exit_price = float(future_cl[-1])
        pnl_gross  = 0.0

        for j in range(len(future_hi)):
            if direction == 1:
                if future_hi[j] >= tp:
                    outcome    = "win"
                    exit_price = float(tp)
                    pnl_gross  = round(tp_pips * pip_val * lot_size, 2)
                    break
                if future_cl[j] <= sl:
                    nx         = float(future_op_next[j]) if np.isfinite(future_op_next[j]) else float(future_cl[j])
                    outcome    = "loss"
                    exit_price = nx
                    sl_pips_actual = sl_pips  # already floored
                    slip_pips = direction * (float(nx) - float(sl)) / pip  # extra beyond SL level, negative = worse
                    pnl_gross = round((-RISK_DOLLARS) + (slip_pips * pip_val * lot_size), 2)
                    break
            else:
                if future_lo[j] <= tp:
                    outcome    = "win"
                    exit_price = float(tp)
                    pnl_gross  = round(tp_pips * pip_val * lot_size, 2)
                    break
                if future_cl[j] >= sl:
                    nx         = float(future_op_next[j]) if np.isfinite(future_op_next[j]) else float(future_cl[j])
                    outcome    = "loss"
                    exit_price = nx
                    sl_pips_actual = sl_pips  # already floored
                    slip_pips = direction * (float(nx) - float(sl)) / pip  # extra beyond SL level, negative = worse
                    pnl_gross = round((-RISK_DOLLARS) + (slip_pips * pip_val * lot_size), 2)
                    break
        else:
            exit_pips = direction * (exit_price - entry) / pip
            pnl_gross = round(exit_pips * pip_val * lot_size, 2)
            outcome   = "timeout_win" if exit_pips > 0 else "timeout_loss"

        pnl_net = round(pnl_gross, 2)
        is_win = outcome in ("win", "timeout_win")

        trades.append({
            "ts":        df.index[i],
            "signal":    direction,
            "entry":     entry,
            "atr":       trade_atr,
            "tp":        tp,
            "sl":        sl,
            "exit_price":  round(exit_price, 5),
            "lot_size":    round(lot_size, 2),
            "outcome":   outcome,
            "win":       is_win,
            "pnl_gross": round(pnl_gross, 2),
            "pnl_net":   pnl_net,
        })

    return trades


def _backtest_pattern(
    df: pd.DataFrame,
    pip: float,
    pattern_name: str,
    timeout: int = TIMEOUT,
    session: str = None,
    indicator_fn=None,
    direction_val: int = 0,
    sl_mult: float = 1.0,
    tp_mult: float = 3.0,
    instrument: str = "EUR/USD",
) -> dict:
    """
    Backtest a single pattern using the local engine.

    This is the core engine used by the API today (separate from `backtest.run_backtest`,
    which evaluates all patterns at once). It exists mainly so endpoints can:
    - apply session filters,
    - apply an optional indicator confirmation function,
    - parameterize SL/TP multipliers and timeouts.
    """
    atr = compute_atr(df)
    signals_df = detect_all(df)
    if pattern_name not in signals_df.columns:
        return {"signals": 0, "wins": 0, "win_pct": 0.0, "cum_net": 0.0, "trades": []}

    trades = _simulate_trades(
        df, signals_df[pattern_name], atr, pip,
        sl_mult, tp_mult, timeout, session, indicator_fn, direction_val,
        instrument=instrument,
    )

    total = len(trades)
    wins = sum(1 for t in trades if t["win"])
    cum_net = round(sum(t["pnl_net"] for t in trades), 2)

    return {
        "signals":  total,
        "wins":     wins,
        "win_pct":  round(wins / total * 100, 1) if total else 0.0,
        "cum_net":  cum_net,
        "trades":   trades,
    }


def _backtest_pattern_multi_timeout(
    df: pd.DataFrame,
    pip: float,
    pattern_name: str,
    session: str = None,
    sl_mult: float = 1.0,
    tp_mult: float = 3.0,
    instrument: str = "EUR/USD",
) -> dict:
    """Run a single pattern at timeouts 5, 10, 20 and return combined results."""
    atr = compute_atr(df)
    signals_df = detect_all(df)
    if pattern_name not in signals_df.columns:
        return {"signals": 0, "win_pct_5": 0.0, "win_pct_10": 0.0, "win_pct_20": 0.0}

    sess_mask = _session_mask(df.index, session)
    sig_series = signals_df[pattern_name]
    n = len(df)

    results = {}
    for timeout in (5, 10, 20):
        trades = _simulate_trades(
            df, sig_series, atr, pip, sl_mult, tp_mult, timeout, session,
            instrument=instrument,
        )
        total = len(trades)
        wins = sum(1 for t in trades if t["win"])
        results[f"win_pct_{timeout}"] = round(wins / total * 100, 1) if total else 0.0
        results["signals"] = total

    return results


def _task_pattern_multi_timeout(args):
    """
    ProcessPool entry point wrapper.

    Notes:
    - Must be top-level for Windows "spawn" semantics.
    - Takes a single tuple argument to simplify `executor.submit` calls.
    """
    df, pip, pattern_name, session, sl_mult, tp_mult, instrument = args
    r = _backtest_pattern_multi_timeout(
        df, pip, pattern_name, session, sl_mult, tp_mult, instrument,
    )
    return pattern_name, r


def _complement_task(args):
    (comp_name, connector,
     df, signals_df, anchor_indices, atr, anchor_signals, anchor_win_pct) = args

    WINDOW = 5
    # Connector semantics:
    # - ordered: complement must occur in the *next* WINDOW candles after anchor
    # - any-order: complement within ±WINDOW candles
    # - optional: wider window (±2×WINDOW)
    if comp_name not in signals_df.columns:
        return None
    comp_signals = signals_df[comp_name].to_numpy()
    comp_indices_set = set(np.where(comp_signals != 0)[0].tolist())

    filtered_anchor_idx = []
    for ai in anchor_indices:
        ai_int = int(ai)
        if connector == "ordered":
            found = any(
                j in comp_indices_set
                for j in range(ai_int + 1, ai_int + WINDOW + 1)
            )
        elif connector == "any-order":
            found = any(
                abs(ai_int - j) <= WINDOW
                for j in comp_indices_set
                if abs(ai_int - j) <= WINDOW
            )
        else:  # optional — wider window
            found = any(
                abs(ai_int - j) <= WINDOW * 2
                for j in comp_indices_set
                if abs(ai_int - j) <= WINDOW * 2
            )
        if found:
            filtered_anchor_idx.append(ai)

    if not filtered_anchor_idx:
        return None

    subset_trades = []
    n_df = len(df)
    open_arr  = df["open"].to_numpy()
    high_arr  = df["high"].to_numpy()
    low_arr   = df["low"].to_numpy()
    close_arr = df["close"].to_numpy()
    atr_arr   = atr.to_numpy()
    anc_arr   = anchor_signals.to_numpy()

    for ai in filtered_anchor_idx:
        ai_int = int(ai)
        if ai_int + 6 >= n_df:
            continue
        trade_atr = float(atr_arr[ai_int])
        if trade_atr == 0 or np.isnan(trade_atr):
            continue
        entry = float(open_arr[ai_int + 1])
        direction_val = int(anc_arr[ai_int])
        tp = entry + direction_val * 3.0 * trade_atr
        sl = entry - direction_val * 1.0 * trade_atr
        future_hi = high_arr[ai_int + 1: ai_int + 6]
        future_lo = low_arr[ai_int + 1: ai_int + 6]
        future_cl = close_arr[ai_int + 1: ai_int + 6]
        if len(future_hi) == 0:
            continue
        if direction_val == 1:
            tp_hits = np.where(future_hi >= tp)[0]
            sl_hits = np.where(future_lo <= sl)[0]
        else:
            tp_hits = np.where(future_lo <= tp)[0]
            sl_hits = np.where(future_hi >= sl)[0]
        nf = len(future_hi)
        tp_idx = int(tp_hits[0]) if len(tp_hits) else nf
        sl_idx = int(sl_hits[0]) if len(sl_hits) else nf
        if tp_idx <= sl_idx and tp_idx < nf:
            subset_trades.append(True)
        elif sl_idx < tp_idx and sl_idx < nf:
            subset_trades.append(False)
        else:
            subset_trades.append(direction_val * (future_cl[-1] - entry) > 0)

    total = len(subset_trades)
    if total == 0:
        return None
    wins = sum(1 for w in subset_trades if w)
    win_pct = round(wins / total * 100, 1)
    delta = round(win_pct - anchor_win_pct, 1)

    return {
        "complement": comp_name,
        "connector":  connector,
        "win_pct":    win_pct,
        "signals":    total,
        "delta":      delta,
    }


# ── AI tips via Haiku ─────────────────────────────────────────────────────────

def _get_ai_tips(patterns_data: list, instrument: str) -> dict:
    """
    Call claude-haiku-4-5-20251001 once to get a short plain-english tip
    per pattern. Returns dict: pattern_name -> tip string.
    """
    try:
        client = anthropic.Anthropic()
        names_and_rates = "\n".join(
            f"- {p['name']}: {p['win_pct_5']}% win rate at 5 candles ({p['signals']} signals)"
            for p in patterns_data
        )
        prompt = (
            f"I'm building a beginner-friendly trading app for {instrument}.\n"
            f"For each candlestick pattern below, write ONE short sentence (max 15 words) "
            f"explaining what it means in plain English for a beginner trader.\n"
            f"Reply ONLY with a JSON object mapping pattern name to tip string.\n\n"
            f"{names_and_rates}"
        )
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=800,
            messages=[{"role": "user", "content": prompt}],
        )
        text = msg.content[0].text.strip()
        # Extract JSON from response
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            return json.loads(text[start:end])
    except Exception as e:
        log.warning(f"AI tips failed: {e}")
    return {}


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/patterns")
def api_patterns():
    """
    Return ranked patterns for the selected market/timeframe.

    Implementation notes:
    - Uses a ProcessPool so each pattern's multi-timeout backtest runs in parallel.
    - Uses Redis to cache the resulting JSON for a short TTL.
    """
    instrument_raw = request.args.get("instrument", "EUR/USD")
    interval = request.args.get("interval", "5m")
    direction = request.args.get("direction", "both")

    instrument = _norm_instrument(instrument_raw)
    if instrument not in INSTRUMENTS:
        return jsonify({"error": f"Unknown instrument: {instrument_raw}"}), 400

    cache_key = f"patterns:{instrument}:{interval}:{direction}"
    cached = cache_get(cache_key)
    if cached is not None:
        return jsonify(cached)

    meta = INSTRUMENTS[instrument]
    pip = meta["pip"]

    try:
        df = get_ohlc(instrument, days=30, interval=interval)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    if df.empty:
        return jsonify([])

    pattern_names = _patterns_for_direction(direction)

    tasks = [(df, pip, name, None, 1.0, 3.0, instrument) for name in pattern_names]

    # ProcessPool is used to parallelize per-pattern simulations. On Windows this uses
    # "spawn", so the task payload must be picklable and entrypoints must be top-level.
    with concurrent.futures.ProcessPoolExecutor(max_workers=4, mp_context=ctx) as executor:
        futures = {executor.submit(_task_pattern_multi_timeout, t): t[2] for t in tasks}
        raw = {}
        for fut in concurrent.futures.as_completed(futures):
            name, r = fut.result()
            raw[name] = r

    patterns_data = []
    for name in pattern_names:
        r = raw.get(name, {})
        patterns_data.append({
            "name":       name,
            "signals":    r.get("signals", 0),
            "win_pct_5":  r.get("win_pct_5", 0.0),
            "win_pct_10": r.get("win_pct_10", 0.0),
            "win_pct_20": r.get("win_pct_20", 0.0),
            "tip":        "",
        })

    patterns_data.sort(key=lambda x: x["win_pct_5"], reverse=True)

    tips = _get_ai_tips(patterns_data, instrument)
    for p in patterns_data:
        p["tip"] = tips.get(p["name"], "")

    cache_set(cache_key, patterns_data, ttl=300)
    return jsonify(patterns_data)


@app.route("/api/complement", methods=["POST"])
def api_complement():
    """
    Suggest complement patterns/connectors that improve win rate vs the anchor alone.

    This endpoint evaluates many candidate pairs in parallel and returns the top 3
    by win-rate delta (minimum signal count threshold is enforced).
    """
    body = request.get_json(force=True)
    anchor = body.get("anchor")
    direction = body.get("direction", "both")
    instrument_raw = body.get("instrument", "EUR/USD")
    interval = body.get("interval", "5m")

    instrument = _norm_instrument(instrument_raw)
    if instrument not in INSTRUMENTS:
        return jsonify({"error": "Unknown instrument"}), 400

    meta = INSTRUMENTS[instrument]
    pip = meta["pip"]

    try:
        df = get_ohlc(instrument, days=30, interval=interval)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    if df.empty:
        return jsonify([])

    atr = compute_atr(df)
    signals_df = detect_all(df)

    if anchor not in signals_df.columns:
        return jsonify({"error": "Unknown anchor pattern"}), 400

    anchor_signals = signals_df[anchor]
    anchor_indices = np.where(anchor_signals.to_numpy() != 0)[0]

    anchor_r = _backtest_pattern(df, pip, anchor, timeout=TIMEOUT, instrument=instrument)
    anchor_win_pct = anchor_r["win_pct"]

    CONNECTORS = ["ordered", "any-order", "optional"]

    combo_tasks = [
        (comp, conn, df, signals_df, anchor_indices, atr, anchor_signals, anchor_win_pct)
        for comp in ALL_PATTERN_NAMES
        for conn in CONNECTORS
        if comp != anchor
    ]

    results = []
    # Parallel evaluation of complement candidates.
    with concurrent.futures.ProcessPoolExecutor(max_workers=4, mp_context=ctx) as executor:
        futures = [executor.submit(_complement_task, t) for t in combo_tasks]
        for fut in concurrent.futures.as_completed(futures):
            r = fut.result()
            if r and r["signals"] >= 3:
                results.append(r)

    results.sort(key=lambda x: x["delta"], reverse=True)
    return jsonify(results[:3])


@app.route("/api/indicator-check", methods=["POST"])
def api_indicator_check():
    body = request.get_json(force=True)
    anchor = body.get("anchor")
    complement = body.get("complement")
    connector = body.get("connector")
    direction = body.get("direction", "both")
    instrument_raw = body.get("instrument", "EUR/USD")
    interval = body.get("interval", "5m")
    indicator = body.get("indicator")

    instrument = _norm_instrument(instrument_raw)
    if instrument not in INSTRUMENTS:
        return jsonify({"error": "Unknown instrument"}), 400

    meta = INSTRUMENTS[instrument]
    pip = meta["pip"]

    try:
        df = get_ohlc(instrument, days=30, interval=interval)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    if df.empty:
        return jsonify({"error": "No data"}), 500

    indicator_map = {
        "ma_cross":  lambda df, idx, d: check_ma_cross(df, idx),
        "rsi":       check_rsi_extreme,
        "ma_stable": check_ma_stable,
    }
    indicator_fn = indicator_map.get(indicator)

    base_r = _backtest_pattern(df, pip, anchor, timeout=TIMEOUT, instrument=instrument)
    filtered_r = _backtest_pattern(
        df, pip, anchor, timeout=TIMEOUT, indicator_fn=indicator_fn, instrument=instrument,
    )

    return jsonify({
        "unfiltered_win_pct": base_r["win_pct"],
        "filtered_win_pct":   filtered_r["win_pct"],
        "delta":              round(filtered_r["win_pct"] - base_r["win_pct"], 1),
        "signals":            filtered_r["signals"],
    })


@app.route("/api/finalise", methods=["POST"])
def api_finalise():
    body = request.get_json(force=True)
    preview = body.get("preview", False)

    instrument_raw = body.get("instrument", "EUR/USD")
    interval = body.get("interval", "5m")
    anchor = body.get("anchor", "")
    complement = body.get("complement")
    connector = body.get("connector", "ordered")
    indicator_filter = body.get("indicator_filter")
    session_filter = body.get("session_filter")
    sl_mult = float(body.get("sl_multiplier", 1.0))
    tp_mult = float(body.get("tp_multiplier", 3.0))
    timeout = int(body.get("timeout", TIMEOUT))
    timeout = max(5, min(timeout, 1000))  # clamp between 5 and 1000
    device_uuid = body.get("device_uuid") or request.headers.get("X-Device-UUID")
    name = body.get("name")

    if tp_mult <= sl_mult:
        return jsonify({"error": "tp_multiplier must be greater than sl_multiplier"}), 400

    instrument = _norm_instrument(instrument_raw)
    if instrument not in INSTRUMENTS:
        return jsonify({"error": "Unknown instrument"}), 400

    meta = INSTRUMENTS[instrument]
    pip = meta["pip"]

    indicator_fn = None
    if indicator_filter:
        itype = indicator_filter if isinstance(indicator_filter, str) else indicator_filter.get("type")
        indicator_map = {
            "ma_cross":  lambda df, idx, d: check_ma_cross(df, idx),
            "rsi":       check_rsi_extreme,
            "ma_stable": check_ma_stable,
        }
        indicator_fn = indicator_map.get(itype)

    def _run(ivl):
        try:
            df = get_ohlc(instrument, days=30, interval=ivl)
        except Exception:
            return None
        if df.empty:
            return None
        r = _backtest_pattern(
            df, pip, anchor, timeout=timeout,
            session=session_filter,
            indicator_fn=indicator_fn,
            sl_mult=sl_mult, tp_mult=tp_mult,
            instrument=instrument,
        )
        return r

    main_r = _run(interval)
    if main_r is None:
        return jsonify({"error": "No data"}), 500

    cross_tf = []
    for other_ivl in ("15m", "1h"):
        if other_ivl == interval:
            continue
        r = _run(other_ivl)
        if r:
            cross_tf.append({
                "interval": other_ivl,
                "win_pct":  r["win_pct"],
                "signals":  r["signals"],
            })

    if preview:
        atr_val = float(main_r["trades"][-1]["atr"]) if main_r.get("trades") else None
        return jsonify({
            "win_pct":          main_r["win_pct"],
            "signals":          main_r["signals"],
            "cross_timeframe":  cross_tf,
            "atr":              atr_val,
        })

    chart_trades = []
    for t in main_r["trades"][-50:]:
        chart_trades.append({
            "ts":         (pd.Timestamp(t["ts"]).tz_localize("UTC") if pd.Timestamp(t["ts"]).tzinfo is None else pd.Timestamp(t["ts"])).isoformat(),
            "signal":     t["signal"],
            "entry":      round(t["entry"], meta["decimals"]),
            "tp":         round(t["tp"], meta["decimals"]),
            "sl":         round(t["sl"], meta["decimals"]),
            "atr":        round(t["atr"], 6),
            "win":        t["win"],
            "exit_price": round(t["exit_price"], meta["decimals"]),
            "exit_type":  "TP" if t["outcome"] == "win" else ("SL" if t["outcome"] == "loss" else "TO"),
            "outcome":    t["outcome"],
            "pnl_net":    t["pnl_net"],
        })

    df_main = pd.DataFrame()
    chart_image = ""
    equity_image = ""
    try:
        df_main = get_ohlc(instrument, days=30, interval=interval)
        if not df_main.empty:
            chart_candles = {
                "ts":    [int(pd.Timestamp(t).timestamp()) for t in df_main.index],
                "open":  df_main["open"].tolist(),
                "high":  df_main["high"].tolist(),
                "low":   df_main["low"].tolist(),
                "close": df_main["close"].tolist(),
            }
        else:
            chart_candles = {}
    except Exception:
        chart_candles = {}

    if not df_main.empty:
        try:
            from chart_renderer import render_chart, render_equity_chart

            n = len(df_main)
            idx = df_main.index

            def _chart_ts_bar(ts_raw):
                t = pd.Timestamp(ts_raw)
                if t.tzinfo is None:
                    t = t.tz_localize("UTC")
                else:
                    t = t.tz_convert("UTC")
                li = int(idx.get_indexer([t], method="nearest")[0])
                return max(0, min(li, n - 1))

            sig = np.zeros(n, dtype=np.int8)
            for t in chart_trades:
                sig[_chart_ts_bar(t["ts"])] = int(t["signal"])

            equity_usd = np.zeros(n, dtype=float)
            cum = 0.0
            all_trades = main_r.get("trades") or []
            for trade in sorted(all_trades, key=lambda x: pd.Timestamp(x["ts"])):
                bi = _chart_ts_bar(trade["ts"])
                cum += float(trade["pnl_net"])
                equity_usd[bi:] = cum

            equity_x = np.arange(n, dtype=float)
            equity_y_pips = np.zeros(n, dtype=float)

            last50 = all_trades[-50:] if all_trades else []
            if last50:
                locs = [_chart_ts_bar(t["ts"]) for t in last50]
                ws, we = min(locs), max(locs) + 1
            else:
                ws, we = 0, n

            chart_b64, _ = render_chart(
                df_main,
                [],
                [],
                window_start=ws,
                window_end=we,
                full_len=n,
                show_volume=False,
                signals=sig,
                pip=pip,
            )
            chart_image = chart_b64
            equity_image = render_equity_chart(
                df_main,
                equity_x,
                equity_y_pips,
                equity_usd,
                idx,
                window_start=ws,
                window_end=we,
                notional=int(RISK_DOLLARS),
                instrument=instrument,
                signals=sig,
            )
        except Exception as e:
            log.warning("chart render failed: %s", e, exc_info=True)

    try:
        saved = save_strategy({
            "name":             name,
            "patterns":         [anchor] + ([complement] if complement else []),
            "connectors":       [connector, connector],
            "direction":        body.get("direction", ""),
            "window_days":      5,
            "instrument":       instrument,
            "interval":         interval,
            "bt_win_pct":       main_r["win_pct"],
            "bt_cum_net":       main_r["cum_net"],
            "bt_cum_gross":     None,
            "bt_signals":       main_r["signals"],
            "bt_tp_hits":       None,
            "device_uuid":      device_uuid,
            "indicator_filter": indicator_filter,
            "session_filter":   session_filter,
        })
    except Exception:
        log.exception("save_strategy failed")
        return jsonify({"error": "Failed to save strategy"}), 500

    return jsonify({
        "id":               saved["id"],
        "name":             saved["name"],
        "win_pct":          main_r["win_pct"],
        "signals":          main_r["signals"],
        "sl_multiplier":    sl_mult,
        "tp_multiplier":    tp_mult,
        "cross_timeframe":  cross_tf,
        "chart_candles":    chart_candles,
        "chart_trades":     chart_trades,
        "chart_image":      chart_image,
        "equity_image":     equity_image,
    })


@app.route("/api/live")
def api_live():
    device_uuid = request.args.get("device_uuid") or request.headers.get("X-Device-UUID")
    if not device_uuid:
        return jsonify([])

    strategies = get_strategies_by_device(device_uuid)
    result = []
    for s in strategies:
        trades = load_strategy_trades(s["id"])
        live_from = s.get("live_from_ts")
        if live_from:
            trades = [t for t in trades if t["ts"] >= live_from]
        total_pnl = round(sum(t.get("pnl_net", 0) for t in trades), 2)
        result.append({
            "id":         s["id"],
            "name":       s["name"],
            "instrument": s.get("instrument"),
            "interval":   s.get("interval"),
            "created_ts": s.get("created_ts"),
            "win_pct":    s.get("bt_win_pct"),
            "live_pnl":   total_pnl,
            "trades":     len(trades),
        })
    return jsonify(result)


@app.route("/api/strategy/<sid>", methods=["DELETE"])
def api_delete_strategy(sid):
    device_uuid = request.args.get("device_uuid") or request.headers.get("X-Device-UUID")
    # Only allow deletion of strategies belonging to this device
    strategies = get_strategies_by_device(device_uuid) if device_uuid else []
    if not any(str(s["id"]) == str(sid) for s in strategies):
        return jsonify({"error": "not found"}), 404
    delete_strategy(sid)
    return jsonify({"ok": True})


_DISPLAY_TO_CODE = {
    "Gold":    "XAUUSD",
    "GBP/USD": "GBPUSD",
    "USD/JPY": "USDJPY",
    "S&P 500": "SPX500USD",
    "Silver":  "XAGUSD",
}

_REVERSAL_ALTS = {
    "EURUSD":    ("Gold",    "Gold tends to show stronger and cleaner reversal signals."),
    "GBPUSD":    ("Gold",    "Gold tends to show stronger reversal signals than most FX pairs."),
    "USDJPY":    ("GBP/USD", "GBP/USD is known for sharp, well-defined reversals."),
    "USDCAD":    ("Gold",    "Gold tends to show stronger reversal signals."),
    "AUDUSD":    ("GBP/USD", "GBP/USD is known for sharp reversals and high volatility."),
    "USDCHF":    ("Gold",    "Gold tends to show stronger reversal signals."),
    "NZDUSD":    ("GBP/USD", "GBP/USD has more volume and sharper reversal moves."),
    "XAUUSD":    ("GBP/USD", "GBP/USD is another instrument known for strong reversals."),
    "XAGUSD":    ("Gold",    "Gold has more liquidity and shows cleaner reversal patterns than Silver."),
    "WTICOUSD":  ("Gold",    "Gold tends to show cleaner and more reliable reversal setups."),
    "SPX500USD": ("Gold",    "Gold tends to show stronger reversal signals than equity indices."),
    "BTCUSD":    ("Gold",    "Gold tends to show more reliable and consistent reversal signals."),
}

_TREND_ALTS = {
    "EURUSD":    ("S&P 500", "Indices like the S&P 500 tend to trend more persistently."),
    "GBPUSD":    ("USD/JPY", "USD/JPY is one of the most consistently trending FX pairs."),
    "USDJPY":    ("S&P 500", "The S&P 500 is one of the strongest and most consistent trending instruments."),
    "USDCAD":    ("USD/JPY", "USD/JPY is a strong trending pair with clear momentum moves."),
    "AUDUSD":    ("USD/JPY", "USD/JPY tends to trend more cleanly than AUD pairs."),
    "USDCHF":    ("USD/JPY", "USD/JPY is a strong trending pair."),
    "NZDUSD":    ("USD/JPY", "USD/JPY has more volume and cleaner trend structure."),
    "XAUUSD":    ("S&P 500", "The S&P 500 tends to produce cleaner, longer-lasting trend signals."),
    "XAGUSD":    ("S&P 500", "The S&P 500 trends more consistently than precious metals."),
    "WTICOUSD":  ("S&P 500", "The S&P 500 tends to produce cleaner trend signals than oil."),
    "SPX500USD": ("USD/JPY", "USD/JPY is a reliably trending pair worth comparing against."),
    "BTCUSD":    ("S&P 500", "The S&P 500 trends more consistently and is easier to validate."),
}


@app.route("/api/analyse", methods=["POST"])
def api_analyse():
    body = request.get_json(force=True)

    instrument   = body.get("instrument", "")
    interval     = body.get("interval", "")
    direction    = body.get("direction", "both")
    anchor       = body.get("anchor", "")
    complement   = body.get("complement")
    session      = body.get("session_filter")
    indicator    = body.get("indicator_filter")
    sl_mult      = body.get("sl_multiplier", 1.0)
    tp_mult      = body.get("tp_multiplier", 3.0)
    win_pct       = body.get("win_pct", 0.0)
    signals       = body.get("signals", 0)
    cross_tf      = body.get("cross_timeframe", [])
    final_equity  = body.get("final_equity", 1000)
    biggest_win   = body.get("biggest_win", 0.0)
    biggest_loss  = body.get("biggest_loss", 0.0)
    trades_won    = body.get("trades_won", 0)
    trades_lost   = body.get("trades_lost", 0)

    cross_lines = ""
    if cross_tf:
        cross_lines = "\n".join(
            f"  {r['interval']}: {r['win_pct']}% win rate, {r['signals']} signals"
            for r in cross_tf
        )
        cross_lines = f"\nCross-timeframe:\n{cross_lines}"

    filters = []
    if session:
        filters.append(f"session filter: {session}")
    if indicator:
        filters.append(f"indicator filter: {indicator}")
    if complement:
        filters.append(f"complement pattern: {complement}")
    filter_line = f"\nFilters: {', '.join(filters)}" if filters else ""

    prompt = (
        f"You are an encouraging trading coach. The user just built their first strategy. "
        f"Comment on the results in exactly 3 short sentences, warm and conversational. "
        f"Be specific to their setup. No jargon. Return plain text, no markdown.\n\n"
        f"Strategy details:\n"
        f"  Instrument: {instrument}, Timeframe: {interval}\n"
        f"  Anchor pattern: {anchor}"
        f"{filter_line}\n"
        f"  SL: {sl_mult}×, TP: {tp_mult}×\n"
        f"  Backtest: {signals} signals, {trades_won}W / {trades_lost}L, {win_pct}% win rate\n"
        f"  Biggest win: +${biggest_win}, Biggest loss: -${biggest_loss}\n"
        f"  Equity: started $1,000 → ended ${final_equity}\n"
        f"{cross_lines}"
    )

    try:
        client = anthropic.Anthropic()
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}],
        )
        commentary = msg.content[0].text.strip()
    except Exception as e:
        log.warning(f"AI analyse failed: {e}")
        commentary = "Your strategy is live and ready to track in real time. Keep an eye on it!"

    # Append mandatory context-aware sentences regardless of AI output
    extra = []
    if signals < 30:
        extra.append(
            f"Worth noting: with only {signals} signals over 30 days, this result could easily be random chance — "
            f"you would want to see this play out over at least 3 months and 100 signals before trusting it with real money."
        )
    if win_pct > 60:
        extra.append(
            "A 60%+ win rate on a short backtest often reflects overfitting to recent market conditions rather than genuine edge — "
            "test it on a different instrument or time period to see if it holds up."
        )
    extra.append(
        "Candlestick patterns alone rarely produce consistent edge — the real value is in the combination of "
        "pattern, session, and market context you have built here."
    )
    commentary = commentary + " " + " ".join(extra)

    # ── Suggested market ──────────────────────────────────────────────────────
    suggested_market       = None
    suggested_market_code  = None
    suggested_market_label = None
    if signals < 15 or win_pct < 50:
        alt_map = _REVERSAL_ALTS if direction in ("reversal", "both") else _TREND_ALTS
        alt = alt_map.get(instrument.upper())
        if alt:
            alt_name, alt_reason = alt
            suggested_market       = f"Have you considered trying this on {alt_name}? {alt_reason}"
            suggested_market_label = alt_name
            suggested_market_code  = _DISPLAY_TO_CODE.get(alt_name)

    # ── Suggested timeframe ───────────────────────────────────────────────────
    suggested_timeframe       = None
    suggested_timeframe_value = None
    if interval == "5m" and signals < 10:
        suggested_timeframe = (
            "You might get more signals on the 15-minute chart — "
            "it cuts out noise while still being active enough to validate quickly."
        )
        suggested_timeframe_value = "15m"
    elif interval == "1h" and signals > 30:
        suggested_timeframe = (
            "With this many signals on the 1-hour chart, a 4-hour chart is worth exploring — "
            "fewer but higher-quality setups can be easier to manage."
        )
        # 4h is not a supported interval in the UI, so no action button

    return jsonify({
        "commentary":               commentary,
        "suggested_market":         suggested_market,
        "suggested_market_code":    suggested_market_code,
        "suggested_market_label":   suggested_market_label,
        "suggested_timeframe":      suggested_timeframe,
        "suggested_timeframe_value": suggested_timeframe_value,
    })


@app.route("/api/tweaks", methods=["POST"])
def api_tweaks():
    body        = request.get_json(force=True)
    instrument  = body.get("instrument", "")
    interval    = body.get("interval", "")
    anchor      = body.get("anchor", "")
    complement  = body.get("complement")
    session     = body.get("session_filter")
    indicator   = body.get("indicator_filter")
    sl_mult     = body.get("sl_multiplier", 1.0)
    tp_mult     = body.get("tp_multiplier", 3.0)
    win_pct     = body.get("win_pct", 0.0)
    signals     = body.get("signals", 0)

    parts = []
    if complement: parts.append(f"complement pattern: {complement}")
    if indicator:  parts.append(f"indicator filter: {indicator}")
    if session:    parts.append(f"session filter: {session}")
    filter_desc = ", ".join(parts) if parts else "none"

    prompt = (
        f"You are a trading strategy coach. The user has a strategy with these results. "
        f"Suggest exactly two specific tweaks they could try to get more signals or better results. "
        f"Each tweak must be a concrete actionable change such as: remove the MA cross filter to get more signals, "
        f"try removing the complement pattern, or switch to 15 minute candles. "
        f"For each tweak give a one-sentence reason. "
        f"Return JSON only, no other text. Format: "
        f'{{\"tweaks\": [{{\"tweak\": \"description\", \"reason\": \"one sentence\"}}, '
        f'{{\"tweak\": \"...\", \"reason\": \"...\"}}]}}\n\n'
        f"Strategy: {instrument} on {interval} timeframe\n"
        f"Anchor: {anchor}, Filters: {filter_desc}\n"
        f"SL: {sl_mult}×, TP: {tp_mult}×\n"
        f"Backtest: {win_pct}% win rate, {signals} signals in 30 days"
    )

    try:
        client = anthropic.Anthropic()
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=400,
            messages=[{"role": "user", "content": prompt}],
        )
        text  = msg.content[0].text.strip()
        start = text.find("{")
        end   = text.rfind("}") + 1
        data  = json.loads(text[start:end]) if start >= 0 and end > start else {"tweaks": []}
    except Exception as e:
        log.warning(f"AI tweaks failed: {e}")
        data = {"tweaks": []}

    return jsonify(data)


@app.route("/api/strategy-pnl", methods=["POST"])
def api_strategy_pnl():
    """
    Backtest a strategy from saved_at to now, returning cum_net and signals.
    Called by the My Strategies panel to show live PnL since save date.
    """
    body = request.get_json(force=True)
    instrument = body.get("instrument", "EURUSD")
    interval = body.get("interval", "5m")
    anchor = body.get("anchor")
    saved_at = body.get("saved_at")
    sl_mult = float(body.get("sl_multiplier", 1.0))
    tp_mult = float(body.get("tp_multiplier", 3.0))
    timeout = int(body.get("timeout", TIMEOUT))
    direction = body.get("direction", "both")
    session_filter = body.get("session_filter")
    complement = body.get("complement")
    connector = body.get("connector", "ordered")

    if not anchor:
        return jsonify({"error": "missing anchor"}), 400

    # Resolve instrument label to code
    meta = None
    instrument_label = None
    for label, cfg in INSTRUMENTS.items():
        if cfg["symbol"].replace("/", "") == instrument or label == instrument:
            meta = cfg
            instrument_label = label
            break
    if meta is None:
        return jsonify({"error": "unknown instrument"}), 400

    pip = meta["pip"]

    try:
        df = get_ohlc(instrument_label, days=90, interval=interval)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    if df.empty:
        return jsonify({"cum_net": 0.0, "signals": 0})

    # Filter to only bars after saved_at
    if saved_at:
        try:
            saved_ts = pd.Timestamp(saved_at, tz="UTC")
            df = df[df.index >= saved_ts]
        except Exception:
            pass

    if df.empty:
        return jsonify({"cum_net": 0.0, "signals": 0})

    direction_val = 0
    if direction == "long":
        direction_val = 1
    if direction == "short":
        direction_val = -1

    result = _backtest_pattern(
        df,
        pip,
        anchor,
        timeout=timeout,
        session=session_filter,
        sl_mult=sl_mult,
        tp_mult=tp_mult,
        direction_val=direction_val,
        instrument=instrument_label,
    )

    return jsonify({
        "cum_net": result.get("cum_net", 0.0),
        "signals": result.get("signals", 0),
        "win_pct": result.get("win_pct", 0.0),
    })


init_strategy_tables()
start_scheduler()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 7860))
    app.run(host="0.0.0.0", port=port, debug=False)
