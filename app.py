"""
app.py — CandleLab Flask application.
Gunicorn: 1 worker, 4 threads. PORT env var, default 7860.
"""
import os
import json
import html
import logging
import multiprocessing
import concurrent.futures
import threading
from collections import defaultdict
import numpy as np
import pandas as pd
from datetime import datetime, timezone
from flask import Flask, request, jsonify, render_template, abort

import anthropic
import psycopg2
from psycopg2.extras import RealDictCursor

from data import get_ohlc, INSTRUMENTS, _oanda_instrument_id
from chart_renderer import render_chart_with_trades
from backtest import compute_atr
from patterns import detect_all, PATTERNS
from signal_engine import detect_signal
from cache import cache_set, cache_get
from scheduler import start_scheduler
from poll_log import init_candlelab_poll_log_table, read_poll_log_pg, read_poll_log_view_rows
from strategy_store import (
    save_strategy,
    get_strategies_by_device,
    init_strategy_tables,
    load_strategy_trades,
    delete_strategy,
    lifecycle_save_draft,
    lifecycle_promote_live,
    lifecycle_close_live,
    lifecycle_delete_drafts,
    lifecycle_list_my_strategies,
    lifecycle_register_user,
)
from indicators import check_ma_cross_direction, check_rsi_extreme, check_ma_stable

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

ctx = multiprocessing.get_context("spawn")

app = Flask(__name__)

_startup_lock = threading.Lock()
_startup_done = False

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


def _parse_indicator_filter_config(raw) -> dict | None:
    """Normalize indicator_filter JSON or plain 'rsi' / 'ma_cross' (executor parity)."""
    if raw is None or raw == "":
        return None
    if isinstance(raw, dict):
        d = dict(raw)
    else:
        s = str(raw).strip()
        if s.startswith("{"):
            try:
                d = json.loads(s)
            except json.JSONDecodeError:
                return None
        else:
            typ = s.lower().replace(" ", "_")
            if typ == "rsi":
                return {"type": "rsi", "oversold": 30.0, "overbought": 70.0}
            if typ == "ma_cross":
                return {"type": "ma_cross", "direction": None}
            if typ == "ma_stable":
                return {"type": "ma_stable"}
            return None
    t = str(d.get("type", "")).strip().lower().replace(" ", "_")
    if not t:
        return None
    out: dict = {"type": t}
    if t == "rsi":
        out["oversold"] = float(d.get("oversold", 30))
        out["overbought"] = float(d.get("overbought", 70))
    elif t == "ma_cross":
        dr = d.get("direction")
        out["direction"] = str(dr).strip().lower() if dr is not None else None
    return out


def _make_indicator_fn(indicator_filter):
    if isinstance(indicator_filter, list):
        parts = [_make_indicator_fn(item) for item in indicator_filter]
        fns = [fn for fn in parts if fn is not None]
        if not fns:
            return None

        def combined(df, idx, dir_str):
            return all(fn(df, idx, dir_str) for fn in fns)

        return combined

    cfg = _parse_indicator_filter_config(indicator_filter)
    if not cfg:
        return None

    def fn(df, idx, dir_str):
        it = str(cfg.get("type", "")).lower()
        if it == "rsi":
            return check_rsi_extreme(
                df,
                idx,
                dir_str,
                float(cfg.get("oversold", 30)),
                float(cfg.get("overbought", 70)),
            )
        if it == "ma_cross":
            cross_dir = cfg.get("direction")
            if cross_dir is None:
                cross_dir = "bullish" if dir_str == "long" else "bearish"
            else:
                cross_dir = str(cross_dir).strip().lower()
            return check_ma_cross_direction(df, idx, cross_dir)
        if it == "ma_stable":
            return check_ma_stable(df, idx, dir_str)
        return True

    return fn


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

    for i in range(n - 1):
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

        entry_idx = i + 1
        nf = len(future_hi)
        exit_idx = entry_idx

        outcome    = "timeout_loss"
        exit_price = float(future_cl[-1])
        pnl_gross  = 0.0

        broke = False
        for j in range(len(future_hi)):
            if direction == 1:
                if future_hi[j] >= tp:
                    outcome    = "win"
                    exit_price = float(tp)
                    pnl_gross  = round(tp_pips * pip_val * lot_size, 2)
                    exit_idx = i + 1 + j
                    broke = True
                    break
                if future_cl[j] <= sl:
                    nx         = float(future_op_next[j]) if np.isfinite(future_op_next[j]) else float(future_cl[j])
                    outcome    = "loss"
                    exit_price = nx
                    sl_pips_actual = sl_pips  # already floored
                    slip_pips = direction * (float(nx) - float(sl)) / pip  # extra beyond SL level, negative = worse
                    pnl_gross = round((-RISK_DOLLARS) + (slip_pips * pip_val * lot_size), 2)
                    exit_idx = i + 1 + j
                    broke = True
                    break
            else:
                if future_lo[j] <= tp:
                    outcome    = "win"
                    exit_price = float(tp)
                    pnl_gross  = round(tp_pips * pip_val * lot_size, 2)
                    exit_idx = i + 1 + j
                    broke = True
                    break
                if future_cl[j] >= sl:
                    nx         = float(future_op_next[j]) if np.isfinite(future_op_next[j]) else float(future_cl[j])
                    outcome    = "loss"
                    exit_price = nx
                    sl_pips_actual = sl_pips  # already floored
                    slip_pips = direction * (float(nx) - float(sl)) / pip  # extra beyond SL level, negative = worse
                    pnl_gross = round((-RISK_DOLLARS) + (slip_pips * pip_val * lot_size), 2)
                    exit_idx = i + 1 + j
                    broke = True
                    break
        if not broke:
            exit_pips = direction * (exit_price - entry) / pip
            pnl_gross = round(exit_pips * pip_val * lot_size, 2)
            outcome   = "timeout_win" if exit_pips > 0 else "timeout_loss"
            exit_idx = i + nf

        pnl_net = round(pnl_gross, 2)
        is_win = outcome in ("win", "timeout_win")

        trades.append({
            "ts":        df.index[i],
            "signal":    direction,
            "entry":     entry,
            "entry_idx": entry_idx,
            "exit_idx":  exit_idx,
            "entry_price": round(entry, 5),
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
    complement: str | None = None,
    connector: str | None = None,
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

    if direction_val == 1:
        direction_str = "long"
    elif direction_val == -1:
        direction_str = "short"
    else:
        direction_str = "both"

    sig_array = detect_signal(
        signals_df,
        pattern_name,
        complement,
        connector,
        direction_str,
        window=10,
    )
    signals_series = pd.Series(sig_array, index=signals_df.index)

    trades = _simulate_trades(
        df, signals_series, atr, pip,
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


def _task_pattern_single_backtest(args):
    """ProcessPool wrapper: one-pattern ``_backtest_pattern`` (``detect_signal`` path)."""
    df, pip, pattern_name, direction_val, instrument = args
    r = _backtest_pattern(
        df,
        pip,
        pattern_name,
        timeout=TIMEOUT,
        instrument=instrument,
        direction_val=direction_val,
    )
    return pattern_name, r


def _complement_task(args):
    (comp_name, connector,
     df, signals_df, anchor_indices, atr, anchor_signals, anchor_win_pct,
     pip, anchor, instrument) = args

    result = _backtest_pattern(
        df,
        pip,
        anchor,
        timeout=TIMEOUT,
        instrument=instrument,
        complement=comp_name,
        connector=connector,
    )
    signals = int(result["signals"])
    if signals < 3:
        return None
    win_pct = float(result["win_pct"])
    delta = round(win_pct - anchor_win_pct, 1)
    return {
        "complement": comp_name,
        "connector":  connector,
        "signals":    signals,
        "win_pct":    win_pct,
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
            f"- {p['name']}: {p.get('win_pct', p.get('win_pct_5', 0))}% win rate ({p['signals']} signals)"
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


def _fetch_live_strategy_row(strategy_id: int) -> dict | None:
    url = os.environ.get("DATABASE_URL")
    if not url:
        return None
    conn = None
    try:
        conn = psycopg2.connect(url)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM candlelab_strategies_live WHERE id = %s",
                (int(strategy_id),),
            )
            row = cur.fetchone()
            return dict(row) if row else None
    except Exception:
        log.exception("fetch live strategy %s", strategy_id)
        return None
    finally:
        if conn is not None:
            conn.close()


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/chart/<int:strategy_id>")
def strategy_chart(strategy_id):
    """Live strategy PNG: wide scrollable candles, combo signals, trade overlays (`render_chart_with_trades`)."""
    init_strategy_tables()
    row = _fetch_live_strategy_row(strategy_id)
    if row is None:
        abort(404)

    instrument_label = _norm_instrument(row.get("instrument") or "EUR/USD")
    if instrument_label not in INSTRUMENTS:
        abort(404)

    anchor = (row.get("anchor") or "").strip()
    if not anchor or anchor not in PATTERNS:
        abort(404)

    complement = row.get("complement")
    if complement is not None and str(complement).strip() == "":
        complement = None
    connector = None
    if complement is not None:
        connector = row.get("connector") or "ordered"

    interval = row.get("interval") or "5m"
    direction_raw = (row.get("direction") or "both").strip().lower()
    if direction_raw in ("reversal", "trend"):
        direction_raw = "both"
    direction_str = direction_raw if direction_raw in ("long", "short", "both") else "both"

    direction_val = 0
    if direction_str == "long":
        direction_val = 1
    elif direction_str == "short":
        direction_val = -1

    session_filter = row.get("session")
    if session_filter is not None:
        sstr = str(session_filter).strip()
        if not sstr or sstr.lower() in ("all", "all sessions"):
            session_filter = None
        else:
            session_filter = sstr

    sl_mult = float(row.get("sl_mult") or 1.0)
    tp_mult = float(row.get("tp_mult") or 3.0)
    timeout = int(row.get("timeout") or TIMEOUT)

    pip = float(INSTRUMENTS[instrument_label]["pip"])

    try:
        go_live_ts = pd.Timestamp(row.get("go_live_at"))
        if go_live_ts.tzinfo is None:
            go_live_ts = go_live_ts.tz_localize("UTC")
        else:
            go_live_ts = go_live_ts.tz_convert("UTC")
    except Exception:
        abort(404)

    oanda_id = _oanda_instrument_id(instrument_label)
    live_df = _executor_read_continuous_series(oanda_id, go_live_ts)

    try:
        pre_live_df = get_ohlc(instrument_label, days=30, interval=interval)
    except Exception:
        pre_live_df = pd.DataFrame()

    frames = []
    if pre_live_df is not None and not pre_live_df.empty:
        frames.append(pre_live_df)
    if live_df is not None and not live_df.empty:
        frames.append(live_df)

    df = pd.DataFrame()
    if frames:
        df = pd.concat(frames)
        df = df[~df.index.duplicated(keep="last")]
        df = df.sort_index()

    chart_b64 = ""
    comp_disp = "—"
    conn_disp = "—"

    if not df.empty:
        signals_df = detect_all(df)
        sig_array = detect_signal(
            signals_df,
            anchor,
            complement,
            connector,
            direction_str,
            window=10,
        )

        anchor_signals = (
            signals_df[anchor].to_numpy(dtype=np.int8, copy=True)
            if anchor in signals_df.columns
            else np.zeros(len(df), dtype=np.int8)
        )
        if complement and complement in signals_df.columns:
            complement_signals = signals_df[complement].to_numpy(dtype=np.int8, copy=True)
            comp_disp = complement
            conn_disp = connector or "ordered"
        else:
            complement_signals = np.zeros(len(df), dtype=np.int8)

        atr = compute_atr(df)
        signals_series = pd.Series(np.asarray(sig_array, dtype=np.int8), index=df.index)

        trades = _simulate_trades(
            df,
            signals_series,
            atr,
            pip,
            sl_mult,
            tp_mult,
            timeout,
            session=session_filter,
            indicator_fn=None,
            direction_val=direction_val,
            instrument=instrument_label,
        )

        chart_b64 = render_chart_with_trades(
            df,
            anchor_signals,
            complement_signals,
            sig_array,
            trades,
            pip,
        )

    strategy_name = (row.get("strategy_name") or "Strategy").strip() or "Strategy"
    gl_str = ""
    raw_gl = row.get("go_live_at")
    try:
        gl_str = pd.Timestamp(raw_gl).strftime("%Y-%m-%d %H:%M UTC") if raw_gl else "—"
    except Exception:
        gl_str = str(raw_gl or "—")

    return render_template(
        "chart.html",
        strategy_name=strategy_name,
        instrument=instrument_label,
        anchor=anchor,
        complement=comp_disp,
        connector=conn_disp,
        go_live_at=gl_str,
        chart_b64=chart_b64,
    )


@app.route("/api/patterns")
def api_patterns():
    """
    Return ranked patterns for the selected market/timeframe.

    Implementation notes:
    - Uses a ProcessPool so each pattern's ``_backtest_pattern`` (``detect_signal``) run is parallel.
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

    direction_val = 0
    if direction == "long":
        direction_val = 1
    elif direction == "short":
        direction_val = -1

    tasks = [(df, pip, name, direction_val, instrument) for name in pattern_names]

    # ProcessPool is used to parallelize per-pattern simulations. On Windows this uses
    # "spawn", so the task payload must be picklable and entrypoints must be top-level.
    with concurrent.futures.ProcessPoolExecutor(max_workers=4, mp_context=ctx) as executor:
        futures = {executor.submit(_task_pattern_single_backtest, t): t[2] for t in tasks}
        raw = {}
        for fut in concurrent.futures.as_completed(futures):
            name, r = fut.result()
            raw[name] = r

    patterns_data = []
    for name in pattern_names:
        r = raw.get(name, {})
        wp = float(r.get("win_pct", 0.0))
        sig = int(r.get("signals", 0))
        patterns_data.append({
            "name":       name,
            "signals":    sig,
            "win_pct":    wp,
            "win_pct_5":  wp,
            "win_pct_10": wp,
            "win_pct_20": wp,
            "tip":        "",
        })

    patterns_data.sort(key=lambda x: x["win_pct"], reverse=True)

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

    combo_tasks = [
        (comp, conn, df, signals_df, anchor_indices, atr, anchor_signals, anchor_win_pct,
         pip, anchor, instrument)
        for comp in ALL_PATTERN_NAMES
        for conn in ("ordered", "any-order")
        if comp != anchor
    ]

    raw = []
    # Parallel evaluation: two connector modes per complement candidate.
    with concurrent.futures.ProcessPoolExecutor(max_workers=4, mp_context=ctx) as executor:
        futures = [executor.submit(_complement_task, t) for t in combo_tasks]
        for fut in concurrent.futures.as_completed(futures):
            r = fut.result()
            if r:
                raw.append(r)

    by_comp = defaultdict(dict)
    for r in raw:
        by_comp[r["complement"]][r["connector"]] = r

    merged_list = []
    for comp, sides in by_comp.items():
        ro = sides.get("ordered")
        ra = sides.get("any-order")
        o_sig = int(ro["signals"]) if ro else 0
        o_wp = float(ro["win_pct"]) if ro else 0.0
        a_sig = int(ra["signals"]) if ra else 0
        a_wp = float(ra["win_pct"]) if ra else 0.0
        if max(o_sig, a_sig) < 3:
            continue
        candidates = []
        if ro and ro["signals"] >= 3:
            candidates.append(ro)
        if ra and ra["signals"] >= 3:
            candidates.append(ra)
        if not candidates:
            continue
        best = max(candidates, key=lambda x: x["delta"])
        merged_list.append({
            "complement": comp,
            "ordered_signals": o_sig,
            "ordered_win_pct": round(o_wp, 1),
            "any_order_signals": a_sig,
            "any_order_win_pct": round(a_wp, 1),
            "connector": best["connector"],
            "delta": best["delta"],
            "win_pct": best["win_pct"],
            "signals": best["signals"],
        })

    merged_list.sort(key=lambda x: x["delta"], reverse=True)
    return jsonify(merged_list[:3])


@app.route("/api/indicator-check", methods=["POST"])
def api_indicator_check():
    body = request.get_json(force=True)
    anchor = body.get("anchor")
    complement = body.get("complement")
    has_complement = complement is not None and str(complement).strip() != ""
    connector = (body.get("connector") or "ordered") if has_complement else None
    direction = body.get("direction", "both")
    instrument_raw = body.get("instrument", "EUR/USD")
    interval = body.get("interval", "5m")
    indicator = body.get("indicator")
    indicator_filter = body.get("indicator_filter")

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

    indicator_fn = None
    if indicator_filter:
        indicator_fn = _make_indicator_fn(indicator_filter)
    elif indicator:
        indicator_fn = _make_indicator_fn(indicator)

    base_r = _backtest_pattern(
        df,
        pip,
        anchor,
        timeout=TIMEOUT,
        indicator_fn=None,
        instrument=instrument,
        complement=complement if has_complement else None,
        connector=connector if has_complement else None,
    )
    filtered_r = _backtest_pattern(
        df,
        pip,
        anchor,
        timeout=TIMEOUT,
        indicator_fn=indicator_fn,
        instrument=instrument,
        complement=complement if has_complement else None,
        connector=connector if has_complement else None,
    )

    return jsonify({
        "unfiltered_win_pct": base_r["win_pct"],
        "filtered_win_pct":   filtered_r["win_pct"],
        "delta":              round(filtered_r["win_pct"] - base_r["win_pct"], 1),
        "signals":            filtered_r["signals"],
        "base_signals":       base_r["signals"],
        "base_win_pct":       base_r["win_pct"],
        "filtered_signals":   filtered_r["signals"],
        "filtered_win_pct":   filtered_r["win_pct"],
    })


@app.route("/api/session-check", methods=["POST"])
def api_session_check():
    """
    Backtest the current strategy config under each named session window (and all sessions).
    Request body mirrors `/api/finalise` fields used for the core pattern backtest.
    """
    body = request.get_json(force=True) or {}
    instrument_raw = body.get("instrument", "EUR/USD")
    interval = body.get("interval", "5m")
    anchor = body.get("anchor", "")
    complement = body.get("complement")
    direction = body.get("direction", "both")
    has_complement = complement is not None and str(complement).strip() != ""
    connector = (body.get("connector") or "ordered") if has_complement else None
    sl_mult = float(body.get("sl_multiplier", 1.0))
    tp_mult = float(body.get("tp_multiplier", 3.0))
    timeout = int(body.get("timeout", TIMEOUT))
    timeout = max(5, min(timeout, 1000))
    indicator_filter = body.get("indicator_filter")

    if tp_mult <= sl_mult:
        return jsonify({"error": "tp_multiplier must be greater than sl_multiplier"}), 400

    instrument = _norm_instrument(instrument_raw)
    if instrument not in INSTRUMENTS:
        return jsonify({"error": "Unknown instrument"}), 400

    pip = INSTRUMENTS[instrument]["pip"]

    direction_val = 0
    if direction == "long":
        direction_val = 1
    elif direction == "short":
        direction_val = -1

    indicator_fn = _make_indicator_fn(indicator_filter) if indicator_filter else None

    try:
        df = get_ohlc(instrument, days=30, interval=interval)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    if df.empty:
        return jsonify({"error": "No data"}), 500

    def _one(sess):
        r = _backtest_pattern(
            df,
            pip,
            anchor,
            timeout=timeout,
            session=sess,
            indicator_fn=indicator_fn,
            direction_val=direction_val,
            sl_mult=sl_mult,
            tp_mult=tp_mult,
            instrument=instrument,
            complement=complement if has_complement else None,
            connector=connector if has_complement else None,
        )
        return {"signals": int(r["signals"]), "win_pct": float(r["win_pct"])}

    out = {
        "london": _one("London"),
        "new_york": _one("New York"),
        "asian": _one("Asian"),
        "overlap": _one("Overlap"),
        "all": _one(None),
    }
    return jsonify(out)


@app.route("/api/timeout-check", methods=["POST"])
def api_timeout_check():
    """
    Run the same core backtest as ``/api/finalise`` (single main interval) at each
    standard timeout value. OHLC is loaded once; ``_backtest_pattern`` runs six times.

    Accepts the same JSON fields as ``/api/finalise``; the engine path matches the
    ``_backtest_pattern`` invocation used inside ``api_finalise``'s main-interval ``_run``.
    """
    body = request.get_json(force=True) or {}
    instrument_raw = body.get("instrument", "EUR/USD")
    interval = body.get("interval", "5m")
    anchor = body.get("anchor", "")
    complement = body.get("complement")
    has_complement = complement is not None and str(complement).strip() != ""
    connector = (body.get("connector") or "ordered") if has_complement else None
    if connector == "optional":
        connector = "any-order"
    indicator_filter = body.get("indicator_filter")
    session_filter = body.get("session_filter")
    sl_mult = float(body.get("sl_multiplier", 1.0))
    tp_mult = float(body.get("tp_multiplier", 3.0))
    indicator_fn = _make_indicator_fn(indicator_filter) if indicator_filter else None

    if tp_mult <= sl_mult:
        return jsonify({"error": "tp_multiplier must be greater than sl_multiplier"}), 400

    instrument = _norm_instrument(instrument_raw)
    if instrument not in INSTRUMENTS:
        return jsonify({"error": "Unknown instrument"}), 400

    pip = INSTRUMENTS[instrument]["pip"]

    try:
        df = get_ohlc(instrument, days=30, interval=interval)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    if df.empty:
        return jsonify({"error": "No data"}), 500

    out = {}
    for t in (5, 10, 20, 50, 100, 1000):
        r = _backtest_pattern(
            df,
            pip,
            anchor,
            timeout=t,
            session=session_filter,
            indicator_fn=indicator_fn,
            sl_mult=sl_mult,
            tp_mult=tp_mult,
            instrument=instrument,
            complement=complement if has_complement else None,
            connector=connector if has_complement else None,
        )
        out[str(t)] = {"signals": int(r["signals"]), "win_pct": float(r["win_pct"])}
    return jsonify(out)


@app.route("/api/finalise", methods=["POST"])
def api_finalise():
    body = request.get_json(force=True)
    preview = body.get("preview", False)
    # When true, run full backtest + charts but skip device-bound save_strategy (draft/live use separate APIs).
    no_persist = body.get("no_persist", False)

    instrument_raw = body.get("instrument", "EUR/USD")
    interval = body.get("interval", "5m")
    anchor = body.get("anchor", "")
    complement = body.get("complement")
    has_complement = complement is not None and str(complement).strip() != ""
    connector = (body.get("connector") or "ordered") if has_complement else None
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

    indicator_fn = _make_indicator_fn(indicator_filter) if indicator_filter else None

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
            complement=complement if has_complement else None,
            connector=connector if has_complement else None,
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

    if no_persist:
        saved = {"id": None, "name": name or ""}
    else:
        try:
            saved = save_strategy({
                "name":             name,
                "patterns":         [anchor] + ([complement] if has_complement else []),
                "connectors":       [connector, connector] if has_complement else [],
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


@app.route("/candlelab-log")
def api_candlelab_log():
    """Last n poll-log JSON rows from ``candlelab_poll_log`` (OANDA id e.g. EUR_USD)."""
    inst = (request.args.get("instrument") or "").strip()
    try:
        n = int(request.args.get("n", 100))
    except (TypeError, ValueError):
        n = 100
    n = max(1, min(n, 5000))
    return jsonify(read_poll_log_pg(inst, n))


@app.route("/candlelab-log-view")
def candlelab_log_view():
    """HTML table: last 200 ``candlelab_poll_log`` rows (optional ``instrument`` query param)."""
    if not os.environ.get("DATABASE_URL"):
        return (
            "<!DOCTYPE html><html><body><p>DATABASE_URL not configured.</p></body></html>",
            503,
        )
    inst = (request.args.get("instrument") or "").strip() or None
    rows = read_poll_log_view_rows(limit=200, instrument=inst)
    subtitle = f"instrument={html.escape(inst)}" if inst else "last 200 rows"
    head = (
        "<!DOCTYPE html><html><head><meta charset=utf-8><title>CandleLab poll log</title>"
        "<style>body{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;margin:16px;"
        "background:#111;color:#eee}"
        "table{border-collapse:collapse;width:100%;max-width:1400px;font-size:13px}"
        "th,td{border:1px solid #444;padding:6px 8px;text-align:left;vertical-align:top}"
        "th{background:#222}tr:nth-child(even){background:#1a1a1a}"
        ".num{text-align:right}</style></head><body>"
        f"<h1>CandleLab poll log</h1><p>{html.escape(subtitle)}</p><table><thead><tr>"
        "<th>ts</th><th>instrument</th><th>session</th>"
        "<th class=num>hour_utc</th><th class=num>spread_pips</th>"
        "<th>patterns_detected</th><th>live_strategies_checked</th></tr></thead><tbody>"
    )
    parts: list[str] = [head]
    for r in rows:
        ts_s = str(r.get("ts") or "")
        pat = r.get("patterns_detected")
        live = r.get("live_strategies_checked")
        pat_s = html.escape(json.dumps(pat, separators=(",", ":"))) if pat is not None else ""
        live_s = html.escape(json.dumps(live, separators=(",", ":"))) if live is not None else ""
        parts.append(
            "<tr>"
            f"<td>{html.escape(ts_s)}</td>"
            f"<td>{html.escape(str(r.get('instrument') or ''))}</td>"
            f"<td>{html.escape(str(r.get('session') or ''))}</td>"
            f"<td class=num>{html.escape(str(r.get('hour_utc') if r.get('hour_utc') is not None else ''))}</td>"
            f"<td class=num>{html.escape(str(r.get('spread_pips') if r.get('spread_pips') is not None else ''))}</td>"
            f"<td>{pat_s}</td>"
            f"<td>{live_s}</td>"
            "</tr>"
        )
    parts.append("</tbody></table></body></html>")
    return "".join(parts)


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
    if complement is not None and str(complement).strip() == "":
        complement = None
    connector = None
    if complement is not None:
        connector = body.get("connector") or "ordered"

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
        complement=complement,
        connector=connector,
    )

    return jsonify({
        "cum_net": result.get("cum_net", 0.0),
        "signals": result.get("signals", 0),
        "win_pct": result.get("win_pct", 0.0),
    })


def _executor_parse_patterns_cell(raw) -> list:
    """Normalize JSONB ``patterns`` array to a Python list (logging / parity; signals use ``detect_all``)."""
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return []
    return []


def _executor_read_continuous_series(oanda_instrument: str, go_live_ts: pd.Timestamp) -> pd.DataFrame:
    """
    One row per unique ``candle_time`` from flattened ``candle_history`` (latest poll wins), ascending.
    """
    url = os.environ.get("DATABASE_URL")
    if not url:
        return pd.DataFrame()
    inst = (oanda_instrument or "").strip()
    if not inst:
        return pd.DataFrame()
    try:
        ts_db = go_live_ts.to_pydatetime() if hasattr(go_live_ts, "to_pydatetime") else go_live_ts
    except Exception:
        ts_db = go_live_ts

    sql = """
        SELECT DISTINCT ON ((candle->>'candle_time')::timestamptz)
            ep.ts AS poll_ts,
            ep.bid,
            ep.ask,
            (candle->>'candle_time')::timestamptz AS candle_time,
            (candle->'ohlc'->>'o')::numeric AS open,
            (candle->'ohlc'->>'h')::numeric AS high,
            (candle->'ohlc'->>'l')::numeric AS low,
            (candle->'ohlc'->>'c')::numeric AS close,
            candle->'patterns' AS patterns
        FROM executor_poll_log ep,
        LATERAL jsonb_array_elements(ep.candle_history) AS candle
        WHERE ep.instrument = %s
        AND ep.ts >= %s
        ORDER BY (candle->>'candle_time')::timestamptz ASC, ep.ts DESC
    """

    conn = None
    raw: list = []
    try:
        conn = psycopg2.connect(url)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (inst, ts_db))
            raw = cur.fetchall()
    except Exception:
        log.exception("executor_pnl: continuous series query failed")
        return pd.DataFrame()
    finally:
        if conn is not None:
            conn.close()

    if not raw:
        return pd.DataFrame()

    rows_out = []
    for r in raw:
        try:
            ct = pd.Timestamp(r["candle_time"])
            if ct.tzinfo is None:
                ct = ct.tz_localize("UTC")
            else:
                ct = ct.tz_convert("UTC")
        except Exception:
            continue
        try:
            rows_out.append({
                "poll_ts": r.get("poll_ts"),
                "candle_time": ct,
                "open": float(r["open"]),
                "high": float(r["high"]),
                "low": float(r["low"]),
                "close": float(r["close"]),
                "bid": float(r["bid"]) if r.get("bid") is not None else np.nan,
                "ask": float(r["ask"]) if r.get("ask") is not None else np.nan,
                "patterns": _executor_parse_patterns_cell(r.get("patterns")),
            })
        except (TypeError, ValueError, KeyError):
            continue

    if not rows_out:
        return pd.DataFrame()

    out = pd.DataFrame(rows_out)
    out = out.set_index("candle_time").sort_index()
    return out


def _executor_session_ok(ts_raw, session_filter: str | None) -> bool:
    if not session_filter or session_filter in ("All", "All Sessions"):
        return True
    try:
        ts = pd.Timestamp(ts_raw)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        else:
            ts = ts.tz_convert("UTC")
    except Exception:
        return True
    return bool(_session_mask(pd.DatetimeIndex([ts]), session_filter)[0])


def _executor_simulate_trade_pnl(
    entry: float,
    direction: int,
    sl: float,
    tp: float,
    trade_atr: float,
    sl_mult: float,
    tp_mult: float,
    pip: float,
    instrument_label: str,
    future_hi: np.ndarray,
    future_lo: np.ndarray,
    future_cl: np.ndarray,
    future_op_next: np.ndarray,
) -> tuple[bool, float]:
    """
    One-trade outcome and net PnL — same bar-walk and dollar math as ``_simulate_trades``,
    using explicit SL/TP price levels (``sl`` / ``tp`` from SL/TP × ATR).
    """
    pip_val = PIP_VALUES.get(instrument_label, 10.0)
    sl_dist_price = abs(float(entry) - float(sl))
    sl_pips = sl_dist_price / pip if pip > 0 else 0.0
    tp_pips = sl_pips * (tp_mult / sl_mult) if sl_mult > 0 else 0.0
    lot_size = (RISK_DOLLARS / (sl_pips * pip_val)) if sl_pips > 0 and pip_val > 0 else 0.0

    if len(future_hi) == 0:
        return False, 0.0

    outcome = "timeout_loss"
    exit_price = float(future_cl[-1])
    pnl_gross = 0.0

    for j in range(len(future_hi)):
        if direction == 1:
            if future_hi[j] >= tp:
                outcome = "win"
                exit_price = float(tp)
                pnl_gross = round(tp_pips * pip_val * lot_size, 2)
                break
            if future_cl[j] <= sl:
                nx = float(future_op_next[j]) if np.isfinite(future_op_next[j]) else float(future_cl[j])
                outcome = "loss"
                slip_pips = direction * (float(nx) - float(sl)) / pip
                pnl_gross = round((-RISK_DOLLARS) + (slip_pips * pip_val * lot_size), 2)
                break
        else:
            if future_lo[j] <= tp:
                outcome = "win"
                exit_price = float(tp)
                pnl_gross = round(tp_pips * pip_val * lot_size, 2)
                break
            if future_cl[j] >= sl:
                nx = float(future_op_next[j]) if np.isfinite(future_op_next[j]) else float(future_cl[j])
                outcome = "loss"
                slip_pips = direction * (float(nx) - float(sl)) / pip
                pnl_gross = round((-RISK_DOLLARS) + (slip_pips * pip_val * lot_size), 2)
                break
    else:
        exit_pips = direction * (exit_price - entry) / pip
        pnl_gross = round(exit_pips * pip_val * lot_size, 2)
        outcome = "timeout_win" if exit_pips > 0 else "timeout_loss"

    pnl_net = round(pnl_gross, 2)
    is_win = outcome in ("win", "timeout_win")
    return is_win, pnl_net


def _executor_aggregate_trades(trades: list[tuple[bool, float]]) -> dict:
    n = len(trades)
    if n == 0:
        return {"signals": 0, "wins": 0, "win_pct": 0.0, "cum_net": 0.0}
    wins = sum(1 for w, _ in trades if w)
    cum_net = round(sum(p for _, p in trades), 2)
    return {
        "signals": n,
        "wins": wins,
        "win_pct": round(wins / n * 100, 1),
        "cum_net": cum_net,
    }


def _executor_compute_from_dataframe(
    df: pd.DataFrame,
    anchor: str,
    complement: str | None,
    connector: str | None,
    direction_str: str,
    session_filter: str | None,
    sl_mult: float,
    tp_mult: float,
    timeout: int,
    tick_size: float,
    pip: float,
    instrument_label: str,
) -> tuple[list[tuple[bool, float]], list[tuple[bool, float]], list[tuple[bool, float]]]:
    """
    Continuous OHLC series: ``detect_all`` + ``detect_signal`` (same as historical backtest),
    then simulate each fired bar. ``raw`` uses next-bar open only; ``all`` applies bid/ask spread.
    """
    raw_trades: list[tuple[bool, float]] = []
    all_trades: list[tuple[bool, float]] = []
    clean_trades: list[tuple[bool, float]] = []
    if df is None or df.empty or len(df) < ATR_PERIOD + 2:
        return raw_trades, all_trades, clean_trades

    need = ["open", "high", "low", "close", "bid", "ask"]
    for c in need:
        if c not in df.columns:
            return raw_trades, all_trades, clean_trades

    ohlc = df[["open", "high", "low", "close"]].astype(float).sort_index()
    if anchor not in PATTERNS:
        return raw_trades, all_trades, clean_trades

    signals_df = detect_all(ohlc)
    sig_array = detect_signal(
        signals_df,
        anchor,
        complement,
        connector,
        direction_str,
        window=10,
    )
    n = len(ohlc)
    atr_full = compute_atr(ohlc)

    for i in range(n):
        if i >= len(sig_array):
            break
        direction_val = int(sig_array[i])
        if direction_val == 0:
            continue
        if not _executor_session_ok(ohlc.index[i], session_filter):
            continue
        if i + 1 >= n:
            continue

        trade_atr = float(atr_full.iloc[i])
        if not np.isfinite(trade_atr) or trade_atr <= 0:
            continue

        bid_v = df["bid"].iloc[i]
        ask_v = df["ask"].iloc[i]
        if pd.notna(bid_v) and pd.notna(ask_v):
            spread = max(0.0, float(ask_v) - float(bid_v))
        else:
            spread = 0.5 * tick_size

        entry_raw = float(ohlc["open"].iloc[i + 1])
        entry_all = entry_raw + direction_val * spread
        sl_r = entry_raw - direction_val * sl_mult * trade_atr
        tp_r = entry_raw + direction_val * tp_mult * trade_atr
        sl_a = entry_all - direction_val * sl_mult * trade_atr
        tp_a = entry_all + direction_val * tp_mult * trade_atr
        sig_close = float(ohlc["close"].iloc[i])
        clean = abs(sig_close - entry_all) <= 3.0 * tick_size

        end = min(i + 1 + int(timeout), n)
        sub = ohlc.iloc[i + 1 : end]
        if len(sub) == 0:
            continue

        fh = sub["high"].to_numpy(dtype=float)
        fl = sub["low"].to_numpy(dtype=float)
        fc = sub["close"].to_numpy(dtype=float)
        fo = sub["open"].to_numpy(dtype=float)
        L = len(sub)
        op_next = np.empty(L, dtype=float)
        for j in range(L):
            if j + 1 < L:
                op_next[j] = float(fo[j + 1])
            else:
                op_next[j] = float(fc[j])

        win_r, pnl_r = _executor_simulate_trade_pnl(
            entry_raw,
            direction_val,
            sl_r,
            tp_r,
            trade_atr,
            sl_mult,
            tp_mult,
            pip,
            instrument_label,
            fh,
            fl,
            fc,
            op_next,
        )
        raw_trades.append((win_r, pnl_r))

        win_a, pnl_a = _executor_simulate_trade_pnl(
            entry_all,
            direction_val,
            sl_a,
            tp_a,
            trade_atr,
            sl_mult,
            tp_mult,
            pip,
            instrument_label,
            fh,
            fl,
            fc,
            op_next,
        )
        all_trades.append((win_a, pnl_a))
        if clean:
            clean_trades.append((win_a, pnl_a))

    return raw_trades, all_trades, clean_trades


@app.route("/api/strategy/executor-pnl", methods=["POST"])
def api_strategy_executor_pnl():
    """
    Live executor stats: continuous OHLC from ``executor_poll_log`` (DISTINCT ON candle_time),
    ``detect_all`` + ``detect_signal`` aligned with historical backtest.
    """
    body = request.get_json(force=True)
    instrument = body.get("instrument", "EURUSD")
    anchor = body.get("anchor")
    complement = body.get("complement")
    if complement is not None and str(complement).strip() == "":
        complement = None
    connector = None
    if complement is not None:
        connector = body.get("connector") or "ordered"
    direction = body.get("direction", "both")
    session_filter = body.get("session_filter")
    sl_mult = float(body.get("sl_multiplier", 1.0))
    tp_mult = float(body.get("tp_multiplier", 3.0))
    timeout = int(body.get("timeout", TIMEOUT))
    go_live_at = body.get("go_live_at")
    tick_size = float(body.get("tick_size") or 0.0001)

    if not anchor:
        return jsonify({"error": "missing anchor"}), 400
    if not go_live_at:
        return jsonify({"error": "missing go_live_at"}), 400

    meta = None
    instrument_label = None
    for label, cfg in INSTRUMENTS.items():
        if cfg["symbol"].replace("/", "") == instrument or label == instrument:
            meta = cfg
            instrument_label = label
            break
    if meta is None:
        return jsonify({"error": "unknown instrument"}), 400

    pip = float(meta["pip"])

    direction_str = "both"
    if direction == "long":
        direction_str = "long"
    elif direction == "short":
        direction_str = "short"

    try:
        go_live_ts = pd.Timestamp(go_live_at, tz="UTC")
    except Exception:
        return jsonify({"error": "bad go_live_at"}), 400

    oanda_id = _oanda_instrument_id(instrument_label)
    series_df = _executor_read_continuous_series(oanda_id, go_live_ts)
    raw_t, all_t, clean_t = _executor_compute_from_dataframe(
        series_df,
        anchor,
        complement,
        connector,
        direction_str,
        session_filter,
        sl_mult,
        tp_mult,
        timeout,
        tick_size,
        pip,
        instrument_label,
    )

    agg_raw = _executor_aggregate_trades(raw_t)
    agg_all = _executor_aggregate_trades(all_t)
    agg_clean = _executor_aggregate_trades(clean_t)
    insufficient = agg_raw["signals"] == 0

    return jsonify({
        "raw": agg_raw,
        "all": agg_all,
        "clean": agg_clean,
        "insufficient": insufficient,
    })


@app.route("/api/user/register", methods=["POST"])
def api_user_register():
    body = request.get_json(force=True) or {}
    try:
        data, code = lifecycle_register_user(
            body.get("username"),
            body.get("pin"),
        )
        return jsonify(data), code
    except Exception as e:
        log.exception("api_user_register")
        return jsonify({"error": str(e)}), 500


@app.route("/api/strategy/save-draft", methods=["POST"])
def api_strategy_save_draft():
    body = request.get_json(force=True)
    try:
        data, code = lifecycle_save_draft(body)
        return jsonify(data), code
    except Exception as e:
        log.exception("api_strategy_save_draft")
        return jsonify({"error": str(e)}), 500


@app.route("/api/strategy/promote-live", methods=["POST"])
def api_strategy_promote_live():
    body = request.get_json(force=True)
    try:
        data, code = lifecycle_promote_live(body)
        return jsonify(data), code
    except Exception as e:
        log.exception("api_strategy_promote_live")
        return jsonify({"error": str(e)}), 500


@app.route("/api/strategy/close-live", methods=["POST"])
def api_strategy_close_live():
    body = request.get_json(force=True)
    try:
        data, code = lifecycle_close_live(body)
        return jsonify(data), code
    except Exception as e:
        log.exception("api_strategy_close_live")
        return jsonify({"error": str(e)}), 500


@app.route("/api/strategy/delete-draft", methods=["DELETE"])
def api_strategy_delete_draft():
    body = request.get_json(silent=True) or {}
    try:
        data, code = lifecycle_delete_drafts(body)
        return jsonify(data), code
    except Exception as e:
        log.exception("api_strategy_delete_draft")
        return jsonify({"error": str(e)}), 500


@app.route("/api/strategy/my-strategies", methods=["GET"])
def api_strategy_my_strategies():
    username = request.args.get("username") or ""
    pin = request.args.get("pin") or ""
    try:
        data, code = lifecycle_list_my_strategies(username, pin)
        return jsonify(data), code
    except Exception as e:
        log.exception("api_strategy_my_strategies")
        return jsonify({"error": str(e)}), 500


with _startup_lock:
    if not _startup_done:
        init_strategy_tables()
        init_candlelab_poll_log_table()
        start_scheduler()
        _startup_done = True

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 7860))
    app.run(host="0.0.0.0", port=port, debug=False)
