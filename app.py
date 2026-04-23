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
import requests
from psycopg2.extras import RealDictCursor

from data import get_ohlc, INSTRUMENTS, _oanda_instrument_id
from chart_renderer import render_trade_panels
from backtest import compute_atr
from patterns import detect_all, PATTERNS
from signal_engine import (
    detect_signal,
    SPREAD_COST_PIPS,
    SPREAD_CLEAN_THRESHOLD,
    SLIPPAGE_CIRCUIT_BREAKER,
)
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


@app.route("/debug/chart-png")
def debug_chart_png():
    from flask import send_file
    import os
    path = "/tmp/debug_chart.png"
    if not os.path.exists(path):
        return "No chart rendered yet", 404
    return send_file(path, mimetype="image/png")


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


def _indicator_type_string_for_chart(raw) -> str | None:
    """Lowercase type string(s) for chart overlays (e.g. ``rsi``, ``ma_cross`` → contains ``ma``)."""
    if raw is None or raw == "":
        return None
    if isinstance(raw, list):
        parts: list[str] = []
        for item in raw:
            cfg = _parse_indicator_filter_config(item)
            if cfg and cfg.get("type"):
                parts.append(str(cfg["type"]).lower())
        return " ".join(parts) if parts else None
    cfg = _parse_indicator_filter_config(raw)
    if not cfg or not cfg.get("type"):
        return None
    return str(cfg["type"]).lower()


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
    "Hammer/Hanging Man",
    "Shooting Star/Inv. Hammer",
    "Engulfing",
    "Morning/Evening Star",
}

_TREND_PATTERNS = {
    "Three Soldiers/Crows",
}

ALL_PATTERN_NAMES = list(PATTERNS.keys())


def _patterns_for_direction(strategy_type: str) -> list:
    """
    Return pattern names valid for the chosen strategy type.
    reversal → reversal patterns only
    continuation → continuation patterns only
    anything else → all patterns
    """
    if strategy_type == "reversal":
        return [p for p in ALL_PATTERN_NAMES if p in _REVERSAL_PATTERNS]
    if strategy_type == "continuation":
        return [p for p in ALL_PATTERN_NAMES if p in _TREND_PATTERNS]
    return ALL_PATTERN_NAMES


# ── Session filter ────────────────────────────────────────────────────────────

_SESSION_HOURS = {
    "London":   (8, 17),
    "New York": (13, 21),
    "Asian":    (0, 9),
    "Overlap":  (13, 17),
    "NY Close": (21, 23),
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
    instrument: str = "EUR/USD",
) -> list:
    """
    Simulate all trades for a single pattern.
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

        entry = float(df["open"].iloc[i + 1])
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

        pnl_pips_gross = (
            (pnl_gross / (pip_val * lot_size)) if lot_size > 0 and pip_val > 0 else 0.0
        )
        instr_key = instrument.replace("/", "_")
        spread = float(SPREAD_COST_PIPS.get(instr_key, 1.0))
        net_pnl_pips = pnl_pips_gross - spread
        result = "tp" if net_pnl_pips > 0 else "sl"
        pnl_net = round(net_pnl_pips * pip_val * lot_size, 2)
        is_win = net_pnl_pips > 0

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
            "pnl_pips": round(net_pnl_pips, 1),
            "spread_cost_pips": spread,
            "result": result,
        })

    return trades


def _backtest_pattern(
    df: pd.DataFrame,
    pip: float,
    pattern_name: str,
    timeout: int = TIMEOUT,
    session: str = None,
    indicator_fn=None,
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
        sl_mult, tp_mult, timeout, session, indicator_fn,
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
    df, pip, pattern_name, instrument = args
    r = _backtest_pattern(
        df,
        pip,
        pattern_name,
        timeout=TIMEOUT,
        instrument=instrument,
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
    """Live strategy PNG: per-trade OHLC panels (`render_trade_panels`)."""
    init_strategy_tables()
    row = _fetch_live_strategy_row(strategy_id)
    if row is None:
        abort(404)

    instrument_label = _norm_instrument(row.get("instrument") or "EUR/USD")
    if instrument_label not in INSTRUMENTS:
        abort(404)

    anchor = (row.get("pattern_1") or row.get("anchor") or "").strip()
    if not anchor or anchor not in PATTERNS:
        abort(404)

    raw_p2 = row.get("pattern_2") if row.get("pattern_2") is not None else row.get("complement")
    raw_cont = row.get("continuation")
    has_p2 = raw_p2 is not None and str(raw_p2).strip() != ""
    has_cont = raw_cont is not None and str(raw_cont).strip() != ""
    complement = raw_p2 if has_p2 else (raw_cont if has_cont else None)
    connector = row.get("connector")
    if connector is None:
        connector = "any-order" if has_p2 else ("ordered" if has_cont else None)

    interval = row.get("interval") or "5m"
    direction = row.get("direction") or "both"
    # direction now derived from pattern signal value — not passed to backtest

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
    indicator_type = _indicator_type_string_for_chart(row.get("indicator_filter"))

    if not df.empty:
        signals_df = detect_all(df)
        sig_array = detect_signal(
            signals_df,
            anchor,
            complement,
            connector,
            "both",
            window=10,
        )

        if complement and complement in signals_df.columns:
            comp_disp = complement
            conn_disp = connector or "ordered"

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
            instrument=instrument_label,
        )

        ma_live_map = _executor_read_ma_live_map_from_poll_log(oanda_id, go_live_ts)
        ma_fast_series, ma_slow_series = _chart_ma_series_aligned(
            df, pre_live_df, ma_live_map
        )
        chart_b64 = render_trade_panels(
            df,
            trades,
            pip,
            instrument_label,
            signals_df=signals_df,
            anchor=anchor,
            complement=complement or None,
            indicator_type=indicator_type,
            go_live_at=go_live_ts,
            ma_fast_series=ma_fast_series,
            ma_slow_series=ma_slow_series,
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
        indicator_type=indicator_type,
        go_live_at=gl_str,
        chart_b64=chart_b64,
    )


def _fetch_oanda_fills_for_strategy(strategy_name: str, go_live_ts: pd.Timestamp) -> dict:
    """
    Read OANDA fill data from the trades table in Postgres.
    Returns dict keyed by opened_at floored to minute (UTC) -> fill dict.
    """
    from data import get_conn
    try:
        with get_conn() as conn:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(
                """
                SELECT opened_at, oanda_fill, oanda_units, oanda_pl,
                       oanda_close_type, oanda_sl, oanda_tp, oanda_exit
                FROM trades
                WHERE strategy_name = %s AND opened_at >= %s
                """,
                (strategy_name, go_live_ts),
            )
            rows = cur.fetchall()
        result = {}
        for r in rows:
            opened_at = r.get("opened_at")
            if opened_at is None:
                continue
            try:
                ts = pd.Timestamp(opened_at)
                if ts.tzinfo is None:
                    ts = ts.tz_localize("UTC")
                else:
                    ts = ts.tz_convert("UTC")
                key = ts.floor("min")
            except Exception:
                continue
            result[key] = {
                "oanda_fill": r.get("oanda_fill"),
                "oanda_units": r.get("oanda_units"),
                "oanda_pl": r.get("oanda_pl"),
                "close_type": r.get("oanda_close_type"),
                "oanda_sl": r.get("oanda_sl"),
                "oanda_tp": r.get("oanda_tp"),
                "oanda_exit": r.get("oanda_exit"),
            }
        return result
    except Exception as e:
        log.warning("_fetch_oanda_fills_for_strategy: db fetch failed: %s", e)
        return {}


@app.route("/trades/<int:strategy_id>")
def strategy_trades(strategy_id):
    """Trade blotter: per-trade detail comparing signal replay vs all entries."""
    init_strategy_tables()
    row = _fetch_live_strategy_row(strategy_id)
    if row is None:
        abort(404)

    instrument_label = _norm_instrument(row.get("instrument") or "EUR/USD")
    if instrument_label not in INSTRUMENTS:
        abort(404)

    anchor = (row.get("pattern_1") or row.get("anchor") or "").strip()
    if not anchor or anchor not in PATTERNS:
        abort(404)

    raw_p2 = row.get("pattern_2") if row.get("pattern_2") is not None else row.get("complement")
    raw_cont = row.get("continuation")
    has_p2 = raw_p2 is not None and str(raw_p2).strip() != ""
    has_cont = raw_cont is not None and str(raw_cont).strip() != ""
    complement = raw_p2 if has_p2 else (raw_cont if has_cont else None)
    connector = row.get("connector")
    if connector is None:
        connector = "any-order" if has_p2 else ("ordered" if has_cont else None)

    sl_mult = float(row.get("sl_mult") or 1.0)
    tp_mult = float(row.get("tp_mult") or 3.0)
    timeout = int(row.get("timeout") or TIMEOUT)
    pip = float(INSTRUMENTS[instrument_label]["pip"])
    tick_size = pip

    indicator_filter = row.get("indicator_filter")
    indicator_fn = _make_indicator_fn(indicator_filter) if indicator_filter else None

    session_filter = row.get("session")
    if session_filter is not None:
        sstr = str(session_filter).strip()
        if not sstr or sstr.lower() in ("all", "all sessions"):
            session_filter = None
        else:
            session_filter = sstr

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
    raw_strategy_name = (row.get("strategy_name") or "").strip()

    trades = []
    if live_df is not None and not live_df.empty:
        trades = _executor_compute_trade_detail(
            live_df, anchor, complement, connector,
            session_filter, sl_mult, tp_mult, timeout,
            tick_size, pip, instrument_label, indicator_fn,
            strategy_id=strategy_id, go_live_ts=go_live_ts,
            strategy_name=raw_strategy_name,
        )

    # Fetch OANDA actual fills from trades table — keyed by opened_at floored to minute
    oanda_fills = {}
    try:
        oanda_fills = _fetch_oanda_fills_for_strategy(f"CandleLab:{raw_strategy_name}", go_live_ts)
    except Exception as e:
        log.warning("strategy_trades: oanda fills fetch failed: %s", e)

    # Annotate each trade with matched OANDA fill
    for t in trades:
        try:
            opened_at = t.get("opened_at")
            if opened_at is None:
                trade_key = None
            else:
                ts_pd = pd.Timestamp(opened_at)
                if ts_pd.tzinfo is None:
                    ts_pd = ts_pd.tz_localize("UTC")
                else:
                    ts_pd = ts_pd.tz_convert("UTC")
                trade_key = ts_pd.floor("min")
        except Exception:
            trade_key = None
        match = oanda_fills.get(trade_key) if trade_key else None
        t["oanda_fill"] = match["oanda_fill"] if match else None
        t["oanda_units"] = match["oanda_units"] if match else None
        t["oanda_pl"] = match["oanda_pl"] if match else None
        t["oanda_close_type"] = match["close_type"] if match else None
        t["oanda_sl"] = match["oanda_sl"] if match else None
        t["oanda_tp"] = match["oanda_tp"] if match else None
        t["oanda_exit"] = match["oanda_exit"] if match else None

    _oanda_pls = [t["oanda_pl"] for t in trades if t.get("oanda_pl") is not None]
    oanda_pnl_total = round(sum(_oanda_pls), 2) if _oanda_pls else None

    # Summary stats
    replay_wins = sum(1 for t in trades if t["replay_result"] == "TP")
    all_wins = sum(1 for t in trades if t["all_result"] == "TP")
    total = len(trades)
    replay_pnl_total = round(sum(t["replay_pnl"] for t in trades), 2)
    all_pnl_total = round(sum(t["all_pnl"] for t in trades if t["all_pnl"] is not None), 2)
    clean_pnl_total = round(sum(t["clean_pnl"] for t in trades if t.get("clean_pnl") is not None), 2)
    flipped = sum(1 for t in trades if t["replay_result"] != t["all_result"] and t["all_result"] is not None)

    strategy_name = (row.get("strategy_name") or "Strategy").strip()
    gl_str = ""
    try:
        gl_str = pd.Timestamp(row.get("go_live_at")).strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        gl_str = "—"

    return render_template(
        "trades.html",
        strategy_name=strategy_name,
        instrument=instrument_label,
        anchor=anchor,
        complement=complement or "—",
        connector=connector or "—",
        go_live_at=gl_str,
        trades=trades,
        total=total,
        replay_wins=replay_wins,
        all_wins=all_wins,
        replay_pnl_total=replay_pnl_total,
        all_pnl_total=all_pnl_total,
        clean_pnl_total=clean_pnl_total,
        oanda_pnl_total=oanda_pnl_total,
        flipped=flipped,
        strategy_id=strategy_id,
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
    strategy_type = request.args.get("strategy_type", "all")

    instrument = _norm_instrument(instrument_raw)
    if instrument not in INSTRUMENTS:
        return jsonify({"error": f"Unknown instrument: {instrument_raw}"}), 400

    cache_key = f"patterns:{instrument}:{interval}:{strategy_type}"
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

    pattern_names = _patterns_for_direction(strategy_type)

    tasks = [(df, pip, name, instrument) for name in pattern_names]

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
    strategy_type = body.get("strategy_type", "reversal")
    instrument_raw = body.get("instrument", "EUR/USD")
    interval = body.get("interval", "5m")

    instrument = _norm_instrument(instrument_raw)
    if instrument not in INSTRUMENTS:
        return jsonify({"error": "Unknown instrument"}), 400

    meta = INSTRUMENTS[instrument]
    pip = meta["pip"]

    if strategy_type == "continuation":
        return jsonify([])

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

    valid_complements = _patterns_for_direction(strategy_type)
    combo_tasks = [
        (comp, conn, df, signals_df, anchor_indices, atr, anchor_signals, anchor_win_pct,
         pip, anchor, instrument)
        for comp in valid_complements
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
    # direction now derived from pattern signal value — not passed to backtest
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
    # direction now derived from pattern signal value — not passed to backtest
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
        "ny_close": _one("NY Close"),
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
    strategy_type = body.get("strategy_type", "reversal")
    pattern_1 = body.get("pattern_1", "")
    pattern_2 = body.get("pattern_2")
    continuation = body.get("continuation")

    has_pattern_2 = pattern_2 is not None and str(pattern_2).strip() != ""
    has_continuation = continuation is not None and str(continuation).strip() != ""

    anchor = pattern_1
    complement = pattern_2 if has_pattern_2 else (continuation if has_continuation else None)
    connector = "any-order" if has_pattern_2 else ("ordered" if has_continuation else None)

    has_complement = complement is not None and str(complement).strip() != ""
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
                "strategy_type":    strategy_type,
                "pattern_1":        pattern_1,
                "pattern_2":        pattern_2,
                "continuation":     continuation,
                "instrument":       instrument,
                "interval":         interval,
                "bt_win_pct":       main_r["win_pct"],
                "bt_cum_net":       main_r["cum_net"],
                "bt_signals":       main_r["signals"],
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
    # direction now derived from pattern signal value — not passed to backtest
    session_filter = body.get("session_filter")
    complement = body.get("complement")
    if complement is not None and str(complement).strip() == "":
        complement = None
    connector = None
    if complement is not None:
        connector = body.get("connector") or "ordered"

    indicator_filter = body.get("indicator_filter")
    indicator_fn = _make_indicator_fn(indicator_filter) if indicator_filter else None

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

    result = _backtest_pattern(
        df,
        pip,
        anchor,
        timeout=timeout,
        session=session_filter,
        indicator_fn=indicator_fn,
        sl_mult=sl_mult,
        tp_mult=tp_mult,
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
        WITH candles AS (
            SELECT DISTINCT ON ((candle->>'candle_time')::timestamptz)
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
        ),
        quotes AS (
            SELECT DISTINCT ON (
                date_trunc('minute', ts) - interval '5 minutes'
            )
                date_trunc('minute', ts) - interval '5 minutes' AS candle_time,
                bid,
                ask
            FROM executor_poll_log
            WHERE instrument = %s
            AND ts >= %s - interval '6 minutes'
            AND bid IS NOT NULL
            AND ask IS NOT NULL
            ORDER BY 
                date_trunc('minute', ts) - interval '5 minutes' ASC,
                ts ASC
        )
        SELECT
            c.candle_time,
            c.open, c.high, c.low, c.close,
            c.patterns,
            q.bid, q.ask
        FROM candles c
        LEFT JOIN quotes q ON q.candle_time = c.candle_time
        ORDER BY c.candle_time ASC
    """

    conn = None
    raw: list = []
    try:
        conn = psycopg2.connect(url)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (inst, ts_db, inst, ts_db))
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


def _chart_ma_utc(ts) -> pd.Timestamp:
    t = pd.Timestamp(ts)
    if t.tzinfo is None:
        return t.tz_localize("UTC")
    return t.tz_convert("UTC")


def _executor_read_ma_live_map_from_poll_log(
    oanda_instrument: str, go_live_ts: pd.Timestamp
) -> dict[pd.Timestamp, tuple[float, float]]:
    """
    From each ``executor_poll_log`` row, take the last ``candle_history`` candle's
    ``indicators.ma_fast`` / ``ma_slow`` keyed by that candle's ``candle_time``.
    Latest poll wins per ``candle_time`` (same ordering idea as DISTINCT ON … ep.ts DESC).
    """
    url = os.environ.get("DATABASE_URL")
    if not url:
        return {}
    inst = (oanda_instrument or "").strip()
    if not inst:
        return {}
    try:
        ts_db = go_live_ts.to_pydatetime() if hasattr(go_live_ts, "to_pydatetime") else go_live_ts
    except Exception:
        ts_db = go_live_ts

    sql = """
        SELECT ep.ts AS poll_ts, ep.candle_history
        FROM executor_poll_log ep
        WHERE ep.instrument = %s
        AND ep.ts >= %s
    """

    conn = None
    raw: list = []
    try:
        conn = psycopg2.connect(url)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (inst, ts_db))
            raw = cur.fetchall()
    except Exception:
        log.exception("strategy_chart: executor_poll_log MA query failed")
        return {}
    finally:
        if conn is not None:
            conn.close()

    entries: list[tuple[pd.Timestamp, pd.Timestamp, float, float]] = []
    for r in raw:
        ch = r.get("candle_history")
        if isinstance(ch, str):
            try:
                ch = json.loads(ch)
            except json.JSONDecodeError:
                continue
        if not isinstance(ch, list) or len(ch) == 0:
            continue
        last = ch[-1]
        if not isinstance(last, dict):
            continue
        ct_raw = last.get("candle_time")
        if ct_raw is None:
            continue
        try:
            ct = _chart_ma_utc(ct_raw)
        except Exception:
            continue
        ind = last.get("indicators")
        if not isinstance(ind, dict):
            ind = {}
        mf_raw = ind.get("ma_fast")
        ms_raw = ind.get("ma_slow")
        if mf_raw is None or ms_raw is None:
            continue
        try:
            mf = float(mf_raw)
            ms = float(ms_raw)
        except (TypeError, ValueError):
            continue
        if not (np.isfinite(mf) and np.isfinite(ms)):
            continue
        try:
            poll_ts = _chart_ma_utc(r.get("poll_ts"))
        except Exception:
            continue
        entries.append((poll_ts, ct, mf, ms))

    entries.sort(key=lambda x: x[0], reverse=True)
    out: dict[pd.Timestamp, tuple[float, float]] = {}
    for _poll_ts, ct, mf, ms in entries:
        if ct in out:
            continue
        out[ct] = (mf, ms)
    return out


def _chart_ma_series_aligned(
    df: pd.DataFrame,
    pre_live_df: pd.DataFrame,
    ma_live_map: dict[pd.Timestamp, tuple[float, float]],
) -> tuple[pd.Series, pd.Series]:
    """
    Rolling SMA5/SMA20 on the full pre-live OHLC series; live bars overlay values from the poll log.
    """
    ma_fast_pre = (
        pre_live_df["close"].rolling(5).mean()
        if pre_live_df is not None and not pre_live_df.empty
        else pd.Series(dtype=float)
    )
    ma_slow_pre = (
        pre_live_df["close"].rolling(20).mean()
        if pre_live_df is not None and not pre_live_df.empty
        else pd.Series(dtype=float)
    )

    ma_fast = pd.Series(np.nan, index=df.index, dtype=float)
    ma_slow = pd.Series(np.nan, index=df.index, dtype=float)

    if not ma_fast_pre.empty:
        common = df.index.intersection(ma_fast_pre.index)
        if len(common):
            ma_fast.loc[common] = ma_fast_pre.loc[common].to_numpy(dtype=float)
            ma_slow.loc[common] = ma_slow_pre.loc[common].to_numpy(dtype=float)

    norm_label: dict[pd.Timestamp, pd.Timestamp] = {}
    for lab in df.index:
        try:
            nk = _chart_ma_utc(lab)
        except Exception:
            continue
        if nk not in norm_label:
            norm_label[nk] = lab

    for ct, (mf, ms) in ma_live_map.items():
        try:
            nk = _chart_ma_utc(ct)
        except Exception:
            continue
        lab = norm_label.get(nk)
        if lab is None:
            continue
        ma_fast.loc[lab] = mf
        ma_slow.loc[lab] = ms

    return ma_fast, ma_slow


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
    session_filter: str | None,
    sl_mult: float,
    tp_mult: float,
    timeout: int,
    tick_size: float,
    pip: float,
    instrument_label: str,
    indicator_fn=None,
) -> tuple[list, list, list, float]:
    """
    Continuous OHLC series: ``detect_all`` + ``detect_signal`` (same as historical backtest),
    then simulate each fired bar. ``raw`` uses next-bar open only; ``all`` uses bid/ask at entry bar.
    """
    raw_trades: list[tuple[bool, float]] = []
    all_trades: list[tuple[bool, float]] = []
    clean_trades: list[tuple[bool, float]] = []
    raw_spread_deduct_usd = 0.0
    if df is None or df.empty or len(df) < ATR_PERIOD + 2:
        return raw_trades, all_trades, clean_trades, raw_spread_deduct_usd

    need = ["open", "high", "low", "close", "bid", "ask"]
    for c in need:
        if c not in df.columns:
            return raw_trades, all_trades, clean_trades, raw_spread_deduct_usd

    df = df.sort_index()
    ohlc = df[["open", "high", "low", "close"]].astype(float)
    if anchor not in PATTERNS:
        return raw_trades, all_trades, clean_trades, raw_spread_deduct_usd

    signals_df = detect_all(ohlc)
    sig_array = detect_signal(
        signals_df,
        anchor,
        complement,
        connector,
        "both",
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
        if indicator_fn is not None:
            dir_str = "long" if direction_val == 1 else "short"
            if not indicator_fn(ohlc, i, dir_str):
                continue
        if i + 1 >= n:
            continue

        trade_atr = float(atr_full.iloc[i])
        if not np.isfinite(trade_atr) or trade_atr <= 0:
            continue

        # Raw entry: next bar open, no spread adjustment
        entry_raw = float(ohlc["open"].iloc[i + 1])
        sl_dist = _sl_distance_price(trade_atr, pip)
        tp_dist = sl_dist * (tp_mult / sl_mult)
        sl_r = entry_raw - direction_val * sl_dist
        tp_r = entry_raw + direction_val * tp_dist

        # All entries: use actual ask (BUY) or bid (SELL) at bar i+1
        # Use i+1 quote — this is the bar we actually transact on
        has_real_quote = False
        entry_all = entry_raw  # fallback
        spread_pips = 0.0
        if i < len(df):
            ask_next = df["ask"].iloc[i]
            bid_next = df["bid"].iloc[i]
            if pd.notna(ask_next) and pd.notna(bid_next):
                ask_next = float(ask_next)
                bid_next = float(bid_next)
                has_real_quote = True
                entry_all = ask_next if direction_val == 1 else bid_next
                spread_pips = (ask_next - bid_next) / pip

        # SL and TP are fixed at signal-time levels — not shifted with fill price
        sl_a = sl_r
        tp_a = tp_r

        # Clean threshold: exclude wide-spread entries
        instr_key = instrument_label.replace("/", "_")
        clean_threshold = SPREAD_CLEAN_THRESHOLD.get(instr_key, 4.0)
        is_clean = has_real_quote and spread_pips <= clean_threshold

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

        pip_val = PIP_VALUES.get(instrument_label, 10.0)
        sl_dist_r = abs(float(entry_raw) - float(sl_r))
        sl_pips_r = sl_dist_r / pip if pip > 0 else 0.0
        lot_size_r = (RISK_DOLLARS / (sl_pips_r * pip_val)) if sl_pips_r > 0 and pip_val > 0 else 0.0
        raw_spread_deduct_usd += float(SPREAD_COST_PIPS.get(instr_key, 1.0)) * pip_val * lot_size_r

        if has_real_quote:
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
            if is_clean:
                clean_trades.append((win_a, pnl_a))

    return raw_trades, all_trades, clean_trades, raw_spread_deduct_usd


def _executor_compute_trade_detail(
    df: pd.DataFrame,
    anchor: str,
    complement: str | None,
    connector: str | None,
    session_filter: str | None,
    sl_mult: float,
    tp_mult: float,
    timeout: int,
    tick_size: float,
    pip: float,
    instrument_label: str,
    indicator_fn=None,
    strategy_id: int | None = None,
    go_live_ts: pd.Timestamp | None = None,
    strategy_name: str | None = None,
) -> list[dict]:
    """
    Same signal detection and simulation as _executor_compute_from_dataframe
    but returns per-trade detail dicts for the blotter page.

    Each dict contains:
        ts: timestamp of signal bar (ISO string)
        direction: "BUY" or "SELL"
        replay_entry: next bar open (float)
        all_entry: actual ask (BUY) or bid (SELL) at signal close (float, None if no quote)
        spread_pips: (ask-bid)/pip at signal bar (float, None if no quote)
        is_clean: bool — directional slippage within SLIPPAGE_CIRCUIT_BREAKER (pips)
        sl: stop loss price (float)
        tp: take profit price (float)
        exit_price: actual exit price (float)
        replay_result: "TP" / "SL" / "TO"
        all_result: "TP" / "SL" / "TO" / None (None if no real quote)
        replay_pnl: net P&L in dollars for replay (float)
        all_pnl: net P&L in dollars for all entries (float, None if no real quote)
        atr: ATR value at signal bar (float)
    """
    trades = []
    if df is None or df.empty or len(df) < ATR_PERIOD + 2:
        return trades

    need = ["open", "high", "low", "close", "bid", "ask"]
    for c in need:
        if c not in df.columns:
            return trades

    df = df.sort_index()
    ohlc = df[["open", "high", "low", "close"]].astype(float)
    if anchor not in PATTERNS:
        return trades

    signals_df = detect_all(ohlc)
    sig_array = detect_signal(
        signals_df, anchor, complement, connector, "both", window=10,
    )
    n = len(ohlc)
    atr_full = compute_atr(ohlc)

    instr_key = instrument_label.replace("/", "_")
    spread_cost_pips = SPREAD_COST_PIPS.get(instr_key, 1.0)

    try:
        from data import get_conn
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT direction, entry_price, opened_at
                    FROM trades
                    WHERE strategy_name = %s
                    AND opened_at >= %s
                    ORDER BY opened_at ASC
                """, (f"CandleLab:{strategy_name}", go_live_ts))
                rows = cur.fetchall()
        db_lookup = {}
        for direction, entry_price, opened_at in rows:
            ts = pd.Timestamp(opened_at)
            if ts.tzinfo is None:
                ts = ts.tz_localize("UTC")
            key = (str(direction).strip().upper(), round(float(entry_price), 5))
            if key not in db_lookup:
                db_lookup[key] = ts
    except Exception as e:
        log.warning("_executor_compute_trade_detail: db lookup failed: %s", e)
        db_lookup = {}

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

        if indicator_fn is not None:
            dir_str = "long" if direction_val == 1 else "short"
            if not indicator_fn(ohlc, i, dir_str):
                continue

        signal_close = float(ohlc["close"].iloc[i])

        # Quotes at signal bar close (iloc[i])
        ask_v = df["ask"].iloc[i]
        bid_v = df["bid"].iloc[i]
        has_quote = pd.notna(ask_v) and pd.notna(bid_v)
        spread_pips_val = None
        all_entry = None
        if has_quote:
            ask_v = float(ask_v)
            bid_v = float(bid_v)
            spread_pips_val = round((ask_v - bid_v) / pip, 2)
            all_entry = ask_v if direction_val == 1 else bid_v

        slippage_pips = None
        if has_quote and all_entry is not None:
            if direction_val == 1:
                slippage_pips = round((float(all_entry) - signal_close) / pip, 2)
            else:
                slippage_pips = round((signal_close - float(all_entry)) / pip, 2)

        # Circuit breaker: exclude if fill is more than 3 pips worse than close
        # Allow if fill is better than close (negative slippage = price improvement)
        # Uses signed slippage: positive = worse fill, negative = better fill
        is_clean = (
            has_quote
            and slippage_pips is not None
            and slippage_pips <= SLIPPAGE_CIRCUIT_BREAKER
        )

        replay_entry = float(ohlc["open"].iloc[i + 1])

        # Apply same SL floor as _simulate_trades
        sl_dist = _sl_distance_price(trade_atr, pip)
        tp_dist = sl_dist * (tp_mult / sl_mult)

        sl_r = replay_entry - direction_val * sl_dist
        tp_r = replay_entry + direction_val * tp_dist

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
            op_next[j] = float(fo[j + 1]) if j + 1 < L else float(fc[j])

        win_r, pnl_r = _executor_simulate_trade_pnl(
            replay_entry, direction_val, sl_r, tp_r, trade_atr,
            sl_mult, tp_mult, pip, instrument_label, fh, fl, fc, op_next,
        )
        # Deduct flat spread cost from replay P&L
        pip_val = PIP_VALUES.get(instrument_label, 10.0)
        sl_pips_r = sl_dist / pip if pip > 0 else 0.0
        lot_size_r = (
            (RISK_DOLLARS / (sl_pips_r * pip_val))
            if sl_pips_r > 0 and pip_val > 0
            else 0.0
        )
        replay_pnl = round(pnl_r - spread_cost_pips * pip_val * lot_size_r, 2)
        replay_result = "TP" if win_r else "SL"

        # Determine exit price from simulation
        # Walk forward to find exit bar
        exit_price = float(fc[-1])
        for j in range(len(fh)):
            if direction_val == 1:
                if fh[j] >= tp_r:
                    exit_price = tp_r
                    break
                if fc[j] <= sl_r:
                    exit_price = op_next[j]
                    break
            else:
                if fl[j] <= tp_r:
                    exit_price = tp_r
                    break
                if fc[j] >= sl_r:
                    exit_price = op_next[j]
                    break

        all_result = None
        all_pnl = None
        if has_quote:
            # SL and TP are fixed at signal-time levels — not shifted with fill price
            sl_a = sl_r
            tp_a = tp_r
            win_a, pnl_a = _executor_simulate_trade_pnl(
                all_entry, direction_val, sl_a, tp_a, trade_atr,
                sl_mult, tp_mult, pip, instrument_label, fh, fl, fc, op_next,
            )
            all_result = "TP" if win_a else "SL"
            all_pnl = round(pnl_a, 2)

        atr_actual = round(trade_atr / pip, 1)
        atr_used = round(sl_dist / pip, 1) if pip > 0 else None

        clean_pnl = None
        if is_clean and has_quote and all_pnl is not None:
            clean_pnl = all_pnl

        trades.append({
            "ts":             ohlc.index[i].strftime("%d/%m %H:%M"),
            "ts_raw":         ohlc.index[i],
            "direction":      "BUY" if direction_val == 1 else "SELL",
            "close":          round(signal_close, 5),
            "replay_entry":   round(replay_entry, 5),
            "all_entry":      round(all_entry, 5) if all_entry else None,
            "slippage_pips":  slippage_pips,
            "spread_pips":    spread_pips_val,
            "is_clean":       is_clean,
            "sl":             round(sl_r, 5),
            "tp":             round(tp_r, 5),
            "exit_price":     round(exit_price, 5),
            "replay_result":  replay_result,
            "all_result":     all_result,
            "replay_pnl":     replay_pnl,
            "all_pnl":        all_pnl,
            "atr_actual":     atr_actual,
            "atr_used":       atr_used,
            "atr":            atr_actual,
            "clean_pnl":      clean_pnl,
        })
        t = trades[-1]
        raw_dir = str(t.get("direction", "")).strip().upper()
        dir_key = "BUY" if raw_dir in ("BUY", "LONG") else "SELL" if raw_dir in ("SELL", "SHORT") else raw_dir
        entry_key = round(float(t.get("all_entry") or 0), 5)
        t["opened_at"] = db_lookup.get((dir_key, entry_key))

    return trades


def _fetch_true_pnl(strategy_name: str, go_live_ts: pd.Timestamp) -> dict | None:
    base = (strategy_name or "").strip()
    if not base:
        return None
    try:
        ts = pd.Timestamp(go_live_ts)
        ts = ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
        cutoff = ts.to_pydatetime()
    except Exception:
        return None
    try:
        from data import get_conn
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT pnl_pips, result FROM trades
                WHERE (strategy_name = %s OR strategy_name = %s)
                AND opened_at >= %s
            """, (base, f"CandleLab:{base}", cutoff))
            rows = cur.fetchall()
    except Exception:
        return None
    if not rows:
        return None
    total = len(rows)
    wins = sum(1 for r in rows if str(r[1] or "").lower() in ("tp", "win"))
    cum_pips = sum(float(r[0]) for r in rows if r[0] is not None)
    return {
        "signals": total,
        "wins": wins,
        "win_pct": round(wins / total * 100, 1),
        "cum_net": round(cum_pips, 1),
    }


@app.route("/api/strategy/true-pnl", methods=["POST"])
def api_strategy_true_pnl():
    body = request.get_json(force=True)
    strategy_name = body.get("strategy_name", "")
    go_live_at = body.get("go_live_at")
    if not strategy_name or not go_live_at:
        return jsonify({}), 400
    try:
        go_live_ts = pd.Timestamp(go_live_at, tz="UTC")
        result = _fetch_true_pnl(strategy_name, go_live_ts)
        return jsonify(result or {})
    except Exception:
        return jsonify({}), 500


@app.route("/api/strategy/oanda-fills", methods=["POST"])
def api_strategy_oanda_fills():
    """
    Fetch actual OANDA fills for a strategy and match against Postgres trades rows.
    Uses clientOrderID (cl-strat-{strategy_id}) for clean open matching.
    Closes matched via tradeID linkage.
    """
    body = request.get_json(force=True)
    strategy_id = body.get("strategy_id")
    strategy_name = body.get("strategy_name", "")
    go_live_at = body.get("go_live_at")

    if not strategy_id or not go_live_at:
        return jsonify({"error": "missing strategy_id or go_live_at"}), 400

    oanda_token = os.environ.get("OANDA_API_TOKEN", "")
    oanda_base = os.environ.get("OANDA_BASE_URL", "https://api-fxpractice.oanda.com")
    oanda_account = os.environ.get("OANDA_ACCOUNT_ID", "")
    if not oanda_token or not oanda_account:
        return jsonify({"error": "missing OANDA credentials"}), 500

    headers_oanda = {"Authorization": f"Bearer {oanda_token}"}
    client_order_prefix = f"cl-strat-{strategy_id}"

    try:
        go_live_ts = pd.Timestamp(go_live_at, tz="UTC")
        from_str = go_live_ts.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return jsonify({"error": "bad go_live_at"}), 400

    # ── 1. Fetch paginated OANDA transactions ──
    all_transactions = []
    try:
        r = requests.get(
            f"{oanda_base}/v3/accounts/{oanda_account}/transactions",
            headers=headers_oanda,
            params={"from": from_str, "type": "ORDER_FILL"},
            timeout=15,
        )
        r.raise_for_status()
        pages = r.json().get("pages", [])
        for page_url in pages:
            pr = requests.get(page_url, headers=headers_oanda, timeout=15)
            pr.raise_for_status()
            all_transactions.extend(pr.json().get("transactions", []))
    except Exception as e:
        log.warning("oanda_fills: transaction fetch failed: %s", e)
        return jsonify({"error": "oanda_fetch_failed"}), 500

    if not all_transactions:
        return jsonify({"fills": [], "summary": {}})

    # ── 2. Split opens and closes ──
    opens = {}
    closes = {}
    for tx in all_transactions:
        trade_opened = tx.get("tradeOpened")
        trades_closed = tx.get("tradesClosed")
        if trade_opened:
            tid = str(trade_opened.get("tradeID", ""))
            if tid:
                opens[tid] = tx
        if trades_closed:
            for tc in trades_closed:
                tid = str(tc.get("tradeID", ""))
                if tid:
                    closes[tid] = {"tx": tx, "detail": tc}

    # ── 3. Filter opens by clientOrderID prefix ──
    strategy_opens = {
        tid: tx for tid, tx in opens.items()
        if str(tx.get("clientOrderID", "")).startswith(client_order_prefix)
    }

    if not strategy_opens:
        return jsonify({"fills": [], "summary": {}})

    # ── 4. Load matching Postgres trades ──
    pg_trades = {}
    try:
        from data import get_conn
        with get_conn() as conn:
            cur = conn.cursor()
            base_name = strategy_name.replace("CandleLab:", "").strip()
            cur.execute("""
                SELECT trade_uuid, direction, entry_price, exit_price,
                       pnl_pips, result, opened_at, closed_at, sl_pips, tp_pips
                FROM trades
                WHERE (strategy_name = %s OR strategy_name = %s)
                AND opened_at >= %s
                ORDER BY opened_at
            """, (base_name, f"CandleLab:{base_name}", go_live_ts.to_pydatetime()))
            rows = cur.fetchall()
            for row in rows:
                pg_trades[str(row[0])] = {
                    "direction": row[1],
                    "entry_price": float(row[2]) if row[2] else None,
                    "exit_price": float(row[3]) if row[3] else None,
                    "pnl_pips": float(row[4]) if row[4] else None,
                    "result": row[5],
                    "opened_at": row[6],
                    "closed_at": row[7],
                    "sl_pips": float(row[8]) if row[8] else None,
                    "tp_pips": float(row[9]) if row[9] else None,
                }
    except Exception as e:
        log.warning("oanda_fills: pg query failed: %s", e)

    # ── 5. Match OANDA opens to Postgres by time proximity ──
    pg_list = sorted(pg_trades.values(), key=lambda x: x["opened_at"])

    fills = []
    total_pl = 0.0
    wins = 0
    losses = 0

    for tid, open_tx in sorted(strategy_opens.items(), key=lambda x: x[1]["time"]):
        oanda_time = pd.Timestamp(open_tx["time"], tz="UTC")
        oanda_price = float(open_tx.get("price", 0) or 0)
        oanda_units = abs(int(open_tx.get("units", 0) or 0))
        oanda_direction = "BUY" if int(open_tx.get("units", 0)) > 0 else "SELL"

        close_info = closes.get(tid)
        oanda_pl = None
        close_type = None
        if close_info:
            oanda_pl = float(close_info["detail"].get("realizedPL", 0) or 0)
            close_type = close_info["tx"].get("reason", "")
            total_pl += oanda_pl
            if oanda_pl > 0:
                wins += 1
            elif oanda_pl < 0:
                losses += 1

        # Match to Postgres row by time proximity (2 min window)
        pg_match = None
        for pg in pg_list:
            pg_open = pd.Timestamp(pg["opened_at"]).tz_localize("UTC") if pg["opened_at"].tzinfo is None else pd.Timestamp(pg["opened_at"]).tz_convert("UTC")
            if abs((oanda_time - pg_open).total_seconds()) <= 120:
                pg_match = pg
                break

        fills.append({
            "time": oanda_time.isoformat(),
            "direction": oanda_direction,
            "oanda_units": oanda_units,
            "oanda_fill": oanda_price,
            "oanda_pl": round(oanda_pl, 2) if oanda_pl is not None else None,
            "close_type": close_type,
            "executor_entry": pg_match["entry_price"] if pg_match else None,
            "executor_pnl_pips": pg_match["pnl_pips"] if pg_match else None,
            "sl_pips": pg_match["sl_pips"] if pg_match else None,
            "tp_pips": pg_match["tp_pips"] if pg_match else None,
            "result": pg_match["result"] if pg_match else None,
            "slippage_pips": round(abs(oanda_price - pg_match["entry_price"]) / 0.0001, 1) if pg_match and pg_match["entry_price"] else None,
        })

    total = wins + losses
    summary = {
        "total_fills": len(strategy_opens),
        "closed": total,
        "wins": wins,
        "losses": losses,
        "win_pct": round(wins / total * 100, 1) if total > 0 else 0,
        "total_pl_usd": round(total_pl, 2),
    }

    return jsonify({"fills": fills, "summary": summary})


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
    # direction now derived from pattern signal value — not passed to backtest
    session_filter = body.get("session_filter")
    sl_mult = float(body.get("sl_multiplier", 1.0))
    tp_mult = float(body.get("tp_multiplier", 3.0))
    timeout = int(body.get("timeout", TIMEOUT))
    go_live_at = body.get("go_live_at")
    tick_size = float(body.get("tick_size") or 0.0001)
    indicator_filter = body.get("indicator_filter")
    indicator_fn = _make_indicator_fn(indicator_filter) if indicator_filter else None
    strategy_name = body.get("strategy_name", "")

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

    try:
        go_live_ts = pd.Timestamp(go_live_at, tz="UTC")
    except Exception:
        return jsonify({"error": "bad go_live_at"}), 400

    oanda_id = _oanda_instrument_id(instrument_label)
    # Cap data window to 30 days max to prevent OOM on Railway free tier
    capped_go_live_ts = max(go_live_ts, pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=30))
    series_df = _executor_read_continuous_series(oanda_id, capped_go_live_ts)
    raw_t, all_t, clean_t, raw_spread_deduct_usd = _executor_compute_from_dataframe(
        series_df,
        anchor,
        complement,
        connector,
        session_filter,
        sl_mult,
        tp_mult,
        timeout,
        tick_size,
        pip,
        instrument_label,
        indicator_fn=indicator_fn,
    )

    agg_raw = _executor_aggregate_trades(raw_t)
    # cum_net is USD (_executor_aggregate_trades sums executor dollar PnLs). Subtract spread in USD:
    # sum over signals of SPREAD_COST_PIPS × pip_val × lot_size (matches ``_simulate_trades``).
    if agg_raw["signals"] > 0:
        agg_raw["cum_net"] = round(agg_raw["cum_net"] - raw_spread_deduct_usd, 2)
    agg_all = _executor_aggregate_trades(all_t)
    agg_clean = _executor_aggregate_trades(clean_t)
    insufficient = agg_raw["signals"] == 0

    true_pnl = _fetch_true_pnl(strategy_name, go_live_ts) if strategy_name else None

    return jsonify({
        "raw":          agg_raw,
        "all":          agg_all,
        "clean":        agg_clean,
        "true":         true_pnl,
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
        # init_strategy_tables()
        init_candlelab_poll_log_table()
        start_scheduler()
        _startup_done = True

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 7860))
    app.run(host="0.0.0.0", port=port, debug=False)
