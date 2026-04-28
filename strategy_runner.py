"""
strategy_runner.py — ADR-017 coordinator: DB + pattern signals + simulation_engine.
No Flask. oanda_candles + executor_poll_log reads only.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone, timedelta

import pandas as pd
from psycopg2.extras import RealDictCursor

from data import get_conn
from indicator_utils import (
    _parse_indicator_filter_config,
    compute_h1_atr_series_from_m5,
    passes_indicator,
)
from patterns import detect_all
from signal_engine import detect_signal
from simulation_engine import get_pip, run_simulation
from time_utils import to_utc_timestamp

log = logging.getLogger(__name__)

WARMUP_BARS = 200
COMPLEMENT_WINDOW = 10


def _instrument_to_oanda(instrument_label: str) -> str:
    return str(instrument_label).replace("/", "_")


def _fetch_oanda_candles(oanda_instrument: str, from_ts) -> pd.DataFrame:
    cols = [
        "open",
        "high",
        "low",
        "close",
        "bid_open",
        "bid_close",
        "ask_open",
        "ask_close",
        "volume",
    ]
    empty = pd.DataFrame(columns=cols)
    empty.index.name = "time"
    try:
        ts_db = (
            from_ts.to_pydatetime()
            if hasattr(from_ts, "to_pydatetime")
            else from_ts
        )
    except Exception:
        ts_db = from_ts
    inst = (oanda_instrument or "").strip()
    if not inst:
        return empty
    sql = """
        SELECT time, open, high, low, close, bid_open, bid_close, ask_open, ask_close, volume
        FROM oanda_candles
        WHERE instrument = %s AND time >= %s
        ORDER BY time ASC
    """
    try:
        with get_conn() as conn:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(sql, (inst, ts_db))
            rows = cur.fetchall()
    except Exception:
        log.exception("_fetch_oanda_candles")
        return empty
    if not rows:
        return empty
    df = pd.DataFrame(rows)
    df["time"] = pd.to_datetime(df["time"], utc=True)
    df = df.set_index("time").sort_index()
    for c in cols:
        if c not in df.columns:
            df[c] = 0.0 if c == "volume" else float("nan")
    return df[cols]


def _get_h1_atr(df_m5: pd.DataFrame) -> pd.Series:
    return compute_h1_atr_series_from_m5(df_m5)


def _resolve_col(signals_df: pd.DataFrame, pattern_name: str) -> str | None:
    if pattern_name is None:
        return None
    key = str(pattern_name).strip().lower()
    for col in signals_df.columns:
        if str(col).strip().lower() == key:
            return str(col)
    return None


def _lookup_h1_atr(h1_atr: pd.Series, signal_time: pd.Timestamp, pip: float) -> float:
    floor_min = 5.0 * float(pip)
    if h1_atr is None or h1_atr.empty:
        return floor_min
    st = to_utc_timestamp(signal_time)
    if st is None:
        return floor_min
    hkey = st.floor("h")
    val = None
    try:
        if hkey in h1_atr.index:
            val = float(h1_atr.loc[hkey])
    except Exception:
        val = None
    if val is None or (isinstance(val, float) and (val != val or val <= 0)):
        s = h1_atr.sort_index()
        try:
            v2 = s.asof(st)
            val = float(v2) if v2 == v2 and v2 > 0 else None
        except Exception:
            val = None
    if val is None or val != val or val <= 0:
        return floor_min
    return max(floor_min, val)


def _session_bar_ok(ts: pd.Timestamp, session_filter: str | None) -> bool:
    if session_filter is None:
        return True
    s = str(session_filter).strip().lower()
    if not s or s == "all":
        return True
    h = int(to_utc_timestamp(ts).hour)
    if "london" in s:
        return 7 <= h <= 15
    if "new" in s or "york" in s:
        return 12 <= h <= 20
    if "asian" in s or "asia" in s:
        return h == 23 or 0 <= h <= 7
    return True


def _empty_agg(mode: str = "aggressive") -> dict:
    return {
        "signals": 0,
        "wins": 0,
        "win_pct": 0.0,
        "cum_net": 0.0,
        "mode": mode,
    }


def _aggregate_sim(results: list[dict], mode: str) -> dict:
    executed = [r for r in results if r.get("result") in ("WIN", "LOSS", "BREAKEVEN")]
    count = len(executed)
    if count == 0:
        return _empty_agg(mode)
    wins = sum(1 for r in executed if r.get("result") == "WIN")
    cum_net = sum(
        float(r["pnl_dollars"])
        for r in executed
        if r.get("pnl_dollars") is not None
    )
    return {
        "signals": count,
        "wins": wins,
        "win_pct": round(wins / count * 100, 1),
        "cum_net": round(cum_net, 2),
        "mode": mode,
    }


def run_30d_backtest(
    instrument_label: str,
    anchor: str,
    complement: str | None,
    connector: str | None,
    session_filter: str | None,
    indicator_filter,
    sl_mult: float,
    tp_mult: float,
    timeout: int,
    continuation: str | None = None,
    reference_date=None,
) -> dict:
    oanda_instrument = _instrument_to_oanda(instrument_label)
    pip = get_pip(oanda_instrument)
    if reference_date is not None:
        now = to_utc_timestamp(reference_date)
    else:
        now = to_utc_timestamp(datetime.now(timezone.utc))
    from_ts = now - timedelta(days=30) - timedelta(minutes=WARMUP_BARS * 5)
    strict_cutoff = to_utc_timestamp(now - timedelta(days=30))

    candles = _fetch_oanda_candles(oanda_instrument, from_ts)
    if candles.empty:
        return _empty_agg("aggressive")

    h1_atr = _get_h1_atr(candles)
    ohlc = candles[["open", "high", "low", "close"]]
    signals_df = detect_all(ohlc)
    anchor_col = _resolve_col(signals_df, anchor)
    if anchor_col is None:
        return _empty_agg("aggressive")

    comp_col = None
    if complement is not None and str(complement).strip():
        comp_col = _resolve_col(signals_df, str(complement).strip())

    continuation_col = None
    if continuation is not None and str(continuation).strip():
        continuation_col = _resolve_col(signals_df, str(continuation).strip())

    sig_array = detect_signal(
        signals_df,
        anchor_col,
        comp_col,
        connector,
        "both",
        window=COMPLEMENT_WINDOW,
        continuation=continuation_col,
    )
    ind_cfg = _parse_indicator_filter_config(indicator_filter)

    signals: list[dict] = []
    n = len(sig_array)
    for i in range(n):
        if int(sig_array[i]) == 0:
            continue
        tsi = to_utc_timestamp(candles.index[i])
        if tsi < strict_cutoff:
            continue
        if not _session_bar_ok(tsi, session_filter):
            continue
        dir_str = "long" if int(sig_array[i]) == 1 else "short"
        if not passes_indicator(ind_cfg, candles, i, dir_str):
            continue
        sig_close = float(candles["close"].iloc[i])
        signal_time = tsi
        sl_dist = _lookup_h1_atr(h1_atr, signal_time, pip)
        direction = "BUY" if int(sig_array[i]) == 1 else "SELL"
        if direction == "BUY":
            sl = sig_close - sl_dist * sl_mult
            tp = sig_close + sl_dist * tp_mult
        else:
            sl = sig_close + sl_dist * sl_mult
            tp = sig_close - sl_dist * tp_mult
        signals.append(
            {
                "signal_time": signal_time,
                "direction": direction,
                "sl": sl,
                "tp": tp,
                "sl_dist": sl_dist,
            }
        )

    results = run_simulation(
        signals, candles, oanda_instrument, timeout, mode="aggressive"
    )
    return _aggregate_sim(results, "aggressive")


def _parse_strategies_evaluated(raw) -> list:
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


def _fetch_placed_signals(oanda_instrument: str, strategy_name: str, anchor_ts) -> list[tuple[pd.Timestamp, str]]:
    out: list[tuple[pd.Timestamp, str]] = []
    inst = (oanda_instrument or "").strip()
    base = (strategy_name or "").strip()
    if not inst or not base:
        return out
    try:
        ts_db = (
            anchor_ts.to_pydatetime()
            if hasattr(anchor_ts, "to_pydatetime")
            else anchor_ts
        )
    except Exception:
        ts_db = anchor_ts
    sql = """
        SELECT DISTINCT ON (candle_time)
            candle_time, strategies_evaluated
        FROM executor_poll_log
        WHERE instrument = %s
          AND strategies_evaluated @> %s::jsonb
          AND candle_time >= %s
        ORDER BY candle_time, id DESC
    """
    placed_json = '[{"placed": true}]'
    try:
        with get_conn() as conn:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(sql, (inst, placed_json, ts_db))
            rows = cur.fetchall()
    except Exception:
        log.exception("_fetch_placed_signals")
        return out
    for row in rows:
        ct = row.get("candle_time")
        if ct is None:
            continue
        st = to_utc_timestamp(ct)
        if st is None:
            continue
        arr = _parse_strategies_evaluated(row.get("strategies_evaluated"))
        for item in arr:
            if not isinstance(item, dict):
                continue
            if item.get("name") != base:
                continue
            if item.get("placed") is not True:
                continue
            d = str(item.get("direction", "")).upper().strip()
            if d not in ("BUY", "SELL"):
                continue
            signal_candle_ts = st - timedelta(minutes=5)
            out.append((signal_candle_ts, d))
            break
    return out


def _build_since_live_simulation(
    strategy_name: str,
    instrument_label: str,
    sl_mult: float,
    tp_mult: float,
    timeout: int,
    anchor_ts,
) -> dict:
    oanda_instrument = _instrument_to_oanda(instrument_label)
    pip = get_pip(oanda_instrument)
    placed = _fetch_placed_signals(oanda_instrument, strategy_name, anchor_ts)
    if not placed:
        return {"aggressive": [], "passive": []}

    candles = _fetch_oanda_candles(oanda_instrument, anchor_ts)
    if candles.empty:
        return {"aggressive": [], "passive": []}

    h1_atr = _get_h1_atr(candles)
    signals: list[dict] = []
    for st, direction in placed:
        try:
            sig_close = float(candles["close"].loc[st])
        except (KeyError, TypeError, ValueError):
            continue
        signal_time = to_utc_timestamp(st)
        if signal_time is None:
            continue
        sl_dist = _lookup_h1_atr(h1_atr, signal_time, pip)
        if direction == "BUY":
            sl = sig_close - sl_dist * sl_mult
            tp = sig_close + sl_dist * tp_mult
        else:
            sl = sig_close + sl_dist * sl_mult
            tp = sig_close - sl_dist * tp_mult
        signals.append(
            {
                "signal_time": signal_time,
                "direction": direction,
                "sl": sl,
                "tp": tp,
                "sl_dist": sl_dist,
            }
        )

    if not signals:
        return {"aggressive": [], "passive": []}

    results_agg = run_simulation(
        signals, candles, oanda_instrument, timeout, mode="aggressive"
    )
    results_pas = run_simulation(
        signals, candles, oanda_instrument, timeout, mode="passive"
    )
    return {
        "aggressive": results_agg,
        "passive": results_pas,
    }


def run_since_live(
    strategy_name: str,
    instrument_label: str,
    sl_mult: float,
    tp_mult: float,
    timeout: int,
    mode: str,
    anchor_ts: pd.Timestamp,
) -> dict:
    m = str(mode).lower()
    if m not in ("aggressive", "passive"):
        m = "aggressive"
    raw = _build_since_live_simulation(
        strategy_name, instrument_label, sl_mult, tp_mult, timeout, anchor_ts
    )
    results = raw.get(m, [])
    return _aggregate_sim(results, m)


def run_since_live_detail(
    strategy_name: str,
    instrument_label: str,
    sl_mult: float,
    tp_mult: float,
    timeout: int,
    anchor_ts,
) -> dict:
    """
    Returns unaggregated simulation results for the See Trades detail view. Dict has keys:
      aggressive: list[dict] — one dict per signal
      passive:    list[dict] — one dict per signal
    Each dict is a run_simulation result with keys:
    signal_time, direction, mode, entry, exit_price,
    sl, tp, pnl_pips, pnl_dollars, result,
    exit_reason, entry_time, exit_time.
    """
    return _build_since_live_simulation(
        strategy_name, instrument_label, sl_mult, tp_mult, timeout, anchor_ts
    )
