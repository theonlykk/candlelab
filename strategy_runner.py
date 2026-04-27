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
from indicator_utils import _parse_indicator_filter_config, passes_indicator
from patterns import detect_all
from signal_engine import detect_signal
from simulation_engine import get_pip, run_simulation

log = logging.getLogger(__name__)

WARMUP_BARS = 200
COMPLEMENT_WINDOW = 10


def _compute_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    hi, lo, cl = df["high"], df["low"], df["close"]
    tr = pd.concat(
        [hi - lo, (hi - cl.shift(1)).abs(), (lo - cl.shift(1)).abs()],
        axis=1,
    ).max(axis=1)
    return tr.ewm(alpha=1.0 / period, adjust=False).mean()


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
    if df_m5 is None or df_m5.empty:
        return pd.Series(dtype=float)
    need = ["open", "high", "low", "close"]
    for c in need:
        if c not in df_m5.columns:
            return pd.Series(dtype=float)
    df_h1 = (
        df_m5[need]
        .resample("1h")
        .agg({"open": "first", "high": "max", "low": "min", "close": "last"})
        .dropna()
    )
    if df_h1.empty:
        return pd.Series(dtype=float)
    return _compute_atr(df_h1, 14)


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
    st = pd.Timestamp(signal_time)
    if st.tzinfo is None:
        st = st.tz_localize("UTC")
    else:
        st = st.tz_convert("UTC")
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
    h = int(pd.Timestamp(ts).tz_convert("UTC").hour)
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
    cum_net = sum(float(r["pnl_pips"]) for r in executed if r.get("pnl_pips") is not None)
    return {
        "signals": count,
        "wins": wins,
        "win_pct": round(wins / count * 100, 1),
        "cum_net": round(cum_net, 1),
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
) -> dict:
    oanda_instrument = _instrument_to_oanda(instrument_label)
    pip = get_pip(oanda_instrument)
    now = datetime.now(timezone.utc)
    from_ts = now - timedelta(days=30) - timedelta(minutes=WARMUP_BARS * 5)
    strict_cutoff = pd.Timestamp(now - timedelta(days=30), tz="UTC")

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

    sig_array = detect_signal(
        signals_df,
        anchor_col,
        comp_col,
        connector,
        "both",
        window=COMPLEMENT_WINDOW,
    )
    ind_cfg = _parse_indicator_filter_config(indicator_filter)

    signals: list[dict] = []
    n = len(sig_array)
    for i in range(n):
        if int(sig_array[i]) == 0:
            continue
        tsi = candles.index[i]
        if tsi.tzinfo is None:
            tsi = pd.Timestamp(tsi).tz_localize("UTC")
        else:
            tsi = pd.Timestamp(tsi).tz_convert("UTC")
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
            {"signal_time": signal_time, "direction": direction, "sl": sl, "tp": tp}
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
        st = pd.Timestamp(ct)
        if st.tzinfo is None:
            st = st.tz_localize("UTC")
        else:
            st = st.tz_convert("UTC")
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
            out.append((st, d))
            break
    return out


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
    oanda_instrument = _instrument_to_oanda(instrument_label)
    pip = get_pip(oanda_instrument)
    placed = _fetch_placed_signals(oanda_instrument, strategy_name, anchor_ts)
    if not placed:
        return _empty_agg(m)

    candles = _fetch_oanda_candles(oanda_instrument, anchor_ts)
    if candles.empty:
        return _empty_agg(m)

    h1_atr = _get_h1_atr(candles)
    signals: list[dict] = []
    for st, direction in placed:
        try:
            sig_close = float(candles["close"].loc[st])
        except (KeyError, TypeError, ValueError):
            continue
        signal_time = pd.Timestamp(st)
        if signal_time.tzinfo is None:
            signal_time = signal_time.tz_localize("UTC")
        else:
            signal_time = signal_time.tz_convert("UTC")
        sl_dist = _lookup_h1_atr(h1_atr, signal_time, pip)
        if direction == "BUY":
            sl = sig_close - sl_dist * sl_mult
            tp = sig_close + sl_dist * tp_mult
        else:
            sl = sig_close + sl_dist * sl_mult
            tp = sig_close - sl_dist * tp_mult
        signals.append(
            {"signal_time": signal_time, "direction": direction, "sl": sl, "tp": tp}
        )

    if not signals:
        return _empty_agg(m)

    results = run_simulation(
        signals, candles, oanda_instrument, timeout, mode=m
    )
    return _aggregate_sim(results, m)
