import decimal
import json
import logging
import os
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import psycopg2.pool

pool = psycopg2.pool.ThreadedConnectionPool(
    minconn=1,
    maxconn=3,
    dsn=os.environ["DATABASE_URL"],
)


def get_leaderboard() -> pd.DataFrame:
    conn = pool.getconn()
    try:
        query = """
            SELECT instrument, granularity,
                   anchor, anchor2, continuation, continuation2,
                   gap, indicator, direction, combo_type, timeout_bars,
                   oos_sqn100, oos_mean_r, oos_n_trades,
                   n_windows_promoted, meta_status,
                   neighborhood_quality_score, sqn_gap,
                   promoted_window_indices,
                   id
            FROM sweep_leaderboard
            ORDER BY
                CASE meta_status WHEN 'GREEN' THEN 0 ELSE 1 END ASC,
                oos_sqn100 DESC NULLS LAST
        """
        df = pd.read_sql(query, conn)
        for col in df.select_dtypes(include='object').columns:
            df[col] = df[col].apply(
                lambda x: float(x) if isinstance(x, decimal.Decimal) else x
            )
        return df
    finally:
        pool.putconn(conn)


def get_oos_curve(
    instrument: str, granularity: str, anchor: str,
    direction: str, timeout_bars: int
) -> pd.DataFrame:
    """
    Fetch oos_r_list directly from sweep_leaderboard.
    No join needed — leaderboard already contains the pooled OOS returns.
    Returns single-row DataFrame with oos_r_list column.
    """
    conn = pool.getconn()
    try:
        query = """
            SELECT oos_r_list
            FROM sweep_leaderboard
            WHERE instrument   = %s
              AND granularity   = %s
              AND anchor        = %s
              AND direction     = %s
              AND timeout_bars  = %s
            LIMIT 1
        """
        df = pd.read_sql(
            query, conn,
            params=(instrument, granularity, anchor,
                    direction, timeout_bars)
        )
        return df
    finally:
        pool.putconn(conn)


def _parse_oos_r_list(value) -> list[float] | None:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (json.JSONDecodeError, TypeError, ValueError):
            return None
    if not isinstance(value, (list, tuple, np.ndarray)):
        return None
    try:
        return [float(r) for r in value]
    except (TypeError, ValueError):
        return None


def build_equity_curve(oos_df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    """
    Build equity curve from pooled oos_r_list in sweep_leaderboard.
    oos_r_list is a flat list of R-multiples across all promoted windows.
    """
    if oos_df.empty:
        return np.array([]), np.array([])

    raw = oos_df["oos_r_list"].iloc[0]
    returns = _parse_oos_r_list(raw)

    if not returns:
        return np.array([]), np.array([])

    y = np.cumsum(returns)
    x = np.arange(len(returns))
    return x, y


def get_candles_batch(instruments: list[str]) -> dict:
    """
    Fetch M30, H4, D1 candles for a list of instruments in one
    DB round trip per timeframe. Returns a nested dict:
        { instrument: { 'M30': df, 'H4': df, 'D1': df } }

    N+1 fix (Gemini ADR-104 ruling): fetch once per unique instrument,
    not once per candidate. Multiple GREEN candidates on the same
    instrument reuse the same DataFrames.
    """
    if not instruments:
        return {}

    result = {inst: {"M30": None, "H4": None, "D1": None}
              for inst in instruments}

    conn = pool.getconn()
    try:
        for granularity, limit in [("M30", 300), ("H4", 100), ("D1", 210)]:
            query = """
                SELECT instrument, time, open, high, low, close
                FROM oanda_candles_x
                WHERE instrument = ANY(%s)
                  AND granularity = %s
                ORDER BY instrument, time ASC
            """
            df = pd.read_sql(query, conn,
                             params=(instruments, granularity))
            if df.empty:
                continue
            for inst in instruments:
                inst_df = df[df["instrument"] == inst].copy()
                if not inst_df.empty:
                    result[inst][granularity] = (
                        inst_df.tail(limit).reset_index(drop=True)
                    )
        return result
    except Exception as e:
        logging.getLogger("dashboard.data").error(
            f"get_candles_batch failed: {e}"
        )
        return result
    finally:
        pool.putconn(conn)


def compute_regime_gates(
    candles: dict,
    direction: str,
    combo_type: str,
) -> dict:
    """
    Compute live regime gate pass/fail from pre-fetched candle DataFrames.

    Args:
        candles: dict with keys 'M30', 'H4', 'D1' — each a DataFrame
                 or None if not available.
        direction: 'long' or 'short'
        combo_type: 'Counter-Trend', 'Pro-Trend', or 'Hybrid'

    Returns dict:
        bbw      -> 'PASS' | 'FAIL' | 'STALE'
        adx      -> 'PASS' | 'FAIL' | 'STALE'
        ma_200   -> 'PASS' | 'FAIL' | 'STALE'
        h4_ema   -> 'PASS' | 'FAIL' | 'STALE'
        z_spread -> 'PENDING' (requires Market State Daemon, ADR-105)
        is_stale -> bool
        data_ts  -> latest M30 bar timestamp or None

    IMPORTANT — Threshold note (Gemini ADR-104 ruling):
    Gates evaluate against static SAFE heuristics until
    sweep_window_thresholds table is built (ADR-105 / V9).
    These are Wilder convention defaults, not IS-calibrated values.
    """
    import numpy as np

    STALE_HOURS   = 2
    BBW_PERIOD    = 20
    ADX_PERIOD    = 14
    MA_PERIOD     = 200
    H4_EMA_PERIOD = 50

    # SAFE thresholds — Wilder convention defaults
    # Will be replaced by IS-calibrated values in ADR-105
    BBW_DEAD_ZONE = 0.00045
    ADX_TRENDING  = 25.0
    ADX_BUILDING  = 20.0
    ADX_BLOWOFF   = 70.0

    now_utc = datetime.now(timezone.utc)
    result  = {
        "bbw":      "STALE",
        "adx":      "STALE",
        "ma_200":   "STALE",
        "h4_ema":   "STALE",
        "z_spread": "PENDING",
        "is_stale": True,
        "data_ts":  None,
    }

    # ── M30 gates: BBW + ADX ─────────────────────────────────────────
    m30 = candles.get("M30")
    if m30 is None or m30.empty:
        return result

    latest_ts = pd.to_datetime(m30["time"].iloc[-1], utc=True)
    result["data_ts"] = latest_ts

    age_hours = (now_utc - latest_ts).total_seconds() / 3600
    if age_hours > STALE_HOURS:
        result["is_stale"] = True
        return result

    result["is_stale"] = False

    # BBW
    rolling = m30["close"].rolling(window=BBW_PERIOD)
    sma     = rolling.mean()
    std     = rolling.std()
    bbw_arr = np.where(sma != 0, (4.0 * std) / sma, np.nan)
    bbw_val = float(bbw_arr[-1]) if len(bbw_arr) > 0 else np.nan
    if np.isfinite(bbw_val):
        result["bbw"] = "PASS" if bbw_val >= BBW_DEAD_ZONE else "FAIL"

    # ADX
    high  = m30["high"]
    low   = m30["low"]
    close = m30["close"]
    tr = pd.concat([
        high - low,
        (high - close.shift(1)).abs(),
        (low  - close.shift(1)).abs(),
    ], axis=1).max(axis=1)
    up       = high.diff()
    down     = low.shift(1) - low
    plus_dm  = np.where((up > down)   & (up > 0),   up,   0.0)
    minus_dm = np.where((down > up)   & (down > 0), down, 0.0)
    alpha    = 1.0 / ADX_PERIOD
    atr_s    = tr.ewm(alpha=alpha, adjust=False).mean()
    plus_di  = (
        100 * pd.Series(plus_dm, index=m30.index)
        .ewm(alpha=alpha, adjust=False).mean() / atr_s
    )
    minus_di = (
        100 * pd.Series(minus_dm, index=m30.index)
        .ewm(alpha=alpha, adjust=False).mean() / atr_s
    )
    dx      = (
        100 * (plus_di - minus_di).abs()
        / (plus_di + minus_di).replace(0, np.nan)
    )
    adx_val = float(dx.ewm(alpha=alpha, adjust=False).mean().iloc[-1])

    if np.isfinite(adx_val):
        if combo_type == "Counter-Trend":
            result["adx"] = (
                "PASS" if adx_val < ADX_TRENDING else "FAIL"
            )
        elif combo_type == "Pro-Trend":
            result["adx"] = (
                "PASS"
                if ADX_BUILDING <= adx_val < ADX_BLOWOFF
                else "FAIL"
            )
        else:
            result["adx"] = "PASS"  # Hybrid — no ADX gate

    # ── D1 MA_200 ────────────────────────────────────────────────────
    d1 = candles.get("D1")
    if d1 is not None and not d1.empty and len(d1) >= MA_PERIOD:
        ma_200     = d1["close"].rolling(MA_PERIOD).mean().iloc[-1]
        last_close = d1["close"].iloc[-1]
        if direction == "long":
            result["ma_200"] = (
                "PASS" if last_close > ma_200 else "FAIL"
            )
        else:
            result["ma_200"] = (
                "PASS" if last_close < ma_200 else "FAIL"
            )

    # ── H4 EMA50 ─────────────────────────────────────────────────────
    h4 = candles.get("H4")
    if h4 is not None and not h4.empty:
        ema50   = h4["close"].ewm(
            span=H4_EMA_PERIOD, adjust=False).mean().iloc[-1]
        last_h4 = h4["close"].iloc[-1]
        if direction == "long":
            result["h4_ema"] = "PASS" if last_h4 > ema50 else "FAIL"
        else:
            result["h4_ema"] = "PASS" if last_h4 < ema50 else "FAIL"

    # z_spread always PENDING until Market State Daemon (ADR-105)
    result["z_spread"] = "PENDING"

    return result
