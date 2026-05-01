# ==============================================================================
# Thin wrapper over candlelab-core: shared indicator filter + ATR helpers.
# Core logic: candlelab_core.indicator_utils / candlelab_core.indicators
# Last updated: Phase 0.1b — consume candlelab-core package.
# ==============================================================================

import pandas as pd

from candlelab_core.indicator_utils import _parse_indicator_filter_config, passes_indicator
from candlelab_core.indicators import check_ma_cross_direction, check_rsi_extreme, check_ma_stable


def compute_h1_atr_from_m5(df_m5: pd.DataFrame | None, period: int = 14) -> float:
    """
    Resample M5 OHLC to H1, drop the last (incomplete) H1 bar, then Wilder-style ATR(period)
    via EWM with alpha=1/period (adjust=False). Returns 0.0 when data is insufficient or invalid.
    """
    if df_m5 is None or df_m5.empty:
        return 0.0
    df_m5 = df_m5.copy()
    df_m5 = df_m5.sort_index()
    if not isinstance(df_m5.index, pd.DatetimeIndex):
        try:
            df_m5.index = pd.to_datetime(df_m5.index, utc=True)
        except (TypeError, ValueError, pd.errors.OutOfBoundsDatetime):
            return 0.0
    if not isinstance(df_m5.index, pd.DatetimeIndex):
        return 0.0
    df_h1 = df_m5.resample("1h").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last"}
    )
    df_h1 = df_h1.dropna()
    if len(df_h1) < 2:
        return 0.0
    df_h1 = df_h1.iloc[:-1]
    if len(df_h1) < period:
        return 0.0
    hi = df_h1["high"]
    lo = df_h1["low"]
    cl = df_h1["close"]
    prev_cl = cl.shift(1)
    tr = pd.concat(
        [hi - lo, (hi - prev_cl).abs(), (lo - prev_cl).abs()],
        axis=1,
    ).max(axis=1)
    atr = tr.ewm(alpha=1.0 / period, adjust=False).mean()
    atr_clean = atr.dropna()
    if atr_clean.empty:
        return 0.0
    atr_val = float(atr_clean.iloc[-1])
    if pd.isna(atr_val) or atr_val <= 0:
        return 0.0
    return atr_val


def compute_h1_atr_series_from_m5(
    df_m5: pd.DataFrame,
    period: int = 14,
) -> pd.Series:
    """
    Returns the full hourly-indexed Wilder RMA ATR(14) Series from an M5 DataFrame.
    Always drops the last incomplete H1 bar before computing. Use
    compute_h1_atr_from_m5() when only the latest scalar value is needed.
    """
    if df_m5 is None or df_m5.empty:
        return pd.Series(dtype=float)
    df_m5 = df_m5.copy()
    df_m5 = df_m5.sort_index()
    if not isinstance(df_m5.index, pd.DatetimeIndex):
        try:
            df_m5.index = pd.to_datetime(df_m5.index, utc=True)
        except (TypeError, ValueError, pd.errors.OutOfBoundsDatetime):
            return pd.Series(dtype=float)
    if not isinstance(df_m5.index, pd.DatetimeIndex):
        return pd.Series(dtype=float)
    df_h1 = df_m5.resample("1h").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last"}
    )
    df_h1 = df_h1.dropna()
    if len(df_h1) < 2:
        return pd.Series(dtype=float)
    df_h1 = df_h1.iloc[:-1]
    if len(df_h1) < period:
        return pd.Series(dtype=float)
    hi = df_h1["high"]
    lo = df_h1["low"]
    cl = df_h1["close"]
    prev_cl = cl.shift(1)
    tr = pd.concat(
        [hi - lo, (hi - prev_cl).abs(), (lo - prev_cl).abs()],
        axis=1,
    ).max(axis=1)
    atr = tr.ewm(alpha=1.0 / period, adjust=False).mean()
    return atr
