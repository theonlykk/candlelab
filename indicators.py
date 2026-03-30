"""
indicators.py — MA and RSI confirmation filters for backtest signals.
All implementations are pure numpy, no scipy, no pandas built-in TA functions.
"""
import numpy as np
import pandas as pd

LOOKBACK = 10


def _sma(arr: np.ndarray, period: int) -> np.ndarray:
    """
    Simple moving average implemented via convolution.

    Returns an array of the same length as `arr` padded with NaNs until `period-1`.
    """
    result = np.full(len(arr), np.nan)
    if len(arr) < period:
        return result
    kernel = np.ones(period) / period
    valid = np.convolve(arr, kernel, mode="valid")
    result[period - 1 :] = valid
    return result


def _rsi(arr: np.ndarray, period: int = 14) -> np.ndarray:
    """
    RSI implementation using Wilder-style exponential smoothing.

    Returns an array of the same length as `arr` padded with NaNs until `period`.
    """
    result = np.full(len(arr), np.nan)
    if len(arr) < period + 1:
        return result
    delta = np.diff(arr)
    gains = np.maximum(delta, 0.0)
    losses = np.maximum(-delta, 0.0)
    avg_gain = float(np.mean(gains[:period]))
    avg_loss = float(np.mean(losses[:period]))
    if avg_loss == 0.0:
        result[period] = 100.0
    else:
        result[period] = 100.0 - 100.0 / (1.0 + avg_gain / avg_loss)
    for i in range(period, len(delta)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        if avg_loss == 0.0:
            result[i + 1] = 100.0
        else:
            result[i + 1] = 100.0 - 100.0 / (1.0 + avg_gain / avg_loss)
    return result


def check_ma_cross(df: pd.DataFrame, signal_idx: int) -> bool:
    """
    Returns True if the 5-period SMA crossed above the 20-period SMA
    anywhere in the LOOKBACK candles before signal_idx.
    """
    close = df["close"].to_numpy(dtype=float)
    end = signal_idx
    start = max(0, end - LOOKBACK)
    if end - start < 2:
        return False

    sma5 = _sma(close[:end], 5)
    sma20 = _sma(close[:end], 20)

    s5 = sma5[start:end]
    s20 = sma20[start:end]

    valid = ~(np.isnan(s5) | np.isnan(s20))
    if np.sum(valid) < 2:
        return False

    s5v = s5[valid]
    s20v = s20[valid]
    cross_up = np.any((s5v[:-1] < s20v[:-1]) & (s5v[1:] >= s20v[1:]))
    return bool(cross_up)


def check_rsi_extreme(df: pd.DataFrame, signal_idx: int, direction: str) -> bool:
    """
    Returns True if RSI(14) was below 30 (direction='long') or above 70
    (direction='short') at any point in the LOOKBACK candles before signal_idx.
    """
    close = df["close"].to_numpy(dtype=float)
    end = signal_idx
    start = max(0, end - LOOKBACK)

    rsi = _rsi(close[:end])
    window = rsi[start:end]
    window = window[~np.isnan(window)]

    if len(window) == 0:
        return False

    if direction == "long":
        return bool(np.any(window < 30.0))
    return bool(np.any(window > 70.0))


def check_ma_stable(df: pd.DataFrame, signal_idx: int, direction: str) -> bool:
    """
    Returns True if both the 5-period and 20-period SMA are sloping
    in the direction of the trade for ALL LOOKBACK candles before signal_idx.
    """
    close = df["close"].to_numpy(dtype=float)
    end = signal_idx
    start = max(0, end - LOOKBACK)
    if end - start < 2:
        return False

    sma5 = _sma(close[:end], 5)
    sma20 = _sma(close[:end], 20)

    s5 = sma5[start:end]
    s20 = sma20[start:end]

    valid = ~(np.isnan(s5) | np.isnan(s20))
    if np.sum(valid) < 2:
        return False

    s5v = s5[valid]
    s20v = s20[valid]

    if direction == "long":
        return bool(np.all(np.diff(s5v) > 0) and np.all(np.diff(s20v) > 0))
    return bool(np.all(np.diff(s5v) < 0) and np.all(np.diff(s20v) < 0))
