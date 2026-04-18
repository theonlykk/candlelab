"""
patterns.py — Candlestick pattern detection for EUR/USD 5min backtest
Each detector returns a Series of signals: +1 (bullish), -1 (bearish), 0 (none)
"""

import numpy as np
import pandas as pd


def _body(df):    return (df["close"] - df["open"]).abs()
def _upper(df):   return df["high"] - df[["open","close"]].max(axis=1)
def _lower(df):   return df[["open","close"]].min(axis=1) - df["low"]
def _range(df):   return df["high"] - df["low"]
def _bull(df):    return df["close"] > df["open"]
def _bear(df):    return df["close"] < df["open"]

# The helper functions above deliberately operate on the full DataFrame and return Series.
# This keeps pattern definitions concise and makes it easy to add new patterns by composing
# the same building blocks.


# ── 1. Doji ───────────────────────────────────────────────────────────────────
def doji(df: pd.DataFrame) -> pd.Series:
    """
    Body ≤ 10% of range. Direction inferred from prior trend (last 3 closes).
    """
    b = _body(df)
    r = _range(df)
    is_doji = (r > 0) & (b / r < 0.10)

    prior_trend = df["close"].diff(3)
    sig = pd.Series(0, index=df.index)
    sig[is_doji & (prior_trend < 0)] =  1   # bullish reversal after downtrend
    sig[is_doji & (prior_trend > 0)] = -1   # bearish reversal after uptrend
    return sig


# ── 2. Hammer / Hanging Man ───────────────────────────────────────────────────
def hammer_hanging_man(df: pd.DataFrame) -> pd.Series:
    """
    Long lower shadow (≥2× body), small upper shadow, small body.
    Hammer = bullish (after downtrend), Hanging Man = bearish (after uptrend).
    """
    b  = _body(df)
    lo = _lower(df)
    up = _upper(df)
    r  = _range(df)

    is_pattern = (r > 0) & (lo >= 2 * b) & (up <= 0.3 * r) & (b > 0)
    prior_trend = df["close"].diff(3)

    sig = pd.Series(0, index=df.index)
    sig[is_pattern & (prior_trend < 0)] =  1  # hammer
    sig[is_pattern & (prior_trend > 0)] = -1  # hanging man
    return sig


# ── 3. Shooting Star / Inverted Hammer ───────────────────────────────────────
def shooting_star_inverted_hammer(df: pd.DataFrame) -> pd.Series:
    """
    Long upper shadow (≥2× body), small lower shadow.
    Shooting Star = bearish (after uptrend), Inv. Hammer = bullish (after downtrend).
    """
    b  = _body(df)
    lo = _lower(df)
    up = _upper(df)
    r  = _range(df)

    is_pattern = (r > 0) & (up >= 2 * b) & (lo <= 0.3 * r) & (b > 0)
    prior_trend = df["close"].diff(3)

    sig = pd.Series(0, index=df.index)
    sig[is_pattern & (prior_trend > 0)] = -1  # shooting star
    sig[is_pattern & (prior_trend < 0)] =  1  # inverted hammer
    return sig


# ── 4. Engulfing ──────────────────────────────────────────────────────────────
def engulfing(df: pd.DataFrame) -> pd.Series:
    sig  = pd.Series(0, index=df.index)
    o, c = df["open"], df["close"]
    po   = o.shift(1)
    pc   = c.shift(1)

    bull = _bear(df.shift(1)) & _bull(df) & (c > po) & (o < pc)
    bear = _bull(df.shift(1)) & _bear(df) & (c < po) & (o > pc)

    sig[bull] =  1
    sig[bear] = -1
    return sig


# ── 5. Morning / Evening Star ─────────────────────────────────────────────────
def morning_evening_star(df: pd.DataFrame) -> pd.Series:
    sig = pd.Series(0, index=df.index)
    o, c = df["open"], df["close"]

    # Candle -2 (large), candle -1 (small body = star), candle 0 (large reversal)
    b0  = _body(df)
    b1  = _body(df.shift(1))
    b2  = _body(df.shift(2))

    star = b1 < 0.3 * b2   # middle candle has small body

    # Morning star: candle-2 bearish, candle-1 gaps lower, candle-0 bullish & closes into candle-2 body
    morning = (
        star &
        _bear(df.shift(2)) &
        _bull(df) &
        (c > (df["open"].shift(2) + df["close"].shift(2)) / 2)
    )
    # Evening star: candle-2 bullish, candle-0 bearish & closes into candle-2 body
    evening = (
        star &
        _bull(df.shift(2)) &
        _bear(df) &
        (c < (df["open"].shift(2) + df["close"].shift(2)) / 2)
    )

    sig[morning] =  1
    sig[evening] = -1
    return sig


# ── 6. Harami ─────────────────────────────────────────────────────────────────
def harami(df: pd.DataFrame) -> pd.Series:
    sig = pd.Series(0, index=df.index)
    o, c   = df["open"], df["close"]
    po, pc = o.shift(1), c.shift(1)

    # Current candle body contained within prior candle body
    body_max  = df[["open","close"]].max(axis=1)
    body_min  = df[["open","close"]].min(axis=1)
    prior_max = df[["open","close"]].shift(1).max(axis=1)
    prior_min = df[["open","close"]].shift(1).min(axis=1)

    contained = (body_max < prior_max) & (body_min > prior_min)

    bull = contained & _bear(df.shift(1)) & _bull(df)
    bear = contained & _bull(df.shift(1)) & _bear(df)

    sig[bull] =  1
    sig[bear] = -1
    return sig


# ── 7. Piercing Line / Dark Cloud Cover ──────────────────────────────────────
def piercing_dark_cloud(df: pd.DataFrame) -> pd.Series:
    sig = pd.Series(0, index=df.index)
    o, c   = df["open"], df["close"]
    po, pc = o.shift(1), c.shift(1)
    mid1   = (po + pc) / 2

    # Piercing: prev bearish, curr opens below prev low, closes above midpoint of prev body
    piercing = (
        _bear(df.shift(1)) &
        _bull(df) &
        (o < df["low"].shift(1)) &
        (c > mid1) & (c < po)
    )
    # Dark cloud: prev bullish, curr opens above prev high, closes below midpoint
    dark_cloud = (
        _bull(df.shift(1)) &
        _bear(df) &
        (o > df["high"].shift(1)) &
        (c < mid1) & (c > po)
    )

    sig[piercing]  =  1
    sig[dark_cloud] = -1
    return sig


# ── 8. Three White Soldiers / Three Black Crows ───────────────────────────────
def three_soldiers_crows(df: pd.DataFrame) -> pd.Series:
    sig = pd.Series(0, index=df.index)
    o, c = df["open"], df["close"]

    # Three consecutive bullish candles, each opening within prior body, closing higher
    soldiers = (
        _bull(df) & _bull(df.shift(1)) & _bull(df.shift(2)) &
        (o > df["open"].shift(1)) & (o < df["close"].shift(1)) &
        (c > df["close"].shift(1)) &
        (df["open"].shift(1) > df["open"].shift(2)) &
        (df["close"].shift(1) > df["close"].shift(2))
    )
    crows = (
        _bear(df) & _bear(df.shift(1)) & _bear(df.shift(2)) &
        (o < df["open"].shift(1)) & (o > df["close"].shift(1)) &
        (c < df["close"].shift(1)) &
        (df["open"].shift(1) < df["open"].shift(2)) &
        (df["close"].shift(1) < df["close"].shift(2))
    )

    sig[soldiers] =  1
    sig[crows]    = -1
    return sig


# ── 9. Spinning Top ──────────────────────────────────────────────────────────
def spinning_top(df: pd.DataFrame) -> pd.Series:
    """
    Small body (< 30% of range), both shadows >= 1× body.
    Direction from prior trend (last 3 closes).
    """
    b  = _body(df)
    up = _upper(df)
    lo = _lower(df)
    r  = _range(df)

    is_pattern = (r > 0) & (b / r.clip(1e-10) < 0.30) & (up >= b) & (lo >= b) & (b > 0)
    prior_trend = df["close"].diff(3)

    sig = pd.Series(0, index=df.index)
    sig[is_pattern & (prior_trend < 0)] =  1
    sig[is_pattern & (prior_trend > 0)] = -1
    return sig


# ── 10. Long-legged Doji ──────────────────────────────────────────────────────
def long_legged_doji(df: pd.DataFrame) -> pd.Series:
    """
    Body <= 5% of range (stricter doji), both upper and lower shadows >= 35% of range.
    Direction from prior trend.
    """
    b  = _body(df)
    up = _upper(df)
    lo = _lower(df)
    r  = _range(df)

    is_pattern = (r > 0) & (b / r.clip(1e-10) <= 0.05) & (up / r.clip(1e-10) >= 0.35) & (lo / r.clip(1e-10) >= 0.35)
    prior_trend = df["close"].diff(3)

    sig = pd.Series(0, index=df.index)
    sig[is_pattern & (prior_trend < 0)] =  1
    sig[is_pattern & (prior_trend > 0)] = -1
    return sig


# ── 11. Rising / Falling Three Methods ───────────────────────────────────────
def rising_falling_three_methods(df: pd.DataFrame) -> pd.Series:
    """
    Rising: C1 large bullish, C2-C4 small bearish contained in C1 body, C5 large bullish > C1 close.
    Falling: mirror.
    """
    sig = pd.Series(0, index=df.index)

    c1_body = _body(df.shift(4))
    c1_bhi  = df[["open", "close"]].shift(4).max(axis=1)
    c1_blo  = df[["open", "close"]].shift(4).min(axis=1)
    c1_r    = _range(df.shift(4))
    c1_large = (c1_body / c1_r.clip(1e-10) > 0.5)
    c5_body = _body(df)
    c5_r    = _range(df)
    c5_large = (c5_body / c5_r.clip(1e-10) > 0.5)

    def _contained(shift_n):
        bhi = df[["open", "close"]].shift(shift_n).max(axis=1)
        blo = df[["open", "close"]].shift(shift_n).min(axis=1)
        small = (_body(df.shift(shift_n)) < 0.7 * c1_body)
        return small & (bhi < c1_bhi) & (blo > c1_blo)

    rising = (
        c1_large & _bull(df.shift(4)) &
        _contained(3) & _bear(df.shift(3)) &
        _contained(2) & _bear(df.shift(2)) &
        _contained(1) & _bear(df.shift(1)) &
        c5_large & _bull(df) & (df["close"] > c1_bhi)
    )
    falling = (
        c1_large & _bear(df.shift(4)) &
        _contained(3) & _bull(df.shift(3)) &
        _contained(2) & _bull(df.shift(2)) &
        _contained(1) & _bull(df.shift(1)) &
        c5_large & _bear(df) & (df["close"] < c1_blo)
    )

    sig[rising]  =  1
    sig[falling] = -1
    return sig


# ── 12. Upside / Downside Tasuki Gap ─────────────────────────────────────────
def upside_downside_tasuki_gap(df: pd.DataFrame) -> pd.Series:
    """
    Upside: C1 large bullish, C2 bullish gap up (open > C1 high), C3 bearish opening in C2 body closing in the gap.
    Downside: mirror.
    """
    sig = pd.Series(0, index=df.index)

    c1_hi  = df["high"].shift(2)
    c1_lo  = df["low"].shift(2)
    c2_o   = df["open"].shift(1)
    c2_c   = df["close"].shift(1)
    c2_bhi = df[["open", "close"]].shift(1).max(axis=1)
    c2_blo = df[["open", "close"]].shift(1).min(axis=1)
    c3_o   = df["open"]
    c3_c   = df["close"]

    upside = (
        _bull(df.shift(2)) &
        _bull(df.shift(1)) &
        (c2_o > c1_hi) &                          # gap up
        _bear(df) &
        (c3_o < c2_bhi) & (c3_o > c2_blo) &      # C3 opens in C2 body
        (c3_c > c1_hi) & (c3_c < c2_o)            # C3 closes in the gap
    )
    downside = (
        _bear(df.shift(2)) &
        _bear(df.shift(1)) &
        (c2_o < c1_lo) &                          # gap down
        _bull(df) &
        (c3_o > c2_blo) & (c3_o < c2_bhi) &      # C3 opens in C2 body
        (c3_c < c1_lo) & (c3_c > c2_o)            # C3 closes in the gap
    )

    sig[upside]   =  1
    sig[downside] = -1
    return sig


# ── Registry ──────────────────────────────────────────────────────────────────
PATTERNS = {
    "Hammer/Hanging Man": hammer_hanging_man,
    "Shooting Star/Inv. Hammer": shooting_star_inverted_hammer,
    "Engulfing": engulfing,
    "Morning/Evening Star": morning_evening_star,
    "Three Soldiers/Crows": three_soldiers_crows,
}

# Display labels for server-side chart annotations (`chart_renderer`).
PATTERN_LABELS = {name: name for name in PATTERNS}


def detect_all(df: pd.DataFrame) -> pd.DataFrame:
    """Return DataFrame with one column per pattern: values in {-1, 0, +1}."""
    return pd.DataFrame({name: fn(df) for name, fn in PATTERNS.items()})
