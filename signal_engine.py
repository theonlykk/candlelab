"""
signal_engine.py — Canonical signal detection engine (candlelab / oanda-trading).

Ring-buffer pairing of two pattern series with pass ordering by PATTERN_IDS.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

SPREAD_COST_PIPS = {
    "GBP_USD": 1.2,
    "USD_CAD": 1.1,
    "USD_JPY": 1.0,
    "USD_CHF": 1.0,
    "NZD_USD": 1.0,
    "EUR_USD": 0.9,
    "AUD_USD": 0.8,
}

# Signal detection window constants (candle counts, M5 default)
WINDOW_REVERSAL_PAIR = 10      # any-order search for two reversal patterns
WINDOW_ORDERED = 10            # ordered search: reversal → continuation
WINDOW_CONTINUATION_TAIL = 5  # continuation window after a confirmed reversal pair

PATTERN_IDS: dict[str, int] = {
    "Hammer/Hanging Man": 137,
    "Shooting Star/Inv. Hammer": 203,
    "Engulfing": 259,
    "Morning/Evening Star": 314,
    "Three Soldiers/Crows": 463,
}

PATTERN_TYPE: dict[str, str] = {
    "Hammer/Hanging Man":       "reversal",
    "Shooting Star/Inv. Hammer": "reversal",
    "Engulfing":                "reversal",
    "Morning/Evening Star":     "reversal",
    "Three Soldiers/Crows":     "continuation",
}

PATTERN_DIRECTION: dict[str, tuple[str, str]] = {
    "Hammer/Hanging Man":        ("long", "short"),
    "Shooting Star/Inv. Hammer": ("long", "short"),
    "Engulfing":                 ("long", "short"),
    "Morning/Evening Star":      ("long", "short"),
    "Three Soldiers/Crows":      ("long", "short"),
}


def get_pattern_direction(pattern: str, signal_value: int) -> str:
    """
    Given a pattern name and signal value (+1 bullish, -1 bearish),
    return the implied trade direction: 'long' or 'short'.
    Returns 'long' for unknown patterns with positive signal.
    """
    entry = PATTERN_DIRECTION.get(pattern)
    if entry is None:
        return "long" if signal_value > 0 else "short"
    return entry[0] if signal_value > 0 else entry[1]


def get_pattern_type(pattern: str) -> str:
    """
    Return 'reversal', 'continuation', or 'unknown' for a pattern name.
    """
    return PATTERN_TYPE.get(pattern, "unknown")


def _pattern_slug(label: str) -> str:
    """
    Stable slug for pattern labels — same rules as ``poll_log._pattern_slug``:
    lowercase, ``/`` → ``_``, space → ``_``, ``-`` → ``_``, collapse ``__``, strip ``_``.
    """
    raw = str(label or "").strip()
    if not raw:
        return ""
    s = (
        raw.lower()
        .replace(".", "")
        .replace("/", "_")
        .replace(" ", "_")
        .replace("-", "_")
    )
    while "__" in s:
        s = s.replace("__", "_")
    return s.strip("_")


PATTERN_SLUG_TO_NAME: dict[str, str] = {
    _pattern_slug(name): name for name in PATTERN_IDS
}


def get_pass_order(anchor: str, complement: str) -> tuple[str, str]:
    """Return (first_pattern, second_pattern) sorted by PATTERN_IDS ascending."""
    id_a = PATTERN_IDS[anchor]
    id_b = PATTERN_IDS[complement]
    if id_a <= id_b:
        return anchor, complement
    return complement, anchor


def _connector_is_any_order(connector: str | None) -> bool:
    """True when Pass 2 should run (reverse pairing); ordered uses Pass 1 only."""
    if connector is None or str(connector).strip() == "":
        return False
    c = str(connector).strip().lower().replace("_", "-")
    return c in ("any-order", "optional")


def build_signal_arrays(
    signals_df: pd.DataFrame,
    anchor: str,
    complement: str | None,
) -> tuple[np.ndarray, np.ndarray | None]:
    """
    Extract anchor and complement columns as int8 numpy arrays.
    complement_arr is None if complement is None or missing from signals_df.
    """
    anchor_arr = signals_df[anchor].to_numpy(dtype=np.int8, copy=True)
    if complement is None:
        return anchor_arr, None
    if complement not in signals_df.columns:
        return anchor_arr, None
    complement_arr = signals_df[complement].to_numpy(dtype=np.int8, copy=True)
    return anchor_arr, complement_arr


def run_ring_buffer(
    anchor_arr: np.ndarray,
    complement_arr: np.ndarray | None,
    connector: str | None,
    window: int = 10,
) -> np.ndarray:
    """
    Core engine: int8 signal array aligned to anchor_arr.

    When complement_arr is None, fire wherever anchor_arr is non-zero.

    When complement_arr is present, caller must pass arrays in pass-1 order:
    anchor_arr = lower PATTERN_ID series, complement_arr = higher PATTERN_ID series.
    (detect_signal reorders via get_pass_order before calling.)

    connector \"ordered\": complement bar at m pairs only with the other pattern at
    an earlier bar t < m (backward ring scan). Pass 1 only.

    connector \"any-order\" / \"optional\": Pass 1 + Pass 2 (roles reversed). Both
    patterns must already appear in history before the signal bar; no sequence
    requirement between pattern IDs beyond what each pass encodes.
    """
    n = anchor_arr.shape[0]
    out = np.zeros(n, dtype=np.int8)
    if complement_arr is None:
        i = 0
        while i < n:
            v = anchor_arr[i]
            if v != 0:
                out[i] = v
            i += 1
        return out

    w_first = anchor_arr.astype(np.int8, copy=True)
    w_second = complement_arr.astype(np.int8, copy=True)

    any_order = _connector_is_any_order(connector)

    buf = np.zeros(window, dtype=np.int8)
    last_t = np.full(window, -1, dtype=np.int32)
    pass1_sig = np.zeros(n, dtype=np.int8)

    m = 0
    while m < n:
        idx = m % window
        buf[idx] = w_first[m]
        last_t[idx] = m

        sv = w_second[m]
        if sv != 0:
            paired = False
            k = 1
            while k <= window:
                t = m - k
                if t < 0:
                    break
                j = t % window
                if last_t[j] == t:
                    bv = buf[j]
                    if bv != 0:
                        pass1_sig[m] = sv
                        buf[j] = 0
                        last_t[j] = -1
                        w_first[t] = 0
                        w_second[m] = 0
                        paired = True
                        break
                k += 1
        m += 1

    pass2_sig = np.zeros(n, dtype=np.int8)
    if any_order:
        buf2 = np.zeros(window, dtype=np.int8)
        last_t2 = np.full(window, -1, dtype=np.int32)

        m = 0
        while m < n:
            idx = m % window
            buf2[idx] = w_second[m]
            last_t2[idx] = m

            fv = w_first[m]
            p1 = pass1_sig[m]
            if fv != 0 and p1 == 0:
                paired = False
                k = 1
                while k <= window:
                    t = m - k
                    if t < 0:
                        break
                    j = t % window
                    if last_t2[j] == t:
                        bv = buf2[j]
                        if bv != 0:
                            pass2_sig[m] = fv
                            buf2[j] = 0
                            last_t2[j] = -1
                            w_second[t] = 0
                            w_first[m] = 0
                            paired = True
                            break
                    k += 1
            m += 1

    i = 0
    while i < n:
        a = pass1_sig[i]
        if a != 0:
            out[i] = a
        else:
            b = pass2_sig[i]
            if b != 0:
                out[i] = b
        i += 1

    return out


def run_ring_buffer_type4(
    pattern1_arr: np.ndarray,
    pattern2_arr: np.ndarray,
    continuation_arr: np.ndarray,
    window_pair: int = WINDOW_REVERSAL_PAIR,
    window_tail: int = WINDOW_CONTINUATION_TAIL,
) -> np.ndarray:
    """
    Type 4 signal: two reversals (any-order) within window_pair candles,
    then continuation within window_tail candles AFTER the pair completes.

    Stage 1: run any-order ring buffer on pattern1/pattern2 to find pair
             completion bars and their direction (+1/-1).
    Stage 2: forward scan — continuation must appear strictly after the
             completion bar (pv == 0 guard), within window_tail candles.
             Continuation sign must match pair direction exactly.
             Most recent pair completion takes precedence (overwrites window).
             Once continuation fires, window resets — no double-firing.

    Cython-friendly: no int() casts inside loop, no list comprehensions,
    no dicts, explicit while loops only. np.int8 values compared directly.
    """
    n = pattern1_arr.shape[0]
    out = np.zeros(n, dtype=np.int8)

    # Stage 1: any-order reversal pair completions
    pair_completions = run_ring_buffer(
        pattern1_arr,
        pattern2_arr,
        connector="any-order",
        window=window_pair,
    )

    # Stage 2: continuation tail scan
    pending_end = -1
    pending_dir = 0

    i = 0
    while i < n:
        pv = pair_completions[i]
        if pv != 0:
            # New pair completion — reset tail window
            # Most recent completion takes precedence
            pending_end = i + window_tail
            pending_dir = pv  # np.int8, no int() cast

        # Continuation must fire strictly after the completion bar (pv == 0)
        # and within the active tail window
        if pending_end >= i and pending_dir != 0 and pv == 0:
            cv = continuation_arr[i]
            if cv != 0 and cv == pending_dir:  # sign must match, no int() cast
                out[i] = pending_dir
                # Reset — prevent same completion firing twice
                pending_end = -1
                pending_dir = 0

        i += 1

    return out


def detect_signal(
    signals_df: pd.DataFrame,
    anchor: str,
    complement: str | None,
    connector: str | None,
    direction: str,
    window: int = 10,
    continuation: str | None = None,
) -> np.ndarray:
    """
    Public entry: build arrays, run ring buffer, apply direction filter.
    Returns int8 array aligned to signals_df rows.

    continuation: if provided alongside complement, activates Type 4 detection
    (run_ring_buffer_type4). window maps to window_pair; tail window uses
    WINDOW_CONTINUATION_TAIL constant.
    """
    anchor_arr, complement_arr = build_signal_arrays(signals_df, anchor, complement)
    if complement_arr is None:
        raw = run_ring_buffer(anchor_arr, None, connector, window)
    else:
        first_pat, _second_pat = get_pass_order(anchor, complement)
        if first_pat == anchor:
            first_arr = anchor_arr
            second_arr = complement_arr
        else:
            first_arr = complement_arr
            second_arr = anchor_arr
        raw = run_ring_buffer(first_arr, second_arr, connector, window)

        # Type 4: if continuation pattern provided, run two-stage detection
        if continuation is not None and continuation in signals_df.columns:
            cont_arr = signals_df[continuation].to_numpy(dtype=np.int8, copy=True)
            raw = run_ring_buffer_type4(
                first_arr,
                second_arr,
                cont_arr,
                window_pair=window,               # pass dynamic window arg
                window_tail=WINDOW_CONTINUATION_TAIL,
            )

    n = raw.shape[0]
    if direction == "long":
        i = 0
        while i < n:
            if raw[i] < 0:
                raw[i] = 0
            i += 1
    elif direction == "short":
        i = 0
        while i < n:
            if raw[i] > 0:
                raw[i] = 0
            i += 1

    return raw
