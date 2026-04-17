"""
signal_engine.py — Canonical signal detection engine (candlelab / oanda-trading).

Ring-buffer pairing of two pattern series with pass ordering by PATTERN_IDS.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

PATTERN_IDS: dict[str, int] = {
    "Doji": 101,
    "Hammer/Hanging Man": 137,
    "Shooting Star/Inv. Hammer": 203,
    "Engulfing": 259,
    "Morning/Evening Star": 314,
    "Harami": 372,
    "Piercing/Dark Cloud": 418,
    "Three Soldiers/Crows": 463,
    "Spinning Top": 521,
    "Long-legged Doji": 574,
    "Rising/Falling Three Methods": 629,
    "Upside/Downside Tasuki Gap": 683,
}


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
    """True for bidirectional window pairing; ordered uses backward-only pairing."""
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
    an earlier bar t < m (backward ring scan).

    connector \"any-order\" / \"optional\": allow pairing within the window before
    or after m — backward scan plus a forward scan on the raw pattern arrays (the
    ring buffer only holds history behind m).
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
            if not paired and any_order:
                k = 1
                while k <= window:
                    t = m + k
                    if t >= n:
                        break
                    if w_first[t] != 0:
                        pass1_sig[m] = sv
                        w_first[t] = 0
                        w_second[m] = 0
                        j = t % window
                        if last_t[j] == t:
                            buf[j] = 0
                            last_t[j] = -1
                        paired = True
                        break
                    k += 1
        m += 1

    buf2 = np.zeros(window, dtype=np.int8)
    last_t2 = np.full(window, -1, dtype=np.int32)
    pass2_sig = np.zeros(n, dtype=np.int8)

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
            if not paired and any_order:
                k = 1
                while k <= window:
                    t = m + k
                    if t >= n:
                        break
                    if w_second[t] != 0:
                        pass2_sig[m] = fv
                        w_second[t] = 0
                        w_first[m] = 0
                        j = t % window
                        if last_t2[j] == t:
                            buf2[j] = 0
                            last_t2[j] = -1
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


def detect_signal(
    signals_df: pd.DataFrame,
    anchor: str,
    complement: str | None,
    connector: str | None,
    direction: str,
    window: int = 10,
) -> np.ndarray:
    """
    Public entry: build arrays, run ring buffer, apply direction filter.
    Returns int8 array aligned to signals_df rows.
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
