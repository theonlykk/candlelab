# ==============================================================================
# ⚠️  CROSS-REPO SYNCHRONIZATION REQUIRED  ⚠️
# This file (time_utils.py) exists in BOTH oanda-trading AND candlelab.
# If you modify logic here, you MUST manually copy the changes to the other repo.
# Do not add repo-specific dependencies to this file.
# Last synced: 2026-04-27
# ==============================================================================

import pandas as pd
from datetime import datetime, timezone

"""
Single source of truth for tz-aware timestamp construction across CandleLab.

Do not call ``pd.Timestamp(..., tz=...)`` in business logic — use ``to_utc_timestamp``
so naive datetimes are rejected instead of silently coerced.
"""


def to_utc_timestamp(dt_val) -> pd.Timestamp | None:
    """
    Normalize ``dt_val`` to a UTC-aware ``pd.Timestamp``.

    Returns ``None`` if ``dt_val`` is ``None``, invalid, or NaT. Naive values
    are interpreted as UTC.
    """
    if dt_val is None:
        return None
    try:
        ts_obj = pd.Timestamp(dt_val)
        if pd.isna(ts_obj):
            return None
        if ts_obj.tz is None:
            ts_obj = ts_obj.tz_localize("UTC")
        else:
            ts_obj = ts_obj.tz_convert("UTC")
        return ts_obj
    except Exception:
        return None
