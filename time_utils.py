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

    Returns ``None`` if ``dt_val`` is ``None``. Raises ``ValueError`` if the
    value is timezone-naive — callers must attach a zone before calling.
    """
    if dt_val is None:
        return None
    ts = pd.Timestamp(dt_val)
    if ts.tz is None:
        raise ValueError(
            "tz-naive datetime passed to to_utc_timestamp(): "
            f"{dt_val!r}. Caller must supply tz-aware datetime."
        )
    return ts.tz_convert("UTC")
