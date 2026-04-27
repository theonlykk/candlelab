# ==============================================================================
# ⚠️  CROSS-REPO SYNCHRONIZATION REQUIRED  ⚠️
# This file (position_utils.py) exists in BOTH theonlykk/oanda-trading AND theonlykk/candlelab.
# If you modify logic here, you MUST manually copy the changes to the other repo.
# Do not add repo-specific dependencies to this file.
# Last synced: 2026-04-27
# ==============================================================================

"""
Single source of truth for instrument pip configuration and position sizing in CandleLab.

Mirrors the live executor's ``INST_CONFIG`` and ``_position_units`` logic in
``strategy_executor.py`` (oanda-trading). Keep in sync manually.
"""

from __future__ import annotations

INST_CONFIG = {
    "EUR/USD": {"pip": 0.0001, "pip_val": 10.0},
    "GBP/USD": {"pip": 0.0001, "pip_val": 10.0},
    "USD/JPY": {"pip": 0.01, "pip_val": 9.30},
    "USD/CAD": {"pip": 0.0001, "pip_val": 7.70},
    "AUD/USD": {"pip": 0.0001, "pip_val": 10.0},
    "USD/CHF": {"pip": 0.0001, "pip_val": 10.0},
    "NZD/USD": {"pip": 0.0001, "pip_val": 10.0},
    "Gold": {"pip": 0.01, "pip_val": 1.0},
    "XAU/USD": {"pip": 0.01, "pip_val": 1.0},
}

DEFAULT_UNITS = 10_000


def calculate_position_units(
    direction: str,
    sl_dist: float,
    pip: float,
    pip_val: float,
) -> int:
    """
    Position size in OANDA units (signed). Matches ``_position_units`` in strategy_executor.
    """
    if sl_dist <= 0 or pip <= 0 or pip_val <= 0:
        u = DEFAULT_UNITS
    else:
        sl_pips = sl_dist / pip
        if sl_pips <= 0:
            u = DEFAULT_UNITS
        else:
            try:
                u = int(100.0 / (sl_pips * pip_val) * 100_000.0)
            except ZeroDivisionError:
                u = DEFAULT_UNITS
    if u < 1:
        u = DEFAULT_UNITS
    cap = 75_000
    u = min(max(u, 1), cap)
    d = str(direction).upper()
    return u if d in ("BUY", "LONG") else -u
