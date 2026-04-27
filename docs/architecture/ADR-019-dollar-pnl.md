# ADR-019: Per-trade dollar P&L and position sizing extraction

## Status

Accepted

## Date

2026-04-27

## Context

Theoretical P&L aggregates exposed `cum_net` as summed `pnl_pips` while the My Strategies UI formatted those values with a **`$`** prefix — mixing pip counts with dollar semantics. **`simulation_engine`** had no pip-to-account-dollar bridge. True P&L paths already report real OANDA dollars (`oanda_pl`), so the Theo / 30d rows were inconsistent with True P&L.

## Decision

- Introduce **`position_utils.py`** as the canonical copy of executor-style **`INST_CONFIG`** (pip / pip_val) and **`calculate_position_units`** (same formula, caps, and **`DEFAULT_UNITS`** as **`strategy_executor._position_units`**).
- Require **`sl_dist`** on each signal dict from **`strategy_runner`** so sizing matches live risk logic.
- Extend **`simulation_engine`** result rows with **`pnl_dollars`** derived from **`pnl_pips`**, **`pip_val`**, and **`abs(units)`** — never signed units for the dollar conversion. Retain **`pnl_pips`** on every result for pip-based analysis.
- Aggregate **`cum_net`** from **`pnl_dollars`** (skipping **`None`**) with 2 decimal places; key name **`cum_net`** unchanged for API/JS compatibility — the existing **`$`** formatter becomes semantically correct.

## Consequences

- **`cum_net`** on 30d backtest and Theo P&L (aggressive / passive) is expressed in **dollars** consistent with the UI.
- **`position_utils.py`** must be **manually synced** to **theonlykk/oanda-trading** after production verification (same cross-repo obligation as **`time_utils.py`** / **`indicator_utils.py`**).
- No JavaScript changes are required for the stat line formatter beyond spot-checking values after deploy.

## Sign safety

Dollar P&L uses **`abs(units)`** so SELL trades do not invert P&L via negative OANDA units. This must not be removed.
