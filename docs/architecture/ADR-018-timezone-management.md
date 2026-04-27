# ADR-018: Centralized timezone management

## Status

Accepted

## Date

2026-04-27

## Context

Mixing `pd.Timestamp(..., tz="UTC")` with values that already carry timezone information can produce `ValueError` (double tzinfo / ambiguous localization) and break HTTP handlers such as `/api/strategy-pnl` with a 500. Silent coercion of naive datetimes to local or assumed UTC zones is unacceptable in financial backtests because it hides caller bugs and skews cutoffs and session filters.

## Decision

- Introduce **`time_utils.py`** at the repo root as the dedicated boundary for UTC normalization.
- **`to_utc_timestamp(dt_val)`** returns **`None`** when **`dt_val`** is **`None`**; otherwise it wraps with **`pd.Timestamp(dt_val)`**, **rejects** timezone-naive inputs with **`ValueError`** (no guessing), and returns **`ts.tz_convert("UTC")`**.
- Business logic must **not** construct timestamps with **`pd.Timestamp(..., tz=...)`**; it must route through **`to_utc_timestamp`** so failures are explicit.

## Consequences

- All future timestamp construction intended for CandleLab trading/backtest logic should use **`to_utc_timestamp`** unless a clearly documented exception applies.
- **`time_utils.py`** is subject to **manual sync** with the **`oanda-trading`** repo (same obligation as **`indicator_utils.py`**).

## Cross-repo obligation

After this change is verified on **`main`**, copy **`time_utils.py`** to **`d:\oanda-trading`** so both codebases stay aligned.
