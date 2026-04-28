# ADR-024: Static Deployment Anchoring for 30d Backtest

- Status: Accepted
- Date: 2026-04-28

## Context

The 30d backtest was anchored to a rolling `datetime.now()` window, so reported metrics drifted day by day. For live strategy cards, the 30d backtest should represent the historical edge at deployment time, not a moving slice.

## Decision

Option A is adopted:

- Extract `go_live_at` from `candlelab_strategies_live` in `_hydrate_strategy_config`.
- Normalize with `to_utc_timestamp`.
- Pass this value as `reference_date` into `run_30d_backtest`.
- Compute the backtest window from `reference_date`, making it fixed as `[go_live_at - 30 days, go_live_at]`.

## Consequences

- 30d backtest metrics become static and preserve the quantitative proof at deployment time.
- If `oanda_candles` data for the anchored window ages out, the card flatlines (correct behavior due to no available data).
- If `go_live_at` is `NULL`, fallback to `datetime.now(timezone.utc)` preserves existing rolling behavior.
