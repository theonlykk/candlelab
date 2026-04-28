# ADR-025: Theo Simulation Passthrough for Detail View

- Status: Accepted
- Date: 2026-04-28

## Context

The See Trades detail view requires per-signal Theo P&L outputs for both aggressive and passive simulation modes. Existing `run_since_live()` only returned aggregated metrics and discarded raw simulation rows. That created duplication pressure and risk of drift between aggregate and detail-path simulation logic.

## Decision

Option C is adopted:

- Extract core since-live simulation orchestration into `_build_since_live_simulation()`.
- Keep `run_since_live()` signature and return contract unchanged for current callers.
- Add `run_since_live_detail()` to expose raw aggressive/passive simulation lists.
- Fetch candles once and reuse the same fetched set for both simulation modes.

## Consequences

- Detail view now has a single backend path to path-dependent Bid/Ask simulation physics aligned with strategy-card aggregates.
- Legacy `replay_entry`/`replay_pips` fields can be retired in a follow-up ADR.
- `signal_time` is the merge key between Theo simulation output and OANDA trade rows.
- `run_since_live_detail` import into `app.py` is intentionally deferred to the next ADR wiring step.
