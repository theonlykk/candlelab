# ADR-023: Wizard Type 4 Parsing Integrity

- Status: Accepted
- Date: 2026-04-28

## Context

`api_finalise()` was ignoring `body.get("connector")`, reconstructing connector state as `"any-order"` when both `pattern_2` and `continuation` were present, where Type 4 semantics require `"type4"`. It also never passed `continuation` through to `_backtest_pattern()` or `detect_signal()`.

As a result, wizard evaluation ran two-pattern matching instead of the Type 4 three-pattern ring buffer path, producing inflated counts (about 50 signals) versus the expected approximately 35.

## Decision

Backend State Supremacy is applied on transient payload parsing:

- Derive connector server-side from structural presence of `pattern_2` and `continuation`.
- Use `"type4"` when both are present, `"any-order"` when only `pattern_2` is present, and `"ordered"` when only `continuation` is present.
- Pass continuation explicitly through `api_finalise` -> `_backtest_pattern` -> `detect_signal`.

## Consequences

- Wizard preview now correctly activates `run_ring_buffer_type4` for Type 4 strategies.
- Wizard and 30d-card signal counts align for Type 4 setups.
