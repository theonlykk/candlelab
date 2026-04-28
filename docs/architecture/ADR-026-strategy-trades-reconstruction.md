# ADR-026: Strategy Trades Detail View Reconstruction

- Status: Accepted
- Date: 2026-04-28

## Context

The See Trades page used legacy heuristic matching and next-bar-open replay fields, which did not provide deterministic alignment with path-dependent simulation physics. It also included pre-epoch and loosely matched rows that cluttered comparison quality.

## Decision

Option C is adopted:

- Introduce `_build_trades_detail_rows` to merge three datasets using minute-floored UTC string keys.
- Use `run_since_live_detail` as the Theo source of truth for both aggressive and passive path-dependent Bid/Ask simulation outputs.
- Remove legacy replay and heuristic join logic from `strategy_trades`.
- Filter OANDA trade fetches by `anchor_ts` (`metrics_from` fallback to `go_live_at`) to exclude pre-epoch noise.
- Enforce None-safe aggregation for all summary P&L calculations.

## Consequences

- Detail view now mirrors aggregate-card simulation physics and produces consistent Theo/OANDA comparisons.
- Theo aggressive and passive outcomes are visible per signal with direct row-level context.
- Signals without OANDA matches are explicitly surfaced as geometry/filter rejections.
- A 9-card summary now reports all three streams (Theo aggressive, Theo passive, OANDA actual).
