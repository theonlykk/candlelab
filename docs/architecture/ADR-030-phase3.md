# ADR-030 Phase 3 — UI Hydration: Candle Store, Query Layer, Scheduler Extension

**Date:** 2026-04-29
**Status:** Accepted
**Repo:** theonlykk/candlelab
**Author:** Khalid Khan (Lead Engineer)
**Reviewer:** Gemini (Staff Architect)

---

## Context

Phase 1 migrated `oanda_candles` PK to `(instrument, granularity, time)`.
Phase 2 patched the executor to write M15 evaluations to
`executor_poll_log`. Phase 3 completes the pipeline by fixing the
candle write path, extending the scheduler, and threading granularity
through the backtest and simulation layers.

SEV-1: `insert_oanda_candles()` was silently failing in production
because `ON CONFLICT (instrument, time)` no longer matched the PK.
Every M5 upsert was a no-op since Phase 1 deployed.

## Decision

Option A — Granularity-First. Thread `granularity` explicitly through
all write and read paths.

**Rejected:**
- Option B (interval-derived inside functions): conflates UI interval
  strings with DB physical keys. Unclear ownership of translation.
- Option C (separate insert function for non-M5): violates DRY.
  Diverges over time. Does not scale for H4/D1.

## Changes

- `data.py`: `insert_oanda_candles()` signature extended with
  `granularity: str = "M5"`. SQL updated: column list, VALUES
  placeholders, ON CONFLICT target now `(instrument, granularity, time)`.
  Both call sites pass granularity resolved from `INTERVAL_MAP`.
- `strategy_runner.py`: `GRANULARITY_MINS` constant added.
  `_fetch_oanda_candles()` filters by `granularity = %s`.
  `run_30d_backtest()` warmup uses `WARMUP_BARS * gran_mins`.
  `_build_since_live_simulation()`, `run_since_live()`,
  `run_since_live_detail()` all accept and thread `granularity`.
- `scheduler.py`: loops `["5m", "15m", "1h"]` unconditionally so
  candle data is always hot for all core timeframes.
- `app.py`: `INTERVAL_MAP` imported from `data.py` (not redefined).
  Routes derive `granularity` from `strategy.interval` via
  `INTERVAL_MAP` and pass it to all runner functions.

## Consequences

- SEV-1 resolved: M5 upserts write correctly again.
- M15/H1 candles stored and queryable with correct granularity tag.
- Backtest warmup is timeframe-aware.
- Future timeframes (H4, D1) require only scheduler loop extension
  and `GRANULARITY_MINS` entry. Zero architectural changes.
- Future timeframes like D1 will request WARMUP windows of 200+ days.
  The OANDA backfill logic will need a review in a future ADR to
  handle long-span pagination safely.

## Negative Space

- Does NOT change `_fetch_placed_signals()`.
- Does NOT change `simulation_engine.py`.
- Does NOT change `signal_engine.py` or `patterns.py`.
- Does NOT add a `granularity` column to `executor_poll_log`.
- Does NOT touch `trades.html`.

## Follow-On

- ADR-030 Phase 1: Complete
- ADR-030 Phase 2: Complete
- ADR-030 Phase 3: This ADR — complete
