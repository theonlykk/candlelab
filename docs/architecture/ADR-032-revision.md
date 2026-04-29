# ADR-032 Revision — Fix Startup Daemon Plumbing for Multi-Timeframe Backfill

**Date:** 2026-04-29
**Status:** Accepted
**Repo:** theonlykk/candlelab
**Author:** Khalid Khan (Lead Engineer)
**Reviewer:** Gemini (Staff Architect)

---

## Context

ADR-032 extended `_backfill_all_worker()` to loop all instruments ×
`["5m", "15m", "1h"]`. However `backfill_all()` — the public function
that spawns the worker as a daemon thread — had zero call sites. It
was never invoked on startup. NZD_USD and USD_CAD had zero M15 rows
despite active M15 strategies.

We considered deriving M15/H1 by resampling M5 via pandas. Rejected
because pandas `.resample()` anchors to UTC midnight, not the FX
market day boundary (17:00 EST NY close). This causes OHLC divergence
from OANDA native bars, breaking simulation parity with the live
executor which uses native OANDA M15 candles.

## Decision

Path 1 — Fix the plumbing. Call `backfill_all()` explicitly inside
`start_scheduler()` so the daemon fires on every deployment.

**Rejected:**
- Path 2 (Resample from M5): pandas boundary misalignment with
  OANDA native bars. H4/D1 would be mathematically wrong vs live
  executor. Simulation divergence from execution engine unacceptable.

## Changes

- `scheduler.py`: `start_scheduler()` now calls `backfill_all()`
  inside a try/except after the startup thread is started. Import
  is local, consistent with existing `data` import pattern in
  `_compute_and_cache`. APScheduler startup is protected — a
  `backfill_all()` failure cannot prevent the scheduler from running.

## Consequences

- On every deployment, `_backfill_all_worker` spawns and fetches
  OANDA native M5, M15, H1 bars for all 12 instruments.
- Initial backfill: ~36 sequential fetches, 1 second apart,
  completing within ~36 seconds in a background daemon thread.
  Flask startup is not blocked.
- Subsequent restarts: incremental gap fills only — fast.
- UI simulation uses OANDA native bars — identical data source to
  the live executor. No mathematical divergence.

## Negative Space

- Does NOT change `backfill_all()` or `_backfill_all_worker()`.
- Does NOT change `data.py` in any way.
- Does NOT introduce pandas resampling for OHLC data.
- Does NOT add a top-level import to `scheduler.py`.
- Does NOT touch any route handlers or `strategy_runner.py`.
