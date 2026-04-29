# ADR-030 Phase 1 — Multi-Timeframe Candle Storage: DB Migration

**Date:** 2026-04-29
**Status:** Complete
**Repo:** theonlykk/candlelab
**Author:** Khalid Khan (Lead Engineer)
**Reviewer:** Gemini (Staff Architect)

---

## Context

`oanda_candles` was implicitly M5-only. Two live strategies —
Sharp Violet Falcon (id=24, NZD/USD, 15m) and Wind Sage Raven
(id=25, USD/CAD, 15m) — were architecturally invisible to CandleLab.
The executor never wrote M15 evaluations to `executor_poll_log`.
CandleLab's signal hydration queries `executor_poll_log` exclusively
(Poll Log Supremacy), so M15/H1 strategies returned no UI data
regardless of trade volume.

The fix requires three phases. This ADR covers Phase 1 — schema
extension only. No Python code was changed.

## Decision

Option A — Universal Time-Series. Single table stores all timeframes
via a `granularity` column.

**Rejected:**
- Option B (Separate Tables): Violates zero-DDL-for-future-timeframes
  constraint. Each new granularity requires new table, new router
  logic, new UI queries.
- Option C (Materialised Views): OANDA native OHLCV and
  Pandas-resampled OHLCV will never match due to intra-candle tick
  aggregation. Operational overhead of REFRESH MATERIALIZED VIEW
  lifecycle unacceptable for production time-series data.

## Migration Executed

```sql
-- Step 1: Add granularity column (O(1) metadata op, no table lock)
ALTER TABLE oanda_candles
ADD COLUMN granularity VARCHAR(5) NOT NULL DEFAULT 'M5';

-- Step 2: CHECK constraint — schema-level firewall (Gemini mandate)
ALTER TABLE oanda_candles
ADD CONSTRAINT oanda_candles_granularity_check
CHECK (granularity IN ('M5', 'M15', 'H1', 'H4', 'D1'));

-- Step 3: Build new unique index concurrently (no exclusive lock)
CREATE UNIQUE INDEX CONCURRENTLY oanda_candles_new_pk_idx
ON oanda_candles (instrument, granularity, time);

-- Step 4: Split-second PK swap (millisecond lock, index pre-built)
ALTER TABLE oanda_candles
DROP CONSTRAINT oanda_candles_pkey,
ADD CONSTRAINT oanda_candles_pkey
PRIMARY KEY USING INDEX oanda_candles_new_pk_idx;
```

## Post-Migration State (verified 2026-04-29)
PK:   (instrument, granularity, time)
Rows: 1,218,831 — all tagged granularity='M5'
CHECK: granularity IN ('M5', 'M15', 'H1', 'H4', 'D1')

## Consequences

- All existing M5 rows correctly backfilled via DEFAULT — no data loss.
- Every query against `oanda_candles` MUST include `granularity` in
  the WHERE clause. Omitting it returns mixed-timeframe data silently.
  Enforced at the Python DAL layer in Phase 3.
- Future timeframes (H4, D1) require only scheduler extension and a
  CHECK constraint amendment. Zero architectural changes.
- The CONCURRENTLY pattern is now the mandated approach for any future
  index builds on this table.

## Negative Space

- Does NOT change any Python code paths.
- Does NOT backfill M15/H1 historical data.
- Does NOT touch the executor (Phase 2).
- Does NOT update the UI hydration layer (Phase 3).
- Redundant NOT NULL CHECK constraints left untouched per Gemini ruling.

## Follow-On

- ADR-030 Phase 2: Executor poll log patch (oanda-trading repo)
- ADR-030 Phase 3: UI hydration — scheduler.py, strategy_runner.py
