# ADR-089 — Phase 6b: Live Executor Currency Matrix (Offline Bridge)

**Date:** 2026-05-30
**Status:** Accepted
**Repo:** theonlykk/candlelab
**Author:** Khalid Khan (Lead Engineer)
**Approved by:** Gemini (Staff Architect)

---

## Context

ADR-085 added Gate 4 (currency matrix Z-spread confirmation) to the sweep validation engine. Phase 6b extends this to the live executor in `oanda-trading` — but the live path requires a clean handoff of IS-calibrated parameters without the sweep engine writing directly to live tables.

**Principle:** Operator retains full control. Export is manually triggered only after meta_sweep discovery confirms GREEN strategies and the operator is satisfied with manual review.

---

## Decision

### 1. `live_regime_parameters` table (DDL in `db_migrate.py`)

Stores per-(instrument, granularity) IS calibration params for live Gate 4:

- `base_mean`, `base_std`, `quote_mean`, `quote_std`
- `z_spread_p50`, `z_spread_p70`
- `sweep_window_start`, `sweep_window_end`, `calibrated_at`

Primary key: `(instrument, granularity)`.

Manual execution only — never at application startup.

### 2. `scripts/export_live_params.py` (standalone bridge)

Run after `meta_sweep.py --mode discovery`:

```bash
python scripts/export_live_params.py --tf M30
python scripts/export_live_params.py --tf H1
python scripts/export_live_params.py   # both
```

**Gating query:**
- `sweep_leaderboard.meta_status = 'GREEN'`
- `sweep_is_results.bucket_label = 'PROMOTED'`
- Non-null `base_mean` and `z_spread_p50`

**Selection:** `DISTINCT ON (instrument, granularity)` ordered by `window_end DESC` — latest IS window per pair.

**Upsert:** `ON CONFLICT (instrument, granularity) DO UPDATE`

---

## Consequences

| Aspect | Detail |
|--------|--------|
| Sweep engine | Never writes to `live_regime_parameters` |
| Operator control | If GREEN strategies look weak, simply do not run export |
| Stale params | Previous week's params remain active until export runs |
| No GREEN rows | Log message, return 0, table untouched |
| oanda-trading | Not touched in Prompt 1 — live reader in follow-up prompt |

---

## Negative space (Prompt 1)

- No changes to `sweep_validation_engine.py`, `meta_sweep.py`
- No changes to Flask app or `candlelab-core`
- No automatic export at sweep or meta_sweep completion
