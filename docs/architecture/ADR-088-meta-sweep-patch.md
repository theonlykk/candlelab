# ADR-088 — Meta Sweep Discovery Mode Patch

**Date:** 2026-05-30
**Status:** Accepted
**Repo:** theonlykk/candlelab
**Author:** Khalid Khan (Lead Engineer)
**Approved by:** Gemini (Staff Architect)

---

## Context

Phase 5 `meta_sweep.py` Red Team teardown identified four issues:

| Finding | Issue |
|---------|-------|
| **1** | Global NQS P70 threshold pooled all instruments — strong pairs diluted weak pairs' GREEN rate |
| **2** | `MIN_NEIGHBORS=3` excluded valid single-pattern combos with constrained neighbor space |
| **3** | `oos_mean_r > 0` pre-filter removed negative-mean candidates before neighborhood scoring |
| **4** | Hardcoded `DEPLOYED` list used legacy v1 format incompatible with sweep engine combo identity |

---

## Decision

Four surgical patches to `scripts/meta_sweep.py`:

### 1. Per-instrument NQS percentile (discovery)

`run_discovery()` now scores, thresholds, classifies, and commits **per instrument** inside the loop. `nqs_p70` computed from `instrument_scores` only — no cross-instrument contamination.

### 2. MIN_NEIGHBORS = 2

Reduced from 3. Single-pattern combos (empty continuation/gap) have fewer valid Hamming neighbors at threshold 1.0.

### 3. Remove `oos_mean_r > 0` pre-filter

`fetch_universe()` retains `n_windows_promoted >= min` and `oos_sqn100 > 0`. Negative-mean candidates enter neighborhood scoring; NQS clipping handles quality.

### 4. Stability mode deferred

Hardcoded `DEPLOYED` list deleted. `run_stability()` is a deferral stub until v2 strategies are promoted and a `sweep_deployed` table exists.

---

## Consequences

| Aspect | Detail |
|--------|--------|
| Discovery | GREEN classifications isolated per instrument |
| Thin universes | 1–2 candidates → all RED_THIN (correct, not a bug) |
| Stability | Prints DEFERRED message; no DB queries |
| Reactivation | Requires `sweep_deployed` table + first v2 live promotion |

---

## Negative space

- No changes to `score_candidate`, `weighted_distance`, `find_neighbors`, `write_meta_status`
- No changes to `PARAM_WEIGHTS`, percentile constants, or `sweep_validation_engine.py`
