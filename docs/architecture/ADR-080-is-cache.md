# ADR-080 — IS Result Caching via sweep_is_results

**Date:** 2026-05-30
**Status:** Accepted
**Repo:** theonlykk/candlelab
**Author:** Khalid Khan (Lead Engineer)

---

## Context

After ADR-078/079, full walk-forward sweeps in `scripts/sweep_validation_ftmo_m30.py` re-run in-sample (IS) simulation for every combo × every window on each invocation. With ~134 combos × 27 windows × 4 timeouts × 26 instruments, IS simulation dominates runtime (~21 hours observed). OOS blocks are already cached via ADR-077 (`sweep_oos_cache`); IS had no equivalent reuse path despite results being written to `sweep_is_results`.

---

## Decision

Add **read-through IS caching** backed by the existing `sweep_is_results` table:

1. Before `detect_signals` in the IS loop, call `_lookup_is_cache()`.
2. On hit, append cached metrics to `is_rows` and `continue` — skip simulation.
3. On miss, run existing IS path; `_write_is_results()` persists `is_r_list` and `is_cache_version` for future lookups.

Version string `IS_CACHE_VERSION = "v1"` is part of the lookup key. Bump the constant when IS logic changes to invalidate stale rows without DB migration or manual deletes.

---

## IS_CACHE_VERSION invalidation

| Mechanism | Behavior |
|-----------|----------|
| Lookup filter | `AND is_cache_version = %s` matches current constant only |
| Bump trigger | Any change to IS scoring, bucket rules, shrinkage, simulation params hashed in lookup, or signal/detection path |
| DB action | None — old rows remain but are invisible to lookup |
| Write path | Every new `_write_is_results` row stores current version |

Examples requiring a bump: `SQN_SHRINKAGE_K`, `MIN_SQN_ELIGIBLE`, `sqn100` logic, `sweep_simulation` SL/TP behavior, regime filter changes.

Examples **not** requiring a bump: band selection thresholds applied *after* IS metrics (relative band is recomputed each window from the full eligible pool).

---

## Why `window_start` / `window_end`, not `window_idx`

`window_idx` is positional (0 = oldest rolling window in the current dataset). When new M30 bars arrive, the same calendar IS period may shift index or boundaries may align differently across runs.

**Timestamps are stable:** a combo evaluated on IS `2024-01-01 → 2024-03-25` always keys to those bar boundaries regardless of how many total windows exist or which slot the window occupies today.

`window_id` is still written for telemetry but is **not** used as a cache key.

---

## Why `IS NOT DISTINCT FROM`, not `=`

Combo identity columns (`anchor`, `anchor2`, `continuation`, `continuation2`, `gap`, `indicator`, `combo_type`) are nullable. In SQL:

- `NULL = NULL` → **UNKNOWN** (no match)
- `NULL IS NOT DISTINCT FROM NULL` → **TRUE**

Pure continuation and single-anchor combos rely on NULL anchors; `=` would never cache-hit those rows.

---

## Cache hit semantics

On hit, `_lookup_is_cache` returns IS metrics and `initial_bucket`. **`passes_band`, `final_bucket`, and `oos_eligible` are reset** before append:

```python
"passes_band": False,
"final_bucket": "REJECTED",
"oos_eligible": False,
```

The unchanged band selection block immediately following reassigns promotion based on the **current window's full ELIGIBLE pool**. Cached per-row promotion from a prior run could be wrong if relative band thresholds or peer combos changed.

---

## Stored payload

| Column | Content |
|--------|---------|
| `is_cache_version` | e.g. `"v1"` |
| `is_r_list` | `json.dumps([float(r) for r in r_mults])` — full R-multiple list for net_r and audit |

Lookup SELECT retrieves: `adjusted_score_is`, buckets (for `initial_bucket` only at inject time), trade stats, `sqn_is`, `is_r_list`.

---

## Consequences

**Positive**

- Repeat sweeps skip IS simulation for unchanged windows/combos.
- No new table — reuses `sweep_is_results` as cache backing store.
- Version bump provides cheap, explicit invalidation.

**Negative / operational**

- Requires `is_cache_version` and `is_r_list` columns on `sweep_is_results` (manual migration).
- First run after version bump is full IS recompute.
- `ON CONFLICT DO NOTHING` on write means duplicate keys from same run still skip insert (lookup uses LIMIT 1).

---

## Negative space — what this does NOT cache

| Not cached | Reason |
|------------|--------|
| OOS simulation | ADR-077 `sweep_oos_cache` handles OOS |
| Band selection / promotion outcome | Recomputed each window from full pool |
| `window_idx` / positional window ID | Unstable across data updates |
| Regime features, indicators, spread | Implicit in stored IS metrics; version bump if logic changes |
| Leaderboard aggregation | Downstream of WFV; unchanged |
| `_make_block_hash` / OOS hash inputs | Untouched |
| `avg_spread` in hash | Still deferred (ADR-079) |

No startup DDL in Python. Cache lookup failures are non-fatal (SAVEPOINT rollback, fall through to full IS sim).

---

## Related

- ADR-078 — IS Gate v2, `sweep_is_results` persistence
- ADR-079 — SQN floor, sqn100 guards, equity seed
- ADR-077 — OOS block cache
