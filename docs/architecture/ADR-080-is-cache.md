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

Add **read-through IS caching** backed by the existing `sweep_is_results` table.

### Phase 2.5b (initial)

Per-combo `_lookup_is_cache()` — one Postgres query per combo per window (N+1 bottleneck).

### Phase 2.5c (current)

**Bulk window load** via `_load_window_is_cache()`:

1. Before the combo loop, one SELECT fetches **all** cached rows for `(instrument, granularity, window_start, window_end, timeout, tp/sl, is_cache_version)`.
2. Results are keyed by combo signature tuple for O(1) in-memory lookup per combo.
3. On hit, append cached metrics with `from_cache=True` and `continue` — skip simulation.
4. On miss, run existing IS path with `from_cache=False`; **write bypass** sends only non-cached rows to `_write_is_results()`.

Version string `IS_CACHE_VERSION = "v1"` is part of the lookup key. Bump the constant when IS logic changes to invalidate stale rows without DB migration or manual deletes.

---

## Bulk lookup architecture

```
for window in windows:
    window_is_cache = _load_window_is_cache(...)   # 1 query
    for combo in combos:
        cached = window_is_cache.get(combo_sig)    # O(1) dict lookup
        if cached: append + continue
        else: simulate + append(from_cache=False)
    new_is_rows = [r for r in is_rows if not r.get("from_cache")]
    if new_is_rows: _write_is_results(new_is_rows)
    else: print("window fully cached")
```

**Combo signature key** (must match between load and lookup):

```python
(direction, anchor, anchor2, continuation, continuation2, gap, indicator, combo_type)
```

Built from `combo.get(...)` on lookup; built from DB row columns on load. Python `None` matches Postgres `NULL` in tuple keys when rows were stored with NULL combo fields.

**Query scope:** Window-level filters only (no per-combo WHERE). All combo identity columns are returned in the SELECT and used to build keys in Python. This replaces per-combo `IS NOT DISTINCT FROM` predicates with a single round-trip.

---

## Write bypass

Cached rows are already in `sweep_is_results`. Re-writing them would:

- Waste network and INSERT work
- Hit `ON CONFLICT DO NOTHING` anyway (no benefit)

Only rows with `from_cache=False` (fresh simulation) are passed to `_write_is_results()`. When every combo hits cache:

```
[is_cache] window fully cached (190 hits, 0 writes)
```

Band selection still runs on the full `is_rows` list (cached + fresh).

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

## Why `IS NOT DISTINCT FROM` (Phase 2.5b only)

Phase 2.5c bulk load no longer uses per-combo SQL predicates. Combo matching is done in Python via tuple keys. The NULL-equality issue that motivated `IS NOT DISTINCT FROM` in the per-combo query is handled because DB NULLs deserialize to Python `None`, matching `combo.get("anchor")` etc.

---

## Cache hit semantics

On hit, `_load_window_is_cache` returns IS metrics and `initial_bucket`. **`passes_band`, `final_bucket`, and `oos_eligible` are reset** before append:

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
- One query per window (not per combo) — eliminates N+1 Postgres bottleneck.
- Write bypass avoids redundant INSERTs for cache hits.
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
