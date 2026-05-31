# ADR-081 — Phase 3: sweep_leaderboard (CSV → Postgres)

**Date:** 2026-05-30
**Status:** Accepted
**Repo:** theonlykk/candlelab
**Author:** Khalid Khan (Lead Engineer)

---

## Context

Walk-forward validation in `scripts/sweep_validation_ftmo_m30.py` produced leaderboard output only as per-instrument CSV files under `scripts/output/ftmo_m30/`. Downstream tooling (Hamming neighborhood analysis, meta-status, deployment queries) requires Postgres-backed aggregation.

A latent **combo identity bug** existed: `write_leaderboard_csv` and `compute_shadow_status` grouped on five dimensions (`anchor`, `continuation`, `gap`, `indicator`, `direction`) while the sweep space includes **2R**, **2R+1C**, **2C**, and **multi-timeout** configs. Distinct combos with different `anchor2`, `continuation2`, `combo_type`, or `timeout_bars` were merged into single leaderboard rows — corrupting OOS metrics.

Additionally, `timeout_bars` was never written to `wfv_df`, so timeout could not participate in grouping even if added to `combo_cols`.

---

## Decision

1. **Extend `wfv_df`** with full combo identity: `anchor2`, `continuation2`, `combo_type`, `timeout_bars` (from loop variable `timeout`, not global `TIMEOUT_BARS`).
2. **Fix `combo_cols`** in `write_leaderboard_csv`, `compute_shadow_status`, and new `_write_leaderboard` to nine dimensions.
3. **Add `_write_leaderboard()`** — parallel Postgres write to `sweep_leaderboard` during Phase 3 validation (CSV path retained).
4. **Add `db_migrate.py`** with canonical DDL for `sweep_leaderboard` — manual migration only, no startup DDL in sweep script.

---

## combo_cols bug fix

| Before (5 dims) | After (9 dims) |
|-----------------|----------------|
| anchor, continuation, gap, indicator, direction | + anchor2, continuation2, combo_type, timeout_bars |

**Impact:** 2R clusters (`anchor2` set), dual continuation (`continuation2`), topology label (`combo_type`), and per-pair timeout variants no longer collapse into one aggregated row.

---

## timeout_bars from loop, not global constant

`PAIR_CONFIG` defines per-instrument `timeouts: [20, 40, 60, 96]`. The WFV outer loop iterates `for timeout in timeouts`. Each promoted OOS row belongs to a **specific timeout variant**.

Using global `TIMEOUT_BARS = 28` would mis-key rows when the loop runs 20, 40, 60, or 96. **`int(timeout)` from the active loop** is the correct identity dimension for leaderboard grouping and `sweep_leaderboard` UNIQUE constraint.

---

## ON CONFLICT DO UPDATE (living snapshot)

Gemini ruling: leaderboard is **not a historical museum**. Each sweep run reflects the current 27-window rolling dataset for that instrument.

| Policy | Behavior |
|--------|----------|
| `DO UPDATE` | Same combo key → overwrite OOS metrics, MDD, Sharpe, `oos_r_list`, `sweep_run_id`, `updated_at` |
| `DO NOTHING` | Would preserve stale metrics from prior data windows — rejected |

`first_seen_at` remains from initial INSERT; `updated_at` refreshes on every upsert.

---

## Per-row SAVEPOINT

Leaderboard writes iterate one aggregated row per combo. A **batch-level** SAVEPOINT would roll back all rows if any single INSERT failed (bad JSON, type coercion, constraint edge case).

**Per-row SAVEPOINT (`row_write`):** one bad row logs error and rolls back only that row; remaining combos still commit. Matches ADR-077/080 non-fatal write pattern.

---

## db_migrate.py as canonical DDL

`sweep_leaderboard` schema and indexes live in `db_migrate.py::migrate_sweep_leaderboard()`. The sweep script never runs DDL at startup. Operators run:

```bash
python db_migrate.py
```

Table includes reserved columns for Phase 4 (`neighbor_count`, `neighborhood_quality_score`, `sqn_gap`, `meta_status`) — populated later, not by this change.

---

## Consequences

**Positive**

- Correct per-combo aggregation across full topology space.
- Postgres leaderboard queryable alongside `sweep_is_results` / `sweep_oos_cache`.
- CSV retained for Phase 3 validation parity.

**Negative / operational**

- Manual migration required before `_write_leaderboard` is effective.
- Upsert overwrites prior sweep metrics for same combo key (by design).

---

## Negative space — what this does NOT do

| Not in scope | Reason |
|--------------|--------|
| Remove CSV output | Phase 3 parallel validation |
| Hamming / meta_status computation | Reserved columns; future phase |
| Change IS cache or OOS cache | Untouched |
| Startup DDL in sweep script | Explicit non-goal |
| Historical leaderboard versioning | Living snapshot only |
| `candlelab-v2`, `pipshed`, `fx_candles` | Out of repo scope |

---

## Related

- ADR-078 — IS Gate v2, `sweep_is_results`
- ADR-080 — IS bulk cache
- ADR-077 — OOS block cache
