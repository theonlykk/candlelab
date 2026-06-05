# ADR-102: Meta Sweep Guards and Dry-Run Mode

**Date:** 2026-06-05

**Status:** Accepted

## Context

A DeepSeek R1 audit of `meta_sweep.py` identified silent failures:

1. **NaN/None crash on `oos_sqn100`** — `median(neighbor_sqns)` raises `StatisticsError` when all neighbors have NULL values from PostgreSQL; direct `>= 0.8` comparison on None raises `TypeError`.

2. **Empty `all_windows` disables recency gate** — when no `promoted_window_indices` data exists, `global_max_window = 0` and `global_recency_threshold = -4`, making every candidate pass the recency check.

3. **No single-instrument dry-run mode** — validating meta_sweep changes required running the full instrument universe.

## Decision

Four changes to `scripts/meta_sweep.py`:

### Change 1 — `score_candidate()` NaN/None guard

Filter None and NaN from `neighbor_sqns` before `median()`. Fall back to `0.0` median if all neighbors are invalid. Set `sqn_gap = float("nan")` when candidate SQN is missing.

### Change 2 — GREEN gate NaN/None guard

Resolve `cand_sqn` to `0.0` when None/NaN before the `>= 0.8` comparison. Resolve `safe_sqn_gap` to `float("inf")` when `sqn_gap` is NaN, treating missing gap as spike.

### Change 3 — Empty `all_windows` guard

When no `promoted_window_indices` data exists across the universe, set `global_recency_threshold = float("inf")` with a WARNING print. Never default to `-4`.

### Change 4 — `--instrument` flag

Add `--instrument` to `parse_args()` and `instrument_filter` parameter to `run_discovery()` for single-instrument dry-run validation.

## Consequences

- `meta_sweep` will not crash on NULL DB values.
- Recency gate cannot be accidentally disabled by missing JSONB data.
- Single-instrument dry-run available before full universe run: `python scripts/meta_sweep.py --mode discovery --tf M30 --instrument GBP_JPY`
