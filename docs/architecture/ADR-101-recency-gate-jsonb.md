# ADR-101: Recency Gate and promoted_window_indices JSONB

**Date:** 2026-06-05

**Status:** Accepted

## Context

The recency gate requires per-window promotion data. The `n_windows_promoted` integer alone is insufficient to determine whether a combo was promoted in recent walk-forward windows.

A schema gap was caught before V8 start: `sweep_leaderboard` had no column storing which window indices each combo was promoted in.

A fatal logic leak was identified in the initial recency gate implementation and caught by Gemini: computing `max_window` from per-candidate history gives every candidate a local threshold of -2 or lower, making the recency gate a no-op.

## Decision

1. Add `promoted_window_indices` JSONB column to `sweep_leaderboard` (DDL applied manually in Jupyter — not in Python).

2. Collect window indices in `_lb_agg_db()` inside `_write_leaderboard()` and persist via INSERT/UPSERT.

3. Pre-compute `global_recency_threshold` once per instrument in `run_discovery()` from the union of all candidates' `promoted_window_indices` — never from any individual candidate.

4. Gate GREEN status on:
   - Recency (promoted in at least one of the last 5 windows relative to global max)
   - `oos_sqn100 >= 0.8`
   - `NQS >= NQS_GLOBAL_FLOOR`
   - `sqn_gap <= SPIKE_TOLERANCE`

5. Label stale candidates `RED_STALE` and weak-SQN candidates `RED_WEAK_SQN`.

6. Raise `timeout_bars` NQS weight from `1.0` to `5.0` — holding period is structurally distinct, not a minor tweak.

7. Bump `IS_CACHE_VERSION` from `"v7"` to `"v8"`.

## Consequences

- V8 results carry full window index history per combo.
- Meta sweep recency gate enforces "trade what's hot."
- Stale and weak candidates are explicitly labelled `RED_STALE` and `RED_WEAK_SQN`.
- Existing v7 IS cache entries will be ignored and recomputed on next sweep run.
