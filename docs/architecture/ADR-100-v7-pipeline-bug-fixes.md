# ADR-100: V7 Pipeline Bug Fixes

**Date:** 2026-06-05

**Status:** Accepted

## Context

Three pipeline bugs were identified by a DeepSeek R1 audit and approved by Gemini:

1. **Currency matrix corruption** — `precompute_currency_matrix()` filled missing prices with `0.0` before computing log returns. Zero prices produce `log(0) = -inf`, corrupting the pseudo-inverse currency strength matrix downstream.

2. **Hallucinated SQN values** — `sqn100()` could produce extreme SQN scores (e.g. 3436) when R-multiples were near-homogeneous. Microscopic but non-zero `std_r` values passed the zero/inf guard, were clamped to `STD_FLOOR = 0.05`, and inflated the SQN ratio.

3. **ADX/BBW warmup bias** — `compute_window_thresholds()` used the full IS-window ADX and BBW arrays, including the first ~200 bars where Wilder EWM smoothing has not converged. Early-bar inflated values skewed percentile-based regime thresholds.

## Decision

Four surgical changes to `scripts/sweep_validation_engine.py`:

### Change 1 — `precompute_currency_matrix()`

Remove `P = P.fillna(0.0)` after `P.ffill()`. Leading NaN prices remain NaN; `R = np.log(P / P.shift(1)).fillna(0.0)` correctly represents zero log-return for bars with missing price history without injecting `-inf` into the strength matrix.

### Change 2 — `sqn100()`

Insert a `std_r < 1e-8` guard between the existing zero/inf check and the `STD_FLOOR` clamp. Near-identical R-multiples with float-precision variance return `0.0` instead of producing hallucinated SQN values.

### Change 3 — `compute_window_thresholds()`

Slice `[200:]` off `adx_vals` and `bbw_vals` before finite-value filtering and percentile computation. Clipping is applied here (not upstream) so raw `is_inds` remain intact for debugging and other consumers. IS windows with fewer than ~250 bars fall back to the SAFE hardcoded thresholds via the existing `len(adx_clean) < 20` guard.

### Change 4 — `IS_CACHE_VERSION` bump

Bump `IS_CACHE_VERSION` from `"v6"` to `"v7"` to invalidate stale IS cache entries that were computed under the buggy pipeline.

## Consequences

- V8 sweep will use corrected currency matrix, SQN, and ADX thresholds.
- `IS_CACHE_VERSION` bumped to `v7` — existing v6 cache rows will be ignored and recomputed on next sweep run.
- No changes to `meta_sweep.py`, `shared_config.py`, `candlelab-core`, `oanda-trading`, or database schema.
