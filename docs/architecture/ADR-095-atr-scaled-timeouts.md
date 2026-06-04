# ADR-095 — ATR-Scaled Dynamic Timeouts

**Date:** 2026-06-03  
**Status:** Accepted  
**Repo:** theonlykk/candlelab  
**Author:** Red Team + Gemini (review) / Cursor (implementation)  
**Target:** `scripts/sweep_validation_engine.py`

---

## Context

Walk-forward validation assigns each combo a static `timeout_bars` value (e.g. 20, 40, 60, 96 M30 bars). That bar count is fixed regardless of current volatility. In high-ATR regimes a 20-bar window may be too short for mean-reversion or continuation to resolve; in low-ATR regimes the same 20 bars may be excessive calendar time, increasing timeout exits and diluting edge.

Static timeouts are fragile across volatility regimes and instruments without changing the combo search space.

---

## Problem

| Issue | Effect |
|-------|--------|
| Fixed bar count | Same wall-clock duration varies wildly with ATR |
| Regime mismatch | EUR/USD quiet week vs GBP/JPY news spike share no timeout semantics |
| Sweep axis inflation | Adding per-regime timeout combos multiplies search space unnecessarily |

---

## Decision

Replace per-trade static exit horizon with **ATR-scaled dynamic timeout** computed once at trade entry (O(1) — one scalar division, no per-bar cost).

### IS-calibrated baseline

Inside each WFV window loop, after `compute_indicators()` on the IS slice:

```python
_is_atr_finite = is_inds["atr"][np.isfinite(is_inds["atr"])]
atr_baseline = float(np.median(_is_atr_finite)) if len(_is_atr_finite) > 0 else 1.0
if atr_baseline <= 0:
    atr_baseline = 1.0
```

Median IS-window ATR is causal — it uses only data before the OOS window, mirroring the Gate threshold calibration pattern from ADR-083.

### Entry scaling (O(1))

At each trade entry in `sweep_simulation()`:

```python
_atr_ratio = clip(atr_at_entry / atr_baseline, 0.1, 10.0)
scaled_timeout = clip(int(timeout_bars / _atr_ratio), timeout_bars * 0.25, timeout_bars * 3.0)
```

| Condition | Scaling | Example (base=20) |
|-----------|---------|-------------------|
| ATR at entry > baseline | Shorter timeout (faster bars) | 20 → 10 |
| ATR at entry < baseline | Longer timeout (slower bars) | 20 → 40 |
| Ratio clipped | Prevents extreme values | floor 5, ceiling 60 |

`timeout_bars` from the combo/sweep axis remains the **baseline input** — not replaced in combo construction, DB writes, or logging.

### WFV symmetry

The **same** `atr_baseline` scalar is passed to both IS and OOS `sweep_simulation()` calls within a window. OOS trades scale against IS-calibrated volatility, not OOS future ATR — non-negotiable for walk-forward integrity.

---

## Cache version bump — v5

| Constant | Old | New |
|----------|-----|-----|
| `IS_CACHE_VERSION` | v4 | v5 |
| `oos_cache_version` in `_make_block_hash()` | v4 | v5 |

IS cache and OOS block cache must miss on v4 entries — timeout exit logic changed materially.

---

## Negative space

This ADR does **not**:

- Change dead zone logic (ADR-093 DST-aware ET window).
- Change D1 `.shift(1)` or `merge_asof` tolerance.
- Change spread formula or IS-calibrated P80 spread (ADR-093 hotfix 2).
- Modify Gate 1/2/3/4 regime filters.
- Touch `meta_sweep.py`, `shared_config.py`, or `d:\oanda-trading\`.
- Add new timeout values to the combo search space — existing `timeout_bars` are baseline inputs only.
- Write `scaled_timeout` to DB or leaderboard — audit trail keeps static `timeout_bars`.
- Scale timeout per-bar inside the exit loop — scaling is entry-only.

---

## Related

- ADR-093 — FTMO migration, spread P80, D1 shift, DST dead zone
- ADR-083 — Regime filter v2 (IS-calibrated gate thresholds)
- ADR-080 — IS cache (`IS_CACHE_VERSION`)
- ADR-077 — OOS block cache (`oos_cache_version`)
