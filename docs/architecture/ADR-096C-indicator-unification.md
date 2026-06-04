# ADR-096C — Indicator Unification

**Date:** 2026-06-03  
**Status:** Accepted  
**Repo:** theonlykk/candlelab  
**Author:** Red Team + Gemini (review) / Cursor (implementation)  
**Target:** `scripts/sweep_validation_engine.py`, `candlelab-core@v1.3.0`

---

## Context

The sweep engine maintained divergent indicator gate logic:

- `passes_rsi_envelope()` / `passes_ma_cross()` — local batch-array implementations
- `candlelab_core.indicators` — canonical Wilder RSI / EMA math for live signal engine

Batch rolling RSI/EMA on sliced DataFrames risked subtle math drift from live `RunningRSI`/`RunningEMA` accumulators.

---

## Decision

### Delete local gate functions

Removed `passes_rsi_envelope()` and `passes_ma_cross()` from `sweep_validation_engine.py`.

### Add causal wrappers

`_passes_rsi_causal()` and `_passes_ma_causal()` preserve identical gate logic (phase1/phase2/phase3 checks) but operate on pre-computed causal arrays — populated via ADR-096B for OOS, batch IS arrays for IS.

### Import from core

```python
from candlelab_core import RunningRSI, RunningEMA
```

Requirement pinned: `candlelab-core @ ...@v1.3.0`

### `_apply_indicator_filter()` call sites

All `passes_rsi_envelope` → `_passes_rsi_causal`  
All `passes_ma_cross` → `_passes_ma_causal`

---

## Negative space

- Does not change gate logic rules (RSI oversold/overbought + slope; MA cross phases).
- Does not modify `passes_trend_alignment()` (pure continuation path).
- Does not move gate logic into candlelab-core (wrappers remain in engine).
- Does not unify `compute_indicators()` IS batch path with Running accumulators (IS stays batch; OOS uses causal override).

---

## Cache bump — v6

| Constant | Value |
|----------|-------|
| `IS_CACHE_VERSION` | v6 |
| `oos_cache_version` | v6 |

Spread physics + indicator causality invalidate all prior cache entries.

---

## Related

- ADR-096A — spread crossing physics
- ADR-096B — causal OOS indicator arrays
- ADR-018 — RSI annotation refactor (candlelab-core)
