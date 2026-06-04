# ADR-096B — Causal Indicator Computation

**Date:** 2026-06-03  
**Status:** Accepted  
**Repo:** theonlykk/candlelab  
**Author:** Red Team + Gemini (review) / Cursor (implementation)  
**Target:** `scripts/sweep_validation_engine.py` — `run_wfv()` window loop

---

## Context

OOS indicator arrays (`rsi`, `sma_fast`, `sma_slow`) were computed via batch `compute_indicators()` on the OOS slice alone. Rolling/ Wilder indicators at OOS bar 0 lack IS warm-up context — values differ from what would be observed causally at live deployment after an IS training window.

IS indicators computed on the IS slice only are already causal within IS and are **not** modified.

---

## Decision

### Warm-start from IS closes

After `atr_baseline` calculation, per window:

```python
_rsi_acc = RunningRSI(period=RSI_PERIOD)
_ema_fast_acc = RunningEMA(period=MA_FAST)
_ema_slow_acc = RunningEMA(period=MA_SLOW)
for _c in is_df["close"].to_numpy():
    _rsi_acc.update(_c)
    _ema_fast_acc.update(_c)
    _ema_slow_acc.update(_c)
```

Uses `RunningRSI` / `RunningEMA` from `candlelab-core@v1.3.0`.

### OOS incremental update (once per window)

After band selection, before `for pr in promoted:`:

```python
for _bi, _c in enumerate(oos_df["close"].to_numpy()):
    _causal_rsi[_bi] = _rsi_acc.update(_c)
    ...
oos_inds["rsi"] = _causal_rsi
oos_inds["sma_fast"] = _causal_ema_fast
oos_inds["sma_slow"] = _causal_ema_slow
```

Computed **once per window**, not per combo.

### WFV symmetry

Same warm-started accumulators feed all promoted OOS combos in the window. IS path uses unchanged `is_inds` from `compute_indicators(is_df)`.

---

## Negative space

- Does not warm-start ADX, BBW, or ATR (regime features unchanged).
- Does not modify IS `is_inds` arrays.
- Does not change `compute_indicators()` implementation for IS path.
- Does not apply Running accumulators to full-dataset precompute (window-scoped only).

---

## Related

- ADR-096C — `RunningRSI`/`RunningEMA` source in candlelab-core
- ADR-095 — ATR-scaled timeouts (orthogonal)
- ADR-080/077 — cache v6 invalidation
