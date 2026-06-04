# ADR-096A — Spread Crossing Physics

**Date:** 2026-06-03  
**Status:** Accepted  
**Repo:** theonlykk/candlelab  
**Author:** Red Team + Gemini (review) / Cursor (implementation)  
**Target:** `scripts/sweep_validation_engine.py` — `fetch_instrument_data()`, `sweep_simulation()`

---

## Context

After ADR-093, `fetch_instrument_data()` aliased `bid_*` and `ask_*` to mid-price OHLC. Spread was applied only as a post-hoc `spread_cost` deduction on WIN/LOSS R-multiples. That model ignored spread crossing at entry/exit — longs entered at mid instead of ask, shorts at mid instead of bid.

---

## Decision

### True bid/ask reconstruction

```python
_half_sp = (
    df["spread_points"]
    .fillna(df["spread_points"].median())
    .clip(lower=0)
    * (PIP[instrument] / 10.0)
    / 2.0
)
df["ask_open"]  = df["open"]  + _half_sp
df["bid_open"]  = df["open"]  - _half_sp
df["ask_close"] = df["close"] + _half_sp
df["bid_close"] = df["close"] - _half_sp
```

`spread_points` is MT5 integer points; `PIP/10` converts to price units; half applied symmetrically around mid.

### Stop-hunt penalty (0.5 factor)

Spread is now embedded in entry/exit prices. Retain a reduced post-hoc penalty:

```python
spread_cost = avg_spread / sl_dist * 0.5
```

The 0.5 factor represents adverse stop-hunt / slippage beyond quoted spread — prevents double-counting full spread in both prices and R deduction.

### `window_avg_spread` retained

IS-calibrated P80 spread (`window_avg_spread`) remains for the ATR stop-distance guard only — not for full spread deduction.

---

## Negative space

- Does not change `window_avg_spread` IS calibration (ADR-093 hotfix 2).
- Does not remove `spread_cost` entirely — halved, not zeroed.
- Does not use per-bar variable spread in simulation loop (half-spread fixed per bar from DB).

---

## Related

- ADR-093 — spread P80, ftmo_candles migration
- ADR-096B — causal indicators (orthogonal)
- ADR-096C — indicator unification (orthogonal)
