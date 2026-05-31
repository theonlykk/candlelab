# ADR-085 — Phase 6: Currency Matrix Confirmation Gate

**Date:** 2026-05-30
**Status:** Accepted
**Repo:** theonlykk/candlelab
**Author:** Khalid Khan (Lead Engineer)
**Approved by:** Gemini (Staff Architect)

---

## Context

The sweep engine (Phases 1–5) applies regime gating via BBW, ADX profile, and D1 MA_200 (ADR-083). These gates are per-instrument and do not exploit cross-sectional information across the 26-pair FX universe.

**Self-confirmation problem:** Using a pair's own return to confirm its signal creates circular logic — the signal and confirmation share the same noise source.

**Lookahead prevention:** Currency strength thresholds must be calibrated from IS window data only. OOS bars use IS-calibrated mean/std parameters stored in `window_thresholds`, never recomputed from OOS data.

---

## Decision

Add **Gate 4 — Currency matrix Z-spread confirmation** to `apply_regime_gate()`.

### Architecture: target-excluded pseudo-inverses

1. **`precompute_currency_matrix(conn, cfg)`** — called once in `main()` before the instrument loop.
2. Fetch all 26 pairs (`sorted(PAIR_CONFIG.keys())`) in one query.
3. Build wide price panel P (T × 26), log returns R.
4. Incidence matrix A (26 × 10) maps pairs to G10 currencies: base +1, quote −1.
5. Augment with sum-to-zero row → A_aug (27 × 10).
6. For each target pair p, exclude row i and compute `pinv(A_excl)` — prevents self-confirmation.
7. Compute per-pair strength panel S_p (T × 10) via pseudo-inverse × excluded returns.

### Pipeline integration

```
precompute_currency_matrix()  → strength_dict (once)
fetch_instrument_data()       → merge base_strength, quote_strength (merge_asof backward)
compute_window_thresholds()   → IS-calibrated z_spread_p50, base_mean, base_std, etc.
apply_regime_gate() Gate 4    → kill signals lacking cross-sectional confirmation
```

### Gate 4 logic

```
z_spread = z(base_strength) - z(quote_strength)
long:  reject if z_spread < z_spread_p50 (IS p50)
short: reject if z_spread > -z_spread_p50
```

IS mean/std stored in `thresholds` dict; OOS applies same calibration — strictly causal.

---

## Consequences

| Aspect | Detail |
|--------|--------|
| Memory | ~62–135 MB for strength_dict (26 pairs × T bars × 10 currencies) |
| Compute | Single precompute; no per-bar or per-window pseudo-inverse recomputation |
| DB | Read-only from `oanda_candles`; no new tables or writes |
| Degradation | Missing pair → NaN strengths; Gate 4 skips silently |
| Degradation | < 20 IS z_spread observations → thresholds None; Gate 4 skips |

Live executor integration (`regime_filter.py` in oanda-trading) is **Phase 6b** — out of scope.

---

## Negative space — Phase 6 did NOT

- Modify `sweep_simulation()` internals
- Touch IS/OOS cache (ADR-077–080), leaderboard writes (ADR-081), IS Gate v2 (ADR-078)
- Modify `meta_sweep.py`, `db_migrate.py`
- Import scipy, QuantLib, ta-lib
- Add new DB tables or startup DDL
