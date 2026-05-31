# ADR-083 — Regime Filter v2 (Phase 4)

**Date:** 2026-05-30
**Status:** Accepted
**Repo:** theonlykk/candlelab
**Author:** Khalid Khan (Lead Engineer)

---

## Context

The sweep engine (`scripts/sweep_validation_engine.py`) applied regime gating in two places:

1. **Inside `sweep_simulation`** — hardcoded ADX/BBW thresholds from `regime_filter.py` imports (`ADX_TREND_THRESHOLD`, `BBW_DEAD_ZONE`, etc.)
2. **Implicitly via profile** — Counter-Trend / Hybrid / Pro-Trend mapped from combo recipe

Red Team review identified three structural problems:

| Finding | Issue |
|---------|-------|
| **Finding 1** | Static offline thresholds calibrated on full history introduce look-ahead bias when applied window-by-window in WFV |
| **Finding 2** | Distance-from-MA measured in raw pips is not comparable across instruments or volatility regimes |
| **Finding 3** | No higher-timeframe structural anchor; intraday ADX/BBW alone miss macro trend context |

Gemini ruling: **decouple `detect_signals` from regime gating**. Pattern detection stays pure; regime filtering is a post-processing gate on the signal array.

---

## Decision

**Phase 4 — `regime_filter_v2`:** Window-calibrated percentile thresholds computed from IS data only, applied via a new `apply_regime_gate()` function after `_apply_indicator_filter` and before `sweep_simulation`.

### 1. D1 MA_200 HTF anchor (`fetch_instrument_data`)

- Fetch daily candles from `oanda_candles` (`granularity = 'D'`)
- Compute rolling 200-period mean on D1 close
- Merge onto primary bars via `pd.merge_asof(..., direction="backward")` — zero lookahead
- Column: `ma_200_d1`
- Non-fatal on failure: sets `ma_200_d1 = NaN`, logs warning

### 2. Window-calibrated thresholds (`compute_window_thresholds`)

Called once per `(window_idx, timeout)` inside `run_wfv`, using **IS slice only**:

| Key | Percentile | Purpose |
|-----|------------|---------|
| `adx_building` | ADX p50 | Pro-Trend floor |
| `adx_trending` | ADX p65 | Counter-Trend ceiling |
| `adx_strong` | ADX p85 | Reserved |
| `adx_blowoff` | ADX p97 | Pro-Trend blowoff ceiling |
| `bbw_dead_zone` | BBW p30 | Compressed-market filter |
| `dist_ma_p30` | ATR-normalized dist p30 | Counter-Trend HTF gate |
| `dist_ma_p70` | ATR-normalized dist p70 | Reserved |

**ATR-normalized distance** (Finding 2 fix):

```
dist_norm = |close - ma_200_d1| / atr
```

**SAFE fallback dict** when fewer than 20 finite ADX/BBW samples — hardcoded conservative defaults prevent crash or silent pass-through on thin windows.

### 3. Three-layer gate (`apply_regime_gate`)

Applied after `_apply_indicator_filter`, before `sweep_simulation`. Returns copies; never mutates input arrays.

| Layer | Logic |
|-------|-------|
| **Gate 1 — BBW dead zone** | Zero signals where `bbw < bbw_dead_zone` |
| **Gate 2 — ADX profile** | Counter-Trend: kill if ADX ≥ p65; Pro-Trend: kill if ADX < p50 or ADX ≥ p97; Hybrid: no ADX gate |
| **Gate 3 — HTF MA_200_D1** | Pro-Trend: long requires close > MA; short requires close < MA; Counter-Trend: kill if ATR-normalized dist ≥ p30; Hybrid: no HTF gate |

Graceful degradation: missing `ma_200_d1`, finite ADX/BBW, or threshold keys skip that layer.

IS-computed `window_thresholds` are reused for OOS — strictly causal (no OOS data in threshold calibration).

### 4. Ghost logic neutralization

`sweep_simulation` lines 988–1017 contain the legacy hardcoded ADX/BBW gate. Since `apply_regime_gate` already sterilizes the signal array, passing `adx_arr=None, bbw_arr=None` from `run_wfv` bypasses the simulation-internal gate without modifying `sweep_simulation` internals.

`is_adx`, `is_bbw`, `oos_adx`, `oos_bbw` variables remain — they populate `is_inds`/`oos_inds` for threshold computation and gate application.

---

## Pipeline order (IS and OOS)

```
detect_signals
  → _apply_indicator_filter
  → apply_regime_gate          ← NEW (Phase 4)
  → sweep_simulation           ← adx_arr=None, bbw_arr=None
```

OOS block cache logic unchanged — cached combos skip the entire detect/filter/gate/simulate path.

---

## Negative space — Phase 4 did NOT

- Modify `detect_signals`
- Modify `_apply_indicator_filter`
- Modify `sweep_simulation` internals
- Modify `compute_regime_features`
- Touch `_make_block_hash`, `_cache_lookup`, `_cache_store`, `_load_window_is_cache`
- Touch OOS block cache logic or Postgres tables (`sweep_oos_cache`, `sweep_is_results`, `sweep_leaderboard`)
- Modify `regime_filter.py` in oanda-trading
- Touch candlelab-v2, pipshed, fx_candles

---

## Consequences

- WFV regime gates are now window-local and IS-calibrated — removes static-threshold look-ahead bias
- D1 structural context available without repainting intraday bars
- Single authoritative gate path; no double-filtering from simulation ghost logic
- Cache invalidation: existing OOS cache entries were computed under old regime logic; a cache bust or version bump may be needed in a follow-up if parity with new gates is required
