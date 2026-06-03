# ADR-093 — Sweep Engine FTMO Migration

**Date:** 2026-06-03  
**Status:** Accepted  
**Repo:** theonlykk/candlelab  
**Author:** Red Team + Gemini (review) / Cursor (implementation)  
**Target:** `scripts/sweep_validation_engine.py`

---

## Context

The universal sweep engine (`sweep_validation_engine.py`) was still reading from `oanda_candles`, using OANDA-era assumptions for D1 timestamps, spread units, and UTC dead-zone hours. FTMO/MT5 data uses different table naming, D1 bar stamps, MT5 point spread encoding, and NY-session illiquidity timing. Six locked changes align the engine with FTMO reality without altering downstream `spread_cost` math or touching other modules.

---

## Decision — Six Changes

### 1. Table rename: `oanda_candles` → `ftmo_candles`

All SQL strings, comments, and docstrings in `sweep_validation_engine.py` now reference `ftmo_candles`. This is a **code-side** rename only; the physical PostgreSQL table must be renamed manually (see Manual Steps).

**Rationale:** Single canonical FTMO candle source. Eliminates confusion between legacy OANDA ingestion and current MT5 pipeline.

### 2. Granularity string: `'D'` → `'D1'`

D1 fetch SQL now uses `granularity = 'D1'` to match FTMO/MT5 ingestion labels.

**Rationale:** OANDA used `'D'`; FTMO stores daily bars under `'D1'`. Mismatch returned zero D1 rows and silently disabled the HTF MA_200 gate.

### 3. D1 `.shift(1)` with weekend gap guard and `merge_asof` tolerance

After computing `ma_200_d1` on D1 closes:

1. **`.shift(1)`** — intraday bars on day *T* only see the MA as it stood at day *T−1* close.
2. **Weekend gap guard** — `warnings.warn` if any D1 gap exceeds 4 calendar days.
3. **`merge_asof(..., tolerance=pd.Timedelta("4 days"))`** — gaps wider than 4 days produce `NaN` in `ma_200_d1`; existing `np.isfinite` checks in `apply_regime_gate()` skip the HTF gate gracefully.

**D1 timestamp convention**

| Source | D1 bar stamp | Effect without shift |
|--------|--------------|----------------------|
| OANDA | ~21:00 UTC (NY close) | MA mostly aligned to prior session |
| FTMO/MT5 | 00:00 UTC | Entire trading day sees unrealized same-day close in rolling MA |

The shift restores causal parity: intraday signals never peek at the current day's unfinished D1 close.

### 4. Spread cost: P80 × (`PIP[instrument]` / 10)

```python
avg_spread = float(df["spread_points"].quantile(0.80)) * (PIP[instrument] / 10.0)
```

**Spread math — MT5 points → price units**

| Concept | Value |
|---------|-------|
| MT5 `spread_points` | Integer points from `(ask_close - bid_close)` in DB |
| 1 MT5 point | 1/10 of a pip (5-digit broker convention) |
| Conversion | `price_units = spread_points × (PIP / 10.0)` |
| Example EUR_USD | PIP = 0.0001 → 1 point = 0.00001 price units |
| Example USD_JPY | PIP = 0.01 → 1 point = 0.001 price units |

P80 (not mean) penalises marginal strategies with a conservative spread assumption. Downstream logic is unchanged:

```python
spread_cost = avg_spread / sl_dist  # lines 1176, 1220, 1226, 1233, 1239
```

### 5. DST-aware dead zone (vectorized)

Static `DEAD_ZONE_HOURS = frozenset({20, 21, 22, 23})` UTC is commented out (not deleted). `detect_signals()` now uses:

```python
et_index = df.index.tz_convert("America/New_York")
dead_mask = et_index.hour.isin([17, 18, 19, 20]).to_numpy()
```

**Rationale:** Targets 17:00–20:00 ET (NY illiquidity window) regardless of EST/EDT. The old UTC frozenset was wrong in EST (winter) — it blocked 15:00–18:00 ET instead of 17:00–20:00 ET. Single vectorized C-level operation; no `pytz`, no per-bar loop, no cache.

### 6. `IS_CACHE_VERSION` bump: `v2` → `v3`

Forces IS cache invalidation after logic changes. Comment documents scope: ftmo_candles + D1 shift(1) + DST dead zone + spread P80.

---

## Negative space

This ADR does **not**:

- Rename the PostgreSQL table (manual DBA step).
- Modify `meta_sweep.py`, `shared_config.py`, `regime_filter.py`, or `export_live_params.py`.
- Touch `d:\oanda-trading\`, `candlelab_strategies_live`, or `strategy_executor.py`.
- Change `PIP` dict values — only how `spread_points` is converted.
- Change `spread_cost = avg_spread / sl_dist` or WIN/LOSS R-multiple subtraction lines.
- Add `import pytz` — `DatetimeIndex.tz_convert` is native pandas.
- Update `TIMEFRAME_CONFIGS["dead_zone_hours"]` keys (legacy; superseded in `detect_signals()`).

---

## Manual Steps (operator)

1. **Rename DB table** — `ALTER TABLE oanda_candles RENAME TO ftmo_candles;` (or equivalent migration). Code expects `ftmo_candles`; without this step all fetches fail.
2. **Verify D1 granularity** — Confirm daily rows use `granularity = 'D1'`. Re-ingest if still labelled `'D'`.
3. **Purge stale IS cache** — Delete or truncate `sweep_is_results` rows with `cache_version != 'v3'`, or full truncate before re-sweep.
4. **Purge OOS block cache (optional)** — If spread/D1 changes materially alter promoted combos, consider clearing `sweep_oos_cache` for affected instruments.
5. **Restart sweep** — Run `sweep_validation_engine.py --tf M30` (and H1 if applicable) from clean cache state.
6. **Validate D1 gaps** — Watch stderr for weekend gap warnings on illiquid pairs; investigate if `ma_200_d1` NaN rate spikes.

---

## Self-Review Checklist

- [x] Every `oanda_candles` → `ftmo_candles` in engine file
- [x] `granularity = 'D1'` in D1 fetch SQL
- [x] `.shift(1)` on `ma_200_d1` before `merge_asof`
- [x] `tolerance=pd.Timedelta("4 days")` in `merge_asof`
- [x] `avg_spread` uses `quantile(0.80) × (PIP[instrument] / 10.0)`
- [x] Dead zone uses `tz_convert("America/New_York")` — vectorized
- [x] `DEAD_ZONE_HOURS` commented out, not deleted
- [x] `IS_CACHE_VERSION = "v3"`
- [x] No other files modified
- [x] No `import pytz`

---

## Related

- ADR-082 — Universal sweep engine (`TIMEFRAME_CONFIGS`, `--tf` CLI)
- ADR-080 — IS cache (`IS_CACHE_VERSION`)
- ADR-083 — Regime filter v2 (HTF MA_200 gate consumes `ma_200_d1`)
- ADR-018 — Timezone management (ET session conventions)
