# ADR-079 — Phase 2.5 Hardening: SQN Floor, sqn100 n_min, Equity Seed

**Date:** 2026-05-30
**Status:** Accepted
**Repo:** theonlykk/candlelab
**Author:** Khalid Khan (Lead Engineer)

---

## Context

After ADR-078 (IS Gate v2), three defects surfaced in `scripts/sweep_validation_ftmo_m30.py` during Phase 2 review:

1. **No absolute SQN floor** — relative band selection could promote configs whose shrinkage-adjusted score was near zero if all ELIGIBLE rows were weak.
2. **sqn100 starvation** — IS evaluation called `sqn100(r_mults, n_min=SQN_MIN_TRADES_IS)` where `SQN_MIN_TRADES_IS = 5`. Combos with 1–4 trades always received `raw_sqn = 0.0`, making WATCHLIST/ELIGIBLE bucket logic blind to their actual SQN.
3. **Equity curve reset on cache hit** — when OOS results came from `sweep_oos_cache`, synthetic equity reconstruction always seeded at `$10,000`, breaking cross-window MDD/Sharpe for combos evaluated across multiple WFV windows.

---

## Decision

Apply three targeted fixes in `run_wfv` (plus one cache-hit equity fix). No schema, cache hash, or OOS simulation changes.

### 1. `MIN_SQN_ELIGIBLE = 1.0` absolute floor

New constant in the IS Gate v2 block.

| Gate | Before | After |
|------|--------|-------|
| REJECTED | `raw_sqn <= 0` | `adjusted_score < MIN_SQN_ELIGIBLE` |
| passes_band | `best_score > 0` | `best_score >= MIN_SQN_ELIGIBLE` |

**Rationale:** Shrinkage-adjusted score is the promotion currency (ADR-078). The floor applies to `adjusted_score`, not raw SQN, so low-trade configs must clear both trade-count buckets *and* the absolute quality bar. A floor of 1.0 aligns with Van Tharp's "average system" SQN threshold — configs below this are not worth OOS compute.

### 2. `sqn100(..., n_min=1)` in IS evaluation

**Before:** `sqn100(r_mults, n_min=SQN_MIN_TRADES_IS)` — `SQN_MIN_TRADES_IS = 5`.

**After:** `sqn100(r_mults, n_min=1)`.

**Why `SQN_MIN_TRADES_IS` was wrong here:** That constant is an IS *promotion floor* for the old tournament model. Using it inside `sqn100()` zeroed out SQN for any combo with fewer than 5 trades, regardless of actual R-multiple quality. WATCHLIST (1–2 trades, positive mean R) could never accumulate meaningful `raw_sqn` or `adjusted_score`, and ELIGIBLE combos with exactly 3–4 trades were scored as if they had no edge. The trade-count gate belongs in bucket assignment (`MIN_TRADES_ELIGIBLE`, `MIN_TRADES_WATCHLIST`), not in the SQN computation itself.

`SQN_MIN_TRADES_IS` remains defined for documentation/legacy reference; the final leaderboard still uses `SQN_MIN_TRADES = 10`.

### 3. Synthetic equity seeding on cache hit

**Before:** `_eq = 10_000.0` on every cache-hit reconstruction.

**After:**
```python
existing = combo_equity_curves.get(combo_key, [])
_eq = existing[-1] if existing else 10_000.0
```

**Why reset was wrong:** `combo_equity_curves` accumulates per-combo equity across WFV windows for MDD/Sharpe in the leaderboard. Cache hits skip `sweep_simulation` (no `equity_after` on trades), so reconstruction from `oos_r_list` is required. Resetting to `$10,000` each window treated every OOS block as an independent backtest, understating drawdown and distorting Sharpe for frequently cached combos.

First window for a combo still seeds at `$10,000` (consistent with `sweep_simulation` initial capital).

---

## Consequences

**Positive**

- WATCHLIST and low-trade ELIGIBLE rows receive honest SQN values.
- Promotion requires both relative band membership and absolute quality ≥ 1.0 adjusted score.
- MDD/Sharpe on cache-hit paths reflect continuous equity across windows.

**Negative / operational**

- More combos may reach WATCHLIST with non-zero scores (audit noise, not OOS cost).
- Slightly stricter promotion when best ELIGIBLE adjusted score is between 0 and 1.0.

---

## Negative space — what was NOT fixed

### avg_spread not added to `_make_block_hash`

OOS block hash inputs remain: instrument, granularity, oos window, combo, timeout, tp/sl mult, sl_mode. **`avg_spread` is not hashed.**

**Why deferred:** Spread is derived from the full instrument dataset mean, not per-window. Adding it would invalidate the entire existing `sweep_oos_cache` population and force full OOS recomputation on next sweep. Spread leakage (if any) is a separate ADR; hash stability and cache reuse take priority for Phase 2.5.

Also untouched:

- `_make_block_hash`, `_cache_lookup`, `_cache_store` implementations
- `sweep_oos_cache` and `sweep_is_results` schemas
- OOS simulation path (`detect_signals`, `sweep_simulation`)
- `write_leaderboard_csv`, IS persistence, bucket topology

---

## Related

- ADR-078 — IS Gate v2 (eligibility band, shrinkage, sweep_is_results)
- ADR-077 — OOS block cache
