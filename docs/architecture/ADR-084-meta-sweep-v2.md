# ADR-084 — Meta Sweep v2 (Phase 5)

**Date:** 2026-05-30
**Status:** Accepted
**Repo:** theonlykk/candlelab
**Author:** Khalid Khan (Lead Engineer)

---

## Context

CandleLab maintained two separate CSV-based meta scripts:

- `scripts/meta_sweep_discovery.py` — scan leaderboards for plateau candidates
- `scripts/meta_sweep_stability.py` — audit deployed strategies against neighborhood health

Both read CSV exports, wrote CSV reports, and duplicated neighborhood-scoring logic. The sweep engine now persists results to `sweep_leaderboard` (ADR-081), making CSV round-trips obsolete.

---

## Decision

**Phase 5:** Replace both scripts with a single DB-native `scripts/meta_sweep.py`.

| Mode | Purpose |
|------|---------|
| `--mode discovery` | Score all eligible leaderboard rows; assign `meta_status` (GREEN / RED / RED_SPIKE / RED_THIN) |
| `--mode stability` | Audit `DEPLOYED` registry; assign GREEN / AMBER / RED; MISSING = hard kill switch |

CLI: `--tf M30|H1` reads `TIMEFRAME_CONFIGS` including new `min_windows_promoted` (M30=5, H1=3).

---

## Weighted Hamming neighborhood (`PARAM_WEIGHTS`)

| Parameter | Weight | Rationale |
|-----------|--------|-----------|
| `direction` | 10.0 | Long vs short = different universe |
| `combo_type` | 10.0 | 1R vs 2R = different universe (Gemini-approved) |
| `continuation` | 5.0 | Pattern change = structural shift |
| `anchor` | 1.0 | Minor topology tweak = valid neighbor |
| `timeout_bars` | 1.0 | Minor parameter tweak = valid neighbor |
| `anchor2`, `continuation2`, `gap`, `indicator` | 0.0 | Part of identity; excluded from distance |

`NEIGHBORHOOD_THRESHOLD = 1.0` — only rows within weighted distance ≤ 1.0 are neighbors.

`weighted_distance` uses early exit when distance exceeds threshold.

---

## Scoring rules

### Neighborhood quality score (NQS)

```
NQS = avg(max(0, neighbor.oos_mean_r)) over neighbors
```

### Spike filter

```
sqn_gap = candidate.oos_sqn100 - median(neighbor.oos_sqn100)
```

Reject if `sqn_gap > SPIKE_TOLERANCE` (1.0) — candidate is an isolated spike, not a plateau member.

### Hard floors

| Constant | Value | Purpose |
|----------|-------|---------|
| `MIN_NEIGHBORS` | 3 | Cannot prove plateau with fewer than 3 neighbors |
| `NPR_ABS_FLOOR` | 0.20 | Absolute minimum NQS regardless of percentile |
| `NQS_ABS_FLOOR` | 0.0 | Stability RED floor |

### Two-layer threshold

| Mode | Percentile | Effective threshold |
|------|------------|---------------------|
| Discovery | P70 | `max(P70(NQS), 0.20)` |
| Stability | P60 | `max(P60(NQS), 0.20)` |

---

## Database integration

### Read: `fetch_universe`

Scoped strictly to `(granularity, instrument)` — no cross-contamination. Filters: `oos_mean_r > 0`, `n_windows_promoted >= min_windows_promoted`, `oos_sqn100 > 0`.

### Write: `write_meta_status`

Updates `sweep_leaderboard` columns: `neighbor_count`, `neighborhood_quality_score`, `sqn_gap`, `meta_status`.

Nullable combo columns matched with **`IS NOT DISTINCT FROM`** — correct NULL equality for `anchor2`, `continuation`, etc.

**SAVEPOINT per row** — one write failure never kills the batch.

---

## Stability: DEPLOYED registry

Full combo identity required vs legacy format:

- `granularity`, `anchor2`, `continuation2`, `combo_type`, `timeout_bars`

**MISSING** status: deployed config not found in leaderboard at all — hard kill switch (logged, no DB write).

---

## Negative space — Phase 5 did NOT

- Modify sweep engine logic beyond `min_windows_promoted` in `TIMEFRAME_CONFIGS`
- Touch `sweep_oos_cache`, `sweep_is_results`, `shadow_book_state`
- Modify `regime_filter.py` in oanda-trading
- Touch candlelab-v2, pipshed, fx_candles

---

## Consequences

- Single source of truth: `sweep_leaderboard` in/out — no CSV drift
- Discovery and stability share identical scoring primitives
- Deployed audit uses full 9-dimension combo identity aligned with ADR-081 leaderboard schema
