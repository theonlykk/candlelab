# ADR-078 — IS Gate v2: Eligibility Band + sweep_is_results Persistence

**Date:** 2026-05-30
**Status:** Accepted
**Repo:** theonlykk/candlelab
**Author:** Khalid Khan (Lead Engineer)

---

## Context

Walk-forward validation in `scripts/sweep_validation_ftmo_m30.py` previously used a **top-5 tournament** on in-sample (IS) SQN: rank all combos, keep the five highest scores above a fixed threshold (`SQN_PROMOTE_THRESHOLD`), and immediately run OOS on that small set. IS results were never persisted — only promoted combos produced OOS rows.

Problems with the tournament model:

- Hard cap of five promoted combos per window regardless of how many genuinely strong configs exist.
- No audit trail for combos that almost qualified or showed early promise with few trades.
- WATCHLIST-like configs (positive mean R, insufficient trade count) were indistinguishable from full rejects.
- Re-running a sweep could not reconstruct IS gate decisions without re-simulating.

## Decision

Replace the top-5 IS tournament with **IS Gate v2**:

1. Evaluate **all** combos in the IS window (unchanged simulation path).
2. Score each combo with **shrinkage-adjusted SQN**.
3. Assign **initial buckets**: `REJECTED`, `WATCHLIST`, or `ELIGIBLE`.
4. Apply **relative band selection** on `ELIGIBLE` rows only → `PROMOTED` (OOS-eligible).
5. **Persist every IS row** to `sweep_is_results` before any OOS execution.
6. Run OOS **only** on `PROMOTED` combos (existing OOS cache from ADR-077 unchanged).

**Hard constraint:** `WATCHLIST` combos must never reach OOS execution (`oos_eligible = False`).

---

## Shrinkage formula

Raw IS SQN uses the existing `sqn100()` helper with `SQN_MIN_TRADES_IS` floor.

```
shrink       = sqrt(n / (n + SQN_SHRINKAGE_K))   # n = trade count; K = 10.0
adjusted_score = raw_sqn * shrink
```

Low trade counts pull adjusted scores toward zero, reducing promotion of noisy high-SQN configs.

---

## Bucket assignment

| Condition | Initial bucket |
|-----------|----------------|
| `n < 1` OR `mean_r <= 0` OR `raw_sqn <= 0` | `REJECTED` |
| `n < MIN_TRADES_ELIGIBLE` (3) AND `mean_r > 0` | `WATCHLIST` |
| Otherwise | `ELIGIBLE` |

### Relative band (ELIGIBLE only)

```
best_score = max(adjusted_score for ELIGIBLE rows), default 0.0

passes_band = best_score > 0
              AND adjusted_score >= best_score * RELATIVE_SCORE_FRACTION  # 0.80
```

| Initial bucket | passes_band | final_bucket | oos_eligible |
|----------------|-------------|--------------|--------------|
| ELIGIBLE | true | `PROMOTED` | true |
| ELIGIBLE | false | `ELIGIBLE_NOT_BANDED` | false |
| WATCHLIST | false | `WATCHLIST` | false |
| REJECTED | false | `REJECTED` | false |

If `best_score <= 0`, nothing is promoted (Gemini patch).

---

## WATCHLIST as counterfactual audit stream

`WATCHLIST` rows represent combos with **positive mean R** but **insufficient trades** for full eligibility (`1 <= n < 3`). They are:

- Written to `sweep_is_results` with `final_bucket = WATCHLIST` and `oos_eligible = false`.
- **Never** passed to the OOS loop — preserving compute budget and preventing premature OOS validation on under-sampled configs.
- Available for downstream analysis: “what would have happened if we lowered the trade floor?” without re-running IS simulation.

This is a first-class audit dimension, not a silent discard.

---

## sweep_is_results schema

Table lives in Railway Postgres (30 columns). Populated by `_write_is_results()` with `ON CONFLICT DO NOTHING` (idempotent re-runs).

| Column group | Columns |
|--------------|---------|
| Telemetry | `run_id`, `batch_id` |
| Window | `instrument`, `granularity`, `window_id`, `window_start`, `window_end` |
| Combo identity | `anchor`, `anchor2`, `continuation`, `continuation2`, `gap`, `indicator`, `direction`, `combo_type`, `pure_cont` |
| Simulation params | `timeout_bars`, `tp_mult`, `sl_mult` |
| IS metrics | `trade_count_is`, `mean_r_is`, `sqn_is`, `net_r_is`, `adjusted_score_is` |
| Gate outcome | `initial_bucket`, `passes_band`, `final_bucket`, `oos_eligible` |

Writes use a Postgres `SAVEPOINT`; failures roll back the batch and log non-fatally.

---

## run_id / batch_id telemetry

| ID | Scope | Purpose |
|----|-------|---------|
| `run_id` | One UUID per `main()` invocation | Correlates all instruments/windows in a single sweep run |
| `batch_id` | One UUID per instrument within that run | Groups IS rows for one pair’s full WFV pass |

Both are printed at sweep start (`Run ID: …`) and stored on every `sweep_is_results` row.

---

## Consequences

**Positive**

- Full IS audit trail per window/combo without re-simulation.
- Promotion scales with relative quality (80% band) instead of arbitrary top-5.
- WATCHLIST preserved for research without OOS cost.
- OOS path and ADR-077 cache unchanged for promoted combos only.

**Negative / operational**

- More IS rows written per run (all combos × all windows).
- `sweep_is_results` requires manual migration before persistence is effective.
- Leaderboard still aggregates OOS-only promoted paths — IS table is parallel telemetry, not a replacement for leaderboard CSV.

---

## Negative space

This ADR does **not**:

- Change OOS simulation, block hash, or `sweep_oos_cache` (ADR-077).
- Modify `detect_signals`, `sweep_simulation`, `build_combo_list`, or output writers.
- Add startup DDL in Python sweep code.
- Touch `candlelab-v2`, `pipshed`, or `fx_candles`.
- Auto-promote WATCHLIST combos to OOS on a future run.
- Replace the final leaderboard SQN floor (`SQN_MIN_TRADES = 10`).

---

## Related

- ADR-077 — Immutable OOS block cache (Part A)
- `scripts/sweep_validation_ftmo_m30.py` — `run_wfv`, `_write_is_results`
