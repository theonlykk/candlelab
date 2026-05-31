# ADR-086 — bucket_label Engine Patch (Phase 7 Pre-Requisite)

**Date:** 2026-05-30
**Status:** Accepted
**Repo:** theonlykk/candlelab
**Author:** Khalid Khan (Lead Engineer)
**Approved by:** Gemini (Staff Architect)

---

## Context

Phase 7 Dash dashboard will display IS gate outcomes and equity curves per combo bucket (PROMOTED / WATCHLIST / REJECTED).

**Red Team Finding 1 (Phase 7):** If the dashboard recomputes bucket labels from current `TIMEFRAME_CONFIGS` thresholds, historical rows will silently mis-classify when configs drift. Equity curves grouped by bucket become wrong without any error signal.

---

## Decision

Write `bucket_label` to `sweep_is_results` at sweep time in `_write_is_results()`. The Phase 7 dashboard reads stored labels — it never recomputes them.

### DDL (manual — not in app startup)

```sql
ALTER TABLE sweep_is_results
ADD COLUMN IF NOT EXISTS bucket_label VARCHAR(50);
```

User must execute against Railway Postgres before the overnight M30 sweep.

### Label logic (cfg-driven)

Computed in `_compute_bucket_label()` using existing cfg keys:

| Label | Condition |
|-------|-----------|
| `PROMOTED` | `n_trades >= min_trades_eligible` AND `sqn100 >= sqn_min_trades_is` |
| `WATCHLIST` | `n_trades >= min_trades_watchlist` |
| `REJECTED` | otherwise |

- `NULL` when `n_trades` or `sqn100` is missing or non-finite — no fabricated default.
- Piggybacks on existing INSERT to `sweep_is_results` — no separate write path.

---

## Consequences

| Aspect | Detail |
|--------|--------|
| Immutability | Historical bucket labels frozen at sweep time |
| Config drift | Dashboard safe — reads stored state, not live cfg |
| Cache hits | Cached IS rows skip write (unchanged ADR-080 behavior) |
| Missing DDL | DB error on INSERT — user must run ALTER TABLE first |

---

## Negative space

- No changes to IS gate threshold values
- No changes to sweep_leaderboard, sweep_oos_cache, Phase 6 currency matrix
- No `db_migrate.py` changes — DDL is manual
- No dashboard code in this ADR
