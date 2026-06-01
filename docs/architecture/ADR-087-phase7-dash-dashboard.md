# ADR-087 — Phase 7: Sweep Results Dashboard (Dash)

**Date:** 2026-05-31
**Status:** Accepted
**Repo:** theonlykk/candlelab
**Author:** Khalid Khan (Lead Engineer)
**Approved by:** Gemini (Staff Architect)

---

## Context

Walk-forward sweep output lives in Postgres (`sweep_leaderboard`, `sweep_oos_cache`, `sweep_is_results`). Operators need read-only analytics during long-running sweeps without querying tables manually or exporting CSVs.

Equity curves must reflect **promoted windows only** (`bucket_label = 'PROMOTED'` on `sweep_is_results`). The SQN heatmap should surface trade-count confidence via variable cell opacity, and `meta_status` (GREEN / AMBER / RED) should gate visual emphasis on the leaderboard table.

---

## Decision

Mount a **Dash** application on the existing CandleLab Flask Railway service at `/dashboard/`:

| Component | Role |
|-----------|------|
| `dashboard/data.py` | `ThreadedConnectionPool` (minconn=1, maxconn=3); `get_leaderboard()`, `get_oos_curve()`, `build_equity_curve()` |
| `dashboard/layout.py` | Header, filters, heatmap, DataTable, equity curve graph |
| `dashboard/callbacks.py` | Manual refresh → `dcc.Store`; in-memory filter + heatmap; DB hit only for OOS curve on row select |
| `dashboard/__init__.py` | `create_dash_app(server)` factory |
| `app.py` | `dash_app = create_dash_app(app)` after Flask init — existing routes untouched |

Dependencies: `dash>=2.17.0`, `plotly>=5.22.0` in `requirements.txt`.

---

## Consequences

| Aspect | Detail |
|--------|--------|
| Deployment | Single Railway service — no separate dashboard host |
| DB load | Pool capped at 3 connections; leaderboard loaded on manual refresh only (no polling) |
| OOS curve | One query per selected row; JOIN enforces `bucket_label = 'PROMOTED'` |
| Equity math | Cumulative **sum** of R multiples; NaN gap inserted between non-contiguous OOS windows |
| Writes | None — dashboard is read-only |
| Cache invalidation | Relies on sweep-side `IS_CACHE_VERSION` / `oos_cache_version` (ADR-087 Phase 4); dashboard does not mutate cache |

---

## Negative space

- No changes to `sweep_validation_engine.py`, `meta_sweep.py`, or `db_migrate.py`
- No new Flask routes modified or removed
- No `candlelab-core` or `oanda-trading` changes
- No scipy, QuantLib, ta-lib, or additional quant libraries

---

## Failure modes

- Missing `DATABASE_URL` → pool init fails loudly at import
- Empty leaderboard → table + heatmap show "No data — sweep may still be running."
- Empty OOS curve → chart annotation "No promoted windows found"
- Malformed `oos_r_list` per window → row skipped in `build_equity_curve`, sweep continues
