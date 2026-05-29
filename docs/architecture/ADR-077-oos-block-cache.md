# ADR-077 — Immutable OOS Block Cache (Part A)

**Date:** 2026-05-28
**Status:** Accepted
**Repo:** theonlykk/candlelab
**Author:** Khalid Khan (Lead Engineer)

---

## Context

Walk-forward validation (WFV) in `scripts/sweep_validation_ftmo_m30.py` recomputes out-of-sample (OOS) simulation blocks on every sweep run. Identical OOS windows (same instrument, granularity, date range, combo config, and simulation parameters) can be evaluated repeatedly across weekly sweeps. That redundant work is expensive and can introduce week-to-week variance in reported OOS metrics when underlying data or floating-point paths differ slightly between runs.

Part A introduces a Postgres-backed, hash-keyed cache for OOS evaluation blocks only. In-sample (IS) promotion logic is unchanged. The cache table is created via manual migration — no DDL in the sweep script.

## Decision

Before computing any promoted-combo OOS block in `run_wfv`, the engine:

1. Builds a deterministic SHA-256 `block_hash` from all inputs that define the OOS evaluation.
2. Looks up `sweep_oos_cache` by `block_hash`. On hit, loads `result_json` and skips `detect_signals`, `_apply_indicator_filter`, and `sweep_simulation`.
3. On miss, runs the original OOS path unchanged, then stores the result with `ON CONFLICT (block_hash) DO NOTHING`.

Cache failures (missing table, connection errors, store conflicts) are non-fatal: lookup errors fall through to full computation; store errors log and continue.

## Block hash specification

The hash is `SHA-256(JSON payload))` where the payload object uses `json.dumps(..., sort_keys=True, default=str)` and UTF-8 encoding of the JSON string.

| Field | Source | Notes |
|-------|--------|--------|
| `instrument` | WFV instrument | e.g. `EUR_USD` |
| `granularity` | `GRANULARITY` constant | `M30` |
| `oos_start` | OOS window start | `pd.Timestamp.isoformat()` |
| `oos_end` | OOS window end | `pd.Timestamp.isoformat()` |
| `combo` | Promoted combo dict | Keys sorted; float values rounded to 6 decimal places |
| `timeout` | MA/timeout loop | Integer bars |
| `tp_mult` | `TP_MULT` | Rounded to 6 dp |
| `sl_mult` | `SL_MULT` | Rounded to 6 dp |
| `sl_mode` | Pair config | e.g. `standard` |

`config_hash` stored in the table is the first 16 hex characters of `block_hash` (denormalized shorthand for browsing).

Cached `result_json` shape:

```json
{
  "oos_r_list": [<float>, ...],
  "oos_n_trades": <int>,
  "oos_mean_r": <float>,
  "oos_sqn100": <float>
}
```

## Consequences

**Positive**

- Repeat sweeps reuse identical OOS R-multiple lists without re-running simulation.
- Results for a given block are stable across runs once cached.
- IS path and untouched helpers (`fetch_instrument_data`, `detect_signals`, `sweep_simulation`, etc.) remain unchanged.

**Negative / operational**

- Manual migration required before cache is effective (`sweep_oos_cache` table + index).
- Cache hits skip trade-level `equity_after` collection; MDD/Sharpe from OOS equity curves may differ on hit vs first compute until recomputed without cache.
- Stale cache entries persist until invalidated manually if combo logic or simulation rules change without hash input changes.

## Negative space

- No DDL in Python sweep code.
- No changes to IS computation, combo building, or output writers.
- No changes to `candlelab-v2`, `pipshed`, or `fx_candles`.

## Migration (manual)

```sql
CREATE TABLE IF NOT EXISTS sweep_oos_cache (
    id SERIAL PRIMARY KEY,
    block_hash TEXT NOT NULL UNIQUE,
    instrument TEXT NOT NULL,
    granularity TEXT NOT NULL,
    oos_start TIMESTAMPTZ NOT NULL,
    oos_end TIMESTAMPTZ NOT NULL,
    config_hash TEXT NOT NULL,
    signal_count INTEGER,
    mean_r NUMERIC,
    sqn100 NUMERIC,
    result_json JSONB,
    computed_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sweep_oos_cache_hash
    ON sweep_oos_cache(block_hash);
```
