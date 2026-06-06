# ADR-104: Dashboard Redesign — Institutional Screengrab Layout

**Date:** 2026-06-06  
**Status:** Accepted  

## Context

ADR-101 introduced new status labels (RED_STALE, RED_WEAK_SQN) and 
promoted_window_indices JSONB. The existing dashboard did not reflect 
these changes. Operator brief: screengrab-ready for institutional 
counterparts (vol traders, macro PMs, quant funds).

Two fatal flaws caught by Gemini in the initial prompt:
1. N+1 query loop — get_live_regime_gates() was called per candidate,
   firing 3 SQL queries per call. With 15 GBP_JPY candidates this 
   would trigger 45 redundant queries for identical candle data.
2. Hardcoded SAFE thresholds presented without disclaimer — static 
   ADX/BBW defaults masquerading as IS-calibrated gate values.

## Decision

Four files changed across three sequential Cursor prompts:

**dashboard/data.py:**
- get_leaderboard() fetches neighborhood_quality_score, sqn_gap,
  promoted_window_indices. ORDER BY puts GREEN rows first.
- get_candles_batch(instruments) — fetches M30/H4/D1 for a list of 
  instruments using ANY(%s). One DB round trip per timeframe regardless
  of candidate count. N+1 loop eliminated.
- compute_regime_gates(candles, direction, combo_type) — accepts 
  pre-fetched DataFrames only. Zero DB calls inside. Computes BBW, ADX,
  MA_200, H4 EMA50. Z-spread deferred to Market State Daemon (ADR-105).

**dashboard/sparkline.py (new):**
- 27-cell barcode SVG. Promoted windows = dark fill. Recent windows 
  (last 5) highlighted green if promoted. Recency zone shaded blue.

**dashboard/layout.py:**
- Full replacement. Six status labels with distinct colour hierarchy.
- regime_panel() — per-GREEN-candidate gate dot matrix (BBW, ADX, 
  MA 200, H4 EMA, Z-Spread).
- Explicit SAFE heuristic disclaimer (amber left border):
  "Note: Live Regime Gates currently evaluate against static SAFE 
  heuristics (e.g. ADX > 25). Dynamic, IS-calibrated thresholds will 
  populate post-V9 migration."

**dashboard/callbacks.py:**
- Full replacement. get_candles_batch() called once before candidate 
  loop. compute_regime_gates() called per candidate using cached 
  DataFrames. render_dashboard returns five outputs.

## Consequences

- No N+1 query loop. Dashboard scales to any number of GREEN candidates.
- All six status labels visible with distinct colours.
- Barcode sparkline shows window-by-window promotion history with 
  recency zone highlighted.
- Live regime gates computed from oanda_candles_x (OANDA retail feed,
  updated continuously by Railway poll cycle).
- Z-spread shows PENDING until Market State Daemon built (ADR-105).
- IS-calibrated thresholds available post-V9 sweep_window_thresholds 
  migration (ADR-105).
- SAFE disclaimer visible to all users — no fabricated data presented
  as calibrated.
