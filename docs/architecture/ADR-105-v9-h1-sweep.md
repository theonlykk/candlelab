# ADR-105: V9 H1 Sweep Configuration

**Date:** 2026-06-07  
**Status:** Accepted

## Context

V8 M30 sweep produced zero GREEN candidates. 37 RED_THIN due to `timeout_bars` weight=5.0 destroying neighbourhoods in sparse universe. DeepSeek + Gemini audit confirmed: filters too intense, M30 statistically underpowered under strict V1.4.0 physics.

## Decision

Five changes for V9 H1 sweep:

1. `IS_CACHE_VERSION` v8→v9
2. H1 config updated to bar-count equivalent of M30 (24-week IS, 8-week OOS, 8-week step, 27 windows)
3. `sqn_min_trades` raised to 20
4. `STD_FLOOR` removed from `sqn100()`
5. `timeout_bars` weight lowered 5.0→0.5
6. Recency gate adapts to timeframe via `recency_lookback_windows` config key (H1=3, M30=5)

## Consequences

V9 will sweep H1 across 26 instruments, ~4.6 years of history. Same bar count per window as M30. Non-overlapping OOS windows. Recency gate covers ~24 weeks for H1.
