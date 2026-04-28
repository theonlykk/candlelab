# ADR-021: Backend state supremacy for `/api/strategy-pnl`

## Status

Accepted

## Date

2026-04-28

## Context

The My Strategies panel builds a rich JSON payload in `fetchStratPnl()` including `anchor`, `complement`, `connector`, and multipliers. That client-side wiring silently dropped the continuation pattern for Type 4 strategies and could send connector semantics inconsistent with stored live strategy state (e.g. `any-order` vs executor Type 4). Together with mismatched TP/timeout fields from the payload vs `candlelab_strategies_live`, users observed large divergences (for example ~50 signals on the wizard `/api/finalise` path vs ~292 on the live card 30d backtest for the same named strategy).

## Decision

- **`/api/strategy-pnl`** accepts **only `strategy_name`** in the POST body (plus whatever the client still sends; only `strategy_name` is read).
- **`_hydrate_strategy_config(strategy_name)`** loads the **latest active** row from **`candlelab_strategies_live`** (`closed_at IS NULL`) and maps columns to execution parameters.
- **`run_30d_backtest`** gains an optional **`continuation`** argument; **`continuation_col`** is resolved via **`_resolve_col`** (same as complement). **`detect_signal`** is invoked with **`continuation=continuation_col`** so Type 4 detection matches **`signal_engine`** behaviour when pattern_2 + continuation + connector are stored in the DB.
- **`/api/finalise`**, **`_backtest_pattern`**, **`_simulate_trades`**, **`simulation_engine`**, **`signal_engine`**, **`indicator_utils`**, **`time_utils`**, and **`position_utils`** are unchanged.

## Consequences

- Payload mismatch bugs between the SPA and canonical DB state are eliminated for the 30d card: **`candlelab_strategies_live`** is the single source of truth for simulation inputs.
- The frontend may continue sending extra keys for compatibility; the server ignores them for this route.
- Operators should deploy backend before relying on corrected counts; clients already send **`strategy_name`**.
