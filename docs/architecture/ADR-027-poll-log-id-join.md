# ADR-027: Poll log ID join key + nearest-neighbor signal anchor

- Status: Accepted
- Date: 2026-04-28

## Context

`executor_poll_log.candle_time` is sometimes the signal candle (N) and sometimes the entry candle (N+1) due to variable polling latency. That made timestamp-based joins non-deterministic. Minute-string joins also failed for 0-offset cases. In simulation, `sig_close` could be computed from the N+1 close (future data) when `candle_time` happened to align with N+1 — a look-ahead bias.

## Decision

- Use `poll_log_id` (canonical integer FK from `executor_poll_log`) as the primary join between simulation results and OANDA trades wherever both sides expose it.
- In `_build_since_live_simulation`, anchor the signal on the **most recent closed candle at or before** the poll timestamp (nearest-neighbor on the candle index) so `sig_close` always comes from candle N regardless of whether the stored poll timestamp is N or N+1.
- Thread `poll_log_id` through the signal dict (`strategy_runner`), every `run_simulation` result (`simulation_engine`), and `_build_trades_detail_rows` (`app`) for aggressive/OANDA matching.
- Retain `_mk()` minute-string matching as fallback for OANDA trades without `poll_log_id`.

## Consequences

- Theo/OANDA join is an exact integer FK match when IDs are present; aggregate strategy card P&L and `_aggregate_sim` behavior are unaffected.
- Look-ahead bias from misaligned closes is eliminated for the since-live path.
- Geometry or filter rejections still appear with Theo rows and blank OANDA columns where no broker trade exists.
