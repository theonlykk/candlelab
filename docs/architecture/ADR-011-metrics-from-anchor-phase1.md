# ADR-011: Metrics anchor migration — Phase 1 (server authority, no engine change)

## Status

Accepted (Phase 1 implemented)

## Context

Strategy card metrics previously trusted **client-sent** timestamps (`saved_at` for 30d backtest, `go_live_at` for executor replay and true P&L). Temporal authority must move to **server-side** sources so the UI cannot skew windows. The `metrics_from` column on `candlelab_strategies_live` is populated for active strategies; true P&L should reflect **realized OANDA dollars** (`oanda_pl`) for closed trades in the new epoch, not legacy pip aggregates.

## Decision (Phase 1)

- **Anchor swap only** — no changes to `_backtest_pattern`, `_executor_compute_from_dataframe`, `_executor_read_continuous_series`, or any simulation math.
- **`/api/strategy-pnl`** — **30-day window** computed server-side as `datetime.now(timezone.utc) - timedelta(days=30)`. **No** `saved_at` in the request body. **Pandas guard:** if `df.index` is timezone-naive, compare using a **naive** cutoff (`cutoff.replace(tzinfo=None)`); if the index is timezone-aware, use `pd.Timestamp(cutoff)` so mixed naive/aware comparisons do not raise.
- **`/api/strategy/executor-pnl`**, **`/api/strategy/true-pnl`**, and **`/api/strategy/oanda-fills`** — **No** client `go_live_at` for temporal filtering. A precursor query loads **`metrics_from` and `go_live_at`** from `candlelab_strategies_live` (`WHERE strategy_name = %s AND closed_at IS NULL ORDER BY id DESC LIMIT 1`). The anchor timestamp is **`metrics_from` if non-NULL, else `go_live_at`** from the same row. Executor and oanda-fills require **`strategy_name`** (replacing the previous `go_live_at` requirement for those flows).
- **`/api/strategy/oanda-fills`** — Postgres `trades` load uses **`opened_at >= anchor_ts`** (strict server anchor). The OANDA REST **`from`** parameter uses **`anchor_ts - timedelta(minutes=5)`** as a clock-drift buffer so broker transaction polling is not cut off at the exact anchor instant; the buffer is **not** applied to the DB query.
- **`/api/strategy/true-pnl`** — Query `trades` with **`strategy_name = %s` only** (no `CandleLab:` OR fallback). Filter: `opened_at >= anchor`, **`status = 'CLOSED'`**, **`oanda_pl IS NOT NULL`**. Aggregate **`cum_net`** as the **sum of `oanda_pl` in dollars**, rounded to **two** decimal places. **Empty result set:** return **`cum_net: 0.00`** (and zero signals), never `None` from an unhandled aggregate.
- **Templates** — unchanged in this phase (clients may still send legacy fields such as `go_live_at`; the server ignores them for these metrics endpoints).

## Phase 2 (mid-week, out of scope here)

- Deploy the **new simulation engine** once enough post-epoch trades exist for validation.
- Engine will live in **`simulation_engine.py`**, not inline in `app.py`.
- **Redis TTL 300 seconds** aligns with M5 cadence — no manual purge required.

## Consequences

- Strategy cards must send **`strategy_name`** for executor PnL, true PnL, and oanda-fills matching, and rely on DB state for anchors; orphaned requests without a matching open live row receive **400**.
- True P&L reflects **closed trades with OANDA P&L populated**; strategies without `oanda_pl` rows show **$0.00** until fills are written.
