# ADR-017: `strategy_runner` coordinator and executor API keys

## Status

Accepted (implemented)

## Context

Live and 30d strategy metrics mixed OHLC sources, executor poll logic, and backtest-style pattern math inside Flask routes. Phase 2 introduces `simulation_engine.py` as a pure execution physics layer; a coordinator is required to load candles, derive signals, and call the engine without bloating `app.py`.

## Decision

- **`strategy_runner.py`** is the **service layer**: PostgreSQL reads (`oanda_candles`, `executor_poll_log`), `detect_all` / `detect_signal`, `indicator_utils` parsing/filters, H1 ATR resample from M5 `oanda_candles`, and **`run_simulation`** invocations. **No Flask** imports.
- **Flask routes** (`/api/strategy-pnl`, `/api/strategy/executor-pnl`) remain **thin HTTP translators**: parse JSON, resolve `instrument_label`, fetch **`anchor_ts`** via existing **`_fetch_metrics_anchor_ts`**, delegate to **`run_30d_backtest`** / **`run_since_live`**, return JSON. **`_fetch_true_pnl`** on the executor route is **unchanged**.
- **`anchor_ts`** is **injected by the route** into **`run_since_live`** so `strategy_runner` does not import `app` (no circular imports).
- **`run_since_live`** accepts **no** anchor/complement/connector — placed signals come **only** from **`executor_poll_log.strategies_evaluated`** (**poll log supremacy** enforced by the function signature).
- **Executor JSON** keys **`aggressive`** and **`passive`** replace **`raw`**, **`all`**, and **`clean`**; optional **`true_pnl`** object accompanies them (distinct from `/api/strategy/true-pnl` card fetch).
- **H1 ATR(14)** uses the same Wilder-style **`ewm(alpha=1/period)`** construction as **`_compute_atr`** in this module, on **M5 → 1h** resampled mid OHLC. **`sl_dist = max(5 * pip, h1_atr_value)`** with **`asof`** fallback to the latest prior bar when the floored hour is missing; **never** returns **0** or **NaN** (floor **`5 * pip`**).
- **UI** labels: **Theo P&L · aggressive** / **Theo P&L · passive**; the former “clean only” row is **removed**.

## Consequences

- **`oanda_candles`** and **`executor_poll_log`** must be populated for new metrics to be non-empty; missing data yields zero-signal aggregates, not synthetic fills.
- **`cum_net`** in runner aggregates is **sum of theoretical `pnl_pips`** (not legacy dollar `cum_net` from `_backtest_pattern`).
