# ADR-008: Strategy trade detail (`/trades/<strategy_id>`) poll join and labels

## Status

Accepted

## Context

The CandleLab strategy trade blotter should align with the execution blotter: join `executor_poll_log` placed-signal context to each `trades` row, expose execution labels and replay metrics, and surface server-side “notable differences” without bloating the template with business rules.

## Decision

- **Python join (trade-first)**  
  Poll rows are loaded for the strategy instrument and a **time window** derived from the page’s `trades` rows. Placed signals are parsed from `strategies_evaluated`. Each trade is matched to **at most one** signal using the same priority as the blotter: **`poll_log_id` hard link**, then **minute-floor match on `signal_time` (fallback `opened_at`) vs poll `candle_time`**, then **±10 minutes on `opened_at` vs `candle_time`** (closest). This mirrors the blotter’s epoch behaviour: **post-migration `poll_log_id`**, legacy rows still tie via **signal time / window**.

- **Strategy name in `WHERE`**  
  The trades query uses **`strategy_name = %s` with the raw live name only** (no `CandleLab:` prefix), reflecting the **post-migration clean schema**.

- **Execution label taxonomy**  
  The four labels match the main blotter exactly: **`EXECUTED`**, **`Pre-refactor - fill data unavailable`**, **`Early system - data unavailable`**, **`SYSTEM_INTEGRITY_GAP`**.

- **Notable differences**  
  Computed **entirely in Python**; the template only renders strings/badges. **No modal / “typical units” check** — sizing is **dynamic** (`$100 / sl_pips / pip_val` capped at **75k**), so a modal units rule would produce **false positives**.

- **Slippage callout**  
  When execution is `EXECUTED`, append slippage text if **`|slippage| > 0.5` pips** (threshold to **revisit** as data accumulates). Other append rules: **`TIMEOUT_EXIT`**, **`oanda_units == 10000`** (“Fallback sizing used”).

- **Poll log window**  
  Bounds use each trade’s **`signal_time`**, falling back to **`opened_at`** when `signal_time` is NULL. Affected rows get **`bounds_note`**. The **`LIMIT 100`** trades cap is **unchanged**.

## Consequences

- Extra read load on `executor_poll_log` bounded by min/max signal window (±1 day padding).
- `next_bar_open` comes from a **self-join** on poll log (`+5 minutes`); missing join → **`replay_entry`** absent, UI shows **Pending** (never a fabricated `0.00` for unknown).
