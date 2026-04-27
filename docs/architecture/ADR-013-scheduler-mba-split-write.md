# ADR-013: OANDA `price=MBA` fetch and split write to `oanda_candles`

## Status

Accepted (implemented in `data.py`)

## Context

Historical OHLC in `ohlc_*` tables used mid-only OANDA candles (`price=M`). The product needs **bid and ask** OHLC for richer analytics while **keeping** existing `ohlc_*` consumers (e.g. chart backtests) on **mid-only** columns without breaking their schema or call sites.

## Decision

- **`_fetch_oanda`** requests **`price=MBA`** in the REST `params` dict — **one** OANDA candles GET per pagination batch (same temporal window as before), so there is **no extra temporal drift** between mid and bid/ask series.
- **`_candles_to_df`** parses **`mid`**, **`bid`**, and **`ask`** objects on each complete candle. **Mid** supplies `open` / `high` / `low` / `close`; **bid** supplies `bid_open` / `bid_close` from `bid["o"]` and `bid["c"]`; **ask** supplies `ask_open` / `ask_close` from `ask["o"]` and `ask["c"]`; **volume** unchanged. Missing **`mid`**, **`bid`**, or **`ask`** on a candle raises **`KeyError`** (no silent `0.0` fill). Incomplete candles (`complete` false) are **skipped** only.
- **`insert_oanda_candles(instrument, df)`** bulk-writes to **`oanda_candles`** using **`psycopg2.extras.execute_values`** with **`ON CONFLICT (instrument, time) DO UPDATE SET`** on all non-key price/volume columns so **restatements** overwrite prior rows safely.
- **`backfill_instrument`** performs a **split write** after each successful `_fetch_oanda`: full MBA **`df`** (or gap-filtered **`new_bars`**) to **`insert_oanda_candles`**, then **`df[["open","high","low","close","volume"]]`** to existing **`insert_bars`** so **`ohlc_*`** tables remain **mid-only** for legacy chart rendering and downstream loaders.
- **`insert_bars`**, **`get_ohlc`**, **`load_bars`**, **`latest_ts`** — **unchanged** in this ADR.

## Consequences

- Callers of `_fetch_oanda` / `_candles_to_df` must tolerate **strict** MBA payloads; malformed candles fail fast with **`KeyError`** instead of silent drops.
- **`oanda_candles`** grows with full MBA rows; **`ohlc_*`** stay schema-compatible with historical behaviour.
