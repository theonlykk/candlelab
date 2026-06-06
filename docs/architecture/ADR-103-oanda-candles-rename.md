# ADR-103: oanda_candles Rename to oanda_candles_x

**Date:** 2026-06-05

**Status:** Accepted

## Context

The `oanda_candles` table was renamed to `oanda_candles_x` during the FTMO pivot to enforce semantic separation between OANDA retail feed and FTMO ECN data. Application code was not updated, causing live INSERT failures in the Railway poll pipeline.

## Decision

Update four SQL string literals in `data.py`, `strategy_runner.py`, `app.py`, and `verify_parity.py` to reference `oanda_candles_x` directly. No views, no abstraction layer.

Old sweep scripts deliberately left referencing the old name — superseded and not actively running on Railway.

## Consequences

- Railway poll pipeline resumes writing to `oanda_candles_x`.
- Live regime gate computation in dashboard will read from `oanda_candles_x`.
- Any future code must explicitly reference `oanda_candles_x`.
