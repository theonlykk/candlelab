# ADR-022: Wizard Execution Physics Parity

- Status: Accepted
- Date: 2026-04-28

## Context

The wizard backtest path used M5 EWM ATR (span=14) with an entry-price SL/TP anchor, while the executor path used H1 Wilder RMA ATR with a signal-close anchor. This mismatch created a simulation integrity breach between what the wizard reported and what live execution physics enforced.

Staff Architect review also identified a risk sizing bug in wizard simulation: `sl_dist` omitted the `sl_mult` factor in position sizing math.

## Decision

Option B is adopted:

- Add per-bar H1 ATR Series support in `indicator_utils.py` via `compute_h1_atr_series_from_m5`.
- Update wizard simulation to use signal-close (`sig_close`) SL/TP anchoring in `_simulate_trades`.
- Correct risk sizing input math to `sl_dist = atr_val * sl_mult`.
- Standardize incomplete-bar handling by always dropping the latest H1 bar via `iloc[:-1]` after `dropna()`.
- Delegate `strategy_runner._get_h1_atr` to `indicator_utils.compute_h1_atr_series_from_m5` for shared ATR behavior.

## Consequences

- Wizard backtest geometry now matches executor geometry for ATR basis and anchor physics.
- `indicator_utils.py` remains a cross-repo sync file and must be manually copied to `oanda-trading` after production verification.
- Option C (retire `_simulate_trades`) is deferred to a future ADR when Bid/Ask spread physics are unified end-to-end.
