"""
backtest.py — ATR-based trade outcome engine
Entry: open of next candle after signal
TP: +3 ATR, SL: -1 ATR (direction-adjusted)
Timeout: exit at close of candle 5 if TP/SL not hit
Win = TP hit before SL; Loss = SL hit first OR timeout close worse than entry

P&L assumes $100 risk per trade (1R = $100).
Spread cost = 1 pip deducted per trade (instrument-adjusted).
"""

import numpy as np
import pandas as pd
from patterns import detect_all

ATR_PERIOD   = 14
TP_MULT      = 3.0
SL_MULT      = 1.0
TIMEOUT      = 5        # candles
RISK_DOLLARS = 100.0    # $ per trade (1R)
SPREAD_PIPS  = 1.0      # pips deducted per trade as transaction cost

# P&L in R-multiples per outcome (gross, before spread)
OUTCOME_R = {
    "win":          +3.0,
    "loss":         -1.0,
    "timeout_win":  +0.5,
    "timeout_loss": -0.3,
}


def compute_atr(df: pd.DataFrame, period: int = ATR_PERIOD) -> pd.Series:
    hi, lo, cl = df["high"], df["low"], df["close"]
    prev_cl = cl.shift(1)
    tr = pd.concat([
        hi - lo,
        (hi - prev_cl).abs(),
        (lo - prev_cl).abs()
    ], axis=1).max(axis=1)
    return tr.ewm(span=period, adjust=False).mean()


def _simulate_trade(entry: float, direction: int, atr: float,
                    future_high: np.ndarray, future_low: np.ndarray,
                    future_close: np.ndarray) -> str:
    tp = entry + direction * TP_MULT * atr
    sl = entry - direction * SL_MULT * atr

    if direction == 1:
        tp_hits = np.where(future_high >= tp)[0]
        sl_hits = np.where(future_low  <= sl)[0]
    else:
        tp_hits = np.where(future_low  <= tp)[0]
        sl_hits = np.where(future_high >= sl)[0]

    n       = len(future_high)
    tp_idx  = int(tp_hits[0]) if len(tp_hits) else n
    sl_idx  = int(sl_hits[0]) if len(sl_hits) else n

    if tp_idx <= sl_idx and tp_idx < n:
        return "win"
    if sl_idx < tp_idx and sl_idx < n:
        return "loss"

    exit_price = future_close[-1]
    pnl = direction * (exit_price - entry)
    return "timeout_win" if pnl > 0 else "timeout_loss"


def _spread_cost(atr: float, pip: float) -> float:
    """
    Spread cost in dollars.
    spread_pips / (SL in pips) × RISK_DOLLARS
    SL in pips = (SL_MULT × atr) / pip
    """
    sl_pips = (SL_MULT * atr) / pip if pip > 0 else 1.0
    if sl_pips <= 0:
        return 0.0
    return (SPREAD_PIPS / sl_pips) * RISK_DOLLARS


def run_backtest(df: pd.DataFrame, pip: float = 0.0001, timeout: int = TIMEOUT) -> dict:
    """
    Run backtest over all patterns on df.
    pip: instrument pip size (e.g. 0.0001 for EUR/USD, 0.01 for JPY)
    timeout: number of candles before forced exit
    Returns dict: pattern_name -> {signals, wins, losses, win_pct, pnl_gross, pnl_net, trades}
    """
    atr     = compute_atr(df)
    signals = detect_all(df)
    results = {}

    for pattern_name in signals.columns:
        sig_series = signals[pattern_name]
        trades = []

        for i in range(len(df) - timeout - 1):
            sig = sig_series.iloc[i]
            if sig == 0:
                continue

            entry_bar = df.iloc[i + 1]
            entry     = entry_bar["open"]
            trade_atr = atr.iloc[i]
            direction = int(sig)

            if trade_atr == 0 or np.isnan(trade_atr):
                continue

            future = df.iloc[i + 1: i + 1 + timeout]
            if len(future) == 0:
                continue

            outcome      = _simulate_trade(entry, direction, trade_atr,
                                           future["high"].to_numpy(),
                                           future["low"].to_numpy(),
                                           future["close"].to_numpy())
            is_win       = outcome in ("win", "timeout_win")
            r_mult       = OUTCOME_R[outcome]
            pnl_gross    = round(r_mult * RISK_DOLLARS, 2)
            spread_cost  = round(_spread_cost(trade_atr, pip), 2)
            pnl_net      = round(pnl_gross - spread_cost, 2)

            trades.append({
                "ts":          df.index[i],
                "signal":      direction,
                "entry":       entry,
                "atr":         trade_atr,
                "tp":          entry + direction * TP_MULT * trade_atr,
                "sl":          entry - direction * SL_MULT * trade_atr,
                "outcome":     outcome,
                "win":         is_win,
                "r_mult":      r_mult,
                "pnl_gross":   pnl_gross,
                "spread_cost": spread_cost,
                "pnl_net":     pnl_net,
            })

        wins      = sum(1 for t in trades if t["win"])
        tp_hits   = sum(1 for t in trades if t["outcome"] == "win")
        total     = len(trades)
        cum_gross = round(sum(t["pnl_gross"] for t in trades), 2)
        cum_net   = round(sum(t["pnl_net"]   for t in trades), 2)

        results[pattern_name] = {
            "signals":   total,
            "wins":      wins,
            "tp_hits":   tp_hits,
            "losses":    total - wins,
            "win_pct":   round(wins / total * 100, 1) if total else 0.0,
            "cum_gross": cum_gross,
            "cum_net":   cum_net,
            "trades":    trades,
        }

    return results


def leaderboard(results: dict) -> list[dict]:
    """Return list sorted by cum_net descending."""
    rows = []
    for name, r in results.items():
        rows.append({
            "pattern":   name,
            "signals":   r["signals"],
            "wins":      r["wins"],
            "losses":    r["losses"],
            "win_pct":   r["win_pct"],
            "cum_gross": r["cum_gross"],
            "cum_net":   r["cum_net"],
            "tp_hits":   r["tp_hits"],
        })
    return sorted(rows, key=lambda x: x["cum_net"], reverse=True)
