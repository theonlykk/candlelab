"""
simulation_engine.py — Execution physics engine for CandleLab Phase 2.
ADR-014: thin engine, no pattern detection, no DB, no strategy config.

Signal schema (list of dicts):
    signal_time  pd.Timestamp UTC
    direction    'BUY' or 'SELL'
    sl           absolute stop loss price
    tp           absolute take profit price
    sl_dist      optional stop distance in price (required for dollar P&L sizing)

oanda_candles DataFrame (indexed by UTC DatetimeIndex):
    open, high, low, close      mid OHLC
    bid_open, bid_close         bid at bar open/close
    ask_open, ask_close         ask at bar open/close

No scipy, no QuantLib, no ta-lib.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from position_utils import INST_CONFIG, calculate_position_units

log = logging.getLogger(__name__)

PIP_MAP = {"USD_JPY": 0.01, "EUR_JPY": 0.01, "GBP_JPY": 0.01}
DEFAULT_PIP = 0.0001


def get_pip(instrument: str) -> float:
    return PIP_MAP.get(instrument.upper(), DEFAULT_PIP)


def _next_open_exit(
    arrays: dict[str, Any],
    candle_index: pd.DatetimeIndex,
    bar_idx: int,
    direction: str,
) -> tuple[float, int]:
    """BUY exits at bid_open of bar_idx+1; SELL at ask_open. OOB → bid_close / ask_close of bar_idx."""
    n = len(candle_index)
    d = str(direction).upper()
    if d == "BUY":
        if bar_idx + 1 < n:
            return float(arrays["bid_open"][bar_idx + 1]), bar_idx + 1
        return float(arrays["bid_close"][bar_idx]), bar_idx
    if bar_idx + 1 < n:
        return float(arrays["ask_open"][bar_idx + 1]), bar_idx + 1
    return float(arrays["ask_close"][bar_idx]), bar_idx


def _pnl_pips(entry: float, exit_price: float, direction: str, pip: float) -> float:
    d = str(direction).upper()
    if d == "BUY":
        return round((exit_price - entry) / pip, 1)
    return round((entry - exit_price) / pip, 1)


def _timeout_result_class(entry: float, exit_price: float, direction: str, pip: float) -> tuple[str, str]:
    pips = _pnl_pips(entry, exit_price, direction, pip)
    if pips > 0:
        return "WIN", "TIMEOUT"
    if pips < 0:
        return "LOSS", "TIMEOUT"
    return "BREAKEVEN", "TIMEOUT"


def _scan_tp_sl_timeout(
    arrays: dict[str, Any],
    candle_index: pd.DatetimeIndex,
    entry: float,
    entry_time: pd.Timestamp,
    fill_idx: int,
    sig_idx: int,
    sl: float,
    tp: float,
    direction: str,
    timeout_bars: int,
    pip: float,
) -> dict[str, Any]:
    n = len(candle_index)
    d = str(direction).upper()
    bars_remaining = timeout_bars - (fill_idx - sig_idx)

    if bars_remaining <= 0:
        exit_price, exit_idx = _next_open_exit(arrays, candle_index, fill_idx, direction)
        exit_time = candle_index[exit_idx]
        res, ex_reason = _timeout_result_class(entry, exit_price, direction, pip)
        return {
            "entry": entry,
            "exit_price": exit_price,
            "sl": sl,
            "tp": tp,
            "pnl_pips": _pnl_pips(entry, exit_price, direction, pip),
            "result": res,
            "exit_reason": ex_reason,
            "entry_time": entry_time,
            "exit_time": exit_time,
        }

    last_valid_bar_idx: int | None = None
    for j in range(1, bars_remaining + 1):
        bar_idx = fill_idx + j
        if bar_idx >= n:
            break
        last_valid_bar_idx = bar_idx

        if d == "BUY":
            if arrays["high"][bar_idx] >= tp:
                exit_time = candle_index[bar_idx]
                return {
                    "entry": entry,
                    "exit_price": float(tp),
                    "sl": sl,
                    "tp": tp,
                    "pnl_pips": _pnl_pips(entry, float(tp), direction, pip),
                    "result": "WIN",
                    "exit_reason": "TP",
                    "entry_time": entry_time,
                    "exit_time": exit_time,
                }
            if arrays["close"][bar_idx] <= sl:
                exit_price, exit_idx = _next_open_exit(arrays, candle_index, bar_idx, direction)
                exit_time = candle_index[exit_idx]
                return {
                    "entry": entry,
                    "exit_price": exit_price,
                    "sl": sl,
                    "tp": tp,
                    "pnl_pips": _pnl_pips(entry, exit_price, direction, pip),
                    "result": "LOSS",
                    "exit_reason": "SL",
                    "entry_time": entry_time,
                    "exit_time": exit_time,
                }
            if j == bars_remaining:
                exit_price, exit_idx = _next_open_exit(arrays, candle_index, bar_idx, direction)
                exit_time = candle_index[exit_idx]
                res, ex_reason = _timeout_result_class(entry, exit_price, direction, pip)
                return {
                    "entry": entry,
                    "exit_price": exit_price,
                    "sl": sl,
                    "tp": tp,
                    "pnl_pips": _pnl_pips(entry, exit_price, direction, pip),
                    "result": res,
                    "exit_reason": ex_reason,
                    "entry_time": entry_time,
                    "exit_time": exit_time,
                }
        else:
            if arrays["low"][bar_idx] <= tp:
                exit_time = candle_index[bar_idx]
                return {
                    "entry": entry,
                    "exit_price": float(tp),
                    "sl": sl,
                    "tp": tp,
                    "pnl_pips": _pnl_pips(entry, float(tp), direction, pip),
                    "result": "WIN",
                    "exit_reason": "TP",
                    "entry_time": entry_time,
                    "exit_time": exit_time,
                }
            if arrays["close"][bar_idx] >= sl:
                exit_price, exit_idx = _next_open_exit(arrays, candle_index, bar_idx, direction)
                exit_time = candle_index[exit_idx]
                return {
                    "entry": entry,
                    "exit_price": exit_price,
                    "sl": sl,
                    "tp": tp,
                    "pnl_pips": _pnl_pips(entry, exit_price, direction, pip),
                    "result": "LOSS",
                    "exit_reason": "SL",
                    "entry_time": entry_time,
                    "exit_time": exit_time,
                }
            if j == bars_remaining:
                exit_price, exit_idx = _next_open_exit(arrays, candle_index, bar_idx, direction)
                exit_time = candle_index[exit_idx]
                res, ex_reason = _timeout_result_class(entry, exit_price, direction, pip)
                return {
                    "entry": entry,
                    "exit_price": exit_price,
                    "sl": sl,
                    "tp": tp,
                    "pnl_pips": _pnl_pips(entry, exit_price, direction, pip),
                    "result": res,
                    "exit_reason": ex_reason,
                    "entry_time": entry_time,
                    "exit_time": exit_time,
                }

    if last_valid_bar_idx is None:
        exit_price, exit_idx = _next_open_exit(arrays, candle_index, fill_idx, direction)
    else:
        exit_price, exit_idx = _next_open_exit(arrays, candle_index, last_valid_bar_idx, direction)
    exit_time = candle_index[exit_idx]
    res, ex_reason = _timeout_result_class(entry, exit_price, direction, pip)
    return {
        "entry": entry,
        "exit_price": exit_price,
        "sl": sl,
        "tp": tp,
        "pnl_pips": _pnl_pips(entry, exit_price, direction, pip),
        "result": res,
        "exit_reason": ex_reason,
        "entry_time": entry_time,
        "exit_time": exit_time,
    }


def _inst_config_for_symbol(instrument: str) -> dict:
    """Map OANDA code (e.g. EUR_USD) or slash label to ``INST_CONFIG`` entry."""
    if not instrument:
        return {}
    u = str(instrument).strip().upper()
    if u in INST_CONFIG:
        return INST_CONFIG[u]
    if "_" in u:
        a, b = u.split("_", 1)
        k = f"{a}/{b}"
        return INST_CONFIG.get(k, {})
    return {}


def _compute_pnl_dollars(
    sig: dict,
    out: dict,
    direction: str,
    pip: float,
    pip_val: float,
) -> float | None:
    if out.get("pnl_pips") is None:
        return None
    if "sl_dist" not in sig:
        log.warning(
            "simulation_engine: signal missing sl_dist, using DEFAULT_UNITS for position sizing"
        )
    sl_dist_sig = float(sig["sl_dist"]) if sig.get("sl_dist") is not None else 0.0
    units = calculate_position_units(direction, sl_dist_sig, pip, pip_val)
    return round(float(out["pnl_pips"]) * pip_val * abs(units) / 100_000.0, 2)


def _signal_idx(candle_index: pd.DatetimeIndex, signal_time: pd.Timestamp) -> int:
    ts = pd.Timestamp(signal_time)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    loc = candle_index.get_loc(ts)
    if isinstance(loc, slice):
        return int(loc.start) if loc.start is not None else int(candle_index.searchsorted(ts))
    if hasattr(loc, "__len__") and not isinstance(loc, (bool, int)):
        return int(loc[0])
    return int(loc)


def run_simulation(
    signals: list[dict],
    candles: pd.DataFrame,
    instrument: str,
    timeout_bars: int,
    mode: str = "aggressive",
) -> list[dict]:
    """
    Run execution physics for each signal. No pattern detection, DB, or strategy config.
    """
    pip = get_pip(instrument)
    cfg = _inst_config_for_symbol(instrument)
    pip_val = float(cfg.get("pip_val", 10.0))
    if not cfg:
        log.warning(
            "position_utils: instrument %r not in INST_CONFIG, pip_val defaulting to 10.0",
            instrument,
        )
    candle_index = candles.index
    arrays = {
        "high": candles["high"].values,
        "low": candles["low"].values,
        "close": candles["close"].values,
        "bid_open": candles["bid_open"].values,
        "ask_open": candles["ask_open"].values,
        "bid_close": candles["bid_close"].values,
        "ask_close": candles["ask_close"].values,
    }
    n = len(candle_index)
    results: list[dict] = []
    m = str(mode).lower()

    for sig in signals:
        signal_time = sig["signal_time"]
        direction = str(sig["direction"]).upper()
        sl = float(sig["sl"])
        tp = float(sig["tp"])

        try:
            sig_idx = _signal_idx(candle_index, signal_time)
        except KeyError:
            results.append(
                {
                    "signal_time": signal_time,
                    "direction": direction,
                    "mode": m,
                    "poll_log_id": sig.get("poll_log_id"),
                    "entry": None,
                    "exit_price": None,
                    "sl": sl,
                    "tp": tp,
                    "pnl_pips": None,
                    "pnl_dollars": None,
                    "result": "DATA_INVALID",
                    "exit_reason": "DATA_INVALID",
                    "entry_time": None,
                    "exit_time": None,
                }
            )
            continue

        if m == "aggressive":
            candle1_idx = sig_idx + 1
            if candle1_idx >= n:
                results.append(
                    {
                        "signal_time": signal_time,
                        "direction": direction,
                        "mode": m,
                        "poll_log_id": sig.get("poll_log_id"),
                        "entry": None,
                        "exit_price": None,
                        "sl": sl,
                        "tp": tp,
                        "pnl_pips": None,
                        "pnl_dollars": None,
                        "result": "DATA_INVALID",
                        "exit_reason": "DATA_INVALID",
                        "entry_time": None,
                        "exit_time": None,
                    }
                )
                continue
            if direction == "BUY":
                entry = float(arrays["ask_open"][candle1_idx])
            else:
                entry = float(arrays["bid_open"][candle1_idx])
            entry_time = candle_index[candle1_idx]
            out = _scan_tp_sl_timeout(
                arrays,
                candle_index,
                entry,
                entry_time,
                candle1_idx,
                sig_idx,
                sl,
                tp,
                direction,
                timeout_bars,
                pip,
            )
            results.append(
                {
                    "signal_time": signal_time,
                    "direction": direction,
                    "mode": m,
                    "poll_log_id": sig.get("poll_log_id"),
                    **out,
                    "pnl_dollars": _compute_pnl_dollars(sig, out, direction, pip, pip_val),
                }
            )
            continue

        # passive
        try:
            sig_idx_p = _signal_idx(candle_index, signal_time)
        except KeyError:
            results.append(
                {
                    "signal_time": signal_time,
                    "direction": direction,
                    "mode": m,
                    "poll_log_id": sig.get("poll_log_id"),
                    "entry": None,
                    "exit_price": None,
                    "sl": sl,
                    "tp": tp,
                    "pnl_pips": None,
                    "pnl_dollars": None,
                    "result": "DATA_INVALID",
                    "exit_reason": "DATA_INVALID",
                    "entry_time": None,
                    "exit_time": None,
                }
            )
            continue

        sig_close = float(arrays["close"][sig_idx_p])
        entry = None
        fill_bar_idx = None
        entry_time = None

        if direction == "BUY":
            for j in range(1, timeout_bars + 1):
                bar_idx = sig_idx_p + j
                if bar_idx >= n:
                    break
                if arrays["ask_open"][bar_idx] <= sig_close:
                    entry = float(arrays["ask_open"][bar_idx])
                    entry_time = candle_index[bar_idx]
                    fill_bar_idx = bar_idx
                    break
                if arrays["low"][bar_idx] <= sig_close:
                    if arrays["low"][bar_idx] <= sl:
                        results.append(
                            {
                                "signal_time": signal_time,
                                "direction": direction,
                                "mode": m,
                                "poll_log_id": sig.get("poll_log_id"),
                                "entry": None,
                                "exit_price": float(sl),
                                "sl": sl,
                                "tp": tp,
                                "pnl_pips": None,
                                "pnl_dollars": None,
                                "result": "LOSS",
                                "exit_reason": "SL_SAME_CANDLE",
                                "entry_time": None,
                                "exit_time": candle_index[bar_idx],
                            }
                        )
                        entry = "__done__"
                        break
                    if arrays["high"][bar_idx] >= tp:
                        results.append(
                            {
                                "signal_time": signal_time,
                                "direction": direction,
                                "mode": m,
                                "poll_log_id": sig.get("poll_log_id"),
                                "entry": None,
                                "exit_price": None,
                                "sl": sl,
                                "tp": tp,
                                "pnl_pips": None,
                                "pnl_dollars": None,
                                "result": "MISSED",
                                "exit_reason": "MISSED_TP",
                                "entry_time": None,
                                "exit_time": candle_index[bar_idx],
                            }
                        )
                        entry = "__done__"
                        break
                    ao = float(arrays["ask_close"][bar_idx])
                    entry = ao if ao <= sig_close else float(sig_close)
                    entry_time = candle_index[bar_idx]
                    fill_bar_idx = bar_idx
                    break
        else:
            for j in range(1, timeout_bars + 1):
                bar_idx = sig_idx_p + j
                if bar_idx >= n:
                    break
                if arrays["bid_open"][bar_idx] >= sig_close:
                    entry = float(arrays["bid_open"][bar_idx])
                    entry_time = candle_index[bar_idx]
                    fill_bar_idx = bar_idx
                    break
                if arrays["high"][bar_idx] >= sig_close:
                    if arrays["high"][bar_idx] >= sl:
                        results.append(
                            {
                                "signal_time": signal_time,
                                "direction": direction,
                                "mode": m,
                                "poll_log_id": sig.get("poll_log_id"),
                                "entry": None,
                                "exit_price": float(sl),
                                "sl": sl,
                                "tp": tp,
                                "pnl_pips": None,
                                "pnl_dollars": None,
                                "result": "LOSS",
                                "exit_reason": "SL_SAME_CANDLE",
                                "entry_time": None,
                                "exit_time": candle_index[bar_idx],
                            }
                        )
                        entry = "__done__"
                        break
                    if arrays["low"][bar_idx] <= tp:
                        results.append(
                            {
                                "signal_time": signal_time,
                                "direction": direction,
                                "mode": m,
                                "poll_log_id": sig.get("poll_log_id"),
                                "entry": None,
                                "exit_price": None,
                                "sl": sl,
                                "tp": tp,
                                "pnl_pips": None,
                                "pnl_dollars": None,
                                "result": "MISSED",
                                "exit_reason": "MISSED_TP",
                                "entry_time": None,
                                "exit_time": candle_index[bar_idx],
                            }
                        )
                        entry = "__done__"
                        break
                    bc = float(arrays["bid_close"][bar_idx])
                    entry = bc if bc >= sig_close else float(sig_close)
                    entry_time = candle_index[bar_idx]
                    fill_bar_idx = bar_idx
                    break

        if entry == "__done__":
            continue
        if entry is None or fill_bar_idx is None:
            results.append(
                {
                    "signal_time": signal_time,
                    "direction": direction,
                    "mode": m,
                    "poll_log_id": sig.get("poll_log_id"),
                    "entry": None,
                    "exit_price": None,
                    "sl": sl,
                    "tp": tp,
                    "pnl_pips": None,
                    "pnl_dollars": None,
                    "result": "SKIPPED",
                    "exit_reason": "TIMEOUT_NO_FILL",
                    "entry_time": None,
                    "exit_time": None,
                }
            )
            continue

        out = _scan_tp_sl_timeout(
            arrays,
            candle_index,
            entry,
            entry_time,
            fill_bar_idx,
            sig_idx_p,
            sl,
            tp,
            direction,
            timeout_bars,
            pip,
        )
        results.append(
            {
                "signal_time": signal_time,
                "direction": direction,
                "mode": m,
                "poll_log_id": sig.get("poll_log_id"),
                **out,
                "pnl_dollars": _compute_pnl_dollars(sig, out, direction, pip, pip_val),
            }
        )

    return results
