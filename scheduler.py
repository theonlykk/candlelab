"""
scheduler.py — Background APScheduler that pre-computes and caches leaderboard data.
"""
import json, logging, threading
log = logging.getLogger(__name__)

RECENT_KEY         = "scheduler:recent_request"
DEFAULT_INSTRUMENT = "EUR/USD"
DEFAULT_INTERVAL   = "5m"
DEFAULT_DAYS       = 30

# Duplicated here to avoid circular import with app.py
PATTERN_GROUPS = {
    "Bearish Reversal": [
        "Shooting Star/Inv. Hammer",
        "Engulfing",
        "Three Soldiers/Crows",
        "Piercing/Dark Cloud",
    ],
    "Bullish Reversal": [
        "Hammer/Hanging Man",
        "Morning/Evening Star",
        "Piercing/Dark Cloud",
        "Three Soldiers/Crows",
    ],
    "Continuation / Bilateral": [
        "Doji",
        "Harami",
    ],
}

_scheduler = None


def _compute_and_cache(instrument: str, interval: str, days: int):
    """Run the full pipeline and store the /api/all payload in Redis."""
    from data import get_ohlc, INSTRUMENTS, atr_to_pips
    from backtest import run_backtest, leaderboard, compute_atr
    from patterns import detect_all
    from cache import cache_set, cache_key

    # This job is designed to run periodically in the background so the UI can load a
    # precomputed payload quickly (when a matching route consumes it).
    log.info(f"Scheduler: computing {instrument} {interval} {days}d")
    try:
        df = get_ohlc(
            instrument,
            days=days,
            interval=interval,
            record_candle_diagnostics=True,
        )
        if df.empty:
            log.warning(f"Scheduler: no data for {instrument}")
            return

        meta    = INSTRUMENTS[instrument]
        pip     = meta["pip"]
        results = run_backtest(df, pip=pip)
        board   = leaderboard(results)

        # Build chart payload (50 candles default)
        n_candles    = 50
        full_atr     = compute_atr(df)
        full_signals = detect_all(df)
        display      = df.tail(n_candles).copy()
        disp_atr     = full_atr.loc[display.index]
        disp_sig     = full_signals.loc[display.index]

        ts     = [t.isoformat() for t in display.index]
        opens  = display["open"].tolist()
        highs  = display["high"].tolist()
        lows   = display["low"].tolist()
        closes = display["close"].tolist()

        raw_boxes = []
        for pat in disp_sig.columns:
            for i, sig in enumerate(disp_sig[pat]):
                if sig == 0:
                    continue
                atr_val = float(disp_atr.iloc[i])
                entry   = float(display["open"].iloc[min(i + 1, len(display) - 1)])
                tp      = entry + sig * 3.0 * atr_val
                sl      = entry - sig * 1.0 * atr_val
                raw_boxes.append({
                    "ts": ts[i], "ts_idx": i, "pattern": pat,
                    "direction": int(sig),
                    "entry": round(entry, 6),
                    "tp":    round(tp, 6),
                    "sl":    round(sl, 6),
                    "atr_pips": atr_to_pips(atr_val, instrument),
                })

        last_close = closes[-1] if closes else 0
        atr_last   = float(full_atr.iloc[-1]) if len(full_atr) else 0

        chart = {
            "ts": ts, "open": opens, "high": highs, "low": lows, "close": closes,
            "boxes":      raw_boxes[-10:],
            "strat_boxes": [],
            "last_close": round(last_close, meta["decimals"]),
            "atr_pips":   atr_to_pips(atr_last, instrument),
            "pip_name":   meta["pip_name"],
            "decimals":   meta["decimals"],
        }

        # Build tradelog (top 10 newest)
        tradelog_list = []
        for pat, r in results.items():
            for t in r["trades"]:
                tradelog_list.append({
                    "ts":          t["ts"].isoformat() if hasattr(t["ts"], "isoformat") else str(t["ts"]),
                    "pattern":     pat,
                    "direction":   "Long" if t["signal"] == 1 else "Short",
                    "entry":       round(t["entry"], meta["decimals"]),
                    "tp":          round(t["tp"],    meta["decimals"]),
                    "sl":          round(t["sl"],    meta["decimals"]),
                    "outcome":     t["outcome"],
                    "win":         t["win"],
                    "atr_pips":    atr_to_pips(t["atr"], instrument),
                    "pnl_gross":   t["pnl_gross"],
                    "pnl_net":     t["pnl_net"],
                    "spread_cost": t.get("spread_cost", 0),
                })
        tradelog_list.sort(key=lambda x: x["ts"], reverse=True)
        tradelog = tradelog_list[:10]

        # Build all_trades (chronological, no 'win' field)
        all_trades = []
        for pat, r in results.items():
            for t in r["trades"]:
                all_trades.append({
                    "ts":          t["ts"].isoformat() if hasattr(t["ts"], "isoformat") else str(t["ts"]),
                    "pattern":     pat,
                    "direction":   "Long" if t["signal"] == 1 else "Short",
                    "entry":       round(t["entry"], meta["decimals"]),
                    "tp":          round(t["tp"],    meta["decimals"]),
                    "sl":          round(t["sl"],    meta["decimals"]),
                    "outcome":     t["outcome"],
                    "pnl_gross":   t["pnl_gross"],
                    "pnl_net":     t["pnl_net"],
                    "spread_cost": t.get("spread_cost", 0),
                    "atr_pips":    atr_to_pips(t["atr"], instrument),
                })
        all_trades.sort(key=lambda x: x["ts"])

        payload = {
            "chart":      chart,
            "board":      board,
            "tradelog":   tradelog,
            "all_trades": all_trades,
            "groups":     PATTERN_GROUPS,
            "instrument": instrument,
            "interval":   interval,
            "pip_name":   meta["pip_name"],
        }

        cache_set(cache_key(instrument, interval, days), payload)
        log.info(f"Scheduler: cached {instrument} {interval} {days}d")
    except Exception:
        log.exception(f"Scheduler: _compute_and_cache failed for {instrument}")


def _get_recent_request():
    """
    Return the most recently requested (instrument, interval, days) tuple for the scheduler.

    The scheduler uses this to bias refreshes toward what users are currently exploring.
    """
    from cache import get_redis
    r = get_redis()
    if r is None:
        return DEFAULT_INSTRUMENT, DEFAULT_INTERVAL, DEFAULT_DAYS
    try:
        raw = r.get(RECENT_KEY)
        if raw:
            d = json.loads(raw)
            return d["instrument"], d["interval"], d["days"]
    except Exception:
        pass
    return DEFAULT_INSTRUMENT, DEFAULT_INTERVAL, DEFAULT_DAYS


def update_recent_request(instrument: str, interval: str, days: int):
    """Persist the latest requested tuple so scheduled refreshes target it."""
    from cache import get_redis
    r = get_redis()
    if r is None:
        return
    try:
        r.set(RECENT_KEY,
              json.dumps({"instrument": instrument, "interval": interval, "days": days}),
              ex=3600)
    except Exception:
        pass


def _scheduled_refresh():
    """APScheduler callback: refresh the cached payload for the most recent request."""
    instrument, interval, days = _get_recent_request()
    _compute_and_cache(instrument, interval, days)
    try:
        from poll_log import append_cycle_poll_logs

        append_cycle_poll_logs()
    except Exception:
        log.exception("Scheduler: poll log append failed")


def start_scheduler():
    """
    Start the background scheduler once per process.

    - Kicks off an initial computation in a daemon thread so app startup isn't blocked.
    - Then refreshes every 5 minutes using APScheduler.
    """
    global _scheduler
    if _scheduler is not None and _scheduler.running:
        return

    # Pre-compute default on startup without blocking Flask startup
    t = threading.Thread(
        target=lambda: _compute_and_cache(DEFAULT_INSTRUMENT, DEFAULT_INTERVAL, DEFAULT_DAYS),
        daemon=True,
    )
    t.start()

    from apscheduler.schedulers.background import BackgroundScheduler
    _scheduler = BackgroundScheduler(daemon=True)
    _scheduler.add_job(_scheduled_refresh, "interval", minutes=5, id="refresh")
    _scheduler.start()
    log.info("Scheduler started (5-minute refresh)")
