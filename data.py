"""
data.py — TradingView (tvdatafeed-enhanced) OHLC fetcher + SQLite persistence
Multi-instrument: FX majors + commodities + indices + crypto
No API key required. Uses OANDA/BINANCE feeds via TradingView.
"""

import os
import time
import sqlite3
import logging
import threading
import pandas as pd
from tvDatafeed import TvDatafeed, Interval
from datetime import datetime, timezone, timedelta

log = logging.getLogger(__name__)
DB_PATH = os.environ.get("DB_PATH", "fx_ohlc.db")

# Lazy-init — only create connection when first needed
_tv = None
_tv_lock = threading.Lock()

# Tracks which (instrument, interval) pairs are currently being backfilled
_backfill_in_progress: set = set()
_backfill_lock = threading.Lock()

def _get_tv():
    """
    Lazily construct a single TvDatafeed client instance.

    TvDatafeed internally handles auth/cookies; constructing it repeatedly is slow and can
    create unnecessary network churn. The lock ensures a single instance in multi-thread
    scenarios (Gunicorn threads).
    """
    global _tv
    if _tv is None:
        with _tv_lock:
            if _tv is None:
                _tv = TvDatafeed()
    return _tv

# ── Instrument registry ───────────────────────────────────────────────────────
INSTRUMENTS = {
    "EUR/USD":  {"symbol": "EURUSD",   "exchange": "OANDA",   "pip": 0.0001, "pip_name": "pips",  "decimals": 5},
    "GBP/USD":  {"symbol": "GBPUSD",   "exchange": "OANDA",   "pip": 0.0001, "pip_name": "pips",  "decimals": 5},
    "USD/JPY":  {"symbol": "USDJPY",   "exchange": "OANDA",   "pip": 0.01,   "pip_name": "pips",  "decimals": 3},
    "USD/CAD":  {"symbol": "USDCAD",   "exchange": "OANDA",   "pip": 0.0001, "pip_name": "pips",  "decimals": 5},
    "AUD/USD":  {"symbol": "AUDUSD",   "exchange": "OANDA",   "pip": 0.0001, "pip_name": "pips",  "decimals": 5},
    "USD/CHF":  {"symbol": "USDCHF",   "exchange": "OANDA",   "pip": 0.0001, "pip_name": "pips",  "decimals": 5},
    "NZD/USD":  {"symbol": "NZDUSD",   "exchange": "OANDA",   "pip": 0.0001, "pip_name": "pips",  "decimals": 5},
    "Gold":     {"symbol": "XAUUSD",   "exchange": "OANDA",   "pip": 0.1,    "pip_name": "cents", "decimals": 2},
    "Silver":   {"symbol": "XAGUSD",   "exchange": "OANDA",   "pip": 0.001,  "pip_name": "cents", "decimals": 4},
    "Oil":      {"symbol": "WTICOUSD", "exchange": "OANDA",   "pip": 0.01,   "pip_name": "cents", "decimals": 3},
    "S&P 500":  {"symbol": "SPX500USD","exchange": "OANDA",   "pip": 0.25,   "pip_name": "pts",   "decimals": 2},
    "Bitcoin":  {"symbol": "BTCUSD",   "exchange": "BINANCE", "pip": 1.0,    "pip_name": "pts",   "decimals": 2},
}

DEFAULT_INSTRUMENT = "EUR/USD"

INTERVAL_MAP = {
    "5m":  Interval.in_5_minute,
    "15m": Interval.in_15_minute,
    "1h":  Interval.in_1_hour,
}
DEFAULT_INTERVAL  = "5m"
_INITIAL_BARS     = {"5m": 12000, "15m": 4000, "1h": 1000}
_MINS_PER_BAR     = {"5m": 5,     "15m": 15,   "1h": 60}


def _table(instrument: str, interval: str = "5m") -> str:
    """
    Derive a per-instrument, per-interval SQLite table name.

    Note: table names are created dynamically at init time for each configured instrument
    and interval. Instrument strings are normalized to a restricted character set here.
    """
    base   = "ohlc_" + instrument.replace("/","").replace(" ","_").replace("&","").lower()
    suffix = interval.replace("/","").lower()
    return f"{base}_{suffix}"


# ── DB helpers ────────────────────────────────────────────────────────────────

def get_conn() -> sqlite3.Connection:
    """
    Return a short-lived SQLite connection configured for concurrent reads/writes.

    WAL journal mode improves read/write concurrency for this workload. Callers generally
    use context managers (`with get_conn() as conn:`) to ensure timely close/commit.
    """
    conn = sqlite3.connect(DB_PATH, timeout=20.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    """Create OHLC tables for all instruments/intervals if missing."""
    with get_conn() as conn:
        for inst in INSTRUMENTS:
            for ivl in INTERVAL_MAP:
                tbl = _table(inst, ivl)
                conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {tbl} (
                        ts TEXT PRIMARY KEY,
                        open REAL, high REAL, low REAL, close REAL
                    )
                """)
        conn.commit()


def latest_ts(instrument: str, interval: str = "5m") -> datetime | None:
    """Return the latest stored UTC timestamp for an instrument/interval, if any."""
    tbl = _table(instrument, interval)
    with get_conn() as conn:
        row = conn.execute(f"SELECT MAX(ts) as m FROM {tbl}").fetchone()
        if row and row["m"]:
            return datetime.fromisoformat(row["m"]).replace(tzinfo=timezone.utc)
    return None


def insert_bars(instrument: str, df: pd.DataFrame, interval: str = "5m"):
    """
    Insert OHLC rows into SQLite.

    - Timestamps are stored as ISO-8601 strings in UTC.
    - Uses INSERT OR IGNORE so re-fetching overlapping windows is safe.
    """
    if df.empty:
        return
    tbl = _table(instrument, interval)
    idx = df.index
    if idx.tzinfo is None:
        idx = idx.tz_localize("UTC")
    else:
        idx = idx.tz_convert("UTC")
    rows = list(zip(
        [ts.isoformat() for ts in idx],
        df["open"].tolist(),
        df["high"].tolist(),
        df["low"].tolist(),
        df["close"].tolist(),
    ))
    with get_conn() as conn:
        conn.executemany(
            f"INSERT OR IGNORE INTO {tbl} (ts,open,high,low,close) VALUES (?,?,?,?,?)",
            rows
        )
        conn.commit()


def load_bars(instrument: str, days: int = 62, interval: str = "5m") -> pd.DataFrame:
    """
    Load the most recent `days` of bars from SQLite for an instrument/interval.

    Returned index is tz-aware UTC.
    """
    tbl    = _table(instrument, interval)
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    with get_conn() as conn:
        df = pd.read_sql(
            f"SELECT * FROM {tbl} WHERE ts >= ? ORDER BY ts",
            conn, params=(cutoff,), parse_dates=["ts"], index_col="ts"
        )
    if df.empty:
        return df
    idx = pd.DatetimeIndex(df.index)
    df.index = idx.tz_convert("UTC") if idx.tzinfo else idx.tz_localize("UTC")
    return df


# ── TradingView fetch ─────────────────────────────────────────────────────────

def _fetch_tv(symbol: str, exchange: str, n_bars: int, interval: str = "5m") -> pd.DataFrame:
    """
    Fetch n_bars of OHLC from TradingView for the given interval.
    Max 5000 bars per call — for larger requests we make multiple calls.
    """
    # tvdatafeed's `n_bars` returns the most recent N bars. For large backfills we
    # make multiple calls and de-duplicate by timestamp when windows overlap.
    MAX_PER_CALL = 4900
    tv_interval  = INTERVAL_MAP.get(interval, Interval.in_5_minute)
    all_frames = []
    remaining  = n_bars

    while remaining > 0:
        batch = min(remaining, MAX_PER_CALL)
        try:
            df = _get_tv().get_hist(
                symbol=symbol,
                exchange=exchange,
                interval=tv_interval,
                n_bars=batch,
            )
        except Exception as e:
            log.error(f"TradingView fetch failed for {symbol}: {e}")
            break

        if df is None or df.empty:
            break

        df = df[["open", "high", "low", "close"]].copy()
        df.index.name = "ts"

        if df.index.tzinfo is None:
            df.index = df.index.tz_localize("UTC")
        else:
            df.index = df.index.tz_convert("UTC")

        all_frames.append(df.dropna())
        remaining -= len(df)

        # If we got fewer bars than requested, no point asking for more
        if len(df) < batch:
            break

        time.sleep(0.5)

    if not all_frames:
        return pd.DataFrame()

    combined = pd.concat(all_frames).sort_index()
    combined = combined[~combined.index.duplicated(keep='last')]
    return combined


# ── Backfill logic ────────────────────────────────────────────────────────────

def backfill_instrument(instrument: str, interval: str = "5m"):
    """
    Ensure SQLite has up-to-date bars for a single instrument/interval.

    This function is intentionally idempotent and guarded by `_backfill_in_progress`
    to avoid concurrent duplicate fetches for the same pair.
    """
    key = (instrument, interval)
    with _backfill_lock:
        if key in _backfill_in_progress:
            return
        _backfill_in_progress.add(key)

    try:
        meta      = INSTRUMENTS[instrument]
        symbol    = meta["symbol"]
        exchange  = meta["exchange"]
        since     = latest_ts(instrument, interval)
        now       = datetime.now(timezone.utc)
        mins_bar  = _MINS_PER_BAR.get(interval, 5)

        if since is None:
            log.info(f"Initial backfill: {instrument} @ {interval}...")
            n_bars = _INITIAL_BARS.get(interval, 12000)
            df = _fetch_tv(symbol, exchange, n_bars=n_bars, interval=interval)
            if not df.empty:
                insert_bars(instrument, df, interval)
                log.info(f"{instrument} @ {interval}: inserted {len(df)} bars")
            else:
                log.error(f"{instrument} @ {interval}: backfill returned empty — symbol={symbol} exchange={exchange}; using cached data")
            return

        gap = (now - since).total_seconds() / 60
        if gap < mins_bar:
            return

        log.info(f"Backfilling {instrument} @ {interval} ({gap:.0f} min gap)...")
        bars_needed = int(gap / mins_bar) + 50
        df = _fetch_tv(symbol, exchange, n_bars=bars_needed, interval=interval)

        if not df.empty:
            new_bars = df[df.index > since]
            insert_bars(instrument, new_bars, interval)
            log.info(f"{instrument} @ {interval}: inserted {len(new_bars)} new bars")
        else:
            log.error(f"{instrument} @ {interval}: incremental fetch returned empty — symbol={symbol} exchange={exchange}")
    finally:
        with _backfill_lock:
            _backfill_in_progress.discard(key)


def _backfill_all_worker():
    """Background worker that performs initial backfills for all instruments."""
    for inst in INSTRUMENTS:
        try:
            backfill_instrument(inst, DEFAULT_INTERVAL)
        except Exception as e:
            log.warning(f"Backfill failed for {inst}: {e}")
        time.sleep(1)


def backfill_all():
    """Kick off backfill in background thread so Flask starts immediately."""
    init_db()
    import threading
    t = threading.Thread(target=_backfill_all_worker, daemon=True)
    t.start()
    log.info("Background backfill started for all instruments.")


def backfill_missing(instrument: str, interval: str = "5m"):
    """Ensure DB is initialized and backfilled for the requested instrument/interval."""
    init_db()
    backfill_instrument(instrument, interval)


def get_ohlc(instrument: str = DEFAULT_INSTRUMENT, days: int = 62, interval: str = "5m") -> pd.DataFrame:
    """
    Public API: return a recent OHLC DataFrame, ensuring data is backfilled first.

    The backfill step may trigger network fetches if the local DB is behind.
    """
    backfill_missing(instrument, interval)
    return load_bars(instrument, days=days, interval=interval)


def atr_to_pips(atr_value: float, instrument: str) -> float:
    """Convert an ATR value in price units to pips/points for display."""
    pip = INSTRUMENTS[instrument]["pip"]
    return round(atr_value / pip, 1)