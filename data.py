"""
data.py — OANDA REST OHLC fetcher + PostgreSQL persistence
Multi-instrument: FX majors + commodities + indices + crypto
Uses OANDA v3 candles API (Bearer token). OHLC tables in PostgreSQL (DATABASE_URL).
"""

import os
import time
import logging
import threading
import pandas as pd
import psycopg2
import psycopg2.extras
import requests
from psycopg2.extensions import connection as PGConnection
from datetime import datetime, timezone, timedelta

log = logging.getLogger(__name__)
DATABASE_URL = os.environ.get("DATABASE_URL")

# Tracks which (instrument, interval) pairs are currently being backfilled
_backfill_in_progress: set = set()
_backfill_lock = threading.Lock()

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
    "Bitcoin":  {"symbol": "BTCUSD",   "exchange": "OANDA",   "pip": 1.0,    "pip_name": "pts",   "decimals": 2},
}

DEFAULT_INSTRUMENT = "EUR/USD"

# CandleLab interval key -> OANDA v3 granularity string
INTERVAL_MAP = {
    "5m":  "M5",
    "15m": "M15",
    "1h":  "H1",
}
DEFAULT_INTERVAL  = "5m"
_INITIAL_BARS     = {"5m": 12000, "15m": 4000, "1h": 1000}
_MINS_PER_BAR     = {"5m": 5,     "15m": 15,   "1h": 60}


def _oanda_instrument_id(instrument: str) -> str:
    """
    Map a CandleLab INSTRUMENTS key (e.g. "EUR/USD") to an OANDA instrument name (e.g. EUR_USD).
    Uses the compact `symbol` field (e.g. EURUSD, WTICOUSD) and inserts an underscore before USD.
    """
    sym = INSTRUMENTS[instrument]["symbol"]
    if sym.endswith("USD") and len(sym) > 6:
        return f"{sym[:-3]}_{sym[-3:]}"
    if len(sym) == 6:
        return f"{sym[:3]}_{sym[3:]}"
    return f"{sym[:3]}_{sym[3:]}"


def _table(instrument: str, interval: str = "5m") -> str:
    """
    Derive a per-instrument, per-interval PostgreSQL table name.

    Note: table names are created dynamically at init time for each configured instrument
    and interval. Instrument strings are normalized to a restricted character set here.
    """
    base   = "ohlc_" + instrument.replace("/","").replace(" ","_").replace("&","").lower()
    suffix = interval.replace("/","").lower()
    return f"{base}_{suffix}"


# ── DB helpers ────────────────────────────────────────────────────────────────

def get_conn() -> PGConnection:
    conn = psycopg2.connect(DATABASE_URL)
    return conn


def init_db():
    """Create OHLC tables for all instruments/intervals if missing."""
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        for inst in INSTRUMENTS:
            for ivl in INTERVAL_MAP:
                tbl = _table(inst, ivl)
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {tbl} (
                        ts TEXT PRIMARY KEY,
                        open DOUBLE PRECISION,
                        high DOUBLE PRECISION,
                        low DOUBLE PRECISION,
                        close DOUBLE PRECISION
                    )
                    """
                )
        conn.commit()


def latest_ts(instrument: str, interval: str = "5m") -> datetime | None:
    """Return the latest stored UTC timestamp for an instrument/interval, if any."""
    tbl = _table(instrument, interval)
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(f"SELECT MAX(ts) AS m FROM {tbl}")
        row = cur.fetchone()
        if row and row["m"]:
            return datetime.fromisoformat(row["m"]).replace(tzinfo=timezone.utc)
    return None


def insert_bars(instrument: str, df: pd.DataFrame, interval: str = "5m"):
    """
    Insert OHLC rows into PostgreSQL.

    - Timestamps are stored as ISO-8601 strings in UTC.
    - Uses ON CONFLICT DO NOTHING so re-fetching overlapping windows is safe.
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
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.executemany(
            f"""
            INSERT INTO {tbl} (ts, open, high, low, close)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (ts) DO NOTHING
            """,
            rows,
        )
        conn.commit()


def load_bars(instrument: str, days: int = 62, interval: str = "5m") -> pd.DataFrame:
    """
    Load the most recent `days` of bars from PostgreSQL for an instrument/interval.

    Returned index is tz-aware UTC.
    """
    tbl    = _table(instrument, interval)
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            f"SELECT * FROM {tbl} WHERE ts >= %s ORDER BY ts",
            (cutoff,),
        )
        rows = cur.fetchall()
        df = pd.DataFrame(rows)
        if df.empty:
            return df
        df["ts"] = pd.to_datetime(df["ts"])
        df = df.set_index("ts")
    if df.empty:
        return df
    idx = pd.DatetimeIndex(df.index)
    df.index = idx.tz_convert("UTC") if idx.tzinfo else idx.tz_localize("UTC")
    return df


# ── OANDA REST fetch ───────────────────────────────────────────────────────────

def _candles_to_df(candles: list) -> pd.DataFrame:
    """Parse OANDA `candles` JSON list into a DataFrame (UTC index, OHLCV)."""
    rows = []
    for c in candles:
        if not c.get("complete", True):
            continue
        mid = c.get("mid") or {}
        try:
            o = float(mid["o"])
            h = float(mid["h"])
            l = float(mid["l"])
            cl = float(mid["c"])
        except (KeyError, TypeError, ValueError):
            continue
        vol = int(c.get("volume", 0) or 0)
        t = pd.Timestamp(c["time"])
        rows.append((t, o, h, l, cl, vol))
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
    df = df.set_index("ts").sort_index()
    if df.index.tzinfo is None:
        df.index = df.index.tz_localize("UTC")
    else:
        df.index = df.index.tz_convert("UTC")
    df.index.name = "ts"
    return df


def _fetch_oanda(symbol: str, granularity: str, n_bars: int) -> pd.DataFrame:
    """
    Fetch up to `n_bars` of mid OHLC candles from OANDA v3 REST.

    - GET {OANDA_BASE_URL}/v3/instruments/{symbol}/candles
    - Query: count, price=M, granularity
    - Authorization: Bearer {OANDA_API_TOKEN}
    - If n_bars > 5000, paginate backwards using the `to` parameter (exclusive end time).

    `symbol` must be an OANDA instrument id (e.g. EUR_USD), not a CandleLab label.
    Returns a DataFrame with UTC datetime index and columns open, high, low, close, volume.
    """
    token = (os.environ.get("OANDA_API_TOKEN") or "").strip()
    base = (os.environ.get("OANDA_BASE_URL") or "").strip().rstrip("/")
    if not token or not base:
        log.error("OANDA_API_TOKEN and OANDA_BASE_URL must be set for OHLC fetch")
        return pd.DataFrame()

    url = f"{base}/v3/instruments/{symbol}/candles"
    headers = {"Authorization": f"Bearer {token}"}

    chunks: list[pd.DataFrame] = []
    remaining = int(n_bars)
    to_exclusive: str | None = None

    while remaining > 0:
        batch = min(5000, remaining)
        params: dict[str, str] = {
            "price": "M",
            "granularity": granularity,
            "count": str(batch),
        }
        if to_exclusive is not None:
            params["to"] = to_exclusive

        try:
            r = requests.get(url, headers=headers, params=params, timeout=60)
        except Exception as e:
            log.error("OANDA request failed for %s: %s", symbol, e)
            break

        if r.status_code != 200:
            log.error(
                "OANDA candles HTTP %s for %s: %s",
                r.status_code,
                symbol,
                (r.text or "")[:500],
            )
            break

        try:
            payload = r.json()
        except Exception as e:
            log.error("OANDA JSON decode failed for %s: %s", symbol, e)
            break

        if "errorMessage" in payload:
            log.error("OANDA API error for %s: %s", symbol, payload.get("errorMessage"))
            break

        candles = payload.get("candles") or []
        if not candles:
            break

        # Next page ends strictly before the oldest candle in this response (chronological order).
        to_exclusive = candles[0]["time"]

        df_piece = _candles_to_df(candles)
        if df_piece.empty:
            break

        chunks.insert(0, df_piece)
        remaining -= len(df_piece)

        if len(candles) < batch:
            break

        time.sleep(0.2)

    if not chunks:
        return pd.DataFrame()

    combined = pd.concat(chunks).sort_index()
    combined = combined[~combined.index.duplicated(keep="last")]
    if len(combined) > n_bars:
        combined = combined.iloc[-n_bars:]
    return combined


# ── Backfill logic ────────────────────────────────────────────────────────────

def backfill_instrument(instrument: str, interval: str = "5m"):
    """
    Ensure PostgreSQL has up-to-date bars for a single instrument/interval.

    This function is intentionally idempotent and guarded by `_backfill_in_progress`
    to avoid concurrent duplicate fetches for the same pair.
    """
    key = (instrument, interval)
    with _backfill_lock:
        if key in _backfill_in_progress:
            return
        _backfill_in_progress.add(key)

    try:
        meta         = INSTRUMENTS[instrument]
        oanda_symbol = _oanda_instrument_id(instrument)
        granularity  = INTERVAL_MAP.get(interval, "M5")
        since        = latest_ts(instrument, interval)
        now          = datetime.now(timezone.utc)
        mins_bar     = _MINS_PER_BAR.get(interval, 5)

        if since is None:
            log.info(f"Initial backfill: {instrument} @ {interval}...")
            n_bars = _INITIAL_BARS.get(interval, 12000)
            df = _fetch_oanda(oanda_symbol, granularity, n_bars=n_bars)
            if not df.empty:
                insert_bars(instrument, df, interval)
                log.info(f"{instrument} @ {interval}: inserted {len(df)} bars")
            else:
                log.error(
                    f"{instrument} @ {interval}: backfill returned empty — "
                    f"OANDA instrument={oanda_symbol}; using cached data"
                )
            return

        gap = (now - since).total_seconds() / 60
        if gap < mins_bar:
            return

        log.info(f"Backfilling {instrument} @ {interval} ({gap:.0f} min gap)...")
        bars_needed = int(gap / mins_bar) + 50
        df = _fetch_oanda(oanda_symbol, granularity, n_bars=bars_needed)

        if not df.empty:
            new_bars = df[df.index > since]
            insert_bars(instrument, new_bars, interval)
            log.info(f"{instrument} @ {interval}: inserted {len(new_bars)} new bars")
        else:
            log.error(
                f"{instrument} @ {interval}: incremental fetch returned empty — "
                f"OANDA instrument={oanda_symbol}"
            )
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
