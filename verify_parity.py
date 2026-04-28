#!/usr/bin/env python3
"""
Read-only parity diagnostic: compare wizard OHLC source vs oanda_candles for EUR/USD M5,
run patterns.detect_all + signal_engine.detect_signal on both, and sanity-check H1 ATR from M5.

Does not modify the database or application modules.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")

from indicator_utils import compute_h1_atr_from_m5  # noqa: E402
from patterns import detect_all  # noqa: E402
from signal_engine import detect_signal  # noqa: E402

# Wizard path uses data.load_bars → table derived from INSTRUMENTS key "EUR/USD" + interval "5m"
OHLC_TABLE_EURUSD_M5 = "ohlc_eurusd_5m"
PIP_EURUSD = 0.0001

# Spec asks for ma_cross; patterns.detect_all() only exposes candlestick pattern columns (see patterns.PATTERNS).
# If missing, fall back so the script still demonstrates parity machinery.
ANCHOR_REQUESTED = "ma_cross"
ANCHOR_FALLBACK = "Engulfing"


def _require_db_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        print("ERROR: DATABASE_URL not set (and not found in .env).", file=sys.stderr)
        sys.exit(1)
    return url


def _fetch_source_a(engine, cutoff_iso: str) -> pd.DataFrame:
    """Same logical source as get_ohlc → load_bars for EUR/USD 5m."""
    q = text(
        f"""
        SELECT ts, open, high, low, close
        FROM {OHLC_TABLE_EURUSD_M5}
        WHERE ts >= :cutoff
        ORDER BY ts
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(q, conn, params={"cutoff": cutoff_iso})
    return df


def _fetch_source_b(engine, cutoff_utc: datetime) -> pd.DataFrame:
    q = text(
        """
        SELECT time, open, high, low, close
        FROM oanda_candles
        WHERE instrument = :inst AND time >= :cutoff
        ORDER BY time ASC
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(
            q,
            conn,
            params={"inst": "EUR_USD", "cutoff": cutoff_utc},
        )
    return df


def _to_ohlc_indexed(df: pd.DataFrame, time_col: str) -> pd.DataFrame:
    out = df.copy()
    out[time_col] = pd.to_datetime(out[time_col], utc=True)
    out = out.set_index(time_col).sort_index()
    out.index = pd.DatetimeIndex(out.index)
    if out.index.tzinfo is None:
        out.index = out.index.tz_localize("UTC")
    else:
        out.index = out.index.tz_convert("UTC")
    out.index.name = "timestamp"
    return out[["open", "high", "low", "close"]]


def _resolve_anchor(signals_df: pd.DataFrame) -> str:
    if ANCHOR_REQUESTED in signals_df.columns:
        return ANCHOR_REQUESTED
    print(
        f"WARN: anchor {ANCHOR_REQUESTED!r} not in detect_all columns "
        f"{list(signals_df.columns)} — patterns.PATTERNS has no ma_cross; "
        f"using {ANCHOR_FALLBACK!r} for detect_signal parity.",
        file=sys.stderr,
    )
    return ANCHOR_FALLBACK


def main() -> None:
    url = _require_db_url()
    engine = create_engine(url, pool_pre_ping=True)

    now = datetime.now(timezone.utc)
    cutoff_dt = now - timedelta(days=30)
    cutoff_iso = cutoff_dt.isoformat()

    print("=== Source A (wizard):", OHLC_TABLE_EURUSD_M5)
    df_a_raw = _fetch_source_a(engine, cutoff_iso)
    print("  rows:", len(df_a_raw))
    if not df_a_raw.empty:
        ts_a = pd.to_datetime(df_a_raw["ts"], utc=True)
        print("  ts min:", ts_a.min(), " max:", ts_a.max())
    ohlc_a = _to_ohlc_indexed(df_a_raw, "ts") if not df_a_raw.empty else pd.DataFrame()

    print("=== Source B (30d card): oanda_candles EUR_USD")
    df_b_raw = _fetch_source_b(engine, cutoff_dt)
    print("  rows:", len(df_b_raw))
    if not df_b_raw.empty:
        ts_b = pd.to_datetime(df_b_raw["time"], utc=True)
        print("  ts min:", ts_b.min(), " max:", ts_b.max())
    ohlc_b = _to_ohlc_indexed(df_b_raw, "time") if not df_b_raw.empty else pd.DataFrame()

    if ohlc_a.empty and ohlc_b.empty:
        print("ERROR: both sources empty — check DATABASE_URL, table names, and date range.", file=sys.stderr)
        sys.exit(2)

    print("\n=== detect_all (patterns.detect_all)")
    if ohlc_a.empty:
        print("  Source A: (empty, skip)")
        sig_a = None
    else:
        sig_a = detect_all(ohlc_a)
    if ohlc_b.empty:
        print("  Source B: (empty, skip)")
        sig_b = None
    else:
        sig_b = detect_all(ohlc_b)
    if sig_b is None or sig_b.empty:
        print("ERROR: need Source B for anchor resolution and ATR section.", file=sys.stderr)
        sys.exit(3)
    anchor = _resolve_anchor(sig_b)
    print("  using anchor column:", anchor)

    print("\n=== detect_signal (signal_engine.detect_signal)")
    if sig_a is None:
        arr_a = np.array([], dtype=np.int8)
        ca = 0
    else:
        arr_a = detect_signal(sig_a, anchor, None, None, "both", 10)
        ca = int(np.sum(arr_a != 0))
    arr_b = detect_signal(sig_b, anchor, None, None, "both", 10)
    cb = int(np.sum(arr_b != 0))
    print("  Source A non-zero signals:", ca)
    print("  Source B non-zero signals:", cb)

    print("\n=== Last 5 Source-B signal bars — H1 ATR from 200×M5 slice (indicator_utils.compute_h1_atr_from_m5)")
    idx_b = sig_b.index
    nz = np.nonzero(arr_b)[0]
    if len(nz) == 0:
        print("  (no signals on Source B)")
        return
    last5_pos = nz[-5:]
    for pos in last5_pos:
        ts_sig = idx_b[pos]
        # All M5 bars up to and including signal timestamp
        hist = ohlc_b.loc[:ts_sig]
        if len(hist) < 50:
            print(f"  {ts_sig}: SKIP (only {len(hist)} bars history)")
            continue
        slice200 = hist.tail(200)
        atr_price = compute_h1_atr_from_m5(slice200, period=14)
        atr_pips = atr_price / PIP_EURUSD if atr_price > 0 else 0.0
        tag = "OK"
        if atr_pips > 0 and (atr_pips < 3 or atr_pips > 20):
            tag = "SUSPECT"
        print(
            f"  {ts_sig.isoformat()} | ATR≈{atr_pips:.2f} pips | {tag}"
        )


if __name__ == "__main__":
    main()
