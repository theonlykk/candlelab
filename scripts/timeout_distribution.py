"""
Timeout distribution analysis — resolution bar profiling for 6 active Tranche 2/3 configs.

Runs sweep_simulation() directly for each active config on ftmo_m30 leaderboard data.
Produces per-config and per-group resolution bar distributions with timeout recommendations.
No DB connection. Reads from output/ftmo_m30/ CSVs and ftmo_candles via existing pipeline.
"""

from __future__ import annotations

import os
import sys
import numpy as np
import pandas as pd

# Add scripts dir to path for sweep_validation imports
sys.path.insert(0, os.path.dirname(__file__))

from sweep_validation_ftmo_m30 import (
    sweep_simulation,
    compute_regime_features,
    build_combo_list,
    PAIR_CONFIG,
    MA_FAST,
    MA_SLOW,
    TIMEOUT_BARS,
    TP_MULT,
    SL_MULT,
    _apply_indicator_filter,
)
from candlelab_core.patterns import detect_all

OUTPUT_DIR = r"d:\candlelab\scripts\output\ftmo_m30"
REPORT_PATH = r"d:\candlelab\scripts\output\timeout_distribution_report.csv"

# Active configs — hardcoded per current book state
ACTIVE_CONFIGS = [
    {"strategy_id": 56, "instrument": "USD_CAD", "anchor": "shooting_star", "continuation": None, "gap": None, "indicator": None, "direction": "long",  "status": "AMBER"},
    {"strategy_id": 57, "instrument": "AUD_JPY", "anchor": "morning_star",  "continuation": None, "gap": None, "indicator": None, "direction": "long",  "status": "AMBER"},
    {"strategy_id": 58, "instrument": "GBP_JPY", "anchor": "shooting_star", "continuation": None, "gap": None, "indicator": None, "direction": "long",  "status": "GREEN"},
    {"strategy_id": 61, "instrument": "USD_JPY", "anchor": "shooting_star", "continuation": None, "gap": None, "indicator": None, "direction": "long",  "status": "GREEN"},
    {"strategy_id": 63, "instrument": "GBP_AUD", "anchor": "engulfing",     "continuation": None, "gap": None, "indicator": None, "direction": "short", "status": "GREEN"},
    {"strategy_id": 68, "instrument": "GBP_USD", "anchor": "engulfing",     "continuation": None, "gap": None, "indicator": None, "direction": "short", "status": "GREEN"},
]

PERCENTILES = [50, 75, 80, 90]


def load_instrument_data(instrument: str) -> pd.DataFrame | None:
    """Load ftmo_m30 leaderboard CSV to get instrument data path, then load candles."""
    # We need the actual OHLC data — load from the sweep's data pipeline
    # Use the same DB connection as sweep scripts
    from dotenv import load_dotenv
    load_dotenv()
    import psycopg2

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print(f"  ERROR: DATABASE_URL not set")
        return None

    try:
        conn = psycopg2.connect(db_url)
        query = """
            SELECT time, open, high, low, close, spread_points
            FROM ftmo_candles
            WHERE instrument = %s AND granularity = 'M30'
            ORDER BY time ASC
        """
        df = pd.read_sql(query, conn, params=(instrument,), index_col="time", parse_dates=["time"])
        conn.close()
        if df.empty:
            print(f"  WARNING: No M30 data for {instrument}")
            return None
        df.index = pd.to_datetime(df.index, utc=True)
        df["bid_open"]  = df["open"]
        df["ask_open"]  = df["open"]
        df["bid_close"] = df["close"]
        df["ask_close"] = df["close"]
        return df
    except Exception as e:
        print(f"  ERROR loading {instrument}: {e}")
        return None


def analyze_config(cfg: dict, df: pd.DataFrame) -> list[dict]:
    """Run sweep_simulation for one config and return trade dicts with resolution_bar."""
    from candlelab_core.signal_engine import detect_signal
    from sweep_validation_ftmo_m30 import profile_from_combo, _apply_indicator_filter

    instrument = cfg["instrument"]
    direction  = cfg["direction"]
    pip_size   = 0.01 if "JPY" in instrument else 0.0001

    avg_spread = float(df["spread_points"].mean()) * (pip_size / 10)
    df = df.sort_index()
    df = compute_regime_features(df)

    pair_cfg = PAIR_CONFIG.get(instrument, {})
    ma_fast  = pair_cfg.get("ma_pairs", [(MA_FAST, MA_SLOW)])[0][0]
    ma_slow  = pair_cfg.get("ma_pairs", [(MA_FAST, MA_SLOW)])[0][1]
    sl_mode  = pair_cfg.get("sl_mode", "standard")

    signals_df = detect_all(df)
    full_sma_fast = df["close"].rolling(ma_fast).mean().to_numpy()
    full_sma_slow = df["close"].rolling(ma_slow).mean().to_numpy()
    from sweep_validation_ftmo_m30 import compute_indicators
    inds    = compute_indicators(df)
    atr_arr = inds["atr"]

    # Build combo matching this config
    combo = {
        "anchor":       cfg["anchor"],
        "continuation": cfg["continuation"],
        "gap":          cfg["gap"],
        "indicator":    cfg["indicator"],
        "direction":    direction,
        "anchor2":      None,
        "continuation2":None,
    }

    from candlelab_core.patterns import PATTERNS
    ANCHOR_MAP = {
        "shooting_star": "Shooting Star/Inv. Hammer",
        "hammer":        "Hammer/Hanging Man",
        "engulfing":     "Engulfing",
        "morning_star":  "Morning/Evening Star",
        "evening_star":  "Morning/Evening Star",
    }
    anchor_col = ANCHOR_MAP.get(cfg["anchor"], cfg["anchor"])
    if anchor_col not in signals_df.columns:
        print(f"  ERROR: anchor column '{anchor_col}' not found in signals_df")
        return []

    sig_val = 1 if direction == "long" else -1
    sig_array    = signals_df[anchor_col].to_numpy() if anchor_col in signals_df.columns else np.zeros(len(df))
    anchor_array = sig_array.copy()

    inds = {
        "atr":      atr_arr,
        "sma_fast": full_sma_fast,
        "sma_slow": full_sma_slow,
        "adx":      df["adx"].to_numpy() if "adx" in df.columns else np.zeros(len(df)),
        "bbw":      df["bbw"].to_numpy() if "bbw" in df.columns else np.zeros(len(df)),
    }

    sig_array, anchor_array = _apply_indicator_filter(sig_array, anchor_array, combo, inds, df)

    trades = sweep_simulation(
        df, sig_array, anchor_array, direction, pip_size,
        atr_arr, avg_spread,
        timeout_bars=TIMEOUT_BARS,
        tp_mult=TP_MULT,
        sl_mult=SL_MULT,
        sl_mode=sl_mode,
        adx_arr=inds["adx"],
        bbw_arr=inds["bbw"],
        profile=profile_from_combo(combo),
        instrument=instrument,
    )
    return trades


def print_report(rows: list[dict]) -> None:
    print(f"\n{'='*80}")
    print(f"{'TIMEOUT DISTRIBUTION ANALYSIS':^80}")
    print(f"{'='*80}")
    print(f"{'ID':>4} {'Instrument':>10} {'Status':>6} {'N':>5} {'Win%':>6} {'p50':>5} {'p75':>5} {'p80':>5} {'p90':>5} {'Rec':>5}")
    print("-" * 80)
    for r in rows:
        print(
            f"{r['strategy_id']:>4} {r['instrument']:>10} {r['status']:>6} "
            f"{r['n_trades']:>5} {r['win_pct']:>5.1f}% "
            f"{r['p50']:>5.0f} {r['p75']:>5.0f} {r['p80']:>5.0f} {r['p90']:>5.0f} "
            f"{r['recommendation']:>5}"
        )
    print(f"{'='*80}")
    print(f"\nCurrent timeout: {TIMEOUT_BARS} bars")


def main() -> None:
    all_rows = []

    for cfg in ACTIVE_CONFIGS:
        instrument = cfg["instrument"]
        print(f"\nAnalyzing {instrument} (strategy {cfg['strategy_id']})...")

        df = load_instrument_data(instrument)
        if df is None:
            continue

        trades = analyze_config(cfg, df)
        if not trades:
            print(f"  No trades generated for {instrument}")
            continue

        resolution_bars = [t["resolution_bar"] for t in trades]
        results         = [t["exit_reason"] for t in trades]
        n               = len(trades)
        win_pct         = 100 * sum(1 for r in results if r == "WIN") / n

        pct_vals = {p: float(np.percentile(resolution_bars, p)) for p in PERCENTILES}
        recommendation  = int(np.ceil(pct_vals[75]))

        print(f"  N={n} | Win%={win_pct:.1f}% | p50={pct_vals[50]:.0f} | p75={pct_vals[75]:.0f} | p80={pct_vals[80]:.0f} | p90={pct_vals[90]:.0f} | Rec={recommendation}")

        all_rows.append({
            "strategy_id":  cfg["strategy_id"],
            "instrument":   instrument,
            "anchor":       cfg["anchor"],
            "direction":    cfg["direction"],
            "status":       cfg["status"],
            "n_trades":     n,
            "win_pct":      round(win_pct, 1),
            "p50":          pct_vals[50],
            "p75":          pct_vals[75],
            "p80":          pct_vals[80],
            "p90":          pct_vals[90],
            "recommendation": recommendation,
        })

    if all_rows:
        print_report(all_rows)
        df_out = pd.DataFrame(all_rows)
        df_out.to_csv(REPORT_PATH, index=False)
        print(f"\nReport written: {REPORT_PATH}")
    else:
        print("\nNo results generated.")


if __name__ == "__main__":
    main()
