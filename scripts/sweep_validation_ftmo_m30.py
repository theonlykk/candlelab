"""
Sweep validation — foundation: DB fetch and signal detection (Prompt 1).
"""

import collections
import hashlib
import os
import json
import uuid
import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
from dotenv import load_dotenv

import sys

sys.path.insert(0, r"d:\oanda-trading")

from regime_filter import (
    ADX_TREND_THRESHOLD,
    ADX_EXTREME_THRESHOLD,
    ADX_CONT_MIN,
    ADX_BLOWOFF_THRESHOLD,
    BBW_DEAD_ZONE_DEFAULT,
    BBW_DEAD_ZONE,
)

from candlelab_core.patterns import (
    engulfing as detect_engulfing,
    hammer_hanging_man as detect_hammer_hanging_man,
    shooting_star_inverted_hammer as detect_shooting_star_inv_hammer,
    detect_inside_bar_breakout,
    detect_1_candle_flag,
    morning_evening_star as detect_morning_evening_star,
)

INSTRUMENTS = [
    "AUD_USD", "NZD_USD", "USD_CHF", "USD_JPY", "USD_CAD", "EUR_USD", "GBP_USD",
    "AUD_JPY", "CAD_JPY", "EUR_JPY", "GBP_JPY", "NZD_JPY", "CHF_JPY",
    "EUR_AUD", "EUR_CAD", "EUR_CHF", "EUR_GBP", "EUR_NZD",
    "GBP_AUD", "GBP_CAD", "GBP_CHF", "GBP_NZD",
    "AUD_CAD", "AUD_NZD", "NZD_CAD", "CAD_CHF",
]
ENABLED_INSTRUMENTS = {
    "AUD_USD": True, "NZD_USD": True, "USD_CHF": True, "USD_JPY": True,
    "USD_CAD": True, "EUR_USD": True, "GBP_USD": True,
    "AUD_JPY": True, "CAD_JPY": True, "EUR_JPY": True, "GBP_JPY": True,
    "NZD_JPY": True, "CHF_JPY": True,
    "EUR_AUD": True, "EUR_CAD": True, "EUR_CHF": True, "EUR_GBP": True,
    "EUR_NZD": True,
    "GBP_AUD": True, "GBP_CAD": True, "GBP_CHF": True, "GBP_NZD": True,
    "AUD_CAD": True, "AUD_NZD": True, "NZD_CAD": True, "CAD_CHF": True,
}
_override = os.environ.get("CANDLELAB_INSTRUMENTS_OVERRIDE")
if _override:
    INSTRUMENTS = [i.strip() for i in _override.split(",")]
    ENABLED_INSTRUMENTS = {i: True for i in INSTRUMENTS}
PIP = {
    "AUD_USD": 0.0001, "NZD_USD": 0.0001, "USD_CHF": 0.0001, "USD_JPY": 0.01,
    "USD_CAD": 0.0001, "EUR_USD": 0.0001, "GBP_USD": 0.0001,
    "AUD_JPY": 0.01, "CAD_JPY": 0.01, "EUR_JPY": 0.01, "GBP_JPY": 0.01,
    "NZD_JPY": 0.01, "CHF_JPY": 0.01,
    "EUR_AUD": 0.0001, "EUR_CAD": 0.0001, "EUR_CHF": 0.0001, "EUR_GBP": 0.0001,
    "EUR_NZD": 0.0001,
    "GBP_AUD": 0.0001, "GBP_CAD": 0.0001, "GBP_CHF": 0.0001, "GBP_NZD": 0.0001,
    "AUD_CAD": 0.0001, "AUD_NZD": 0.0001, "NZD_CAD": 0.0001, "CAD_CHF": 0.0001,
}
PAIR_CONFIG = {
    "AUD_USD": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "NZD_USD": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "USD_CHF": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "USD_JPY": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "USD_CAD": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "EUR_USD": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "GBP_USD": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "AUD_JPY": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "CAD_JPY": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "EUR_JPY": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "GBP_JPY": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "NZD_JPY": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "CHF_JPY": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "EUR_AUD": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "EUR_CAD": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "EUR_CHF": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "EUR_GBP": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "EUR_NZD": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "GBP_AUD": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "GBP_CAD": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "GBP_CHF": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "GBP_NZD": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "AUD_CAD": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "AUD_NZD": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "NZD_CAD": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "CAD_CHF": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
}
GRANULARITY = "M30"
IS_WEEKS = 12
OOS_WEEKS = 4
N_WINDOWS = 27
SQN_MIN_TRADES_IS = 5  # IS promotion floor — new, replaces SQN_MIN_TRADES in IS gate
SQN_MIN_TRADES = 10  # final leaderboard floor — unchanged
SQN_PROMOTE_THRESHOLD = 1.2
TP_MULT = 3.0
SL_MULT = 1.0
TIMEOUT_BARS = 28
# IS Gate v2
SQN_SHRINKAGE_K = 10.0
MIN_TRADES_ELIGIBLE = 3
MIN_TRADES_WATCHLIST = 1
MIN_SQN_ELIGIBLE = 1.0
RELATIVE_SCORE_FRACTION = 0.80
IS_CACHE_VERSION = "v1"  # increment when IS logic changes
ATR_PERIOD = 14
MA_FAST = 10  # was 5
MA_SLOW = 50  # was 20
RSI_PERIOD = 14
RSI_OVERSOLD = 30.0
RSI_OVERBOUGHT = 70.0
DEAD_ZONE_HOURS = frozenset({20, 21, 22, 23})  # new — UTC hours excluded from all signals
OUTPUT_DIR = r"d:\candlelab\scripts\output\ftmo_m30"
import os as _os
_os.makedirs(OUTPUT_DIR, exist_ok=True)
ROSTER_FILE = r"d:\candlelab\scripts\output\ftmo_m30\deployment_roster.json"

ANCHORS = ["engulfing", "hammer", "shooting_star", "morning_star"]
CONTINUATIONS = [None, "inside_bar", "one_candle_flag"]  # one_candle_flag re-enabled for AUD_JPY validation
CONTINUATION_GAPS = [2, 4, 6]  # new axis — only applies when continuation is not None
INDICATORS = [None, "rsi_envelope", "ma_cross"]
DIRECTIONS = ["long", "short"]

PATTERN_FN = {
    "engulfing": detect_engulfing,
    "hammer": detect_hammer_hanging_man,
    "shooting_star": detect_shooting_star_inv_hammer,
    "inside_bar": detect_inside_bar_breakout,
    "one_candle_flag": detect_1_candle_flag,
    "morning_star": detect_morning_evening_star,
}

def build_combo_list() -> list[dict]:
    """
    Unique strategy combos: 126 reversal-anchored (after dedup) plus 8 pure
    continuation rows (anchor=None) = 134 total. Gap is stored per row; when
    continuation is None, gap is always None so gap axis does not multiply
    identical sweeps.
    """
    ENABLED_TOPOLOGIES = {
        "1R": True,
        "1R+1C": True,
        "1C": True,
        "2R": True,  # cluster topology — no edge found; set True to re-research
        "2R+1C": False,  # cluster topology — no edge found; set True to re-research
        "2C": False,  # cluster topology — no edge found; set True to re-research
    }
    combos = []
    for anchor in ANCHORS:
        for continuation in CONTINUATIONS:
            for gap in CONTINUATION_GAPS:
                for indicator in INDICATORS:
                    for direction in DIRECTIONS:
                        combos.append(
                            {
                                "anchor": anchor,
                                "continuation": continuation,
                                "gap": gap if continuation is not None else None,
                                "indicator": indicator,
                                "direction": direction,
                            }
                        )
    seen = set()
    deduped = []
    for c in combos:
        key = (
            c.get("anchor"),
            c.get("anchor2"),
            c.get("continuation"),
            c.get("continuation2"),
            c["gap"],
            c["indicator"],
            c["direction"],
        )
        if key not in seen:
            seen.add(key)
            deduped.append(c)

    if not ENABLED_TOPOLOGIES["1R"] or not ENABLED_TOPOLOGIES["1R+1C"]:
        deduped = [
            c
            for c in deduped
            if (ENABLED_TOPOLOGIES["1R"] or c["continuation"] is not None)
            and (ENABLED_TOPOLOGIES["1R+1C"] or c["continuation"] is None)
        ]

    # Pure continuation combos (anchor=None) — 8 additional rows
    # 2 continuations × 1 gap (None) × 2 directions × 2 indicator states = 8
    # Gap is irrelevant without an anchor — there is nothing to measure digestion from
    pure_continuations = ["inside_bar"]  # one_candle_flag benched
    pure_cont_indicators = [None, "rsi_envelope"]  # ma_cross replaced by trend alignment

    if ENABLED_TOPOLOGIES["1C"]:
        for continuation in pure_continuations:
            for direction in DIRECTIONS:
                for indicator in pure_cont_indicators:
                    deduped.append(
                        {
                            "anchor": None,
                            "continuation": continuation,
                            "gap": None,
                            "indicator": indicator,
                            "direction": direction,
                            "pure_cont": True,
                        }
                    )

    # ── 2R combos (dual reversal cluster, any-order 10-bar window) ──
    anchor_pairs = [
        ("engulfing", "hammer"),
        ("engulfing", "shooting_star"),
        ("hammer", "shooting_star"),
    ]
    if ENABLED_TOPOLOGIES["2R"]:
        for a1, a2 in anchor_pairs:
            for indicator in INDICATORS:
                for direction in DIRECTIONS:
                    deduped.append(
                        {
                            "anchor": a1,
                            "anchor2": a2,
                            "continuation": None,
                            "continuation2": None,
                            "gap": None,
                            "indicator": indicator,
                            "direction": direction,
                            "combo_type": "2R",
                        }
                    )

    # ── 2R+1C combos ──
    if ENABLED_TOPOLOGIES["2R+1C"]:
        for a1, a2 in anchor_pairs:
            for continuation in ["inside_bar", "one_candle_flag"]:
                for gap in CONTINUATION_GAPS:
                    for indicator in INDICATORS:
                        for direction in DIRECTIONS:
                            deduped.append(
                                {
                                    "anchor": a1,
                                    "anchor2": a2,
                                    "continuation": continuation,
                                    "continuation2": None,
                                    "gap": gap,
                                    "indicator": indicator,
                                    "direction": direction,
                                    "combo_type": "2R+1C",
                                }
                            )

    # ── 2C combos (dual continuation cluster, any-order 10-bar window) ──
    if ENABLED_TOPOLOGIES["2C"]:
        for indicator in [None, "rsi_envelope"]:
            for direction in DIRECTIONS:
                deduped.append(
                    {
                        "anchor": None,
                        "anchor2": None,
                        "continuation": "inside_bar",
                        "continuation2": "one_candle_flag",
                        "gap": None,
                        "indicator": indicator,
                        "direction": direction,
                        "pure_cont": True,
                        "combo_type": "2C",
                    }
                )

    print(f"[build_combo_list] Total combos: {len(deduped)}")
    return deduped


def _make_block_hash(
    instrument: str,
    granularity: str,
    oos_start: pd.Timestamp,
    oos_end: pd.Timestamp,
    combo: dict,
    timeout: int,
    tp_mult: float,
    sl_mult: float,
    sl_mode: str,
) -> str:
    """
    Deterministic SHA-256 hash identifying a unique OOS evaluation block.
    All float params rounded to 6dp. Dict keys sorted for stability.
    """
    payload = {
        "instrument": instrument,
        "granularity": granularity,
        "oos_start": oos_start.isoformat(),
        "oos_end": oos_end.isoformat(),
        "combo": {k: round(v, 6) if isinstance(v, float) else v
                  for k, v in sorted(combo.items())},
        "timeout": timeout,
        "tp_mult": round(tp_mult, 6),
        "sl_mult": round(sl_mult, 6),
        "sl_mode": sl_mode,
    }
    raw = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode()).hexdigest()


def _cache_lookup(block_hash: str, conn) -> dict | None:
    """
    Returns cached OOS result dict if found, else None.
    Never raises — cache misses are silent.
    """
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SAVEPOINT cache_lookup")
            cur.execute(
                "SELECT result_json FROM sweep_oos_cache WHERE block_hash = %s",
                (block_hash,),
            )
            row = cur.fetchone()
            result = row["result_json"] if row else None
            cur.execute("RELEASE SAVEPOINT cache_lookup")
        return result
    except Exception as e:
        with conn.cursor() as rollback_cur:
            rollback_cur.execute("ROLLBACK TO SAVEPOINT cache_lookup")
        print(f"  [cache] lookup error (non-fatal): {e}")
        return None


def _cache_store(block_hash: str, instrument: str, oos_start: pd.Timestamp,
                 oos_end: pd.Timestamp, combo: dict, timeout: int,
                 oos_r: list[float], conn) -> None:
    """
    Stores OOS result in cache. Silently skips on conflict (already cached).
    Never raises — cache write failures are non-fatal.
    """
    n = int(len(oos_r))
    mean_r = float(np.mean(oos_r)) if n > 0 else 0.0
    std_r = float(np.std(oos_r, ddof=1)) if n >= 2 else 0.0
    sqn = float(round((mean_r / std_r) * np.sqrt(min(n, 100)), 4)) if std_r > 0 else 0.0

    result = {
        "oos_r_list": [float(r) for r in oos_r],
        "oos_n_trades": n,
        "oos_mean_r": mean_r,
        "oos_sqn100": sqn,
    }
    try:
        with conn.cursor() as cur:
            cur.execute("SAVEPOINT cache_store")
            cur.execute(
                """
                INSERT INTO sweep_oos_cache
                    (block_hash, instrument, granularity, oos_start, oos_end,
                     config_hash, signal_count, mean_r, sqn100, result_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (block_hash) DO NOTHING
                """,
                (
                    block_hash,
                    instrument,
                    GRANULARITY,
                    oos_start,
                    oos_end,
                    block_hash[:16],
                    n,
                    mean_r,
                    sqn,
                    json.dumps(result),
                ),
            )
            cur.execute("RELEASE SAVEPOINT cache_store")
        conn.commit()
    except Exception as e:
        with conn.cursor() as rollback_cur:
            rollback_cur.execute("ROLLBACK TO SAVEPOINT cache_store")
        print(f"  [cache] store error (non-fatal): {e}")


def _load_window_is_cache(
    instrument: str,
    granularity: str,
    is_start: pd.Timestamp,
    is_end: pd.Timestamp,
    timeout: int,
    conn,
) -> dict:
    """
    Bulk-fetch all cached IS results for a single window in one
    network round-trip. Returns a dict keyed by combo signature
    tuple for O(1) per-combo lookup.

    Key: (direction, anchor, anchor2, continuation, continuation2,
           gap, indicator, combo_type)
    Value: dict of IS metrics

    Returns empty dict on any error — non-fatal, falls through to
    full IS simulation.
    """
    sql = """
        SELECT anchor, anchor2, continuation, continuation2,
               gap, indicator, direction, combo_type,
               adjusted_score_is, initial_bucket, final_bucket,
               oos_eligible, trade_count_is, mean_r_is, sqn_is,
               is_r_list
        FROM sweep_is_results
        WHERE instrument = %s
        AND granularity = %s
        AND window_start = %s
        AND window_end = %s
        AND is_cache_version = %s
        AND timeout_bars = %s
        AND tp_mult = %s
        AND sl_mult = %s
    """
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SAVEPOINT window_is_cache_load")
            cur.execute(sql, (
                instrument, granularity,
                is_start, is_end,
                IS_CACHE_VERSION,
                int(timeout), float(TP_MULT), float(SL_MULT),
            ))
            rows = cur.fetchall()
            cur.execute("RELEASE SAVEPOINT window_is_cache_load")

        cache = {}
        for row in rows:
            key = (
                row["direction"],
                row["anchor"],
                row["anchor2"],
                row["continuation"],
                row["continuation2"],
                row["gap"],
                row["indicator"],
                row["combo_type"],
            )
            r_mults = list(row["is_r_list"]) if row["is_r_list"] else []
            cache[key] = {
                "adjusted_score": float(row["adjusted_score_is"]),
                "initial_bucket": row["initial_bucket"],
                "final_bucket":   row["final_bucket"],
                "oos_eligible":   bool(row["oos_eligible"]),
                "n_trades":       int(row["trade_count_is"]),
                "mean_r":         float(row["mean_r_is"]),
                "raw_sqn":        float(row["sqn_is"]),
                "r_mults":        r_mults,
            }
        return cache

    except Exception as e:
        with conn.cursor() as rc:
            rc.execute("ROLLBACK TO SAVEPOINT window_is_cache_load")
        print(f"  [is_cache] window load error (non-fatal): {e}")
        return {}


def _write_is_results(
    is_rows: list[dict],
    instrument: str,
    window_id: int,
    window_start,
    window_end,
    timeout: int,
    run_id: str,
    batch_id: str,
    conn,
) -> None:
    """
    Write all IS evaluation results to sweep_is_results.
    Uses ON CONFLICT DO NOTHING — safe to re-run.
    Never raises — write failure is non-fatal.
    """
    sql = """
        INSERT INTO sweep_is_results (
            run_id, batch_id,
            instrument, granularity, window_id, window_start, window_end,
            anchor, anchor2, continuation, continuation2,
            gap, indicator, direction, combo_type, pure_cont,
            timeout_bars, tp_mult, sl_mult,
            trade_count_is, mean_r_is, sqn_is, net_r_is,
            adjusted_score_is, initial_bucket, passes_band,
            final_bucket, oos_eligible,
            is_cache_version, is_r_list
        ) VALUES (
            %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s
        )
        ON CONFLICT DO NOTHING
    """
    try:
        with conn.cursor() as cur:
            cur.execute("SAVEPOINT is_results_write")
            for row in is_rows:
                combo = row["combo"]
                r_list = row.get("r_mults", [])
                net_r = float(sum(r_list)) if r_list else 0.0
                cur.execute(sql, (
                    run_id, batch_id,
                    instrument, GRANULARITY, window_id,
                    window_start, window_end,
                    combo.get("anchor"), combo.get("anchor2"),
                    combo.get("continuation"), combo.get("continuation2"),
                    combo.get("gap"), combo.get("indicator"),
                    combo.get("direction"), combo.get("combo_type"),
                    bool(combo.get("pure_cont", False)),
                    int(timeout), float(TP_MULT), float(SL_MULT),
                    int(row["n_trades"]),
                    float(row["mean_r"]),
                    float(row["raw_sqn"]),
                    float(net_r),
                    float(row["adjusted_score"]),
                    row["initial_bucket"],
                    bool(row["passes_band"]),
                    row["final_bucket"],
                    bool(row["oos_eligible"]),
                    IS_CACHE_VERSION,
                    json.dumps([float(r) for r in row.get("r_mults", [])]),
                ))
            cur.execute("RELEASE SAVEPOINT is_results_write")
        conn.commit()
        print(f"  [is_results] wrote {len(is_rows)} rows")
    except Exception as e:
        with conn.cursor() as rollback_cur:
            rollback_cur.execute(
                "ROLLBACK TO SAVEPOINT is_results_write"
            )
        print(f"  [is_results] write error (non-fatal): {e}")


def fetch_instrument_data(instrument: str, conn) -> pd.DataFrame:
    """Load M30 candles from ftmo_candles with real measured spread_points."""
    sql = """
SELECT time, open, high, low, close, volume,
       (ask_close - bid_close) AS spread_points
FROM oanda_candles
WHERE instrument = %s
  AND granularity = %s
ORDER BY time ASC
"""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (instrument, GRANULARITY))
        rows = cur.fetchall()

    df = pd.DataFrame(rows)

    if len(df) == 0:
        raise ValueError(
            "fetch_instrument_data: no rows returned for instrument "
            + repr(instrument)
        )

    df["time"] = pd.to_datetime(df["time"], utc=True)
    df = df.set_index("time").sort_index()

    df["bid_open"] = df["open"]
    df["ask_open"] = df["open"]
    df["bid_close"] = df["close"]
    df["ask_close"] = df["close"]

    return df


def _cluster_detect(
    a: np.ndarray,
    b: np.ndarray,
    dead_mask: np.ndarray,
    dir_val: int,
    window: int = 10,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Fully vectorized either-order cluster detection.
    O(N) complexity. No Python for-loops.
    A signal fires on bar j when pattern B fires and pattern A fired
    within the preceding `window` bars (or vice versa).
    Dead zone bars are zeroed out after detection.
    """
    n = len(a)
    indices = np.arange(n)

    a_hit = a == dir_val
    b_hit = b == dir_val

    # Track the exact index of the last A fire (forward-filled)
    a_last = np.full(n, -1, dtype=np.int64)
    a_last[a_hit] = indices[a_hit]
    a_last = np.maximum.accumulate(a_last)

    # Track the exact index of the last B fire (forward-filled)
    b_last = np.full(n, -1, dtype=np.int64)
    b_last[b_hit] = indices[b_hit]
    b_last = np.maximum.accumulate(b_last)

    # B fires now, A fired strictly before but within window
    b_fires_after_a = (
        b_hit
        & (a_last != -1)
        & ((indices - a_last) < window)
        & (a_last != indices)
    )

    # A fires now, B fired strictly before but within window
    a_fires_after_b = (
        a_hit
        & (b_last != -1)
        & ((indices - b_last) < window)
        & (b_last != indices)
    )

    # Construct output arrays
    sig_mask = (b_fires_after_a | a_fires_after_b) & (~dead_mask)

    sig_array = np.zeros(n, dtype=np.int64)
    sig_array[sig_mask] = dir_val

    anchor_array = np.zeros(n, dtype=np.int64)
    anchor_array[b_fires_after_a] = a_last[b_fires_after_a]
    anchor_array[a_fires_after_b] = b_last[a_fires_after_b]

    return sig_array, anchor_array


def detect_signals(
    df,
    anchor: str | None,
    continuation,
    direction,
    gap: int = 5,
    anchor2: str | None = None,
    continuation2: str | None = None,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Per-bar signals (+1 long, -1 short, 0 none) and anchor bar index.
    gap: max bars to search forward for continuation pattern (ignored if continuation=None).
    Dead zone guard: bars in DEAD_ZONE_HOURS (21, 22, 23 UTC) are zeroed out.
    """
    if direction == "long":
        dir_val = 1
    elif direction == "short":
        dir_val = -1
    else:
        raise ValueError("direction must be 'long' or 'short', got " + repr(direction))

    n = len(df)
    sig_array = np.zeros(n, dtype=np.int64)
    anchor_array = np.zeros(n, dtype=np.int64)

    # Dead zone mask — applied to ALL signals regardless of continuation
    if isinstance(df.index, pd.DatetimeIndex):
        dead_mask = df.index.hour.isin(DEAD_ZONE_HOURS)
    else:
        dead_mask = np.zeros(n, dtype=bool)

    # 2C — dual continuation cluster (any-order, 10-bar window)
    if anchor is None and continuation is not None and continuation2 is not None:
        c1 = PATTERN_FN[continuation](df).fillna(0).astype(np.int64).to_numpy()
        c2 = PATTERN_FN[continuation2](df).fillna(0).astype(np.int64).to_numpy()
        return _cluster_detect(c1, c2, dead_mask, dir_val)

    # 2R — dual reversal cluster (any-order, 10-bar window)
    if anchor is not None and anchor2 is not None and continuation is None:
        a1 = PATTERN_FN[anchor](df).fillna(0).astype(np.int64).to_numpy()
        a2 = PATTERN_FN[anchor2](df).fillna(0).astype(np.int64).to_numpy()
        return _cluster_detect(a1, a2, dead_mask, dir_val)

    # 2R+1C — dual reversal cluster + single continuation
    if anchor is not None and anchor2 is not None and continuation is not None:
        a1 = PATTERN_FN[anchor](df).fillna(0).astype(np.int64).to_numpy()
        a2 = PATTERN_FN[anchor2](df).fillna(0).astype(np.int64).to_numpy()
        cluster_sig, cluster_anc = _cluster_detect(a1, a2, dead_mask, dir_val)
        c = PATTERN_FN[continuation](df).fillna(0).astype(np.int64).to_numpy()
        sig_array = np.zeros(n, dtype=np.int64)
        anchor_array = np.zeros(n, dtype=np.int64)
        cluster_indices = np.where(cluster_sig == dir_val)[0]
        for i in cluster_indices:
            j_end = min(int(i) + gap, n - 1)
            for j in range(int(i) + 1, j_end + 1):
                if c[j] == dir_val and not dead_mask[j]:
                    sig_array[j] = dir_val
                    anchor_array[j] = cluster_anc[i]
                    break
        return sig_array, anchor_array

    # Handle pure continuation combos (anchor=None)
    if anchor is None:
        if continuation is None:
            raise ValueError("anchor=None requires a continuation pattern")
        cont_series = PATTERN_FN[continuation](df).fillna(0).astype(np.int64)
        c = cont_series.to_numpy()
        for i in range(n):
            if c[i] == dir_val and not dead_mask[i]:
                sig_array[i] = dir_val
                anchor_array[i] = i  # anchor_idx == signal_idx for pure cont
        return sig_array, anchor_array

    anchor_series = PATTERN_FN[anchor](df).fillna(0).astype(np.int64)
    a = anchor_series.to_numpy()

    if continuation is None:
        for i in range(n):
            if a[i] == dir_val and not dead_mask[i]:
                sig_array[i] = dir_val
                anchor_array[i] = i
        return sig_array, anchor_array

    cont_series = PATTERN_FN[continuation](df).fillna(0).astype(np.int64)
    c = cont_series.to_numpy()

    for i in range(n):
        if a[i] != dir_val:
            continue
        j_end = min(i + gap, n - 1)
        if i + 1 > j_end:
            continue
        for j in range(i + 1, j_end + 1):
            if c[j] == dir_val and not dead_mask[j]:
                sig_array[j] = dir_val
                anchor_array[j] = i
                break

    return sig_array, anchor_array


def wilder_rma(series: np.ndarray, period: int) -> np.ndarray:
    result = np.full(len(series), np.nan)
    if len(series) < period:
        return result
    result[period - 1] = np.mean(series[:period])
    alpha = 1.0 / period
    for i in range(period, len(series)):
        result[i] = alpha * series[i] + (1 - alpha) * result[i - 1]
    return result


def compute_atr(df: pd.DataFrame, period: int = ATR_PERIOD) -> np.ndarray:
    high = df["high"].values
    low = df["low"].values
    close = df["close"].values
    tr = np.maximum(
        high[1:] - low[1:],
        np.maximum(
            np.abs(high[1:] - close[:-1]),
            np.abs(low[1:] - close[:-1]),
        ),
    )
    tr = np.concatenate([[high[0] - low[0]], tr])
    return wilder_rma(tr, period)


def compute_indicators(df: pd.DataFrame) -> dict:
    close = df["close"].to_numpy()

    delta = np.diff(close, prepend=close[0])
    gain = np.where(delta > 0, delta, 0.0)
    loss = np.where(delta < 0, -delta, 0.0)
    avg_gain = wilder_rma(gain, RSI_PERIOD)
    avg_loss = wilder_rma(loss, RSI_PERIOD)
    rs = np.where(avg_loss == 0, np.inf, avg_gain / avg_loss)
    rsi = 100.0 - (100.0 / (1.0 + rs))

    sma_fast = df["close"].rolling(MA_FAST).mean().to_numpy()
    sma_slow = df["close"].rolling(MA_SLOW).mean().to_numpy()

    atr = compute_atr(df)

    return {"rsi": rsi, "sma_fast": sma_fast, "sma_slow": sma_slow, "atr": atr}


def passes_rsi_envelope(
    rsi: np.ndarray,
    signal_idx: int,
    anchor_idx: int,
    direction: str,
) -> bool:
    look_start = max(0, anchor_idx - 5)
    look_end = min(len(rsi) - 1, anchor_idx + 10)
    window_rsi = rsi[look_start : look_end + 1]
    if np.any(np.isnan(window_rsi)):
        return False
    if signal_idx < 1 or signal_idx >= len(rsi):
        return False
    if direction == "long":
        phase1 = bool(np.any(window_rsi <= RSI_OVERSOLD))
        phase3 = bool(rsi[signal_idx] > rsi[signal_idx - 1])
    else:
        phase1 = bool(np.any(window_rsi >= RSI_OVERBOUGHT))
        phase3 = bool(rsi[signal_idx] < rsi[signal_idx - 1])
    return phase1 and phase3


def passes_ma_cross(
    sma_fast: np.ndarray,
    sma_slow: np.ndarray,
    signal_idx: int,
    anchor_idx: int,
    direction: str,
    has_continuation: bool,
) -> bool:
    start = anchor_idx if has_continuation else max(0, signal_idx - 5)
    end = signal_idx
    if end < start or end >= len(sma_fast):
        return False
    fast_w = sma_fast[start : end + 1]
    slow_w = sma_slow[start : end + 1]
    if np.any(np.isnan(fast_w)) or np.any(np.isnan(slow_w)):
        return False
    if direction == "long":
        phase1 = bool(np.any(fast_w < slow_w))
        crosses = (fast_w[:-1] < slow_w[:-1]) & (fast_w[1:] >= slow_w[1:])
        phase2 = bool(np.any(crosses))
        phase3 = bool(sma_fast[signal_idx] > sma_slow[signal_idx])
    else:
        phase1 = bool(np.any(fast_w > slow_w))
        crosses = (fast_w[:-1] > slow_w[:-1]) & (fast_w[1:] <= slow_w[1:])
        phase2 = bool(np.any(crosses))
        phase3 = bool(sma_fast[signal_idx] < sma_slow[signal_idx])
    return phase1 and phase2 and phase3


def passes_trend_alignment(
    sma_fast: np.ndarray,
    sma_slow: np.ndarray,
    df: pd.DataFrame,
    signal_idx: int,
    direction: str,
) -> bool:
    """
    Full-stack trend alignment for pure continuation combos (anchor=None).
    Requires BOTH conditions at the signal bar:
      Long:  bid_close > sma_slow  AND  sma_fast > sma_slow
      Short: ask_close < sma_slow  AND  sma_fast < sma_slow

    Uses bid_close/ask_close (not mid-price) for OANDA BAM retail parity.
    Returns False if any value is NaN or signal_idx is out of bounds.
    """
    if signal_idx < 0 or signal_idx >= len(sma_fast):
        return False
    if np.isnan(sma_fast[signal_idx]) or np.isnan(sma_slow[signal_idx]):
        return False

    fast_val = sma_fast[signal_idx]
    slow_val = sma_slow[signal_idx]

    if direction == "long":
        price = float(df["bid_close"].iloc[signal_idx])
        return bool(price > slow_val and fast_val > slow_val)
    else:
        price = float(df["ask_close"].iloc[signal_idx])
        return bool(price < slow_val and fast_val < slow_val)


def sweep_simulation(
    df: pd.DataFrame,
    sig_array: np.ndarray,
    anchor_array: np.ndarray,
    direction: str,
    pip_size: float,
    atr: np.ndarray,
    avg_spread: float,
    timeout_bars: int = TIMEOUT_BARS,
    tp_mult: float = TP_MULT,
    sl_mult: float = SL_MULT,
    sl_mode: str = "standard",
    initial_capital: float = 10_000.0,
    risk_pct: float = 0.01,
    adx_arr: np.ndarray | None = None,
    bbw_arr: np.ndarray | None = None,
    profile: str = "Hybrid",
    instrument: str = "",
) -> list[dict]:
    n = len(df)
    high = df["high"].to_numpy()
    low = df["low"].to_numpy()
    bid_open = df["bid_open"].to_numpy()
    ask_open = df["ask_open"].to_numpy()
    bid_close = df["bid_close"].to_numpy()
    ask_close = df["ask_close"].to_numpy()

    if direction == "long":
        sig_val = 1
    elif direction == "short":
        sig_val = -1
    else:
        raise ValueError("direction must be 'long' or 'short', got " + repr(direction))

    trades: list[dict] = []
    block_fill = -1
    block_exit = -1
    current_equity = initial_capital

    for sig_idx in range(n):
        if int(sig_array[sig_idx]) != sig_val:
            continue
        if block_exit >= 0 and block_fill <= sig_idx <= block_exit:
            continue

        fill_idx = sig_idx + 1
        if fill_idx >= n:
            continue

        if adx_arr is not None and bbw_arr is not None:
            _adx = (
                float(adx_arr[sig_idx])
                if np.isfinite(adx_arr[sig_idx])
                else None
            )
            _bbw = (
                float(bbw_arr[sig_idx])
                if np.isfinite(bbw_arr[sig_idx])
                else None
            )

            _bbw_threshold = BBW_DEAD_ZONE.get(
                instrument, BBW_DEAD_ZONE_DEFAULT
            )
            if _bbw is not None and 0 < _bbw < _bbw_threshold:
                continue

            if _adx is not None:
                if profile == "Counter-Trend":
                    if _adx >= ADX_TREND_THRESHOLD:
                        continue
                elif profile == "Hybrid":
                    if _adx >= ADX_EXTREME_THRESHOLD:
                        continue
                elif profile == "Pro-Trend":
                    if _adx < ADX_CONT_MIN:
                        continue
                    if _adx >= ADX_BLOWOFF_THRESHOLD:
                        continue

        if direction == "long":
            entry = float(ask_open[fill_idx])
        else:
            entry = float(bid_open[fill_idx])

        atr_i = atr[sig_idx]
        if not np.isfinite(atr_i):
            continue

        if sl_mode == "atr_only":
            sl_dist = float(atr_i)
        else:
            sl_dist = max(5.0 * pip_size, float(atr_i))
        spread_cost = avg_spread / sl_dist

        if direction == "long":
            sl_price = entry - sl_dist * sl_mult
            tp_price = entry + sl_dist * tp_mult
        else:
            sl_price = entry + sl_dist * sl_mult
            tp_price = entry - sl_dist * tp_mult

        bars_remaining = max(0, (sig_idx + timeout_bars - fill_idx) - 1)

        r_multiple = float("nan")
        result = ""
        exit_bar = fill_idx

        if bars_remaining <= 0:
            exit_bar = fill_idx
            if exit_bar >= n:
                continue
            if direction == "long":
                exit_price = float(bid_close[exit_bar])
                r_multiple = (exit_price - entry) / sl_dist
            else:
                exit_price = float(ask_close[exit_bar])
                r_multiple = (entry - exit_price) / sl_dist
            result = "TIMEOUT"
        else:
            broke = False
            for j in range(1, bars_remaining + 1):
                bar = fill_idx + j
                if bar >= n:
                    exit_bar = min(fill_idx + bars_remaining, n - 1)
                    exit_bar = max(exit_bar, fill_idx)
                    if direction == "long":
                        exit_price = float(bid_close[exit_bar])
                        r_multiple = (exit_price - entry) / sl_dist
                    else:
                        exit_price = float(ask_close[exit_bar])
                        r_multiple = (entry - exit_price) / sl_dist
                    result = "TIMEOUT"
                    broke = True
                    break
                if direction == "long":
                    if low[bar] <= sl_price:
                        r_multiple = -sl_mult - spread_cost
                        result = "LOSS"
                        exit_bar = bar
                        broke = True
                        break
                    if high[bar] >= tp_price:
                        r_multiple = tp_mult - spread_cost
                        result = "WIN"
                        exit_bar = bar
                        broke = True
                        break
                else:
                    if high[bar] >= sl_price:
                        r_multiple = -sl_mult - spread_cost
                        result = "LOSS"
                        exit_bar = bar
                        broke = True
                        break
                    if low[bar] <= tp_price:
                        r_multiple = tp_mult - spread_cost
                        result = "WIN"
                        exit_bar = bar
                        broke = True
                        break
            if not broke:
                exit_bar = fill_idx + bars_remaining
                if exit_bar >= n:
                    exit_bar = n - 1
                exit_bar = max(exit_bar, fill_idx)
                if direction == "long":
                    exit_price = float(bid_close[exit_bar])
                    r_multiple = (exit_price - entry) / sl_dist
                else:
                    exit_price = float(ask_close[exit_bar])
                    r_multiple = (entry - exit_price) / sl_dist
                result = "TIMEOUT"

        if not np.isfinite(r_multiple):
            continue

        risk_dollars = current_equity * risk_pct
        trade_pnl = risk_dollars * r_multiple
        current_equity += trade_pnl

        trades.append(
            {
                "sig_idx": sig_idx,
                "fill_idx": fill_idx,
                "result": result,
                "r_multiple": float(r_multiple),
                "sl_dist": sl_dist,
                "pnl_dollars": float(trade_pnl),
                "equity_after": float(current_equity),
                "resolution_bar": int(exit_bar - fill_idx),
                "exit_reason": result,
            }
        )
        block_fill = fill_idx
        block_exit = exit_bar

    return trades


def sqn100(r_multiples: list[float], n_min: int = SQN_MIN_TRADES) -> float:
    n = len(r_multiples)
    if n < n_min:
        return 0.0
    if n < 2:
        return 0.0
    r = np.array(r_multiples, dtype=float)
    mean_r = np.mean(r)
    std_r = np.std(r, ddof=1)
    if std_r == 0.0 or not np.isfinite(std_r):
        return 0.0
    return round((mean_r / std_r) * np.sqrt(min(n, 100)), 4)


def compute_mdd(equity_curve: list[float]) -> float:
    """
    Maximum drawdown as a fraction (e.g. 0.15 = 15% drawdown).
    Returns 0.0 if fewer than 2 data points.
    """
    if len(equity_curve) < 2:
        return 0.0
    eq = np.array(equity_curve, dtype=float)
    peak = np.maximum.accumulate(eq)
    drawdowns = (eq - peak) / peak
    return float(abs(drawdowns.min()))


def compute_sharpe(equity_curve: list[float], n_trades: int = 0) -> float:
    """
    Annualized Sharpe ratio from trade-by-trade equity changes.
    Assumes risk-free rate = 0%.
    Annualisation factor = sqrt(n_trades) where n_trades is the total
    number of OOS trades for this combo across the full backtest window.
    Returns 0.0 if fewer than 2 data points or zero volatility.
    """
    if len(equity_curve) < 2 or n_trades < 2:
        return 0.0
    eq = np.array(equity_curve, dtype=float)
    returns = np.diff(eq) / eq[:-1]
    if len(returns) == 0:
        return 0.0
    std = np.std(returns, ddof=1)
    if std == 0.0:
        return 0.0
    mean_return = np.mean(returns)
    return float((mean_return / std) * np.sqrt(n_trades))


def _apply_indicator_filter(
    sig: np.ndarray,
    anc: np.ndarray,
    combo: dict,
    inds: dict,
    df: pd.DataFrame,
) -> tuple[np.ndarray, np.ndarray]:
    is_pure_cont = combo.get("pure_cont", False)
    filtered_sig = sig.copy()
    filtered_anc = anc.copy()
    signal_indices = np.where(sig != 0)[0]
    for si in signal_indices:
        si = int(si)
        ai = int(anc[si])

        if is_pure_cont:
            if not passes_trend_alignment(
                inds["sma_fast"],
                inds["sma_slow"],
                df,
                si,
                combo["direction"],
            ):
                filtered_sig[si] = 0
                filtered_anc[si] = 0
                continue
            if combo["indicator"] == "rsi_envelope":
                if not passes_rsi_envelope(
                    inds["rsi"], si, ai, combo["direction"]
                ):
                    filtered_sig[si] = 0
                    filtered_anc[si] = 0
        else:
            if combo["indicator"] == "rsi_envelope":
                if not passes_rsi_envelope(
                    inds["rsi"], si, ai, combo["direction"]
                ):
                    filtered_sig[si] = 0
                    filtered_anc[si] = 0
            elif combo["indicator"] == "ma_cross":
                if not passes_ma_cross(
                    inds["sma_fast"],
                    inds["sma_slow"],
                    si,
                    ai,
                    combo["direction"],
                    combo["continuation"] is not None,
                ):
                    filtered_sig[si] = 0
                    filtered_anc[si] = 0
    return filtered_sig, filtered_anc


def compute_regime_features(
    df: pd.DataFrame, adx_period: int = 14, bbw_period: int = 20
) -> pd.DataFrame:
    """
    Vectorized ADX + BBW computation using pandas ewm.
    Mathematically identical to Wilder smoothing (alpha=1/period,
    adjust=False). Converges with the 3-phase loop after ~150 bars.
    Warmup drift is irrelevant for 177k-bar datasets.

    Adds columns: df['adx'], df['bbw']
    Expects columns: df['high'], df['low'], df['close']
    """
    tr1 = df["high"] - df["low"]
    tr2 = (df["high"] - df["close"].shift(1)).abs()
    tr3 = (df["low"] - df["close"].shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    up = df["high"].diff()
    down = df["low"].shift(1) - df["low"]

    plus_dm = np.where((up > down) & (up > 0), up, 0.0)
    minus_dm = np.where((down > up) & (down > 0), down, 0.0)

    alpha = 1.0 / adx_period

    atr = tr.ewm(alpha=alpha, adjust=False).mean()
    plus_di = (
        100
        * pd.Series(plus_dm, index=df.index).ewm(alpha=alpha, adjust=False).mean()
        / atr
    )
    minus_di = (
        100
        * pd.Series(minus_dm, index=df.index).ewm(alpha=alpha, adjust=False).mean()
        / atr
    )

    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)

    df = df.copy()
    df["adx"] = dx.ewm(alpha=alpha, adjust=False).mean().round(2)

    rolling = df["close"].rolling(window=bbw_period)
    sma = rolling.mean()
    std = rolling.std()
    df["bbw"] = np.where(sma != 0, (4.0 * std) / sma, np.nan)
    df["bbw"] = df["bbw"].round(6)

    return df


def profile_from_combo(combo: dict) -> str:
    """
    Map a sweep combo dict to its execution profile.
    Uses the same recipe logic as TOLERANCE_MAP in Work server.py.

    Recipe derivation:
      has_cluster → 2R
      r_count==1  → R
      r_count>=2  → 2R
      c_count==1  → recipe + '+C' or 'C'
      c_count>=2  → recipe + '+2C' or '2C'

    Profile mapping:
      R, 2R       → Counter-Trend
      R+C, 2R+C   → Hybrid
      C, 2C       → Pro-Trend
    """
    REVERSAL_PATTERNS = {
        "engulfing",
        "hammer",
        "hanging_man",
        "morning_star",
        "evening_star",
        "shooting_star",
        "inverted_hammer",
    }
    CONTINUATION_PATTERNS = {
        "inside_bar",
        "one_candle_flag",
        "three_white_soldiers",
        "three_black_crows",
    }

    anchor = combo.get("anchor") or ""
    anchor2 = combo.get("anchor2") or ""
    continuation = combo.get("continuation") or ""
    has_cluster = combo.get("anchor") == "cluster"

    r_count = sum(1 for p in [anchor, anchor2] if p in REVERSAL_PATTERNS)

    c_count = sum(
        1 for p in [anchor, anchor2, continuation] if p in CONTINUATION_PATTERNS
    )

    if has_cluster:
        recipe = "2R"
    elif r_count == 1:
        recipe = "R"
    elif r_count >= 2:
        recipe = "2R"
    else:
        recipe = ""

    if c_count == 1:
        recipe = (recipe + "+C") if recipe else "C"
    elif c_count >= 2:
        recipe = (recipe + "+2C") if recipe else "2C"

    PROFILE_MAP = {
        "R": "Counter-Trend",
        "2R": "Counter-Trend",
        "R+C": "Hybrid",
        "2R+C": "Hybrid",
        "C": "Pro-Trend",
        "2C": "Pro-Trend",
    }
    return PROFILE_MAP.get(recipe, "Hybrid")


_WFV_COLUMNS = [
    "window_idx",
    "is_start",
    "is_end",
    "oos_start",
    "oos_end",
    "anchor",
    "anchor2",
    "continuation",
    "continuation2",
    "gap",
    "indicator",
    "direction",
    "combo_type",
    "timeout_bars",
    "is_sqn100",
    "is_n_trades",
    "is_mean_r",
    "oos_sqn100",
    "oos_n_trades",
    "oos_mean_r",
    "mdd",
    "sharpe",
]


def run_wfv(
    df: pd.DataFrame,
    instrument: str,
    pip_size: float,
    conn,
    run_id: str,
    batch_id: str,
) -> pd.DataFrame:
    # Spread from oanda_candles (ask_close - bid_close) — already in price units
    avg_spread = float(df["spread_points"].mean())
    df = df.sort_index()

    df = compute_regime_features(df)
    adx_arr = df["adx"].to_numpy()
    bbw_arr = df["bbw"].to_numpy()

    data_end = df.index.max()
    combos = build_combo_list()

    pair_cfg = PAIR_CONFIG.get(instrument, {})
    ma_pairs = pair_cfg.get("ma_pairs", [(MA_FAST, MA_SLOW)])
    timeouts = pair_cfg.get("timeouts", [TIMEOUT_BARS])
    directions = pair_cfg.get("directions", DIRECTIONS)
    sl_mode = pair_cfg.get("sl_mode", "standard")

    all_rows: list[dict] = []
    combo_equity_curves = collections.defaultdict(list)

    for ma_fast, ma_slow in ma_pairs:
        full_sma_fast = df["close"].rolling(ma_fast).mean().to_numpy()
        full_sma_slow = df["close"].rolling(ma_slow).mean().to_numpy()

        for timeout in timeouts:
            for window_idx in range(N_WINDOWS):
                weeks_shift = (N_WINDOWS - 1 - window_idx) * OOS_WEEKS
                oos_end = data_end - pd.Timedelta(weeks=weeks_shift)
                oos_start = oos_end - pd.Timedelta(weeks=OOS_WEEKS)
                is_start = oos_start - pd.Timedelta(weeks=IS_WEEKS)

                is_df = df.loc[(df.index >= is_start) & (df.index < oos_start)]
                oos_df = df.loc[(df.index >= oos_start) & (df.index <= oos_end)]

                if len(is_df) < 1000 or len(oos_df) < 100:
                    continue

                is_inds = compute_indicators(is_df)
                oos_inds = compute_indicators(oos_df)

                is_start_pos = (
                    df.index.get_loc(is_df.index[0]) if len(is_df) > 0 else 0
                )
                is_end_pos = (
                    df.index.get_loc(is_df.index[-1]) + 1 if len(is_df) > 0 else 0
                )
                oos_start_pos = (
                    df.index.get_loc(oos_df.index[0]) if len(oos_df) > 0 else 0
                )
                oos_end_pos = (
                    df.index.get_loc(oos_df.index[-1]) + 1
                    if len(oos_df) > 0
                    else 0
                )

                is_inds["sma_fast"] = full_sma_fast[is_start_pos:is_end_pos]
                oos_inds["sma_fast"] = full_sma_fast[oos_start_pos:oos_end_pos]
                is_inds["sma_slow"] = full_sma_slow[is_start_pos:is_end_pos]
                oos_inds["sma_slow"] = full_sma_slow[oos_start_pos:oos_end_pos]

                is_adx = adx_arr[is_start_pos:is_end_pos]
                is_bbw = bbw_arr[is_start_pos:is_end_pos]
                oos_adx = adx_arr[oos_start_pos:oos_end_pos]
                oos_bbw = bbw_arr[oos_start_pos:oos_end_pos]

                combos_for_pair = [
                    c for c in combos if c["direction"] in directions
                ]

                is_start_ts = is_df.index[0]
                is_end_ts = is_df.index[-1]
                oos_start_ts = oos_df.index[0]
                oos_end_ts = oos_df.index[-1]

                # --- IS EVALUATION (ALL combos) ---
                is_rows: list[dict] = []

                # Bulk-fetch entire window cache in one network call
                window_is_cache = _load_window_is_cache(
                    instrument=instrument,
                    granularity=GRANULARITY,
                    is_start=is_start_ts,
                    is_end=is_end_ts,
                    timeout=timeout,
                    conn=conn,
                )

                for combo in combos_for_pair:
                    # Build combo signature for cache lookup
                    combo_sig = (
                        combo.get("direction"),
                        combo.get("anchor"),
                        combo.get("anchor2"),
                        combo.get("continuation"),
                        combo.get("continuation2"),
                        combo.get("gap"),
                        combo.get("indicator"),
                        combo.get("combo_type"),
                    )
                    cached_is = window_is_cache.get(combo_sig)

                    if cached_is is not None:
                        is_rows.append({
                            "combo":          combo,
                            "n_trades":       cached_is["n_trades"],
                            "mean_r":         cached_is["mean_r"],
                            "raw_sqn":        cached_is["raw_sqn"],
                            "r_mults":        cached_is["r_mults"],
                            "adjusted_score": cached_is["adjusted_score"],
                            "initial_bucket": cached_is["initial_bucket"],
                            "passes_band":    False,
                            "final_bucket":   "REJECTED",
                            "oos_eligible":   False,
                            "is_sqn100":      cached_is["raw_sqn"],
                            "is_n_trades":    cached_is["n_trades"],
                            "is_mean_r":      cached_is["mean_r"],
                            "from_cache":     True,
                        })
                        continue

                    # Cache miss — run full IS simulation

                    sig, anc = detect_signals(
                        is_df,
                        combo["anchor"],
                        combo["continuation"],
                        combo["direction"],
                        gap=combo["gap"] if combo["gap"] is not None else 5,
                        anchor2=combo.get("anchor2"),
                        continuation2=combo.get("continuation2"),
                    )
                    sig, anc = _apply_indicator_filter(
                        sig, anc, combo, is_inds, is_df
                    )
                    trades = sweep_simulation(
                        is_df, sig, anc,
                        combo["direction"], pip_size,
                        is_inds["atr"], avg_spread,
                        timeout_bars=timeout, sl_mode=sl_mode,
                        adx_arr=is_adx, bbw_arr=is_bbw,
                        profile=profile_from_combo(combo),
                        instrument=instrument,
                    )
                    r_mults = [float(t["r_multiple"]) for t in trades]
                    n = len(r_mults)
                    mean_r = float(np.mean(r_mults)) if r_mults else 0.0
                    raw_sqn = sqn100(r_mults, n_min=1)

                    # Shrinkage-adjusted SQN
                    shrink = float(np.sqrt(n / (n + SQN_SHRINKAGE_K))) if n > 0 else 0.0
                    adjusted_score = raw_sqn * shrink

                    # Bucket assignment
                    if n < 1 or mean_r <= 0 or adjusted_score < MIN_SQN_ELIGIBLE:
                        initial_bucket = "REJECTED"
                    elif n < MIN_TRADES_ELIGIBLE and mean_r > 0:
                        initial_bucket = "WATCHLIST"
                    else:
                        initial_bucket = "ELIGIBLE"

                    is_rows.append({
                        "combo": combo,
                        "n_trades": n,
                        "mean_r": mean_r,
                        "raw_sqn": raw_sqn,
                        "r_mults": r_mults,
                        "adjusted_score": adjusted_score,
                        "initial_bucket": initial_bucket,
                        "passes_band": False,
                        "final_bucket": "REJECTED",
                        "oos_eligible": False,
                        "is_sqn100": raw_sqn,
                        "is_n_trades": n,
                        "is_mean_r": mean_r,
                        "from_cache": False,
                    })

                # --- BAND SELECTION (ELIGIBLE only) ---
                eligible = [r for r in is_rows if r["initial_bucket"] == "ELIGIBLE"]
                best_score = max(
                    (r["adjusted_score"] for r in eligible), default=0.0
                )

                for r in is_rows:
                    if r["initial_bucket"] == "ELIGIBLE":
                        # Gemini patch: if best_score <= 0, nothing promoted
                        passes_band = (
                            best_score >= MIN_SQN_ELIGIBLE
                            and r["adjusted_score"] >= best_score * RELATIVE_SCORE_FRACTION
                        )
                        r["passes_band"] = passes_band
                        r["final_bucket"] = "PROMOTED" if passes_band else "ELIGIBLE_NOT_BANDED"
                        r["oos_eligible"] = passes_band
                    elif r["initial_bucket"] == "WATCHLIST":
                        r["passes_band"] = False
                        r["final_bucket"] = "WATCHLIST"
                        r["oos_eligible"] = False
                    else:
                        r["passes_band"] = False
                        r["final_bucket"] = "REJECTED"
                        r["oos_eligible"] = False

                # Write bypass — skip rows that came from cache
                new_is_rows = [r for r in is_rows if not r.get("from_cache", False)]
                if new_is_rows:
                    _write_is_results(
                        is_rows=new_is_rows,
                        instrument=instrument,
                        window_id=window_idx,
                        window_start=is_start_ts,
                        window_end=is_end_ts,
                        timeout=timeout,
                        run_id=run_id,
                        batch_id=batch_id,
                        conn=conn,
                    )
                else:
                    n_cached = sum(1 for r in is_rows if r.get("from_cache", False))
                    print(f"  [is_cache] window fully cached ({n_cached} hits, 0 writes)")

                promoted = [r for r in is_rows if r["oos_eligible"]]

                print(
                    f"  Window {window_idx + 1:02d}/{N_WINDOWS} | "
                    f"IS {is_start_ts.date()}->{is_end_ts.date()} | "
                    f"OOS {oos_start_ts.date()}->{oos_end_ts.date()} | "
                    f"eligible={len(eligible)} "
                    f"promoted={len(promoted)} "
                    f"watchlist={sum(1 for r in is_rows if r['initial_bucket']=='WATCHLIST')} "
                    f"rejected={sum(1 for r in is_rows if r['initial_bucket']=='REJECTED')}"
                )

                # --- OOS EXECUTION (PROMOTED only) ---
                for pr in promoted:
                    combo = pr["combo"]

                    block_hash = _make_block_hash(
                        instrument=instrument,
                        granularity=GRANULARITY,
                        oos_start=oos_start_ts,
                        oos_end=oos_end_ts,
                        combo=combo,
                        timeout=timeout,
                        tp_mult=TP_MULT,
                        sl_mult=SL_MULT,
                        sl_mode=sl_mode,
                    )

                    cached = _cache_lookup(block_hash, conn)
                    if cached is not None:
                        oos_r = cached["oos_r_list"]
                        oos_trades = []
                        print(f"    [cache HIT] {instrument} w{window_idx+1:02d} {combo.get('anchor')}/{combo.get('continuation')}")
                    else:
                        sig_o, anc_o = detect_signals(
                            oos_df,
                            combo["anchor"],
                            combo["continuation"],
                            combo["direction"],
                            gap=combo["gap"] if combo["gap"] is not None else 5,
                            anchor2=combo.get("anchor2"),
                            continuation2=combo.get("continuation2"),
                        )
                        sig_o, anc_o = _apply_indicator_filter(
                            sig_o, anc_o, combo, oos_inds, oos_df
                        )
                        oos_trades = sweep_simulation(
                            oos_df,
                            sig_o,
                            anc_o,
                            combo["direction"],
                            pip_size,
                            oos_inds["atr"],
                            avg_spread,
                            timeout_bars=timeout,
                            sl_mode=sl_mode,
                            adx_arr=oos_adx,
                            bbw_arr=oos_bbw,
                            profile=profile_from_combo(combo),
                            instrument=instrument,
                        )
                        oos_r = [float(t["r_multiple"]) for t in oos_trades]
                        _cache_store(block_hash, instrument, oos_start_ts, oos_end_ts,
                                     combo, timeout, oos_r, conn)
                    _gap = combo.get("gap")
                    combo_key = (
                        combo.get("anchor"),
                        combo.get("continuation"),
                        _gap,
                        combo.get("indicator"),
                        combo.get("direction"),
                    )
                    for t in oos_trades:
                        if "equity_after" in t:
                            combo_equity_curves[combo_key].append(
                                float(t["equity_after"])
                            )
                    # On cache hit oos_trades is empty — derive count
                    # and equity curve directly from oos_r
                    oos_n_trades_override = len(oos_r)
                    if not oos_trades and oos_r:
                        # Reconstruct synthetic equity curve from R-multiples.
                        # Seed from last known equity for this combo across windows
                        # to prevent artificial reset to 10,000 on each cache hit.
                        existing = combo_equity_curves.get(combo_key, [])
                        _eq = existing[-1] if existing else 10_000.0
                        for _r in oos_r:
                            _eq *= (1 + 0.01 * _r)
                            combo_equity_curves[combo_key].append(_eq)
                    oos_n_trades = len(oos_r)
                    oos_mean_r = float(np.mean(oos_r)) if oos_r else 0.0
                    if len(oos_r) >= 2:
                        _oos_std = float(np.std(oos_r, ddof=1))
                        oos_sqn100 = float(np.mean(oos_r) / _oos_std * np.sqrt(min(len(oos_r), 100))) if _oos_std > 0 else 0.0
                    else:
                        oos_sqn100 = 0.0

                    all_rows.append(
                        {
                            "window_idx": window_idx,
                            "is_start": is_start_ts,
                            "is_end": is_end_ts,
                            "oos_start": oos_start_ts,
                            "oos_end": oos_end_ts,
                            "anchor": combo["anchor"],
                            "continuation": combo["continuation"],
                            "gap": combo["gap"],
                            "indicator": combo["indicator"],
                            "direction": combo["direction"],
                            "anchor2":        combo.get("anchor2"),
                            "continuation2":  combo.get("continuation2"),
                            "combo_type":     combo.get("combo_type"),
                            "timeout_bars":   int(timeout),
                            "is_sqn100": pr["is_sqn100"],
                            "is_n_trades": pr["is_n_trades"],
                            "is_mean_r": pr["is_mean_r"],
                            "oos_sqn100": oos_sqn100,
                            "oos_r_list": oos_r,
                            "oos_n_trades": oos_n_trades,
                            "oos_mean_r": oos_mean_r,
                            "mdd": 0.0,
                            "sharpe": 0.0,
                            "regime_filtered": True,
                        }
                    )

    if not all_rows:
        return pd.DataFrame(columns=_WFV_COLUMNS)
    wfv_df = pd.DataFrame(all_rows)

    if not wfv_df.empty and combo_equity_curves:
        mdd_map = {k: compute_mdd(v) for k, v in combo_equity_curves.items()}

        sharpe_map = {k: compute_sharpe(v, n_trades=len(v)) for k, v in combo_equity_curves.items()}

        wfv_df["_combo_key"] = list(
            zip(
                wfv_df["anchor"],
                [None if pd.isna(x) else x for x in wfv_df["continuation"]],
                [None if pd.isna(x) else x for x in wfv_df["gap"]],
                [None if pd.isna(x) else x for x in wfv_df["indicator"]],
                wfv_df["direction"],
            )
        )
        wfv_df["mdd"] = wfv_df["_combo_key"].map(mdd_map).fillna(0.0)
        wfv_df["sharpe"] = wfv_df["_combo_key"].map(sharpe_map).fillna(0.0)
        wfv_df.drop(columns=["_combo_key"], inplace=True)
    else:
        wfv_df["mdd"] = 0.0
        wfv_df["sharpe"] = 0.0

    return wfv_df


def compute_shadow_status(wfv_df: pd.DataFrame, instrument: str) -> dict:
    total_trades = int(wfv_df["oos_n_trades"].sum())
    if total_trades == 0:
        agg_mean_r = 0.0
    else:
        agg_mean_r = float(
            (wfv_df["oos_mean_r"] * wfv_df["oos_n_trades"]).sum() / total_trades
        )

    eligible = wfv_df[wfv_df["oos_n_trades"] >= 1]
    if len(eligible) == 0:
        base_rate = 0.0
    else:
        base_rate = float((eligible["oos_mean_r"] > 0).sum() / len(eligible))

    combo_cols = [
        "anchor", "anchor2", "continuation", "continuation2",
        "gap", "indicator", "direction", "combo_type", "timeout_bars"
    ]

    def _combo_agg(g: pd.DataFrame) -> pd.Series:
        nt = int(g["oos_n_trades"].sum())
        if nt > 0:
            wm = float((g["oos_mean_r"] * g["oos_n_trades"]).sum() / nt)
        else:
            wm = 0.0
        return pd.Series(
            {
                "oos_n_trades": nt,
                "oos_mean_r": wm,
                "n_windows": len(g),
            }
        )

    grouped = wfv_df.groupby(
        combo_cols, sort=False, dropna=False
    ).apply(_combo_agg).reset_index()
    eligible_combos = grouped[grouped["oos_n_trades"] >= SQN_MIN_TRADES]
    if len(eligible_combos) == 0:
        top_combo_str = ""
        top_sqn100 = 0.0
        top_configs: list[dict] = []
    else:
        eligible_combos = eligible_combos.sort_values(
            "oos_mean_r", ascending=False
        )
        top = eligible_combos.iloc[0]
        top_combo_str = (
            f"{top['anchor']}|{top['continuation']}|"
            f"{top['indicator']}|{top['direction']}"
        )
        top_sqn100 = float(top["oos_mean_r"])

        top5 = eligible_combos.head(5)
        top_configs = [
            {
                "anchor": row["anchor"],
                "continuation": row["continuation"],
                "indicator": row["indicator"],
                "direction": row["direction"],
                "oos_mean_r": round(float(row["oos_mean_r"]), 4),
                "oos_n_trades": int(row["oos_n_trades"]),
            }
            for _, row in top5.iterrows()
        ]

    if agg_mean_r > 0 and base_rate >= 0.5:
        status = "DEPLOY"
    else:
        status = "PAUSE"

    return {
        "instrument": instrument,
        "status": status,
        "agg_mean_r": round(agg_mean_r, 6),
        "base_rate": round(base_rate, 4),
        "top_combo": top_combo_str,
        "sqn100": round(top_sqn100, 4),
        "top_configs": top_configs,
    }


def write_leaderboard_csv(wfv_df: pd.DataFrame, instrument: str) -> None:
    combo_cols = [
        "anchor", "anchor2", "continuation", "continuation2",
        "gap", "indicator", "direction", "combo_type", "timeout_bars"
    ]

    def _lb_agg(g: pd.DataFrame) -> pd.Series:
        nt = int(g["oos_n_trades"].sum())
        if nt > 0:
            oos_mean = float((g["oos_mean_r"] * g["oos_n_trades"]).sum() / nt)
        else:
            oos_mean = 0.0
        pooled_r = [r for r_list in g["oos_r_list"] for r in r_list]
        if len(pooled_r) >= 2:
            _pooled_std = float(np.std(pooled_r, ddof=1))
            agg_sqn100 = float(np.mean(pooled_r) / _pooled_std * np.sqrt(min(len(pooled_r), 100))) if _pooled_std > 0 else 0.0
        else:
            agg_sqn100 = 0.0
        return pd.Series(
            {
                "oos_sqn100": agg_sqn100,
                "oos_mean_r": oos_mean,
                "oos_n_trades": nt,
                "is_sqn100": float(g["is_sqn100"].mean()),
                "n_windows_promoted": len(g),
                "mdd": float(g["mdd"].mean()) if "mdd" in g.columns else 0.0,
                "sharpe": float(g["sharpe"].mean()) if "sharpe" in g.columns else 0.0,
            }
        )

    grouped = (
        wfv_df.groupby(combo_cols, sort=False, dropna=False)
        .apply(_lb_agg)
        .reset_index()
        .pipe(
            lambda df: df.assign(
                _passes_gate=(
                    (df["oos_mean_r"] > 0)
                    & (df["mdd"] < 0.05)
                    & (df["sharpe"] > 1.0)
                    & (df["oos_n_trades"] >= 10)
                )
            )
        )
        .sort_values(["_passes_gate", "sharpe"], ascending=[False, False])
        .drop(columns=["_passes_gate"])
    )
    path = os.path.join(OUTPUT_DIR, f"leaderboard_{instrument}.csv")
    grouped.to_csv(path, index=False)


def _write_leaderboard(
    wfv_df: pd.DataFrame,
    instrument: str,
    run_id: str,
    conn,
) -> None:
    """
    Write aggregated OOS leaderboard results to sweep_leaderboard.
    Runs parallel to write_leaderboard_csv during Phase 3 validation.
    ON CONFLICT DO UPDATE — leaderboard is a living snapshot of the
    current 27-window dataset (Gemini ruling: not a historical museum).
    SAVEPOINT is per-row so one bad row never kills the batch.
    Never raises — write failures are non-fatal.
    """
    if wfv_df.empty:
        return

    combo_cols = [
        "anchor", "anchor2", "continuation", "continuation2",
        "gap", "indicator", "direction", "combo_type", "timeout_bars"
    ]

    def _lb_agg_db(g: pd.DataFrame) -> pd.Series:
        nt = int(g["oos_n_trades"].sum())
        oos_mean = float(
            (g["oos_mean_r"] * g["oos_n_trades"]).sum() / nt
        ) if nt > 0 else 0.0
        pooled_r = [r for r_list in g["oos_r_list"] for r in r_list]
        if len(pooled_r) >= 2:
            _std = float(np.std(pooled_r, ddof=1))
            sqn = float(
                np.mean(pooled_r) / _std * np.sqrt(min(len(pooled_r), 100))
            ) if _std > 0 else 0.0
        else:
            sqn = 0.0
        return pd.Series({
            "oos_sqn100":         sqn,
            "oos_mean_r":         oos_mean,
            "oos_n_trades":       nt,
            "n_windows_promoted": int(len(g)),
            "mdd":    float(g["mdd"].mean()) if "mdd" in g.columns else 0.0,
            "sharpe": float(g["sharpe"].mean()) if "sharpe" in g.columns else 0.0,
            "oos_r_list": pooled_r,
        })

    existing_combo_cols = [c for c in combo_cols if c in wfv_df.columns]
    grouped = (
        wfv_df.groupby(existing_combo_cols, sort=False, dropna=False)
        .apply(_lb_agg_db)
        .reset_index()
    )

    sql = """
        INSERT INTO sweep_leaderboard (
            granularity, instrument,
            anchor, anchor2, continuation, continuation2,
            gap, indicator, direction, combo_type,
            timeout_bars,
            oos_sqn100, oos_mean_r, oos_n_trades,
            n_windows_promoted, mdd, sharpe, oos_r_list,
            sweep_run_id
        ) VALUES (
            %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s,
            %s, %s, %s,
            %s, %s, %s, %s,
            %s
        )
        ON CONFLICT (
            granularity, instrument,
            anchor, anchor2, continuation, continuation2,
            gap, indicator, direction, combo_type,
            timeout_bars
        ) DO UPDATE SET
            oos_sqn100         = EXCLUDED.oos_sqn100,
            oos_mean_r         = EXCLUDED.oos_mean_r,
            oos_n_trades       = EXCLUDED.oos_n_trades,
            n_windows_promoted = EXCLUDED.n_windows_promoted,
            mdd                = EXCLUDED.mdd,
            sharpe             = EXCLUDED.sharpe,
            oos_r_list         = EXCLUDED.oos_r_list,
            sweep_run_id       = EXCLUDED.sweep_run_id,
            updated_at         = NOW()
    """

    def _safe_float(v):
        try:
            f = float(v)
            return None if f != f else f
        except (TypeError, ValueError):
            return None

    def _safe_int(v):
        try:
            return int(v)
        except (TypeError, ValueError):
            return None

    def _safe_str(v):
        if v is None or (isinstance(v, float) and v != v):
            return None
        s = str(v).strip()
        return None if s.lower() in ("none", "nan", "") else s

    n_written = 0
    n_errors = 0
    with conn.cursor() as cur:
        for _, row in grouped.iterrows():
            try:
                cur.execute("SAVEPOINT row_write")
                r_list = row.get("oos_r_list", [])
                r_list_json = json.dumps(
                    [float(r) for r in r_list if r == r]
                ) if r_list else json.dumps([])

                cur.execute(sql, (
                    GRANULARITY, instrument,
                    _safe_str(row.get("anchor")),
                    _safe_str(row.get("anchor2")),
                    _safe_str(row.get("continuation")),
                    _safe_str(row.get("continuation2")),
                    _safe_int(row.get("gap")),
                    _safe_str(row.get("indicator")),
                    _safe_str(row.get("direction")),
                    _safe_str(row.get("combo_type")),
                    _safe_int(row.get("timeout_bars")),
                    _safe_float(row.get("oos_sqn100")),
                    _safe_float(row.get("oos_mean_r")),
                    _safe_int(row.get("oos_n_trades")),
                    _safe_int(row.get("n_windows_promoted")),
                    _safe_float(row.get("mdd")),
                    _safe_float(row.get("sharpe")),
                    r_list_json,
                    run_id,
                ))
                cur.execute("RELEASE SAVEPOINT row_write")
                n_written += 1
            except Exception as e:
                cur.execute("ROLLBACK TO SAVEPOINT row_write")
                n_errors += 1
                print(f"  [leaderboard] row write error: {e}")
    conn.commit()
    if n_written > 0 or n_errors > 0:
        print(f"  [leaderboard] {n_written} rows written, {n_errors} errors")


def write_paper_roster(promoted_combos: list[dict]) -> None:
    """
    Write paper_roster.json — manually promoted combos for shadow monitoring.
    Schema mirrors deployment_roster.json for downstream parser compatibility.
    """
    roster = {}
    for entry in promoted_combos:
        instrument = entry["instrument"]
        if instrument not in roster:
            roster[instrument] = {
                "status": "PAUSE",
                "strategies": [],
            }
        roster[instrument]["strategies"].append(
            {
                "combo": entry["combo"],
                "direction": entry["direction"],
                "oos_mean_r": entry["oos_mean_r"],
                "n_trades": entry["n_trades"],
            }
        )
    path = os.path.join(OUTPUT_DIR, "paper_roster.json")
    with open(path, "w") as f:
        json.dump(roster, f, indent=2)
    print(f"  Paper roster written: output/paper_roster.json")


def write_shadow_book_state(shadow_rows: list[dict], conn) -> None:
    sql = """
        INSERT INTO shadow_book_state
            (instrument, status, agg_mean_r, base_rate, top_combo, sqn100, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (instrument) DO UPDATE SET
            status     = EXCLUDED.status,
            agg_mean_r = EXCLUDED.agg_mean_r,
            base_rate  = EXCLUDED.base_rate,
            top_combo  = EXCLUDED.top_combo,
            sqn100     = EXCLUDED.sqn100,
            updated_at = NOW()
    """
    cur = conn.cursor()
    for row in shadow_rows:
        cur.execute(
            sql,
            (
                row["instrument"],
                row["status"],
                row["agg_mean_r"],
                row["base_rate"],
                row["top_combo"],
                row["sqn100"],
            ),
        )
    conn.commit()
    cur.close()


def main():
    load_dotenv()
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL not set. Add it to d:\\candlelab\\.env")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    combos = build_combo_list()
    print(f"Combo space: {len(combos)} combinations")
    test_combo = next(c for c in combos if c["continuation"] is not None)
    print(f"Smoke test combo: {test_combo}")
    test_combo_pure = next(c for c in combos if c.get("pure_cont"))
    print(f"Pure cont smoke test combo: {test_combo_pure}")

    all_shadow_rows = []
    roster = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "instruments": {},
    }

    global_run_id = str(uuid.uuid4())
    print(f"Run ID: {global_run_id}")

    with psycopg2.connect(db_url) as conn:
        for instrument in INSTRUMENTS:
            if not ENABLED_INSTRUMENTS.get(instrument, True):
                print(f"  Skipping {instrument} (disabled in ENABLED_INSTRUMENTS)")
                continue
            print(f"\n{'='*60}")
            print(f"Sweeping {instrument}")
            print(f"{'='*60}")

            instrument_batch_id = str(uuid.uuid4())

            try:
                df = fetch_instrument_data(instrument, conn)
                print(f"  Fetched {len(df):,} {GRANULARITY} bars")

                wfv_df = run_wfv(
                    df, instrument, PIP[instrument], conn,
                    global_run_id, instrument_batch_id
                )
            except ValueError as e:
                print(f"  Skipping {instrument}: {e}")
                continue

            if wfv_df.empty:
                print(
                    f"  WARNING: No promoted combos for {instrument} — "
                    f"check data coverage and SQN threshold"
                )
                shadow = {
                    "instrument": instrument,
                    "status": "PAUSE",
                    "agg_mean_r": 0.0,
                    "base_rate": 0.0,
                    "top_combo": "",
                    "sqn100": 0.0,
                    "top_configs": [],
                }
            else:
                write_leaderboard_csv(wfv_df, instrument)
                _write_leaderboard(wfv_df, instrument, global_run_id, conn)
                print(f"  Leaderboard written: output/leaderboard_{instrument}.csv")
                shadow = compute_shadow_status(wfv_df, instrument)

            all_shadow_rows.append(shadow)
            roster["instruments"][instrument] = {
                "shadow_status": shadow["status"],
                "agg_mean_r": shadow["agg_mean_r"],
                "base_rate": shadow["base_rate"],
                "top_configs": shadow["top_configs"],
            }
            print(
                f"  {instrument}: {shadow['status']} | "
                f"agg_mean_r={shadow['agg_mean_r']:.4f} | "
                f"base_rate={shadow['base_rate']:.2%}"
            )

        write_shadow_book_state(all_shadow_rows, conn)
        print("\nShadow book state written to Postgres.")

    with open(ROSTER_FILE, "w") as f:
        json.dump(roster, f, indent=2)
    print(f"Deployment roster written to {ROSTER_FILE}")
    print("\nSweep complete.")


if __name__ == "__main__":
    main()
