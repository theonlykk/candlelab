"""
CandleLab Meta Sweep — unified Discovery and Stability engine.
Reads from sweep_leaderboard. Writes meta_status back to sweep_leaderboard.
No CSV input or output.

Usage:
    python scripts/meta_sweep.py --mode discovery --tf M30
    python scripts/meta_sweep.py --mode stability --tf M30
    python scripts/meta_sweep.py --mode discovery --tf H1
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from statistics import median
from typing import Any

import numpy as np
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

# Import TIMEFRAME_CONFIGS from the engine
sys.path.insert(0, os.path.dirname(__file__))
from sweep_validation_engine import TIMEFRAME_CONFIGS

load_dotenv(r"d:\candlelab\.env")
DB_URL = os.environ["DATABASE_URL"]

# ---------------------------------------------------------------------------
# Parameter weights — Gemini-approved (ADR Phase 5)
# ---------------------------------------------------------------------------
PARAM_WEIGHTS: dict[str, float] = {
    "direction":     10.0,  # flip = different universe
    "combo_type":    10.0,  # 1R vs 2R = different universe
    "continuation":   5.0,  # pattern change = structural shift
    "anchor":         1.0,  # minor topology tweak = valid neighbor
    "timeout_bars":   1.0,  # minor parameter tweak = valid neighbor
    # zero-weight — part of identity but excluded from distance
    "anchor2":        0.0,
    "continuation2":  0.0,
    "gap":            0.0,
    "indicator":      0.0,
}

NEIGHBORHOOD_THRESHOLD = 1.0   # weighted distance <= 1.0 = valid neighbor
MIN_NEIGHBORS          = 3     # fewer than 3 neighbors = cannot prove plateau
SPIKE_TOLERANCE        = 1.0   # candidate SQN cannot exceed neighborhood by > 1.0
NPR_ABS_FLOOR          = 0.20  # absolute minimum neighborhood quality score
NQS_ABS_FLOOR          = 0.0   # absolute minimum nqs

# Discovery uses P70, Stability uses P60
DISCOVERY_PERCENTILE   = 70
STABILITY_PERCENTILE   = 60

# ---------------------------------------------------------------------------
# Deployed strategy registry (Stability mode)
# Full combo identity required — add anchor2, continuation2,
# combo_type, timeout_bars, granularity vs old format
# ---------------------------------------------------------------------------
DEPLOYED: list[dict] = [
    {"strategy_id": 55, "granularity": "M30", "instrument": "GBP_CAD",
     "anchor": "shooting_star", "anchor2": None, "continuation": None,
     "continuation2": None, "gap": None, "indicator": None,
     "direction": "long", "combo_type": None, "timeout_bars": 20},
    {"strategy_id": 56, "granularity": "M30", "instrument": "USD_CAD",
     "anchor": "shooting_star", "anchor2": None, "continuation": None,
     "continuation2": None, "gap": None, "indicator": None,
     "direction": "long", "combo_type": None, "timeout_bars": 20},
    {"strategy_id": 57, "granularity": "M30", "instrument": "AUD_JPY",
     "anchor": "morning_star", "anchor2": None, "continuation": None,
     "continuation2": None, "gap": None, "indicator": None,
     "direction": "long", "combo_type": None, "timeout_bars": 20},
    {"strategy_id": 58, "granularity": "M30", "instrument": "GBP_JPY",
     "anchor": "shooting_star", "anchor2": None, "continuation": None,
     "continuation2": None, "gap": None, "indicator": None,
     "direction": "long", "combo_type": None, "timeout_bars": 20},
    {"strategy_id": 59, "granularity": "M30", "instrument": "CAD_JPY",
     "anchor": "hammer", "anchor2": None, "continuation": None,
     "continuation2": None, "gap": None, "indicator": None,
     "direction": "long", "combo_type": None, "timeout_bars": 20},
    {"strategy_id": 60, "granularity": "M30", "instrument": "AUD_NZD",
     "anchor": "hammer", "anchor2": None, "continuation": None,
     "continuation2": None, "gap": None, "indicator": None,
     "direction": "short", "combo_type": None, "timeout_bars": 20},
    {"strategy_id": 61, "granularity": "M30", "instrument": "USD_JPY",
     "anchor": "shooting_star", "anchor2": None, "continuation": None,
     "continuation2": None, "gap": None, "indicator": None,
     "direction": "long", "combo_type": None, "timeout_bars": 20},
    {"strategy_id": 62, "granularity": "M30", "instrument": "GBP_USD",
     "anchor": "engulfing", "anchor2": None, "continuation": None,
     "continuation2": None, "gap": None, "indicator": None,
     "direction": "long", "combo_type": None, "timeout_bars": 20},
    {"strategy_id": 63, "granularity": "M30", "instrument": "GBP_AUD",
     "anchor": "engulfing", "anchor2": None, "continuation": None,
     "continuation2": None, "gap": None, "indicator": None,
     "direction": "short", "combo_type": None, "timeout_bars": 20},
    {"strategy_id": 64, "granularity": "M30", "instrument": "CHF_JPY",
     "anchor": "engulfing", "anchor2": None, "continuation": None,
     "continuation2": None, "gap": None, "indicator": None,
     "direction": "short", "combo_type": None, "timeout_bars": 20},
    {"strategy_id": 65, "granularity": "M30", "instrument": "GBP_NZD",
     "anchor": "hammer", "anchor2": None, "continuation": None,
     "continuation2": None, "gap": None, "indicator": None,
     "direction": "long", "combo_type": None, "timeout_bars": 20},
    {"strategy_id": 66, "granularity": "M30", "instrument": "EUR_NZD",
     "anchor": "hammer", "anchor2": None, "continuation": None,
     "continuation2": None, "gap": None, "indicator": None,
     "direction": "long", "combo_type": None, "timeout_bars": 20},
    {"strategy_id": 67, "granularity": "M30", "instrument": "NZD_USD",
     "anchor": "engulfing", "anchor2": None, "continuation": None,
     "continuation2": None, "gap": None, "indicator": None,
     "direction": "long", "combo_type": None, "timeout_bars": 20},
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _norm(v: Any) -> Any:
    """Normalize None / empty string / 'nan' to None for comparison."""
    if v is None:
        return None
    if isinstance(v, float) and np.isnan(v):
        return None
    s = str(v).strip()
    if s.lower() in ("none", "nan", ""):
        return None
    return s


def _norm_row(row: dict) -> dict:
    """Normalize all combo identity columns in a row dict."""
    COMBO_COLS = [
        "anchor", "anchor2", "continuation", "continuation2",
        "gap", "indicator", "direction", "combo_type",
    ]
    out = dict(row)
    for col in COMBO_COLS:
        out[col] = _norm(out.get(col))
    if "timeout_bars" in out and out["timeout_bars"] is not None:
        out["timeout_bars"] = int(out["timeout_bars"])
    return out


def weighted_distance(a: dict, b: dict) -> float:
    """
    Compute weighted Hamming distance between two combo dicts.
    Returns early if distance exceeds NEIGHBORHOOD_THRESHOLD.
    """
    dist = 0.0
    for param, weight in PARAM_WEIGHTS.items():
        if weight == 0.0:
            continue
        if a.get(param) != b.get(param):
            dist += weight
            if dist > NEIGHBORHOOD_THRESHOLD:
                return dist
    return dist


def find_neighbors(candidate: dict, universe: list[dict]) -> list[dict]:
    """Return all rows within NEIGHBORHOOD_THRESHOLD weighted distance."""
    return [
        r for r in universe
        if r is not candidate
        and weighted_distance(candidate, r) <= NEIGHBORHOOD_THRESHOLD
    ]


def score_candidate(
    candidate: dict,
    universe: list[dict],
) -> dict | None:
    """
    Score a candidate's neighborhood.
    Returns None if neighbor_count < MIN_NEIGHBORS.
    """
    neighbors = find_neighbors(candidate, universe)
    n = len(neighbors)
    if n < MIN_NEIGHBORS:
        return None

    # Weighted NPR — average clipped mean_r of neighbors
    nqs = sum(max(0.0, r["oos_mean_r"]) for r in neighbors) / n

    # SQN gap — spike filter
    neighbor_sqns = [r["oos_sqn100"] for r in neighbors]
    neighbor_median_sqn = float(median(neighbor_sqns))
    sqn_gap = float(candidate["oos_sqn100"]) - neighbor_median_sqn

    return {
        "neighbor_count":             n,
        "neighborhood_quality_score": nqs,
        "sqn_gap":                    sqn_gap,
        "neighbor_median_sqn":        neighbor_median_sqn,
    }


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def fetch_universe(conn, granularity: str, instrument: str,
                   min_windows: int) -> list[dict]:
    """
    Fetch eligible candidates for neighborhood scoring.
    Strictly scoped to granularity + instrument — no cross-contamination.
    """
    sql = """
        SELECT
            granularity, instrument,
            anchor, anchor2, continuation, continuation2,
            gap, indicator, direction, combo_type, timeout_bars,
            oos_sqn100, oos_mean_r, n_windows_promoted,
            neighborhood_quality_score, sqn_gap, meta_status
        FROM sweep_leaderboard
        WHERE granularity = %s
          AND instrument  = %s
          AND oos_mean_r  > 0
          AND n_windows_promoted >= %s
          AND oos_sqn100  > 0
        ORDER BY oos_sqn100 DESC
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (granularity, instrument, min_windows))
        rows = cur.fetchall()
    return [_norm_row(dict(r)) for r in rows]


def write_meta_status(
    conn,
    granularity: str,
    instrument: str,
    row: dict,
    scores: dict,
    status: str,
) -> None:
    """
    Write meta sweep results back to sweep_leaderboard.
    Uses IS NOT DISTINCT FROM for nullable columns.
    SAVEPOINT per row — one failure never kills the batch.
    """
    sql = """
        UPDATE sweep_leaderboard
        SET
            neighbor_count              = %s,
            neighborhood_quality_score  = %s,
            sqn_gap                     = %s,
            meta_status                 = %s,
            updated_at                  = NOW()
        WHERE granularity    = %s
          AND instrument     = %s
          AND direction      = %s
          AND (anchor             IS NOT DISTINCT FROM %s)
          AND (anchor2            IS NOT DISTINCT FROM %s)
          AND (continuation       IS NOT DISTINCT FROM %s)
          AND (continuation2      IS NOT DISTINCT FROM %s)
          AND (gap                IS NOT DISTINCT FROM %s)
          AND (indicator          IS NOT DISTINCT FROM %s)
          AND (combo_type         IS NOT DISTINCT FROM %s)
          AND timeout_bars        = %s
    """
    try:
        with conn.cursor() as cur:
            cur.execute("SAVEPOINT meta_write")
            cur.execute(sql, (
                scores.get("neighbor_count"),
                scores.get("neighborhood_quality_score"),
                scores.get("sqn_gap"),
                status,
                granularity, instrument,
                row["direction"],
                row["anchor"], row["anchor2"],
                row["continuation"], row["continuation2"],
                row["gap"], row["indicator"],
                row["combo_type"],
                row["timeout_bars"],
            ))
            cur.execute("RELEASE SAVEPOINT meta_write")
    except Exception as e:
        with conn.cursor() as rc:
            rc.execute("ROLLBACK TO SAVEPOINT meta_write")
        print(f"  [meta] write error for {instrument}/{row.get('anchor')}: {e}")


# ---------------------------------------------------------------------------
# Discovery mode
# ---------------------------------------------------------------------------

def run_discovery(conn, granularity: str, cfg: dict) -> None:
    """
    Score all eligible candidates across all instruments.
    Assign meta_status = GREEN (promoted) or RED (rejected/spike/thin).
    """
    min_windows = cfg.get("min_windows_promoted", 5)

    # Fetch all instruments in this granularity
    with conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT instrument FROM sweep_leaderboard WHERE granularity = %s",
            (granularity,)
        )
        instruments = [r[0] for r in cur.fetchall()]

    all_scores: list[dict] = []
    instrument_rows: dict[str, list[dict]] = {}

    print(f"\nDiscovery — {granularity} ({len(instruments)} instruments)")

    for instrument in sorted(instruments):
        universe = fetch_universe(conn, granularity, instrument, min_windows)
        if not universe:
            continue
        instrument_rows[instrument] = universe

        for candidate in universe:
            result = score_candidate(candidate, universe)
            if result is not None:
                result["instrument"] = instrument
                result["candidate"]  = candidate
                all_scores.append(result)

    if not all_scores:
        print("  No scoreable candidates found.")
        return

    # Dynamic thresholds from actual distribution
    nqs_vals = [s["neighborhood_quality_score"] for s in all_scores]
    nqs_p70  = float(np.percentile(nqs_vals, DISCOVERY_PERCENTILE))
    threshold = max(nqs_p70, NPR_ABS_FLOOR)

    print(f"  NQS P{DISCOVERY_PERCENTILE}: {nqs_p70:.4f} | "
          f"Effective threshold: {threshold:.4f}")

    n_green = n_red = n_spike = n_thin = 0

    for s in all_scores:
        candidate = s["candidate"]
        instrument = s["instrument"]
        nqs      = s["neighborhood_quality_score"]
        sqn_gap  = s["sqn_gap"]

        if (nqs >= threshold
                and nqs > NQS_ABS_FLOOR
                and sqn_gap <= SPIKE_TOLERANCE):
            status = "GREEN"
            n_green += 1
        elif sqn_gap > SPIKE_TOLERANCE:
            status = "RED_SPIKE"
            n_spike += 1
        else:
            status = "RED"
            n_red += 1

        write_meta_status(
            conn, granularity, instrument,
            candidate, s, status,
        )

    # Mark thin candidates (< MIN_NEIGHBORS) as RED_THIN
    for instrument, universe in instrument_rows.items():
        for candidate in universe:
            neighbors = find_neighbors(candidate, universe)
            if len(neighbors) < MIN_NEIGHBORS:
                write_meta_status(
                    conn, granularity, instrument,
                    candidate, {"neighbor_count": len(neighbors),
                                "neighborhood_quality_score": 0.0,
                                "sqn_gap": 0.0},
                    "RED_THIN",
                )
                n_thin += 1

    conn.commit()
    total = n_green + n_red + n_spike + n_thin
    print(f"  GREEN={n_green} RED={n_red} "
          f"RED_SPIKE={n_spike} RED_THIN={n_thin} "
          f"total={total}")


# ---------------------------------------------------------------------------
# Stability mode
# ---------------------------------------------------------------------------

def run_stability(conn, granularity: str, cfg: dict) -> None:
    """
    Audit currently deployed strategies.
    MISSING = hard kill switch (config not in leaderboard at all).
    RED/AMBER/GREEN based on neighborhood health.
    """
    min_windows = cfg.get("min_windows_promoted", 5)
    deployed_for_tf = [d for d in DEPLOYED if d["granularity"] == granularity]

    if not deployed_for_tf:
        print(f"\nStability — {granularity}: no deployed strategies registered.")
        return

    print(f"\nStability — {granularity} ({len(deployed_for_tf)} deployed)")

    # Fetch leaderboard per instrument for deployed strategies
    instruments = list({d["instrument"] for d in deployed_for_tf})
    universes: dict[str, list[dict]] = {}
    for inst in instruments:
        universes[inst] = fetch_universe(
            conn, granularity, inst, min_windows
        )

    all_scores: list[dict] = []
    missing: list[dict] = []

    for deployed in deployed_for_tf:
        inst    = deployed["instrument"]
        universe = universes.get(inst, [])
        d_norm  = _norm_row(deployed)

        # Find exact match in leaderboard
        exact = None
        for row in universe:
            if all(
                row.get(k) == d_norm.get(k)
                for k in ["anchor", "anchor2", "continuation",
                           "continuation2", "gap", "indicator",
                           "direction", "combo_type", "timeout_bars"]
            ):
                exact = row
                break

        if exact is None:
            missing.append(deployed)
            print(f"  MISSING: {inst} strategy_id={deployed['strategy_id']}")
            continue

        result = score_candidate(exact, universe)
        if result is not None:
            result["instrument"] = inst
            result["candidate"]  = exact
            result["strategy_id"] = deployed["strategy_id"]
            all_scores.append(result)
        else:
            # Thin neighborhood
            write_meta_status(
                conn, granularity, inst,
                exact,
                {"neighbor_count": len(find_neighbors(exact, universe)),
                 "neighborhood_quality_score": 0.0,
                 "sqn_gap": 0.0},
                "RED",
            )

    if not all_scores:
        conn.commit()
        return

    # Dynamic threshold
    nqs_vals  = [s["neighborhood_quality_score"] for s in all_scores]
    nqs_p60   = float(np.percentile(nqs_vals, STABILITY_PERCENTILE))
    threshold = max(nqs_p60, NPR_ABS_FLOOR)

    print(f"  NQS P{STABILITY_PERCENTILE}: {nqs_p60:.4f} | "
          f"Effective threshold: {threshold:.4f}")

    n_green = n_amber = n_red = 0

    for s in all_scores:
        candidate   = s["candidate"]
        instrument  = s["instrument"]
        strategy_id = s["strategy_id"]
        nqs         = s["neighborhood_quality_score"]
        sqn_gap     = s["sqn_gap"]
        n_nbr       = s["neighbor_count"]

        if (nqs >= threshold
                and nqs > NQS_ABS_FLOOR
                and sqn_gap <= SPIKE_TOLERANCE
                and n_nbr >= MIN_NEIGHBORS):
            status = "GREEN"
            n_green += 1
        elif n_nbr < MIN_NEIGHBORS or nqs <= NQS_ABS_FLOOR:
            status = "RED"
            n_red += 1
        else:
            status = "AMBER"
            n_amber += 1

        print(f"  [{status:5s}] {instrument} id={strategy_id} "
              f"nqs={nqs:.4f} gap={sqn_gap:.4f} nbrs={n_nbr}")

        write_meta_status(
            conn, granularity, instrument,
            candidate, s, status,
        )

    conn.commit()
    print(f"  GREEN={n_green} AMBER={n_amber} RED={n_red} "
          f"MISSING={len(missing)}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="CandleLab Meta Sweep — Discovery and Stability"
    )
    parser.add_argument(
        "--mode", choices=["discovery", "stability"],
        required=True, help="discovery or stability"
    )
    parser.add_argument(
        "--tf", choices=["M30", "H1"],
        default="M30", help="Timeframe (default: M30)"
    )
    args, _ = parser.parse_known_args()
    return args


if __name__ == "__main__":
    args = parse_args()
    cfg  = TIMEFRAME_CONFIGS[args.tf]

    with psycopg2.connect(DB_URL) as conn:
        if args.mode == "discovery":
            run_discovery(conn, args.tf, cfg)
        else:
            run_stability(conn, args.tf, cfg)

    print("\nMeta sweep complete.")
