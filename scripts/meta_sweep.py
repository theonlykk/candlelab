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
import decimal
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
MIN_NEIGHBORS          = 2   # ADR-088: reduced from 3 — single-pattern combos have constrained neighbor space
SPIKE_TOLERANCE        = 1.0   # candidate SQN cannot exceed neighborhood by > 1.0
NPR_ABS_FLOOR          = 0.20  # absolute minimum neighborhood quality score
NQS_ABS_FLOOR          = 0.0   # absolute minimum nqs

# Discovery uses P70, Stability uses P60
DISCOVERY_PERCENTILE   = 70
STABILITY_PERCENTILE   = 60

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
          AND n_windows_promoted >= %s
          AND oos_sqn100  > 0
        ORDER BY oos_sqn100 DESC
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (granularity, instrument, min_windows))
        rows = cur.fetchall()
    return [
        _norm_row({
            k: float(v) if isinstance(v, decimal.Decimal) else v
            for k, v in dict(r).items()
        })
        for r in rows
    ]


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
    Score all eligible candidates per instrument.
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

    print(f"\nDiscovery — {granularity} ({len(instruments)} instruments)")

    for instrument in sorted(instruments):
        universe = fetch_universe(conn, granularity, instrument,
                                  min_windows)
        if not universe:
            continue

        instrument_scores = []
        for candidate in universe:
            result = score_candidate(candidate, universe)
            if result is not None:
                result["instrument"] = instrument
                result["candidate"]  = candidate
                instrument_scores.append(result)

        if not instrument_scores:
            # Mark thin candidates for this instrument
            for candidate in universe:
                neighbors = find_neighbors(candidate, universe)
                if len(neighbors) < MIN_NEIGHBORS:
                    write_meta_status(
                        conn, granularity, instrument,
                        candidate,
                        {"neighbor_count": len(neighbors),
                         "neighborhood_quality_score": 0.0,
                         "sqn_gap": 0.0},
                        "RED_THIN",
                    )
            continue

        # Per-instrument threshold — no cross-instrument contamination
        nqs_vals = [float(s["neighborhood_quality_score"])
                    for s in instrument_scores]
        nqs_p70  = float(np.percentile(nqs_vals, DISCOVERY_PERCENTILE))
        threshold = max(nqs_p70, NPR_ABS_FLOOR)

        print(f"  {instrument}: NQS P{DISCOVERY_PERCENTILE}="
              f"{nqs_p70:.4f} | threshold={threshold:.4f}")

        n_green = n_red = n_spike = n_thin = 0

        for s in instrument_scores:
            candidate = s["candidate"]
            nqs       = s["neighborhood_quality_score"]
            sqn_gap   = s["sqn_gap"]

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

        # Mark thin candidates
        for candidate in universe:
            neighbors = find_neighbors(candidate, universe)
            if len(neighbors) < MIN_NEIGHBORS:
                write_meta_status(
                    conn, granularity, instrument,
                    candidate,
                    {"neighbor_count": len(neighbors),
                     "neighborhood_quality_score": 0.0,
                     "sqn_gap": 0.0},
                    "RED_THIN",
                )
                n_thin += 1

        conn.commit()
        print(f"  {instrument}: GREEN={n_green} RED={n_red} "
              f"RED_SPIKE={n_spike} RED_THIN={n_thin}")


# ---------------------------------------------------------------------------
# Stability mode
# ---------------------------------------------------------------------------

def run_stability(conn, granularity: str, cfg: dict) -> None:
    """
    Stability mode deferred pending v2 strategy promotion.
    No deployed strategies exist in sweep engine combo format yet.
    Revisit when first v2 strategy is promoted to live — ADR-088.
    """
    print(
        f"\nStability — {granularity}: DEFERRED. "
        "No v2 strategies promoted yet. "
        "Run discovery mode first."
    )


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
