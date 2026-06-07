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
    "timeout_bars":   0.5,  # ADR-105: lowered from 5.0 — different timeouts are neighbours;
                            # weight 0.5 allows cross-timeout NQS while respecting holding
                            # period distinction. Weight 5.0 destroyed neighbourhoods in
                            # sparse V8 universe.
    # zero-weight — part of identity but excluded from distance
    "anchor2":        0.0,
    "continuation2":  0.0,
    "gap":            0.0,
    "indicator":      0.0,
}

NEIGHBORHOOD_THRESHOLD = 1.0   # weighted distance <= 1.0 = valid neighbor
MIN_NEIGHBORS          = 2   # ADR-088: reduced from 3 — single-pattern combos have constrained neighbor space
SPIKE_TOLERANCE        = 1.0   # candidate SQN cannot exceed neighborhood by > 1.0
NQS_GLOBAL_FLOOR       = 0.30  # ADR-094: absolute global standard — no relative ranking

# Stability uses P60 (discovery uses NQS_GLOBAL_FLOOR — ADR-094)
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
    for k, v in row.items():
        if isinstance(v, decimal.Decimal):
            row[k] = float(v)
    return row


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


def apply_sap(oos_mean_r: float, timeout_bars: int) -> float:
    """
    Slippage Asymmetry Penalty (ADR-094).
    Discounts oos_mean_r for short-timeout combos which are more vulnerable
    to stop-hunts and spread blowouts at session edges.
    Penalty schedule (M30):
        timeout_bars <= 20:  15% discount
        timeout_bars <= 40:  10% discount
        timeout_bars <= 60:   5% discount
        timeout_bars >  60:   0% discount
    Note: Full sl_mult-based SAP deferred to ADR-096 (combo space expansion).
    """
    if timeout_bars <= 20:
        discount = 0.15
    elif timeout_bars <= 40:
        discount = 0.10
    elif timeout_bars <= 60:
        discount = 0.05
    else:
        discount = 0.0
    return oos_mean_r * (1.0 - discount)


def compute_nqs_with_cvsp(neighbor_mean_rs: list) -> float:
    """
    Neighborhood Quality Score with Cross-Validation Stability Penalty (ADR-094).

    NQS_raw = mean of max(0, r) for r in neighbor_mean_rs
    CVSP    = 1 + sqrt(variance of neighbor_mean_rs)
    NQS     = NQS_raw / CVSP

    A low-variance neighborhood (stable plateau) → minimal penalty.
    A high-variance neighborhood (fragile spike) → heavy penalty.
    Requires MIN_NEIGHBORS = 2 for variance to be defined.
    """
    if len(neighbor_mean_rs) < 2:
        return 0.0
    nqs_raw = float(np.mean([max(0.0, r) for r in neighbor_mean_rs]))
    variance = float(np.var(neighbor_mean_rs, ddof=1))
    cvsp = 1.0 + np.sqrt(variance)
    return nqs_raw / cvsp


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

    neighbor_mean_rs = [
        apply_sap(r["oos_mean_r"], r["timeout_bars"])
        for r in neighbors
    ]
    nqs = compute_nqs_with_cvsp(neighbor_mean_rs)

    # SQN gap — spike filter
    # Guard: filter None/NaN from neighbor SQN list before median.
    # NULL oos_sqn100 from DB becomes None via psycopg2; NaN from
    # degenerate combos must also be excluded. If all neighbors are
    # None/NaN, fall back to 0.0 to avoid StatisticsError crash.
    neighbor_sqns = [
        r["oos_sqn100"] for r in neighbors
        if r["oos_sqn100"] is not None
        and r["oos_sqn100"] == r["oos_sqn100"]  # NaN != NaN
    ]
    if not neighbor_sqns:
        neighbor_median_sqn = 0.0
    else:
        neighbor_median_sqn = float(median(neighbor_sqns))

    # Guard: candidate oos_sqn100 may also be None/NaN from DB.
    raw_cand_sqn = candidate.get("oos_sqn100")
    if raw_cand_sqn is None or raw_cand_sqn != raw_cand_sqn:
        sqn_gap = float("nan")
    else:
        sqn_gap = float(raw_cand_sqn) - neighbor_median_sqn

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
            promoted_window_indices,
            neighborhood_quality_score, sqn_gap, meta_status
        FROM sweep_leaderboard
        WHERE granularity = %s
          AND instrument  = %s
          AND n_windows_promoted >= %s
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
                float(scores.get("neighborhood_quality_score")),
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

def run_discovery(conn, granularity: str, cfg: dict,
                  instrument_filter: str | None = None) -> None:
    """
    Score all eligible candidates per instrument.
    Assign meta_status = GREEN (promoted) or RED (rejected/spike/thin).
    """
    # ADR-097B: wipe stale meta_status before scoring.
    # Ensures no results from a previous meta_sweep run (with different
    # parameters) contaminate the current leaderboard.
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE sweep_leaderboard
            SET meta_status                = NULL,
                neighborhood_quality_score = NULL,
                neighbor_count             = NULL,
                sqn_gap                    = NULL
            WHERE granularity = %s
        """, (granularity,))
    conn.commit()
    print(f"  [meta] wiped stale meta_status for {granularity}")

    min_windows = cfg.get("min_windows_promoted", 3)

    # Fetch all instruments in this granularity
    with conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT instrument FROM sweep_leaderboard WHERE granularity = %s",
            (granularity,)
        )
        instruments = [r[0] for r in cur.fetchall()]

    # Single-instrument filter for dry-run validation (--instrument flag).
    if instrument_filter is not None:
        if instrument_filter not in instruments:
            print(f"  [meta] WARNING: {instrument_filter} not found in "
                  f"leaderboard for {granularity}. Available: "
                  f"{sorted(instruments)}")
            return
        instruments = [instrument_filter]
        print(f"  [meta] DRY-RUN: single instrument mode → {instrument_filter}")

    print(f"\nDiscovery — {granularity} ({len(instruments)} instruments)")

    for instrument in sorted(instruments):
        universe = fetch_universe(conn, granularity, instrument,
                                  min_windows)
        if not universe:
            continue
        # Pre-compute global recency threshold ONCE across all candidates.
        # Must use the global max window index — never per-candidate max.
        # A candidate is recent if promoted in any of the last 5 windows
        # relative to the most recent window in the entire sweep.
        # WARNING: never compute max_window from per-candidate history —
        # that gives every candidate a local threshold of -2 or lower and
        # makes the recency gate a no-op. (Gemini ruling ADR-101.)
        all_windows: list[int] = []
        for _cand in universe:
            raw = _cand.get("promoted_window_indices")
            if isinstance(raw, str):
                import json as _json
                all_windows.extend(int(w) for w in _json.loads(raw))
            elif isinstance(raw, list):
                all_windows.extend(int(w) for w in raw)
        if not all_windows:
            # No candidate has promoted_window_indices data.
            # This means either: first run (no sweep data yet), or
            # _write_leaderboard failed to populate the column.
            # Set threshold to +inf so ALL candidates fail recency
            # and are labeled RED_STALE. Never default to -4 which
            # would disable the gate entirely. (DeepSeek SF-2 ruling.)
            print(
                "  [meta] WARNING: no promoted_window_indices data found "
                "across entire universe. Recency gate will reject all "
                "candidates. Check _write_leaderboard window_idx column."
            )
            global_recency_threshold = float("inf")
        else:
            global_max_window = max(all_windows)
            # Recency lookback adapts to timeframe (Gemini ADR-105 ruling):
            # H1 uses last 3 windows (~24 weeks) to match M30's ~20-week
            # recency window. M30 uses last 5 windows (~20 weeks).
            _recency_lookback = cfg.get("recency_lookback_windows", 5)
            global_recency_threshold = global_max_window - (_recency_lookback - 1)
            # e.g. H1: max=26, lookback=3 → threshold=24 (windows 24,25,26)
            # e.g. M30: max=26, lookback=5 → threshold=22 (windows 22-26)

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
            conn.commit()
            continue

        print(f"  {instrument}: NQS_GLOBAL_FLOOR={NQS_GLOBAL_FLOOR:.2f}")

        n_green = n_red = n_spike = n_thin = 0
        n_stale = 0
        n_weak  = 0

        for s in instrument_scores:
            candidate = s["candidate"]
            nqs       = s["neighborhood_quality_score"]
            sqn_gap   = s["sqn_gap"]

            # Parse promoted_window_indices for recency gate.
            # global_recency_threshold pre-computed above — never
            # use per-candidate max (Gemini ruling ADR-101 fatal leak).
            raw_pwi = candidate.get("promoted_window_indices")
            if isinstance(raw_pwi, str):
                import json as _json
                promoted_windows = _json.loads(raw_pwi)
            elif isinstance(raw_pwi, list):
                promoted_windows = raw_pwi
            else:
                promoted_windows = []

            is_recent = any(
                w >= global_recency_threshold for w in promoted_windows
            )

            # Guard: resolve oos_sqn100 safely — None or NaN from DB
            # must not reach the >= comparison (raises TypeError).
            cand_sqn = candidate.get("oos_sqn100")
            if cand_sqn is None or cand_sqn != cand_sqn:
                cand_sqn = 0.0  # treat missing SQN as zero — fails gate

            # Guard: sqn_gap may be NaN if candidate or neighbor SQN
            # was missing. Treat NaN gap as spike (unsafe candidate).
            safe_sqn_gap = sqn_gap if sqn_gap == sqn_gap else float("inf")

            if (nqs >= NQS_GLOBAL_FLOOR
                    and nqs > 0
                    and safe_sqn_gap <= SPIKE_TOLERANCE
                    and cand_sqn >= 0.8
                    and is_recent):
                status = "GREEN"
                n_green += 1
            elif not is_recent:
                status = "RED_STALE"
                n_stale += 1
            elif cand_sqn < 0.8:
                status = "RED_WEAK_SQN"
                n_weak += 1
            elif safe_sqn_gap > SPIKE_TOLERANCE:
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
              f"RED_SPIKE={n_spike} RED_THIN={n_thin} "
              f"RED_STALE={n_stale} RED_WEAK_SQN={n_weak}")


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
    parser.add_argument(
        "--instrument", default=None,
        help=(
            "Optional: run discovery on a single instrument only "
            "(e.g. --instrument GBP_JPY). Useful for dry-run validation "
            "before running the full universe. Default: all instruments."
        )
    )
    args, _ = parser.parse_known_args()
    return args


if __name__ == "__main__":
    args = parse_args()
    cfg  = TIMEFRAME_CONFIGS[args.tf]

    with psycopg2.connect(DB_URL) as conn:
        if args.mode == "discovery":
            run_discovery(conn, args.tf, cfg,
                          instrument_filter=getattr(args, "instrument", None))
        else:
            run_stability(conn, args.tf, cfg)

    print("\nMeta sweep complete.")
