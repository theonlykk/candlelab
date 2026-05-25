"""
Meta sweep stability — parameter stability report for deployed Tranche 2 strategies.

Reads existing leaderboard CSVs from output/ftmo_m30/ only. No DB, no sweep re-run.
"""

from __future__ import annotations

import os
import warnings

import pandas as pd

OUTPUT_DIR = r"d:\candlelab\scripts\output\ftmo_m30"
REPORT_PATH = os.path.join(OUTPUT_DIR, "meta_stability_report.csv")

PARAM_COLS = ["anchor", "continuation", "gap", "indicator", "direction"]

DEPLOYED = [
    {"strategy_id": 55, "instrument": "GBP_CAD", "anchor": "shooting_star", "continuation": "", "gap": "", "indicator": "", "direction": "long"},
    {"strategy_id": 56, "instrument": "USD_CAD", "anchor": "shooting_star", "continuation": "", "gap": "", "indicator": "", "direction": "long"},
    {"strategy_id": 57, "instrument": "AUD_JPY", "anchor": "morning_star", "continuation": "", "gap": "", "indicator": "", "direction": "long"},
    {"strategy_id": 58, "instrument": "GBP_JPY", "anchor": "shooting_star", "continuation": "", "gap": "", "indicator": "", "direction": "long"},
    {"strategy_id": 59, "instrument": "CAD_JPY", "anchor": "hammer", "continuation": "", "gap": "", "indicator": "", "direction": "short"},
    {"strategy_id": 60, "instrument": "AUD_NZD", "anchor": "hammer", "continuation": "", "gap": "", "indicator": "", "direction": "short"},
    {"strategy_id": 61, "instrument": "USD_JPY", "anchor": "shooting_star", "continuation": "", "gap": "", "indicator": "", "direction": "long"},
    {"strategy_id": 62, "instrument": "GBP_USD", "anchor": "engulfing", "continuation": "", "gap": "", "indicator": "", "direction": "long"},
    {"strategy_id": 63, "instrument": "GBP_AUD", "anchor": "engulfing", "continuation": "", "gap": "", "indicator": "", "direction": "short"},
    {"strategy_id": 64, "instrument": "CHF_JPY", "anchor": "engulfing", "continuation": "", "gap": "", "indicator": "", "direction": "short"},
    {"strategy_id": 65, "instrument": "GBP_NZD", "anchor": "hammer", "continuation": "", "gap": "", "indicator": "", "direction": "long"},
    {"strategy_id": 66, "instrument": "EUR_NZD", "anchor": "hammer", "continuation": "", "gap": "", "indicator": "", "direction": "long"},
    {"strategy_id": 67, "instrument": "NZD_USD", "anchor": "engulfing", "continuation": "", "gap": "", "indicator": "", "direction": "long"},
]

STATUS_PRIORITY = {"RED": 1, "AMBER": 2, "GREEN": 3}

CSV_COLUMNS = [
    "strategy_id",
    "instrument",
    "anchor",
    "direction",
    "deployed_sqn100",
    "deployed_mean_r",
    "n_windows_promoted",
    "neighbor_count",
    "neighbor_pass_rate",
    "neighbor_median_sqn",
    "sqn_degradation",
    "status",
]


def norm_param(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    s = str(val).strip()
    if s.lower() in ("nan", "none"):
        return ""
    return s


def normalize_deployed(cfg: dict) -> dict:
    out = dict(cfg)
    for col in PARAM_COLS:
        out[col] = norm_param(out.get(col, ""))
    return out


def normalize_leaderboard(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in PARAM_COLS:
        if col not in out.columns:
            out[col] = ""
        out[col] = out[col].apply(norm_param)
    out["gap"] = out["gap"].astype(str)
    return out


def hamming_distance(row: pd.Series, cfg: dict) -> int:
    return sum(1 for col in PARAM_COLS if row[col] != cfg[col])


def assign_status(npr: float, neighbor_median_sqn: float, sqn_degradation: float) -> str:
    if npr == 0 or neighbor_median_sqn <= 0:
        return "RED"
    if npr >= 0.30 and sqn_degradation >= 0.33:
        return "GREEN"
    if 0 < npr < 0.30 or sqn_degradation < 0.33:
        return "AMBER"
    return "AMBER"


def load_leaderboard(instrument: str) -> pd.DataFrame | None:
    path = os.path.join(OUTPUT_DIR, f"leaderboard_{instrument}.csv")
    if not os.path.isfile(path):
        warnings.warn(f"Leaderboard CSV missing: {path}")
        return None
    return normalize_leaderboard(pd.read_csv(path))


def find_deployed_row(df: pd.DataFrame, cfg: dict) -> pd.Series | None:
    mask = pd.Series(True, index=df.index)
    for col in PARAM_COLS:
        mask &= df[col] == cfg[col]
    hits = df[mask]
    if hits.empty:
        return None
    if len(hits) > 1:
        warnings.warn(
            f"Multiple leaderboard rows match deployed config for {cfg['instrument']} "
            f"(strategy_id={cfg['strategy_id']}); using first match"
        )
    return hits.iloc[0]


def find_neighbors(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    dists = df.apply(lambda row: hamming_distance(row, cfg), axis=1)
    return df[dists == 1].copy()


def analyze_deployed(cfg: dict) -> dict:
    cfg = normalize_deployed(cfg)
    base = {
        "strategy_id": cfg["strategy_id"],
        "instrument": cfg["instrument"],
        "anchor": cfg["anchor"],
        "direction": cfg["direction"],
        "deployed_sqn100": None,
        "deployed_mean_r": None,
        "n_windows_promoted": None,
        "neighbor_count": None,
        "neighbor_pass_rate": None,
        "neighbor_median_sqn": None,
        "sqn_degradation": None,
        "status": "RED",
    }

    df = load_leaderboard(cfg["instrument"])
    if df is None:
        return base

    deployed_row = find_deployed_row(df, cfg)
    if deployed_row is None:
        warnings.warn(
            f"Deployed config not found in leaderboard for {cfg['instrument']} "
            f"(strategy_id={cfg['strategy_id']})"
        )
        return base

    deployed_sqn100 = float(deployed_row["oos_sqn100"])
    deployed_mean_r = float(deployed_row["oos_mean_r"])
    n_windows_promoted = float(deployed_row["n_windows_promoted"])

    neighbors = find_neighbors(df, cfg)
    neighbor_count = len(neighbors)

    if neighbor_count == 0:
        npr = 0.0
        neighbor_median_sqn = 0.0
    else:
        passes = (
            (neighbors["oos_mean_r"] > 0)
            & (neighbors["n_windows_promoted"] >= 5)
        ).sum()
        npr = float(passes) / neighbor_count
        neighbor_median_sqn = float(neighbors["oos_sqn100"].median())

    if deployed_sqn100 == 0:
        sqn_degradation = 0.0
    else:
        sqn_degradation = neighbor_median_sqn / deployed_sqn100

    status = assign_status(npr, neighbor_median_sqn, sqn_degradation)

    return {
        "strategy_id": cfg["strategy_id"],
        "instrument": cfg["instrument"],
        "anchor": cfg["anchor"],
        "direction": cfg["direction"],
        "deployed_sqn100": deployed_sqn100,
        "deployed_mean_r": deployed_mean_r,
        "n_windows_promoted": n_windows_promoted,
        "neighbor_count": neighbor_count,
        "neighbor_pass_rate": npr,
        "neighbor_median_sqn": neighbor_median_sqn,
        "sqn_degradation": sqn_degradation,
        "status": status,
    }


def print_console_table(rows: list[dict]) -> None:
    headers = [
        "id",
        "instrument",
        "anchor",
        "dir",
        "dep_sqn",
        "dep_mean_r",
        "win_prom",
        "n_nbr",
        "NPR",
        "nbr_sqn",
        "sqn_deg",
        "status",
    ]
    print("\n" + " ".join(f"{h:>10}" for h in headers))
    print("-" * (11 * len(headers)))
    for r in rows:
        dep_sqn = r["deployed_sqn100"]
        dep_mean = r["deployed_mean_r"]
        win_prom = r["n_windows_promoted"]
        nbr_sqn = r["neighbor_median_sqn"]
        sqn_deg = r["sqn_degradation"]
        npr = r["neighbor_pass_rate"]
        print(
            f"{r['strategy_id']:>10d} "
            f"{r['instrument']:>10} "
            f"{r['anchor']:>10} "
            f"{r['direction']:>10} "
            f"{dep_sqn if dep_sqn is not None else 'NA':>10} "
            f"{dep_mean if dep_mean is not None else 'NA':>10} "
            f"{win_prom if win_prom is not None else 'NA':>10} "
            f"{r['neighbor_count'] if r['neighbor_count'] is not None else 'NA':>10} "
            f"{npr if npr is not None else 'NA':>10.3f} "
            f"{nbr_sqn if nbr_sqn is not None else 'NA':>10} "
            f"{sqn_deg if sqn_deg is not None else 'NA':>10} "
            f"{r['status']:>10}"
        )


def main() -> None:
    normalized_deployed = [normalize_deployed(cfg) for cfg in DEPLOYED]
    results = [analyze_deployed(cfg) for cfg in normalized_deployed]

    results.sort(
        key=lambda r: (
            STATUS_PRIORITY[r["status"]],
            r["sqn_degradation"] if r["sqn_degradation"] is not None else float("inf"),
        )
    )

    print_console_table(results)

    report_df = pd.DataFrame(results, columns=CSV_COLUMNS)
    report_df.to_csv(REPORT_PATH, index=False)
    print(f"\nReport written: {REPORT_PATH}")


if __name__ == "__main__":
    main()
