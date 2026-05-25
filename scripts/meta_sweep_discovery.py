"""
Meta sweep discovery — Tranche 3 candidate discovery from sweep leaderboards.

Reads existing leaderboard CSVs from configured source dirs. No DB, no sweep re-run.
"""

from __future__ import annotations

import glob
import os
import warnings

import pandas as pd

ACTIVE_BOOK = [
    {"strategy_id": 55, "instrument": "GBP_CAD"},
    {"strategy_id": 56, "instrument": "USD_CAD"},
    {"strategy_id": 57, "instrument": "AUD_JPY"},
    {"strategy_id": 58, "instrument": "GBP_JPY"},
    {"strategy_id": 61, "instrument": "USD_JPY"},
    {"strategy_id": 63, "instrument": "GBP_AUD"},
]
MAX_PER_CURRENCY = 3

T3_MIN_NPR = 0.40
T3_MIN_SQN_DEG = 0.50
T3_MIN_OWN_SQN = 1.0
T3_MIN_N_WINDOWS = 5
T3_MIN_MEAN_R = 0.0

SOURCES = [
    {"label": "ftmo_m30", "dir": r"d:\candlelab\scripts\output\ftmo_m30"},
    {"label": "ftmo_h1", "dir": r"d:\candlelab\scripts\output\ftmo_h1"},
]

OUTPUT_PATH = r"d:\candlelab\scripts\output\tranche3_candidates.csv"

PARAM_COLS = ["anchor", "continuation", "gap", "indicator", "direction"]

CSV_COLUMNS = [
    "source",
    "instrument",
    "anchor",
    "continuation",
    "gap",
    "indicator",
    "direction",
    "oos_sqn100",
    "oos_mean_r",
    "n_windows_promoted",
    "neighbor_count",
    "neighbor_pass_rate",
    "neighbor_median_sqn",
    "sqn_degradation",
    "currency_status",
]

CURRENCY_STATUS_PRIORITY = {"OK": 0, "BLOCKED_CURRENCY": 1}


def build_blocked_currencies(active_book: list[dict], max_per_currency: int) -> set[str]:
    counts: dict[str, int] = {}
    for entry in active_book:
        base, quote = entry["instrument"].split("_", 1)
        counts[base] = counts.get(base, 0) + 1
        counts[quote] = counts.get(quote, 0) + 1
    return {currency for currency, count in counts.items() if count >= max_per_currency}


def normalize_param_series(series: pd.Series) -> pd.Series:
    out = series.fillna("").astype(str).str.strip()
    return out.replace({"nan": "", "none": "", "NaN": "", "None": ""})


def normalize_leaderboard(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in PARAM_COLS:
        if col not in out.columns:
            out[col] = ""
        out[col] = normalize_param_series(out[col])
    out["gap"] = out["gap"].astype(str)
    return out


def instrument_from_filename(path: str) -> str:
    base = os.path.basename(path)
    return base[len("leaderboard_") : -len(".csv")]


def find_hamming1_neighbors(df: pd.DataFrame, candidate: pd.Series) -> pd.DataFrame:
    mismatches = pd.Series(0, index=df.index, dtype=int)
    for col in PARAM_COLS:
        mismatches += (df[col] != candidate[col]).astype(int)
    return df[mismatches == 1].copy()


def currency_status_for_instrument(instrument: str, blocked: set[str]) -> str:
    base, quote = instrument.split("_", 1)
    if base in blocked or quote in blocked:
        return "BLOCKED_CURRENCY"
    return "OK"


def passes_green_gate(npr: float, sqn_degradation: float, neighbor_median_sqn: float) -> bool:
    return (
        npr >= T3_MIN_NPR
        and sqn_degradation >= T3_MIN_SQN_DEG
        and neighbor_median_sqn > 0
    )


def analyze_candidate(
    source_label: str,
    instrument: str,
    candidate: pd.Series,
    full_df: pd.DataFrame,
    blocked_currencies: set[str],
) -> dict | None:
    candidate_sqn100 = float(candidate["oos_sqn100"])
    neighbors = find_hamming1_neighbors(full_df, candidate)
    neighbor_count = len(neighbors)
    if neighbor_count == 0:
        return None

    passes = (
        (neighbors["oos_mean_r"] > T3_MIN_MEAN_R)
        & (neighbors["n_windows_promoted"] >= T3_MIN_N_WINDOWS)
    ).sum()
    npr = float(passes) / neighbor_count
    neighbor_median_sqn = float(neighbors["oos_sqn100"].median())

    if candidate_sqn100 == 0:
        sqn_degradation = 0.0
    else:
        sqn_degradation = neighbor_median_sqn / candidate_sqn100

    if not passes_green_gate(npr, sqn_degradation, neighbor_median_sqn):
        return None

    return {
        "source": source_label,
        "instrument": instrument,
        "anchor": candidate["anchor"],
        "continuation": candidate["continuation"],
        "gap": candidate["gap"],
        "indicator": candidate["indicator"],
        "direction": candidate["direction"],
        "oos_sqn100": candidate_sqn100,
        "oos_mean_r": float(candidate["oos_mean_r"]),
        "n_windows_promoted": float(candidate["n_windows_promoted"]),
        "neighbor_count": neighbor_count,
        "neighbor_pass_rate": npr,
        "neighbor_median_sqn": neighbor_median_sqn,
        "sqn_degradation": sqn_degradation,
        "currency_status": currency_status_for_instrument(instrument, blocked_currencies),
    }


def discover_from_source(source: dict, blocked_currencies: set[str]) -> list[dict]:
    source_dir = source["dir"]
    source_label = source["label"]
    pattern = os.path.join(source_dir, "leaderboard_*.csv")
    candidates: list[dict] = []

    for csv_path in sorted(glob.glob(pattern)):
        base = os.path.basename(csv_path)
        if base == "meta_stability_report.csv":
            continue

        try:
            raw = pd.read_csv(csv_path)
        except Exception as exc:
            warnings.warn(f"Failed to read {csv_path}: {exc}")
            continue

        if raw.empty:
            warnings.warn(f"Leaderboard CSV empty, skipping: {csv_path}")
            continue

        instrument = instrument_from_filename(csv_path)
        full_df = normalize_leaderboard(raw)
        pool = full_df[full_df["oos_sqn100"] >= T3_MIN_OWN_SQN]

        for _, row in pool.iterrows():
            result = analyze_candidate(
                source_label, instrument, row, full_df, blocked_currencies
            )
            if result is not None:
                candidates.append(result)

    return candidates


def print_console_table(rows: list[dict]) -> None:
    headers = [
        "source",
        "instrument",
        "anchor",
        "dir",
        "oos_sqn",
        "oos_mean_r",
        "win_prom",
        "n_nbr",
        "NPR",
        "nbr_sqn",
        "sqn_deg",
        "currency",
    ]
    print("\n" + " ".join(f"{h:>10}" for h in headers))
    print("-" * (11 * len(headers)))
    for r in rows:
        print(
            f"{r['source']:>10} "
            f"{r['instrument']:>10} "
            f"{r['anchor']:>10} "
            f"{r['direction']:>10} "
            f"{r['oos_sqn100']:>10.3f} "
            f"{r['oos_mean_r']:>10.3f} "
            f"{r['n_windows_promoted']:>10.0f} "
            f"{r['neighbor_count']:>10d} "
            f"{r['neighbor_pass_rate']:>10.3f} "
            f"{r['neighbor_median_sqn']:>10.3f} "
            f"{r['sqn_degradation']:>10.3f} "
            f"{r['currency_status']:>10}"
        )


def main() -> None:
    blocked_currencies = build_blocked_currencies(ACTIVE_BOOK, MAX_PER_CURRENCY)

    all_candidates: list[dict] = []
    for source in SOURCES:
        all_candidates.extend(discover_from_source(source, blocked_currencies))

    all_candidates.sort(
        key=lambda r: (
            CURRENCY_STATUS_PRIORITY[r["currency_status"]],
            -r["sqn_degradation"],
            -r["oos_sqn100"],
        )
    )

    print_console_table(all_candidates)

    out_df = pd.DataFrame(all_candidates, columns=CSV_COLUMNS)
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    out_df.to_csv(OUTPUT_PATH, index=False)
    print(f"\nCandidates written: {OUTPUT_PATH} ({len(all_candidates)} rows)")


if __name__ == "__main__":
    main()
