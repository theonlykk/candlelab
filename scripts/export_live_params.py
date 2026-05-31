"""
export_live_params.py — Bridge between offline sweep analytics and
live executor. Run after meta_sweep.py --mode discovery completes.

Usage:
    python scripts/export_live_params.py --tf M30
    python scripts/export_live_params.py --tf H1
    python scripts/export_live_params.py  # runs both granularities
"""
from __future__ import annotations

import argparse
import os

import psycopg2
from dotenv import load_dotenv

load_dotenv(r"d:\candlelab\.env")
DB_URL = os.environ["DATABASE_URL"]

EXPORT_SQL = """
    SELECT DISTINCT ON (l.instrument, l.granularity)
        l.instrument,
        l.granularity,
        i.base_mean,
        i.base_std,
        i.quote_mean,
        i.quote_std,
        i.z_spread_p50,
        i.z_spread_p70,
        i.window_start AS sweep_window_start,
        i.window_end   AS sweep_window_end
    FROM sweep_leaderboard l
    JOIN sweep_is_results i
      ON l.instrument  = i.instrument
     AND l.granularity = i.granularity
     AND l.combo_hash  = i.combo_hash
    WHERE l.meta_status = 'GREEN'
      AND i.bucket_label = 'PROMOTED'
      AND i.base_mean IS NOT NULL
      AND i.z_spread_p50 IS NOT NULL
      {granularity_filter}
    ORDER BY l.instrument, l.granularity, i.window_end DESC
"""

UPSERT_SQL = """
    INSERT INTO live_regime_parameters (
        instrument, granularity,
        base_mean, base_std, quote_mean, quote_std,
        z_spread_p50, z_spread_p70,
        calibrated_at, sweep_window_start, sweep_window_end
    ) VALUES (
        %(instrument)s, %(granularity)s,
        %(base_mean)s, %(base_std)s,
        %(quote_mean)s, %(quote_std)s,
        %(z_spread_p50)s, %(z_spread_p70)s,
        NOW(), %(sweep_window_start)s, %(sweep_window_end)s
    )
    ON CONFLICT (instrument, granularity) DO UPDATE SET
        base_mean          = EXCLUDED.base_mean,
        base_std           = EXCLUDED.base_std,
        quote_mean         = EXCLUDED.quote_mean,
        quote_std          = EXCLUDED.quote_std,
        z_spread_p50       = EXCLUDED.z_spread_p50,
        z_spread_p70       = EXCLUDED.z_spread_p70,
        calibrated_at      = NOW(),
        sweep_window_start = EXCLUDED.sweep_window_start,
        sweep_window_end   = EXCLUDED.sweep_window_end
"""


def export(conn, granularity: str | None = None) -> int:
    """
    Export IS calibration params for GREEN instruments.
    Returns count of rows upserted.
    granularity: "M30", "H1", or None (both).
    """
    if granularity:
        gf = "AND l.granularity = %(granularity)s"
    else:
        gf = ""

    sql = EXPORT_SQL.format(granularity_filter=gf)
    params = {"granularity": granularity} if granularity else {}

    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]

    if not rows:
        print(
            f"  No GREEN instruments found"
            f"{' for ' + granularity if granularity else ''}. "
            "live_regime_parameters not updated."
        )
        return 0

    n = 0
    with conn.cursor() as cur:
        for row in rows:
            row_dict = dict(zip(cols, row))
            try:
                cur.execute("SAVEPOINT export_write")
                cur.execute(UPSERT_SQL, row_dict)
                cur.execute("RELEASE SAVEPOINT export_write")
                print(
                    f"  EXPORTED: {row_dict['instrument']} "
                    f"{row_dict['granularity']} — "
                    f"window_end={row_dict['sweep_window_end']}"
                )
                n += 1
            except Exception as e:
                cur.execute("ROLLBACK TO SAVEPOINT export_write")
                print(
                    f"  ERROR: {row_dict.get('instrument')} "
                    f"{row_dict.get('granularity')}: {e}"
                )

    conn.commit()
    return n


def parse_args():
    parser = argparse.ArgumentParser(
        description="Export sweep IS params to live_regime_parameters"
    )
    parser.add_argument(
        "--tf", choices=["M30", "H1"],
        default=None,
        help="Granularity to export (default: both)"
    )
    args, _ = parser.parse_known_args()
    return args


if __name__ == "__main__":
    args = parse_args()
    with psycopg2.connect(DB_URL) as conn:
        print(f"\nExporting live regime parameters "
              f"({'all granularities' if not args.tf else args.tf})...")
        n = export(conn, args.tf)
        print(f"\nExport complete. {n} instrument(s) updated.")
