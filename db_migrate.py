"""
CandleLab database migrations.
Run manually — never executed by the application at startup.
Add new migrations as functions. Call them explicitly.
"""
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv(r"d:\candlelab\.env")
DB_URL = os.environ["DATABASE_URL"]


def migrate_sweep_leaderboard():
    """ADR-081: Phase 3 sweep_leaderboard table and indexes."""
    ddl = """
        CREATE TABLE IF NOT EXISTS sweep_leaderboard (
            id SERIAL PRIMARY KEY,
            granularity TEXT NOT NULL,
            instrument TEXT NOT NULL,
            anchor TEXT, anchor2 TEXT,
            continuation TEXT, continuation2 TEXT,
            gap INTEGER, indicator TEXT,
            direction TEXT NOT NULL,
            combo_type TEXT,
            timeout_bars INTEGER NOT NULL,
            oos_sqn100 NUMERIC, oos_mean_r NUMERIC,
            oos_n_trades INTEGER, n_windows_promoted INTEGER,
            mdd NUMERIC, sharpe NUMERIC, oos_r_list JSONB,
            neighbor_count INTEGER,
            neighborhood_quality_score NUMERIC,
            sqn_gap NUMERIC, meta_status TEXT,
            sweep_run_id TEXT,
            first_seen_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (
                granularity, instrument,
                anchor, anchor2, continuation, continuation2,
                gap, indicator, direction, combo_type,
                timeout_bars
            )
        );
        CREATE INDEX IF NOT EXISTS idx_leaderboard_hamming
            ON sweep_leaderboard(granularity, instrument,
                                 direction, continuation, anchor);
        CREATE INDEX IF NOT EXISTS idx_leaderboard_meta
            ON sweep_leaderboard(meta_status);
        CREATE INDEX IF NOT EXISTS idx_leaderboard_oos_sqn
            ON sweep_leaderboard(oos_sqn100 DESC);
        CREATE INDEX IF NOT EXISTS idx_leaderboard_windows
            ON sweep_leaderboard(n_windows_promoted);
        CREATE INDEX IF NOT EXISTS idx_leaderboard_updated
            ON sweep_leaderboard(updated_at DESC);
    """
    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
    print("migrate_sweep_leaderboard: done")


# ---------------------------------------------------------------------------
# ADR-089: live_regime_parameters — IS-calibrated Z-score params for
# live Gate 4 execution. Written by export_live_params.py after meta_sweep.
# Manual execution only — never run at application startup.
# ---------------------------------------------------------------------------

def migrate_live_regime_parameters():
    """ADR-089: live_regime_parameters table for live Gate 4 Z-score params."""
    ddl = """
        -- ---------------------------------------------------------------------------
        -- ADR-089: live_regime_parameters — IS-calibrated Z-score params for
        -- live Gate 4 execution. Written by export_live_params.py after meta_sweep.
        -- Manual execution only — never run at application startup.
        -- ---------------------------------------------------------------------------

        CREATE TABLE IF NOT EXISTS live_regime_parameters (
            instrument          VARCHAR(10)      NOT NULL,
            granularity         VARCHAR(10)      NOT NULL,
            base_mean           DOUBLE PRECISION NOT NULL,
            base_std            DOUBLE PRECISION NOT NULL,
            quote_mean          DOUBLE PRECISION NOT NULL,
            quote_std           DOUBLE PRECISION NOT NULL,
            z_spread_p50        DOUBLE PRECISION NOT NULL,
            z_spread_p70        DOUBLE PRECISION NOT NULL,
            calibrated_at       TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
            sweep_window_start  TIMESTAMPTZ      NOT NULL,
            sweep_window_end    TIMESTAMPTZ      NOT NULL,
            PRIMARY KEY (instrument, granularity)
        );
    """
    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
    print("migrate_live_regime_parameters: done")


if __name__ == "__main__":
    migrate_sweep_leaderboard()
