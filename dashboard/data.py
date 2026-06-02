import decimal
import json
import os

import numpy as np
import pandas as pd
import psycopg2.pool

pool = psycopg2.pool.ThreadedConnectionPool(
    minconn=1,
    maxconn=3,
    dsn=os.environ["DATABASE_URL"],
)


def get_leaderboard() -> pd.DataFrame:
    conn = pool.getconn()
    try:
        query = """
            SELECT instrument, granularity,
                   anchor, anchor2, continuation, continuation2,
                   gap, indicator, direction, combo_type, timeout_bars,
                   oos_sqn100, oos_mean_r, oos_n_trades,
                   n_windows_promoted, meta_status,
                   id
            FROM sweep_leaderboard
            ORDER BY oos_sqn100 DESC NULLS LAST
        """
        df = pd.read_sql(query, conn)
        for col in df.select_dtypes(include='object').columns:
            df[col] = df[col].apply(
                lambda x: float(x) if isinstance(x, decimal.Decimal) else x
            )
        return df
    finally:
        pool.putconn(conn)


def get_oos_curve(
    instrument: str, granularity: str, anchor: str,
    direction: str, timeout_bars: int
) -> pd.DataFrame:
    """
    Fetch oos_r_list directly from sweep_leaderboard.
    No join needed — leaderboard already contains the pooled OOS returns.
    Returns single-row DataFrame with oos_r_list column.
    """
    conn = pool.getconn()
    try:
        query = """
            SELECT oos_r_list
            FROM sweep_leaderboard
            WHERE instrument   = %s
              AND granularity   = %s
              AND anchor        = %s
              AND direction     = %s
              AND timeout_bars  = %s
            LIMIT 1
        """
        df = pd.read_sql(
            query, conn,
            params=(instrument, granularity, anchor,
                    direction, timeout_bars)
        )
        return df
    finally:
        pool.putconn(conn)


def _parse_oos_r_list(value) -> list[float] | None:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (json.JSONDecodeError, TypeError, ValueError):
            return None
    if not isinstance(value, (list, tuple, np.ndarray)):
        return None
    try:
        return [float(r) for r in value]
    except (TypeError, ValueError):
        return None


def build_equity_curve(oos_df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    """
    Build equity curve from pooled oos_r_list in sweep_leaderboard.
    oos_r_list is a flat list of R-multiples across all promoted windows.
    """
    if oos_df.empty:
        return np.array([]), np.array([])

    raw = oos_df["oos_r_list"].iloc[0]
    returns = _parse_oos_r_list(raw)

    if not returns:
        return np.array([]), np.array([])

    y = np.cumsum(returns)
    x = np.arange(len(returns))
    return x, y
