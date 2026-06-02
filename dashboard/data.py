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
            SELECT instrument, granularity, combo_hash,
                   oos_sqn100, oos_win_rate, oos_trade_count,
                   oos_mean_r, meta_status
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
    instrument: str, granularity: str, combo_hash: str
) -> pd.DataFrame:
    conn = pool.getconn()
    try:
        query = """
            SELECT o.window_start, o.window_end, o.oos_r_list
            FROM sweep_oos_cache o
            JOIN sweep_is_results i
              ON o.instrument = i.instrument
             AND o.granularity = i.granularity
             AND o.window_start = i.window_start
             AND o.combo_hash = i.combo_hash
            WHERE o.instrument = %s
              AND o.granularity = %s
              AND o.combo_hash = %s
              AND i.bucket_label = 'PROMOTED'
            ORDER BY o.window_start ASC
        """
        df = pd.read_sql(
            query, conn, params=(instrument, granularity, combo_hash)
        )
        for col in df.select_dtypes(include='object').columns:
            df[col] = df[col].apply(
                lambda x: float(x) if isinstance(x, decimal.Decimal) else x
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
    Flatten per-window oos_r_list into one returns series.
    Insert NaN between non-contiguous windows; cumulative sum of R multiples.
    """
    returns: list[float] = []
    rows = oos_df.to_dict("records")

    for i, row in enumerate(rows):
        chunk = _parse_oos_r_list(row.get("oos_r_list"))
        if chunk is None:
            continue
        if i > 0 and returns:
            prev = rows[i - 1]
            if prev.get("window_end") != row.get("window_start"):
                returns.append(np.nan)
        returns.extend(chunk)

    if not returns:
        return np.array([]), np.array([])

    y = np.cumsum(returns)
    x = np.arange(len(returns))
    return x, y
