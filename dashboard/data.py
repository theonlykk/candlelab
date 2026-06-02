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
    conn = pool.getconn()
    try:
        query = """
            SELECT 
                o.oos_start AS window_start,
                o.oos_end   AS window_end,
                (o.result_json->>'oos_r_list')::text AS oos_r_list
            FROM sweep_oos_cache o
            JOIN sweep_is_results i
              ON o.instrument   = i.instrument
             AND o.granularity  = i.granularity
             AND o.oos_start    = i.window_start
             AND i.anchor       = %s
             AND i.direction    = %s
             AND i.timeout_bars = %s
            WHERE o.instrument  = %s
              AND o.granularity = %s
              AND i.bucket_label = 'PROMOTED'
            ORDER BY o.oos_start ASC
        """
        df = pd.read_sql(
            query, conn,
            params=(anchor, direction, timeout_bars,
                    instrument, granularity)
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
