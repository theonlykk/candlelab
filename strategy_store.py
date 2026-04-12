"""
strategy_store.py — PostgreSQL persistence for user strategies
"""
import json
import logging
import random
from datetime import datetime, timezone

import psycopg2.extras

from data import get_conn

log = logging.getLogger(__name__)

VERBS   = ["Swift","Bold","Keen","Sharp","Quick","Fierce","Bright","Dark",
           "Silent","Wild","Brave","Calm","Iron","Gold","Silver","Stone",
           "Steel","Flame","Storm","Wind"]
COLOURS = ["Crimson","Azure","Jade","Onyx","Amber","Indigo","Coral",
           "Ivory","Ebony","Scarlet","Violet","Cobalt","Slate","Rose",
           "Teal","Ochre","Cyan","Pearl","Maroon","Sage"]
ANIMALS = ["Falcon","Wolf","Eagle","Lynx","Raven","Panther","Hawk",
           "Viper","Crane","Tiger","Bear","Fox","Owl","Boar","Stag",
           "Kite","Wren","Drake","Heron","Puma"]


def _auto_name() -> str:
    """Generate a human-readable strategy name for users who skip naming."""
    return f"{random.choice(VERBS)} {random.choice(COLOURS)} {random.choice(ANIMALS)}"


def init_strategy_tables():
    """
    Create (and lightly migrate) the PostgreSQL tables used to persist strategies and trades.

    This function is safe to call repeatedly; it:
    - creates base tables if missing
    - checks for missing columns and adds them via ALTER TABLE
    """
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS candlelab_strategies (
                id           SERIAL PRIMARY KEY,
                name         TEXT,
                patterns     TEXT,
                direction    TEXT,
                window_days  INTEGER,
                instrument   TEXT,
                interval     TEXT,
                created_ts   TEXT,
                live_from_ts TEXT,
                active       INTEGER DEFAULT 1,
                bt_win_pct   DOUBLE PRECISION,
                bt_cum_net   DOUBLE PRECISION,
                bt_cum_gross DOUBLE PRECISION,
                bt_signals   INTEGER,
                bt_tp_hits   INTEGER
            )
        """)
        cur.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            """,
            ("candlelab_strategies",),
        )
        existing = {r["column_name"] for r in cur.fetchall()}
        cur.execute(
            """
            SELECT data_type FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s AND column_name = 'id'
            """,
            ("candlelab_strategies",),
        )
        id_type = cur.fetchone()
        if id_type and id_type.get("data_type") == "text":
            try:
                cur.execute("ALTER TABLE candlelab_strategies DROP COLUMN id")
                cur.execute(
                    "ALTER TABLE candlelab_strategies ADD COLUMN id SERIAL PRIMARY KEY"
                )
                conn.commit()
                cur.execute(
                    """
                    SELECT column_name FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = %s
                    """,
                    ("candlelab_strategies",),
                )
                existing = {r["column_name"] for r in cur.fetchall()}
            except Exception:
                log.exception(
                    "candlelab_strategies.id migration from TEXT to SERIAL failed"
                )
                conn.rollback()
        if "window" in existing and "window_days" not in existing:
            cur.execute(
                "ALTER TABLE candlelab_strategies RENAME COLUMN window TO window_days"
            )
            conn.commit()
            cur.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s
                """,
                ("candlelab_strategies",),
            )
            existing = {r["column_name"] for r in cur.fetchall()}
        for col, typedef in [
            ("name", "TEXT"),
            ("patterns", "TEXT"),
            ("connectors", "TEXT"),
            ("direction", "TEXT"),
            ("window_days", "INTEGER"),
            ("instrument", "TEXT"),
            ("interval", "TEXT"),
            ("created_ts", "TEXT"),
            ("live_from_ts", "TEXT"),
            ("active", "INTEGER"),
            ("device_uuid", "TEXT"),
            ("indicator_filter", "TEXT"),
            ("session_filter", "TEXT"),
            ("bt_win_pct", "DOUBLE PRECISION"),
            ("bt_cum_net", "DOUBLE PRECISION"),
            ("bt_cum_gross", "DOUBLE PRECISION"),
            ("bt_signals", "INTEGER"),
            ("bt_tp_hits", "INTEGER"),
        ]:
            if col not in existing:
                cur.execute(
                    f"ALTER TABLE candlelab_strategies ADD COLUMN {col} {typedef}"
                )
        cur.execute("""
            CREATE TABLE IF NOT EXISTS candlelab_trades (
                id           SERIAL PRIMARY KEY,
                strategy_id  TEXT,
                ts           TEXT,
                entry        DOUBLE PRECISION,
                tp           DOUBLE PRECISION,
                sl           DOUBLE PRECISION,
                outcome      TEXT,
                pnl_gross    DOUBLE PRECISION,
                pnl_net      DOUBLE PRECISION,
                UNIQUE(strategy_id, ts)
            )
        """)
        conn.commit()


def save_strategy(config: dict) -> dict:
    """
    Persist a new strategy and return `{id, name}`.

    `config` is the JSON payload produced by the wizard. Some nested fields are stored as
    JSON text (patterns/connectors/indicator_filter) to keep schema stable as the UI evolves.
    """
    init_strategy_tables()
    name = config.get("name") or _auto_name()
    now  = datetime.now(timezone.utc).isoformat()
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            INSERT INTO candlelab_strategies
              (name, patterns, connectors, direction, window_days, instrument, interval,
               created_ts, live_from_ts, active,
               bt_win_pct, bt_cum_net, bt_cum_gross, bt_signals, bt_tp_hits,
               device_uuid, indicator_filter, session_filter)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id
            """,
            (
                name,
                json.dumps(config.get("patterns", [])),
                json.dumps(config.get("connectors", ["ordered", "ordered"])),
                str(config.get("direction", "")),
                int(config.get("window_days", config.get("window", 5))),
                config.get("instrument", "EUR/USD"),
                config.get("interval", "5m"),
                now,
                now,
                1,
                config.get("bt_win_pct"),
                config.get("bt_cum_net"),
                config.get("bt_cum_gross"),
                config.get("bt_signals"),
                config.get("bt_tp_hits"),
                config.get("device_uuid"),
                json.dumps(config["indicator_filter"]) if config.get("indicator_filter") else None,
                config.get("session_filter"),
            ),
        )
        row = cur.fetchone()
        saved_id = str(row["id"])
        conn.commit()
    return {"id": saved_id, "name": name}


def load_strategies() -> list:
    """Load all active strategies (legacy/global view)."""
    init_strategy_tables()
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT * FROM candlelab_strategies WHERE active=1 ORDER BY created_ts DESC"
        )
        rows = cur.fetchall()
    result = []
    for r in rows:
        d = dict(r)
        d["patterns"] = json.loads(d["patterns"])
        try:
            d["connectors"] = json.loads(d.get("connectors") or '["ordered","ordered"]')
        except Exception:
            d["connectors"] = ["ordered", "ordered"]
        result.append(d)
    return result


def delete_strategy(sid: str):
    """Soft-delete a strategy (keeps history, hides from active lists)."""
    init_strategy_tables()
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("UPDATE candlelab_strategies SET active=0 WHERE id=%s", (sid,))
        conn.commit()


def log_strategy_trade(strategy_id: str, trade: dict):
    """
    Record a live trade outcome for a strategy.

    Uses ON CONFLICT DO NOTHING on (strategy_id, ts) to prevent accidental duplicates if a
    polling loop replays the same trade.
    """
    init_strategy_tables()
    ts = trade["ts"].isoformat() if hasattr(trade["ts"], "isoformat") else str(trade["ts"])
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            INSERT INTO candlelab_trades
              (strategy_id, ts, entry, tp, sl, outcome, pnl_gross, pnl_net)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (strategy_id, ts) DO NOTHING
            """,
            (
                strategy_id,
                ts,
                float(trade["entry"]),
                float(trade["tp"]),
                float(trade["sl"]),
                trade["outcome"],
                float(trade["pnl_gross"]),
                float(trade["pnl_net"]),
            ),
        )
        conn.commit()


def get_strategies_by_device(device_uuid: str) -> list:
    """Load active strategies scoped to a single device UUID (the primary UI view)."""
    init_strategy_tables()
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT * FROM candlelab_strategies WHERE active=1 AND device_uuid=%s ORDER BY created_ts DESC",
            (device_uuid,),
        )
        rows = cur.fetchall()
    result = []
    for r in rows:
        d = dict(r)
        d["patterns"] = json.loads(d["patterns"]) if d.get("patterns") else []
        try:
            d["connectors"] = json.loads(d.get("connectors") or '["ordered","ordered"]')
        except Exception:
            d["connectors"] = ["ordered", "ordered"]
        result.append(d)
    return result


def load_strategy_trades(strategy_id: str) -> list:
    """Load recorded trades for a strategy, newest first."""
    init_strategy_tables()
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT * FROM candlelab_trades WHERE strategy_id=%s ORDER BY ts DESC",
            (strategy_id,),
        )
        rows = cur.fetchall()
    return [dict(r) for r in rows]
