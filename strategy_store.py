"""
strategy_store.py — PostgreSQL persistence for user strategies
"""
import hashlib
import json
import logging
import random
from datetime import datetime, timezone

import psycopg2.extras
from psycopg2.extras import Json

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


def _hash_pin(pin: str) -> str:
    return hashlib.sha256((pin or "").encode()).hexdigest()


def _normalize_strategy_direction(raw) -> str:
    d = (raw or "both").strip().lower()
    if d in ("reversal", "trend"):
        return "both"
    if d in ("long", "short", "both"):
        return d
    return "both"


def _ensure_draft_live_tables(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS candlelab_strategies_live (
            id SERIAL PRIMARY KEY,
            username VARCHAR(100) NOT NULL,
            strategy_name VARCHAR(200),
            instrument VARCHAR(20),
            interval VARCHAR(10),
            anchor VARCHAR(100),
            complement VARCHAR(100),
            connector VARCHAR(20),
            direction VARCHAR(10),
            session VARCHAR(20),
            sl_mult DOUBLE PRECISION,
            tp_mult DOUBLE PRECISION,
            timeout INT,
            indicator_filter JSONB,
            go_live_at TIMESTAMPTZ DEFAULT NOW(),
            closed_at TIMESTAMPTZ,
            user_strategies_id INT,
            saved_at TIMESTAMPTZ DEFAULT NOW()
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS candlelab_strategies_draft (
            id SERIAL PRIMARY KEY,
            username VARCHAR(100) NOT NULL,
            strategy_name VARCHAR(200),
            instrument VARCHAR(20),
            interval VARCHAR(10),
            anchor VARCHAR(100),
            complement VARCHAR(100),
            connector VARCHAR(20),
            direction VARCHAR(10),
            session VARCHAR(20),
            sl_mult DOUBLE PRECISION,
            tp_mult DOUBLE PRECISION,
            timeout INT,
            indicator_filter JSONB,
            saved_at TIMESTAMPTZ DEFAULT NOW(),
            edited_from_live_id INT REFERENCES candlelab_strategies_live(id)
        );
        """
    )


def _verify_user_pin(cur, username: str, pin: str) -> bool:
    cur.execute(
        "SELECT pin_hash FROM user_strategies WHERE username=%s LIMIT 1",
        (username,),
    )
    row = cur.fetchone()
    if not row:
        return False
    return row["pin_hash"] == _hash_pin(pin)


def _fetch_user_pin_hash(cur, username: str) -> str | None:
    cur.execute(
        "SELECT pin_hash FROM user_strategies WHERE username=%s LIMIT 1",
        (username,),
    )
    row = cur.fetchone()
    return row["pin_hash"] if row else None


def _row_to_strategy_dict(r: dict) -> dict:
    d = dict(r)
    if d.get("saved_at") and hasattr(d["saved_at"], "isoformat"):
        d["saved_at"] = d["saved_at"].isoformat()
    if d.get("go_live_at") and hasattr(d["go_live_at"], "isoformat"):
        d["go_live_at"] = d["go_live_at"].isoformat()
    if d.get("closed_at") and hasattr(d["closed_at"], "isoformat"):
        d["closed_at"] = d["closed_at"].isoformat()
    ind = d.get("indicator_filter")
    if isinstance(ind, str):
        try:
            d["indicator_filter"] = json.loads(ind)
        except Exception:
            pass
    return d


def lifecycle_save_draft(body: dict) -> tuple[dict, int]:
    """Insert or update candlelab_strategies_draft. Body: strategy fields + username + pin; optional draft_id."""
    username = (body.get("username") or "").strip()
    pin = str(body.get("pin", ""))
    if not username or not pin:
        return {"error": "missing_params"}, 400
    draft_id = body.get("draft_id")
    init_strategy_tables()
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if not _verify_user_pin(cur, username, pin):
            return {"error": "auth_failed"}, 401
        strategy_name = body.get("strategy_name") or _auto_name()
        instrument = body.get("instrument", "EUR/USD")
        interval = body.get("interval", "5m")
        anchor = body.get("anchor") or ""
        complement = body.get("complement")
        connector = body.get("connector") or "ordered"
        direction = _normalize_strategy_direction(body.get("direction", "both"))
        session = body.get("session") or "All"
        sl_mult = float(body.get("sl_mult", 1.0))
        tp_mult = float(body.get("tp_mult", 3.0))
        timeout = int(body.get("timeout", 1000))
        ind_raw = body.get("indicator_filter")
        ind_val = Json(ind_raw) if ind_raw is not None else None
        edited_from = body.get("edited_from_live_id")
        if draft_id is not None:
            cur.execute(
                """
                UPDATE candlelab_strategies_draft SET
                    strategy_name=%s, instrument=%s, interval=%s, anchor=%s,
                    complement=%s, connector=%s, direction=%s, session=%s,
                    sl_mult=%s, tp_mult=%s, timeout=%s, indicator_filter=%s,
                    saved_at=NOW(), edited_from_live_id=COALESCE(%s, edited_from_live_id)
                WHERE id=%s AND username=%s
                RETURNING id
                """,
                (
                    strategy_name,
                    instrument,
                    interval,
                    anchor,
                    complement,
                    connector,
                    direction,
                    session,
                    sl_mult,
                    tp_mult,
                    timeout,
                    ind_val,
                    edited_from,
                    int(draft_id),
                    username,
                ),
            )
            row = cur.fetchone()
            conn.commit()
            if not row:
                return {"error": "not_found"}, 404
            return {"draft_id": row["id"]}, 200
        cur.execute(
            """
            INSERT INTO candlelab_strategies_draft (
                username, strategy_name, instrument, interval, anchor, complement,
                connector, direction, session, sl_mult, tp_mult, timeout,
                indicator_filter, edited_from_live_id
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id
            """,
            (
                username,
                strategy_name,
                instrument,
                interval,
                anchor,
                complement,
                connector,
                direction,
                session,
                sl_mult,
                tp_mult,
                timeout,
                ind_val,
                edited_from,
            ),
        )
        row = cur.fetchone()
        conn.commit()
        return {"draft_id": row["id"]}, 200


def lifecycle_promote_live(body: dict) -> tuple[dict, int]:
    draft_id = body.get("draft_id")
    username = (body.get("username") or "").strip()
    pin = str(body.get("pin", ""))
    if draft_id is None or not username or not pin:
        return {"error": "missing_params"}, 400
    init_strategy_tables()
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if not _verify_user_pin(cur, username, pin):
            return {"error": "auth_failed"}, 401
        cur.execute(
            """
            SELECT * FROM candlelab_strategies_draft
            WHERE id=%s AND username=%s
            """,
            (int(draft_id), username),
        )
        dr = cur.fetchone()
        if not dr:
            return {"error": "draft_not_found"}, 404
        dr = dict(dr)
        pin_hash = _fetch_user_pin_hash(cur, username)
        if not pin_hash:
            return {"error": "user_not_found"}, 404
        direction = _normalize_strategy_direction(dr.get("direction", "both"))
        ind = dr.get("indicator_filter")
        ind_us = (
            json.dumps(ind)
            if ind is not None and isinstance(ind, (dict, list))
            else ind
        )
        cur.execute(
            """
            INSERT INTO candlelab_strategies_live (
                username, strategy_name, instrument, interval, anchor, complement,
                connector, direction, session, sl_mult, tp_mult, timeout, indicator_filter
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id
            """,
            (
                username,
                dr.get("strategy_name"),
                dr.get("instrument"),
                dr.get("interval"),
                dr.get("anchor"),
                dr.get("complement"),
                dr.get("connector") or "ordered",
                direction,
                dr.get("session") or "All",
                float(dr.get("sl_mult") or 1.0),
                float(dr.get("tp_mult") or 3.0),
                int(dr.get("timeout") or 1000),
                Json(ind) if ind is not None else None,
            ),
        )
        live_row = cur.fetchone()
        live_id = live_row["id"]
        cur.execute(
            """
            INSERT INTO user_strategies (
                username, pin_hash, instrument, interval, anchor,
                complement, connector, session, sl_mult, tp_mult,
                timeout, direction, strategy_name, indicator_filter, active
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, TRUE)
            RETURNING id
            """,
            (
                username,
                pin_hash,
                dr.get("instrument"),
                dr.get("interval"),
                dr.get("anchor"),
                dr.get("complement"),
                dr.get("connector") or "ordered",
                dr.get("session") or "All",
                float(dr.get("sl_mult") or 1.0),
                float(dr.get("tp_mult") or 3.0),
                int(dr.get("timeout") or 1000),
                direction,
                dr.get("strategy_name"),
                ind_us,
            ),
        )
        us_row = cur.fetchone()
        us_id = us_row["id"]
        cur.execute(
            """
            UPDATE candlelab_strategies_live
            SET user_strategies_id=%s WHERE id=%s
            """,
            (us_id, live_id),
        )
        cur.execute(
            "DELETE FROM candlelab_strategies_draft WHERE id=%s",
            (int(draft_id),),
        )
        conn.commit()
    return {"live_id": live_id}, 200


def lifecycle_close_live(body: dict) -> tuple[dict, int]:
    live_id = body.get("live_id")
    username = (body.get("username") or "").strip()
    pin = str(body.get("pin", ""))
    if live_id is None or not username or not pin:
        return {"error": "missing_params"}, 400
    init_strategy_tables()
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if not _verify_user_pin(cur, username, pin):
            return {"error": "auth_failed"}, 401
        cur.execute(
            """
            SELECT id, user_strategies_id FROM candlelab_strategies_live
            WHERE id=%s AND username=%s AND closed_at IS NULL
            """,
            (int(live_id), username),
        )
        lr = cur.fetchone()
        if not lr:
            return {"error": "not_found"}, 404
        us_id = lr.get("user_strategies_id")
        cur.execute(
            """
            UPDATE candlelab_strategies_live
            SET closed_at=NOW() WHERE id=%s
            """,
            (int(live_id),),
        )
        if us_id:
            cur.execute(
                "UPDATE user_strategies SET active=FALSE WHERE id=%s",
                (us_id,),
            )
        conn.commit()
    return {"success": True}, 200


def lifecycle_delete_drafts(body: dict) -> tuple[dict, int]:
    username = (body.get("username") or "").strip()
    pin = str(body.get("pin", ""))
    ids = body.get("draft_ids") or []
    if not username or not pin or not isinstance(ids, list) or not ids:
        return {"error": "missing_params"}, 400
    init_strategy_tables()
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if not _verify_user_pin(cur, username, pin):
            return {"error": "auth_failed"}, 401
        cur2 = conn.cursor()
        cur2.execute(
            """
            DELETE FROM candlelab_strategies_draft
            WHERE username=%s AND id = ANY(%s)
            """,
            (username, list(map(int, ids))),
        )
        n = cur2.rowcount
        conn.commit()
    return {"deleted": n}, 200


def lifecycle_list_my_strategies(username: str, pin: str) -> tuple[dict, int]:
    username = (username or "").strip()
    pin = str(pin or "")
    if not username or not pin:
        return {"error": "missing_params"}, 400
    init_strategy_tables()
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if not _verify_user_pin(cur, username, pin):
            return {"error": "auth_failed"}, 401
        cur.execute(
            """
            SELECT id, username, strategy_name, instrument, interval, anchor, complement,
                   connector, direction, session, sl_mult, tp_mult, timeout, indicator_filter,
                   saved_at, edited_from_live_id
            FROM candlelab_strategies_draft
            WHERE username=%s
            ORDER BY saved_at DESC
            """,
            (username,),
        )
        drafts = [_row_to_strategy_dict(dict(r)) for r in cur.fetchall()]
        cur.execute(
            """
            SELECT id, username, strategy_name, instrument, interval, anchor, complement,
                   connector, direction, session, sl_mult, tp_mult, timeout, indicator_filter,
                   go_live_at, closed_at, user_strategies_id, saved_at
            FROM candlelab_strategies_live
            WHERE username=%s AND closed_at IS NULL
            ORDER BY go_live_at DESC
            """,
            (username,),
        )
        live = [_row_to_strategy_dict(dict(r)) for r in cur.fetchall()]
    return {"drafts": drafts, "live": live}, 200


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
        _ensure_draft_live_tables(cur)
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
