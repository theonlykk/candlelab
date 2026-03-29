"""
strategy_store.py — SQLite persistence for user strategies
"""
import json
import uuid
import random
from datetime import datetime, timezone
from data import get_conn

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
    return f"{random.choice(VERBS)} {random.choice(COLOURS)} {random.choice(ANIMALS)}"


def init_strategy_tables():
    with get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS user_strategies (
                id           TEXT PRIMARY KEY,
                name         TEXT,
                patterns     TEXT,
                direction    TEXT,
                window       INT,
                instrument   TEXT,
                interval     TEXT,
                created_ts   TEXT,
                live_from_ts TEXT,
                active       INT DEFAULT 1,
                bt_win_pct   REAL,
                bt_cum_net   REAL,
                bt_cum_gross REAL,
                bt_signals   INT,
                bt_tp_hits   INT
            )
        """)
        # Migration: add bt_ columns to existing tables if missing
        existing = {r[1] for r in conn.execute(
            "PRAGMA table_info(user_strategies)"
        ).fetchall()}
        for col, typedef in [("bt_win_pct","REAL"), ("bt_cum_net","REAL"),
                              ("bt_cum_gross","REAL"), ("bt_signals","INT"),
                              ("bt_tp_hits","INT"), ("connectors","TEXT")]:
            if col not in existing:
                conn.execute(f"ALTER TABLE user_strategies ADD COLUMN {col} {typedef}")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS strategy_trades (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy_id  TEXT,
                ts           TEXT,
                entry        REAL,
                tp           REAL,
                sl           REAL,
                outcome      TEXT,
                pnl_gross    REAL,
                pnl_net      REAL,
                UNIQUE(strategy_id, ts)
            )
        """)
        conn.commit()


def save_strategy(config: dict) -> dict:
    init_strategy_tables()
    sid  = str(uuid.uuid4())
    name = _auto_name()
    now  = datetime.now(timezone.utc).isoformat()
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO user_strategies
              (id, name, patterns, connectors, direction, window, instrument, interval,
               created_ts, live_from_ts, active,
               bt_win_pct, bt_cum_net, bt_cum_gross, bt_signals, bt_tp_hits)
            VALUES (?,?,?,?,?,?,?,?,?,?,1,?,?,?,?,?)
        """, (
            sid, name,
            json.dumps(config["patterns"]),
            json.dumps(config.get("connectors", ["ordered", "ordered"])),
            str(config["direction"]),
            int(config.get("window", 5)),
            config.get("instrument", "EUR/USD"),
            config.get("interval", "5m"),
            now, now,
            config.get("bt_win_pct"),
            config.get("bt_cum_net"),
            config.get("bt_cum_gross"),
            config.get("bt_signals"),
            config.get("bt_tp_hits"),
        ))
        conn.commit()
    return {"id": sid, "name": name}


def load_strategies() -> list:
    init_strategy_tables()
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM user_strategies WHERE active=1 ORDER BY created_ts DESC"
        ).fetchall()
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
    init_strategy_tables()
    with get_conn() as conn:
        conn.execute("UPDATE user_strategies SET active=0 WHERE id=?", (sid,))
        conn.commit()


def log_strategy_trade(strategy_id: str, trade: dict):
    init_strategy_tables()
    ts = trade["ts"].isoformat() if hasattr(trade["ts"], "isoformat") else str(trade["ts"])
    with get_conn() as conn:
        conn.execute("""
            INSERT OR IGNORE INTO strategy_trades
              (strategy_id, ts, entry, tp, sl, outcome, pnl_gross, pnl_net)
            VALUES (?,?,?,?,?,?,?,?)
        """, (
            strategy_id, ts,
            float(trade["entry"]), float(trade["tp"]), float(trade["sl"]),
            trade["outcome"], float(trade["pnl_gross"]), float(trade["pnl_net"]),
        ))
        conn.commit()


def load_strategy_trades(strategy_id: str) -> list:
    init_strategy_tables()
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM strategy_trades WHERE strategy_id=? ORDER BY ts DESC",
            (strategy_id,)
        ).fetchall()
    return [dict(r) for r in rows]
