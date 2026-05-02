"""
File-based poll log for the CandleLab 5-minute scheduler cycle.
Each instrument gets one JSON line per cycle in /tmp/logs/candlelab_poll.log.
Rows are also stored in PostgreSQL table ``candlelab_poll_log`` when DATABASE_URL is set.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import Json, RealDictCursor

from candlelab_core.indicators import _rsi, _sma, check_ma_cross, check_ma_stable, check_rsi_extreme

log = logging.getLogger(__name__)


def init_candlelab_poll_log_table() -> None:
    """Create ``candlelab_poll_log`` and index if they do not exist."""
    url = os.environ.get("DATABASE_URL")
    if not url:
        log.warning("poll_log: DATABASE_URL not set, skip candlelab_poll_log init")
        return
    try:
        conn = psycopg2.connect(url)
    except Exception:
        log.exception("poll_log: init_candlelab_poll_log_table connect failed")
        return
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS candlelab_poll_log (
                    id SERIAL PRIMARY KEY,
                    ts TIMESTAMPTZ NOT NULL,
                    instrument VARCHAR NOT NULL,
                    candle_time TIMESTAMPTZ,
                    session VARCHAR,
                    hour_utc INT,
                    spread_pips NUMERIC(8,4),
                    open NUMERIC(12,5),
                    high NUMERIC(12,5),
                    low NUMERIC(12,5),
                    close NUMERIC(12,5),
                    buffer_len INT,
                    patterns_detected JSONB,
                    candle_history JSONB,
                    live_strategies_checked JSONB
                );
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS candlelab_poll_log_instrument_ts_idx
                ON candlelab_poll_log (instrument, ts DESC);
                """
            )
        conn.commit()
    except Exception:
        log.exception("poll_log: init_candlelab_poll_log_table failed")
        conn.rollback()
    finally:
        conn.close()


def _parse_ts_iso_z(s: str) -> datetime:
    s = (s or "").strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _session_from_hour_utc(hour_utc: int) -> str:
    if 0 <= hour_utc <= 6:
        return "asian"
    if 7 <= hour_utc <= 11:
        return "london"
    if 12 <= hour_utc <= 16:
        return "new_york"
    return "asian"


def _insert_poll_log_row(rec: dict) -> None:
    url = os.environ.get("DATABASE_URL")
    if not url:
        return
    try:
        ts = _parse_ts_iso_z(str(rec.get("ts") or ""))
    except Exception:
        log.warning("poll_log: _insert_poll_log_row bad ts %r", rec.get("ts"))
        return

    hour_utc = ts.hour
    session = _session_from_hour_utc(hour_utc)
    ohlc = rec.get("ohlc") or None
    spread_pips = None
    o_open = o_high = o_low = o_close = None
    if isinstance(ohlc, dict) and ohlc:
        try:
            h = float(ohlc["h"])
            l = float(ohlc["l"])
            _sp = round((h - l) * 10000, 4)
            # FX-style ranges only: *10000 pip scaling is meaningless for metals/indices/crypto.
            spread_pips = _sp if _sp < 100 else None
            o_open = float(ohlc["o"])
            o_high = float(ohlc["h"])
            o_low = float(ohlc["l"])
            o_close = float(ohlc["c"])
        except (KeyError, TypeError, ValueError):
            spread_pips = None
            o_open = o_high = o_low = o_close = None

    candle_time = rec.get("candle_time")
    ct_val = None
    if candle_time:
        try:
            ct_val = _parse_ts_iso_z(str(candle_time))
        except Exception:
            ct_val = None

    ch = rec.get("candle_history")
    buffer_len = len(ch) if isinstance(ch, list) else None

    pat = rec.get("patterns_detected")
    live = rec.get("live_strategies_checked")
    inst = str(rec.get("instrument") or "")

    try:
        conn = psycopg2.connect(url)
    except Exception:
        log.exception("poll_log: _insert_poll_log_row connect failed")
        return
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO candlelab_poll_log (
                    ts, instrument, candle_time, session, hour_utc, spread_pips,
                    open, high, low, close, buffer_len,
                    patterns_detected, candle_history, live_strategies_checked
                ) VALUES (
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s
                )
                """,
                (
                    ts,
                    inst,
                    ct_val,
                    session,
                    hour_utc,
                    spread_pips,
                    o_open,
                    o_high,
                    o_low,
                    o_close,
                    buffer_len,
                    Json(pat if pat is not None else []),
                    Json(ch if ch is not None else []),
                    Json(live if live is not None else []),
                ),
            )
        conn.commit()
    except Exception:
        log.exception("poll_log: _insert_poll_log_row insert failed")
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        conn.close()


def _poll_log_val_jsonable(v: Any) -> Any:
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, datetime):
        return v.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    if isinstance(v, (list, dict)):
        return v
    if isinstance(v, memoryview):
        return v.tobytes().decode("utf-8", errors="replace")
    return v


def _poll_log_row_to_jsonable(row: dict) -> dict:
    out: dict[str, Any] = {}
    for k, v in row.items():
        if v is None:
            out[k] = None
        else:
            out[k] = _poll_log_val_jsonable(v)
    return out


def read_poll_log_pg(instrument: str, n: int) -> list:
    want = (instrument or "").strip()
    if not want or n <= 0:
        return []
    url = os.environ.get("DATABASE_URL")
    if not url:
        return []
    try:
        conn = psycopg2.connect(url)
    except Exception:
        log.exception("poll_log: read_poll_log_pg connect failed")
        return []
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT * FROM candlelab_poll_log
                WHERE instrument = %s
                ORDER BY ts DESC
                LIMIT %s
                """,
                (want, n),
            )
            rows = cur.fetchall()
    except Exception:
        log.exception("poll_log: read_poll_log_pg query failed")
        return []
    finally:
        conn.close()
    return [_poll_log_row_to_jsonable(dict(r)) for r in rows]


def read_poll_log_view_rows(limit: int = 200, instrument: str | None = None) -> list[dict]:
    """Last ``limit`` rows for HTML view (newest first). Optional ``instrument`` filter."""
    if limit <= 0:
        return []
    url = os.environ.get("DATABASE_URL")
    if not url:
        return []
    inst = (instrument or "").strip()
    try:
        conn = psycopg2.connect(url)
    except Exception:
        log.exception("poll_log: read_poll_log_view_rows connect failed")
        return []
    cols = (
        "ts, instrument, session, hour_utc, spread_pips, "
        "patterns_detected, live_strategies_checked"
    )
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if inst:
                cur.execute(
                    f"""
                    SELECT {cols}
                    FROM candlelab_poll_log
                    WHERE instrument = %s
                    ORDER BY ts DESC
                    LIMIT %s
                    """,
                    (inst, limit),
                )
            else:
                cur.execute(
                    f"""
                    SELECT {cols}
                    FROM candlelab_poll_log
                    ORDER BY ts DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
            rows = cur.fetchall()
    except Exception:
        log.exception("poll_log: read_poll_log_view_rows query failed")
        return []
    finally:
        conn.close()
    return [_poll_log_row_to_jsonable(dict(r)) for r in rows]


LOG_DIR = Path("/tmp/logs")
LOG_FILE = LOG_DIR / "candlelab_poll.log"
MAX_LINES = 500


def _pattern_slug(label: str) -> str:
    """Stable slug for comparing DB pattern labels to detect_all column names (executor parity)."""
    raw = str(label or "").strip()
    if not raw:
        return ""
    s = (
        raw.lower()
        .replace(".", "")
        .replace("/", "_")
        .replace(" ", "_")
        .replace("-", "_")
    )
    while "__" in s:
        s = s.replace("__", "_")
    return s.strip("_")


def _resolve_pattern_key(anchor: str | None) -> str | None:
    """Map anchor/complement string to the matching ``detect_all`` column name via slug equality."""
    if not anchor or not str(anchor).strip():
        return None
    from candlelab_core.patterns import PATTERNS

    key = _pattern_slug(str(anchor).strip())
    if not key:
        return None
    for col in PATTERNS:
        if _pattern_slug(str(col)) == key:
            return col
    return None


def _last_bar_patterns(sig_df) -> list[str]:
    if sig_df is None or len(sig_df) == 0:
        return []
    last = sig_df.iloc[-1]
    out: list[str] = []
    for col in sig_df.columns:
        try:
            v = int(last[col])
        except (TypeError, ValueError):
            continue
        if v != 0:
            out.append(_pattern_slug(str(col)))
    return sorted(set(out))


def _candle_time_iso_z(idx) -> str:
    ts = pd.Timestamp(idx)
    if ts.tzinfo is None:
        ct = ts.tz_localize("UTC")
    else:
        ct = ts.tz_convert("UTC")
    return ct.strftime("%Y-%m-%dT%H:%M:%SZ")


def _null_poll_indicators() -> dict:
    return {"rsi": None, "ma_fast": None, "ma_slow": None, "ma_cross": None}


def _poll_log_indicators_at_row(df: pd.DataFrame, L: int) -> dict:
    """RSI(14), SMA5/SMA20, fast/slow label — same semantics as oanda-trading executor poll log."""
    out = _null_poll_indicators()
    try:
        if df is None or df.empty or L < 0:
            return out
        close = df["close"].to_numpy(dtype=float)
        if L >= len(close):
            return out
        rsi_arr = _rsi(close, 14)
        sma5 = _sma(close, 5)
        sma20 = _sma(close, 20)
        rv = float(rsi_arr[L]) if L < len(rsi_arr) else float("nan")
        mf = float(sma5[L]) if L < len(sma5) else float("nan")
        ms = float(sma20[L]) if L < len(sma20) else float("nan")
        if not np.isnan(rv):
            out["rsi"] = round(rv, 1)
        if not np.isnan(mf):
            out["ma_fast"] = round(mf, 4)
        if not np.isnan(ms):
            out["ma_slow"] = round(ms, 4)
        if not np.isnan(mf) and not np.isnan(ms):
            if mf > ms:
                out["ma_cross"] = "bullish"
            elif mf < ms:
                out["ma_cross"] = "bearish"
            else:
                out["ma_cross"] = "neutral"
    except Exception:
        pass
    return out


def _parse_indicator_filter(raw) -> str | None:
    if raw is None or raw == "":
        return None
    if isinstance(raw, dict):
        t = raw.get("type")
        return str(t).strip().lower() if t else None
    s = str(raw).strip()
    if s.startswith("{"):
        try:
            d = json.loads(s)
            t = d.get("type")
            return str(t).strip().lower() if t else None
        except json.JSONDecodeError:
            return None
    return s.lower().replace(" ", "_")


def _poll_indicator_filter_detail(ind_type: str, df: pd.DataFrame, L: int, dir_str: str) -> dict:
    it = ind_type.lower()
    close = df["close"].to_numpy(dtype=float)
    rsi_arr = _rsi(close, 14)
    rsi_at = (
        round(float(rsi_arr[L]), 1)
        if L < len(rsi_arr) and not np.isnan(rsi_arr[L])
        else None
    )
    sma5 = _sma(close, 5)
    sma20 = _sma(close, 20)
    mf = float(sma5[L]) if L < len(sma5) and not np.isnan(sma5[L]) else None
    ms = float(sma20[L]) if L < len(sma20) and not np.isnan(sma20[L]) else None
    mf_r = round(mf, 4) if mf is not None else None
    ms_r = round(ms, 4) if ms is not None else None

    if it == "rsi":
        return {
            "rsi": rsi_at,
            "filter": "rsi",
            "required": "< 30" if dir_str == "long" else "> 70",
            "passed": check_rsi_extreme(df, L, dir_str),
        }
    if it == "ma_cross":
        return {
            "ma_fast": mf_r,
            "ma_slow": ms_r,
            "filter": "ma_cross",
            "required": "5 SMA cross above 20 SMA (10-bar lookback)",
            "passed": check_ma_cross(df, L),
        }
    if it == "ma_stable":
        return {
            "ma_fast": mf_r,
            "ma_slow": ms_r,
            "filter": "ma_stable",
            "required": "5 & 20 SMA slope with trade direction (10-bar lookback)",
            "passed": check_ma_stable(df, L, dir_str),
        }
    return {"filter": ind_type, "required": None, "passed": True}


def _patterns_at_row(sig_df, row_index: int) -> list[str]:
    if sig_df is None or len(sig_df) == 0 or row_index < 0 or row_index >= len(sig_df):
        return []
    row = sig_df.iloc[row_index]
    out: list[str] = []
    for col in sig_df.columns:
        try:
            v = int(row[col])
        except (TypeError, ValueError):
            continue
        if v != 0:
            out.append(_pattern_slug(str(col)))
    return sorted(set(out))


def _build_candle_history(df, sig_df, dec: int, n_bars: int = 25) -> list[dict]:
    """Last ``n_bars`` rows oldest-first; ``patterns`` from ``detect_all`` at each row (logging only)."""
    if df is None or df.empty:
        return []
    n = min(n_bars, len(df))
    start = len(df) - n
    hist: list[dict] = []
    for pos in range(start, len(df)):
        idx = df.index[pos]
        r = df.iloc[pos]
        sub = df.iloc[: pos + 1]
        Lsub = len(sub) - 1
        hist.append({
            "candle_time": _candle_time_iso_z(idx),
            "ohlc": {
                "o": round(float(r["open"]), dec),
                "h": round(float(r["high"]), dec),
                "l": round(float(r["low"]), dec),
                "c": round(float(r["close"]), dec),
            },
            "patterns": _patterns_at_row(sig_df, pos) if sig_df is not None else [],
            "indicators": _poll_log_indicators_at_row(sub, Lsub),
        })
    return hist


def _strategy_poll_row(
    row: dict,
    sig_df,
    df: pd.DataFrame | None,
) -> dict:
    anchor_key = _resolve_pattern_key(row.get("anchor"))
    comp_key = _resolve_pattern_key(row.get("complement")) if row.get("complement") else None
    has_comp = bool(comp_key)

    sig_anchor = 0
    if anchor_key and anchor_key in sig_df.columns and len(sig_df):
        try:
            sig_anchor = int(sig_df[anchor_key].iloc[-1])
        except (TypeError, ValueError):
            sig_anchor = 0

    sig_comp = 0
    if comp_key and comp_key in sig_df.columns and len(sig_df):
        try:
            sig_comp = int(sig_df[comp_key].iloc[-1])
        except (TypeError, ValueError):
            sig_comp = 0

    anchor_fired = anchor_key is not None and sig_anchor != 0
    complement_fired = bool(comp_key) and sig_comp != 0

    direction = (row.get("direction") or "both").strip().lower()
    dir_ok = direction in ("both", "reversal", "trend", "")
    if direction == "long":
        dir_ok = sig_anchor > 0
    elif direction == "short":
        dir_ok = sig_anchor < 0

    signal = anchor_fired and (not has_comp or complement_fired) and dir_ok

    ind_detail = None
    if (
        df is not None
        and not df.empty
        and sig_df is not None
        and len(sig_df) > 0
    ):
        ind_type = _parse_indicator_filter(row.get("indicator_filter"))
        if ind_type:
            L = len(df) - 1
            dir_str = "long" if sig_anchor > 0 else "short" if sig_anchor < 0 else "long"
            ind_detail = _poll_indicator_filter_detail(ind_type, df, L, dir_str)

    return {
        "id": int(row["id"]) if row.get("id") is not None else None,
        "name": row.get("strategy_name") or "",
        "anchor": row.get("anchor") or "",
        "anchor_fired": bool(anchor_fired),
        "complement_fired": bool(complement_fired),
        "signal": bool(signal),
        "indicator_detail": ind_detail,
    }


def _build_record_for_instrument(inst_key: str) -> dict:
    from data import INSTRUMENTS, get_ohlc, _oanda_instrument_id
    from candlelab_core.patterns import detect_all
    from strategy_store import list_open_live_for_instrument

    now = datetime.now(timezone.utc)
    ts_iso = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    oanda_id = _oanda_instrument_id(inst_key)

    candle_time: str | None = None
    ohlc: dict | None = None
    patterns_detected: list[str] = []
    candle_history: list[dict] = []
    sig_df = None

    try:
        df = get_ohlc(inst_key, days=14, interval="5m", record_candle_diagnostics=False)
    except Exception:
        log.exception("poll_log: get_ohlc failed for %s", inst_key)
        df = None

    if df is not None and not df.empty:
        last = df.iloc[-1]
        idx = df.index[-1]
        ts = pd.Timestamp(idx)
        if ts.tzinfo is None:
            ct = ts.tz_localize("UTC")
        else:
            ct = ts.tz_convert("UTC")
        candle_time = ct.strftime("%Y-%m-%dT%H:%M:%SZ")
        meta = INSTRUMENTS[inst_key]
        dec = int(meta["decimals"])
        ohlc = {
            "o": round(float(last["open"]), dec),
            "h": round(float(last["high"]), dec),
            "l": round(float(last["low"]), dec),
            "c": round(float(last["close"]), dec),
        }
        try:
            sig_df = detect_all(df)
            patterns_detected = _last_bar_patterns(sig_df)
        except Exception:
            log.exception("poll_log: detect_all failed for %s", inst_key)
            sig_df = None
        candle_history = _build_candle_history(df, sig_df, dec, n_bars=25)

    live_rows: list[dict] = []
    try:
        raw_live = list_open_live_for_instrument(inst_key)
    except Exception:
        log.exception("poll_log: list_open_live_for_instrument failed for %s", inst_key)
        raw_live = []

    if sig_df is None or sig_df.empty:
        sig_df = None

    for row in raw_live:
        if sig_df is not None:
            live_rows.append(_strategy_poll_row(row, sig_df, df))
        else:
            live_rows.append({
                "id": int(row["id"]) if row.get("id") is not None else None,
                "name": row.get("strategy_name") or "",
                "anchor": row.get("anchor") or "",
                "anchor_fired": False,
                "complement_fired": False,
                "signal": False,
                "indicator_detail": None,
            })

    return {
        "ts": ts_iso,
        "instrument": oanda_id,
        "candle_time": candle_time,
        "ohlc": ohlc,
        "patterns_detected": patterns_detected,
        "candle_history": candle_history,
        "live_strategies_checked": live_rows,
    }


def _trim_log_file() -> None:
    if not LOG_FILE.is_file():
        return
    try:
        text = LOG_FILE.read_text(encoding="utf-8")
    except OSError:
        return
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if len(lines) <= MAX_LINES:
        return
    keep = lines[-MAX_LINES:]
    try:
        LOG_FILE.write_text("\n".join(keep) + "\n", encoding="utf-8")
    except OSError:
        log.exception("poll_log: trim write failed")


def append_cycle_poll_logs() -> None:
    """Append one JSON line per configured instrument; trim to last MAX_LINES."""
    from data import INSTRUMENTS

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    lines_out: list[str] = []
    for inst_key in INSTRUMENTS:
        try:
            rec = _build_record_for_instrument(inst_key)
            _insert_poll_log_row(rec)
            lines_out.append(json.dumps(rec, separators=(",", ":")))
        except Exception:
            log.exception("poll_log: build record failed for %s", inst_key)

    if not lines_out:
        return
    try:
        with LOG_FILE.open("a", encoding="utf-8") as f:
            for ln in lines_out:
                f.write(ln + "\n")
    except OSError:
        log.exception("poll_log: append failed")
        return
    _trim_log_file()


def read_poll_log_entries(instrument: str, n: int, log_path: Path | None = None) -> list:
    """
    Return up to the last n parsed JSON objects whose "instrument" field matches `instrument`.
    ``log_path`` defaults to ``LOG_FILE`` (/tmp/logs/candlelab_poll.log).
    """
    path = log_path or LOG_FILE
    want = (instrument or "").strip()
    if not want or n <= 0:
        return []
    if not path.is_file():
        return []
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError:
        return []
    out: list[dict] = []
    for ln in reversed(raw.splitlines()):
        ln = ln.strip()
        if not ln:
            continue
        try:
            obj = json.loads(ln)
        except json.JSONDecodeError:
            continue
        if obj.get("instrument") == want:
            out.append(obj)
            if len(out) >= n:
                break
    out.reverse()
    return out
