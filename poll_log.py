"""
File-based poll log for the CandleLab 5-minute scheduler cycle.
Each instrument gets one JSON line per cycle in /tmp/logs/candlelab_poll.log.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

LOG_DIR = Path("/tmp/logs")
LOG_FILE = LOG_DIR / "candlelab_poll.log"
MAX_LINES = 10_000


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
    from patterns import PATTERNS

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
        hist.append({
            "candle_time": _candle_time_iso_z(idx),
            "ohlc": {
                "o": round(float(r["open"]), dec),
                "h": round(float(r["high"]), dec),
                "l": round(float(r["low"]), dec),
                "c": round(float(r["close"]), dec),
            },
            "patterns": _patterns_at_row(sig_df, pos) if sig_df is not None else [],
        })
    return hist


def _strategy_poll_row(
    row: dict,
    sig_df,
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

    return {
        "id": int(row["id"]) if row.get("id") is not None else None,
        "name": row.get("strategy_name") or "",
        "anchor": row.get("anchor") or "",
        "anchor_fired": bool(anchor_fired),
        "complement_fired": bool(complement_fired),
        "signal": bool(signal),
    }


def _build_record_for_instrument(inst_key: str) -> dict:
    from data import INSTRUMENTS, get_ohlc, _oanda_instrument_id
    from patterns import detect_all
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
            live_rows.append(_strategy_poll_row(row, sig_df))
        else:
            live_rows.append({
                "id": int(row["id"]) if row.get("id") is not None else None,
                "name": row.get("strategy_name") or "",
                "anchor": row.get("anchor") or "",
                "anchor_fired": False,
                "complement_fired": False,
                "signal": False,
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
