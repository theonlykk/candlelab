# ==============================================================================
# ⚠️  CROSS-REPO SYNCHRONIZATION REQUIRED  ⚠️
# This file (indicator_utils.py) exists in BOTH oanda-trading AND candlelab.
# If you modify logic here, you MUST manually copy the changes to the other repo.
# Do not add repo-specific dependencies to this file.
# Last synced: 2026-04-27
# ==============================================================================

import json
import logging
import pandas as pd

log = logging.getLogger(__name__)

from indicators import check_ma_cross_direction, check_rsi_extreme, check_ma_stable


def _parse_indicator_filter_config(raw) -> dict | None:
    """
    Normalize candlelab_strategies_live.indicator_filter to a dict.
    Plain strings 'rsi' / 'ma_cross' use defaults; JSON objects use stored thresholds.
    """
    if raw is None or raw == "":
        return None
    if isinstance(raw, dict):
        d = dict(raw)
    else:
        s = str(raw).strip()
        if s.startswith("{"):
            try:
                d = json.loads(s)
            except json.JSONDecodeError:
                return None
        else:
            typ = s.lower().replace(" ", "_")
            if typ == "rsi":
                return {"type": "rsi", "oversold": 30.0, "overbought": 70.0}
            if typ == "ma_cross":
                return {"type": "ma_cross", "direction": None}
            if typ == "ma_stable":
                return {"type": "ma_stable"}
            return None
    t = str(d.get("type", "")).strip().lower().replace(" ", "_")
    if not t:
        return None
    out: dict = {"type": t}
    if t == "rsi":
        out["oversold"] = float(d.get("oversold", 30))
        out["overbought"] = float(d.get("overbought", 70))
    elif t == "ma_cross":
        dr = d.get("direction")
        out["direction"] = str(dr).strip().lower() if dr is not None else None
    return out


def passes_indicator(
    ind_cfg: dict | None, df: pd.DataFrame, sig_idx: int, dir_str: str
) -> bool:
    if not ind_cfg:
        return True
    it = str(ind_cfg.get("type", "")).lower()
    if it == "ma_cross":
        cross_dir = ind_cfg.get("direction")
        if cross_dir is None:
            cross_dir = "bullish" if dir_str.lower() in ("long", "buy") else "bearish"
        else:
            cross_dir = str(cross_dir).strip().lower()
        return check_ma_cross_direction(df, sig_idx, cross_dir)
    if it == "rsi":
        return check_rsi_extreme(
            df,
            sig_idx,
            dir_str,
            float(ind_cfg.get("oversold", 30)),
            float(ind_cfg.get("overbought", 70)),
        )
    if it == "ma_stable":
        return check_ma_stable(df, sig_idx, dir_str)
    log.debug("Unknown indicator_filter %r — treating as pass", ind_cfg)
    return True
