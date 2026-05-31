"""
shared_config.py — Canonical pair universe for CandleLab sweep engine
and Market State Daemon. Single source of truth.
Both sweep_validation_engine.py and market_state_daemon.py import
PAIR_CONFIG from here — never define it independently.
"""

PAIR_CONFIG: dict = {
    "AUD_USD": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "NZD_USD": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "USD_CHF": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "USD_JPY": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "USD_CAD": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "EUR_USD": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "GBP_USD": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "AUD_JPY": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "CAD_JPY": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "EUR_JPY": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "GBP_JPY": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "NZD_JPY": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "CHF_JPY": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "EUR_AUD": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "EUR_CAD": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "EUR_CHF": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "EUR_GBP": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "EUR_NZD": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "GBP_AUD": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "GBP_CAD": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "GBP_CHF": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "GBP_NZD": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "AUD_CAD": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "AUD_NZD": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "NZD_CAD": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
    "CAD_CHF": {"ma_pairs": [(10, 50)], "timeouts": [20, 40, 60, 96], "directions": ["long", "short"], "sl_mode": "standard", "enabled": True},
}

# Sorted alphabetically — canonical order for incidence matrix construction
ALL_PAIRS: list[str] = sorted(PAIR_CONFIG.keys())
