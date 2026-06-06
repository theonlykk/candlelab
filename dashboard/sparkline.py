"""
sparkline.py — 27-cell barcode sparkline for promoted_window_indices.
Gemini ruling ADR-104 dashboard: barcode for pre-attentive visual processing.
"""
import json

N_WINDOWS     = 27
RECENCY_COUNT = 5
CELL_W        = 6
CELL_H        = 14
GAP           = 1
RADIUS        = 1
TOTAL_W       = N_WINDOWS * (CELL_W + GAP) - GAP
TOTAL_H       = CELL_H

COLOR_PROMOTED        = "#212121"
COLOR_EMPTY           = "#e0e0e0"
COLOR_RECENCY_BG      = "#e3f2fd"
COLOR_PROMOTED_RECENT = "#4caf50"


def build_sparkline(
    promoted_window_indices, n_windows: int = N_WINDOWS
) -> str:
    if promoted_window_indices is None:
        promoted = set()
    elif isinstance(promoted_window_indices, str):
        try:
            promoted = set(json.loads(promoted_window_indices))
        except Exception:
            promoted = set()
    elif isinstance(promoted_window_indices, (list, tuple)):
        promoted = set(int(w) for w in promoted_window_indices)
    else:
        promoted = set()

    recency_start = n_windows - RECENCY_COUNT
    chars = []
    for i in range(n_windows):
        if i in promoted:
            chars.append("▓" if i < recency_start else "█")
        else:
            chars.append("░" if i < recency_start else "▒")
    return "".join(chars)
