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

    max_window        = max(promoted) if promoted else n_windows - 1
    recency_threshold = max_window - (RECENCY_COUNT - 1)

    cells             = []
    recency_start_idx = n_windows - RECENCY_COUNT
    recency_x         = recency_start_idx * (CELL_W + GAP)
    recency_bg_w      = RECENCY_COUNT * (CELL_W + GAP) - GAP
    cells.append(
        f'<rect x="{recency_x}" y="0" '
        f'width="{recency_bg_w + 2}" height="{CELL_H}" '
        f'rx="2" ry="2" fill="{COLOR_RECENCY_BG}" />'
    )

    for i in range(n_windows):
        x           = i * (CELL_W + GAP)
        is_promoted = i in promoted
        is_recent   = i >= recency_threshold
        if is_promoted and is_recent:
            fill = COLOR_PROMOTED_RECENT
        elif is_promoted:
            fill = COLOR_PROMOTED
        else:
            fill = COLOR_EMPTY
        cells.append(
            f'<rect x="{x}" y="0" width="{CELL_W}" '
            f'height="{CELL_H}" rx="{RADIUS}" ry="{RADIUS}" '
            f'fill="{fill}" />'
        )

    svg_body = "\n  ".join(cells)
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" '
        f'width="{TOTAL_W}" height="{TOTAL_H}" '
        f'viewBox="0 0 {TOTAL_W} {TOTAL_H}">\n  '
        f'{svg_body}\n</svg>'
    )
