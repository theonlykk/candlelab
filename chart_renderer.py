"""
chart_renderer.py — Server-side matplotlib candlestick chart renderer.
Generates PNG as base64 + extracts pixel candle_map.
No mplfinance, no third-party charting libraries.
"""
import io
import re
import base64
from collections import defaultdict

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.gridspec as gridspec
import matplotlib.transforms as mtransforms
from matplotlib.ticker import FixedLocator, FuncFormatter, MaxNLocator, NullLocator

# Colour palette
C_BULL        = "#26a69a"
C_BEAR        = "#ef5350"
C_BULL_BORDER = "#26a69a"   # bullish pattern border
C_BEAR_BORDER = "#ffa726"   # bearish pattern border (amber)
C_BB          = "#1565c0"
C_EMA         = "#fb8c00"
C_MACD        = "#1565c0"
C_SIGNAL      = "#e53935"
C_HIST_POS    = "#26a69a"
C_HIST_NEG    = "#ef5350"
C_RSI         = "#7b1fa2"
C_EQUITY      = "#1565c0"
C_WIN_FILL    = "#4caf50"   # window overlay fill
C_WIN_BORDER  = "#4caf50"   # window overlay border
C_GRID        = "#eeeeee"
C_TEXT        = "#212121"
C_BG          = "#ffffff"

# Multiple EMA/SMA colours
C_EMA9   = "#ff7043"   # orange-red
C_EMA20  = "#fb8c00"   # amber
C_EMA50  = "#26a69a"   # teal
C_EMA200 = "#7b1fa2"   # purple

# (filter_key, column_name, linestyle, linewidth, colour) — filter_key matches indicators.py / UI
_MA_OVERLAY_SPECS = (
    ("ema9", "ema9", "-", 1.2, C_EMA9),
    ("ema20", "ema20", "-", 1.4, C_EMA20),
    ("ema50", "ema50", "-", 1.4, C_EMA50),
    ("ema200", "ema200", "-", 1.6, C_EMA200),
    ("sma10", "sma10", "--", 1.0, C_EMA9),
    ("sma20", "sma20", "--", 1.0, C_EMA20),
    ("sma50", "sma50", "--", 1.2, C_EMA50),
    ("sma200", "sma200", "--", 1.4, C_EMA200),
)


def _ma_overlay_label(fk: str) -> str:
    """EMA 9 / SMA 200 style label from filter key (e.g. ema50 → EMA 50)."""
    m = re.match(r"^(ema|sma)(\d+)$", fk, re.IGNORECASE)
    if m:
        return f"{m.group(1).upper()} {m.group(2)}"
    return fk.upper()


# Figure dimensions and export settings
FIG_W      = 16
FIG_H      = 9
# Standalone equity figure height (~200px at 96dpi)
FIG_H_EQUITY = 200.0 / 96.0
EXPORT_DPI = 96

# Font sizes — readable on mobile
FS_TICK  = 20    # axis tick labels (y-axis price labels)
FS_LABEL = 11    # axis labels
FS_ANN   = 9     # pattern symbols (min 9pt for readability)
FS_BRKT  = 9     # multi-candle bracket labels
FS_XTICK = 20    # x-axis date ticks


def _bar_ts_utc(window_df: pd.DataFrame, i: int) -> pd.Timestamp:
    """UTC Timestamp for bar i from window_df index (DatetimeIndex or convertible)."""
    ts = window_df.index[i]
    if not isinstance(ts, pd.Timestamp):
        ts = pd.Timestamp(ts)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def _apply_two_level_datetime_xaxis(ax_bottom, window_df: pd.DataFrame) -> None:
    """
    Single bottom x-axis: major ticks = UTC date changes (DD/MM), longer ticks;
    minor ticks = HH:MM every ~5–10 bars, shorter ticks. Bar index = window_df row.
    """
    n = len(window_df)
    if n == 0:
        return

    major_pos: list[int] = []
    major_map: dict[int, str] = {}
    prev_date = None
    for i in range(n):
        d = _bar_ts_utc(window_df, i).date()
        if prev_date is None or d != prev_date:
            major_pos.append(i)
            major_map[i] = _bar_ts_utc(window_df, i).strftime("%d/%m")
            prev_date = d

    time_step = max(1, min(10, max(5, n // 7)))
    minor_pos = list(range(0, n, time_step))
    if minor_pos[-1] != n - 1:
        minor_pos.append(n - 1)
    minor_map = {i: _bar_ts_utc(window_df, i).strftime("%H:%M") for i in minor_pos}

    def _major_fmt(x, _pos):
        xi = int(round(float(x)))
        return major_map.get(xi, "")

    def _minor_fmt(x, _pos):
        xi = int(round(float(x)))
        return minor_map.get(xi, "")

    ax_bottom.xaxis.set_major_locator(FixedLocator(major_pos))
    ax_bottom.xaxis.set_major_formatter(FuncFormatter(_major_fmt))
    ax_bottom.xaxis.set_minor_locator(FixedLocator(minor_pos))
    ax_bottom.xaxis.set_minor_formatter(FuncFormatter(_minor_fmt))

    ax_bottom.tick_params(
        axis="x",
        which="major",
        labelsize=FS_XTICK,
        labelcolor=C_TEXT,
        colors=C_TEXT,
        bottom=True,
        top=False,
        labelbottom=True,
        length=10,
        width=1.0,
        pad=4,
    )
    ax_bottom.tick_params(
        axis="x",
        which="minor",
        labelsize=FS_XTICK,
        labelcolor=C_TEXT,
        colors=C_TEXT,
        bottom=True,
        top=False,
        labelbottom=True,
        length=4,
        width=0.7,
        pad=14,
    )


def _equity_ts_at_index(full_index: pd.Index, i: int) -> pd.Timestamp:
    ts = full_index[i]
    if not isinstance(ts, pd.Timestamp):
        ts = pd.Timestamp(ts)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def _apply_equity_sparse_date_xaxis(ax, n_bars: int, times: list[pd.Timestamp]) -> None:
    """Sparse date-only x-axis (5–8 ticks); DD/MM if span ≤730d else MM/YY."""
    if n_bars < 1 or len(times) != n_bars:
        return
    n_tick = int(np.clip(8 if n_bars > 8 else max(5, min(n_bars, 8)), 5, 8))
    if n_bars <= 1:
        positions = [0]
    else:
        positions = sorted(
            {int(round(v)) for v in np.linspace(0, n_bars - 1, num=min(n_tick, n_bars))}
        )
    t0, t1 = times[0], times[n_bars - 1]
    span_days = max((t1 - t0).total_seconds() / 86400.0, 1e-9)
    fmt = "%m/%y" if span_days > 730 else "%d/%m"
    tick_map = {p: times[p].strftime(fmt) for p in positions}

    def _lab(x, _pos):
        xi = int(round(float(x)))
        return tick_map.get(xi, "")

    ax.xaxis.set_major_locator(FixedLocator(positions))
    ax.xaxis.set_major_formatter(FuncFormatter(_lab))
    ax.xaxis.set_minor_locator(NullLocator())
    ax.tick_params(
        axis="x",
        which="major",
        labelsize=FS_XTICK,
        labelcolor=C_TEXT,
        bottom=True,
        top=False,
        labelbottom=True,
        length=8,
        pad=6,
    )


def _apply_equity_sparse_date_axis_index(ax, full_index: pd.Index, n_bars: int) -> None:
    times = [_equity_ts_at_index(full_index, i) for i in range(n_bars)]
    _apply_equity_sparse_date_xaxis(ax, n_bars, times)


# Multi-candle patterns that span N prior candles
_MULTI_SPAN = {
    "morning_star":         3,
    "evening_star":         3,
    "three_soldiers_crows": 3,
    "piercing_dark_cloud":  2,
    "harami":               2,
    "engulfing":            2,
}


def _draw_candles(ax, opens, highs, lows, closes):
    """Draw bare OHLC candlesticks. All inputs are float numpy arrays."""
    n = len(opens)
    for i in range(n):
        bull  = closes[i] >= opens[i]
        color = C_BULL if bull else C_BEAR
        ax.plot([i, i], [lows[i], highs[i]], color=color, linewidth=0.8, zorder=1)
        bottom = min(opens[i], closes[i])
        height = max(abs(closes[i] - opens[i]), (highs[i] - lows[i]) * 0.005)
        rect = mpatches.Rectangle(
            (i - 0.4, bottom), 0.8, height,
            facecolor=color, edgecolor=color, linewidth=0, zorder=2,
        )
        ax.add_patch(rect)


def _draw_regime_context_shading(
    ax,
    window_df: pd.DataFrame,
    highs: np.ndarray,
    lows: np.ndarray,
) -> None:
    """
    Background by kz_active / regime_bull / regime_bear (columns from app pipeline).
    Grey axvspan outside KZ; tiled arrow glyphs inside KZ by confluence state.
    """
    n = len(window_df)
    if n == 0 or "kz_active" not in window_df.columns:
        return
    kz = window_df["kz_active"].fillna(False).to_numpy(dtype=bool)
    bull = (
        window_df["regime_bull"].fillna(True).to_numpy(dtype=bool)
        if "regime_bull" in window_df.columns
        else np.ones(n, dtype=bool)
    )
    bear = (
        window_df["regime_bear"].fillna(True).to_numpy(dtype=bool)
        if "regime_bear" in window_df.columns
        else np.ones(n, dtype=bool)
    )
    states = np.empty(n, dtype=np.int8)
    for i in range(n):
        if not kz[i]:
            states[i] = 0
        elif bull[i] and not bear[i]:
            states[i] = 1
        elif bear[i] and not bull[i]:
            states[i] = 2
        elif bull[i] and bear[i]:
            states[i] = 3
        else:
            states[i] = 4

    y_lo = float(np.nanmin(lows))
    y_hi = float(np.nanmax(highs))
    if not np.isfinite(y_lo) or not np.isfinite(y_hi):
        return
    y_mid = 0.5 * (y_lo + y_hi)

    i = 0
    while i < n:
        st = int(states[i])
        j = i
        while j < n and int(states[j]) == st:
            j += 1
        i0, i1 = i, j - 1
        x0, x1 = float(i0) - 0.5, float(i1) + 0.5
        span_bars = i1 - i0 + 1
        step = max(0.85, span_bars / 8.0)

        if st == 0:
            ax.axvspan(x0, x1, facecolor="#888888", alpha=0.10, zorder=1)
        elif st == 1:
            xv = i0
            while xv <= i1 + 1e-6:
                ax.text(
                    xv, y_mid, "↑",
                    ha="center", va="center", fontsize=13,
                    color="#2e7d32", alpha=0.08, zorder=1,
                )
                xv += step
        elif st == 2:
            xv = i0
            while xv <= i1 + 1e-6:
                ax.text(
                    xv, y_mid, "↓",
                    ha="center", va="center", fontsize=13,
                    color="#c62828", alpha=0.08, zorder=1,
                )
                xv += step
        elif st == 3:
            xv = i0
            while xv <= i1 + 1e-6:
                ax.text(
                    xv, y_mid, "↕",
                    ha="center", va="center", fontsize=13,
                    color="#888888", alpha=0.08, zorder=1,
                )
                xv += step
        i = j


def _annotate_patterns(ax, df, active_patterns, highs, lows):
    """Coloured borders + symbols on pattern candles; brackets on multi-candle."""
    from patterns import PATTERN_LABELS
    pat_cols = {
        name: f"pat_{name}" for name in active_patterns
        if f"pat_{name}" in df.columns
    }
    if not pat_cols:
        return

    # Pre-extract OHLC as numpy arrays — avoids df.iloc[i] inside the loop
    opens_arr  = df["open"].to_numpy(dtype=float)
    closes_arr = df["close"].to_numpy(dtype=float)

    price_range = max(highs) - min(lows)
    if price_range <= 0:
        price_range = max(np.ptp(highs), np.ptp(lows), 1e-12)

    single_by_bar: dict[int, list[tuple[int, str, str]]] = defaultdict(list)
    multi_bracket_lift = 0

    for name, col in pat_cols.items():
        sig_arr = df[col].to_numpy(dtype=np.int8)
        span    = _MULTI_SPAN.get(name, 1)

        # Only iterate over bars that actually have a signal
        signal_positions = np.where(sig_arr != 0)[0]
        for i in signal_positions:
            sig          = int(sig_arr[i])
            border_color = C_BEAR_BORDER if sig == -1 else C_BULL_BORDER
            symbol       = "↓" if sig == -1 else "↑"
            start_i      = max(0, i - span + 1)

            for j in range(start_i, i + 1):
                bottom = min(opens_arr[j], closes_arr[j])
                height = max(
                    abs(closes_arr[j] - opens_arr[j]),
                    (highs[j] - lows[j]) * 0.005,
                )
                rect = mpatches.Rectangle(
                    (j - 0.45, bottom - height * 0.05),
                    0.9, height * 1.10,
                    facecolor="none", edgecolor=border_color,
                    linewidth=1.8, zorder=3,
                )
                ax.add_patch(rect)

            if span == 1:
                single_by_bar[i].append((sig, border_color, symbol))
            else:
                mid_x  = (start_i + i) / 2.0
                y_top  = max(highs[start_i: i + 1]) * 1.002
                offset = price_range * 0.04
                lift   = multi_bracket_lift * price_range * 0.055
                y_br   = y_top + offset + lift
                multi_bracket_lift += 1
                label  = PATTERN_LABELS.get(name, name.replace("_", " ").title())
                ax.annotate(
                    "",
                    xy=(start_i - 0.4, y_br), xytext=(i + 0.4, y_br),
                    arrowprops=dict(arrowstyle="-", color=border_color, lw=1.4),
                    zorder=4,
                )
                ax.text(mid_x, y_br + offset * 0.5, label,
                        ha="center", va="bottom", fontsize=max(9, FS_BRKT),
                        color=border_color, fontweight="bold", zorder=4)

    stack_step = price_range * 0.07
    for i, items in single_by_bar.items():
        bulls = [t for t in items if t[0] == 1]
        bears = [t for t in items if t[0] == -1]
        pad_i = max((highs[i] - lows[i]) * 0.18, price_range * 0.012)
        for k, (_sig, border_color, symbol) in enumerate(bulls):
            stack = (k - (len(bulls) - 1) / 2.0) * stack_step if len(bulls) > 1 else 0.0
            y_pos = lows[i] - pad_i - stack
            ax.text(
                i, y_pos, symbol, ha="center", va="top",
                fontsize=max(9, FS_ANN), color=border_color,
                fontweight="bold", zorder=4,
            )
        for k, (_sig, border_color, symbol) in enumerate(bears):
            stack = (k - (len(bears) - 1) / 2.0) * stack_step if len(bears) > 1 else 0.0
            y_pos = highs[i] + pad_i + stack
            ax.text(
                i, y_pos, symbol, ha="center", va="bottom",
                fontsize=max(9, FS_ANN), color=border_color,
                fontweight="bold", zorder=4,
            )


def _build_annotation(row: pd.Series, active_patterns: list) -> str:
    """Build the tooltip annotation string for a single candle row."""
    parts = []
    for name in active_patterns:
        col = f"pat_{name}"
        val = row.get(col, 0)
        if val != 0:
            from patterns import PATTERN_LABELS
            label     = PATTERN_LABELS.get(name, name)
            direction = "Bull" if val == 1 else "Bear"
            parts.append(f"{label} ({direction})")
    rsi_v = row.get("rsi", float("nan"))
    if not pd.isna(rsi_v):
        if rsi_v > 70:
            parts.append(f"RSI {rsi_v:.0f} OB")
        elif rsi_v < 30:
            parts.append(f"RSI {rsi_v:.0f} OS")
    bb_u = row.get("bb_upper", float("nan"))
    bb_l = row.get("bb_lower", float("nan"))
    if not pd.isna(bb_u):
        if row.get("close", 0) > bb_u:
            parts.append("BB↑")
        elif row.get("close", 0) < bb_l:
            parts.append("BB↓")
    if not parts:
        o = row.get("open", 0)
        h = row.get("high", 0)
        l = row.get("low", 0)
        c = row.get("close", 0)
        parts.append(f"O:{o:.5g} H:{h:.5g} L:{l:.5g} C:{c:.5g}")
    return " — ".join(parts)


def _equity_position_heights_colors(signals: np.ndarray, n: int) -> tuple[np.ndarray, int]:
    """
    Forward-filled regime for the position overlay: last non-zero signal repeats until
    the next signal. Values are +1 (long), -1 (short), or 0 (flat before any signal).

    Overlay (drawn by caller): net > 0 → amber #FFCC80; net < 0 → blue #90CAF9;
    net == 0 → no bar. peak = max(1, max(|net|)) for symmetric y-limits (-peak, peak).
    """
    sig = np.asarray(signals, dtype=np.int8).ravel()
    if len(sig) < n:
        sig = np.pad(sig, (0, n - len(sig)), constant_values=0)
    else:
        sig = sig[:n]
    last = 0
    net = np.zeros(n, dtype=np.float64)
    for i in range(n):
        if sig[i] != 0:
            last = int(sig[i])
        net[i] = last
    peak = max(1, int(np.max(np.abs(net))) if n > 0 else 0)
    return net, peak


def _draw_trade_entry_markers(
    ax,
    x: np.ndarray,
    highs: np.ndarray,
    lows: np.ndarray,
    signals: np.ndarray,
    pip: float | None,
) -> None:
    n = len(x)
    sig = np.asarray(signals, dtype=np.int8).ravel()
    if len(sig) < n:
        sig = np.pad(sig, (0, n - len(sig)), constant_values=0)
    else:
        sig = sig[:n]
    if pip is not None and pip > 0:
        off = 3.0 * float(pip)
    else:
        rng = float(np.nanmax(highs) - np.nanmin(lows))
        off = max(rng * 0.002, 1e-12)
    for i in range(n):
        s = int(sig[i])
        if s == 1:
            ax.scatter(
                float(x[i]),
                float(lows[i] - off),
                marker="^",
                s=120,
                color=C_BULL,
                zorder=5,
                edgecolors="none",
                linewidths=0,
            )
        elif s == -1:
            ax.scatter(
                float(x[i]),
                float(highs[i] + off),
                marker="v",
                s=120,
                color=C_BEAR,
                zorder=5,
                edgecolors="none",
                linewidths=0,
            )


def render_equity_chart(
    full_df: pd.DataFrame,
    equity_x: np.ndarray,
    equity_y_pips: np.ndarray,
    equity_y_usd: np.ndarray,
    full_index: pd.Index,
    window_start: int,
    window_end: int,
    notional: float,
    instrument: str,
    signals: np.ndarray | None = None,
) -> str:
    """
    Full-period equity (USD) on primary axis; faint close price on secondary (right).
    Vertical band marks the current window in bar-index space.
    """
    _ = equity_y_pips, instrument
    n_full = len(equity_x)
    if n_full < 1:
        fig, ax = plt.subplots(1, 1, figsize=(FIG_W, FIG_H_EQUITY), facecolor=C_BG)
        ax.text(0.5, 0.5, "No equity", ha="center", va="center",
                transform=ax.transAxes, fontsize=FS_LABEL)
        buf = io.BytesIO()
        fig.tight_layout()
        fig.subplots_adjust(bottom=0.2)
        fig.savefig(buf, format="png", dpi=EXPORT_DPI, facecolor=C_BG)
        buf.seek(0)
        plt.close(fig)
        return base64.b64encode(buf.read()).decode("utf-8")

    n = min(n_full, len(full_df), len(full_index))
    if n < 1:
        fig, ax = plt.subplots(1, 1, figsize=(FIG_W, FIG_H_EQUITY), facecolor=C_BG)
        ax.text(0.5, 0.5, "No equity", ha="center", va="center",
                transform=ax.transAxes, fontsize=FS_LABEL)
        buf = io.BytesIO()
        fig.tight_layout()
        fig.subplots_adjust(bottom=0.2)
        fig.savefig(buf, format="png", dpi=EXPORT_DPI, facecolor=C_BG)
        buf.seek(0)
        plt.close(fig)
        return base64.b64encode(buf.read()).decode("utf-8")

    x_full = np.arange(n, dtype=float)
    ed = np.asarray(equity_y_usd[:n], dtype=float)
    idx = full_index[:n]
    pos_ylim = None

    fig, ax_eq = plt.subplots(1, 1, figsize=(FIG_W, FIG_H_EQUITY), facecolor=C_BG)
    ax_px = ax_eq.twinx()
    ax_pos = ax_eq.twinx() if signals is not None else None
    if ax_pos is not None:
        ax_pos.spines["right"].set_position(("outward", 54))
        ax_px.set_zorder(1)
        ax_pos.set_zorder(2)
        ax_eq.set_zorder(3)
    else:
        ax_px.set_zorder(1)
        ax_eq.set_zorder(2)

    closes = full_df["close"].to_numpy(dtype=float)[:n]
    vc = np.isfinite(closes)
    if vc.any():
        ax_px.plot(
            x_full[vc], closes[vc], color="#B0BEC5",
            linewidth=0.8, alpha=0.5, zorder=0,
        )
    ax_px.set_ylabel("")
    ax_px.tick_params(axis="y", labelsize=9, labelcolor="#90a4ae")
    for s in ax_px.spines.values():
        s.set_visible(False)

    ax_eq.set_facecolor(C_BG)
    for s in ax_eq.spines.values():
        s.set_visible(False)
    ax_eq.grid(axis="y", color=C_GRID, linewidth=0.5, zorder=0)

    ws, we = int(window_start), int(window_end)
    if we > ws and n > 0:
        wl = max(-0.5, ws - 0.5)
        wr = min(float(n) - 0.5, we - 0.5)
        if wr > wl:
            ax_eq.axvspan(
                wl, wr, facecolor="#E3F2FD", alpha=0.35, zorder=0,
            )
            for xv in (wl, wr):
                ax_eq.axvline(
                    xv,
                    color="#444444",
                    linestyle="--",
                    linewidth=1.0,
                    zorder=2.5,
                )

    if ax_pos is not None:
        net_pos, peak_pos = _equity_position_heights_colors(signals, n)
        # peak_pos is max(1, max(|net|)) from _equity_position_heights_colors — symmetric
        # ylim, not max(net) alone (shorts need room below 0).
        pos_ylim = (float(-peak_pos), float(peak_pos))
        mask = net_pos != 0
        if np.any(mask):
            ix = np.nonzero(mask)[0]
            # mask excludes net==0: >0 → amber long, <0 → blue short (not >= 0).
            bar_colors = [
                "#FFCC80" if float(net_pos[j]) > 0 else "#90CAF9"
                for j in ix
            ]
            ax_pos.bar(
                ix.astype(float),
                net_pos[ix],
                width=0.85,
                color=bar_colors,
                align="center",
                alpha=0.35,
                zorder=1,
                edgecolor="none",
            )
        ax_pos.set_ylim(pos_ylim)
        ax_pos.axhline(0.0, color=C_TEXT, linewidth=0.45, alpha=0.5, zorder=2)
        ax_pos.yaxis.set_major_locator(MaxNLocator(integer=True, min_n_ticks=2))
        ax_pos.tick_params(axis="y", labelsize=8, labelcolor=C_TEXT)
        ax_pos.set_ylabel("")
        for s in ax_pos.spines.values():
            s.set_visible(False)
        ax_pos.spines["right"].set_visible(True)

    ax_eq.fill_between(x_full, ed, 0.0, where=(ed >= 0),
                       color=C_BULL, alpha=0.22, zorder=2)
    ax_eq.fill_between(x_full, ed, 0.0, where=(ed < 0),
                       color=C_BEAR, alpha=0.22, zorder=2)
    ax_eq.plot(x_full, ed, color=C_EQUITY, linewidth=1.35, alpha=0.95, zorder=3)
    ax_eq.axhline(0.0, color=C_TEXT, linewidth=0.55, alpha=0.45, zorder=2)
    ax_eq.set_ylabel("P&L ($)", fontsize=FS_LABEL, color=C_TEXT)
    ax_eq.yaxis.set_major_formatter(
        FuncFormatter(lambda v, _p: f"${v:,.0f}"),
    )
    ax_eq.tick_params(axis="y", labelsize=FS_TICK - 4, labelcolor=C_TEXT)

    nt = int(round(notional))
    ax_eq.set_title(
        f"Equity (${nt:,} notional)",
        fontsize=FS_LABEL, loc="left", color=C_TEXT, pad=16, y=1.07,
    )
    ax_eq.set_xlim(-0.8, max(n - 1, 0) + 0.2)
    _apply_equity_sparse_date_axis_index(ax_eq, idx, n)
    ax_eq.patch.set_visible(False)

    if np.all(np.isfinite(ed)) and np.allclose(ed, 0.0, rtol=0.0, atol=1e-6):
        ax_eq.text(
            0.5,
            0.5,
            "No trades triggered in this period",
            transform=ax_eq.transAxes,
            ha="center",
            va="center",
            fontsize=13,
            color="#555555",
            fontweight="bold",
            zorder=10,
        )

    buf = io.BytesIO()
    fig.tight_layout()
    fig.subplots_adjust(bottom=0.2)
    if pos_ylim is not None and ax_pos is not None:
        ax_pos.set_ylim(pos_ylim)
    fig.savefig(buf, format="png", dpi=EXPORT_DPI, facecolor=C_BG)
    buf.seek(0)
    image_b64 = base64.b64encode(buf.read()).decode("utf-8")
    plt.close(fig)
    return image_b64


def render_chart(window_df: pd.DataFrame,
                 active_filters: list,
                 active_patterns: list,
                 window_start: int = 0,
                 window_end: int = 50,
                 full_len: int = 0,
                 show_volume: bool = True,
                 signals: np.ndarray | None = None,
                 pip: float | None = None) -> tuple[str, dict]:
    """
    Price, indicators, and optional volume. No equity panel.
    When full_len > 1, a subtle axes-fraction band on the price panel shows
    where [window_start, window_end) sits in the full backtest.
    Optional `signals` (per-bar int8, window length) draws entry markers; `pip` scales offset.
    Returns (image_base64, candle_map).

    Layout: Price (8), then each active indicator panel (3 each), optional volume (2).
    X-axis labels only on the bottom-most panel.
    """
    n = len(window_df)
    if n == 0:
        fig, ax = plt.subplots(1, 1, figsize=(FIG_W, FIG_H))
        ax.text(0.5, 0.5, "No data", ha="center", va="center",
                transform=ax.transAxes, fontsize=FS_LABEL)
        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=EXPORT_DPI)
        buf.seek(0)
        plt.close(fig)
        return base64.b64encode(buf.read()).decode(), {}

    has_bb_width = "bb_width" in active_filters and "bb_width" in window_df.columns
    has_rsi      = "rsi"  in active_filters and "rsi"  in window_df.columns
    has_macd     = "macd" in active_filters and "macd" in window_df.columns
    has_stoch    = "stochastic" in active_filters and "stoch_k" in window_df.columns
    has_cci      = "cci" in active_filters and "cci" in window_df.columns
    has_wr       = "williams_r" in active_filters and "williams_r" in window_df.columns
    has_atr_ind  = "atr_ind" in active_filters and "atr_ind" in window_df.columns
    has_hist_vol = "hist_vol" in active_filters and "hist_vol" in window_df.columns
    af = active_filters or []
    _has_volume_col = "volume" in window_df.columns
    has_vol = bool(_has_volume_col and (show_volume or "volume_spike" in af))

    ratios = [8]
    if has_bb_width:
        ratios.append(3)
    if has_rsi:
        ratios.append(3)
    if has_macd:
        ratios.append(3)
    if has_stoch:
        ratios.append(3)
    if has_cci:
        ratios.append(3)
    if has_wr:
        ratios.append(3)
    if has_atr_ind:
        ratios.append(3)
    if has_hist_vol:
        ratios.append(3)
    if has_vol:
        ratios.append(2)

    n_panels = len(ratios)
    fig = plt.figure(figsize=(FIG_W, FIG_H), facecolor=C_BG)
    gs  = gridspec.GridSpec(n_panels, 1, height_ratios=ratios,
                            hspace=0.08, left=0.07, right=0.93,
                            top=0.97, bottom=0.14)

    panel_idx = 0
    ax_price    = fig.add_subplot(gs[panel_idx]); panel_idx += 1
    ax_bb_width = None
    ax_rsi      = None
    ax_macd     = None
    ax_stoch    = None
    ax_cci      = None
    ax_wr       = None
    ax_atr_ind  = None
    ax_hist_vol = None
    ax_vol      = None

    if has_bb_width:
        ax_bb_width = fig.add_subplot(gs[panel_idx]); panel_idx += 1
    if has_rsi:
        ax_rsi = fig.add_subplot(gs[panel_idx]); panel_idx += 1
    if has_macd:
        ax_macd = fig.add_subplot(gs[panel_idx]); panel_idx += 1
    if has_stoch:
        ax_stoch = fig.add_subplot(gs[panel_idx]); panel_idx += 1
    if has_cci:
        ax_cci = fig.add_subplot(gs[panel_idx]); panel_idx += 1
    if has_wr:
        ax_wr = fig.add_subplot(gs[panel_idx]); panel_idx += 1
    if has_atr_ind:
        ax_atr_ind = fig.add_subplot(gs[panel_idx]); panel_idx += 1
    if has_hist_vol:
        ax_hist_vol = fig.add_subplot(gs[panel_idx]); panel_idx += 1
    if has_vol:
        ax_vol = fig.add_subplot(gs[panel_idx]); panel_idx += 1

    x = np.arange(n)
    opens  = window_df["open"].to_numpy(dtype=float)
    highs  = window_df["high"].to_numpy(dtype=float)
    lows   = window_df["low"].to_numpy(dtype=float)
    closes = window_df["close"].to_numpy(dtype=float)

    # ── 1. Price panel ──────────────────────────────────────────────────────
    ax_price.set_facecolor(C_BG)
    ax_price.grid(axis="y", color=C_GRID, linewidth=0.5, zorder=0)
    ax_price.tick_params(axis="x", which="both", labelbottom=False)
    ax_price.spines[:].set_visible(False)

    if full_len > 0:
        ws, we = int(window_start), int(window_end)
        fl = float(full_len)
        xa0 = ws / fl
        xa1 = we / fl
        xa0 = max(0.0, min(1.0, xa0))
        xa1 = max(0.0, min(1.0, xa1))
        if we > ws and xa1 > xa0:
            trans = mtransforms.blended_transform_factory(
                ax_price.transAxes, ax_price.transData,
            )
            ax_price.axvspan(
                xa0, xa1, transform=trans,
                facecolor="#E3F2FD", alpha=0.2, zorder=0,
            )

    _draw_candles(ax_price, opens, highs, lows, closes)
    _draw_regime_context_shading(ax_price, window_df, highs, lows)

    if signals is not None:
        _draw_trade_entry_markers(ax_price, x, highs, lows, signals, pip)

    # Bollinger Bands
    if "bb" in active_filters and "bb_upper" in window_df.columns:
        u = window_df["bb_upper"].to_numpy(dtype=float)
        m = window_df["bb_mid"].to_numpy(dtype=float)
        l = window_df["bb_lower"].to_numpy(dtype=float)
        valid = ~(np.isnan(u) | np.isnan(l))
        if valid.any():
            ax_price.plot(x[valid], u[valid], color=C_BB, linewidth=0.9,
                          alpha=0.8, linestyle="--", zorder=2)
            ax_price.plot(x[valid], m[valid], color=C_BB, linewidth=0.6,
                          alpha=0.5, linestyle=":", zorder=2)
            ax_price.plot(x[valid], l[valid], color=C_BB, linewidth=0.9,
                          alpha=0.8, linestyle="--", zorder=2)
            ax_price.fill_between(x[valid], u[valid], l[valid],
                                  alpha=0.05, color=C_BB, zorder=1)

    # EMA/SMA overlays — legacy "ema" maps to the ema20 column (same as indicators.py)
    _ma_overlay_keys = set(active_filters or [])
    if "ema" in _ma_overlay_keys:
        _ma_overlay_keys.add("ema20")

    for fk, column, ls, lw, color in _MA_OVERLAY_SPECS:
        if fk not in _ma_overlay_keys:
            continue
        if column not in window_df.columns:
            continue
        y = pd.to_numeric(window_df[column], errors="coerce").to_numpy(dtype=float)
        y_plot = np.where(np.isfinite(y), y, np.nan)
        n_fin = int(np.isfinite(y_plot).sum())
        if n_fin == 0:
            continue
        plot_kw = {"label": fk.upper()}
        # A single finite point produces no visible line segment; show a marker.
        if n_fin == 1:
            plot_kw["marker"] = "o"
            plot_kw["markersize"] = 5
        ax_price.plot(
            x, y_plot, color=color, linewidth=lw,
            linestyle=ls, alpha=0.9, zorder=3,
            **plot_kw,
        )
        fin_idx = np.flatnonzero(np.isfinite(y_plot))
        if len(fin_idx):
            li = int(fin_idx[-1])
            x_last = float(x[li])
            y_last = float(y_plot[li])
            ax_price.annotate(
                _ma_overlay_label(fk),
                xy=(x_last, y_last),
                fontsize=20,
                color=color,
                va="center",
                ha="left",
            )

    # Keltner Channels — bands match (lw/alpha); mid EMA dashed
    if "keltner" in active_filters and "kc_upper" in window_df.columns:
        kcu = window_df["kc_upper"].to_numpy(dtype=float)
        kcm = window_df["kc_mid"].to_numpy(dtype=float)
        kcl = window_df["kc_lower"].to_numpy(dtype=float)
        _kc_band = dict(
            color="#e53935", linewidth=1.5, linestyle="-", alpha=0.92, zorder=2,
        )
        valid_u = np.isfinite(kcu)
        valid_l = np.isfinite(kcl)
        valid_m = np.isfinite(kcm)
        if valid_u.any():
            ax_price.plot(x[valid_u], kcu[valid_u], **_kc_band)
        if valid_l.any():
            ax_price.plot(x[valid_l], kcl[valid_l], **_kc_band)
        if valid_m.any():
            ax_price.plot(
                x[valid_m], kcm[valid_m], color="#e53935", linewidth=1.0,
                linestyle="--", alpha=0.78, zorder=2,
            )
        fin_u = np.flatnonzero(np.isfinite(kcu))
        if len(fin_u):
            li = int(fin_u[-1])
            ax_price.annotate(
                "Kel",
                xy=(float(x[li]), float(kcu[li])),
                fontsize=FS_LABEL,
                color="#e53935",
                va="center",
                ha="left",
            )
        fin_l = np.flatnonzero(np.isfinite(kcl))
        if len(fin_l):
            li = int(fin_l[-1])
            ax_price.annotate(
                "Kel",
                xy=(float(x[li]), float(kcl[li])),
                fontsize=FS_LABEL,
                color="#e53935",
                va="center",
                ha="left",
            )

    # Donchian Channels — upper/lower same style; mid dotted reference
    if "donchian" in active_filters and "dc_upper" in window_df.columns:
        dcu = window_df["dc_upper"].to_numpy(dtype=float)
        dcm = window_df["dc_mid"].to_numpy(dtype=float)
        dcl = window_df["dc_lower"].to_numpy(dtype=float)
        _dc_band = dict(
            color="#0d47a1", linewidth=1.5, linestyle="-", alpha=0.9, zorder=2,
        )
        valid_u = np.isfinite(dcu)
        valid_l = np.isfinite(dcl)
        valid_m = np.isfinite(dcm)
        if valid_u.any():
            ax_price.plot(x[valid_u], dcu[valid_u], **_dc_band)
        if valid_l.any():
            ax_price.plot(x[valid_l], dcl[valid_l], **_dc_band)
        if valid_m.any():
            ax_price.plot(
                x[valid_m], dcm[valid_m], color="#1565c0", linewidth=1.0,
                linestyle=":", alpha=0.55, zorder=2,
            )
        fin_u = np.flatnonzero(np.isfinite(dcu))
        if len(fin_u):
            li = int(fin_u[-1])
            ax_price.annotate(
                "Don",
                xy=(float(x[li]), float(dcu[li])),
                fontsize=FS_LABEL,
                color="#0d47a1",
                va="center",
                ha="left",
            )
        fin_l = np.flatnonzero(np.isfinite(dcl))
        if len(fin_l):
            li = int(fin_l[-1])
            ax_price.annotate(
                "Don",
                xy=(float(x[li]), float(dcl[li])),
                fontsize=FS_LABEL,
                color="#0d47a1",
                va="center",
                ha="left",
            )

    # Squeeze — bottom of price panel: green = no squeeze, red = squeeze on
    if "squeeze" in active_filters and "squeeze" in window_df.columns:
        sq = window_df["squeeze"].to_numpy(dtype=bool)
        rng = max(highs.max() - lows.min(), 1e-12)
        dot_y = lows.min() - rng * 0.035
        colors_sq = np.where(sq, "#ef5350", "#4caf50")
        ax_price.scatter(
            x, np.full(n, dot_y), c=colors_sq, s=22, zorder=5, marker="o",
            edgecolors="none",
        )
        ax_price.annotate(
            "SQZ",
            xy=(float(x[n - 1]), float(dot_y)),
            fontsize=FS_LABEL,
            color=C_TEXT,
            va="center",
            ha="left",
        )

    if active_patterns:
        _annotate_patterns(ax_price, window_df, active_patterns, highs, lows)

    pad = (highs.max() - lows.min()) * 0.06
    y_lo = lows.min() - pad
    if "squeeze" in active_filters and "squeeze" in window_df.columns:
        rng = max(highs.max() - lows.min(), 1e-12)
        y_lo = min(y_lo, lows.min() - rng * 0.055)
    ax_price.set_ylim(y_lo, highs.max() + pad)
    ax_price.tick_params(axis="y", labelsize=FS_TICK, labelcolor=C_TEXT)

    # ── 2. BB Width panel (below price) ─────────────────────────────────────
    if has_bb_width:
        bw_vals = window_df["bb_width"].to_numpy(dtype=float)
        ax_bb_width.set_facecolor(C_BG)
        ax_bb_width.grid(axis="y", color=C_GRID, linewidth=0.5)
        ax_bb_width.spines[:].set_visible(False)
        valid_bw = np.isfinite(bw_vals)
        if valid_bw.any():
            ax_bb_width.plot(
                x[valid_bw], bw_vals[valid_bw], color=C_BB, linewidth=1.2, zorder=2,
            )
        ax_bb_width.set_ylabel("BB Width", fontsize=FS_LABEL, color=C_TEXT)
        ax_bb_width.tick_params(
            axis="both", labelsize=FS_TICK, labelcolor=C_TEXT, labelbottom=False,
        )

    # ── 3. RSI panel ────────────────────────────────────────────────────────
    if has_rsi:
        rsi_vals = window_df["rsi"].to_numpy(dtype=float)
        ax_rsi.set_facecolor(C_BG)
        ax_rsi.grid(axis="y", color=C_GRID, linewidth=0.5)
        ax_rsi.spines[:].set_visible(False)
        valid = ~np.isnan(rsi_vals)
        if valid.any():
            ax_rsi.plot(x[valid], rsi_vals[valid], color=C_RSI, linewidth=1.2)
        ax_rsi.axhline(70, color=C_BEAR, linewidth=0.9, linestyle="--", alpha=0.7)
        ax_rsi.axhline(30, color=C_BULL, linewidth=0.9, linestyle="--", alpha=0.7)
        ax_rsi.set_ylim(0, 100)
        ax_rsi.set_ylabel("RSI", fontsize=FS_LABEL, color=C_TEXT)
        ax_rsi.tick_params(axis="both", labelsize=FS_TICK,
                           labelcolor=C_TEXT, labelbottom=False)

    # ── 4. MACD panel ───────────────────────────────────────────────────────
    if has_macd:
        macd_vals = window_df["macd"].to_numpy(dtype=float)
        sig_vals  = window_df["macd_signal"].to_numpy(dtype=float)
        hist_vals = window_df["macd_hist"].to_numpy(dtype=float)
        ax_macd.set_facecolor(C_BG)
        ax_macd.grid(axis="y", color=C_GRID, linewidth=0.5)
        ax_macd.spines[:].set_visible(False)
        for i in range(n):
            if not np.isnan(hist_vals[i]):
                c = C_HIST_POS if hist_vals[i] >= 0 else C_HIST_NEG
                ax_macd.bar(i, hist_vals[i], color=c, alpha=0.7, width=0.7, zorder=1)
        valid_m = ~np.isnan(macd_vals)
        valid_s = ~np.isnan(sig_vals)
        if valid_m.any():
            ax_macd.plot(x[valid_m], macd_vals[valid_m],
                         color=C_MACD, linewidth=1.0)
        if valid_s.any():
            ax_macd.plot(x[valid_s], sig_vals[valid_s],
                         color=C_SIGNAL, linewidth=1.0)
        ax_macd.axhline(0, color=C_TEXT, linewidth=0.4, alpha=0.4)
        ax_macd.set_ylabel("MACD", fontsize=FS_LABEL, color=C_TEXT)
        ax_macd.tick_params(axis="both", labelsize=FS_TICK,
                            labelcolor=C_TEXT, labelbottom=False)

    # ── 5. Stochastic panel ─────────────────────────────────────────────────
    if has_stoch:
        k_vals = window_df["stoch_k"].to_numpy(dtype=float)
        d_vals = window_df["stoch_d"].to_numpy(dtype=float)
        ax_stoch.set_facecolor(C_BG)
        ax_stoch.grid(axis="y", color=C_GRID, linewidth=0.5)
        ax_stoch.spines[:].set_visible(False)
        valid_k = ~np.isnan(k_vals)
        valid_d = ~np.isnan(d_vals)
        if valid_k.any():
            ax_stoch.plot(x[valid_k], k_vals[valid_k], color="#1565c0", linewidth=1.2,
                          label="%K")
        if valid_d.any():
            ax_stoch.plot(x[valid_d], d_vals[valid_d], color="#e53935", linewidth=1.0,
                          label="%D")
        ax_stoch.axhline(80, color=C_BEAR, linewidth=0.8, linestyle="--", alpha=0.6)
        ax_stoch.axhline(20, color=C_BULL, linewidth=0.8, linestyle="--", alpha=0.6)
        ax_stoch.set_ylim(0, 100)
        ax_stoch.set_ylabel("Stoch", fontsize=FS_LABEL, color=C_TEXT)
        ax_stoch.tick_params(axis="both", labelsize=FS_TICK,
                             labelcolor=C_TEXT, labelbottom=False)

    # ── 6. CCI panel ────────────────────────────────────────────────────────
    if has_cci:
        cci_vals = window_df["cci"].to_numpy(dtype=float)
        ax_cci.set_facecolor(C_BG)
        ax_cci.grid(axis="y", color=C_GRID, linewidth=0.5)
        ax_cci.spines[:].set_visible(False)
        valid = ~np.isnan(cci_vals)
        if valid.any():
            ax_cci.plot(x[valid], cci_vals[valid], color="#7b1fa2", linewidth=1.2)
        ax_cci.axhline(100,  color=C_BEAR, linewidth=0.8, linestyle="--", alpha=0.6)
        ax_cci.axhline(-100, color=C_BULL, linewidth=0.8, linestyle="--", alpha=0.6)
        ax_cci.axhline(0,    color=C_TEXT, linewidth=0.4, alpha=0.3)
        ax_cci.set_ylabel("CCI", fontsize=FS_LABEL, color=C_TEXT)
        ax_cci.tick_params(axis="both", labelsize=FS_TICK,
                           labelcolor=C_TEXT, labelbottom=False)

    # ── 7. Williams %R panel ─────────────────────────────────────────────────
    if has_wr:
        wr_vals = window_df["williams_r"].to_numpy(dtype=float)
        ax_wr.set_facecolor(C_BG)
        ax_wr.grid(axis="y", color=C_GRID, linewidth=0.5)
        ax_wr.spines[:].set_visible(False)
        valid = ~np.isnan(wr_vals)
        if valid.any():
            ax_wr.plot(x[valid], wr_vals[valid], color=C_EMA50, linewidth=1.2)
        ax_wr.axhline(-20, color=C_BEAR, linewidth=0.8, linestyle="--", alpha=0.6)
        ax_wr.axhline(-80, color=C_BULL, linewidth=0.8, linestyle="--", alpha=0.6)
        ax_wr.set_ylim(-100, 0)
        ax_wr.set_ylabel("W%R", fontsize=FS_LABEL, color=C_TEXT)
        ax_wr.tick_params(axis="both", labelsize=FS_TICK,
                          labelcolor=C_TEXT, labelbottom=False)

    # ── 8. ATR panel ────────────────────────────────────────────────────────
    if has_atr_ind:
        atr_vals = window_df["atr_ind"].to_numpy(dtype=float)
        ax_atr_ind.set_facecolor(C_BG)
        ax_atr_ind.grid(axis="y", color=C_GRID, linewidth=0.5)
        ax_atr_ind.spines[:].set_visible(False)
        valid = ~np.isnan(atr_vals)
        if valid.any():
            ax_atr_ind.fill_between(x[valid], atr_vals[valid], 0,
                                    color="#ffa726", alpha=0.5, zorder=1)
            ax_atr_ind.plot(x[valid], atr_vals[valid], color="#fb8c00",
                            linewidth=1.0, zorder=2)
        ax_atr_ind.set_ylabel("ATR", fontsize=FS_LABEL, color=C_TEXT)
        ax_atr_ind.tick_params(axis="both", labelsize=FS_TICK,
                               labelcolor=C_TEXT, labelbottom=False)

    # ── 9. Hist Vol panel ────────────────────────────────────────────────────
    if has_hist_vol:
        hv_vals = window_df["hist_vol"].to_numpy(dtype=float)
        ax_hist_vol.set_facecolor(C_BG)
        ax_hist_vol.grid(axis="y", color=C_GRID, linewidth=0.5)
        ax_hist_vol.spines[:].set_visible(False)
        valid = ~np.isnan(hv_vals)
        if valid.any():
            ax_hist_vol.plot(x[valid], hv_vals[valid], color="#7b1fa2",
                             linewidth=1.2)
        ax_hist_vol.set_ylabel("HV%", fontsize=FS_LABEL, color=C_TEXT)
        ax_hist_vol.tick_params(axis="both", labelsize=FS_TICK,
                                labelcolor=C_TEXT, labelbottom=False)

    # ── 10. Volume panel ────────────────────────────────────────────────────
    if has_vol and ax_vol is not None and _has_volume_col:
        vols = window_df["volume"].to_numpy(dtype=float)
        if "volume_spike" in active_filters:
            if "volume_spike" in window_df.columns:
                spike_mask = window_df["volume_spike"].to_numpy(dtype=float) == 1.0
            else:
                vma20 = pd.Series(vols).rolling(20, min_periods=1).mean().to_numpy()
                spike_mask = (
                    np.isfinite(vols) & np.isfinite(vma20) & (vols > 2.0 * vma20)
                )
        else:
            spike_mask = np.zeros(n, dtype=bool)
        ax_vol.set_facecolor(C_BG)
        ax_vol.spines[:].set_visible(False)
        ax_vol.grid(axis="y", color=C_GRID, linewidth=0.5)
        for i in range(n):
            if spike_mask[i]:
                c, a = "#ff3d00", 0.95
            else:
                c, a = "#b0bec5", 0.75
            ax_vol.bar(i, vols[i], color=c, alpha=a, width=0.7)
        ax_vol.set_ylabel("Vol", fontsize=FS_LABEL, color=C_TEXT)
        ax_vol.tick_params(axis="both", labelsize=FS_TICK,
                           labelcolor=C_TEXT, labelbottom=False)

    _xwin_lo, _xwin_hi = -0.8, n - 0.2
    for _ax in (ax_price, ax_bb_width, ax_rsi, ax_macd, ax_stoch, ax_cci, ax_wr,
                ax_atr_ind, ax_hist_vol, ax_vol):
        if _ax is not None:
            _ax.set_xlim(_xwin_lo, _xwin_hi)

    if has_vol and ax_vol is not None:
        bottom_dt = ax_vol
    else:
        bottom_dt = None
        for candidate in (ax_hist_vol, ax_atr_ind, ax_wr, ax_cci,
                          ax_stoch, ax_macd, ax_rsi, ax_bb_width):
            if candidate is not None:
                bottom_dt = candidate
                break
        if bottom_dt is None:
            bottom_dt = ax_price
    _apply_two_level_datetime_xaxis(bottom_dt, window_df)
    bottom_dt.tick_params(axis="x", labelbottom=True)
    if bottom_dt is not ax_price:
        ax_price.tick_params(axis="x", labelbottom=False)

    # ── Extract candle_map ───────────────────────────────────────────────────
    fig.canvas.draw()
    candle_map = {}
    for i, (ts, row) in enumerate(window_df.iterrows()):
        try:
            xd, yh = ax_price.transData.transform((i, highs[i]))
            _,  yl = ax_price.transData.transform((i, lows[i]))
            ann    = _build_annotation(row, active_patterns)
            ts_str = ts.isoformat() if hasattr(ts, "isoformat") else str(ts)
            candle_map[ts_str] = {
                "x":          int(round(xd)),
                "y_high":     int(round(yh)),
                "y_low":      int(round(yl)),
                "annotation": ann,
            }
        except Exception:
            pass

    # ── Serialise ────────────────────────────────────────────────────────────
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=EXPORT_DPI, facecolor=C_BG)
    buf.seek(0)
    image_b64 = base64.b64encode(buf.read()).decode("utf-8")
    plt.close(fig)
    return image_b64, candle_map
