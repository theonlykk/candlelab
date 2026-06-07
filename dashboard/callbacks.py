from datetime import datetime
import io

import pandas as pd
import plotly.graph_objects as go
from dash import Input, Output

from .data import (
    build_equity_curve,
    get_leaderboard,
    get_oos_curve,
    get_candles_batch,
    compute_regime_gates,
)
from .sparkline import build_sparkline
from .layout import regime_panel


def _empty_figure(annotation: str) -> go.Figure:
    fig = go.Figure()
    fig.update_layout(
        annotations=[{
            "text": annotation,
            "xref": "paper", "yref": "paper",
            "x": 0.5, "y": 0.5,
            "showarrow": False,
            "font": {"size": 14, "color": "#9e9e9e"},
        }],
        xaxis={"visible": False},
        yaxis={"visible": False},
        paper_bgcolor="#ffffff",
        plot_bgcolor="#ffffff",
        margin={"t": 20, "b": 20, "l": 20, "r": 20},
    )
    return fig


def _build_heatmap(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return _empty_figure("No data — sweep may still be running.")

    sqn_pivot = df.pivot_table(
        index="instrument", columns="granularity",
        values="oos_sqn100", aggfunc="max"
    )
    trades_pivot = df.pivot_table(
        index="instrument", columns="granularity",
        values="oos_n_trades", aggfunc="sum"
    )

    fig = go.Figure(data=go.Heatmap(
        z=sqn_pivot.values,
        x=sqn_pivot.columns.tolist(),
        y=sqn_pivot.index.tolist(),
        customdata=trades_pivot.values,
        colorscale="RdYlGn",
        zmid=1.5,
        hovertemplate=(
            "<b>%{y}</b><br>SQN: %{z:.2f}<br>"
            "Trades: %{customdata}<extra></extra>"
        ),
    ))
    fig.update_layout(
        title={"text": "OOS SQN100 Heatmap", "font": {"size": 13}},
        xaxis_title="Granularity",
        yaxis_title="Instrument",
        paper_bgcolor="#ffffff",
        plot_bgcolor="#ffffff",
        margin={"t": 40, "b": 40, "l": 120, "r": 20},
        height=500,
    )
    return fig


def _build_setup_label(row: dict) -> str:
    anchor = row.get("anchor") or ""
    cont   = row.get("continuation") or ""
    if cont and cont not in ("None", "none", ""):
        return f"{anchor} + {cont}"
    return anchor


def _build_red_summary(df: pd.DataFrame) -> str:
    counts = df["meta_status"].value_counts()
    parts  = []
    for status in [
        "RED_SPIKE", "RED_WEAK_SQN", "RED_STALE", "RED_THIN", "RED"
    ]:
        n = counts.get(status, 0)
        if n > 0:
            parts.append(f"{status}: {n}")
    null_n = df["meta_status"].isna().sum()
    if null_n > 0:
        parts.append(f"Unscored: {null_n}")
    return "  ·  ".join(parts) if parts else "No RED candidates"


def register(app) -> None:

    @app.callback(
        Output("leaderboard-store", "data"),
        Output("last-updated", "children"),
        Input("refresh-button", "n_clicks"),
        prevent_initial_call=False,
    )
    def refresh_store(_n_clicks):
        df = get_leaderboard()
        df["setup"] = df.apply(_build_setup_label, axis=1)
        df["sparkline_html"] = df["promoted_window_indices"].apply(
            build_sparkline
        )
        timestamp = datetime.utcnow().strftime(
            "Last updated: %Y-%m-%d %H:%M UTC"
        )
        return (
            df.to_json(orient="records", date_format="iso"),
            timestamp,
        )

    @app.callback(
        Output("leaderboard-table", "data"),
        Output("sqn-heatmap", "figure"),
        Output("summary-counts", "children"),
        Output("red-summary-bar", "children"),
        Output("regime-panel-container", "children"),
        Input("leaderboard-store", "data"),
        Input("meta-filter", "value"),
        Input("granularity-filter", "value"),
    )
    def render_dashboard(data, meta_filter, granularity_filter):
        if not data:
            empty = _empty_figure(
                "No data — sweep may still be running."
            )
            return [], empty, "", "No data", []

        df = pd.read_json(io.StringIO(data), orient="records")

        total   = len(df)
        n_green = (df["meta_status"] == "GREEN").sum()
        summary = (
            f"{total} candidates  ·  "
            f"{n_green} GREEN  ·  "
            f"{total - n_green} RED variants"
        )
        red_summary = _build_red_summary(df)

        # ── Regime panel — N+1 fix (Gemini ADR-104 ruling) ──────────
        # Fetch candles once per unique instrument, not per candidate.
        green_df      = df[df["meta_status"] == "GREEN"].copy()
        regime_panels = []

        if not green_df.empty:
            unique_instruments = green_df["instrument"].unique().tolist()

            # Single batch fetch — one DB round trip per timeframe
            candle_cache = get_candles_batch(unique_instruments)

            for _, row in green_df.iterrows():
                inst       = row.get("instrument", "")
                direction  = row.get("direction", "long")
                combo_type = row.get("combo_type", "Counter-Trend")

                # Reuse pre-fetched candles — no repeat DB calls
                candles = candle_cache.get(inst, {
                    "M30": None, "H4": None, "D1": None
                })
                gates = compute_regime_gates(
                    candles, direction, combo_type
                )
                regime_panels.append(
                    regime_panel(gates, inst, direction)
                )

        # Apply filters for table
        if meta_filter != "ALL":
            if meta_filter == "NULL":
                df = df[df["meta_status"].isna()]
            else:
                df = df[df["meta_status"] == meta_filter]

        if granularity_filter != "ALL":
            df = df[df["granularity"] == granularity_filter]

        if df.empty:
            return (
                [],
                _empty_figure("No matching candidates."),
                summary,
                red_summary,
                regime_panels,
            )

        return (
            df.to_dict("records"),
            _build_heatmap(df),
            summary,
            red_summary,
            regime_panels,
        )

    @app.callback(
        Output("equity-curve", "figure"),
        Input("leaderboard-table", "selected_rows"),
        Input("leaderboard-table", "data"),
    )
    def render_equity_curve(selected_rows, table_data):
        if not selected_rows or not table_data:
            return _empty_figure("Select a row to view equity curve")

        row    = table_data[selected_rows[0]]
        oos_df = get_oos_curve(
            row.get("instrument", ""),
            row.get("granularity", "M30"),
            row.get("anchor", ""),
            row.get("direction", ""),
            row.get("timeout_bars", 20),
        )

        if oos_df.empty:
            return _empty_figure("No promoted windows found")

        x, y = build_equity_curve(oos_df)
        if len(x) == 0:
            return _empty_figure("No promoted windows found")

        is_green   = row.get("meta_status") == "GREEN"
        line_color = "#4caf50" if is_green else "#90a4ae"

        fig = go.Figure(go.Scatter(
            x=x, y=y, mode="lines",
            line={"color": line_color, "width": 2},
            connectgaps=False,
        ))
        fig.add_hline(
            y=0, line_dash="dot",
            line_color="#e0e0e0", line_width=1,
        )
        fig.update_layout(
            title={
                "text": (
                    f"{row.get('instrument', '')} "
                    f"{row.get('granularity', '')} — "
                    f"{row.get('setup', row.get('anchor', ''))} "
                    f"{row.get('direction', '')} — OOS Equity"
                ),
                "font": {"size": 13},
            },
            xaxis_title="Trade #",
            yaxis_title="Cumulative R",
            hovermode="x unified",
            paper_bgcolor="#ffffff",
            plot_bgcolor="#ffffff",
            xaxis={"gridcolor": "#f0f0f0"},
            yaxis={"gridcolor": "#f0f0f0", "zeroline": False},
            margin={"t": 50, "b": 40, "l": 50, "r": 20},
            height=320,
        )
        return fig

    @app.callback(
        Output("help-panel", "style"),
        Input("help-toggle", "n_clicks"),
        prevent_initial_call=True,
    )
    def toggle_help(n_clicks):
        if n_clicks % 2 == 1:
            return {"display": "block"}
        return {"display": "none"}
