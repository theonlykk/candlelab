from datetime import datetime
import io

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from dash import Input, Output

from .data import build_equity_curve, get_leaderboard, get_oos_curve


def _empty_figure(annotation: str) -> go.Figure:
    fig = go.Figure()
    fig.update_layout(
        annotations=[
            {
                "text": annotation,
                "xref": "paper",
                "yref": "paper",
                "x": 0.5,
                "y": 0.5,
                "showarrow": False,
                "font": {"size": 14},
            }
        ],
        xaxis={"visible": False},
        yaxis={"visible": False},
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

    t_vals = trades_pivot.to_numpy(dtype=float)
    t_min = np.nanmin(t_vals)
    t_max = np.nanmax(t_vals)
    if t_max == t_min:
        opacity = np.full(trades_pivot.shape, 1.0)
    else:
        opacity = 0.4 + 0.6 * (trades_pivot - t_min) / (t_max - t_min)

    fig = go.Figure(
        data=go.Heatmap(
            z=sqn_pivot.values,
            x=sqn_pivot.columns.tolist(),
            y=sqn_pivot.index.tolist(),
            customdata=trades_pivot.values,
            opacity=opacity.values,
            colorscale="RdYlGn",
            zmid=1.5,
            hovertemplate="SQN: %{z:.2f}<br>Trades: %{customdata}<extra></extra>",
        )
    )
    fig.update_layout(
        title="OOS SQN100 Heatmap",
        xaxis_title="Granularity",
        yaxis_title="Instrument",
    )
    return fig


def register(app) -> None:
    @app.callback(
        Output("leaderboard-store", "data"),
        Output("last-updated", "children"),
        Input("refresh-button", "n_clicks"),
        prevent_initial_call=False,
    )
    def refresh_store(_n_clicks):
        df = get_leaderboard()
        timestamp = datetime.utcnow().strftime(
            "Last updated: %Y-%m-%d %H:%M UTC"
        )
        return df.to_json(orient="records", date_format="iso"), timestamp

    @app.callback(
        Output("leaderboard-table", "data"),
        Output("sqn-heatmap", "figure"),
        Input("leaderboard-store", "data"),
        Input("meta-filter", "value"),
        Input("granularity-filter", "value"),
    )
    def render_dashboard(data, meta_filter, granularity_filter):
        if not data:
            empty = _empty_figure("No data — sweep may still be running.")
            return [], empty

        df = pd.read_json(io.StringIO(data), orient="records")

        if meta_filter != "ALL":
            if meta_filter == "NULL":
                df = df[df["meta_status"].isna()]
            else:
                df = df[df["meta_status"] == meta_filter]

        if granularity_filter != "ALL":
            df = df[df["granularity"] == granularity_filter]

        if df.empty:
            return [], _empty_figure("No data — sweep may still be running.")

        table_data = df.to_dict("records")
        return table_data, _build_heatmap(df)

    @app.callback(
        Output("equity-curve", "figure"),
        Input("leaderboard-table", "selected_rows"),
        Input("leaderboard-table", "data"),
    )
    def render_equity_curve(selected_rows, table_data):
        if not selected_rows or not table_data:
            return _empty_figure("Select a row to view equity curve")

        row = table_data[selected_rows[0]]

        oos_df = get_oos_curve(
            row["instrument"],
            row["granularity"],
            row["anchor"],
            row["direction"],
            row["timeout_bars"],
        )

        if oos_df.empty:
            return _empty_figure("No promoted windows found")

        x, y = build_equity_curve(oos_df)
        if len(x) == 0:
            return _empty_figure("No promoted windows found")

        fig = go.Figure(
            go.Scatter(x=x, y=y, mode="lines", connectgaps=False)
        )
        fig.update_layout(
            title=(
                f"{row['instrument']} {row['granularity']} — OOS Equity"
            ),
            xaxis_title="Bars",
            yaxis_title="Cumulative R",
            hovermode="x unified",
        )
        return fig
