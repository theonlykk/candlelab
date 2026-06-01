from dash import dcc, html
from dash import dash_table


def get_layout() -> html.Div:
    return html.Div(
        [
            html.Div(
                [
                    html.H2("CandleLab Sweep Dashboard"),
                    html.Span(id="last-updated"),
                    html.Button(
                        "Refresh Data",
                        id="refresh-button",
                        n_clicks=0,
                    ),
                ],
                style={
                    "display": "flex",
                    "alignItems": "center",
                    "gap": "1rem",
                    "marginBottom": "1rem",
                },
            ),
            dcc.Store(id="leaderboard-store", storage_type="memory"),
            html.Div(
                [
                    dcc.Dropdown(
                        id="meta-filter",
                        options=[
                            {"label": "ALL", "value": "ALL"},
                            {"label": "GREEN", "value": "GREEN"},
                            {"label": "AMBER", "value": "AMBER"},
                            {"label": "RED", "value": "RED"},
                            {"label": "NULL", "value": "NULL"},
                        ],
                        value="ALL",
                        clearable=False,
                        style={"width": "200px"},
                    ),
                    dcc.Dropdown(
                        id="granularity-filter",
                        options=[
                            {"label": "ALL", "value": "ALL"},
                            {"label": "M30", "value": "M30"},
                            {"label": "H1", "value": "H1"},
                        ],
                        value="ALL",
                        clearable=False,
                        style={"width": "200px"},
                    ),
                ],
                style={
                    "display": "flex",
                    "gap": "1rem",
                    "marginBottom": "1rem",
                },
            ),
            dcc.Graph(id="sqn-heatmap"),
            dash_table.DataTable(
                id="leaderboard-table",
                columns=[
                    {"name": "Instrument", "id": "instrument"},
                    {"name": "Granularity", "id": "granularity"},
                    {"name": "SQN", "id": "oos_sqn100"},
                    {"name": "Win Rate", "id": "oos_win_rate"},
                    {"name": "Trades", "id": "oos_trade_count"},
                    {"name": "Mean R", "id": "oos_mean_r"},
                    {"name": "Meta Status", "id": "meta_status"},
                ],
                row_selectable="single",
                style_data_conditional=[
                    {
                        "if": {"filter_query": '{meta_status} = "GREEN"'},
                        "backgroundColor": "#e8f5e9",
                    },
                    {
                        "if": {"filter_query": '{meta_status} = "AMBER"'},
                        "backgroundColor": "#fff8e1",
                    },
                    {
                        "if": {"filter_query": '{meta_status} = "RED"'},
                        "backgroundColor": "#ffebee",
                    },
                    {
                        "if": {"filter_query": "{meta_status} is blank"},
                        "backgroundColor": "#f5f5f5",
                    },
                ],
                style_table={"overflowX": "auto"},
                style_cell={"textAlign": "left", "padding": "8px"},
            ),
            html.H4("OOS Equity Curve (Promoted Windows Only)"),
            dcc.Graph(id="equity-curve"),
        ],
        style={"padding": "1.5rem", "fontFamily": "sans-serif"},
    )
