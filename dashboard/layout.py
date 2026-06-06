from dash import dcc, html
from dash import dash_table

STATUS_COLORS = {
    "GREEN":        {"bg": "#e8f5e9", "text": "#1b5e20"},
    "RED_SPIKE":    {"bg": "#ffebee", "text": "#b71c1c"},
    "RED_WEAK_SQN": {"bg": "#fff8e1", "text": "#f57f17"},
    "RED_STALE":    {"bg": "#f5f5f5", "text": "#9e9e9e"},
    "RED_THIN":     {"bg": "#f5f5f5", "text": "#9e9e9e"},
    "RED":          {"bg": "#fafafa", "text": "#bdbdbd"},
}

LEADERBOARD_COLUMNS = [
    {"name": "Instrument", "id": "instrument"},
    {"name": "Setup",      "id": "setup"},
    {"name": "Dir",        "id": "direction"},
    {"name": "T/O",        "id": "timeout_bars"},
    {"name": "SQN",        "id": "oos_sqn100",
     "type": "numeric", "format": {"specifier": ".2f"}},
    {"name": "Mean R",     "id": "oos_mean_r",
     "type": "numeric", "format": {"specifier": ".3f"}},
    {"name": "Trades",     "id": "oos_n_trades",  "type": "numeric"},
    {"name": "Windows",    "id": "n_windows_promoted", "type": "numeric"},
    {"name": "NQS",        "id": "neighborhood_quality_score",
     "type": "numeric", "format": {"specifier": ".2f"}},
    {"name": "Status",     "id": "meta_status"},
    {"name": "Recency",    "id": "sparkline_html"},
]

STYLE_DATA_CONDITIONAL = [
    {"if": {"filter_query": '{meta_status} = "GREEN"'},
     "backgroundColor": STATUS_COLORS["GREEN"]["bg"],
     "fontWeight": "600"},
    {"if": {"filter_query": '{meta_status} = "RED_SPIKE"'},
     "backgroundColor": STATUS_COLORS["RED_SPIKE"]["bg"]},
    {"if": {"filter_query": '{meta_status} = "RED_WEAK_SQN"'},
     "backgroundColor": STATUS_COLORS["RED_WEAK_SQN"]["bg"]},
    {"if": {"filter_query": '{meta_status} = "RED_STALE"'},
     "backgroundColor": STATUS_COLORS["RED_STALE"]["bg"],
     "color": "#9e9e9e"},
    {"if": {"filter_query": '{meta_status} = "RED_THIN"'},
     "backgroundColor": STATUS_COLORS["RED_THIN"]["bg"],
     "color": "#9e9e9e"},
    {"if": {"filter_query": '{meta_status} = "RED"'},
     "backgroundColor": STATUS_COLORS["RED"]["bg"],
     "color": "#bdbdbd"},
]

_DOT = {
    "PASS":    ("●", "#4caf50"),
    "FAIL":    ("●", "#c62828"),
    "PENDING": ("◐", "#f9a825"),
    "STALE":   ("○", "#bdbdbd"),
}


def _gate_dot(status: str) -> html.Span:
    char, color = _DOT.get(status, ("○", "#bdbdbd"))
    return html.Span(char, style={"color": color, "fontSize": "1rem"})


def regime_panel(gates: dict, instrument: str,
                 direction: str) -> html.Div:
    ts       = gates.get("data_ts")
    ts_str   = ts.strftime("%Y-%m-%d %H:%M UTC") if ts else "No data"
    is_stale = gates.get("is_stale", True)

    stale_label = (
        html.Span(" ⚠ STALE",
                  style={"color": "#f9a825", "fontSize": "0.75rem"})
        if is_stale else
        html.Span(f" as of {ts_str}",
                  style={"color": "#9e9e9e", "fontSize": "0.75rem"})
    )

    gate_labels = [
        ("BBW",      gates.get("bbw",      "STALE")),
        ("ADX",      gates.get("adx",      "STALE")),
        ("MA 200",   gates.get("ma_200",   "STALE")),
        ("H4 EMA",   gates.get("h4_ema",   "STALE")),
        ("Z-Spread", gates.get("z_spread", "PENDING")),
    ]

    return html.Div(
        style={
            "background": "#ffffff",
            "borderRadius": "6px",
            "border": "1px solid #e0e0e0",
            "padding": "0.75rem 1rem",
            "marginBottom": "0.5rem",
        },
        children=[
            html.Div(
                style={"display": "flex", "alignItems": "center",
                       "marginBottom": "0.5rem"},
                children=[
                    html.Strong(
                        f"{instrument} {direction.upper()}",
                        style={"fontSize": "0.9rem",
                               "marginRight": "0.5rem"},
                    ),
                    stale_label,
                ],
            ),
            html.Div(
                style={"display": "flex", "gap": "1.5rem"},
                children=[
                    html.Div(
                        style={"textAlign": "center"},
                        children=[
                            _gate_dot(status),
                            html.Div(label, style={
                                "fontSize": "0.7rem",
                                "color": "#757575",
                                "marginTop": "2px",
                            }),
                        ],
                    )
                    for label, status in gate_labels
                ],
            ),
        ],
    )


def get_layout() -> html.Div:
    return html.Div(
        style={
            "padding": "1.5rem 2rem",
            "fontFamily": (
                "system-ui, -apple-system, Segoe UI, "
                "Roboto, sans-serif"
            ),
            "background": "#f5f5f5",
            "minHeight": "100vh",
            "color": "#212121",
        },
        children=[
            # ── Header ──────────────────────────────────────────────
            html.Div(
                style={
                    "background": "#ffffff",
                    "borderRadius": "8px",
                    "padding": "1rem 1.5rem",
                    "marginBottom": "1rem",
                    "display": "flex",
                    "alignItems": "center",
                    "justifyContent": "space-between",
                    "boxShadow": "0 1px 3px rgba(0,0,0,0.08)",
                },
                children=[
                    html.Div([
                        html.H2(
                            "CandleLab Sweep Dashboard",
                            style={"margin": "0",
                                   "fontSize": "1.25rem",
                                   "fontWeight": "700"},
                        ),
                        html.Span(
                            id="last-updated",
                            style={"fontSize": "0.8rem",
                                   "color": "#757575",
                                   "display": "block"},
                        ),
                    ]),
                    html.Div([
                        html.Span(
                            id="summary-counts",
                            style={"fontSize": "0.85rem",
                                   "color": "#555",
                                   "marginRight": "1rem"},
                        ),
                        html.Button(
                            "↻ Refresh",
                            id="refresh-button",
                            n_clicks=0,
                            style={
                                "background": "#4caf50",
                                "color": "#fff",
                                "border": "none",
                                "borderRadius": "4px",
                                "padding": "6px 16px",
                                "cursor": "pointer",
                                "fontWeight": "600",
                                "fontSize": "0.875rem",
                            },
                        ),
                    ], style={"display": "flex",
                              "alignItems": "center"}),
                ],
            ),

            dcc.Store(id="leaderboard-store", storage_type="memory"),

            # ── Live Regime Panel ────────────────────────────────────
            html.Div(
                id="regime-panel-container",
                style={"marginBottom": "1rem"},
            ),

            # ── Heuristic disclaimer (Gemini ADR-104) ───────────────
            html.Div(
                style={
                    "background": "#fffde7",
                    "borderRadius": "6px",
                    "padding": "0.5rem 1rem",
                    "marginBottom": "1rem",
                    "fontSize": "0.78rem",
                    "color": "#757575",
                    "borderLeft": "3px solid #f9a825",
                },
                children=(
                    "Note: Live Regime Gates currently evaluate against "
                    "static SAFE heuristics (e.g. ADX > 25). "
                    "Dynamic, IS-calibrated thresholds will populate "
                    "post-V9 migration."
                ),
            ),

            # ── RED summary bar ──────────────────────────────────────
            html.Div(
                id="red-summary-bar",
                style={
                    "background": "#ffffff",
                    "borderRadius": "6px",
                    "padding": "0.6rem 1.25rem",
                    "marginBottom": "1rem",
                    "fontSize": "0.82rem",
                    "color": "#757575",
                    "boxShadow": "0 1px 2px rgba(0,0,0,0.06)",
                },
            ),

            # ── Filters ─────────────────────────────────────────────
            html.Div(
                style={"display": "flex", "gap": "1rem",
                       "marginBottom": "1rem",
                       "alignItems": "center"},
                children=[
                    html.Label(
                        "Status:",
                        style={"fontSize": "0.85rem",
                               "color": "#555"},
                    ),
                    dcc.Dropdown(
                        id="meta-filter",
                        options=[
                            {"label": "ALL",          "value": "ALL"},
                            {"label": "GREEN",        "value": "GREEN"},
                            {"label": "RED_SPIKE",    "value": "RED_SPIKE"},
                            {"label": "RED_WEAK_SQN", "value": "RED_WEAK_SQN"},
                            {"label": "RED_STALE",    "value": "RED_STALE"},
                            {"label": "RED_THIN",     "value": "RED_THIN"},
                            {"label": "RED",          "value": "RED"},
                            {"label": "NULL",         "value": "NULL"},
                        ],
                        value="ALL",
                        clearable=False,
                        style={"width": "180px"},
                    ),
                    html.Label(
                        "Timeframe:",
                        style={"fontSize": "0.85rem",
                               "color": "#555"},
                    ),
                    dcc.Dropdown(
                        id="granularity-filter",
                        options=[
                            {"label": "ALL", "value": "ALL"},
                            {"label": "M30", "value": "M30"},
                            {"label": "H1",  "value": "H1"},
                        ],
                        value="ALL",
                        clearable=False,
                        style={"width": "120px"},
                    ),
                ],
            ),

            # ── SQN Heatmap ──────────────────────────────────────────
            html.Div(
                style={
                    "background": "#ffffff",
                    "borderRadius": "8px",
                    "padding": "0.5rem",
                    "marginBottom": "1rem",
                    "boxShadow": "0 1px 3px rgba(0,0,0,0.08)",
                },
                children=[dcc.Graph(id="sqn-heatmap")],
            ),

            # ── Leaderboard ──────────────────────────────────────────
            html.Div(
                style={
                    "background": "#ffffff",
                    "borderRadius": "8px",
                    "padding": "1rem",
                    "marginBottom": "1rem",
                    "boxShadow": "0 1px 3px rgba(0,0,0,0.08)",
                    "overflowX": "auto",
                },
                children=[
                    html.H4(
                        "Leaderboard",
                        style={"margin": "0 0 0.75rem",
                               "fontSize": "0.95rem",
                               "fontWeight": "600"},
                    ),
                    dash_table.DataTable(
                        id="leaderboard-table",
                        columns=LEADERBOARD_COLUMNS,
                        row_selectable="single",
                        dangerously_allow_html=True,
                        sort_action="native",
                        style_table={"overflowX": "auto"},
                        style_header={
                            "backgroundColor": "#fafafa",
                            "fontWeight": "600",
                            "fontSize": "0.8rem",
                            "color": "#555",
                            "borderBottom": "2px solid #e0e0e0",
                            "padding": "8px 10px",
                        },
                        style_cell={
                            "textAlign": "left",
                            "padding": "7px 10px",
                            "fontSize": "0.82rem",
                            "border": "none",
                            "borderBottom": "1px solid #f0f0f0",
                            "whiteSpace": "normal",
                        },
                        style_data_conditional=STYLE_DATA_CONDITIONAL,
                        page_size=50,
                    ),
                ],
            ),

            # ── OOS Equity Curve ─────────────────────────────────────
            html.Div(
                style={
                    "background": "#ffffff",
                    "borderRadius": "8px",
                    "padding": "1rem",
                    "boxShadow": "0 1px 3px rgba(0,0,0,0.08)",
                },
                children=[
                    html.H4(
                        "OOS Equity Curve",
                        style={"margin": "0 0 0.5rem",
                               "fontSize": "0.95rem",
                               "fontWeight": "600"},
                    ),
                    dcc.Graph(id="equity-curve"),
                ],
            ),
        ],
    )
