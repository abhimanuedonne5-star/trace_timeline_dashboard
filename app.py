import os
import pandas as pd
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output, State, callback_context
from databricks.sdk import WorkspaceClient

# ── Databricks SDK client (auto-credentialed inside Databricks Apps) ──────────
w = WorkspaceClient()

WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID")
CATALOG      = os.environ.get("CATALOG", "your_catalog")
SCHEMA       = os.environ.get("SCHEMA",  "your_schema")
TABLE        = os.environ.get("TABLE",   "trace_events")

FULL_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"


def run_query(sql: str) -> pd.DataFrame:
    result = w.statement_execution.execute_statement(
        warehouse_id  = WAREHOUSE_ID,
        statement     = sql,
        wait_timeout  = "50s",
    )
    if not result.manifest or not result.manifest.schema:
        return pd.DataFrame()
    columns = [col.name for col in result.manifest.schema.columns]
    rows    = result.result.data_array or []
    df      = pd.DataFrame(rows, columns=columns)
    return df.apply(pd.to_numeric, errors="ignore")


# ── Color palette ─────────────────────────────────────────────────────────────
PALETTE = [
    "#00d4ff", "#a78bfa", "#34d399", "#fbbf24", "#f87171",
    "#f472b6", "#60a5fa", "#2dd4bf", "#fb923c", "#c084fc",
    "#22d3ee", "#a3e635", "#ff6b6b", "#4ade80", "#e879f9",
    "#38bdf8", "#facc15", "#86efac", "#fda4af", "#818cf8",
]

COMP_COLORS = {}
_ci = [0]

def get_color(comp: str) -> str:
    if comp not in COMP_COLORS:
        COMP_COLORS[comp] = PALETTE[_ci[0] % len(PALETTE)]
        _ci[0] += 1
    return COMP_COLORS[comp]


# ── Build Gantt figure ────────────────────────────────────────────────────────
def build_gantt(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return go.Figure().update_layout(
            template   = "plotly_dark",
            paper_bgcolor = "#070810",
            plot_bgcolor  = "#070810",
            title = dict(text="No data for selected trace", font=dict(color="#6b7280")),
        )

    # Sort by start_offset_ms so rows appear in execution order
    df = df.sort_values("start_offset_ms").reset_index(drop=True)

    # Label for Y axis: component · operation
    df["label"] = df.apply(
        lambda r: f"{r['component']}  ·  {r['operation']}" if r["operation"] else r["component"],
        axis=1
    )

    fig = go.Figure()

    # Draw bars using shapes + scatter for hover
    seen_comps = {}
    for _, row in df.iterrows():
        col        = get_color(row["component"])
        is_root    = row["operation_type"] in ("REQUEST_END", "PREFILL_LATENCY")
        bar_height = 0.25 if is_root else 0.6
        opacity    = 0.30 if is_root else 0.90

        show_legend = row["component"] not in seen_comps
        seen_comps[row["component"]] = True

        # Invisible scatter for hover tooltip
        fig.add_trace(go.Scatter(
            x          = [row["start_offset_ms"] + row["duration_ms"] / 2],
            y          = [row["label"]],
            mode       = "markers",
            marker     = dict(size=0, opacity=0),
            name       = row["component"],
            legendgroup= row["component"],
            showlegend = show_legend,
            hovertemplate = (
                f"<b style='color:{col}'>{row['component']}</b><br>"
                f"<span style='color:#9ca3af'>Operation:</span>  {row['operation'] or '—'}<br>"
                f"<span style='color:#9ca3af'>Type:</span>       {row['operation_type']}<br>"
                f"<span style='color:#9ca3af'>Start offset:</span> {row['start_offset_ms']:.3f} ms<br>"
                f"<span style='color:#9ca3af'>Duration:</span>   {row['duration_ms']:.4f} ms<br>"
                f"<span style='color:#9ca3af'>End offset:</span>  {row['start_offset_ms'] + row['duration_ms']:.3f} ms"
                "<extra></extra>"
            ),
        ))

        # Actual bar as shape
        y_idx     = df[df["label"] == row["label"]].index[0]
        y_center  = row["label"]
        fig.add_shape(
            type      = "rect",
            x0        = row["start_offset_ms"],
            x1        = max(row["start_offset_ms"] + row["duration_ms"],
                            row["start_offset_ms"] + 0.5),   # min visible width
            y0        = y_idx - bar_height / 2,
            y1        = y_idx + bar_height / 2,
            fillcolor = col,
            opacity   = opacity,
            line      = dict(width=0),
            layer     = "above",
        )

    # Y axis labels
    y_labels = df["label"].tolist()

    total_ms   = df["start_offset_ms"].max() + df["duration_ms"].max()
    row_height = max(34, min(52, 800 // max(len(df), 1)))
    fig_height = max(480, len(df) * row_height + 140)

    fig.update_layout(
        template      = "plotly_dark",
        paper_bgcolor = "#070810",
        plot_bgcolor  = "#0d0f1a",
        height        = fig_height,
        margin        = dict(l=0, r=40, t=40, b=60),
        font          = dict(family="JetBrains Mono, monospace", size=11, color="#e2e8f0"),
        title         = dict(
            text      = f"Trace Timeline  —  {len(df)} events  |  total: {total_ms:.1f} ms",
            font      = dict(size=14, color="#94a3b8"),
            x         = 0,
            xref      = "paper",
        ),
        xaxis = dict(
            title      = "Offset from first event (ms)",
            gridcolor  = "#1c2035",
            zeroline   = False,
            tickfont   = dict(size=10, color="#6b7280"),
            title_font = dict(size=11, color="#6b7280"),
            rangeslider= dict(visible=True, thickness=0.04, bgcolor="#0d0f1a"),
        ),
        yaxis = dict(
            tickmode   = "array",
            tickvals   = list(range(len(y_labels))),
            ticktext   = y_labels,
            tickfont   = dict(size=10, color="#cbd5e1"),
            gridcolor  = "#1c2035",
            autorange  = "reversed",
        ),
        legend = dict(
            bgcolor     = "#0d0f1a",
            bordercolor = "#1c2035",
            borderwidth = 1,
            font        = dict(size=10, color="#94a3b8"),
            title       = dict(text="Component", font=dict(color="#64748b")),
        ),
        hoverlabel = dict(
            bgcolor   = "#111827",
            bordercolor= "#374151",
            font      = dict(family="JetBrains Mono, monospace", size=11),
        ),
        dragmode = "zoom",
    )

    return fig


# ── Dash App ──────────────────────────────────────────────────────────────────
app = Dash(
    __name__,
    title        = "Trace Timeline",
    suppress_callback_exceptions = True,
)

# ── Styles ─────────────────────────────────────────────────────────────────────
DROPDOWN_STYLE = dict(
    backgroundColor = "#0d0f1a",
    color           = "#e2e8f0",
    border          = "1px solid #1e2a3a",
    borderRadius    = "6px",
    fontFamily      = "JetBrains Mono, monospace",
    fontSize        = "12px",
    width           = "100%",
)

LABEL_STYLE = dict(
    fontSize   = "9px",
    fontFamily = "JetBrains Mono, monospace",
    letterSpacing = "2px",
    textTransform = "uppercase",
    color      = "#475569",
    marginBottom = "6px",
    display    = "block",
)

STAT_CARD = lambda label, value_id: html.Div([
    html.Span(label, style=dict(fontSize="9px", color="#475569",
                                letterSpacing="2px", textTransform="uppercase",
                                fontFamily="JetBrains Mono, monospace",
                                display="block", marginBottom="4px")),
    html.Span("—", id=value_id, style=dict(fontSize="18px", fontWeight="700",
                                           color="#00d4ff",
                                           fontFamily="JetBrains Mono, monospace")),
], style=dict(background="#0d0f1a", border="1px solid #1c2035",
              borderRadius="6px", padding="14px 20px", minWidth="120px"))


# ── Layout ────────────────────────────────────────────────────────────────────
app.layout = html.Div(
    style=dict(
        background   = "#070810",
        minHeight    = "100vh",
        fontFamily   = "JetBrains Mono, monospace",
        padding      = "28px 32px",
        color        = "#e2e8f0",
    ),
    children=[

        # ── Header ────────────────────────────────────────────────────────────
        html.Div([
            html.H1("Trace Execution Timeline",
                    style=dict(fontFamily="'Syne', sans-serif", fontSize="26px",
                               fontWeight="800", letterSpacing="-0.5px",
                               color="#e2e8f0", margin="0 0 4px 0")),
            html.P("Select a User → Conversation → Trace to explore component execution",
                   style=dict(fontSize="11px", color="#475569",
                              letterSpacing="1px", margin="0 0 28px 0")),
        ]),

        # ── Filters row ───────────────────────────────────────────────────────
        html.Div([

            # User ID
            html.Div([
                html.Label("User ID", style=LABEL_STYLE),
                dcc.Dropdown(
                    id          = "dd-user",
                    placeholder = "Select a user...",
                    clearable   = True,
                    style       = DROPDOWN_STYLE,
                    className   = "dark-dropdown",
                ),
            ], style=dict(flex="1", minWidth="200px")),

            # Arrow
            html.Div("→", style=dict(color="#1e3a5f", fontSize="20px",
                                     alignSelf="flex-end", paddingBottom="8px",
                                     paddingLeft="8px", paddingRight="8px")),

            # Conversation ID
            html.Div([
                html.Label("Conversation ID", style=LABEL_STYLE),
                dcc.Dropdown(
                    id          = "dd-conv",
                    placeholder = "Select a conversation...",
                    clearable   = True,
                    disabled    = True,
                    style       = DROPDOWN_STYLE,
                    className   = "dark-dropdown",
                ),
            ], style=dict(flex="1", minWidth="220px")),

            # Arrow
            html.Div("→", style=dict(color="#1e3a5f", fontSize="20px",
                                     alignSelf="flex-end", paddingBottom="8px",
                                     paddingLeft="8px", paddingRight="8px")),

            # Trace ID
            html.Div([
                html.Label("Trace ID", style=LABEL_STYLE),
                dcc.Dropdown(
                    id          = "dd-trace",
                    placeholder = "Select a trace...",
                    clearable   = True,
                    disabled    = True,
                    style       = DROPDOWN_STYLE,
                    className   = "dark-dropdown",
                ),
            ], style=dict(flex="1", minWidth="300px")),

        ], style=dict(display="flex", alignItems="flex-start", gap="4px",
                      marginBottom="24px", flexWrap="wrap")),

        # ── Stat cards ────────────────────────────────────────────────────────
        html.Div([
            STAT_CARD("Events",     "stat-events"),
            STAT_CARD("Total Time", "stat-total"),
            STAT_CARD("Components", "stat-comps"),
            STAT_CARD("Max Single", "stat-max"),
        ], id="stat-row",
           style=dict(display="none", gap="12px", marginBottom="20px",
                      flexWrap="wrap")),

        # ── Loading + Chart ───────────────────────────────────────────────────
        html.Div(
            id    = "chart-area",
            style = dict(
                background   = "#0d0f1a",
                border       = "1px solid #1c2035",
                borderRadius = "8px",
                minHeight    = "300px",
                display      = "flex",
                alignItems   = "center",
                justifyContent = "center",
            ),
            children=[
                html.Div([
                    html.Div("◎", style=dict(fontSize="32px", color="#1e3a5f",
                                             textAlign="center", marginBottom="12px")),
                    html.P("Select User → Conversation → Trace to load the timeline",
                           style=dict(color="#374151", fontSize="12px",
                                      textAlign="center",
                                      fontFamily="JetBrains Mono, monospace")),
                ])
            ]
        ),

        # ── Hidden stores ─────────────────────────────────────────────────────
        dcc.Store(id="store-users"),
        dcc.Store(id="store-data"),

        # Google Fonts
        html.Link(rel="stylesheet",
                  href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;700&family=Syne:wght@700;800&display=swap"),
    ]
)


# ── Callbacks ─────────────────────────────────────────────────────────────────

# 1. Load user list on startup
@app.callback(
    Output("dd-user", "options"),
    Input("dd-user",  "id"),          # fires once on load
)
def load_users(_):
    try:
        df = run_query(f"""
            SELECT DISTINCT user_id
            FROM {FULL_TABLE}
            WHERE user_id IS NOT NULL
            ORDER BY user_id
        """)
        return [{"label": uid, "value": uid} for uid in df["user_id"].tolist()]
    except Exception as e:
        print(f"Error loading users: {e}")
        return []


# 2. User selected → load conversations
@app.callback(
    Output("dd-conv",  "options"),
    Output("dd-conv",  "disabled"),
    Output("dd-conv",  "value"),
    Input("dd-user",   "value"),
)
def load_conversations(user_id):
    if not user_id:
        return [], True, None
    try:
        df = run_query(f"""
            SELECT DISTINCT conversation_id
            FROM {FULL_TABLE}
            WHERE user_id = '{user_id}'
              AND conversation_id IS NOT NULL
            ORDER BY conversation_id
        """)
        opts = [{"label": cid, "value": cid} for cid in df["conversation_id"].tolist()]
        return opts, False, None
    except Exception as e:
        print(f"Error loading conversations: {e}")
        return [], True, None


# 3. Conversation selected → load traces
@app.callback(
    Output("dd-trace",  "options"),
    Output("dd-trace",  "disabled"),
    Output("dd-trace",  "value"),
    Input("dd-conv",    "value"),
    State("dd-user",    "value"),
)
def load_traces(conv_id, user_id):
    if not conv_id or not user_id:
        return [], True, None
    try:
        df = run_query(f"""
            SELECT
                trace_id,
                MIN(timestamp)     AS first_event,
                COUNT(*)           AS event_count,
                SUM(duration_ms)   AS total_ms
            FROM {FULL_TABLE}
            WHERE user_id         = '{user_id}'
              AND conversation_id = '{conv_id}'
              AND trace_id IS NOT NULL
            GROUP BY trace_id
            ORDER BY first_event DESC
        """)
        opts = [
            {
                "label": f"{row['trace_id']}  ({int(row['event_count'])} events · {row['total_ms']:.0f} ms)",
                "value": row["trace_id"],
            }
            for _, row in df.iterrows()
        ]
        return opts, False, None
    except Exception as e:
        print(f"Error loading traces: {e}")
        return [], True, None


# 4. Trace selected → render timeline
@app.callback(
    Output("chart-area",  "children"),
    Output("stat-row",    "style"),
    Output("stat-events", "children"),
    Output("stat-total",  "children"),
    Output("stat-comps",  "children"),
    Output("stat-max",    "children"),
    Input("dd-trace",     "value"),
    State("dd-user",      "value"),
    State("dd-conv",      "value"),
)
def render_timeline(trace_id, user_id, conv_id):
    hidden = dict(display="none")
    empty  = ("—", "—", "—", "—")

    if not trace_id:
        placeholder = html.Div([
            html.Div("◎", style=dict(fontSize="32px", color="#1e3a5f",
                                     textAlign="center", marginBottom="12px")),
            html.P("Select a Trace ID to render the timeline",
                   style=dict(color="#374151", fontSize="12px",
                               textAlign="center",
                               fontFamily="JetBrains Mono, monospace")),
        ])
        return placeholder, hidden, *empty

    try:
        df = run_query(f"""
            SELECT
                component,
                operation,
                operation_type,
                source,
                CAST(duration_ms AS DOUBLE)   AS duration_ms,

                -- start_time = timestamp - duration_ms
                CAST(
                    (UNIX_MICROS(timestamp) - CAST(duration_ms * 1000 AS BIGINT))
                AS DOUBLE) / 1000.0           AS start_epoch_ms,

                -- offset from first event in this trace
                CAST(
                    (
                        (UNIX_MICROS(timestamp) - CAST(duration_ms * 1000 AS BIGINT))
                        - MIN(UNIX_MICROS(timestamp) - CAST(duration_ms * 1000 AS BIGINT))
                            OVER (PARTITION BY trace_id)
                    )
                AS DOUBLE) / 1000.0           AS start_offset_ms

            FROM {FULL_TABLE}
            WHERE trace_id         = '{trace_id}'
              AND user_id          = '{user_id}'
              AND conversation_id  = '{conv_id}'
            ORDER BY start_offset_ms ASC
        """)

        if df.empty:
            return (
                html.P("No events found for this trace.",
                       style=dict(color="#6b7280", textAlign="center", padding="40px")),
                hidden, *empty
            )

        # Fill missing component names
        df["component"] = df["component"].fillna(df["source"]).fillna("UNKNOWN")
        df["operation"] = df["operation"].fillna("")

        fig        = build_gantt(df)
        total_ms   = df["start_offset_ms"].max() + df["duration_ms"].max()
        max_comp   = df.loc[df["duration_ms"].idxmax()]
        max_label  = f"{max_comp['component']} ({max_comp['duration_ms']:.1f}ms)"
        n_comps    = df["component"].nunique()

        def fmt_ms(ms):
            return f"{ms/1000:.3f}s" if ms >= 1000 else f"{ms:.2f}ms"

        stats_style = dict(display="flex", gap="12px",
                           marginBottom="20px", flexWrap="wrap")

        chart = dcc.Graph(
            figure = fig,
            config = dict(
                displayModeBar = True,
                modeBarButtonsToRemove = ["select2d", "lasso2d"],
                displaylogo = False,
                toImageButtonOptions = dict(
                    format="png", filename="trace_timeline", scale=2
                ),
            ),
            style = dict(width="100%"),
        )

        return (
            chart,
            stats_style,
            str(len(df)),
            fmt_ms(total_ms),
            str(n_comps),
            max_label,
        )

    except Exception as e:
        print(f"Error rendering timeline: {e}")
        err = html.P(f"Error: {str(e)}",
                     style=dict(color="#ef4444", padding="20px",
                                fontFamily="JetBrains Mono, monospace",
                                fontSize="11px"))
        return err, hidden, *empty


if __name__ == "__main__":
    port = int(os.environ.get("DATABRICKS_APP_PORT", 8050))
    app.run(host="0.0.0.0", port=port, debug=False)
