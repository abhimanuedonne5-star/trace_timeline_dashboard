import os
import pandas as pd
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output, State, callback_context, ALL, MATCH
from databricks.sdk import WorkspaceClient
import json

# ── Databricks SDK ─────────────────────────────────────────────────────────────
# WorkspaceClient() inside a Databricks App uses ambient OAuth — no token needed
w = WorkspaceClient()

CATALOG      = os.environ.get("CATALOG",      "dev_omni")
SCHEMA       = os.environ.get("SCHEMA",       "dev_omni_gold")
TABLE        = os.environ.get("TABLE",        "trace_events")
WAREHOUSE_ID = os.environ.get("WAREHOUSE_ID", "2a6b5b84e8974695")

FULL_TABLE = f"`{CATALOG}`.`{SCHEMA}`.`{TABLE}`"


def run_query(sql: str) -> pd.DataFrame:
    resp = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=sql,
        wait_timeout="30s",
    )
    if not resp.result or not resp.result.data_array:
        return pd.DataFrame()
    cols = [c.name for c in resp.manifest.schema.columns]
    return pd.DataFrame(resp.result.data_array, columns=cols)


# ── Colors ────────────────────────────────────────────────────────────────────
PALETTE = [
    "#00d4ff","#a78bfa","#34d399","#fbbf24","#f87171",
    "#f472b6","#60a5fa","#2dd4bf","#fb923c","#c084fc",
    "#22d3ee","#a3e635","#ff6b6b","#4ade80","#e879f9",
    "#38bdf8","#facc15","#86efac","#fda4af","#818cf8",
]
_cc = {}
_ci = [0]

def get_color(comp: str) -> str:
    if comp not in _cc:
        _cc[comp] = PALETTE[_ci[0] % len(PALETTE)]
        _ci[0] += 1
    return _cc[comp]


# ── Gantt builder ─────────────────────────────────────────────────────────────
def build_gantt(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        fig = go.Figure()
        fig.update_layout(paper_bgcolor="#070810", plot_bgcolor="#070810",
                          font=dict(color="#374151"),
                          annotations=[dict(text="No data", x=0.5, y=0.5,
                                           showarrow=False, font=dict(color="#374151", size=14))])
        return fig

    df = df.copy()
    df["start_offset_ms"] = pd.to_numeric(df["start_offset_ms"], errors="coerce").fillna(0)
    df["duration_ms"]     = pd.to_numeric(df["duration_ms"],     errors="coerce").fillna(0)

    # Sort by start_time (start_offset_ms asc)
    df = df.sort_values("start_offset_ms").reset_index(drop=True)

    df["component"] = df["component"].fillna(df.get("source","UNKNOWN")).fillna("UNKNOWN")
    df["operation"] = df["operation"].fillna("")

    total_ms = (df["start_offset_ms"] + df["duration_ms"]).max()
    fig      = go.Figure()
    seen     = set()

    for i, row in df.iterrows():
        col     = get_color(row["component"])
        is_root = str(row.get("operation_type","")).upper() in ("REQUEST_END","PREFILL_LATENCY")
        bar_h   = 0.20 if is_root else 0.55
        opacity = 0.25 if is_root else 0.88
        show_lg = row["component"] not in seen
        seen.add(row["component"])

        dur_fmt   = f"{row['duration_ms']/1000:.3f}s" if row['duration_ms'] >= 1000 else f"{row['duration_ms']:.3f}ms"
        start_fmt = f"{row['start_offset_ms']:.3f}ms"
        end_fmt   = f"{row['start_offset_ms']+row['duration_ms']:.3f}ms"

        # Invisible scatter for hover
        fig.add_trace(go.Scatter(
            x=[row["start_offset_ms"] + row["duration_ms"] / 2],
            y=[i],
            mode="markers",
            marker=dict(size=1, opacity=0, color=col),
            name=row["component"],
            legendgroup=row["component"],
            showlegend=show_lg,
            hovertemplate=(
                f"<b style='color:{col};font-size:13px'>{row['component']}</b>"
                + (f"<br><span style='color:#6b7280;font-size:11px'>{row['operation']}</span>" if row['operation'] else "")
                + f"<br><br><span style='color:#475569'>Start  </span><b>{start_fmt}</b>"
                f"<br><span style='color:#475569'>Duration</span><b>{dur_fmt}</b>"
                f"<br><span style='color:#475569'>End    </span><b>{end_fmt}</b>"
                f"<br><span style='color:#475569'>Type   </span><span style='color:#94a3b8'>{row.get('operation_type','')}</span>"
                "<extra></extra>"
            ),
        ))

        # Bar shape
        min_w = max(row["duration_ms"], total_ms * 0.0008)
        fig.add_shape(
            type="rect",
            x0=row["start_offset_ms"], x1=row["start_offset_ms"] + min_w,
            y0=i - bar_h / 2,         y1=i + bar_h / 2,
            fillcolor=col, opacity=opacity,
            line=dict(width=0), layer="above",
        )

    n   = len(df)
    fh  = max(520, n * 40 + 160)

    # Y-axis tick: component (bold big) + operation (small muted) as HTML-like annotation
    # Plotly ticktext supports some HTML via textfont but for two-line we use custom annotations
    tick_labels = []
    for _, row in df.iterrows():
        comp = row["component"]
        op   = row["operation"]
        if op:
            tick_labels.append(f"<b style='color:#e2e8f0'>{comp}</b>")
        else:
            tick_labels.append(f"<b>{comp}</b>")

    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="#070810",
        plot_bgcolor="#0d0f1a",
        height=fh,
        margin=dict(l=0, r=28, t=48, b=56),
        font=dict(family="JetBrains Mono, monospace", size=11, color="#e2e8f0"),
        title=dict(
            text=(
                f"<span style='color:#334155'>Timeline</span>"
                f"  <span style='color:#1e3a5f'>|</span>"
                f"  <span style='color:#00d4ff'>{n} events</span>"
                f"  <span style='color:#1e3a5f'>|</span>"
                f"  <span style='color:#475569'>{total_ms:.2f} ms</span>"
            ),
            font=dict(size=13), x=0, xref="paper",
        ),
        xaxis=dict(
            title="Offset from first event (ms)",
            gridcolor="#1c2035", zeroline=False,
            tickfont=dict(size=10, color="#475569"),
            title_font=dict(size=10, color="#475569"),
            rangeslider=dict(visible=True, thickness=0.03, bgcolor="#09090f"),
        ),
        yaxis=dict(
            tickmode="array",
            tickvals=list(range(n)),
            ticktext=tick_labels,
            tickfont=dict(size=11, color="#cbd5e1"),
            gridcolor="#111520",
            autorange="reversed",
        ),
        legend=dict(
            bgcolor="#0a0b12", bordercolor="#1c2035", borderwidth=1,
            font=dict(size=10, color="#94a3b8"),
            title=dict(text="Component", font=dict(color="#334155", size=10)),
        ),
        hoverlabel=dict(
            bgcolor="#0d1117", bordercolor="#1e293b",
            font=dict(family="JetBrains Mono, monospace", size=11),
        ),
        dragmode="zoom",
    )

    # Add operation name as annotation (small, muted) next to each bar
    for i, row in df.iterrows():
        if row["operation"]:
            fig.add_annotation(
                x=0, y=i,
                xref="paper", yref="y",
                text=f"<span style='color:#475569;font-size:9px'> {row['operation']}</span>",
                showarrow=False,
                xanchor="right", yanchor="middle",
                font=dict(size=9, color="#475569",
                          family="JetBrains Mono, monospace"),
            )

    return fig


# ── App ────────────────────────────────────────────────────────────────────────
app = Dash(__name__, title="Trace Timeline", suppress_callback_exceptions=True)

# ── Styles ─────────────────────────────────────────────────────────────────────
BG      = "#070810"
SURFACE = "#0d0f1a"
BORDER  = "#1c2035"
TEXT    = "#e2e8f0"
MUTED   = "#475569"
ACCENT  = "#00d4ff"

LABEL_S = dict(fontSize="9px", fontFamily="JetBrains Mono, monospace",
               letterSpacing="2px", textTransform="uppercase",
               color=MUTED, marginBottom="6px", display="block")

DD_S = dict(backgroundColor=SURFACE, color=TEXT,
            border=f"1px solid #1e2a3a", borderRadius="6px",
            fontFamily="JetBrains Mono, monospace", fontSize="12px")

def stat_card(label, sid):
    return html.Div([
        html.Span(label, style=dict(fontSize="9px", color=MUTED, letterSpacing="2px",
                                    textTransform="uppercase",
                                    fontFamily="JetBrains Mono, monospace",
                                    display="block", marginBottom="4px")),
        html.Span("—", id=sid, style=dict(fontSize="18px", fontWeight="700",
                                          color=ACCENT,
                                          fontFamily="JetBrains Mono, monospace")),
    ], style=dict(background=SURFACE, border=f"1px solid {BORDER}",
                  borderRadius="6px", padding="12px 18px", minWidth="120px"))

# ── Tree node builders ─────────────────────────────────────────────────────────
def user_node(uid, expanded=False):
    return html.Div([
        html.Div(
            id={"type": "user-row", "id": uid},
            n_clicks=0,
            style=dict(display="flex", alignItems="center", gap="8px",
                       padding="7px 10px", cursor="pointer", borderRadius="5px",
                       background="transparent", userSelect="none"),
            children=[
                html.Span("▶" if not expanded else "▼",
                          id={"type": "user-arrow", "id": uid},
                          style=dict(color=ACCENT, fontSize="9px",
                                     transition="transform .15s", minWidth="10px")),
                html.Span("👤", style=dict(fontSize="13px")),
                html.Span(uid, style=dict(fontSize="12px", fontWeight="700",
                                          color="#cbd5e1",
                                          fontFamily="JetBrains Mono, monospace")),
            ],
        ),
        html.Div(id={"type": "user-children", "id": uid},
                 style=dict(display="block" if expanded else "none",
                            paddingLeft="22px")),
    ], style=dict(borderBottom=f"1px solid {BORDER}10"))


def conv_node(uid, cid, expanded=False):
    return html.Div([
        html.Div(
            id={"type": "conv-row", "id": f"{uid}||{cid}"},
            n_clicks=0,
            style=dict(display="flex", alignItems="center", gap="8px",
                       padding="6px 10px", cursor="pointer", borderRadius="5px",
                       userSelect="none"),
            children=[
                html.Span("▶" if not expanded else "▼",
                          id={"type": "conv-arrow", "id": f"{uid}||{cid}"},
                          style=dict(color="#a78bfa", fontSize="9px", minWidth="10px")),
                html.Span("💬", style=dict(fontSize="12px")),
                html.Span(cid, style=dict(fontSize="11px", fontWeight="600",
                                          color="#94a3b8",
                                          fontFamily="JetBrains Mono, monospace")),
            ],
        ),
        html.Div(id={"type": "conv-children", "id": f"{uid}||{cid}"},
                 style=dict(display="block" if expanded else "none",
                            paddingLeft="22px")),
    ])


def trace_node(uid, cid, tid, event_count=None, total_ms=None):
    label_extra = ""
    if event_count is not None:
        ms_str = f"{float(total_ms)/1000:.2f}s" if float(total_ms) >= 1000 else f"{float(total_ms):.0f}ms"
        label_extra = f"  {int(float(event_count))} evt · {ms_str}"
    return html.Div(
        id={"type": "trace-row", "id": f"{uid}||{cid}||{tid}"},
        n_clicks=0,
        style=dict(display="flex", alignItems="center", gap="8px",
                   padding="5px 10px", cursor="pointer", borderRadius="5px",
                   userSelect="none"),
        children=[
            html.Span("⬡", style=dict(color="#34d399", fontSize="10px", minWidth="10px")),
            html.Span("🔍", style=dict(fontSize="11px")),
            html.Div([
                html.Span(tid[:40] + ("…" if len(tid) > 40 else ""),
                          style=dict(fontSize="10px", fontWeight="600",
                                     color="#64748b",
                                     fontFamily="JetBrains Mono, monospace",
                                     display="block")),
                html.Span(label_extra,
                          style=dict(fontSize="9px", color="#334155",
                                     fontFamily="JetBrains Mono, monospace")),
            ]),
        ],
    )


# ── Layout ─────────────────────────────────────────────────────────────────────
app.layout = html.Div(
    style=dict(background=BG, minHeight="100vh",
               fontFamily="JetBrains Mono, monospace", color=TEXT),
    children=[
        html.Link(rel="stylesheet",
                  href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;700&family=Syne:wght@700;800&display=swap"),

        # ── Top bar ────────────────────────────────────────────────────────────
        html.Div([
            html.H1("Trace Execution Timeline",
                    style=dict(fontFamily="'Syne', sans-serif", fontSize="22px",
                               fontWeight="800", color=TEXT, margin="0 0 2px 0")),
            html.Span("User  →  Conversation  →  Trace  →  Timeline",
                      style=dict(fontSize="9px", color="#334155",
                                 letterSpacing="3px", textTransform="uppercase")),
        ], style=dict(padding="22px 28px 18px", borderBottom=f"1px solid {BORDER}")),

        # ── Main layout: sidebar + content ─────────────────────────────────────
        html.Div([

            # ── LEFT: Filter bar + Tree ────────────────────────────────────────
            html.Div([

                # Independent filters
                html.Div([
                    html.Div("Quick Filters", style=dict(
                        fontSize="9px", color=MUTED, letterSpacing="2px",
                        textTransform="uppercase", marginBottom="12px",
                        paddingBottom="8px", borderBottom=f"1px solid {BORDER}",
                    )),

                    html.Label("User ID", style=LABEL_S),
                    dcc.Dropdown(id="f-user", placeholder="Any user…",
                                 clearable=True, style=DD_S, className="dark-dd",
                                 optionHeight=32),
                    html.Div(style=dict(height="10px")),

                    html.Label("Conversation ID", style=LABEL_S),
                    dcc.Dropdown(id="f-conv", placeholder="Any conversation…",
                                 clearable=True, style=DD_S, className="dark-dd",
                                 optionHeight=32),
                    html.Div(style=dict(height="10px")),

                    html.Label("Trace ID", style=LABEL_S),
                    dcc.Dropdown(id="f-trace", placeholder="Any trace…",
                                 clearable=True, style=DD_S, className="dark-dd",
                                 optionHeight=32),
                ], style=dict(padding="14px 14px 16px",
                               borderBottom=f"1px solid {BORDER}")),

                # Tree header
                html.Div([
                    html.Div("Explorer", style=dict(
                        fontSize="9px", color=MUTED, letterSpacing="2px",
                        textTransform="uppercase", marginBottom="10px",
                    )),
                    html.Div(id="tree-root",
                             style=dict(overflowY="auto", maxHeight="calc(100vh - 360px)")),
                ], style=dict(padding="14px 10px 10px")),

            ], style=dict(
                width="300px", minWidth="300px",
                background=SURFACE, borderRight=f"1px solid {BORDER}",
                display="flex", flexDirection="column",
                height="calc(100vh - 60px)", overflowY="auto",
            )),

            # ── RIGHT: Timeline ────────────────────────────────────────────────
            html.Div([

                # Breadcrumb
                html.Div(id="breadcrumb", style=dict(
                    padding="10px 24px", borderBottom=f"1px solid {BORDER}",
                    fontSize="10px", color=MUTED, minHeight="36px",
                    display="flex", alignItems="center", gap="6px",
                    fontFamily="JetBrains Mono, monospace",
                )),

                # Stat cards
                html.Div([
                    stat_card("Events",   "st-events"),
                    stat_card("Duration", "st-dur"),
                    stat_card("Components", "st-comps"),
                    stat_card("Slowest",  "st-slow"),
                ], id="stat-row",
                   style=dict(display="none", gap="10px", padding="14px 24px",
                              flexWrap="wrap", borderBottom=f"1px solid {BORDER}")),

                # Chart / empty state
                html.Div(id="chart-area",
                         style=dict(padding="20px 24px", flex="1"),
                         children=html.Div([
                             html.Div("◎", style=dict(
                                 fontSize="40px", color="#1e3a5f",
                                 textAlign="center", marginBottom="14px")),
                             html.P("Click a Trace ID in the explorer to render its timeline",
                                    style=dict(color="#1e3a5f", fontSize="11px",
                                               textAlign="center",
                                               fontFamily="JetBrains Mono, monospace",
                                               letterSpacing="1px")),
                         ], style=dict(paddingTop="80px"))),

            ], style=dict(flex="1", display="flex", flexDirection="column",
                          overflow="hidden")),

        ], style=dict(display="flex", height="calc(100vh - 60px)")),

        # Stores
        dcc.Store(id="store-active-trace", data={}),   # {uid, cid, tid}
        dcc.Store(id="store-expanded",     data={}),   # {uid: bool, uid||cid: bool}
    ]
)


# ── CALLBACKS ──────────────────────────────────────────────────────────────────

# 1. Load all filter dropdowns independently on startup
@app.callback(
    Output("f-user",  "options"),
    Output("f-conv",  "options"),
    Output("f-trace", "options"),
    Input("f-user",   "id"),
)
def load_all_filters(_):
    try:
        users = run_query(f"SELECT DISTINCT user_id FROM {FULL_TABLE} WHERE user_id IS NOT NULL ORDER BY user_id")
        convs = run_query(f"SELECT DISTINCT conversation_id FROM {FULL_TABLE} WHERE conversation_id IS NOT NULL ORDER BY conversation_id")
        traces = run_query(f"SELECT DISTINCT trace_id FROM {FULL_TABLE} WHERE trace_id IS NOT NULL ORDER BY trace_id")
        u_opts = [{"label": v, "value": v} for v in users["user_id"].tolist()]
        c_opts = [{"label": v, "value": v} for v in convs["conversation_id"].tolist()]
        t_opts = [{"label": v, "value": v} for v in traces["trace_id"].tolist()]
        return u_opts, c_opts, t_opts
    except Exception as e:
        print(f"[load_filters] {e}")
        return [], [], []


# 2. Build / rebuild the tree when filters change or nodes are expanded
@app.callback(
    Output("tree-root",     "children"),
    Output("store-expanded","data"),
    Input("f-user",         "value"),
    Input("f-conv",         "value"),
    Input("f-trace",        "value"),
    Input({"type": "user-row",  "id": ALL}, "n_clicks"),
    Input({"type": "conv-row",  "id": ALL}, "n_clicks"),
    State({"type": "user-row",  "id": ALL}, "id"),
    State({"type": "conv-row",  "id": ALL}, "id"),
    State("store-expanded", "data"),
    prevent_initial_call=False,
)
def build_tree(f_user, f_conv, f_trace,
               user_clicks, conv_clicks,
               user_ids, conv_ids,
               expanded):
    ctx = callback_context
    expanded = expanded or {}

    # Toggle expanded state from clicks
    if ctx.triggered:
        for t in ctx.triggered:
            tid = t["prop_id"]
            if "user-row" in tid and t["value"]:
                key = json.loads(tid.split(".")[0])["id"]
                expanded[key] = not expanded.get(key, False)
            elif "conv-row" in tid and t["value"]:
                key = json.loads(tid.split(".")[0])["id"]
                expanded[key] = not expanded.get(key, False)

    try:
        # Build WHERE clause from filters
        where_parts = ["1=1"]
        if f_user:  where_parts.append(f"user_id = '{f_user}'")
        if f_conv:  where_parts.append(f"conversation_id = '{f_conv}'")
        if f_trace: where_parts.append(f"trace_id = '{f_trace}'")
        where = " AND ".join(where_parts)

        users_df = run_query(f"""
            SELECT DISTINCT user_id FROM {FULL_TABLE}
            WHERE {where} AND user_id IS NOT NULL
            ORDER BY user_id
        """)

        tree = []
        for _, ur in users_df.iterrows():
            uid      = ur["user_id"]
            u_exp    = expanded.get(uid, False)
            u_children = []

            if u_exp:
                convs_df = run_query(f"""
                    SELECT DISTINCT conversation_id FROM {FULL_TABLE}
                    WHERE user_id = '{uid}'
                      AND conversation_id IS NOT NULL
                      AND {where}
                    ORDER BY conversation_id
                """)
                for _, cr in convs_df.iterrows():
                    cid   = cr["conversation_id"]
                    c_key = f"{uid}||{cid}"
                    c_exp = expanded.get(c_key, False)
                    c_children = []

                    if c_exp:
                        traces_df = run_query(f"""
                            SELECT trace_id,
                                   COUNT(*)                   AS event_count,
                                   ROUND(SUM(duration_ms), 1) AS total_ms
                            FROM {FULL_TABLE}
                            WHERE user_id = '{uid}'
                              AND conversation_id = '{cid}'
                              AND trace_id IS NOT NULL
                              AND {where}
                            GROUP BY trace_id
                            ORDER BY MIN(timestamp) ASC
                        """)
                        for _, tr in traces_df.iterrows():
                            c_children.append(
                                trace_node(uid, cid, tr["trace_id"],
                                           tr["event_count"], tr["total_ms"])
                            )

                    u_children.append(conv_node(uid, cid, expanded=c_exp))
                    # Inject children into conv node (last added)
                    if c_children:
                        u_children[-1].children[1].children = c_children

            node = user_node(uid, expanded=u_exp)
            if u_children:
                node.children[1].children = u_children
            tree.append(node)

        return tree, expanded

    except Exception as e:
        print(f"[build_tree] {e}")
        return [html.P(str(e), style=dict(color="#ef4444", fontSize="10px",
                                          padding="10px"))], expanded


# 3. Handle trace click → render timeline
@app.callback(
    Output("store-active-trace", "data"),
    Input({"type": "trace-row", "id": ALL}, "n_clicks"),
    State({"type": "trace-row", "id": ALL}, "id"),
    prevent_initial_call=True,
)
def set_active_trace(clicks, ids):
    ctx = callback_context
    if not ctx.triggered:
        return {}
    for t in ctx.triggered:
        if t["value"]:
            raw = json.loads(t["prop_id"].split(".")[0])["id"]
            parts = raw.split("||")
            if len(parts) == 3:
                return {"uid": parts[0], "cid": parts[1], "tid": parts[2]}
    return {}


# 4. Also handle filter → trace dropdown direct selection
@app.callback(
    Output("store-active-trace", "data", allow_duplicate=True),
    Input("f-trace", "value"),
    prevent_initial_call=True,
)
def set_trace_from_filter(trace_id):
    if not trace_id:
        return {}
    try:
        df = run_query(f"""
            SELECT user_id, conversation_id
            FROM {FULL_TABLE}
            WHERE trace_id = '{trace_id}'
            LIMIT 1
        """)
        if df.empty:
            return {}
        return {
            "uid": df.iloc[0]["user_id"],
            "cid": df.iloc[0]["conversation_id"],
            "tid": trace_id,
        }
    except Exception as e:
        print(f"[set_trace_from_filter] {e}")
        return {}


# 5. Render timeline from active trace store
@app.callback(
    Output("chart-area",  "children"),
    Output("stat-row",    "style"),
    Output("st-events",   "children"),
    Output("st-dur",      "children"),
    Output("st-comps",    "children"),
    Output("st-slow",     "children"),
    Output("breadcrumb",  "children"),
    Input("store-active-trace", "data"),
)
def render_timeline(active):
    hidden  = dict(display="none")
    blank   = ("—","—","—","—")
    empty_bc = html.Span("Select a trace to begin", style=dict(color="#1e3a5f"))
    placeholder = html.Div([
        html.Div("◎", style=dict(fontSize="40px", color="#1e3a5f",
                                  textAlign="center", marginBottom="14px")),
        html.P("Click a Trace ID in the explorer to render its timeline",
               style=dict(color="#1e3a5f", fontSize="11px", textAlign="center",
                           fontFamily="JetBrains Mono, monospace", letterSpacing="1px")),
    ], style=dict(paddingTop="80px"))

    if not active or not active.get("tid"):
        return placeholder, hidden, *blank, empty_bc

    uid, cid, tid = active["uid"], active["cid"], active["tid"]

    try:
        df = run_query(f"""
            SELECT
                component,
                operation,
                operation_type,
                source,
                CAST(duration_ms AS DOUBLE) AS duration_ms,
                CAST(
                    (
                        (UNIX_MICROS(timestamp) - CAST(duration_ms * 1000 AS BIGINT))
                        - MIN(UNIX_MICROS(timestamp) - CAST(duration_ms * 1000 AS BIGINT))
                            OVER (PARTITION BY trace_id)
                    )
                AS DOUBLE) / 1000.0         AS start_offset_ms
            FROM {FULL_TABLE}
            WHERE trace_id        = '{tid}'
              AND user_id         = '{uid}'
              AND conversation_id = '{cid}'
            ORDER BY start_offset_ms ASC
        """)

        if df.empty:
            return (html.P("No events found.", style=dict(color="#6b7280", padding="40px",
                                                          textAlign="center")),
                    hidden, *blank, empty_bc)

        df["start_offset_ms"] = pd.to_numeric(df["start_offset_ms"], errors="coerce").fillna(0)
        df["duration_ms"]     = pd.to_numeric(df["duration_ms"],     errors="coerce").fillna(0)

        fig      = build_gantt(df)
        total_ms = (df["start_offset_ms"] + df["duration_ms"]).max()
        slowest  = df.loc[df["duration_ms"].idxmax()]
        n_comps  = df["component"].fillna("").nunique()

        def fmt(ms):
            return f"{ms/1000:.3f}s" if ms >= 1000 else f"{ms:.2f}ms"

        # Breadcrumb
        bc = [
            html.Span("👤", style=dict(marginRight="4px")),
            html.Span(uid,  style=dict(color="#64748b")),
            html.Span(" → ", style=dict(color="#1e3a5f")),
            html.Span("💬", style=dict(marginRight="4px")),
            html.Span(cid,  style=dict(color="#64748b")),
            html.Span(" → ", style=dict(color="#1e3a5f")),
            html.Span("🔍", style=dict(marginRight="4px")),
            html.Span(tid[:48] + ("…" if len(tid) > 48 else ""),
                      style=dict(color=ACCENT, fontWeight="700")),
        ]

        chart = dcc.Graph(
            figure=fig,
            config=dict(
                displayModeBar=True,
                modeBarButtonsToRemove=["select2d","lasso2d"],
                displaylogo=False,
                toImageButtonOptions=dict(format="png",
                                          filename="trace_timeline", scale=2),
            ),
            style=dict(width="100%"),
        )

        return (
            chart,
            dict(display="flex", gap="10px", padding="14px 24px",
                 flexWrap="wrap", borderBottom=f"1px solid {BORDER}"),
            str(len(df)),
            fmt(total_ms),
            str(n_comps),
            f"{slowest['component']} · {fmt(float(slowest['duration_ms']))}",
            bc,
        )

    except Exception as e:
        print(f"[render_timeline] {e}")
        err = html.Div([
            html.P("⚠ Error", style=dict(color="#ef4444", fontWeight="700")),
            html.Pre(str(e), style=dict(color="#6b7280", fontSize="10px",
                                        whiteSpace="pre-wrap")),
        ], style=dict(padding="24px", fontFamily="JetBrains Mono, monospace"))
        return err, hidden, *blank, empty_bc


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8050))
    app.run(host="0.0.0.0", port=port, debug=False)
