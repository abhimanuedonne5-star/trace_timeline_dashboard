import os
import pandas as pd
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output, State, callback_context, ALL, MATCH, no_update
from databricks.sdk import WorkspaceClient
import json

# ── Databricks SDK ─────────────────────────────────────────────────────────────
w = WorkspaceClient()

CATALOG      = os.environ.get("CATALOG",      "dev_omni")
SCHEMA       = os.environ.get("SCHEMA",       "dev_omni_gold")
TABLE        = os.environ.get("TABLE",        "trace_events")
WAREHOUSE_ID = os.environ.get("WAREHOUSE_ID", "2a6b5b84e8974695")

FULL_TABLE = f"`{CATALOG}`.`{SCHEMA}`.`{TABLE}`"


def fmt_ts(ts: str) -> str:
    """Normalise datetime-local input ('2024-01-15T10:30') to SQL timestamp string."""
    if not ts:
        return ""
    return ts.replace("T", " ") + (":00" if ts.count(":") == 1 else "")


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
        fig.update_layout(paper_bgcolor="#05060f", plot_bgcolor="#05060f",
                          font=dict(color="#374151"),
                          annotations=[dict(text="No data", x=0.5, y=0.5,
                                           showarrow=False, font=dict(color="#374151", size=14))])
        return fig

    df = df.copy()
    df["start_offset_ms"] = pd.to_numeric(df["start_offset_ms"], errors="coerce").fillna(0)
    df["duration_ms"]     = pd.to_numeric(df["duration_ms"],     errors="coerce").fillna(0)
    df = df.sort_values("start_offset_ms").reset_index(drop=True)
    df["component"] = df["component"].fillna(df.get("source", "UNKNOWN")).fillna("UNKNOWN")
    df["operation"] = df["operation"].fillna("")

    total_ms = (df["start_offset_ms"] + df["duration_ms"]).max()
    fig      = go.Figure()
    seen     = set()

    for i, row in df.iterrows():
        col     = get_color(row["component"])
        is_root = str(row.get("operation_type", "")).upper() in ("REQUEST_END", "PREFILL_LATENCY")
        bar_h   = 0.18 if is_root else 0.42
        opacity = 0.22 if is_root else 0.85
        show_lg = row["component"] not in seen
        seen.add(row["component"])

        dur_fmt   = f"{row['duration_ms']/1000:.3f}s" if row['duration_ms'] >= 1000 else f"{row['duration_ms']:.3f}ms"
        start_fmt = f"{row['start_offset_ms']:.3f}ms"
        end_fmt   = f"{row['start_offset_ms'] + row['duration_ms']:.3f}ms"

        fig.add_trace(go.Scatter(
            x=[row["start_offset_ms"] + row["duration_ms"] / 2],
            y=[i],
            mode="markers",
            marker=dict(size=1, opacity=0, color=col),
            name=row["component"],
            legendgroup=row["component"],
            showlegend=False,
            hovertemplate=(
                f"<b style='color:{col};font-size:13px'>{row['component']}</b>"
                + (f"<br><span style='color:#6b7280;font-size:11px'>{row['operation']}</span>" if row['operation'] else "")
                + f"<br><br><span style='color:#64748b'>Start   </span><b>{start_fmt}</b>"
                f"<br><span style='color:#64748b'>Duration</span><b>{dur_fmt}</b>"
                f"<br><span style='color:#64748b'>End     </span><b>{end_fmt}</b>"
                f"<br><span style='color:#64748b'>Type    </span><span style='color:#94a3b8'>{row.get('operation_type','')}</span>"
                "<extra></extra>"
            ),
        ))

        min_w = max(row["duration_ms"], total_ms * 0.0008)
        fig.add_shape(
            type="rect",
            x0=row["start_offset_ms"], x1=row["start_offset_ms"] + min_w,
            y0=i - bar_h / 2,         y1=i + bar_h / 2,
            fillcolor=col, opacity=opacity,
            line=dict(color=col, width=1), layer="above",
        )

    n  = len(df)
    fh = max(560, n * 56 + 180)

    # Two-line tick labels: component (bold) on line 1, operation (muted) on line 2
    tick_labels = []
    for _, row in df.iterrows():
        comp = row["component"]
        op   = row["operation"]
        if op:
            tick_labels.append(
                f"<b style='color:#cbd5e1'>{comp}</b>"
                f"<br><span style='color:#334155;font-size:9px'>{op}</span>"
            )
        else:
            tick_labels.append(f"<b style='color:#cbd5e1'>{comp}</b>")

    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="#05060f",
        plot_bgcolor="#0b0d1a",
        height=fh,
        margin=dict(l=8, r=24, t=44, b=52),
        font=dict(family="JetBrains Mono, monospace", size=11, color="#e2e8f0"),
        title=dict(
            text=(
                f"<span style='color:#1e3a5f'>Timeline</span>"
                f"  <span style='color:#0f2030'>|</span>"
                f"  <span style='color:#00d4ff;font-weight:700'>{n} events</span>"
                f"  <span style='color:#0f2030'>|</span>"
                f"  <span style='color:#334155'>{total_ms:.2f} ms total</span>"
            ),
            font=dict(size=12), x=0, xref="paper",
        ),
        showlegend=False,
        xaxis=dict(
            title=dict(text="Offset from first event (ms)",
                       font=dict(size=10, color="#475569")),
            gridcolor="#111827", zeroline=True,
            zerolinecolor="#1e3a5f", zerolinewidth=1,
            tickfont=dict(size=10, color="#64748b"),
            tickformat=".2f",
            ticksuffix=" ms",
            nticks=12,
            showgrid=True,
            showline=True,
            linecolor="#1a2035",
            ticks="outside",
            ticklen=4,
            tickcolor="#1a2035",
            rangeslider=dict(visible=True, thickness=0.025, bgcolor="#07080f"),
        ),
        yaxis=dict(
            tickmode="array",
            tickvals=list(range(n)),
            ticktext=tick_labels,
            tickfont=dict(size=11, color="#94a3b8"),
            gridcolor="#0d1020",
            autorange="reversed",
            automargin=True,
            ticks="outside",
            ticklen=10,
            tickwidth=1,
            tickcolor="#1a2035",
        ),
        hoverlabel=dict(
            bgcolor="#0a0c18", bordercolor="#1e293b",
            font=dict(family="JetBrains Mono, monospace", size=11),
        ),
        dragmode="zoom",
    )

    return fig


# ── App ────────────────────────────────────────────────────────────────────────
app = Dash(__name__, title="Trace Timeline", suppress_callback_exceptions=True)

# ── Design tokens ──────────────────────────────────────────────────────────────
BG      = "#05060f"
SURFACE = "#0b0d1a"
SURFACE2= "#0f1120"
BORDER  = "#1a2035"
TEXT    = "#e2e8f0"
MUTED   = "#475569"
DIM     = "#1e3a5f"
ACCENT  = "#00d4ff"
PURPLE  = "#a78bfa"
GREEN   = "#34d399"

LABEL_S = dict(
    fontSize="9px", fontFamily="JetBrains Mono, monospace",
    letterSpacing="2px", textTransform="uppercase",
    color=MUTED, marginBottom="5px", display="block",
)

DD_S = dict(
    backgroundColor=SURFACE2, color=TEXT,
    border=f"1px solid {BORDER}", borderRadius="6px",
    fontFamily="JetBrains Mono, monospace", fontSize="12px",
)


def stat_card(label, sid, accent_color=ACCENT):
    return html.Div([
        html.Span(label, style=dict(
            fontSize="9px", color=MUTED, letterSpacing="2px",
            textTransform="uppercase", fontFamily="JetBrains Mono, monospace",
            display="block", marginBottom="6px",
        )),
        html.Span("—", id=sid, style=dict(
            fontSize="17px", fontWeight="700",
            color=accent_color,
            fontFamily="JetBrains Mono, monospace",
        )),
    ], style=dict(
        background=SURFACE2, border=f"1px solid {BORDER}",
        borderRadius="6px", padding="10px 16px", minWidth="110px",
        boxShadow=f"0 0 10px {accent_color}08",
    ))


# ── Tree node builders ─────────────────────────────────────────────────────────
def user_node(uid, expanded=False):
    return html.Div([
        html.Div(
            id={"type": "user-row", "id": uid},
            n_clicks=0,
            style=dict(
                display="flex", alignItems="center", gap="8px",
                padding="8px 10px", cursor="pointer", borderRadius="6px",
                background="transparent", userSelect="none",
                transition="background .12s",
            ),
            children=[
                html.Span("▼" if expanded else "▶",
                          id={"type": "user-arrow", "id": uid},
                          style=dict(color=ACCENT, fontSize="8px",
                                     minWidth="10px", transition="transform .15s")),
                html.Span("👤", style=dict(fontSize="13px")),
                html.Span(uid, style=dict(
                    fontSize="12px", fontWeight="700", color="#cbd5e1",
                    fontFamily="JetBrains Mono, monospace",
                    overflow="hidden", textOverflow="ellipsis", whiteSpace="nowrap",
                )),
            ],
        ),
        html.Div(
            id={"type": "user-children", "id": uid},
            style=dict(display="block" if expanded else "none", paddingLeft="18px"),
        ),
    ], style=dict(borderBottom=f"1px solid {BORDER}18", marginBottom="1px"))


def conv_node(uid, cid, expanded=False):
    return html.Div([
        html.Div(
            id={"type": "conv-row", "id": f"{uid}||{cid}"},
            n_clicks=0,
            style=dict(
                display="flex", alignItems="center", gap="8px",
                padding="6px 10px", cursor="pointer", borderRadius="6px",
                userSelect="none", transition="background .12s",
            ),
            children=[
                html.Span("▼" if expanded else "▶",
                          id={"type": "conv-arrow", "id": f"{uid}||{cid}"},
                          style=dict(color=PURPLE, fontSize="8px", minWidth="10px")),
                html.Span("💬", style=dict(fontSize="11px")),
                html.Span(cid, style=dict(
                    fontSize="11px", fontWeight="600", color="#7c8db5",
                    fontFamily="JetBrains Mono, monospace",
                    overflow="hidden", textOverflow="ellipsis", whiteSpace="nowrap",
                )),
            ],
        ),
        html.Div(
            id={"type": "conv-children", "id": f"{uid}||{cid}"},
            style=dict(display="block" if expanded else "none", paddingLeft="18px"),
        ),
    ])


def trace_node(uid, cid, tid, event_count=None, total_ms=None):
    label_extra = ""
    if event_count is not None:
        ms_str = f"{float(total_ms)/1000:.2f}s" if float(total_ms) >= 1000 else f"{float(total_ms):.0f}ms"
        label_extra = f"{int(float(event_count))} evt · {ms_str}"
    return html.Div(
        id={"type": "trace-row", "id": f"{uid}||{cid}||{tid}"},
        n_clicks=0,
        style=dict(
            display="flex", alignItems="flex-start", gap="8px",
            padding="6px 10px", cursor="pointer", borderRadius="6px",
            userSelect="none", transition="background .12s",
        ),
        children=[
            html.Span("◈", style=dict(color=GREEN, fontSize="9px",
                                      minWidth="10px", marginTop="2px")),
            html.Div([
                html.Span(
                    tid[:36] + ("…" if len(tid) > 36 else ""),
                    style=dict(
                        fontSize="10px", fontWeight="600", color="#4a5568",
                        fontFamily="JetBrains Mono, monospace",
                        display="block", lineHeight="1.4",
                    ),
                ),
                html.Span(label_extra, style=dict(
                    fontSize="9px", color="#2d3f5a",
                    fontFamily="JetBrains Mono, monospace",
                )),
            ]),
        ],
    )


# ── Layout ─────────────────────────────────────────────────────────────────────
app.layout = html.Div(
    style=dict(
        background=BG, height="100vh", overflow="hidden",
        display="flex", flexDirection="column",
        fontFamily="JetBrains Mono, monospace", color=TEXT,
    ),
    children=[
        html.Link(
            rel="stylesheet",
            href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;600;700&family=Syne:wght@700;800&display=swap",
        ),

        # ── Top bar + filters combined ─────────────────────────────────────────
        html.Div([
            # Title
            html.Div([
                html.H1("Trace Execution Timeline", style=dict(
                    fontFamily="'Syne', sans-serif", fontSize="18px",
                    fontWeight="800", color=TEXT, margin="0 0 1px 0",
                    letterSpacing="-0.3px",
                )),
                html.Span("User  →  Conversation  →  Trace  →  Timeline", style=dict(
                    fontSize="8px", color=DIM, letterSpacing="3px",
                    textTransform="uppercase",
                )),
            ], style=dict(minWidth="260px")),

            # Divider
            html.Div(style=dict(width="1px", background=BORDER,
                                alignSelf="stretch", margin="0 20px")),

            # Filters side by side
            html.Div([
                html.Label("User ID", style=LABEL_S),
                dcc.Dropdown(
                    id="f-user", placeholder="Any user…",
                    clearable=True, style=DD_S, className="dark-dd",
                    optionHeight=32,
                ),
            ], style=dict(flex="1", minWidth="150px")),

            html.Div([
                html.Label("Conversation ID", style=LABEL_S),
                dcc.Dropdown(
                    id="f-conv", placeholder="Any conversation…",
                    clearable=True, style=DD_S, className="dark-dd",
                    optionHeight=32,
                ),
            ], style=dict(flex="1", minWidth="150px")),

            html.Div([
                html.Label("Trace ID", style=LABEL_S),
                dcc.Dropdown(
                    id="f-trace", placeholder="Any trace…",
                    clearable=True, style=DD_S, className="dark-dd",
                    optionHeight=32,
                ),
            ], style=dict(flex="1", minWidth="150px")),
        ], style=dict(
            display="flex", alignItems="center", gap="14px",
            padding="10px 20px",
            borderBottom=f"1px solid {BORDER}",
            background=SURFACE,
            flexShrink="0",
        )),

        # ── Main layout: sidebar + drag handle + content ───────────────────────
        html.Div([

            # ── LEFT: Explorer tree ────────────────────────────────────────────
            html.Div([

                # Time range filters
                html.Div([
                    html.Div([
                        html.Span("⏱", style=dict(fontSize="9px", marginRight="5px",
                                                   color=ACCENT)),
                        html.Span("Time Range", style=dict(
                            fontSize="9px", color=MUTED, letterSpacing="2px",
                            textTransform="uppercase",
                        )),
                    ], style=dict(marginBottom="10px")),

                    html.Label("From", style=LABEL_S),
                    dcc.Input(
                        id="f-from", type="datetime-local",
                        debounce=True, className="ts-input",
                        style=dict(
                            width="100%", background=SURFACE2, color=TEXT,
                            border=f"1px solid {BORDER}", borderRadius="6px",
                            padding="5px 8px", fontSize="11px",
                            fontFamily="JetBrains Mono, monospace",
                            marginBottom="8px", display="block",
                        ),
                    ),

                    html.Label("To", style=LABEL_S),
                    dcc.Input(
                        id="f-to", type="datetime-local",
                        debounce=True, className="ts-input",
                        style=dict(
                            width="100%", background=SURFACE2, color=TEXT,
                            border=f"1px solid {BORDER}", borderRadius="6px",
                            padding="5px 8px", fontSize="11px",
                            fontFamily="JetBrains Mono, monospace",
                            display="block",
                        ),
                    ),
                ], style=dict(
                    padding="10px 12px 12px",
                    borderBottom=f"1px solid {BORDER}",
                    flexShrink="0",
                )),

                # Explorer header + tree
                html.Div([
                    html.Span("◈", style=dict(fontSize="8px", color=GREEN,
                                              marginRight="6px")),
                    html.Span("Explorer", style=dict(
                        fontSize="9px", color=MUTED, letterSpacing="2px",
                        textTransform="uppercase",
                    )),
                ], style=dict(
                    padding="10px 12px 8px",
                    borderBottom=f"1px solid {BORDER}",
                    flexShrink="0",
                )),
                html.Div(
                    id="tree-root",
                    style=dict(
                        overflowY="auto",
                        flex="1",
                        padding="4px 6px 10px",
                    ),
                ),
            ], id="sidebar-panel", style=dict(
                width="260px", minWidth="160px",
                background=SURFACE,
                display="flex", flexDirection="column",
                overflow="hidden",
                flexShrink="0",
            )),

            # ── Drag handle ───────────────────────────────────────────────────
            html.Div(id="drag-handle"),

            # ── RIGHT: Timeline ────────────────────────────────────────────────
            html.Div([

                # Breadcrumb
                html.Div(id="breadcrumb", style=dict(
                    padding="7px 18px", borderBottom=f"1px solid {BORDER}",
                    fontSize="10px", color=MUTED, minHeight="30px",
                    display="flex", alignItems="center", gap="6px",
                    fontFamily="JetBrains Mono, monospace",
                    background=SURFACE, flexShrink="0",
                )),

                # Stat cards
                html.Div([
                    stat_card("Events",     "st-events", ACCENT),
                    stat_card("Duration",   "st-dur",    PURPLE),
                    stat_card("Components", "st-comps",  GREEN),
                    stat_card("Slowest",    "st-slow",   "#fbbf24"),
                ], id="stat-row",
                   style=dict(
                       display="none", gap="10px", padding="10px 18px",
                       flexWrap="wrap", borderBottom=f"1px solid {BORDER}",
                       background=SURFACE2, flexShrink="0",
                   )),

                # Chart / empty state
                html.Div(
                    id="chart-area",
                    style=dict(
                        padding="8px 12px 8px 0", flex="1",
                        overflowY="auto", overflowX="hidden",
                    ),
                    children=html.Div([
                        html.Div("◎", style=dict(
                            fontSize="44px", color=DIM,
                            textAlign="center", marginBottom="16px",
                        )),
                        html.P(
                            "Click a Trace ID in the explorer to render its timeline",
                            style=dict(
                                color=DIM, fontSize="11px", textAlign="center",
                                fontFamily="JetBrains Mono, monospace",
                                letterSpacing="1px",
                            ),
                        ),
                    ], style=dict(paddingTop="80px")),
                ),

            ], style=dict(
                flex="1", display="flex", flexDirection="column",
                overflow="hidden", minWidth="0",
            )),

        ], style=dict(
            display="flex", flex="1", overflow="hidden",
        )),

        # Stores
        dcc.Store(id="store-active-trace", data={}),
        dcc.Store(id="store-expanded",     data={}),
        dcc.Store(id="store-tree-cache",   data={}),  # {key: list} — avoids re-querying
    ]
)


# ── CALLBACKS ──────────────────────────────────────────────────────────────────

# 1. Cascading filter dropdowns
#    - On load : populate all three with all distinct values
#    - User changes   → re-scope conv + trace options, clear their values
#    - Conv changes   → re-scope trace options, clear its value
#    - From/To change → re-scope all options to the time window
@app.callback(
    Output("f-user",  "options"),
    Output("f-conv",  "options"),
    Output("f-trace", "options"),
    Output("f-conv",  "value",   allow_duplicate=True),
    Output("f-trace", "value",   allow_duplicate=True),
    Input("f-user",   "id"),       # fires once on mount
    Input("f-user",   "value"),
    Input("f-conv",   "value"),
    Input("f-from",   "value"),
    Input("f-to",     "value"),
    prevent_initial_call="initial_duplicate",
)
def cascade_filters(_, f_user, f_conv, f_from, f_to):
    ctx      = callback_context
    trigger  = ctx.triggered[0]["prop_id"] if ctx.triggered else ""

    # Decide which downstream values to clear
    clear_conv  = no_update
    clear_trace = no_update
    if "f-user.value"  in trigger:
        clear_conv  = None
        clear_trace = None
    elif "f-conv.value" in trigger:
        clear_trace = None
    elif "f-from.value" in trigger or "f-to.value" in trigger:
        clear_conv  = None
        clear_trace = None

    # Build shared time-range WHERE fragment
    ts_parts = []
    if f_from: ts_parts.append(f"timestamp >= CAST('{fmt_ts(f_from)}' AS TIMESTAMP)")
    if f_to:   ts_parts.append(f"timestamp <= CAST('{fmt_ts(f_to)}'   AS TIMESTAMP)")
    ts_where = (" AND " + " AND ".join(ts_parts)) if ts_parts else ""

    try:
        # Users: unscoped (always show all)
        u_df   = run_query(f"SELECT DISTINCT user_id FROM {FULL_TABLE} WHERE user_id IS NOT NULL{ts_where} ORDER BY user_id")
        u_opts = [{"label": v, "value": v} for v in u_df["user_id"].tolist()]

        # Conversations: scoped to selected user
        c_where = f" AND user_id = '{f_user}'" if f_user else ""
        c_df    = run_query(f"SELECT DISTINCT conversation_id FROM {FULL_TABLE} WHERE conversation_id IS NOT NULL{c_where}{ts_where} ORDER BY conversation_id")
        c_opts  = [{"label": v, "value": v} for v in c_df["conversation_id"].tolist()]

        # Traces: scoped to selected user + conv
        t_where = ""
        if f_user: t_where += f" AND user_id = '{f_user}'"
        if f_conv: t_where += f" AND conversation_id = '{f_conv}'"
        t_df    = run_query(f"SELECT DISTINCT trace_id FROM {FULL_TABLE} WHERE trace_id IS NOT NULL{t_where}{ts_where} ORDER BY trace_id")
        t_opts  = [{"label": v, "value": v} for v in t_df["trace_id"].tolist()]

        return u_opts, c_opts, t_opts, clear_conv, clear_trace

    except Exception as e:
        print(f"[cascade_filters] {e}")
        return [], [], [], clear_conv, clear_trace


# 2. Build / rebuild the tree — results are cached so nodes only query once
@app.callback(
    Output("tree-root",        "children"),
    Output("store-expanded",   "data"),
    Output("store-tree-cache", "data"),
    Input("f-user",            "value"),
    Input("f-conv",            "value"),
    Input("f-trace",           "value"),
    Input("f-from",            "value"),
    Input("f-to",              "value"),
    Input({"type": "user-row", "id": ALL}, "n_clicks"),
    Input({"type": "conv-row", "id": ALL}, "n_clicks"),
    State({"type": "user-row", "id": ALL}, "id"),
    State({"type": "conv-row", "id": ALL}, "id"),
    State("store-expanded",    "data"),
    State("store-tree-cache",  "data"),
    prevent_initial_call=False,
)
def build_tree(f_user, f_conv, f_trace, f_from, f_to,
               user_clicks, conv_clicks,
               user_ids, conv_ids,
               expanded, cache):
    ctx      = callback_context
    expanded = expanded or {}
    cache    = cache    or {}

    # Detect filter change → clear cache + collapse all
    triggered_props = [t["prop_id"] for t in ctx.triggered] if ctx.triggered else []
    filter_changed  = any(p.startswith("f-") for p in triggered_props)
    if filter_changed:
        cache    = {}
        expanded = {}
        # Auto-expand the selected user so their conversations are visible immediately
        if f_user:
            expanded[f_user] = True

    # Toggle the clicked node
    if ctx.triggered:
        for t in ctx.triggered:
            pid = t["prop_id"]
            if "user-row" in pid and t["value"]:
                key = json.loads(pid.split(".")[0])["id"]
                expanded[key] = not expanded.get(key, False)
            elif "conv-row" in pid and t["value"]:
                key = json.loads(pid.split(".")[0])["id"]
                expanded[key] = not expanded.get(key, False)

    where_parts = ["1=1"]
    if f_user:  where_parts.append(f"user_id = '{f_user}'")
    if f_conv:  where_parts.append(f"conversation_id = '{f_conv}'")
    if f_trace: where_parts.append(f"trace_id = '{f_trace}'")
    if f_from:  where_parts.append(f"timestamp >= CAST('{fmt_ts(f_from)}' AS TIMESTAMP)")
    if f_to:    where_parts.append(f"timestamp <= CAST('{fmt_ts(f_to)}' AS TIMESTAMP)")
    where = " AND ".join(where_parts)

    # ── Fetch users (cached) ──────────────────────────────────────────────────
    u_key = f"users|{where}"
    if u_key not in cache:
        try:
            df = run_query(f"""
                SELECT DISTINCT user_id FROM {FULL_TABLE}
                WHERE {where} AND user_id IS NOT NULL ORDER BY user_id
            """)
            cache[u_key] = df["user_id"].tolist()
        except Exception as e:
            print(f"[build_tree/users] {e}")
            return ([html.P(str(e), style=dict(color="#ef4444", fontSize="10px",
                                               padding="10px",
                                               fontFamily="JetBrains Mono, monospace"))],
                    expanded, cache)

    tree = []
    for uid in cache[u_key]:
        u_exp      = expanded.get(uid, False)
        u_children = []

        if u_exp:
            # ── Fetch conversations for this user (cached) ────────────────────
            c_key = f"convs|{uid}|{where}"
            if c_key not in cache:
                try:
                    df = run_query(f"""
                        SELECT DISTINCT conversation_id FROM {FULL_TABLE}
                        WHERE user_id = '{uid}'
                          AND conversation_id IS NOT NULL AND {where}
                        ORDER BY conversation_id
                    """)
                    cache[c_key] = df["conversation_id"].tolist()
                except Exception as e:
                    print(f"[build_tree/convs] {e}")
                    cache[c_key] = []

            for cid in cache[c_key]:
                ck     = f"{uid}||{cid}"
                c_exp  = expanded.get(ck, False)
                c_children = []

                if c_exp:
                    # ── Fetch traces for this conversation (cached) ───────────
                    t_key = f"traces|{uid}|{cid}|{where}"
                    if t_key not in cache:
                        try:
                            df = run_query(f"""
                                SELECT trace_id,
                                       COUNT(*)                   AS event_count,
                                       ROUND(SUM(duration_ms), 1) AS total_ms
                                FROM {FULL_TABLE}
                                WHERE user_id = '{uid}'
                                  AND conversation_id = '{cid}'
                                  AND trace_id IS NOT NULL AND {where}
                                GROUP BY trace_id
                                ORDER BY MIN(timestamp) ASC
                            """)
                            cache[t_key] = df.to_dict("records")
                        except Exception as e:
                            print(f"[build_tree/traces] {e}")
                            cache[t_key] = []

                    for tr in cache[t_key]:
                        c_children.append(
                            trace_node(uid, cid, tr["trace_id"],
                                       tr["event_count"], tr["total_ms"])
                        )

                u_children.append(conv_node(uid, cid, expanded=c_exp))
                if c_children:
                    u_children[-1].children[1].children = c_children

        node = user_node(uid, expanded=u_exp)
        if u_children:
            node.children[1].children = u_children
        tree.append(node)

    return tree, expanded, cache


# 3. Handle trace click → store active trace
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


# 4. Handle filter trace dropdown → store active trace
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


# ── Summary view helpers ───────────────────────────────────────────────────────
def summary_stat(label, value, color=ACCENT):
    return html.Div([
        html.Div(value, style=dict(fontSize="22px", fontWeight="700",
                                   color=color, fontFamily="JetBrains Mono, monospace",
                                   lineHeight="1.2")),
        html.Div(label, style=dict(fontSize="9px", color=MUTED,
                                   letterSpacing="2px", textTransform="uppercase",
                                   fontFamily="JetBrains Mono, monospace",
                                   marginTop="4px")),
    ], style=dict(background=SURFACE2, border=f"1px solid {BORDER}",
                  borderRadius="8px", padding="14px 20px", minWidth="120px",
                  boxShadow=f"0 0 12px {color}0a"))


def summary_table_row(cells, header=False):
    cell_style = dict(
        padding="8px 14px",
        fontSize="9px" if header else "11px",
        fontFamily="JetBrains Mono, monospace",
        borderBottom=f"1px solid {BORDER}",
        color=MUTED if header else "#94a3b8",
        fontWeight="600" if header else "400",
        letterSpacing="1px" if header else "0",
        textTransform="uppercase" if header else "none",
        whiteSpace="nowrap", overflow="hidden", textOverflow="ellipsis",
    )
    return html.Tr([html.Th(c, style=cell_style) if header
                    else html.Td(c, style=cell_style) for c in cells])


# 5. Render: placeholder / user overview / conversation overview / trace Gantt
@app.callback(
    Output("chart-area",  "children"),
    Output("stat-row",    "style"),
    Output("st-events",   "children"),
    Output("st-dur",      "children"),
    Output("st-comps",    "children"),
    Output("st-slow",     "children"),
    Output("breadcrumb",  "children"),
    Input("store-active-trace", "data"),
    Input("f-user",  "value"),
    Input("f-conv",  "value"),
    State("f-from",  "value"),
    State("f-to",    "value"),
)
def render_main(active, f_user, f_conv, f_from, f_to):
    hidden   = dict(display="none")
    blank    = ("—", "—", "—", "—")

    def fmt(ms):
        ms = float(ms)
        return f"{ms/1000:.3f}s" if ms >= 1000 else f"{ms:.2f}ms"

    ts_parts = []
    if f_from: ts_parts.append(f"timestamp >= CAST('{fmt_ts(f_from)}' AS TIMESTAMP)")
    if f_to:   ts_parts.append(f"timestamp <= CAST('{fmt_ts(f_to)}'   AS TIMESTAMP)")
    ts_where = (" AND " + " AND ".join(ts_parts)) if ts_parts else ""

    # ── TRACE selected → Gantt timeline ───────────────────────────────────────
    if active and active.get("tid"):
        uid, cid, tid = active["uid"], active["cid"], active["tid"]
        try:
            df = run_query(f"""
                SELECT component, operation, operation_type, source,
                    CAST(duration_ms AS DOUBLE) AS duration_ms,
                    CAST(
                        (
                            (UNIX_MICROS(timestamp) - CAST(duration_ms * 1000 AS BIGINT))
                            - MIN(UNIX_MICROS(timestamp) - CAST(duration_ms * 1000 AS BIGINT))
                                OVER (PARTITION BY trace_id)
                        )
                    AS DOUBLE) / 1000.0 AS start_offset_ms
                FROM {FULL_TABLE}
                WHERE trace_id        = '{tid}'
                  AND user_id         = '{uid}'
                  AND conversation_id = '{cid}'
                  {("AND timestamp >= CAST('" + fmt_ts(f_from) + "' AS TIMESTAMP)") if f_from else ""}
                  {("AND timestamp <= CAST('" + fmt_ts(f_to)   + "' AS TIMESTAMP)") if f_to   else ""}
                ORDER BY start_offset_ms ASC
            """)
            if df.empty:
                return (html.P("No events found.", style=dict(color="#6b7280",
                               padding="40px", textAlign="center")),
                        hidden, *blank,
                        html.Span("No events in selected range", style=dict(color=DIM)))

            df["start_offset_ms"] = pd.to_numeric(df["start_offset_ms"], errors="coerce").fillna(0)
            df["duration_ms"]     = pd.to_numeric(df["duration_ms"],     errors="coerce").fillna(0)
            fig      = build_gantt(df)
            total_ms = (df["start_offset_ms"] + df["duration_ms"]).max()
            slowest  = df.loc[df["duration_ms"].idxmax()]
            n_comps  = df["component"].fillna("").nunique()
            bc = [
                html.Span("👤", style=dict(marginRight="4px")),
                html.Span(uid, style=dict(color="#4a5568")),
                html.Span(" → ", style=dict(color=DIM)),
                html.Span("💬", style=dict(marginRight="4px")),
                html.Span(cid, style=dict(color="#4a5568")),
                html.Span(" → ", style=dict(color=DIM)),
                html.Span("🔍", style=dict(marginRight="4px")),
                html.Span(tid[:48] + ("…" if len(tid) > 48 else ""),
                          style=dict(color=ACCENT, fontWeight="700")),
            ]
            chart = dcc.Graph(figure=fig,
                              config=dict(displayModeBar=True,
                                          modeBarButtonsToRemove=["select2d", "lasso2d"],
                                          displaylogo=False,
                                          toImageButtonOptions=dict(format="png",
                                              filename="trace_timeline", scale=2)),
                              style=dict(width="100%"))
            return (chart,
                    dict(display="flex", gap="12px", padding="14px 24px",
                         flexWrap="wrap", borderBottom=f"1px solid {BORDER}",
                         background=SURFACE2),
                    str(len(df)), fmt(total_ms), str(n_comps),
                    f"{slowest['component']} · {fmt(float(slowest['duration_ms']))}",
                    bc)
        except Exception as e:
            print(f"[render/trace] {e}")
            return (html.Div([html.P("⚠ Error", style=dict(color="#ef4444", fontWeight="700")),
                              html.Pre(str(e), style=dict(color="#475569", fontSize="10px",
                                                          whiteSpace="pre-wrap"))],
                             style=dict(padding="24px", fontFamily="JetBrains Mono, monospace")),
                    hidden, *blank, html.Span("Error", style=dict(color="#ef4444")))

    # ── CONVERSATION selected → trace list ────────────────────────────────────
    if f_conv and f_user:
        try:
            df = run_query(f"""
                SELECT trace_id,
                       COUNT(*)                        AS event_count,
                       ROUND(SUM(duration_ms), 1)      AS total_ms,
                       COUNT(DISTINCT component)        AS components,
                       MIN(timestamp)                   AS started_at
                FROM {FULL_TABLE}
                WHERE user_id = '{f_user}' AND conversation_id = '{f_conv}'
                  AND trace_id IS NOT NULL{ts_where}
                GROUP BY trace_id
                ORDER BY started_at ASC
            """)
            bc = [
                html.Span("👤", style=dict(marginRight="4px")),
                html.Span(f_user, style=dict(color="#4a5568")),
                html.Span(" → ", style=dict(color=DIM)),
                html.Span("💬", style=dict(marginRight="4px")),
                html.Span(f_conv, style=dict(color=PURPLE, fontWeight="700")),
                html.Span("  ·  ", style=dict(color=DIM)),
                html.Span(f"{len(df)} traces", style=dict(color=MUTED, fontSize="9px",
                           letterSpacing="1px", textTransform="uppercase")),
            ]
            rows = [summary_table_row(["Trace ID", "Events", "Duration",
                                       "Components", "Started At"], header=True)]
            for _, r in df.iterrows():
                tid_short = str(r["trace_id"])[:44] + ("…" if len(str(r["trace_id"])) > 44 else "")
                rows.append(html.Tr(
                    [html.Td(c, style=dict(
                        padding="8px 14px", fontSize="11px",
                        fontFamily="JetBrains Mono, monospace",
                        borderBottom=f"1px solid {BORDER}",
                        color=col, whiteSpace="nowrap",
                    )) for c, col in [
                        (tid_short,           ACCENT),
                        (str(int(float(r["event_count"]))), "#94a3b8"),
                        (fmt(float(r["total_ms"])),         GREEN),
                        (str(int(float(r["components"]))),  "#94a3b8"),
                        (str(r["started_at"])[:19],         MUTED),
                    ]],
                    id={"type": "trace-row",
                        "id": f"{f_user}||{f_conv}||{r['trace_id']}"},
                    n_clicks=0,
                    style=dict(cursor="pointer"),
                    className="summary-row",
                ))
            content = html.Div([
                html.Div([
                    html.Span("💬", style=dict(fontSize="18px", marginRight="10px")),
                    html.Span(f_conv, style=dict(fontSize="14px", fontWeight="700",
                                                  color=TEXT,
                                                  fontFamily="JetBrains Mono, monospace")),
                ], style=dict(display="flex", alignItems="center",
                              marginBottom="20px")),
                html.Div([
                    summary_stat("Traces",    str(len(df)),                          PURPLE),
                    summary_stat("Events",    str(int(df["event_count"].astype(float).sum())), ACCENT),
                    summary_stat("Duration",  fmt(df["total_ms"].astype(float).sum()), GREEN),
                ], style=dict(display="flex", gap="12px", flexWrap="wrap",
                              marginBottom="24px")),
                html.Div("Click a trace to open its timeline", style=dict(
                    fontSize="9px", color=DIM, letterSpacing="2px",
                    textTransform="uppercase", marginBottom="10px",
                )),
                html.Div(html.Table(rows, style=dict(
                    width="100%", borderCollapse="collapse",
                )), style=dict(
                    background=SURFACE2, border=f"1px solid {BORDER}",
                    borderRadius="8px", overflow="hidden",
                )),
            ], style=dict(padding="24px"))
            return content, hidden, *blank, bc
        except Exception as e:
            print(f"[render/conv] {e}")

    # ── USER selected → conversation list ─────────────────────────────────────
    if f_user:
        try:
            df = run_query(f"""
                SELECT conversation_id,
                       COUNT(DISTINCT trace_id)         AS traces,
                       COUNT(*)                         AS events,
                       ROUND(SUM(duration_ms), 1)       AS total_ms,
                       MIN(timestamp)                   AS first_event,
                       MAX(timestamp)                   AS last_event
                FROM {FULL_TABLE}
                WHERE user_id = '{f_user}' AND conversation_id IS NOT NULL{ts_where}
                GROUP BY conversation_id
                ORDER BY first_event ASC
            """)
            bc = [
                html.Span("👤", style=dict(marginRight="4px")),
                html.Span(f_user, style=dict(color=ACCENT, fontWeight="700")),
                html.Span("  ·  ", style=dict(color=DIM)),
                html.Span(f"{len(df)} conversations", style=dict(
                    color=MUTED, fontSize="9px",
                    letterSpacing="1px", textTransform="uppercase")),
            ]
            rows = [summary_table_row(
                ["Conversation ID", "Traces", "Events", "Total Duration",
                 "First Event", "Last Event"], header=True)]
            for _, r in df.iterrows():
                cid_short = str(r["conversation_id"])[:36] + \
                            ("…" if len(str(r["conversation_id"])) > 36 else "")
                rows.append(html.Tr(
                    [html.Td(c, style=dict(
                        padding="8px 14px", fontSize="11px",
                        fontFamily="JetBrains Mono, monospace",
                        borderBottom=f"1px solid {BORDER}",
                        color=col, whiteSpace="nowrap",
                    )) for c, col in [
                        (cid_short,                         PURPLE),
                        (str(int(float(r["traces"]))),       "#94a3b8"),
                        (str(int(float(r["events"]))),       "#94a3b8"),
                        (fmt(float(r["total_ms"])),          GREEN),
                        (str(r["first_event"])[:19],         MUTED),
                        (str(r["last_event"])[:19],          MUTED),
                    ]],
                    style=dict(cursor="default"),
                    className="summary-row",
                ))
            content = html.Div([
                html.Div([
                    html.Span("👤", style=dict(fontSize="18px", marginRight="10px")),
                    html.Span(f_user, style=dict(fontSize="14px", fontWeight="700",
                                                  color=TEXT,
                                                  fontFamily="JetBrains Mono, monospace")),
                ], style=dict(display="flex", alignItems="center",
                              marginBottom="20px")),
                html.Div([
                    summary_stat("Conversations", str(len(df)),                           ACCENT),
                    summary_stat("Total Traces",  str(int(df["traces"].astype(float).sum())), PURPLE),
                    summary_stat("Total Events",  str(int(df["events"].astype(float).sum())), GREEN),
                    summary_stat("Total Duration",fmt(df["total_ms"].astype(float).sum()),  "#fbbf24"),
                ], style=dict(display="flex", gap="12px", flexWrap="wrap",
                              marginBottom="24px")),
                html.Div("Select a conversation in the explorer to drill in",
                         style=dict(fontSize="9px", color=DIM, letterSpacing="2px",
                                    textTransform="uppercase", marginBottom="10px")),
                html.Div(html.Table(rows, style=dict(
                    width="100%", borderCollapse="collapse",
                )), style=dict(
                    background=SURFACE2, border=f"1px solid {BORDER}",
                    borderRadius="8px", overflow="hidden",
                )),
            ], style=dict(padding="24px"))
            return content, hidden, *blank, bc
        except Exception as e:
            print(f"[render/user] {e}")

    # ── Nothing selected → placeholder ────────────────────────────────────────
    placeholder = html.Div([
        html.Div("◎", style=dict(fontSize="44px", color=DIM,
                                  textAlign="center", marginBottom="16px")),
        html.P("Select a User ID or pick a Trace from the explorer",
               style=dict(color=DIM, fontSize="11px", textAlign="center",
                           fontFamily="JetBrains Mono, monospace", letterSpacing="1px")),
    ], style=dict(paddingTop="100px"))
    return placeholder, hidden, *blank, html.Span("", style=dict(color=DIM))


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8050))
    app.run(host="0.0.0.0", port=port, debug=False)
