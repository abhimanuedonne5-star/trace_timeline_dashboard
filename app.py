import os
import time
import threading
import pandas as pd
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output, State, callback_context, ALL, MATCH, no_update
from databricks.sdk import WorkspaceClient
import json

# ── Server-side SQL cache (TTL-based) ──────────────────────────────────────────
# Entries expire after CACHE_TTL seconds so the dashboard reflects table updates.
CACHE_TTL   = 60          # seconds — tune as needed
_cache: dict = {}         # key → (stored_at_epoch, value)
_cache_lock = threading.Lock()

# ── Databricks SDK ─────────────────────────────────────────────────────────────
w = WorkspaceClient()

CATALOG      = os.environ.get("CATALOG",      "dev_omni")
SCHEMA       = os.environ.get("SCHEMA",       "dev_omni_gold")
TABLE        = os.environ.get("TABLE",        "trace_events")
WAREHOUSE_ID = os.environ.get("WAREHOUSE_ID", "2a6b5b84e8974695")

FULL_TABLE = f"`{CATALOG}`.`{SCHEMA}`.`{TABLE}`"


def _cache_get(key):
    with _cache_lock:
        entry = _cache.get(key)
    if entry is None:
        return None
    stored_at, value = entry
    if time.time() - stored_at > CACHE_TTL:
        with _cache_lock:
            _cache.pop(key, None)
        return None
    return value

def _cache_set(key, value):
    with _cache_lock:
        _cache[key] = (time.time(), value)

def _cache_clear_prefix(prefix: str):
    with _cache_lock:
        for k in [k for k in _cache if k.startswith(prefix)]:
            del _cache[k]

def _cache_clear_all():
    with _cache_lock:
        _cache.clear()


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
    "#00e5ff","#bf7fff","#00ff9f","#ffe033","#ff5c5c",
    "#ff4db8","#4db8ff","#00ffea","#ff7a1a","#d966ff",
    "#00ccff","#ccff00","#ff4040","#33ff77","#ff33ff",
    "#33bbff","#ffdd00","#66ff99","#ff6680","#7777ff",
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
        fig.update_layout(paper_bgcolor="#080b14", plot_bgcolor="#080b14",
                          font=dict(color="#7b8fa8"),
                          annotations=[dict(text="No data", x=0.5, y=0.5,
                                           showarrow=False, font=dict(color="#7b8fa8", size=14))])
        return fig

    df = df.copy()
    df["start_offset_ms"] = pd.to_numeric(df["start_offset_ms"], errors="coerce").fillna(0)
    df["duration_ms"]     = pd.to_numeric(df["duration_ms"],     errors="coerce").fillna(0)
    df = df.sort_values("start_offset_ms").reset_index(drop=True)
    df["component"]     = df["component"].fillna(df.get("source", "UNKNOWN")).fillna("UNKNOWN")
    df["operation"]     = df["operation"].fillna("")
    df["operation_type"]= df["operation_type"].fillna("") if "operation_type" in df.columns else ""

    total_ms = (df["start_offset_ms"] + df["duration_ms"]).max()
    n        = len(df)
    min_w_floor = total_ms * 0.0008

    bases      = []
    bar_widths = []
    bar_heights= []
    colors     = []
    opacities  = []
    customdata = []   # [color, component, operation, op_type, start_fmt, dur_fmt, end_fmt]

    for row in df.itertuples(index=False):
        col      = get_color(row.component)
        is_root  = str(row.operation_type).upper() in ("REQUEST_END", "PREFILL_LATENCY")
        opacity  = 0.40 if is_root else 1.0
        bar_h    = 0.20 if is_root else 0.60
        dur_ms   = float(row.duration_ms)
        start_ms = float(row.start_offset_ms)

        dur_fmt   = f"{dur_ms/1000:.3f}s" if dur_ms >= 1000 else f"{dur_ms:.3f}ms"
        start_fmt = f"{start_ms:.3f}ms"
        end_fmt   = f"{start_ms + dur_ms:.3f}ms"

        bases.append(start_ms)
        bar_widths.append(max(dur_ms, min_w_floor))
        bar_heights.append(bar_h)
        colors.append(col)
        opacities.append(opacity)
        customdata.append([col, row.component, row.operation,
                           str(row.operation_type), start_fmt, dur_fmt, end_fmt])

    # ── Single go.Bar trace — hover works across the full bar area ────────────
    fig = go.Figure(go.Bar(
        base=bases,
        x=bar_widths,
        y=list(range(n)),
        width=bar_heights,
        orientation="h",
        marker=dict(
            color=colors,
            opacity=opacities,
            line=dict(color=colors, width=1),
        ),
        customdata=customdata,
        hovertemplate=(
            "<b style='color:%{customdata[0]};font-size:13px'>%{customdata[1]}</b>"
            "<br><span style='color:#a0b4cc;font-size:11px'>%{customdata[2]}</span>"
            "<br><br><span style='color:#7b8fa8'>Start   </span>"
            "<b style='color:#f0f4ff'>%{customdata[4]}</b>"
            "<br><span style='color:#7b8fa8'>Duration</span>"
            "<b style='color:#f0f4ff'>%{customdata[5]}</b>"
            "<br><span style='color:#7b8fa8'>End     </span>"
            "<b style='color:#f0f4ff'>%{customdata[6]}</b>"
            "<br><span style='color:#7b8fa8'>Type    </span>"
            "<span style='color:#c084fc'>%{customdata[3]}</span>"
            "<extra></extra>"
        ),
        showlegend=False,
    ))

    fh = max(560, n * 56 + 180)

    # Two-line tick labels (vectorised via list comprehension)
    tick_labels = [
        (f"<b style='color:#e8eeff'>{r.component}</b>"
         f"<br><span style='color:#7b8fa8;font-size:9px'>{r.operation}</span>"
         if r.operation
         else f"<b style='color:#e8eeff'>{r.component}</b>")
        for r in df.itertuples(index=False)
    ]

    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="#080b14",
        plot_bgcolor="#0e1220",
        height=fh,
        margin=dict(l=8, r=24, t=44, b=52),
        font=dict(family="JetBrains Mono, monospace", size=11, color="#f0f4ff"),
        title=dict(
            text=(
                f"<span style='color:#7b8fa8'>Timeline</span>"
                f"  <span style='color:#253352'>|</span>"
                f"  <span style='color:#00d4ff;font-weight:700'>{n} events</span>"
                f"  <span style='color:#253352'>|</span>"
                f"  <span style='color:#a0b4cc'>{total_ms:.2f} ms total</span>"
            ),
            font=dict(size=12), x=0, xref="paper",
        ),
        showlegend=False,
        xaxis=dict(
            title=dict(text="Offset from first event (ms)",
                       font=dict(size=10, color="#7b8fa8")),
            gridcolor="#1a2540", zeroline=True,
            zerolinecolor="#2a3f5f", zerolinewidth=1,
            tickfont=dict(size=10, color="#8ba0bc"),
            tickformat=".2f",
            ticksuffix=" ms",
            nticks=12,
            showgrid=True,
            showline=True,
            linecolor="#253352",
            ticks="outside",
            ticklen=4,
            tickcolor="#253352",
            rangeslider=dict(visible=True, thickness=0.025, bgcolor="#080b14"),
        ),
        yaxis=dict(
            tickmode="array",
            tickvals=list(range(n)),
            ticktext=tick_labels,
            tickfont=dict(size=11, color="#c8d8ec"),
            gridcolor="#121b2e",
            autorange="reversed",
            automargin=True,
            ticks="outside",
            ticklen=20,
            tickwidth=1,
            tickcolor="#253352",
        ),
        hoverlabel=dict(
            bgcolor="#0e1220", bordercolor="#253352",
            font=dict(family="JetBrains Mono, monospace", size=11),
        ),
        dragmode="zoom",
        barmode="overlay",
    )

    return fig


# ── App ────────────────────────────────────────────────────────────────────────
app = Dash(__name__, title="Trace Timeline", suppress_callback_exceptions=True)

# ── Design tokens ──────────────────────────────────────────────────────────────
BG      = "#080b14"   # page background
SURFACE = "#0e1220"   # sidebar / top bar
SURFACE2= "#131829"   # cards, filter inputs, stat row
BORDER  = "#1f2d45"   # all borders — visible but not harsh
TEXT    = "#f0f4ff"   # primary text — near-white with a cool tint
MUTED   = "#7b8fa8"   # secondary labels — clearly readable
DIM     = "#3d5a7a"   # placeholder / hint text — visible but de-emphasised
ACCENT  = "#00d4ff"   # cyan — primary interactive colour
PURPLE  = "#c084fc"   # brighter purple for conversations
GREEN   = "#4ade80"   # brighter green for traces / positive values

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
                    fontSize="12px", fontWeight="700", color="#e8eeff",
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
                    fontSize="11px", fontWeight="600", color="#c084fc",
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
                        fontSize="10px", fontWeight="600", color="#7ec8a0",
                        fontFamily="JetBrains Mono, monospace",
                        display="block", lineHeight="1.4",
                    ),
                ),
                html.Span(label_extra, style=dict(
                    fontSize="9px", color="#5a8a6a",
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

            # Refresh button
            html.Button(
                "⟳  Refresh",
                id="btn-refresh",
                n_clicks=0,
                style=dict(
                    background="transparent",
                    border=f"1px solid {BORDER}",
                    borderRadius="6px",
                    color=MUTED,
                    fontSize="11px",
                    fontFamily="JetBrains Mono, monospace",
                    padding="6px 14px",
                    cursor="pointer",
                    whiteSpace="nowrap",
                    flexShrink="0",
                    transition="border-color 0.15s, color 0.15s",
                ),
            ),
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
        dcc.Store(id="store-expanded", data={}),
    ]
)


# ── CALLBACKS ──────────────────────────────────────────────────────────────────

# 0. Refresh button — clears entire server-side cache so next render re-fetches
@app.callback(
    Output("store-expanded", "data", allow_duplicate=True),
    Input("btn-refresh", "n_clicks"),
    prevent_initial_call=True,
)
def on_refresh(n):
    _cache_clear_all()
    return {}   # also collapse the tree so it re-fetches user list fresh


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

    # Build cache keys
    u_ck = f"_cf_users|{ts_where}"
    c_ck = f"_cf_convs|{f_user}|{ts_where}"
    t_ck = f"_cf_traces|{f_user}|{f_conv}|{ts_where}"

    u_opts = c_opts = t_opts = []

    def fetch_users():
        nonlocal u_opts
        v = _cache_get(u_ck)
        if v is None:
            try:
                df = run_query(f"SELECT DISTINCT user_id FROM {FULL_TABLE} WHERE user_id IS NOT NULL{ts_where} ORDER BY user_id")
                v = [{"label": x, "value": x} for x in df["user_id"].tolist()]
                _cache_set(u_ck, v)
            except Exception as e:
                print(f"[cascade_filters/users] {e}")
                v = []
        u_opts = v

    def fetch_convs():
        nonlocal c_opts
        v = _cache_get(c_ck)
        if v is None:
            try:
                c_where = f" AND user_id = '{f_user}'" if f_user else ""
                df = run_query(f"SELECT DISTINCT conversation_id FROM {FULL_TABLE} WHERE conversation_id IS NOT NULL{c_where}{ts_where} ORDER BY conversation_id")
                v = [{"label": x, "value": x} for x in df["conversation_id"].tolist()]
                _cache_set(c_ck, v)
            except Exception as e:
                print(f"[cascade_filters/convs] {e}")
                v = []
        c_opts = v

    def fetch_traces():
        nonlocal t_opts
        v = _cache_get(t_ck)
        if v is None:
            try:
                t_where = ""
                if f_user: t_where += f" AND user_id = '{f_user}'"
                if f_conv: t_where += f" AND conversation_id = '{f_conv}'"
                df = run_query(f"SELECT DISTINCT trace_id FROM {FULL_TABLE} WHERE trace_id IS NOT NULL{t_where}{ts_where} ORDER BY trace_id")
                v = [{"label": x, "value": x} for x in df["trace_id"].tolist()]
                _cache_set(t_ck, v)
            except Exception as e:
                print(f"[cascade_filters/traces] {e}")
                v = []
        t_opts = v

    threads = [threading.Thread(target=f) for f in (fetch_users, fetch_convs, fetch_traces)]
    for t in threads: t.start()
    for t in threads: t.join()

    return u_opts, c_opts, t_opts, clear_conv, clear_trace


# 2a. Manage expanded state — handles both filter resets and node toggles
#     Separated from rendering so re-drawing the tree never eats a click.
@app.callback(
    Output("store-expanded", "data"),
    Input("f-user",  "value"),
    Input("f-conv",  "value"),
    Input("f-trace", "value"),
    Input("f-from",  "value"),
    Input("f-to",    "value"),
    Input({"type": "user-row", "id": ALL}, "n_clicks"),
    Input({"type": "conv-row", "id": ALL}, "n_clicks"),
    State("store-expanded", "data"),
    prevent_initial_call=False,
)
def manage_expanded(f_user, f_conv, f_trace, f_from, f_to,
                    user_clicks, conv_clicks, expanded):
    ctx      = callback_context
    expanded = expanded or {}

    triggered_props = [t["prop_id"] for t in ctx.triggered] if ctx.triggered else []
    filter_changed  = any(p.startswith("f-") for p in triggered_props)

    if filter_changed:
        # Clear server-side caches so next render re-fetches with new filters
        _cache_clear_prefix("_tree_")
        # Also clear cascade filter cache if time window changed
        if any("f-from" in p or "f-to" in p for p in triggered_props):
            _cache_clear_prefix("_cf_")
        expanded = {}
        if f_user:
            expanded[f_user] = True
        return expanded

    # Toggle the clicked node only
    for t in ctx.triggered:
        pid = t["prop_id"]
        if t["value"] and ("user-row" in pid or "conv-row" in pid):
            key = json.loads(pid.split(".")[0])["id"]
            expanded[key] = not expanded.get(key, False)

    return expanded


# 2b. Render tree from expanded state + filters — pure rendering, no state writes
#     Uses server-side _cache dict (no dcc.Store round-trip overhead).
@app.callback(
    Output("tree-root", "children"),
    Input("store-expanded", "data"),
    State("f-user",  "value"),
    State("f-conv",  "value"),
    State("f-trace", "value"),
    State("f-from",  "value"),
    State("f-to",    "value"),
    prevent_initial_call=False,
)
def render_tree(expanded, f_user, f_conv, f_trace, f_from, f_to):
    expanded = expanded or {}

    where_parts = ["1=1"]
    if f_user:  where_parts.append(f"user_id = '{f_user}'")
    if f_conv:  where_parts.append(f"conversation_id = '{f_conv}'")
    if f_trace: where_parts.append(f"trace_id = '{f_trace}'")
    if f_from:  where_parts.append(f"timestamp >= CAST('{fmt_ts(f_from)}' AS TIMESTAMP)")
    if f_to:    where_parts.append(f"timestamp <= CAST('{fmt_ts(f_to)}' AS TIMESTAMP)")
    where = " AND ".join(where_parts)

    # ── Fetch users ────────────────────────────────────────────────────────────
    u_key = f"_tree_users|{where}"
    users = _cache_get(u_key)
    if users is None:
        try:
            df = run_query(f"""
                SELECT DISTINCT user_id FROM {FULL_TABLE}
                WHERE {where} AND user_id IS NOT NULL ORDER BY user_id
            """)
            users = df["user_id"].tolist()
            _cache_set(u_key, users)
        except Exception as e:
            print(f"[render_tree/users] {e}")
            return [html.P(str(e), style=dict(color="#ef4444", fontSize="10px",
                                              padding="10px",
                                              fontFamily="JetBrains Mono, monospace"))]

    tree = []
    for uid in users:
        u_exp      = expanded.get(uid, False)
        u_children = []

        if u_exp:
            c_key = f"_tree_convs|{uid}|{where}"
            convs = _cache_get(c_key)
            if convs is None:
                try:
                    df = run_query(f"""
                        SELECT DISTINCT conversation_id FROM {FULL_TABLE}
                        WHERE user_id = '{uid}'
                          AND conversation_id IS NOT NULL AND {where}
                        ORDER BY conversation_id
                    """)
                    convs = df["conversation_id"].tolist()
                    _cache_set(c_key, convs)
                except Exception as e:
                    print(f"[render_tree/convs] {e}")
                    convs = []

            for cid in convs:
                ck    = f"{uid}||{cid}"
                c_exp = expanded.get(ck, False)
                c_children = []

                if c_exp:
                    t_key = f"_tree_traces|{uid}|{cid}|{where}"
                    traces = _cache_get(t_key)
                    if traces is None:
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
                            traces = df.to_dict("records")
                            _cache_set(t_key, traces)
                        except Exception as e:
                            print(f"[render_tree/traces] {e}")
                            traces = []

                    for tr in traces:
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

    return tree


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
        color="#8ba0bc" if header else "#c8d8ec",
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
                html.Span(uid, style=dict(color="#8ba0bc")),
                html.Span(" → ", style=dict(color=DIM)),
                html.Span("💬", style=dict(marginRight="4px")),
                html.Span(cid, style=dict(color="#8ba0bc")),
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
                              html.Pre(str(e), style=dict(color="#7b8fa8", fontSize="10px",
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
                html.Span(f_user, style=dict(color="#8ba0bc")),
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
