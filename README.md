# Trace Execution Timeline — Databricks App

Interactive timeline dashboard with cascading filters:
User ID → Conversation ID → Trace ID → Gantt chart

## Files

```
trace_timeline_app/
├── app.py              # Main Dash application
├── app.yaml            # Databricks App manifest
├── requirements.txt    # Python dependencies
└── assets/
    └── style.css       # Dark theme styles
```

## Deployment Steps

### Step 1 — Upload to Databricks Workspace

Option A (Databricks CLI):
```bash
databricks workspace import-dir ./trace_timeline_app /Workspace/Users/your@email.com/trace_timeline_app --overwrite
```

Option B (UI):
- Workspace → your folder → right-click → Import
- Upload all files maintaining the folder structure

### Step 2 — Edit app.yaml

Open app.yaml and replace:
- `your_warehouse_id`  → your SQL Warehouse HTTP path
  (find it: SQL Warehouses → your warehouse → Connection details → HTTP Path)
- `your_catalog`       → your Unity Catalog catalog name
- `your_schema`        → your schema name
- `trace_events`       → your actual table name

### Step 3 — Create the Databricks App

1. Left sidebar → Apps (or Compute → Apps)
2. Click "Create App"
3. Choose "Custom" (not a template)
4. Name: trace-timeline
5. Source: point to your workspace folder  /Workspace/Users/.../trace_timeline_app
6. Click "Deploy"

### Step 4 — Set permissions

In the App settings → Permissions:
- Add your team members as "Can View" or "Can Manage"

### Step 5 — Open the App

Click the URL shown after deployment. The app will show:
- User ID dropdown (populated from your table)
- Select a user → Conversation IDs appear
- Select a conversation → Trace IDs appear (with event count + total ms)
- Select a trace → Interactive Gantt timeline renders

## How the timeline works

- X axis = time offset in ms from the first event in the trace
- Each row = one component · operation
- Root spans (REQUEST_END, PREFILL_LATENCY) shown as thin transparent bars
- Component spans shown as full bars
- Hover over any bar for exact start offset, duration, end offset
- Use the rangeslider at the bottom to zoom into sub-ms events
- Plotly zoom/pan tools in the top-right of the chart

## Troubleshooting

| Error | Fix |
|-------|-----|
| No users in dropdown | Check CATALOG, SCHEMA, TABLE in app.yaml |
| Connection error | Verify HTTP_PATH and that SQL Warehouse is running |
| Empty timeline | Check that trace_id/user_id/conversation_id values match exactly |
| start_offset_ms all 0 | UNIX_MICROS() requires Databricks Runtime 10.4+. Use DATEDIFF fallback in app.py |

## start_time calculation

The table has `timestamp` (event end time) and `duration_ms`.
start_time = timestamp - duration_ms

In SQL:
```sql
CAST(
    (UNIX_MICROS(timestamp) - CAST(duration_ms * 1000 AS BIGINT))
AS DOUBLE) / 1000.0   AS start_epoch_ms
```
