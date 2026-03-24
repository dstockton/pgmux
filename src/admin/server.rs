use anyhow::Result;
use axum::{
    extract::State,
    http::StatusCode,
    response::{Html, IntoResponse, Json},
    routing::get,
    Router,
};
use std::sync::Arc;
use tracing::info;

use super::metrics::Metrics;
use crate::config::Config;
use crate::monitor::DbSizeMonitor;
use crate::pool::PoolManager;

/// Shared state for the admin server.
#[derive(Clone)]
#[allow(dead_code)]
pub struct AdminState {
    pub cfg: Arc<Config>,
    pub metrics: Arc<Metrics>,
    pub pool_manager: Arc<PoolManager>,
    pub size_monitor: Arc<DbSizeMonitor>,
}

/// Start the admin HTTP server.
pub async fn run(
    cfg: Arc<Config>,
    metrics: Arc<Metrics>,
    pool_manager: Arc<PoolManager>,
    size_monitor: Arc<DbSizeMonitor>,
) -> Result<()> {
    let state = AdminState {
        cfg: cfg.clone(),
        metrics,
        pool_manager,
        size_monitor,
    };

    let app = Router::new()
        .route("/", get(dashboard_handler))
        .route("/health", get(health_handler))
        .route("/metrics", get(metrics_handler))
        .route("/api/stats", get(stats_handler))
        .route("/api/pools", get(pools_handler))
        .route("/api/databases", get(databases_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&cfg.admin_listen_addr).await?;
    info!("Admin server listening on {}", cfg.admin_listen_addr);
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health_handler() -> impl IntoResponse {
    Json(serde_json::json!({"status": "ok"}))
}

async fn metrics_handler(State(state): State<AdminState>) -> impl IntoResponse {
    let mut buf = String::new();
    prometheus_client::encoding::text::encode(&mut buf, &state.metrics.registry)
        .expect("encoding metrics");
    (
        StatusCode::OK,
        [(
            "content-type",
            "application/openmetrics-text; version=1.0.0; charset=utf-8",
        )],
        buf,
    )
}

async fn stats_handler(State(state): State<AdminState>) -> impl IntoResponse {
    let pool_stats = state.pool_manager.get_stats();
    let db_info = state.size_monitor.get_all_db_info();

    Json(serde_json::json!({
        "total_server_connections": state.pool_manager.total_connections(),
        "pools": pool_stats,
        "databases": db_info,
    }))
}

async fn pools_handler(State(state): State<AdminState>) -> impl IntoResponse {
    Json(state.pool_manager.get_stats())
}

async fn databases_handler(State(state): State<AdminState>) -> impl IntoResponse {
    Json(state.size_monitor.get_all_db_info())
}

async fn dashboard_handler(State(state): State<AdminState>) -> impl IntoResponse {
    let pool_stats = state.pool_manager.get_stats();
    let db_info = state.size_monitor.get_all_db_info();
    let total_conns = state.pool_manager.total_connections();

    Html(generate_dashboard_html(pool_stats, db_info, total_conns))
}

fn generate_dashboard_html(
    pools: Vec<crate::pool::PoolStats>,
    databases: Vec<crate::monitor::DbSizeInfo>,
    total_connections: u64,
) -> String {
    let pools_json = serde_json::to_string(&pools).unwrap_or_else(|_| "[]".to_string());
    let dbs_json = serde_json::to_string(&databases).unwrap_or_else(|_| "[]".to_string());

    format!(
        r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>PgMux Dashboard</title>
<style>
  :root {{
    --bg: #0f1117;
    --card-bg: #1a1d28;
    --border: #2a2d3a;
    --text: #e1e4ec;
    --text-dim: #8b8fa3;
    --accent: #6366f1;
    --accent-light: #818cf8;
    --green: #22c55e;
    --red: #ef4444;
    --yellow: #eab308;
    --chart-grid: #2a2d3a;
  }}
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background: var(--bg);
    color: var(--text);
    line-height: 1.6;
  }}
  .container {{ max-width: 1400px; margin: 0 auto; padding: 24px; }}
  header {{
    display: flex; align-items: center; justify-content: space-between;
    margin-bottom: 32px; padding-bottom: 16px;
    border-bottom: 1px solid var(--border);
  }}
  header h1 {{
    font-size: 24px; font-weight: 700;
    background: linear-gradient(135deg, var(--accent), var(--accent-light));
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
  }}
  .refresh-info {{ color: var(--text-dim); font-size: 13px; }}
  .stats-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
    gap: 16px; margin-bottom: 32px;
  }}
  .stat-card {{
    background: var(--card-bg); border: 1px solid var(--border);
    border-radius: 12px; padding: 20px;
  }}
  .stat-card .label {{ color: var(--text-dim); font-size: 13px; text-transform: uppercase; letter-spacing: 0.5px; }}
  .stat-card .value {{ font-size: 36px; font-weight: 700; margin-top: 4px; }}
  .stat-card .sub {{ color: var(--text-dim); font-size: 13px; margin-top: 4px; }}
  .section {{ margin-bottom: 32px; }}
  .section h2 {{ font-size: 18px; font-weight: 600; margin-bottom: 16px; }}
  table {{
    width: 100%; border-collapse: collapse;
    background: var(--card-bg); border: 1px solid var(--border);
    border-radius: 12px; overflow: hidden;
  }}
  th, td {{ padding: 12px 16px; text-align: left; border-bottom: 1px solid var(--border); }}
  th {{ color: var(--text-dim); font-size: 12px; text-transform: uppercase; letter-spacing: 0.5px; font-weight: 600; }}
  td {{ font-size: 14px; }}
  tr:last-child td {{ border-bottom: none; }}
  .badge {{
    display: inline-block; padding: 2px 8px; border-radius: 6px;
    font-size: 12px; font-weight: 600;
  }}
  .badge-ok {{ background: rgba(34,197,94,0.15); color: var(--green); }}
  .badge-warn {{ background: rgba(234,179,8,0.15); color: var(--yellow); }}
  .badge-danger {{ background: rgba(239,68,68,0.15); color: var(--red); }}
  .progress-bar {{
    width: 100%; height: 8px; background: var(--border);
    border-radius: 4px; overflow: hidden; margin-top: 4px;
  }}
  .progress-fill {{ height: 100%; border-radius: 4px; transition: width 0.5s ease; }}
  .chart-container {{
    background: var(--card-bg); border: 1px solid var(--border);
    border-radius: 12px; padding: 20px; margin-bottom: 16px;
  }}
  canvas {{ width: 100% !important; }}
  .charts-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }}
  @media (max-width: 768px) {{ .charts-grid {{ grid-template-columns: 1fr; }} }}
</style>
</head>
<body>
<div class="container">
  <header>
    <h1>PgMux</h1>
    <div class="refresh-info">Auto-refresh: 5s | <span id="last-update"></span></div>
  </header>

  <div class="stats-grid">
    <div class="stat-card">
      <div class="label">Client Connections</div>
      <div class="value" id="client-conns">-</div>
      <div class="sub">Active sessions</div>
    </div>
    <div class="stat-card">
      <div class="label">Server Connections</div>
      <div class="value" id="server-conns">{total_connections}</div>
      <div class="sub">Backend pool total</div>
    </div>
    <div class="stat-card">
      <div class="label">Connection Pools</div>
      <div class="value" id="pool-count">{pool_count}</div>
      <div class="sub">Unique DB/role combos</div>
    </div>
    <div class="stat-card">
      <div class="label">Databases Monitored</div>
      <div class="value" id="db-count">{db_count}</div>
      <div class="sub" id="db-over-count">0 over limit</div>
    </div>
  </div>

  <div class="charts-grid">
    <div class="chart-container">
      <h2>Connections Over Time</h2>
      <canvas id="conns-chart" height="200"></canvas>
    </div>
    <div class="chart-container">
      <h2>Database Sizes</h2>
      <canvas id="db-chart" height="200"></canvas>
    </div>
  </div>

  <div class="section">
    <h2>Connection Pools</h2>
    <table>
      <thead>
        <tr><th>Pool Key</th><th>Active</th><th>Idle</th><th>Total Acquired</th><th>Errors</th><th>Status</th></tr>
      </thead>
      <tbody id="pools-body">
        {pools_rows}
      </tbody>
    </table>
  </div>

  <div class="section">
    <h2>Database Size Enforcement</h2>
    <table>
      <thead>
        <tr><th>Database</th><th>Current Size</th><th>Limit</th><th>Usage</th><th>Status</th></tr>
      </thead>
      <tbody id="dbs-body">
        {dbs_rows}
      </tbody>
    </table>
  </div>
</div>

<script>
const poolsData = {pools_json};
const dbsData = {dbs_json};
const connHistory = {{ labels: [], clients: [], servers: [] }};

function formatBytes(bytes) {{
  if (bytes == null) return '-';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let i = 0;
  let val = bytes;
  while (val >= 1024 && i < units.length - 1) {{ val /= 1024; i++; }}
  return val.toFixed(i > 0 ? 2 : 0) + ' ' + units[i];
}}

function drawChart(canvasId, labels, datasets) {{
  const canvas = document.getElementById(canvasId);
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.getBoundingClientRect();
  canvas.width = rect.width * dpr;
  canvas.height = rect.height * dpr;
  ctx.scale(dpr, dpr);
  const w = rect.width, h = rect.height;
  const pad = {{ top: 20, right: 20, bottom: 30, left: 50 }};
  const cw = w - pad.left - pad.right, ch = h - pad.top - pad.bottom;

  ctx.clearRect(0, 0, w, h);

  if (!datasets.length || !datasets[0].data.length) {{
    ctx.fillStyle = '#8b8fa3';
    ctx.font = '14px sans-serif';
    ctx.textAlign = 'center';
    ctx.fillText('Waiting for data...', w/2, h/2);
    return;
  }}

  let maxVal = 0;
  datasets.forEach(ds => ds.data.forEach(v => {{ if (v > maxVal) maxVal = v; }}));
  if (maxVal === 0) maxVal = 1;
  maxVal *= 1.1;

  // Grid
  ctx.strokeStyle = '#2a2d3a';
  ctx.lineWidth = 1;
  for (let i = 0; i <= 4; i++) {{
    const y = pad.top + ch - (ch * i / 4);
    ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(pad.left + cw, y); ctx.stroke();
    ctx.fillStyle = '#8b8fa3'; ctx.font = '11px sans-serif'; ctx.textAlign = 'right';
    ctx.fillText(Math.round(maxVal * i / 4).toString(), pad.left - 8, y + 4);
  }}

  // Lines
  datasets.forEach(ds => {{
    ctx.strokeStyle = ds.color;
    ctx.lineWidth = 2;
    ctx.beginPath();
    ds.data.forEach((v, i) => {{
      const x = pad.left + (cw * i / Math.max(ds.data.length - 1, 1));
      const y = pad.top + ch - (ch * v / maxVal);
      if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    }});
    ctx.stroke();

    // Fill
    ctx.globalAlpha = 0.1;
    ctx.fillStyle = ds.color;
    ctx.lineTo(pad.left + cw, pad.top + ch);
    ctx.lineTo(pad.left, pad.top + ch);
    ctx.fill();
    ctx.globalAlpha = 1;
  }});

  // Legend
  let lx = pad.left;
  datasets.forEach(ds => {{
    ctx.fillStyle = ds.color;
    ctx.fillRect(lx, pad.top - 16, 12, 12);
    ctx.fillStyle = '#e1e4ec'; ctx.font = '12px sans-serif'; ctx.textAlign = 'left';
    ctx.fillText(ds.label, lx + 16, pad.top - 6);
    lx += ctx.measureText(ds.label).width + 36;
  }});
}}

function drawBarChart(canvasId, labels, values, limits) {{
  const canvas = document.getElementById(canvasId);
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.getBoundingClientRect();
  canvas.width = rect.width * dpr;
  canvas.height = rect.height * dpr;
  ctx.scale(dpr, dpr);
  const w = rect.width, h = rect.height;
  const pad = {{ top: 20, right: 20, bottom: 40, left: 60 }};
  const cw = w - pad.left - pad.right, ch = h - pad.top - pad.bottom;

  ctx.clearRect(0, 0, w, h);

  if (!labels.length) {{
    ctx.fillStyle = '#8b8fa3'; ctx.font = '14px sans-serif'; ctx.textAlign = 'center';
    ctx.fillText('No databases tracked', w/2, h/2);
    return;
  }}

  let maxVal = 0;
  values.forEach(v => {{ if (v > maxVal) maxVal = v; }});
  limits.forEach(v => {{ if (v && v > maxVal) maxVal = v; }});
  if (maxVal === 0) maxVal = 1;
  maxVal *= 1.1;

  const barW = Math.min(40, cw / labels.length * 0.6);
  const gap = cw / labels.length;

  labels.forEach((label, i) => {{
    const x = pad.left + gap * i + gap / 2 - barW / 2;
    const barH = ch * (values[i] || 0) / maxVal;
    const y = pad.top + ch - barH;

    const overLimit = limits[i] && values[i] > limits[i];
    ctx.fillStyle = overLimit ? '#ef4444' : '#6366f1';
    ctx.fillRect(x, y, barW, barH);

    // Limit line
    if (limits[i]) {{
      const ly = pad.top + ch - ch * limits[i] / maxVal;
      ctx.strokeStyle = '#eab308';
      ctx.lineWidth = 2;
      ctx.setLineDash([4, 4]);
      ctx.beginPath(); ctx.moveTo(x - 4, ly); ctx.lineTo(x + barW + 4, ly); ctx.stroke();
      ctx.setLineDash([]);
    }}

    // Label
    ctx.fillStyle = '#8b8fa3'; ctx.font = '11px sans-serif'; ctx.textAlign = 'center';
    ctx.save(); ctx.translate(x + barW/2, pad.top + ch + 8);
    ctx.fillText(label.length > 12 ? label.substring(0, 12) + '...' : label, 0, 10);
    ctx.restore();
  }});

  // Y-axis labels
  for (let i = 0; i <= 4; i++) {{
    const y = pad.top + ch - ch * i / 4;
    ctx.fillStyle = '#8b8fa3'; ctx.font = '11px sans-serif'; ctx.textAlign = 'right';
    ctx.fillText(formatBytes(maxVal * i / 4), pad.left - 8, y + 4);
  }}
}}

async function refresh() {{
  try {{
    const resp = await fetch('/api/stats');
    const data = await resp.json();

    document.getElementById('server-conns').textContent = data.total_server_connections;
    document.getElementById('pool-count').textContent = data.pools.length;
    document.getElementById('db-count').textContent = data.databases.length;

    const overCount = data.databases.filter(d => d.over_limit).length;
    document.getElementById('db-over-count').textContent = overCount + ' over limit';
    document.getElementById('db-over-count').style.color = overCount > 0 ? '#ef4444' : '#8b8fa3';

    // Update pools table
    const tbody = document.getElementById('pools-body');
    tbody.innerHTML = data.pools.map(p => `
      <tr>
        <td>${{p.key}}</td>
        <td>${{p.active_connections}}</td>
        <td>${{p.idle_connections}}</td>
        <td>${{p.total_acquired}}</td>
        <td>${{p.total_errors}}</td>
        <td>${{p.total_errors > 0 ? '<span class="badge badge-warn">errors</span>' : '<span class="badge badge-ok">healthy</span>'}}</td>
      </tr>
    `).join('');

    // Update databases table
    const dbody = document.getElementById('dbs-body');
    dbody.innerHTML = data.databases.map(d => {{
      const pct = d.size_bytes && d.limit_bytes ? Math.round(d.size_bytes / d.limit_bytes * 100) : null;
      const color = d.over_limit ? '#ef4444' : pct && pct > 80 ? '#eab308' : '#22c55e';
      return `
        <tr>
          <td>${{d.database}}</td>
          <td>${{formatBytes(d.size_bytes)}}</td>
          <td>${{d.limit_bytes ? formatBytes(d.limit_bytes) : 'unlimited'}}</td>
          <td>
            ${{pct !== null ? `
              <div>${{pct}}%</div>
              <div class="progress-bar"><div class="progress-fill" style="width:${{Math.min(pct,100)}}%;background:${{color}}"></div></div>
            ` : '-'}}
          </td>
          <td>${{d.over_limit ? '<span class="badge badge-danger">READ-ONLY</span>' : '<span class="badge badge-ok">OK</span>'}}</td>
        </tr>
      `;
    }}).join('');

    // Update charts
    const now = new Date().toLocaleTimeString();
    connHistory.labels.push(now);
    connHistory.servers.push(data.total_server_connections);
    if (connHistory.labels.length > 60) {{
      connHistory.labels.shift();
      connHistory.servers.shift();
    }}

    drawChart('conns-chart', connHistory.labels, [
      {{ label: 'Server Connections', data: connHistory.servers, color: '#6366f1' }},
    ]);

    const dbLabels = data.databases.map(d => d.database);
    const dbValues = data.databases.map(d => d.size_bytes || 0);
    const dbLimits = data.databases.map(d => d.limit_bytes || null);
    drawBarChart('db-chart', dbLabels, dbValues, dbLimits);

    document.getElementById('last-update').textContent = now;
  }} catch (e) {{
    console.error('Refresh failed:', e);
  }}
}}

// Initial render
refresh();
setInterval(refresh, 5000);
</script>
</body>
</html>"##,
        total_connections = total_connections,
        pool_count = pools.len(),
        db_count = databases.len(),
        pools_json = pools_json,
        dbs_json = dbs_json,
        pools_rows = pools
            .iter()
            .map(|p| format!(
                "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                p.key,
                p.active_connections,
                p.idle_connections,
                p.total_acquired,
                p.total_errors,
                if p.total_errors > 0 {
                    r#"<span class="badge badge-warn">errors</span>"#
                } else {
                    r#"<span class="badge badge-ok">healthy</span>"#
                }
            ))
            .collect::<Vec<_>>()
            .join("\n"),
        dbs_rows = databases
            .iter()
            .map(|d| {
                let size_str = d
                    .size_bytes
                    .map(crate::monitor::format_bytes)
                    .unwrap_or_else(|| "-".to_string());
                let limit_str = d
                    .limit_bytes
                    .map(crate::monitor::format_bytes)
                    .unwrap_or_else(|| "unlimited".to_string());
                let status = if d.over_limit {
                    r#"<span class="badge badge-danger">READ-ONLY</span>"#
                } else {
                    r#"<span class="badge badge-ok">OK</span>"#
                };
                format!(
                    "<tr><td>{}</td><td>{}</td><td>{}</td><td>-</td><td>{}</td></tr>",
                    d.database, size_str, limit_str, status
                )
            })
            .collect::<Vec<_>>()
            .join("\n"),
    )
}
