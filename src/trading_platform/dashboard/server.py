from __future__ import annotations

import html
import json
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Callable
from urllib.parse import parse_qs
from wsgiref.simple_server import make_server

from trading_platform.dashboard.service import DashboardDataService


def _json_response(payload: dict) -> tuple[str, list[tuple[str, str]], bytes]:
    return (
        "200 OK",
        [("Content-Type", "application/json; charset=utf-8")],
        json.dumps(payload, indent=2, default=str).encode("utf-8"),
    )


def _not_found() -> tuple[str, list[tuple[str, str]], bytes]:
    return "404 Not Found", [("Content-Type", "application/json; charset=utf-8")], b'{"error":"not_found"}'


def _page_shell(title: str, body: str) -> bytes:
    nav = """
<div class="app-shell">
  <header class="topbar">
    <div class="brand-block">
      <div class="eyebrow">Internal Trading Terminal</div>
      <h1>Trading Platform Dashboard</h1>
      <div class="muted topbar-copy">Read-only artifact monitor for research, portfolio, execution, and live readiness.</div>
    </div>
    <div class="topbar-meta">
      <span class="meta-chip">Artifact-driven</span>
      <span class="meta-chip">Read-only</span>
      <span class="meta-chip">Local</span>
    </div>
  </header>
  <nav class="nav-strip">
    <a href="/">Overview</a>
    <a href="/research">Research</a>
    <a href="/strategies">Strategies</a>
    <a href="/portfolio">Portfolio</a>
    <a href="/execution">Execution</a>
    <a href="/live">Live</a>
    <a href="/runs">Runs</a>
  </nav>
  <main class="page-shell">
"""
    css = """
html { color-scheme: dark; }
* { box-sizing: border-box; }
body {
  font-family: "Segoe UI", "Inter", Arial, sans-serif;
  margin: 0;
  background:
    radial-gradient(circle at top left, rgba(68, 211, 255, 0.08), transparent 26%),
    radial-gradient(circle at top right, rgba(77, 124, 255, 0.10), transparent 22%),
    linear-gradient(180deg, #06111f 0%, #081522 38%, #09131d 100%);
  color: #e5eef8;
  min-height: 100vh;
}
a { color: #89c6ff; }
.app-shell { max-width: 1480px; margin: 0 auto; padding: 24px 24px 48px; }
.topbar {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 18px;
  padding: 24px 26px;
  margin-bottom: 14px;
  border: 1px solid rgba(135, 168, 204, 0.18);
  border-radius: 20px;
  background: linear-gradient(135deg, rgba(15, 30, 50, 0.95), rgba(11, 21, 34, 0.92));
  box-shadow: 0 24px 60px rgba(0, 0, 0, 0.34);
}
.brand-block h1 {
  margin: 4px 0 6px;
  font-size: 1.75rem;
  letter-spacing: -0.03em;
}
.eyebrow {
  text-transform: uppercase;
  letter-spacing: 0.14em;
  font-size: 0.72rem;
  color: #7eb6ff;
  font-weight: 700;
}
.topbar-copy { max-width: 720px; }
.topbar-meta { display: flex; gap: 8px; flex-wrap: wrap; justify-content: flex-end; }
.meta-chip {
  display: inline-flex;
  align-items: center;
  padding: 7px 10px;
  border-radius: 999px;
  background: rgba(96, 165, 250, 0.10);
  border: 1px solid rgba(96, 165, 250, 0.20);
  color: #d8ebff;
  font-size: 0.82rem;
}
.nav-strip {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  padding: 10px 0 20px;
}
.nav-strip a {
  text-decoration: none;
  color: #c9d6e5;
  font-weight: 600;
  padding: 9px 14px;
  border-radius: 999px;
  border: 1px solid rgba(135, 168, 204, 0.14);
  background: rgba(13, 25, 39, 0.78);
}
.nav-strip a:hover { border-color: rgba(137, 198, 255, 0.45); color: #f3f8fd; }
.page-shell { padding: 6px 0 12px; }
h2 {
  margin: 28px 0 12px;
  font-size: 1rem;
  text-transform: uppercase;
  letter-spacing: 0.10em;
  color: #96aeca;
}
.grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 14px; margin-bottom: 24px; }
.card, .summary-item, .chart-panel, .table-wrap, .empty-state {
  background: linear-gradient(180deg, rgba(12, 23, 36, 0.95), rgba(10, 18, 29, 0.96));
  border: 1px solid rgba(135, 168, 204, 0.14);
  border-radius: 18px;
  box-shadow: inset 0 1px 0 rgba(255,255,255,0.03), 0 18px 40px rgba(0,0,0,0.26);
}
.card {
  padding: 16px 18px;
  min-height: 120px;
}
.metric {
  font-size: 1.9rem;
  font-weight: 800;
  margin: 8px 0;
  letter-spacing: -0.03em;
  color: #f5fbff;
}
.muted { color: #8da4be; font-size: 0.92rem; }
.card-label {
  text-transform: uppercase;
  letter-spacing: 0.09em;
  font-size: 0.72rem;
  color: #7ea4cc;
  font-weight: 700;
}
.card-detail { color: #8da4be; font-size: 0.88rem; line-height: 1.4; }
.table-wrap { overflow: auto; margin: 12px 0 24px; }
table {
  width: 100%;
  border-collapse: collapse;
  background: transparent;
}
th, td {
  text-align: left;
  padding: 11px 12px;
  border-bottom: 1px solid rgba(135, 168, 204, 0.10);
  vertical-align: top;
  font-size: 0.92rem;
}
th {
  background: rgba(17, 31, 47, 0.94);
  color: #99aec7;
  position: sticky;
  top: 0;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  font-size: 0.72rem;
}
tr:hover td { background: rgba(255,255,255,0.015); }
.badge {
  display: inline-block;
  padding: 4px 9px;
  border-radius: 999px;
  font-size: 0.76rem;
  font-weight: 700;
  border: 1px solid rgba(255,255,255,0.08);
}
.healthy,.pass,.approved,.succeeded,.filled,.open,.long { background: rgba(16, 185, 129, 0.14); color: #7af0be; }
.warning,.warn,.paper,.candidate,.pending { background: rgba(245, 158, 11, 0.14); color: #f5c26b; }
.critical,.fail,.failed,.live_disabled,.retired,.rejected,.canceled,.short,.closed { background: rgba(239, 68, 68, 0.14); color: #ff9d9d; }
.chart {
  display: flex;
  gap: 12px;
  align-items: end;
  min-height: 160px;
  padding: 18px 12px 10px;
  overflow-x: auto;
}
.bar-wrap { display: flex; flex-direction: column; align-items: center; gap: 8px; width: 42px; color: #9fb4ca; }
.bar { width: 30px; background: linear-gradient(180deg, #46a0ff, #1b4d8c); border-radius: 10px 10px 2px 2px; box-shadow: 0 0 18px rgba(70,160,255,0.20); }
.subnav { display: flex; justify-content: space-between; align-items: center; margin-bottom: 18px; gap: 12px; flex-wrap: wrap; }
.link { color: #8dcaff; font-weight: 600; text-decoration: none; }
.link:hover { color: #d3ecff; }
.summary-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 12px; margin-bottom: 20px; }
.summary-item { padding: 14px 16px; }
.summary-item .label {
  color: #80a3c8;
  font-size: 0.72rem;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.09em;
}
.summary-item .value { font-size: 1.22rem; font-weight: 800; margin-top: 8px; color: #f5fbff; letter-spacing: -0.03em; }
.chart-panel { padding: 18px; margin-bottom: 20px; }
.chart-panel h2 { margin-top: 0; }
.chart-frame {
  width: 100%;
  min-height: 360px;
  background:
    linear-gradient(180deg, rgba(20, 38, 58, 0.88), rgba(10, 20, 32, 0.94)),
    repeating-linear-gradient(0deg, rgba(255,255,255,0.02) 0 1px, transparent 1px 48px);
  border: 1px solid rgba(135, 168, 204, 0.12);
  border-radius: 16px;
}
.legend { display: flex; flex-wrap: wrap; gap: 12px; margin: 12px 0 0; color: #90a9c3; font-size: 0.84rem; }
.legend span { display: inline-flex; gap: 8px; align-items: center; padding: 6px 10px; border-radius: 999px; background: rgba(255,255,255,0.03); border: 1px solid rgba(135, 168, 204, 0.10); }
.swatch { width: 10px; height: 10px; border-radius: 999px; display: inline-block; box-shadow: 0 0 10px rgba(255,255,255,0.08); }
.trade-table { margin-top: 20px; }
.code {
  font-family: "Cascadia Mono", "Consolas", monospace;
  font-size: 0.86rem;
  padding: 3px 6px;
  border-radius: 8px;
  background: rgba(255,255,255,0.04);
}
.controls { display: flex; flex-wrap: wrap; gap: 8px; margin: 0 0 18px; }
.control-group { display: flex; flex-wrap: wrap; gap: 8px; margin: 0 0 14px; align-items: center; }
.toggle {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  padding: 7px 11px;
  border-radius: 999px;
  border: 1px solid rgba(135, 168, 204, 0.16);
  background: rgba(13, 25, 39, 0.84);
  color: #c5d8ec;
  font-size: 0.82rem;
}
.toggle input { accent-color: #60a5fa; }
.pill {
  display: inline-flex;
  align-items: center;
  padding: 7px 11px;
  border-radius: 999px;
  border: 1px solid rgba(135, 168, 204, 0.16);
  background: rgba(13, 25, 39, 0.84);
  color: #aecdff;
  text-decoration: none;
  font-size: 0.84rem;
  font-weight: 600;
}
.pill:hover { border-color: rgba(137, 198, 255, 0.44); color: #f4faff; }
.pill.active {
  background: linear-gradient(180deg, rgba(59, 130, 246, 0.28), rgba(37, 99, 235, 0.24));
  color: #ffffff;
  border-color: rgba(96, 165, 250, 0.42);
  box-shadow: 0 0 22px rgba(59,130,246,0.16);
}
.empty-state {
  padding: 22px 18px;
  margin: 12px 0 24px;
  color: #8da4be;
}
.context-panel {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  gap: 12px;
  margin: 0 0 18px;
}
.context-card {
  padding: 14px 16px;
  border-radius: 16px;
  border: 1px solid rgba(135, 168, 204, 0.14);
  background: rgba(11, 21, 34, 0.88);
}
.context-label {
  text-transform: uppercase;
  letter-spacing: 0.08em;
  font-size: 0.70rem;
  color: #80a3c8;
  font-weight: 700;
  margin-bottom: 8px;
}
.context-value {
  font-size: 0.98rem;
  font-weight: 700;
  color: #edf6ff;
  word-break: break-word;
}
.context-meta {
  margin-top: 6px;
  font-size: 0.82rem;
  color: #8da4be;
}
.alert-inline {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  padding: 7px 11px;
  border-radius: 999px;
  border: 1px solid rgba(245, 158, 11, 0.24);
  background: rgba(245, 158, 11, 0.10);
  color: #ffd58d;
  font-size: 0.82rem;
  font-weight: 600;
}
.readout {
  display: flex;
  justify-content: space-between;
  gap: 12px;
  flex-wrap: wrap;
  padding: 12px 14px;
  margin: 12px 0 0;
  border-radius: 14px;
  background: rgba(255,255,255,0.03);
  border: 1px solid rgba(135, 168, 204, 0.10);
}
.readout strong { color: #edf6ff; }
.table-wrap tr.is-highlight td { background: rgba(96, 165, 250, 0.12); }
@media (max-width: 900px) {
  .app-shell { padding: 14px 14px 28px; }
  .topbar { padding: 18px; border-radius: 16px; }
  .brand-block h1 { font-size: 1.4rem; }
  .metric { font-size: 1.55rem; }
}
"""
    return f"""<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>{html.escape(title)}</title><style>{css}</style></head><body>{nav}<section class="subnav"><div><div class="eyebrow">View</div><h1 style="margin:6px 0 0;font-size:1.55rem;letter-spacing:-0.03em;">{html.escape(title)}</h1></div><div class="muted">Server-rendered, lightweight, internal trading dashboard.</div></section>{body}</main></div></body></html>""".encode("utf-8")


def _badge(value: str | None) -> str:
    text = html.escape(str(value or "unknown"))
    css = str(value or "unknown").replace(" ", "_")
    return f'<span class="badge {css}">{text}</span>'


def _parse_timestamp(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        text = str(value).replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)
    except ValueError:
        return None


def _freshness_badge(*, timestamp: object | None = None, path: str | None = None, stale_after_hours: int = 24) -> str:
    candidate = _parse_timestamp(timestamp)
    if candidate is None and path:
        file_path = Path(path)
        if file_path.exists():
            candidate = datetime.fromtimestamp(file_path.stat().st_mtime, tz=UTC)
    if candidate is None:
        return _badge("unknown")
    age_hours = (datetime.now(UTC) - candidate.astimezone(UTC)).total_seconds() / 3600.0
    label = "stale" if age_hours > stale_after_hours else "fresh"
    detail = candidate.astimezone(UTC).strftime("%Y-%m-%d %H:%M UTC")
    return f'{_badge(label)} <span class="muted">{html.escape(detail)}</span>'


def _context_cards(rows: list[tuple[str, object, str]]) -> str:
    return '<div class="context-panel">' + "".join(
        f'<div class="context-card"><div class="context-label">{html.escape(label)}</div><div class="context-value">{html.escape(str(value if value not in (None, "") else "n/a"))}</div><div class="context-meta">{detail}</div></div>'
        for label, value, detail in rows
    ) + "</div>"


def _cards(rows: list[tuple[str, object, str]]) -> str:
    return '<div class="grid">' + "".join(
        f'<div class="card"><div class="card-label">{html.escape(label)}</div><div class="metric">{html.escape(str(value))}</div><div class="card-detail">{html.escape(detail)}</div></div>'
        for label, value, detail in rows
    ) + "</div>"


def _timeline(rows: list[dict]) -> str:
    if not rows:
        return "<div class='empty-state'>No lifecycle events found for this trade.</div>"
    items = []
    for row in rows:
        items.append(
            "<div class='context-card'>"
            f"<div class='context-label'>{html.escape(str(row.get('kind') or 'event'))}</div>"
            f"<div class='context-value'>{html.escape(str(row.get('label') or 'n/a'))}</div>"
            f"<div class='context-meta'>{html.escape(str(row.get('ts') or 'n/a'))} | {html.escape(str(row.get('detail') or ''))}</div>"
            f"<div style='margin-top:8px'>{_badge(str(row.get('status') or 'recorded'))}</div>"
            "</div>"
        )
    return "<div class='context-panel'>" + "".join(items) + "</div>"


def _row_to_mapping(row: object) -> dict[str, object]:
    if row is None:
        return {}
    if isinstance(row, Mapping):
        return dict(row)
    if hasattr(row, "_asdict"):
        try:
            value = row._asdict()
        except TypeError:
            value = None
        if isinstance(value, Mapping):
            return dict(value)
    if hasattr(row, "to_dict"):
        try:
            value = row.to_dict()
        except TypeError:
            value = None
        if isinstance(value, Mapping):
            return dict(value)
    if hasattr(row, "__dict__"):
        try:
            value = vars(row)
        except TypeError:
            value = None
        if isinstance(value, Mapping):
            return dict(value)
    return {}


def _table(columns: list[str], rows: list[dict]) -> str:
    if not rows:
        return "<div class='empty-state'>No data available for this section.</div>"
    header = "".join(f"<th>{html.escape(col)}</th>" for col in columns)
    body_rows = []
    for row in rows:
        mapping = _row_to_mapping(row)
        cells = []
        for column in columns:
            value = mapping.get(column, "")
            if column == "symbol" and value not in (None, ""):
                symbol = html.escape(str(value).upper())
                rendered = f'<a class="link" href="/symbols/{symbol}">{symbol}</a>'
            elif column == "strategy_id" and value not in (None, ""):
                strategy_id = html.escape(str(value))
                rendered = f'<a class="link" href="/strategies/{strategy_id}">{strategy_id}</a>'
            elif column == "trade_id" and value not in (None, ""):
                trade_id = html.escape(str(value))
                rendered = f'<a class="link" href="/trades/{trade_id}">{trade_id}</a>'
            else:
                rendered = _badge(str(value)) if column.endswith('status') or column == 'status' else html.escape(str(value))
            cells.append(f"<td>{rendered}</td>")
        body_rows.append("<tr>" + "".join(cells) + "</tr>")
    return f"<div class='table-wrap'><table><thead><tr>{header}</tr></thead><tbody>{''.join(body_rows)}</tbody></table></div>"


def _bar_chart(values: list[tuple[str, float]]) -> str:
    if not values:
        return "<div class='empty-state'>No trend data available.</div>"
    max_value = max(value for _, value in values) or 1.0
    bars = []
    for label, value in values:
        height = max(int((value / max_value) * 100), 4) if value > 0 else 4
        bars.append(f"<div class='bar-wrap'><div class='bar' style='height:{height}px'></div><div class='muted'>{html.escape(label)}</div><div>{value:.0f}</div></div>")
    return "<div class='chart'>" + "".join(bars) + "</div>"


def _overview_page(service: DashboardDataService) -> bytes:
    overview = service.overview_payload()
    discovery = service.discovery_payload()
    runs = service.runs_payload()["runs"][:8]
    run_bars = [(str(index + 1), float(row.get("critical_alert_count", 0) + row.get("warning_alert_count", 0))) for index, row in enumerate(reversed(runs))]
    body = _cards(
        [
            ("Latest Pipeline Status", overview["latest_run"].get("status") or "n/a", overview["latest_run"].get("run_name") or "no pipeline runs found"),
            ("Monitoring Health", overview["monitoring"].get("status") or "n/a", "latest run health"),
            ("Approved Strategies", overview["registry"].get("approved_strategy_count") or 0, "from registry"),
            ("Research Candidates", overview["research"].get("eligible_candidate_count") or 0, "promotion-ready runs"),
            ("Validated Strategies", overview["research"].get("validated_pass_count") or 0, "walk-forward validation pass"),
            ("Strategy Portfolio", overview["research"].get("strategy_portfolio_selected_count") or 0, "selected promoted strategies"),
            ("Current Regime", overview["market_regime"].get("regime_label") or "n/a", "latest regime snapshot"),
            ("Adaptive Weight Change", overview["adaptive_allocation"].get("absolute_weight_change") or 0, "latest adaptive snapshot"),
            ("Automation", overview["orchestration"].get("status") or "n/a", overview["orchestration"].get("run_id") or "no automated runs"),
            ("Experiments", overview["experiments"].get("experiment_count") or 0, overview["experiments"].get("latest_experiment_name") or "no experiment runs"),
            ("System Return", overview["system_evaluation"].get("total_return") or 0, "latest evaluated orchestration run"),
            ("System Sharpe", overview["system_evaluation"].get("sharpe") or 0, "proxy from paper equity curve"),
            ("Strategy Warnings", overview["strategy_monitoring"].get("warning_strategy_count") or 0, "latest monitoring snapshot"),
            ("Demoted Strategies", overview["strategy_lifecycle"].get("demoted_count") or 0, "lifecycle governance"),
            ("Generated Positions", overview["portfolio"].get("generated_position_count") or 0, "latest portfolio"),
            ("Executable Orders", overview["execution"].get("executable_order_count") or 0, "latest execution package"),
            ("Broker Health", overview["broker_health"].get("status") or "n/a", overview["broker_health"].get("message") or "not available"),
            ("Recent Symbols", discovery["summary"].get("recent_symbol_count") or 0, "linked symbol discovery"),
            ("Recent Trades", discovery["summary"].get("recent_trade_count") or 0, "linked trade discovery"),
        ]
    )
    body += "<h2>Alert Counts Over Recent Runs</h2>" + _bar_chart(run_bars)
    body += "<h2>Recent Symbols</h2>" + _table(
        ["symbol", "trade_count", "latest_trade_id", "latest_entry_ts", "latest_strategy_id", "status"],
        discovery.get("recent_symbols", []),
    )
    body += "<h2>Recent Trades</h2>" + _table(
        ["trade_id", "symbol", "strategy_id", "side", "entry_ts", "realized_pnl", "status"],
        discovery.get("recent_trades", []),
    )
    body += "<h2>Recent Strategies</h2>" + _table(
        ["strategy_id", "trade_count", "closed_trade_count", "latest_symbol", "latest_entry_ts", "latest_source", "latest_run_id"],
        discovery.get("recent_strategies", []),
    )
    body += "<h2>Recent Run / Source Contexts</h2>" + _table(
        ["source", "run_id", "mode", "trade_count", "strategy_count", "symbol_count", "latest_entry_ts"],
        discovery.get("recent_run_contexts", []),
    )
    body += "<h2>Quick Links</h2>" + _table(["label", "path"], overview["quick_links"])
    return _page_shell("Dashboard Overview", body)


def _strategies_page(service: DashboardDataService, query: dict[str, list[str]]) -> bytes:
    payload = service.strategies_payload()
    lifecycle = payload.get("strategy_lifecycle", [])
    rows = payload["strategies"]
    status_filter = (query.get("status") or [None])[0]
    family_filter = (query.get("family") or [None])[0]
    tag_filter = (query.get("tag") or [None])[0]
    if status_filter:
        rows = [row for row in rows if row.get("status") == status_filter]
    if family_filter:
        rows = [row for row in rows if row.get("family") == family_filter]
    if tag_filter:
        rows = [row for row in rows if tag_filter in row.get("tags", [])]
    body = _cards(
        [
            ("Registry Path", payload.get("registry_path") or "n/a", "artifact source"),
            ("Strategy Count", len(payload["strategies"]), "all registry entries"),
            ("Approved", payload["summary"]["status_counts"].get("approved", 0), "approved lineup"),
            ("Families", len(payload["summary"]["family_counts"]), "strategy families"),
        ]
    )
    body += "<h2>Status Counts</h2>" + _bar_chart([(key, float(value)) for key, value in payload["summary"]["status_counts"].items()])
    body += "<h2>Registry</h2>" + _table(["strategy_id", "status", "family", "version", "preset_name", "universe", "current_deployment_stage", "promotion_passed", "degradation_status"], rows)
    body += "<h2>Lifecycle States</h2>" + _table(
        ["strategy_id", "preset_name", "current_state", "validation_status", "monitoring_recommendation", "adaptive_adjusted_weight", "latest_reasons"],
        lifecycle,
    )
    body += "<h2>Champion / Challenger</h2>" + _table(list(payload["champion_challenger"][0].keys()) if payload["champion_challenger"] else ["family", "champion"], payload["champion_challenger"])
    return _page_shell("Strategies", body)


def _research_page(service: DashboardDataService) -> bytes:
    payload = service.research_latest_payload()
    monitoring = service.strategy_monitoring_payload()
    adaptive = payload.get("adaptive_allocation", {})
    market_regime = payload.get("market_regime", {})
    validation = payload.get("strategy_validation", {})
    lifecycle = payload.get("strategy_lifecycle", {})
    summary = payload.get("summary", {})
    body = _cards(
        [
            ("Research Runs", summary.get("run_count", 0), "indexed manifests"),
            ("Validated Pass", summary.get("validated_pass_count", 0), "walk-forward evidence"),
            ("Eligible Candidates", summary.get("eligible_candidate_count", 0), "promotion readiness"),
            ("Promoted Strategies", summary.get("promoted_strategy_count", 0), "generated paper presets"),
            ("Monitoring Warnings", monitoring.get("summary", {}).get("warning_strategy_count", 0), "selected strategy reviews"),
            ("Adaptive Changes", adaptive.get("summary", {}).get("absolute_weight_change", 0), "weight turnover for next cycle"),
            ("Current Regime", market_regime.get("summary", {}).get("regime_label") or "n/a", "simple market context"),
            ("Demoted", summary.get("demoted_strategy_count", 0), "governance exclusions"),
            ("Signal Families", len(summary.get("signal_family_counts", {})), "observed across runs"),
            ("Universes", len(summary.get("universe_counts", {})), "observed across runs"),
        ]
    )
    body += "<h2>Signal Family Counts</h2>" + _bar_chart([(key, float(value)) for key, value in summary.get("signal_family_counts", {}).items()])
    body += "<h2>Top Leaderboard Entries</h2>" + _table(
        ["rank", "run_id", "signal_family", "universe", "metric_name", "metric_value", "promotion_recommendation"],
        payload.get("leaderboard", []),
    )
    body += "<h2>Promotion Candidates</h2>" + _table(
        ["run_id", "eligible", "promotion_recommendation", "mean_spearman_ic", "portfolio_sharpe", "reasons"],
        payload.get("promotion_candidates", []),
    )
    body += "<h2>Strategy Validation</h2>" + _table(
        ["run_id", "signal_family", "universe", "number_of_folds", "out_of_sample_sharpe", "proxy_confidence_score", "validation_status", "validation_reason"],
        [
            {
                "run_id": row.get("run_id"),
                "signal_family": row.get("signal_family"),
                "universe": row.get("universe"),
                "number_of_folds": row.get("number_of_folds"),
                "out_of_sample_sharpe": row.get("out_of_sample_metrics", {}).get("out_of_sample_sharpe"),
                "proxy_confidence_score": row.get("proxy_confidence_score"),
                "validation_status": row.get("validation_status"),
                "validation_reason": row.get("validation_reason"),
            }
            for row in validation.get("rows", [])
        ],
    )
    body += "<h2>Promoted Strategies</h2>" + _table(
        ["preset_name", "source_run_id", "status", "validation_status", "ranking_metric", "ranking_value", "generated_preset_path"],
        payload.get("promoted_strategies", []),
    )
    body += "<h2>Lifecycle State</h2>" + _table(
        ["strategy_id", "preset_name", "current_state", "validation_status", "monitoring_recommendation", "adaptive_adjusted_weight", "latest_reasons"],
        lifecycle.get("strategies", []),
    )
    body += "<h2>Strategy Portfolio</h2>" + _table(
        ["preset_name", "allocation_weight", "signal_family", "universe", "selection_rank"],
        payload.get("strategy_portfolio", {}).get("selected_strategies", []),
    )
    body += "<h2>Strategy Portfolio Exclusions</h2>" + _table(
        ["preset_name", "reason"],
        payload.get("strategy_portfolio", {}).get("excluded_candidates", []),
    )
    body += "<h2>Strategy Monitoring</h2>" + _table(
        ["preset_name", "current_status", "portfolio_weight", "realized_sharpe", "drawdown", "recommendation", "warning_flags"],
        monitoring.get("strategies", []),
    )
    body += "<h2>Kill-Switch Recommendations</h2>" + _table(
        ["preset_name", "recommendation", "reasons", "portfolio_weight", "paper_observation_count"],
        monitoring.get("recommendations", []),
    )
    body += "<h2>Adaptive Allocation</h2>" + _table(
        ["preset_name", "prior_weight", "adjusted_weight", "current_regime_label", "regime_compatibility", "monitoring_recommendation", "reason_for_adjustment", "capped_by_policy"],
        adaptive.get("strategies", []),
    )
    body += "<h2>Market Regime</h2>" + _table(
        ["timestamp", "regime_label", "confidence_score", "realized_volatility", "long_return"],
        market_regime.get("history", []),
    )
    body += "<h2>Recent Research Runs</h2>" + _table(
        ["run_id", "timestamp", "workflow_type", "signal_family", "universe", "candidate_count", "promoted_signal_count"],
        payload.get("recent_runs", []),
    )
    return _page_shell("Research", body)


def _portfolio_page(service: DashboardDataService) -> bytes:
    payload = service.portfolio_payload()
    overview = service.portfolio_overview_payload()
    summary = payload.get("summary", {})
    adaptive = payload.get("adaptive_allocation", {})
    market_regime = payload.get("market_regime", {})
    portfolio_summary = overview.get("summary", {})
    overview_meta = overview.get("meta", {})
    freshness = _context_cards(
        [
            ("Portfolio Summary", overview_meta.get("summary_source") or "n/a", _freshness_badge(path=overview_meta.get("summary_source"))),
            ("Equity Curve", overview_meta.get("equity_curve_source") or "n/a", _freshness_badge(path=overview_meta.get("equity_curve_source"))),
            ("Positions", overview_meta.get("positions_source") or "n/a", _freshness_badge(path=overview_meta.get("positions_source"))),
        ]
    )
    warning_bits = []
    if portfolio_summary.get("latest_drawdown") is not None and float(portfolio_summary.get("latest_drawdown") or 0.0) < -0.05:
        warning_bits.append("drawdown exceeds 5%")
    if not overview.get("positions"):
        warning_bits.append("no open positions found")
    body = _cards(
        [
            ("Gross Exposure", summary.get("gross_exposure_after_constraints", "n/a"), "after constraints"),
            ("Net Exposure", summary.get("net_exposure_after_constraints", "n/a"), "after constraints"),
            ("Position Count", len(payload.get("combined_positions", [])), "latest combined portfolio"),
            ("Clipped Symbols", len(payload.get("clipped_symbols", [])), "constraint actions"),
            ("Current Regime", market_regime.get("summary", {}).get("regime_label") or "n/a", "allocation context"),
            ("Portfolio Equity", portfolio_summary.get("equity") or "n/a", "latest portfolio summary"),
            ("Portfolio Cash", portfolio_summary.get("cash") or "n/a", "latest portfolio summary"),
            ("Latest Drawdown", portfolio_summary.get("latest_drawdown") or "n/a", "from equity curve"),
        ]
    )
    body += freshness
    if warning_bits:
        body += '<div class="control-group">' + "".join(f'<span class="alert-inline">{html.escape(item)}</span>' for item in warning_bits) + "</div>"
    body += "<h2>Equity Curve</h2>" + _bar_chart([(str(index + 1), float(row.get("equity") or 0.0)) for index, row in enumerate(overview.get("equity_curve", [])[-8:])])
    body += "<h2>Drawdown Curve</h2>" + _bar_chart([(str(index + 1), abs(float(row.get("drawdown") or 0.0)) * 100.0) for index, row in enumerate(overview.get("drawdown_curve", [])[-8:])])
    body += "<h2>Current Open Positions</h2>" + _table(["symbol", "side", "qty", "avg_price", "market_value"], overview.get("positions", []))
    body += "<h2>Exposure By Symbol</h2>" + _table(["symbol", "side", "market_value", "weight_proxy"], overview.get("exposure", []))
    body += "<h2>Realized PnL By Symbol</h2>" + _table(["symbol", "trade_count", "closed_trade_count", "cumulative_realized_pnl", "win_rate"], overview.get("pnl_by_symbol", []))
    body += "<h2>Best Recent Trades</h2>" + _table(["trade_id", "symbol", "side", "realized_pnl", "entry_ts", "exit_ts", "strategy_id"], overview.get("best_trades", []))
    body += "<h2>Worst Recent Trades</h2>" + _table(["trade_id", "symbol", "side", "realized_pnl", "entry_ts", "exit_ts", "strategy_id"], overview.get("worst_trades", []))
    body += "<h2>Recent Realized PnL</h2>" + _table(["period", "realized_pnl"], overview.get("recent_realized_pnl", []))
    body += "<h2>Recent Activity</h2>" + _table(["kind", "ts", "symbol", "side", "qty", "price", "status"], overview.get("recent_activity", []))
    body += "<h2>Adaptive Weight Changes</h2>" + _table(
        ["preset_name", "prior_weight", "adjusted_weight", "delta_weight", "monitoring_recommendation"],
        adaptive.get("top_changes", []),
    )
    body += "<h2>Regime Snapshot</h2>" + _table(
        ["timestamp", "regime_label", "confidence_score", "realized_volatility", "long_return"],
        market_regime.get("history", []),
    )
    body += "<h2>Top Position Weights</h2>" + _bar_chart([(str(row.get("symbol")), float(abs(row.get("target_weight", 0.0)))) for row in payload.get("top_positions", [])[:8]])
    body += "<h2>Sleeve Weights</h2>" + _table(["sleeve_name", "scaled_target_weight"], payload.get("sleeve_weights", []))
    body += "<h2>Top Positions</h2>" + _table(["symbol", "target_weight", "side", "latest_price"], payload.get("top_positions", []))
    body += "<h2>Overlap Diagnostics</h2>" + _table(list(payload["overlap"][0].keys()) if payload.get("overlap") else ["symbol"], payload.get("overlap", []))
    body += "<h2>Constraint Clipping</h2>" + _table(list(payload["clipped_symbols"][0].keys()) if payload.get("clipped_symbols") else ["constraint_name"], payload.get("clipped_symbols", []))
    return _page_shell("Portfolio", body)


def _execution_page(service: DashboardDataService) -> bytes:
    payload = service.execution_payload()
    diagnostics = service.execution_diagnostics_payload()
    summary = payload.get("summary", {})
    requested_notional = float(summary.get("requested_notional", 0.0) or 0.0)
    executed_notional = float(summary.get("executed_notional", 0.0) or 0.0)
    diagnostics_summary = diagnostics.get("summary", {})
    diagnostics_meta = diagnostics.get("meta", {})
    warning_bits = []
    if int(diagnostics_summary.get("rejected_order_count") or 0) > 0:
        warning_bits.append(f"rejected orders: {int(diagnostics_summary.get('rejected_order_count') or 0)}")
    if int(diagnostics_summary.get("missing_fill_count") or 0) > 0:
        warning_bits.append(f"missing fills: {int(diagnostics_summary.get('missing_fill_count') or 0)}")
    if int(diagnostics_summary.get("orphan_signal_count") or 0) > 0:
        warning_bits.append(f"orphan signals: {int(diagnostics_summary.get('orphan_signal_count') or 0)}")
    body = _cards(
        [
            ("Requested Orders", summary.get("requested_order_count", 0), "raw desired orders"),
            ("Executable Orders", summary.get("executable_order_count", 0), "after constraints"),
            ("Rejected Orders", summary.get("rejected_order_count", 0), "hard rejections"),
            ("Expected Cost", summary.get("expected_total_cost", 0.0), "fees + slippage"),
            ("Avg Signal-Fill Latency", diagnostics_summary.get("average_signal_to_fill_latency_seconds") or "n/a", "seconds"),
            ("Avg Slippage", diagnostics_summary.get("average_slippage_bps") or "n/a", "bps proxy"),
        ]
    )
    body += _context_cards(
        [
            ("Orders Source", diagnostics_meta.get("orders_source") or "n/a", _freshness_badge(path=diagnostics_meta.get("orders_source"))),
            ("Fills Source", diagnostics_meta.get("fills_source") or "n/a", _freshness_badge(path=diagnostics_meta.get("fills_source"))),
            ("Rejected Source", diagnostics_meta.get("rejected_source") or "n/a", _freshness_badge(path=diagnostics_meta.get("rejected_source"))),
        ]
    )
    if warning_bits:
        body += '<div class="control-group">' + "".join(f'<span class="alert-inline">{html.escape(item)}</span>' for item in warning_bits) + "</div>"
    body += "<h2>Requested vs Executable Notional</h2>" + _bar_chart([("requested", requested_notional), ("executed", executed_notional)])
    body += "<h2>Execution Diagnostics</h2>" + _table(
        ["symbol", "signal_ts", "fill_ts", "latency_seconds", "signal_price", "fill_price", "slippage_bps"],
        diagnostics.get("rows", []),
    )
    body += "<h2>Executable Orders</h2>" + _table(["symbol", "side", "requested_shares", "adjusted_shares", "estimated_fill_price", "commission", "clipping_reason"], payload.get("executable_orders", [])[:20])
    body += "<h2>Rejected Orders</h2>" + _table(["symbol", "side", "requested_shares", "rejection_reason"], payload.get("rejected_orders", [])[:20])
    body += "<h2>Liquidity Diagnostics</h2>" + _table(list(payload["liquidity_diagnostics"][0].keys()) if payload.get("liquidity_diagnostics") else ["symbol"], payload.get("liquidity_diagnostics", [])[:20])
    return _page_shell("Execution", body)


def _strategy_detail_page(service: DashboardDataService, strategy_id: str) -> bytes:
    payload = service.strategy_detail_payload(strategy_id)
    summary = payload.get("summary", {})
    sources = payload.get("meta", {}).get("sources", [])
    body = _cards(
        [
            ("Closed Trades", summary.get("closed_trade_count", 0), "explicit trade ledgers"),
            ("Open Trades", summary.get("open_trade_count", 0), "explicit trade ledgers"),
            ("Win Rate", summary.get("win_rate") or "n/a", "closed trades only"),
            ("Average Win", summary.get("average_win") or "n/a", "closed trades only"),
            ("Average Loss", summary.get("average_loss") or "n/a", "closed trades only"),
            ("Expectancy", summary.get("expectancy") or "n/a", "mean realized pnl"),
            ("Avg Hold (hrs)", summary.get("average_holding_period_hours") or "n/a", "closed trades only"),
            ("Realized PnL", summary.get("cumulative_realized_pnl") or 0.0, "closed trades only"),
        ]
    )
    if sources:
        body += _context_cards(
            [
                (
                    "Primary Source",
                    sources[0].get("source") or sources[0].get("path") or "n/a",
                    f"{_freshness_badge(path=sources[0].get('path'))} <span class=\"muted\">run={html.escape(str(sources[0].get('run_id') or 'latest'))}</span>",
                )
            ]
        )
    body += "<h2>Recent Symbols</h2>" + _table(
        ["symbol"],
        [{"symbol": symbol} for symbol in summary.get("recent_symbols", [])],
    )
    body += "<h2>PnL By Symbol</h2>" + _table(
        ["symbol", "trade_count", "closed_trade_count", "cumulative_realized_pnl", "win_rate"],
        payload.get("pnl_by_symbol", []),
    )
    body += "<h2>Run / Source Comparison</h2>" + _table(
        ["source", "run_id", "mode", "trade_count", "closed_trade_count", "open_trade_count", "cumulative_realized_pnl", "win_rate"],
        payload.get("comparisons", []),
    )
    body += "<h2>Best Trades</h2>" + _table(
        ["trade_id", "symbol", "side", "realized_pnl", "entry_ts", "exit_ts", "strategy_id"],
        payload.get("best_trades", []),
    )
    body += "<h2>Worst Trades</h2>" + _table(
        ["trade_id", "symbol", "side", "realized_pnl", "entry_ts", "exit_ts", "strategy_id"],
        payload.get("worst_trades", []),
    )
    body += "<h2>Recent Realized PnL</h2>" + _table(["period", "realized_pnl"], payload.get("recent_realized_pnl", []))
    body += "<h2>Recent Trades</h2>" + _table(
        ["trade_id", "symbol", "side", "qty", "entry_ts", "exit_ts", "entry_price", "exit_price", "realized_pnl", "status"],
        payload.get("trades", []),
    )
    return _page_shell(f"Strategy Detail: {strategy_id}", body)


def _trade_detail_page(service: DashboardDataService, trade_id: str) -> bytes:
    payload = service.trade_detail_payload(trade_id)
    trade = payload.get("trade") or {}
    chart_payload = payload.get("chart", {})
    if not trade:
        return _page_shell(f"Trade Detail: {trade_id}", "<div class='empty-state'>Trade not found.</div>")
    context = _context_cards(
        [
            ("Symbol", trade.get("symbol") or "n/a", f"<a class=\"link\" href=\"/symbols/{html.escape(str(trade.get('symbol') or ''))}\">open symbol</a>"),
            ("Strategy", payload.get("meta", {}).get("strategy_id") or "n/a", f"{_freshness_badge(path=payload.get('meta', {}).get('trade_source'))} <span class=\"muted\">run={html.escape(str(payload.get('meta', {}).get('run_id') or 'latest'))}</span>"),
            ("Trade Source", payload.get("meta", {}).get("trade_source_mode") or "n/a", html.escape(str(payload.get("meta", {}).get("trade_source") or "n/a"))),
            ("Explain Why", (payload.get("explain", {}).get("signal") or {}).get("label") or (payload.get("explain", {}).get("signal") or {}).get("type") or "n/a", html.escape(str(payload.get("explain", {}).get("indicator_snapshot") or {}))),
        ]
    )
    provenance = payload.get("provenance", {})
    body = _cards(
        [
            ("Trade ID", trade.get("trade_id") or trade_id, "explicit ledger"),
            ("Side", trade.get("side") or "n/a", "trade direction"),
            ("Quantity", trade.get("qty") or 0, "filled quantity"),
            ("Realized PnL", trade.get("realized_pnl") or 0.0, "closed trade pnl"),
            ("Hold Duration (hrs)", trade.get("hold_duration_hours") or "n/a", "entry to exit"),
            ("Status", trade.get("status") or "n/a", "trade lifecycle"),
        ]
    )
    body += context
    if provenance:
        body += _context_cards(
            [
                ("Decision Provenance", provenance.get("selection_status") or "n/a", html.escape(str(provenance.get("order_intent_summary") or "no order intent summary"))),
                ("Ranking Score", provenance.get("ranking_score") or "n/a", html.escape(str(provenance.get("latest", {}).get("signal_type") or "no signal type"))),
                ("Universe Rank", provenance.get("universe_rank") or "n/a", html.escape(str(provenance.get("target_weight") or "no target weight"))),
                ("Constraint Hits", ", ".join(provenance.get("constraint_hits", [])) or "n/a", html.escape(str((provenance.get("latest") or {}).get("artifact_path") or "n/a"))),
            ]
        )
    body += f"""
<div class="chart-panel">
  <h2>Trade Window</h2>
  <svg id="trade-chart" class="chart-frame" viewBox="0 0 960 320" preserveAspectRatio="none"></svg>
  <div id="trade-readout" class="readout"><div><strong>{html.escape(str(trade.get("trade_id") or trade_id))}</strong></div><div class="muted">Hover price points and markers for details.</div></div>
</div>
<script>
const tradePayload = {json.dumps(chart_payload)};
const tradeMeta = {json.dumps(trade)};
function setTradeReadout(primary, secondary) {{
  document.getElementById('trade-readout').innerHTML = `<div><strong>${{primary}}</strong></div><div class="muted">${{secondary}}</div>`;
}}
function renderTradeChart(payload) {{
  const svg = document.getElementById('trade-chart');
  const bars = payload.bars || [];
  if (!bars.length) {{
    svg.innerHTML = "<text x='24' y='40' fill='#8da4be'>No trade-window bars available.</text>";
    return;
  }}
  const width = 960;
  const height = 320;
  const padLeft = 54;
  const padRight = 20;
  const padTop = 20;
  const padBottom = 28;
  const lows = bars.map((row) => Number(row.low ?? row.close)).filter((value) => Number.isFinite(value));
  const highs = bars.map((row) => Number(row.high ?? row.close)).filter((value) => Number.isFinite(value));
  const minPrice = Math.min(...lows);
  const maxPrice = Math.max(...highs);
  const span = Math.max(maxPrice - minPrice, 1e-9);
  const xForIndex = (index) => padLeft + ((width - padLeft - padRight) * index / Math.max(bars.length - 1, 1));
  const yForPrice = (price) => padTop + ((maxPrice - price) / span) * (height - padTop - padBottom);
  const pricePath = bars.map((bar, index) => `${{index === 0 ? 'M' : 'L'}}${{xForIndex(index)}},${{yForPrice(Number(bar.close))}}`).join(' ');
  const signals = (payload.signals || []).map((row) => {{
    const index = bars.findIndex((bar) => bar.ts === row.ts);
    if (index < 0) return '';
    return `<circle cx="${{xForIndex(index)}}" cy="${{yForPrice(Number(row.price ?? bars[index].close))}}" r="5" fill="white" stroke="#d97706" stroke-width="2" data-info="${{row.label || row.type}} | ${{row.ts || ''}}"></circle>`;
  }}).join('');
  const fills = (payload.fills || []).map((row) => {{
    const index = bars.findIndex((bar) => bar.ts === row.ts);
    if (index < 0) return '';
    const color = row.side === 'sell' ? '#b91c1c' : '#0f766e';
    return `<rect x="${{xForIndex(index) - 4}}" y="${{yForPrice(Number(row.price ?? bars[index].close)) - 4}}" width="8" height="8" fill="${{color}}" data-info="${{row.side || 'fill'}} | qty=${{row.qty || ''}} | price=${{row.price || ''}}"></rect>`;
  }}).join('');
  svg.innerHTML = `<path d="${{pricePath}}" fill="none" stroke="#89c6ff" stroke-width="2"></path>${{signals}}${{fills}}`;
  svg.querySelectorAll('[data-info]').forEach((node) => {{
    node.addEventListener('mouseenter', () => setTradeReadout(tradeMeta.trade_id || 'trade', node.getAttribute('data-info') || ''));
    node.addEventListener('mouseleave', () => setTradeReadout(tradeMeta.trade_id || 'trade', 'Hover price points and markers for details.'));
  }});
}}
renderTradeChart(tradePayload);
</script>
"""
    body += "<h2>Order Lifecycle</h2>" + _timeline(payload.get("lifecycle", []))
    body += "<h2>Decision Provenance Rows</h2>" + _table(
        ["ts", "signal_type", "ranking_score", "universe_rank", "selection_status", "exclusion_reason", "target_weight", "order_intent_summary"],
        payload.get("provenance", {}).get("rows", []),
    )
    body += "<h2>Associated Signals</h2>" + _table(["ts", "type", "label", "price", "score"], payload.get("signals", []))
    body += "<h2>Associated Fills</h2>" + _table(["ts", "side", "qty", "price", "order_id", "status"], payload.get("fills", []))
    body += "<h2>Associated Orders</h2>" + _table(["ts", "side", "qty", "price", "order_id", "status", "reason"], payload.get("orders", []))
    body += "<h2>Related Comparison Context</h2>" + _table(
        ["trade_id", "symbol", "side", "entry_ts", "exit_ts", "realized_pnl", "status"],
        payload.get("comparison", {}).get("related_trades", []),
    )
    return _page_shell(f"Trade Detail: {trade_id}", body)


def _live_page(service: DashboardDataService) -> bytes:
    payload = service.live_payload()
    dry_run = payload.get("dry_run_summary", {})
    submit = payload.get("submission_summary", {})
    body = _cards(
        [
            ("Dry-Run Orders", dry_run.get("adjusted_order_count", 0), "latest live preview"),
            ("Risk Passed", submit.get("risk_passed", "n/a"), "latest submit package"),
            ("Submitted Orders", submit.get("submitted_order_count", 0), "live submit results"),
            ("Duplicate Skips", submit.get("duplicate_order_skip_count", 0), "duplicate protection"),
        ]
    )
    body += "<h2>Pre-Trade Risk Checks</h2>" + _table(["check_name", "passed", "hard_block", "severity", "message"], payload.get("risk_checks", []))
    body += "<h2>Blocked Reasons</h2>" + _table(["check_name", "severity", "message"], payload.get("blocked_checks", []))
    body += "<h2>Duplicate Protection Events</h2>" + _table(["symbol", "status", "message", "client_order_id"], payload.get("duplicate_events", []))
    return _page_shell("Live", body)


def _runs_page(service: DashboardDataService) -> bytes:
    payload = service.runs_payload()
    runs = payload["runs"]
    orchestration_runs = payload.get("orchestration_runs", [])
    experiments = payload.get("experiments", {})
    system_eval = payload.get("system_evaluation", {})
    body = _cards(
        [
            ("Run Count", len(runs), "recent discovered runs"),
            ("Latest Status", runs[0]["status"] if runs else "n/a", "most recent run"),
            ("Latest Health", runs[0]["health_status"] if runs else "n/a", "most recent run"),
            ("Failures", runs[0]["failed_stage_count"] if runs else 0, "most recent run"),
            ("Best Evaluated Run", system_eval.get("summary", {}).get("best_run_id") or "n/a", "from system evaluation history"),
        ]
    )
    body += "<h2>Critical Alerts Trend</h2>" + _bar_chart([(str(index + 1), float(row.get("critical_alert_count", 0))) for index, row in enumerate(reversed(runs[:10]))])
    body += "<h2>Recent Runs</h2>" + _table(["run_name", "status", "health_status", "schedule_type", "started_at", "failed_stage_count", "artifact_dir"], runs)
    body += "<h2>Automated Orchestration Runs</h2>" + _table(
        ["run_id", "run_name", "experiment_name", "variant_name", "status", "schedule_frequency", "selected_strategy_count", "total_return", "sharpe", "warning_strategy_count", "kill_switch_recommendation_count", "run_dir"],
        orchestration_runs,
    )
    body += "<h2>Recent Experiments</h2>" + _table(
        ["experiment_name", "experiment_run_id", "status", "variant_count", "variant_run_count", "succeeded_count", "failed_count", "run_dir"],
        experiments.get("rows", []),
    )
    body += "<h2>System Evaluation History</h2>" + _table(
        ["run_id", "experiment_name", "variant_name", "total_return", "sharpe", "max_drawdown", "warning_count", "kill_switch_count", "regime"],
        system_eval.get("rows", []),
    )
    latest = service.latest_run_detail_payload()
    body += "<h2>Latest Stage Status</h2>" + _table(["stage_name", "status", "started_at", "ended_at", "duration_seconds", "error_message"], latest.get("stages", []))
    return _page_shell("Runs", body)


def _parse_positive_int(value: str | None, default: int) -> int:
    if value is None:
        return default
    try:
        parsed = int(value)
    except ValueError:
        return default
    return parsed if parsed > 0 else default


def _query_value(query: dict[str, list[str]], key: str) -> str | None:
    value = (query.get(key) or [None])[0]
    if value in (None, ""):
        return None
    return value


def _validate_symbol(symbol: str) -> str | None:
    normalized = symbol.strip().upper()
    if not normalized:
        return None
    allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-")
    return normalized if all(char in allowed for char in normalized) else None


def _symbol_detail_page(service: DashboardDataService, symbol: str, query: dict[str, list[str]]) -> bytes:
    timeframe = (query.get("timeframe") or ["1d"])[0]
    lookback = _parse_positive_int((query.get("lookback") or ["200"])[0], 200)
    run_id = _query_value(query, "run_id")
    source = _query_value(query, "source")
    mode = _query_value(query, "mode")
    payload = service.chart_payload(symbol, timeframe=timeframe, lookback=lookback, run_id=run_id, source=source, mode=mode)
    latest_bar = payload["bars"][-1] if payload["bars"] else {}
    latest_signal = payload["signals"][-1] if payload["signals"] else {}
    position = payload.get("position", {})
    meta = payload.get("meta", {})
    source_links = []
    for option in payload.get("meta", {}).get("available_chart_sources", []):
        option_source = option.get("source")
        option_run_id = option.get("run_id")
        href = f"/symbols/{html.escape(symbol)}?timeframe={html.escape(timeframe)}&lookback={lookback}"
        if option_source:
            href += f"&source={html.escape(str(option_source))}"
        if option_run_id:
            href += f"&run_id={html.escape(str(option_run_id))}"
        if mode:
            href += f"&mode={html.escape(mode)}"
        active = option_source == source and option_run_id == run_id
        source_links.append(f'<a class="pill{" active" if active else ""}" href="{href}">{html.escape(str(option_source or "default"))} / {html.escape(str(option_run_id or "latest"))}</a>')
    default_href = f"/symbols/{html.escape(symbol)}?timeframe={html.escape(timeframe)}&lookback={lookback}"
    if mode:
        default_href += f"&mode={html.escape(mode)}"
    source_links.insert(0, f'<a class="pill{" active" if source is None and run_id is None else ""}" href="{default_href}">latest</a>')
    lookback_links = []
    for candidate in (50, 100, 200, 400):
        href = f"/symbols/{html.escape(symbol)}?timeframe={html.escape(timeframe)}&lookback={candidate}"
        if source:
            href += f"&source={html.escape(source)}"
        if run_id:
            href += f"&run_id={html.escape(run_id)}"
        if mode:
            href += f"&mode={html.escape(mode)}"
        lookback_links.append(f'<a class="pill{" active" if candidate == lookback else ""}" href="{href}">{candidate} bars</a>')
    context = _context_cards(
        [
            ("Selected Source", meta.get("selected_source") or "latest", _freshness_badge(path=meta.get("signal_source") or meta.get("trade_source"))),
            ("Selected Run", meta.get("selected_run_id") or "latest", html.escape(str(meta.get("selected_mode") or mode or "default mode"))),
            ("Trade Context", meta.get("trade_source_mode") or "n/a", html.escape(str(meta.get("trade_source") or "no trade source"))),
            ("Position Update", position.get("updated_at") or "n/a", _freshness_badge(timestamp=position.get("updated_at"), path=meta.get("position_source"))),
        ]
    )
    indicator_snapshot = ", ".join(
        f"{name}={values[-1].get('value')}"
        for name, values in list(meta.get("available_indicator_values", {}).items()) if values
    ) if False else ", ".join(
        f"{name}={payload.get('indicators', {}).get(name, [{}])[-1].get('value')}"
        for name in list(payload.get("indicators", {}).keys())[:3]
        if payload.get("indicators", {}).get(name)
    )
    comparison_rows = []
    for option in payload.get("meta", {}).get("available_chart_sources", [])[:3]:
        if option.get("source") == source and option.get("run_id") == run_id:
            continue
        candidate = service.chart_payload(
            symbol,
            timeframe=timeframe,
            lookback=min(lookback, 100),
            run_id=option.get("run_id"),
            source=option.get("source"),
            mode=mode,
        )
        comparison_rows.append(
            {
                "source": option.get("source") or "latest",
                "run_id": option.get("run_id") or "latest",
                "signal_count": candidate.get("meta", {}).get("signal_count"),
                "trade_count": candidate.get("meta", {}).get("trade_count"),
                "latest_signal": (candidate.get("signals") or [{}])[-1].get("label") or (candidate.get("signals") or [{}])[-1].get("type"),
                "trade_source_mode": candidate.get("meta", {}).get("trade_source_mode"),
            }
        )
    body = f"""
<div class="subnav">
  <a class="link" href="/portfolio">Back to Portfolio</a>
  <div class="muted">JSON: <a class="link code" href="/api/chart/{html.escape(symbol)}?timeframe={html.escape(timeframe)}&lookback={lookback}{f'&source={html.escape(source)}' if source else ''}{f'&run_id={html.escape(run_id)}' if run_id else ''}{f'&mode={html.escape(mode)}' if mode else ''}">/api/chart/{html.escape(symbol)}</a></div>
</div>
{context}
{_context_cards([("Explain Why", latest_signal.get("label") or latest_signal.get("type") or "no active signal", html.escape(f"score={latest_signal.get('score')} | indicators: {indicator_snapshot or 'n/a'}"))])}
{_context_cards([("Decision Provenance", (payload.get("provenance") or [{}])[0].get("selection_status") or "n/a", html.escape(str((payload.get("provenance") or [{}])[0].get("order_intent_summary") or "no provenance artifact"))), ("Ranking Score", (payload.get("provenance") or [{}])[0].get("ranking_score") or "n/a", html.escape(str((payload.get("provenance") or [{}])[0].get("signal_type") or "no signal type"))), ("Universe Rank", (payload.get("provenance") or [{}])[0].get("universe_rank") or "n/a", html.escape(str((payload.get("provenance") or [{}])[0].get("target_weight") or "no target weight"))), ("Constraint Hits", ", ".join((payload.get("provenance") or [{}])[0].get("constraint_hits", [])) or "n/a", html.escape(str((payload.get("provenance") or [{}])[0].get("artifact_path") or "n/a")))])}
<div class="summary-grid">
  <div class="summary-item"><div class="label">Symbol</div><div class="value">{html.escape(symbol)}</div></div>
  <div class="summary-item"><div class="label">Latest Close</div><div class="value">{html.escape(str(latest_bar.get("close", "n/a")))}</div></div>
  <div class="summary-item"><div class="label">Position Qty</div><div class="value">{html.escape(str(position.get("qty", 0)))}</div></div>
  <div class="summary-item"><div class="label">Avg Price</div><div class="value">{html.escape(str(position.get("avg_price", "n/a")))}</div></div>
  <div class="summary-item"><div class="label">Unrealized PnL</div><div class="value">{html.escape(str(position.get("unrealized_pnl", "n/a")))}</div></div>
  <div class="summary-item"><div class="label">Latest Signal</div><div class="value">{html.escape(str(latest_signal.get("label") or latest_signal.get("type") or "n/a"))}</div></div>
  <div class="summary-item"><div class="label">Trade Source</div><div class="value">{html.escape(str(payload.get("meta", {}).get("trade_source_mode") or "n/a"))}</div></div>
</div>
<div class="control-group"><span class="muted">Lookback:</span>{''.join(lookback_links)}</div>
<div class="controls">{''.join(source_links)}</div>
<div class="control-group">
  <label class="toggle"><input type="checkbox" id="toggle-signals" checked>Signals</label>
  <label class="toggle"><input type="checkbox" id="toggle-fills" checked>Fills</label>
  <label class="toggle"><input type="checkbox" id="toggle-indicators" checked>Indicators</label>
  <label class="toggle"><input type="checkbox" id="toggle-trades" checked>Trade ranges</label>
</div>
<div class="chart-panel">
  <h2>Price + Trading Markers</h2>
  <svg id="symbol-chart" class="chart-frame" viewBox="0 0 960 360" preserveAspectRatio="none" aria-label="Price chart for {html.escape(symbol)}"></svg>
  <div class="legend">
    <span><i class="swatch" style="background:#16324f"></i>Price</span>
    <span><i class="swatch" style="background:#2f5d62"></i>Indicator</span>
    <span><i class="swatch" style="background:#d97706"></i>Signal</span>
    <span><i class="swatch" style="background:#0f766e"></i>Buy fill</span>
    <span><i class="swatch" style="background:#b91c1c"></i>Sell fill</span>
  </div>
  <div id="chart-readout" class="readout"><div><strong>{html.escape(symbol)}</strong> chart readout</div><div class="muted">Hover candles, signals, fills, or trade ranges for details.</div></div>
</div>
<h2>Trade History</h2>
<div class="trade-table" id="trade-table"></div>
<h2>Related Source Comparison</h2>
{_table(["source", "run_id", "signal_count", "trade_count", "latest_signal", "trade_source_mode"], comparison_rows)}
<h2>Decision Provenance Rows</h2>
{_table(["ts", "signal_type", "ranking_score", "universe_rank", "selection_status", "exclusion_reason", "target_weight", "order_intent_summary"], payload.get("provenance", [])[:8])}
<script>
const chartPayload = {json.dumps(payload)};

function formatValue(value) {{
  if (value === null || value === undefined || value === '') return 'n/a';
  return String(value);
}}

function renderTradeTable(payload) {{
  const rows = payload.trades || [];
  const host = document.getElementById('trade-table');
  if (!rows.length) {{
    host.innerHTML = "<div class='empty-state'>No trade records available.</div>";
    return;
  }}
  const columns = ['trade_id', 'side', 'qty', 'entry_ts', 'exit_ts', 'entry_price', 'exit_price', 'realized_pnl', 'status'];
  const head = '<thead><tr>' + columns.map((col) => `<th>${{col}}</th>`).join('') + '</tr></thead>';
  const body = '<tbody>' + rows.map((row) => `<tr data-trade-row="${{row.trade_id || ''}}">` + columns.map((col) => `<td>${{formatValue(row[col])}}</td>`).join('') + '</tr>').join('') + '</tbody>';
  host.innerHTML = `<div class="table-wrap"><table>${{head}}${{body}}</table></div>`;
  host.querySelectorAll('[data-trade-row]').forEach((row) => {{
    row.addEventListener('mouseenter', () => highlightTrade(row.dataset.tradeRow, true));
    row.addEventListener('mouseleave', () => highlightTrade(row.dataset.tradeRow, false));
  }});
}}

function setReadout(primary, secondary) {{
  const readout = document.getElementById('chart-readout');
  readout.innerHTML = `<div><strong>${{primary}}</strong></div><div class="muted">${{secondary}}</div>`;
}}

function highlightTrade(tradeId, enabled) {{
  document.querySelectorAll(`[data-trade-segment="${{tradeId}}"]`).forEach((node) => {{
    node.style.opacity = enabled ? '1' : '0.8';
    node.style.strokeWidth = enabled ? '3' : '1.5';
  }});
  document.querySelectorAll(`[data-trade-row="${{tradeId}}"]`).forEach((node) => {{
    node.classList.toggle('is-highlight', enabled);
  }});
}}

function renderChart(payload) {{
  const svg = document.getElementById('symbol-chart');
  const bars = payload.bars || [];
  if (!bars.length) {{
    svg.innerHTML = "<text x='24' y='40' fill='#69757f'>No bars found for this symbol.</text>";
    return;
  }}

  const width = 960;
  const height = 360;
  const padLeft = 54;
  const padRight = 20;
  const padTop = 20;
  const padBottom = 36;
  const closes = bars.map((row) => Number(row.close)).filter((value) => Number.isFinite(value));
  const lows = bars.map((row) => Number(row.low ?? row.close)).filter((value) => Number.isFinite(value));
  const highs = bars.map((row) => Number(row.high ?? row.close)).filter((value) => Number.isFinite(value));
  const minPrice = Math.min(...lows);
  const maxPrice = Math.max(...highs);
  const span = Math.max(maxPrice - minPrice, 1e-9);
  const xForIndex = (index) => padLeft + ((width - padLeft - padRight) * index / Math.max(bars.length - 1, 1));
  const yForPrice = (price) => padTop + ((maxPrice - price) / span) * (height - padTop - padBottom);
  const linePath = bars.map((bar, index) => `${{index === 0 ? 'M' : 'L'}}${{xForIndex(index).toFixed(2)}},${{yForPrice(Number(bar.close)).toFixed(2)}}`).join(' ');
  const candleWidth = Math.max(4, Math.min(12, (width - padLeft - padRight) / Math.max(bars.length, 1) * 0.55));
  const hasOhlc = Boolean(payload.meta && payload.meta.has_ohlc);

  const indicatorEntries = Object.entries(payload.indicators || {{}}).slice(0, 2);
  const indicatorPaths = indicatorEntries.map(([name, series], seriesIndex) => {{
    const points = series
      .map((row) => {{
        const barIndex = bars.findIndex((bar) => bar.ts === row.ts);
        if (barIndex < 0 || !Number.isFinite(Number(row.value))) return null;
        return `${{barIndex === 0 ? 'M' : 'L'}}${{xForIndex(barIndex).toFixed(2)}},${{yForPrice(Number(row.value)).toFixed(2)}}`;
      }})
      .filter(Boolean);
    const colors = ['#2f5d62', '#4d7c0f'];
    return `<path class="indicator-layer" d="${{points.join(' ')}}" fill="none" stroke="${{colors[seriesIndex % colors.length]}}" stroke-width="1.6" stroke-dasharray="4 3" data-info="${{name}} indicator"></path>`;
  }}).join('');

  const signalMarkup = (payload.signals || []).map((row) => {{
    const barIndex = bars.findIndex((bar) => bar.ts === row.ts);
    if (barIndex < 0) return '';
    const x = xForIndex(barIndex);
    const y = yForPrice(Number(row.price ?? bars[barIndex].close));
    return `<g class="signal-layer" data-info="${{row.label || row.type || 'signal'}} | ${{row.ts || ''}} | price=${{formatValue(row.price)}}"><circle cx="${{x}}" cy="${{y}}" r="5" fill="white" stroke="#d97706" stroke-width="2"></circle></g>`;
  }}).join('');

  const fillMarkup = (payload.fills || []).map((row) => {{
    const barIndex = bars.findIndex((bar) => bar.ts === row.ts);
    if (barIndex < 0) return '';
    const x = xForIndex(barIndex);
    const y = yForPrice(Number(row.price ?? bars[barIndex].close));
    const color = row.side === 'sell' ? '#b91c1c' : '#0f766e';
    return `<g class="fill-layer" data-info="${{row.side || 'fill'}} | qty=${{formatValue(row.qty)}} | price=${{formatValue(row.price)}} | ${{row.ts || ''}}"><rect x="${{x - 4}}" y="${{y - 4}}" width="8" height="8" fill="${{color}}"></rect></g>`;
  }}).join('');

  const tradeSegments = (payload.trades || []).map((trade) => {{
    if (!trade.entry_ts || !trade.exit_ts) return '';
    const entryIndex = bars.findIndex((bar) => bar.ts === trade.entry_ts);
    const exitIndex = bars.findIndex((bar) => bar.ts === trade.exit_ts);
    if (entryIndex < 0 || exitIndex < 0) return '';
    const entryY = yForPrice(Number(trade.entry_price ?? bars[entryIndex].close));
    const exitY = yForPrice(Number(trade.exit_price ?? bars[exitIndex].close));
    const color = trade.side === 'short' ? '#7c3aed' : '#2563eb';
    return `<line class="trade-layer" data-trade-segment="${{trade.trade_id || ''}}" data-info="trade=${{trade.trade_id || ''}} | pnl=${{formatValue(trade.realized_pnl)}} | ${{trade.entry_ts}} -> ${{trade.exit_ts}}" x1="${{xForIndex(entryIndex)}}" y1="${{entryY}}" x2="${{xForIndex(exitIndex)}}" y2="${{exitY}}" stroke="${{color}}" stroke-width="1.5" stroke-dasharray="5 4" opacity="0.8"></line>`;
  }}).join('');

  const yTicks = [0, 0.25, 0.5, 0.75, 1].map((ratio) => {{
    const price = maxPrice - span * ratio;
    const y = padTop + (height - padTop - padBottom) * ratio;
    return `<g><line x1="${{padLeft}}" y1="${{y}}" x2="${{width - padRight}}" y2="${{y}}" stroke="#ece4d8" stroke-width="1"></line><text x="10" y="${{y + 4}}" fill="#69757f" font-size="11">${{price.toFixed(2)}}</text></g>`;
  }}).join('');

  const candleMarkup = hasOhlc ? bars.map((bar, index) => {{
    if (![bar.open, bar.high, bar.low, bar.close].every((value) => Number.isFinite(Number(value)))) return '';
    const x = xForIndex(index);
    const openY = yForPrice(Number(bar.open));
    const closeY = yForPrice(Number(bar.close));
    const highY = yForPrice(Number(bar.high));
    const lowY = yForPrice(Number(bar.low));
    const top = Math.min(openY, closeY);
    const bodyHeight = Math.max(Math.abs(closeY - openY), 1.5);
    const color = Number(bar.close) >= Number(bar.open) ? '#0f766e' : '#b91c1c';
    return `<g class="price-layer" data-info="${{bar.ts || ''}} | O:${{formatValue(bar.open)}} H:${{formatValue(bar.high)}} L:${{formatValue(bar.low)}} C:${{formatValue(bar.close)}}"><line x1="${{x}}" y1="${{highY}}" x2="${{x}}" y2="${{lowY}}" stroke="${{color}}" stroke-width="1.2"></line><rect x="${{x - candleWidth / 2}}" y="${{top}}" width="${{candleWidth}}" height="${{bodyHeight}}" fill="${{color}}" opacity="0.8"></rect></g>`;
  }}).join('') : '';

  svg.innerHTML = `
    <rect x="0" y="0" width="${{width}}" height="${{height}}" fill="transparent"></rect>
    ${{yTicks}}
    ${{hasOhlc ? candleMarkup : `<path d="${{linePath}}" fill="none" stroke="#16324f" stroke-width="2"></path>`}}
    ${{indicatorPaths}}
    ${{tradeSegments}}
    ${{signalMarkup}}
    ${{fillMarkup}}
  `;

  [['toggle-signals', '.signal-layer'], ['toggle-fills', '.fill-layer'], ['toggle-indicators', '.indicator-layer'], ['toggle-trades', '.trade-layer']].forEach(([toggleId, selector]) => {{
    const toggle = document.getElementById(toggleId);
    const apply = () => document.querySelectorAll(selector).forEach((node) => {{
      node.style.display = toggle && toggle.checked ? '' : 'none';
    }});
    if (toggle) {{
      toggle.addEventListener('change', apply);
      apply();
    }}
  }});

  svg.querySelectorAll('[data-info]').forEach((node) => {{
    node.addEventListener('mouseenter', () => setReadout('{html.escape(symbol)}', node.getAttribute('data-info') || ''));
    node.addEventListener('mouseleave', () => setReadout('{html.escape(symbol)} chart readout', 'Hover candles, signals, fills, or trade ranges for details.'));
  }});
}}

renderChart(chartPayload);
renderTradeTable(chartPayload);
</script>
"""
    return _page_shell(f"Symbol Detail: {symbol}", body)


def create_dashboard_app(artifacts_root: str | Path) -> Callable:
    service = DashboardDataService(artifacts_root)

    def app(environ, start_response):
        path = environ.get("PATH_INFO", "/")
        query = parse_qs(environ.get("QUERY_STRING", ""))
        if path.startswith("/api/chart/"):
            symbol = _validate_symbol(path.removeprefix("/api/chart/"))
            if symbol is None:
                status, headers, body = _not_found()
            else:
                status, headers, body = _json_response(
                    service.chart_payload(
                        symbol,
                        timeframe=(query.get("timeframe") or ["1d"])[0],
                        lookback=_parse_positive_int((query.get("lookback") or ["200"])[0], 200),
                        run_id=_query_value(query, "run_id"),
                        source=_query_value(query, "source"),
                        mode=_query_value(query, "mode"),
                    )
                )
        elif path.startswith("/api/trades/"):
            symbol = _validate_symbol(path.removeprefix("/api/trades/"))
            if symbol is None:
                status, headers, body = _not_found()
            else:
                status, headers, body = _json_response(
                    service.trades_payload(
                        symbol,
                        run_id=_query_value(query, "run_id"),
                        source=_query_value(query, "source"),
                        mode=_query_value(query, "mode"),
                    )
                )
        elif path.startswith("/api/signals/"):
            symbol = _validate_symbol(path.removeprefix("/api/signals/"))
            if symbol is None:
                status, headers, body = _not_found()
            else:
                status, headers, body = _json_response(
                    service.signals_payload(
                        symbol,
                        lookback=_parse_positive_int((query.get("lookback") or ["200"])[0], 200),
                        run_id=_query_value(query, "run_id"),
                        source=_query_value(query, "source"),
                        mode=_query_value(query, "mode"),
                    )
                )
        elif path == "/api/overview":
            status, headers, body = _json_response(service.overview_payload())
        elif path == "/api/discovery/overview":
            status, headers, body = _json_response(service.discovery_payload())
        elif path == "/api/discovery/recent-trades":
            discovery = service.discovery_payload()
            status, headers, body = _json_response(
                {"generated_at": discovery.get("generated_at"), "recent_trades": discovery.get("recent_trades", []), "summary": discovery.get("summary", {})}
            )
        elif path == "/api/discovery/recent-symbols":
            discovery = service.discovery_payload()
            status, headers, body = _json_response(
                {"generated_at": discovery.get("generated_at"), "recent_symbols": discovery.get("recent_symbols", []), "summary": discovery.get("summary", {})}
            )
        elif path == "/api/runs":
            status, headers, body = _json_response(service.runs_payload())
        elif path == "/api/runs/latest":
            status, headers, body = _json_response(service.latest_run_detail_payload())
        elif path == "/api/orchestration/latest":
            status, headers, body = _json_response(service.latest_automated_orchestration_payload())
        elif path == "/api/system-eval/latest":
            status, headers, body = _json_response(service.system_evaluation_payload())
        elif path == "/api/system-eval/history":
            status, headers, body = _json_response(service.system_evaluation_history_payload())
        elif path == "/api/experiments/latest":
            status, headers, body = _json_response(service.experiments_payload())
        elif path == "/api/strategies":
            status, headers, body = _json_response(service.strategies_payload())
        elif path == "/api/research/latest":
            status, headers, body = _json_response(service.research_latest_payload())
        elif path == "/api/strategy-validation/latest":
            status, headers, body = _json_response(service.strategy_validation_payload())
        elif path == "/api/strategy-lifecycle/latest":
            status, headers, body = _json_response(service.strategy_lifecycle_payload())
        elif path == "/api/strategy-monitor/latest":
            status, headers, body = _json_response(service.strategy_monitoring_payload())
        elif path == "/api/adaptive-allocation/latest":
            status, headers, body = _json_response(service.adaptive_allocation_payload())
        elif path == "/api/regime/latest":
            status, headers, body = _json_response(service.market_regime_payload())
        elif path == "/api/portfolio/overview":
            status, headers, body = _json_response(service.portfolio_overview_payload())
        elif path == "/api/portfolio/equity":
            overview = service.portfolio_overview_payload()
            status, headers, body = _json_response({"generated_at": overview.get("generated_at"), "equity_curve": overview.get("equity_curve", []), "drawdown_curve": overview.get("drawdown_curve", []), "meta": overview.get("meta", {})})
        elif path == "/api/portfolio/activity":
            overview = service.portfolio_overview_payload()
            status, headers, body = _json_response({"generated_at": overview.get("generated_at"), "recent_activity": overview.get("recent_activity", []), "meta": overview.get("meta", {})})
        elif path == "/api/portfolio/latest":
            status, headers, body = _json_response(service.portfolio_payload())
        elif path == "/api/execution/diagnostics":
            status, headers, body = _json_response(service.execution_diagnostics_payload())
        elif path == "/api/execution/latest":
            status, headers, body = _json_response(service.execution_payload())
        elif path == "/api/live/latest":
            status, headers, body = _json_response(service.live_payload())
        elif path == "/api/alerts/latest":
            status, headers, body = _json_response(service.latest_alerts_payload())
        elif path.startswith("/api/trade/"):
            trade_id = path.removeprefix("/api/trade/")
            if not trade_id:
                status, headers, body = _not_found()
            else:
                status, headers, body = _json_response(service.trade_detail_payload(trade_id))
        elif path.startswith("/api/strategies/"):
            strategy_id = path.removeprefix("/api/strategies/")
            if not strategy_id:
                status, headers, body = _not_found()
            else:
                status, headers, body = _json_response(service.strategy_detail_payload(strategy_id))
        elif path == "/":
            status, headers, body = "200 OK", [("Content-Type", "text/html; charset=utf-8")], _overview_page(service)
        elif path == "/strategies":
            status, headers, body = "200 OK", [("Content-Type", "text/html; charset=utf-8")], _strategies_page(service, query)
        elif path.startswith("/strategies/"):
            strategy_id = path.removeprefix("/strategies/")
            if not strategy_id:
                status, headers, body = _not_found()
            else:
                status, headers, body = "200 OK", [("Content-Type", "text/html; charset=utf-8")], _strategy_detail_page(service, strategy_id)
        elif path.startswith("/trades/"):
            trade_id = path.removeprefix("/trades/")
            if not trade_id:
                status, headers, body = _not_found()
            else:
                status, headers, body = "200 OK", [("Content-Type", "text/html; charset=utf-8")], _trade_detail_page(service, trade_id)
        elif path == "/research":
            status, headers, body = "200 OK", [("Content-Type", "text/html; charset=utf-8")], _research_page(service)
        elif path == "/portfolio":
            status, headers, body = "200 OK", [("Content-Type", "text/html; charset=utf-8")], _portfolio_page(service)
        elif path == "/execution":
            status, headers, body = "200 OK", [("Content-Type", "text/html; charset=utf-8")], _execution_page(service)
        elif path == "/live":
            status, headers, body = "200 OK", [("Content-Type", "text/html; charset=utf-8")], _live_page(service)
        elif path == "/runs":
            status, headers, body = "200 OK", [("Content-Type", "text/html; charset=utf-8")], _runs_page(service)
        elif path.startswith("/symbols/"):
            symbol = _validate_symbol(path.removeprefix("/symbols/"))
            if symbol is None:
                status, headers, body = _not_found()
            else:
                status, headers, body = "200 OK", [("Content-Type", "text/html; charset=utf-8")], _symbol_detail_page(service, symbol, query)
        else:
            status, headers, body = _not_found()
        start_response(status, headers)
        return [body]

    return app


def serve_dashboard(*, artifacts_root: str | Path, host: str = "127.0.0.1", port: int = 8000) -> None:
    app = create_dashboard_app(artifacts_root)
    with make_server(host, port, app) as server:
        print(f"Serving dashboard at http://{host}:{port}")
        server.serve_forever()


def build_dashboard_static_data(*, artifacts_root: str | Path, output_dir: str | Path) -> dict[str, Path]:
    service = DashboardDataService(artifacts_root)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    payloads = {
        "overview.json": service.overview_payload(),
        "runs.json": service.runs_payload(),
        "runs_latest.json": service.latest_run_detail_payload(),
        "orchestration_latest.json": service.latest_automated_orchestration_payload(),
        "experiments_latest.json": service.experiments_payload(),
        "system_evaluation_latest.json": service.system_evaluation_payload(),
        "system_evaluation_history.json": service.system_evaluation_history_payload(),
        "strategies.json": service.strategies_payload(),
        "research_latest.json": service.research_latest_payload(),
        "strategy_validation_latest.json": service.strategy_validation_payload(),
        "strategy_lifecycle_latest.json": service.strategy_lifecycle_payload(),
        "strategy_monitoring_latest.json": service.strategy_monitoring_payload(),
        "adaptive_allocation_latest.json": service.adaptive_allocation_payload(),
        "regime_latest.json": service.market_regime_payload(),
        "portfolio_latest.json": service.portfolio_payload(),
        "portfolio_overview.json": service.portfolio_overview_payload(),
        "execution_latest.json": service.execution_payload(),
        "execution_diagnostics.json": service.execution_diagnostics_payload(),
        "live_latest.json": service.live_payload(),
        "alerts_latest.json": service.latest_alerts_payload(),
    }
    paths: dict[str, Path] = {}
    for name, payload in payloads.items():
        path = output_path / name
        path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        paths[name.replace(".", "_")] = path
    return paths
