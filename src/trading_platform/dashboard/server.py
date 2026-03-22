from __future__ import annotations

import html
import json
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
<nav>
  <a href="/">Overview</a>
  <a href="/strategies">Strategies</a>
  <a href="/portfolio">Portfolio</a>
  <a href="/execution">Execution</a>
  <a href="/live">Live</a>
  <a href="/runs">Runs</a>
</nav>
"""
    css = """
body { font-family: Segoe UI, Arial, sans-serif; margin: 0; background: #f5f4ef; color: #1e2a33; }
header { background: linear-gradient(135deg, #16324f, #2f5d62); color: white; padding: 20px 28px; }
nav { display: flex; gap: 14px; padding: 12px 28px; background: #efe8dc; border-bottom: 1px solid #d8cfc1; }
nav a { color: #16324f; text-decoration: none; font-weight: 600; }
main { padding: 24px 28px 40px; }
.grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 14px; margin-bottom: 24px; }
.card { background: white; border: 1px solid #ddd2c2; border-radius: 10px; padding: 14px 16px; box-shadow: 0 1px 2px rgba(0,0,0,0.05); }
.metric { font-size: 1.7rem; font-weight: 700; margin: 6px 0; }
.muted { color: #69757f; font-size: 0.92rem; }
table { width: 100%; border-collapse: collapse; background: white; border: 1px solid #ddd2c2; margin: 12px 0 24px; }
th, td { text-align: left; padding: 8px 10px; border-bottom: 1px solid #ece4d8; vertical-align: top; }
th { background: #f8f3eb; }
.badge { display: inline-block; padding: 3px 8px; border-radius: 999px; font-size: 0.82rem; font-weight: 700; }
.healthy,.pass,.approved,.succeeded { background: #d9f2df; color: #0f5f2c; }
.warning,.warn,.paper,.candidate { background: #fff0c7; color: #8b5e00; }
.critical,.fail,.failed,.live_disabled,.retired { background: #f8d3d3; color: #8f1d1d; }
.chart { display: flex; gap: 8px; align-items: end; min-height: 120px; padding: 10px 0; }
.bar-wrap { display: flex; flex-direction: column; align-items: center; gap: 6px; width: 36px; }
.bar { width: 30px; background: linear-gradient(180deg, #2f5d62, #16324f); border-radius: 6px 6px 0 0; }
"""
    return f"""<!doctype html><html><head><meta charset="utf-8"><title>{html.escape(title)}</title><style>{css}</style></head><body><header><h1>{html.escape(title)}</h1><div class="muted">Local read-only dashboard for trading artifacts</div></header>{nav}<main>{body}</main></body></html>""".encode("utf-8")


def _badge(value: str | None) -> str:
    text = html.escape(str(value or "unknown"))
    css = str(value or "unknown").replace(" ", "_")
    return f'<span class="badge {css}">{text}</span>'


def _cards(rows: list[tuple[str, object, str]]) -> str:
    return '<div class="grid">' + "".join(
        f'<div class="card"><div class="muted">{html.escape(label)}</div><div class="metric">{html.escape(str(value))}</div><div class="muted">{html.escape(detail)}</div></div>'
        for label, value, detail in rows
    ) + "</div>"


def _table(columns: list[str], rows: list[dict]) -> str:
    if not rows:
        return "<p class='muted'>No data available.</p>"
    header = "".join(f"<th>{html.escape(col)}</th>" for col in columns)
    body_rows = []
    for row in rows:
        cells = []
        for column in columns:
            value = row.get(column, "")
            cells.append(f"<td>{_badge(str(value)) if column.endswith('status') or column == 'status' else html.escape(str(value))}</td>")
        body_rows.append("<tr>" + "".join(cells) + "</tr>")
    return f"<table><thead><tr>{header}</tr></thead><tbody>{''.join(body_rows)}</tbody></table>"


def _bar_chart(values: list[tuple[str, float]]) -> str:
    if not values:
        return "<p class='muted'>No trend data available.</p>"
    max_value = max(value for _, value in values) or 1.0
    bars = []
    for label, value in values:
        height = max(int((value / max_value) * 100), 4) if value > 0 else 4
        bars.append(f"<div class='bar-wrap'><div class='bar' style='height:{height}px'></div><div class='muted'>{html.escape(label)}</div><div>{value:.0f}</div></div>")
    return "<div class='chart'>" + "".join(bars) + "</div>"


def _overview_page(service: DashboardDataService) -> bytes:
    overview = service.overview_payload()
    runs = service.runs_payload()["runs"][:8]
    run_bars = [(str(index + 1), float(row.get("critical_alert_count", 0) + row.get("warning_alert_count", 0))) for index, row in enumerate(reversed(runs))]
    body = _cards(
        [
            ("Latest Pipeline Status", overview["latest_run"].get("status") or "n/a", overview["latest_run"].get("run_name") or "no pipeline runs found"),
            ("Monitoring Health", overview["monitoring"].get("status") or "n/a", "latest run health"),
            ("Approved Strategies", overview["registry"].get("approved_strategy_count") or 0, "from registry"),
            ("Generated Positions", overview["portfolio"].get("generated_position_count") or 0, "latest portfolio"),
            ("Executable Orders", overview["execution"].get("executable_order_count") or 0, "latest execution package"),
            ("Broker Health", overview["broker_health"].get("status") or "n/a", overview["broker_health"].get("message") or "not available"),
        ]
    )
    body += "<h2>Alert Counts Over Recent Runs</h2>" + _bar_chart(run_bars)
    body += "<h2>Quick Links</h2>" + _table(["label", "path"], overview["quick_links"])
    return _page_shell("Dashboard Overview", body)


def _strategies_page(service: DashboardDataService, query: dict[str, list[str]]) -> bytes:
    payload = service.strategies_payload()
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
    body += "<h2>Champion / Challenger</h2>" + _table(list(payload["champion_challenger"][0].keys()) if payload["champion_challenger"] else ["family", "champion"], payload["champion_challenger"])
    return _page_shell("Strategies", body)


def _portfolio_page(service: DashboardDataService) -> bytes:
    payload = service.portfolio_payload()
    summary = payload.get("summary", {})
    body = _cards(
        [
            ("Gross Exposure", summary.get("gross_exposure_after_constraints", "n/a"), "after constraints"),
            ("Net Exposure", summary.get("net_exposure_after_constraints", "n/a"), "after constraints"),
            ("Position Count", len(payload.get("combined_positions", [])), "latest combined portfolio"),
            ("Clipped Symbols", len(payload.get("clipped_symbols", [])), "constraint actions"),
        ]
    )
    body += "<h2>Top Position Weights</h2>" + _bar_chart([(str(row.get("symbol")), float(abs(row.get("target_weight", 0.0)))) for row in payload.get("top_positions", [])[:8]])
    body += "<h2>Sleeve Weights</h2>" + _table(["sleeve_name", "scaled_target_weight"], payload.get("sleeve_weights", []))
    body += "<h2>Top Positions</h2>" + _table(["symbol", "target_weight", "side", "latest_price"], payload.get("top_positions", []))
    body += "<h2>Overlap Diagnostics</h2>" + _table(list(payload["overlap"][0].keys()) if payload.get("overlap") else ["symbol"], payload.get("overlap", []))
    body += "<h2>Constraint Clipping</h2>" + _table(list(payload["clipped_symbols"][0].keys()) if payload.get("clipped_symbols") else ["constraint_name"], payload.get("clipped_symbols", []))
    return _page_shell("Portfolio", body)


def _execution_page(service: DashboardDataService) -> bytes:
    payload = service.execution_payload()
    summary = payload.get("summary", {})
    requested_notional = float(summary.get("requested_notional", 0.0) or 0.0)
    executed_notional = float(summary.get("executed_notional", 0.0) or 0.0)
    body = _cards(
        [
            ("Requested Orders", summary.get("requested_order_count", 0), "raw desired orders"),
            ("Executable Orders", summary.get("executable_order_count", 0), "after constraints"),
            ("Rejected Orders", summary.get("rejected_order_count", 0), "hard rejections"),
            ("Expected Cost", summary.get("expected_total_cost", 0.0), "fees + slippage"),
        ]
    )
    body += "<h2>Requested vs Executable Notional</h2>" + _bar_chart([("requested", requested_notional), ("executed", executed_notional)])
    body += "<h2>Executable Orders</h2>" + _table(["symbol", "side", "requested_shares", "adjusted_shares", "estimated_fill_price", "commission", "clipping_reason"], payload.get("executable_orders", [])[:20])
    body += "<h2>Rejected Orders</h2>" + _table(["symbol", "side", "requested_shares", "rejection_reason"], payload.get("rejected_orders", [])[:20])
    body += "<h2>Liquidity Diagnostics</h2>" + _table(list(payload["liquidity_diagnostics"][0].keys()) if payload.get("liquidity_diagnostics") else ["symbol"], payload.get("liquidity_diagnostics", [])[:20])
    return _page_shell("Execution", body)


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
    body = _cards(
        [
            ("Run Count", len(runs), "recent discovered runs"),
            ("Latest Status", runs[0]["status"] if runs else "n/a", "most recent run"),
            ("Latest Health", runs[0]["health_status"] if runs else "n/a", "most recent run"),
            ("Failures", runs[0]["failed_stage_count"] if runs else 0, "most recent run"),
        ]
    )
    body += "<h2>Critical Alerts Trend</h2>" + _bar_chart([(str(index + 1), float(row.get("critical_alert_count", 0))) for index, row in enumerate(reversed(runs[:10]))])
    body += "<h2>Recent Runs</h2>" + _table(["run_name", "status", "health_status", "schedule_type", "started_at", "failed_stage_count", "artifact_dir"], runs)
    latest = service.latest_run_detail_payload()
    body += "<h2>Latest Stage Status</h2>" + _table(["stage_name", "status", "started_at", "ended_at", "duration_seconds", "error_message"], latest.get("stages", []))
    return _page_shell("Runs", body)


def create_dashboard_app(artifacts_root: str | Path) -> Callable:
    service = DashboardDataService(artifacts_root)

    def app(environ, start_response):
        path = environ.get("PATH_INFO", "/")
        query = parse_qs(environ.get("QUERY_STRING", ""))
        if path == "/api/overview":
            status, headers, body = _json_response(service.overview_payload())
        elif path == "/api/runs":
            status, headers, body = _json_response(service.runs_payload())
        elif path == "/api/runs/latest":
            status, headers, body = _json_response(service.latest_run_detail_payload())
        elif path == "/api/strategies":
            status, headers, body = _json_response(service.strategies_payload())
        elif path == "/api/portfolio/latest":
            status, headers, body = _json_response(service.portfolio_payload())
        elif path == "/api/execution/latest":
            status, headers, body = _json_response(service.execution_payload())
        elif path == "/api/live/latest":
            status, headers, body = _json_response(service.live_payload())
        elif path == "/api/alerts/latest":
            status, headers, body = _json_response(service.latest_alerts_payload())
        elif path == "/":
            status, headers, body = "200 OK", [("Content-Type", "text/html; charset=utf-8")], _overview_page(service)
        elif path == "/strategies":
            status, headers, body = "200 OK", [("Content-Type", "text/html; charset=utf-8")], _strategies_page(service, query)
        elif path == "/portfolio":
            status, headers, body = "200 OK", [("Content-Type", "text/html; charset=utf-8")], _portfolio_page(service)
        elif path == "/execution":
            status, headers, body = "200 OK", [("Content-Type", "text/html; charset=utf-8")], _execution_page(service)
        elif path == "/live":
            status, headers, body = "200 OK", [("Content-Type", "text/html; charset=utf-8")], _live_page(service)
        elif path == "/runs":
            status, headers, body = "200 OK", [("Content-Type", "text/html; charset=utf-8")], _runs_page(service)
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
        "strategies.json": service.strategies_payload(),
        "portfolio_latest.json": service.portfolio_payload(),
        "execution_latest.json": service.execution_payload(),
        "live_latest.json": service.live_payload(),
        "alerts_latest.json": service.latest_alerts_payload(),
    }
    paths: dict[str, Path] = {}
    for name, payload in payloads.items():
        path = output_path / name
        path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        paths[name.replace(".", "_")] = path
    return paths
