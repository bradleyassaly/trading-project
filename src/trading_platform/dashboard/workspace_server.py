from __future__ import annotations

import html
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Callable
from urllib.parse import parse_qs
from wsgiref.simple_server import make_server

from trading_platform.dashboard.hybrid_dashboard_service import HybridDashboardDataService
from trading_platform.dashboard.service import DashboardDataService


NAV_ITEMS = [
    ("/", "Overview"),
    ("/trades", "Trades"),
    ("/strategies", "Strategies"),
    ("/portfolio", "Portfolio"),
    ("/research", "Research"),
    ("/ops", "Ops"),
]


def _json_response(payload: dict) -> tuple[str, list[tuple[str, str]], bytes]:
    return (
        "200 OK",
        [("Content-Type", "application/json; charset=utf-8")],
        json.dumps(payload, indent=2, default=str).encode("utf-8"),
    )


def _not_found() -> tuple[str, list[tuple[str, str]], bytes]:
    return "404 Not Found", [("Content-Type", "application/json; charset=utf-8")], b'{"error":"not_found"}'


def _escape(value: object) -> str:
    return html.escape(str(value if value not in (None, "") else "n/a"))


def _badge(value: object) -> str:
    text = str(value or "unknown")
    css = text.lower().replace(" ", "-").replace("_", "-")
    return f'<span class="badge {html.escape(css)}">{html.escape(text)}</span>'


def _format_number(value: object, *, pct: bool = False, money: bool = False, digits: int = 2) -> str:
    if value in (None, ""):
        return "n/a"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    if pct:
        return f"{number:.{digits}%}"
    if money:
        return f"${number:,.{digits}f}"
    if abs(number) >= 1000 and number.is_integer():
        return f"{int(number):,}"
    return f"{number:,.{digits}f}"


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
    rendered = candidate.astimezone(UTC).strftime("%Y-%m-%d %H:%M UTC")
    return f"{_badge(label)} <span class='subtle'>{html.escape(rendered)}</span>"


def _query_value(query: dict[str, list[str]], key: str) -> str | None:
    value = (query.get(key) or [None])[0]
    return value if value not in (None, "") else None


def _parse_positive_int(value: str | None, default: int) -> int:
    try:
        parsed = int(value or default)
    except ValueError:
        return default
    return parsed if parsed > 0 else default


def _query_filters(query: dict[str, list[str]], *keys: str, default_limit: int = 20) -> dict[str, str | int]:
    payload: dict[str, str | int] = {}
    for key in keys:
        value = _query_value(query, key)
        if value is not None:
            payload[key] = value
    payload["limit"] = _parse_positive_int(_query_value(query, "limit"), default_limit)
    payload["offset"] = max(int(_query_value(query, "offset") or 0), 0)
    return payload


def _validate_symbol(symbol: str) -> str | None:
    normalized = symbol.strip().upper()
    if not normalized:
        return None
    allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-")
    return normalized if all(char in allowed for char in normalized) else None


def _link_for(column: str, value: object) -> str | None:
    if value in (None, ""):
        return None
    text = html.escape(str(value))
    if column == "symbol":
        return f'<a class="table-link" href="/symbols/{text}">{text}</a>'
    if column == "strategy_id":
        return f'<a class="table-link" href="/strategies/{text}">{text}</a>'
    if column == "trade_id":
        return f'<a class="table-link" href="/trades/{text}">{text}</a>'
    if column == "run_id":
        return f'<a class="table-link" href="/runs/{text}">{text}</a>'
    if column in {"run_dir", "artifact_dir", "path"}:
        return f'<span class="mono">{text}</span>'
    return None


def _render_value(column: str, value: object) -> str:
    linked = _link_for(column, value)
    if linked:
        return linked
    if column.endswith("status") or column == "status":
        return _badge(value)
    if column.endswith("_pnl") or column in {"equity", "cash", "market_value", "portfolio_market_value", "expected_total_cost"}:
        return html.escape(_format_number(value, money=True))
    if "weight" in column or column in {"win_rate", "confidence_score", "drawdown", "gross_exposure", "net_exposure"}:
        try:
            number = float(value)
        except (TypeError, ValueError):
            return _escape(value)
        if abs(number) <= 1.5:
            return html.escape(_format_number(number, pct=True))
        return html.escape(_format_number(number))
    return _escape(value)


def _table(columns: list[str], rows: list[dict], *, empty: str = "No data available.") -> str:
    if not rows:
        return f"<section class='panel'><div class='empty'>{html.escape(empty)}</div></section>"
    head = "".join(f"<th>{html.escape(col.replace('_', ' '))}</th>" for col in columns)
    body = []
    for row in rows:
        mapping = _row_mapping(row)
        body.append("<tr>" + "".join(f"<td>{_render_value(column, mapping.get(column))}</td>" for column in columns) + "</tr>")
    return f"<section class='panel'><div class='table-wrap'><table><thead><tr>{head}</tr></thead><tbody>{''.join(body)}</tbody></table></div></section>"


def _metric_cards(rows: list[tuple[str, object, str]]) -> str:
    cards = []
    for label, value, detail in rows:
        cards.append(
            "<article class='metric-card'>"
            f"<div class='metric-label'>{html.escape(label)}</div>"
            f"<div class='metric-value'>{_escape(value)}</div>"
            f"<div class='metric-detail'>{detail}</div>"
            "</article>"
        )
    return f"<section class='metric-grid'>{''.join(cards)}</section>"


def _info_cards(rows: list[tuple[str, object, str]]) -> str:
    cards = []
    for label, value, detail in rows:
        cards.append(
            "<article class='info-card'>"
            f"<div class='info-label'>{html.escape(label)}</div>"
            f"<div class='info-value'>{_escape(value)}</div>"
            f"<div class='info-detail'>{detail}</div>"
            "</article>"
        )
    return f"<section class='info-grid'>{''.join(cards)}</section>"


def _section(title: str, content: str, *, subtitle: str | None = None) -> str:
    subtitle_html = f"<div class='section-subtitle'>{html.escape(subtitle)}</div>" if subtitle else ""
    return f"<section class='section'><div class='section-header'><h2>{html.escape(title)}</h2>{subtitle_html}</div>{content}</section>"


def _route_name(path: str) -> str:
    for prefix, label in NAV_ITEMS:
        if prefix != "/" and path.startswith(prefix):
            return label
    return "Overview"


def _row_mapping(row: object) -> dict:
    if isinstance(row, dict):
        return row
    if hasattr(row, "_asdict"):
        try:
            value = row._asdict()
        except TypeError:
            value = None
        if isinstance(value, dict):
            return value
    if hasattr(row, "to_dict"):
        try:
            value = row.to_dict()
        except TypeError:
            value = None
        if isinstance(value, dict):
            return value
    return {}


def _page_shell(*, title: str, active_path: str, body: str) -> bytes:
    nav = "".join(
        f"<a class='nav-link{' active' if active_path == path or (path != '/' and active_path.startswith(path)) else ''}' href='{path}'>{label}</a>"
        for path, label in NAV_ITEMS
    )
    css = """
html { color-scheme: light; } * { box-sizing: border-box; }
body { margin:0; font-family:"IBM Plex Sans","Aptos","Segoe UI",sans-serif; background:radial-gradient(circle at top left, rgba(26,68,120,.14), transparent 26%), linear-gradient(180deg,#eef3f8 0%,#e6edf5 100%); color:#142233; }
a { color:#164b7a; } .workspace { display:grid; grid-template-columns:260px minmax(0,1fr); min-height:100vh; }
.sidebar { background:linear-gradient(180deg,#0f2136 0%,#142840 100%); color:#dbe8f5; padding:28px 22px; border-right:1px solid rgba(255,255,255,.08); }
.brand { margin-bottom:28px; padding-bottom:22px; border-bottom:1px solid rgba(255,255,255,.10); } .eyebrow { text-transform:uppercase; letter-spacing:.14em; font-size:.72rem; color:#8cb9e2; font-weight:700; }
.brand h1 { margin:8px 0 10px; font-size:1.4rem; line-height:1.1; letter-spacing:-.03em; } .brand-copy { color:rgba(219,232,245,.72); font-size:.93rem; line-height:1.45; }
.nav { display:grid; gap:8px; } .nav-link { text-decoration:none; color:#d8e6f5; padding:11px 14px; border-radius:14px; border:1px solid transparent; background:rgba(255,255,255,.02); font-weight:600; }
.nav-link.active { background:linear-gradient(180deg, rgba(74,142,204,.26), rgba(34,77,117,.30)); border-color:rgba(140,185,226,.28); } .nav-link:hover { border-color:rgba(255,255,255,.18); }
.sidebar-meta { margin-top:28px; display:grid; gap:10px; } .sidebar-chip { border-radius:999px; padding:7px 10px; background:rgba(255,255,255,.06); border:1px solid rgba(255,255,255,.10); font-size:.82rem; display:inline-flex; width:fit-content; }
.main { padding:28px; } .topbar { display:flex; justify-content:space-between; align-items:start; gap:18px; margin-bottom:24px; } .topbar h1 { margin:6px 0 0; font-size:2rem; letter-spacing:-.04em; }
.topbar-copy { color:#5b6c7f; max-width:720px; line-height:1.5; } .topbar-chips { display:flex; flex-wrap:wrap; gap:8px; } .topbar-chip { display:inline-flex; align-items:center; padding:7px 10px; border-radius:999px; border:1px solid #c8d5e4; background:rgba(255,255,255,.72); color:#37506b; font-size:.82rem; }
.metric-grid,.info-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:14px; } .metric-card,.info-card,.panel,.hero { background:rgba(255,255,255,.74); backdrop-filter:blur(14px); border:1px solid rgba(104,129,156,.18); border-radius:20px; box-shadow:0 16px 34px rgba(30,54,78,.08); }
.metric-card,.info-card,.panel { padding:18px; } .metric-label,.info-label,.subtle { color:#68829c; } .metric-label,.info-label { text-transform:uppercase; letter-spacing:.12em; font-size:.70rem; font-weight:700; }
.metric-value { margin-top:10px; font-size:1.7rem; font-weight:700; letter-spacing:-.04em; } .metric-detail,.info-detail,.section-subtitle,.subtle { margin-top:8px; color:#5b6c7f; font-size:.9rem; line-height:1.45; }
.info-value { margin-top:10px; font-size:1.06rem; font-weight:700; } .section { margin-top:26px; } .section-header { display:flex; justify-content:space-between; align-items:end; gap:12px; margin-bottom:12px; } .section h2 { margin:0; font-size:1.1rem; letter-spacing:-.02em; }
.hero { padding:22px; margin-bottom:24px; } .hero-grid { display:grid; grid-template-columns:1.6fr 1fr; gap:18px; } .hero-summary { display:grid; gap:14px; } .hero-title { font-size:1.55rem; font-weight:700; letter-spacing:-.04em; }
.hero-copy { color:#516478; line-height:1.55; } .hero-actions { display:flex; flex-wrap:wrap; gap:8px; } .action-link { text-decoration:none; border-radius:999px; padding:9px 12px; background:#183b5d; color:white; font-weight:600; }
.action-link.secondary { background:rgba(22,75,122,.08); color:#164b7a; border:1px solid rgba(22,75,122,.16); } .table-wrap { overflow:auto; } table { width:100%; border-collapse:collapse; }
th,td { padding:12px 14px; text-align:left; border-bottom:1px solid rgba(104,129,156,.14); vertical-align:top; font-size:.92rem; } th { color:#627b95; text-transform:uppercase; letter-spacing:.08em; font-size:.72rem; position:sticky; top:0; background:rgba(246,249,252,.96); }
.badge { display:inline-flex; align-items:center; border-radius:999px; padding:4px 9px; font-size:.76rem; font-weight:700; background:#e8eef5; color:#39526c; }
.badge.succeeded,.badge.filled,.badge.open,.badge.approved,.badge.pass,.badge.healthy,.badge.included,.badge.long,.badge.fresh { background:#dff3e9; color:#176748; }
.badge.warning,.badge.pending,.badge.paper,.badge.weak,.badge.stale { background:#fff1d8; color:#8f6414; }
.badge.failed,.badge.rejected,.badge.closed,.badge.demoted,.badge.critical,.badge.sell,.badge.short,.badge.excluded { background:#fbe2df; color:#9e3027; }
.empty { padding:20px 4px 4px; color:#627b95; } .mono { font-family:"IBM Plex Mono","Cascadia Mono",monospace; font-size:.84rem; } .table-link { color:#164b7a; text-decoration:none; font-weight:600; }
.trade-chart { width:100%; min-height:360px; border-radius:18px; background:linear-gradient(180deg, rgba(12,30,48,.98), rgba(17,37,57,.98)), repeating-linear-gradient(0deg, rgba(255,255,255,.05) 0 1px, transparent 1px 48px); }
.readout { margin-top:12px; padding:12px 14px; border-radius:14px; background:rgba(15,33,54,.05); color:#40556c; } .timeline { display:grid; gap:10px; } .timeline-item { padding:14px 16px; border:1px solid rgba(104,129,156,.14); border-radius:16px; background:rgba(247,249,251,.72); }
.timeline-title { display:flex; justify-content:space-between; gap:12px; align-items:center; } .filter-bar { display:flex; flex-wrap:wrap; gap:10px; padding:14px 0 4px; } .filter-chip { padding:8px 11px; border-radius:999px; background:rgba(22,75,122,.08); border:1px solid rgba(22,75,122,.12); color:#164b7a; text-decoration:none; font-weight:600; font-size:.84rem; }
.filter-chip.active { background:#183b5d; border-color:#183b5d; color:white; } @media (max-width:1100px) { .workspace { grid-template-columns:1fr; } .sidebar { border-right:0; border-bottom:1px solid rgba(255,255,255,.08); } .hero-grid { grid-template-columns:1fr; } } @media (max-width:760px) { .main { padding:18px; } .topbar { flex-direction:column; } }
"""
    page = f"""<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>{html.escape(title)}</title><style>{css}</style></head><body><div class="workspace"><aside class="sidebar"><div class="brand"><div class="eyebrow">Trading Workspace</div><h1>Quant Control Center</h1><div class="brand-copy">Artifact-driven, local-first dashboard focused on explainability, portfolio context, and operational visibility.</div></div><nav class="nav">{nav}</nav><div class="sidebar-meta"><span class="sidebar-chip">Read-only</span><span class="sidebar-chip">Local artifacts</span><span class="sidebar-chip">Decision lineage</span></div></aside><main class="main"><header class="topbar"><div><div class="eyebrow">{html.escape(_route_name(active_path))}</div><h1>{html.escape(title)}</h1><div class="topbar-copy">Structured trade intelligence across signal generation, portfolio selection, execution, and outcomes.</div></div><div class="topbar-chips"><span class="topbar-chip">Professional workspace</span><span class="topbar-chip">Audit-first</span><span class="topbar-chip">Laptop-friendly</span></div></header>{body}</main></div></body></html>"""
    return page.encode("utf-8")


def _overview_page(service: DashboardDataService) -> bytes:
    overview = service.overview_payload()
    blotter = service.trade_blotter_payload()
    discovery = service.discovery_payload()
    ops = service.ops_payload()
    body = "<section class='hero'><div class='hero-grid'><div class='hero-summary'><div class='eyebrow'>Command Center</div><div class='hero-title'>Trade intelligence, portfolio state, and system health in one workspace.</div><div class='hero-copy'>Start with recent trades and positions, then drill directly into trade-level decision lineage and execution evidence.</div><div class='hero-actions'><a class='action-link' href='/trades'>Open Trades Blotter</a><a class='action-link secondary' href='/portfolio'>Portfolio & Risk</a><a class='action-link secondary' href='/ops'>Ops & Run Health</a></div></div>"
    body += _info_cards([("Latest Run", overview.get("latest_run", {}).get("run_name") or "n/a", _freshness_badge(timestamp=overview.get("latest_run", {}).get("started_at"))), ("Monitoring", overview.get("monitoring", {}).get("status") or "n/a", "Latest run health state"), ("Open Trades", blotter.get("summary", {}).get("open_trade_count") or 0, "Current blotter state")])
    body += "</div></section>"
    body += _section("Desk Overview", _metric_cards([("Equity / Return", _format_number(overview.get("system_evaluation", {}).get("total_return"), pct=True), "Latest evaluated system return"), ("Sharpe", _format_number(overview.get("system_evaluation", {}).get("sharpe")), "System evaluation"), ("Open Trades", blotter.get("summary", {}).get("open_trade_count") or 0, "Trade blotter"), ("Closed Trades", blotter.get("summary", {}).get("closed_trade_count") or 0, "Trade blotter"), ("Portfolio Positions", overview.get("portfolio", {}).get("generated_position_count") or 0, "Latest allocation output"), ("Executable Orders", overview.get("execution", {}).get("executable_order_count") or 0, "Current execution package"), ("Approved Strategies", overview.get("registry", {}).get("approved_strategy_count") or 0, "Registry"), ("Critical Alerts", ops.get("summary", {}).get("critical_alert_count") or 0, "Latest run")]))
    body += _section("Recent Trades", _table(["trade_id", "timestamp", "symbol", "side", "strategy_id", "ranking_score", "target_weight", "order_status", "realized_pnl", "status"], blotter.get("trades", [])[:8], empty="No recent trades found."), subtitle="Recent proposed, open, and closed trades with direct drill-down links.")
    body += _section("Recent Symbols", _table(["symbol", "trade_count", "latest_trade_id", "latest_entry_ts", "latest_strategy_id", "status"], discovery.get("recent_symbols", []), empty="No recent symbols found."))
    body += _section("Open Positions", _table(["symbol", "side", "qty", "avg_price", "market_value"], service.portfolio_overview_payload().get("positions", [])[:8], empty="No open positions found."))
    body += _section("Strategy Pulse", _table(["strategy_id", "trade_count", "closed_trade_count", "latest_symbol", "latest_entry_ts", "latest_source", "latest_run_id"], discovery.get("recent_strategies", []), empty="No strategy activity found."))
    return _page_shell(title="Overview", active_path="/", body=body)


def _trades_page(service: DashboardDataService, query: dict[str, list[str]]) -> bytes:
    filters = _query_filters(query, "status", "strategy", "symbol", "run_id", "date_from", "date_to", default_limit=50)
    payload = service.trade_blotter_payload(filters)
    rows = payload.get("trades", [])
    status_filter = _query_value(query, "status")
    strategy_filter = _query_value(query, "strategy")
    strategies = sorted({str(row.get("strategy_id")) for row in payload.get("trades", []) if row.get("strategy_id")})
    chips = [("All", "/trades", status_filter is None), ("Open", "/trades?status=open", status_filter == "open"), ("Closed", "/trades?status=closed", status_filter == "closed")]
    filter_links = "".join(f"<a class='filter-chip{' active' if active else ''}' href='{href}'>{label}</a>" for label, href, active in chips)
    filter_links += "".join(f"<a class='filter-chip{' active' if strategy_filter == strategy else ''}' href='/trades?strategy={html.escape(strategy)}'>{html.escape(strategy)}</a>" for strategy in strategies[:4])
    pagination = payload.get("pagination", {})
    body = _metric_cards([("Trade Count", payload.get("summary", {}).get("trade_count") or 0, "Across discovered explicit ledgers"), ("Open", payload.get("summary", {}).get("open_trade_count") or 0, "Currently active"), ("Closed", payload.get("summary", {}).get("closed_trade_count") or 0, "Completed trades"), ("Page Size", pagination.get("limit") or len(rows), "Requested slice"), ("Source", payload.get("source") or "n/a", "Read path")])
    body += f"<section class='panel'><div class='subtle'>total={_escape(pagination.get('total_count') or len(rows))} offset={_escape(pagination.get('offset') or 0)} has_more={_escape(pagination.get('has_more'))} source={_escape(payload.get('source'))}</div><div class='filter-bar'>{filter_links}</div></section>"
    body += _section("Trades Blotter", _table(["trade_id", "timestamp", "symbol", "side", "qty", "target_weight", "strategy_id", "signal_score", "ranking_score", "expected_edge", "order_status", "realized_pnl", "unrealized_pnl", "status"], rows, empty="No trades match the current filters."), subtitle="The blotter centers the platform on inspectable trades rather than raw artifact files.")
    return _page_shell(title="Trades Blotter", active_path="/trades", body=body)


def _timeline(rows: list[dict]) -> str:
    if not rows:
        return "<section class='panel'><div class='empty'>No lifecycle events found.</div></section>"
    items = []
    for row in rows:
        items.append("<div class='timeline-item'>" f"<div class='timeline-title'><strong>{html.escape(str(row.get('label') or row.get('kind') or 'event'))}</strong>{_badge(row.get('status'))}</div>" f"<div class='subtle'>{_escape(row.get('ts'))}</div><div class='metric-detail'>{_escape(row.get('detail'))}</div></div>")
    return f"<section class='panel'><div class='timeline'>{''.join(items)}</div></section>"


def _trade_chart(payload: dict, trade: dict) -> str:
    return f"""<section class="panel"><svg id="trade-chart" class="trade-chart" viewBox="0 0 960 360" preserveAspectRatio="none"></svg><div id="trade-readout" class="readout"><strong>{html.escape(str(trade.get('trade_id') or 'trade'))}</strong><div>Hover markers for price and execution context.</div></div></section><script>
const tradeChartPayload = {json.dumps(payload)}; const tradeSummary = {json.dumps(trade)};
function tradeReadout(primary, secondary) {{ document.getElementById("trade-readout").innerHTML = `<strong>${{primary}}</strong><div>${{secondary}}</div>`; }}
function renderTradeChart() {{
 const svg = document.getElementById("trade-chart"); const bars = tradeChartPayload.bars || []; if (!bars.length) {{ svg.innerHTML = "<text x='24' y='44' fill='#b9c9da'>No price history available for this trade window.</text>"; return; }}
 const width=960,height=360,padL=54,padR=24,padT=22,padB=30; const lows=bars.map((row)=>Number(row.low ?? row.close)).filter(Number.isFinite); const highs=bars.map((row)=>Number(row.high ?? row.close)).filter(Number.isFinite); const minP=Math.min(...lows),maxP=Math.max(...highs),span=Math.max(maxP-minP,1e-9);
 const x=(i)=>padL+((width-padL-padR)*i/Math.max(bars.length-1,1)); const y=(price)=>padT+((maxP-price)/span)*(height-padT-padB); const idx=(ts)=>bars.findIndex((bar)=>bar.ts===ts);
 const line = bars.map((bar,i)=>`${{i===0?'M':'L'}}${{x(i)}},${{y(Number(bar.close))}}`).join(" "); const grid=[0,0.25,0.5,0.75,1].map((r)=>{{ const py=padT+(height-padT-padB)*r; const price=(maxP-span*r).toFixed(2); return `<g><line x1="${{padL}}" y1="${{py}}" x2="${{width-padR}}" y2="${{py}}" stroke="rgba(255,255,255,0.08)"></line><text x="10" y="${{py+4}}" fill="#d2dee9" font-size="11">${{price}}</text></g>`; }}).join("");
 const markers=[]; const entryIdx=idx(tradeSummary.entry_ts); const exitIdx=idx(tradeSummary.exit_ts); if (entryIdx>=0) markers.push(`<circle cx="${{x(entryIdx)}}" cy="${{y(Number(tradeSummary.entry_price ?? bars[entryIdx].close))}}" r="7" fill="#1ec98f" data-info="Entry | ${{tradeSummary.entry_ts}} | price=${{tradeSummary.entry_price}}"></circle>`); if (exitIdx>=0) markers.push(`<circle cx="${{x(exitIdx)}}" cy="${{y(Number(tradeSummary.exit_price ?? bars[exitIdx].close))}}" r="7" fill="#ff8d6a" data-info="Exit | ${{tradeSummary.exit_ts}} | price=${{tradeSummary.exit_price}}"></circle>`);
 const fills=(tradeChartPayload.fills||[]).map((row)=>{{ const i=idx(row.ts); if(i<0) return ""; return `<rect x="${{x(i)-4}}" y="${{y(Number(row.price ?? bars[i].close))-4}}" width="8" height="8" fill="#f9cf58" data-info="Fill | side=${{row.side||''}} qty=${{row.qty||''}} price=${{row.price||''}}"></rect>`; }}).join("");
 const signals=(tradeChartPayload.signals||[]).map((row)=>{{ const i=idx(row.ts); if(i<0) return ""; return `<circle cx="${{x(i)}}" cy="${{y(Number(row.price ?? bars[i].close))}}" r="4.5" fill="#ffffff" stroke="#3f7ed2" stroke-width="2" data-info="Signal | ${{row.label || row.type || ''}} | score=${{row.score ?? 'n/a'}}"></circle>`; }}).join("");
 svg.innerHTML = `${{grid}}<path d="${{line}}" fill="none" stroke="#8cc8ff" stroke-width="2.4"></path>${{markers.join("")}}${{signals}}${{fills}}`; svg.querySelectorAll("[data-info]").forEach((node)=>{{ node.addEventListener("mouseenter", ()=>tradeReadout(tradeSummary.trade_id || "trade", node.getAttribute("data-info") || "")); node.addEventListener("mouseleave", ()=>tradeReadout(tradeSummary.trade_id || "trade", "Hover markers for price and execution context.")); }});
}} renderTradeChart();</script>"""


def _trade_detail_page(service: DashboardDataService, trade_id: str) -> bytes:
    payload = service.trade_detail_payload(trade_id)
    trade = payload.get("trade") or {}
    if not trade:
        return _page_shell(title=f"Trade {trade_id}", active_path="/trades", body="<section class='panel'><div class='empty'>Trade not found.</div></section>")
    explain = payload.get("explain", {})
    signal = explain.get("signal") or {}
    body = "<section class='hero'><div class='hero-grid'><div class='hero-summary'>"
    body += f"<div class='eyebrow'>Trade Intelligence</div><div class='hero-title'>{_escape(trade.get('symbol'))} {html.escape(str(trade.get('side') or '').upper())} | {_escape(trade.get('trade_id'))}</div>"
    body += "<div class='hero-copy'>Decision lineage is organized from trade summary through signal evidence, portfolio selection context, execution trace, and realized outcome. Missing data remains explicit instead of fabricated.</div>"
    body += f"<div class='hero-actions'><a class='action-link' href='/symbols/{html.escape(str(trade.get('symbol') or ''))}'>Open Symbol Context</a><a class='action-link secondary' href='/strategies/{html.escape(str(trade.get('strategy_id') or ''))}'>Open Strategy</a><a class='action-link secondary' href='/trades'>Back to Blotter</a></div></div>"
    body += _info_cards([("Status", trade.get("status") or "n/a", "Lifecycle state"), ("Strategy", trade.get("strategy_id") or "n/a", "Generating strategy"), ("Run", payload.get("related_metadata", {}).get("run_id") or "latest", _freshness_badge(path=payload.get("related_metadata", {}).get("trade_source")))])
    body += "</div></section>"
    body += _section("Trade Summary", _metric_cards([("Side", trade.get("side") or "n/a", "Trade direction"), ("Quantity", trade.get("qty") or 0, "Ledger size"), ("Entry", trade.get("entry_price") or "n/a", _escape(trade.get("entry_ts"))), ("Exit", trade.get("exit_price") or "n/a", _escape(trade.get("exit_ts"))), ("Realized PnL", _format_number(trade.get("realized_pnl"), money=True), "Closed outcome"), ("Hold (hrs)", _format_number(trade.get("hold_duration_hours")), "Entry to exit")]))
    body += _section("Why This Trade Happened", _info_cards([("Signal / Trigger", signal.get("label") or signal.get("type") or "Not available", "Nearest signal before entry"), ("Signal Score", signal.get("score") or "n/a", "From current artifacts if present"), ("Ranking Score", payload.get("provenance", {}).get("ranking_score") or "n/a", "Selection/ranking evidence"), ("Universe Rank", payload.get("provenance", {}).get("universe_rank") or "n/a", "Relative candidate rank"), ("Target Weight", payload.get("portfolio_context", {}).get("target_weight") or "n/a", "Portfolio sizing target"), ("Constraint Hits", ", ".join(payload.get("portfolio_context", {}).get("constraint_hits", [])) or "None recorded", "Constraint-aware selection notes")]) + _table(["ts", "signal_type", "ranking_score", "universe_rank", "selection_status", "exclusion_reason", "target_weight", "order_intent_summary"], payload.get("provenance", {}).get("rows", []), empty="No decision provenance rows found."), subtitle="Evidence is sourced from available signal and provenance artifacts only.")
    body += _section("Portfolio Context", _info_cards([("Selection Status", payload.get("portfolio_context", {}).get("selection_status") or "n/a", "Included/excluded at the decision point"), ("Selected Among Alternatives", payload.get("portfolio_context", {}).get("selected_among_alternatives"), f"Candidate count: {_escape(payload.get('portfolio_context', {}).get('candidate_count'))}"), ("Current Portfolio Qty", payload.get("portfolio_context", {}).get("portfolio_qty") or 0, "Latest position snapshot"), ("Market Value", _format_number(payload.get("portfolio_context", {}).get("portfolio_market_value"), money=True), "Current portfolio context"), ("Unrealized PnL", _format_number(payload.get("portfolio_context", {}).get("unrealized_pnl"), money=True), "If still held"), ("Regime Context", (explain.get("regime") or {}).get("regime_label") or "n/a", "Latest market regime artifact")]))
    body += _section("Trade Chart", _trade_chart(payload.get("chart", {}), trade), subtitle="Lightweight annotated price chart with entry, exit, fills, and nearby signals.")
    body += _section("Associated Signals", _table(["ts", "type", "label", "price", "score"], payload.get("signals", []), empty="No associated signals found."))
    body += _section("Execution Review", _info_cards([("Order Count", payload.get("execution_review", {}).get("order_count") or 0, "Orders discovered in the trade window"), ("Fill Count", payload.get("execution_review", {}).get("fill_count") or 0, "Fills discovered in the trade window"), ("Executed Qty", payload.get("execution_review", {}).get("executed_qty") or 0, "Aggregate discovered fills"), ("Avg Fill Price", payload.get("execution_review", {}).get("average_fill_price") or "n/a", "Weighted by discovered fill quantity"), ("Latest Order Status", payload.get("execution_review", {}).get("latest_order_status") or "n/a", "Most recent order state"), ("Latest Fill Status", payload.get("execution_review", {}).get("latest_fill_status") or "n/a", "Most recent fill state")]) + _table(["ts", "side", "qty", "price", "order_id", "status", "reason"], payload.get("orders", []), empty="No associated orders found.") + _table(["ts", "side", "qty", "price", "order_id", "status"], payload.get("fills", []), empty="No associated fills found."))
    body += _section("Order Lifecycle", _timeline(payload.get("lifecycle", [])))
    body += _section("Outcome Review", _info_cards([("Trade Status", payload.get("outcome_review", {}).get("trade_status") or "n/a", "Open vs closed"), ("Realized PnL", _format_number(payload.get("outcome_review", {}).get("realized_pnl"), money=True), "Closed-trade result"), ("Unrealized PnL", _format_number(payload.get("outcome_review", {}).get("unrealized_pnl"), money=True), "Current mark-to-market if open"), ("Price Change", _format_number(payload.get("outcome_review", {}).get("price_change")), "Entry to exit price delta"), ("Holding Period", _format_number(payload.get("outcome_review", {}).get("holding_period_hours")), "Hours held"), ("Indicator Snapshot", ", ".join(f"{key}={value}" for key, value in (explain.get("indicator_snapshot") or {}).items()) or "Not available", "Features captured around entry")]))
    body += _section("Decision Provenance", _table(["ts", "signal_type", "ranking_score", "universe_rank", "selection_status", "exclusion_reason", "target_weight", "order_intent_summary"], payload.get("provenance", {}).get("rows", []), empty="No decision provenance rows found."))
    body += _section("Related Strategy / Run Metadata", _info_cards([("Source", payload.get("related_metadata", {}).get("source") or "n/a", "Artifact source"), ("Run ID", payload.get("related_metadata", {}).get("run_id") or "n/a", "Related run metadata"), ("Mode", payload.get("related_metadata", {}).get("mode") or "n/a", "Paper/live/research mode"), ("Trade Source Mode", payload.get("related_metadata", {}).get("trade_source_mode") or "n/a", "Explicit vs derived"), ("Trade Source", payload.get("related_metadata", {}).get("trade_source") or "n/a", "Underlying artifact file"), ("Position Source", payload.get("related_metadata", {}).get("position_source") or "n/a", "Latest position artifact")]) + _table(["trade_id", "symbol", "side", "qty", "entry_ts", "exit_ts", "realized_pnl", "status", "strategy_id"], payload.get("comparison", {}).get("related_trades", []), empty="No related trades found."))
    return _page_shell(title=f"Trade Detail: {trade_id}", active_path="/trades", body=body)


def _strategies_page(service: DashboardDataService, query: dict[str, list[str]]) -> bytes:
    filters = _query_filters(query, "status", "strategy", "decision", "date_from", "date_to", default_limit=20)
    payload = service.strategies_payload(filters); rows = payload.get("strategies", []); status_filter = _query_value(query, "status")
    if status_filter: rows = [row for row in rows if row.get("status") == status_filter]
    body = _metric_cards([("Strategy Count", len(payload.get("strategies", [])), "Registry entries"), ("Approved", payload.get("summary", {}).get("status_counts", {}).get("approved", 0), "Production-approved"), ("Families", len(payload.get("summary", {}).get("family_counts", {})), "Observed families"), ("Under Review", payload.get("summary", {}).get("lifecycle_counts", {}).get("under_review", 0), "Lifecycle governance")])
    body += _section("Strategy Registry", _table(["strategy_id", "status", "family", "version", "preset_name", "universe", "current_deployment_stage"], rows))
    body += _section("Lifecycle State", _table(["strategy_id", "preset_name", "current_state", "validation_status", "monitoring_recommendation", "adaptive_adjusted_weight", "latest_reasons"], payload.get("strategy_lifecycle", []), empty="No lifecycle rows found."))
    body += _section("Recent Promotions", _table(["promotion_decision_id", "strategy_name", "strategy_version", "decision", "promoted_status", "source_research_run_name"], payload.get("recent_promotions", []), empty="No DB-backed promotions found."))
    body += _section("Champion / Challenger", _table(list(payload.get("champion_challenger", [{}])[0].keys()) if payload.get("champion_challenger") else ["family"], payload.get("champion_challenger", []), empty="No champion/challenger mapping found."))
    return _page_shell(title="Strategies", active_path="/strategies", body=body)


def _strategy_detail_page(service: DashboardDataService, strategy_id: str) -> bytes:
    payload = service.strategy_detail_payload(strategy_id); summary = payload.get("summary", {})
    body = _metric_cards([("Closed Trades", summary.get("closed_trade_count") or 0, "Explicit ledgers"), ("Open Trades", summary.get("open_trade_count") or 0, "Currently active"), ("Win Rate", _format_number(summary.get("win_rate"), pct=True), "Closed trades only"), ("Expectancy", _format_number(summary.get("expectancy"), money=True), "Average realized pnl"), ("Avg Hold (hrs)", _format_number(summary.get("average_holding_period_hours")), "Closed trades"), ("Realized PnL", _format_number(summary.get("cumulative_realized_pnl"), money=True), "Closed trades")])
    body += _section("Recent Trades", _table(["trade_id", "symbol", "side", "qty", "entry_ts", "exit_ts", "realized_pnl", "status"], payload.get("trades", []), empty="No trades found."))
    body += _section("Run / Source Comparison", _table(["source", "run_id", "mode", "trade_count", "closed_trade_count", "open_trade_count", "cumulative_realized_pnl", "win_rate"], payload.get("comparisons", []), empty="No source comparisons found."))
    body += _section("PnL By Symbol", _table(["symbol", "trade_count", "closed_trade_count", "cumulative_realized_pnl", "win_rate"], payload.get("pnl_by_symbol", []), empty="No pnl rows found."))
    return _page_shell(title=f"Strategy Detail: {strategy_id}", active_path="/strategies", body=body)


def _portfolio_page(service: DashboardDataService) -> bytes:
    payload = service.portfolio_payload(); overview = service.portfolio_overview_payload(); summary = payload.get("summary", {}); portfolio_summary = overview.get("summary", {})
    body = _metric_cards([("Equity", _format_number(portfolio_summary.get("equity"), money=True), "Latest portfolio summary"), ("Cash", _format_number(portfolio_summary.get("cash"), money=True), "Latest portfolio summary"), ("Open Positions", portfolio_summary.get("open_position_count") or 0, "Current holdings"), ("Gross Exposure", _format_number(summary.get("gross_exposure_after_constraints"), pct=True), "After constraints"), ("Net Exposure", _format_number(summary.get("net_exposure_after_constraints"), pct=True), "After constraints"), ("Latest Drawdown", _format_number(portfolio_summary.get("latest_drawdown"), pct=True), "From equity curve")])
    body = _section("Portfolio Summary", body) + ""
    body += _section("Current Open Positions", _table(["symbol", "side", "qty", "avg_price", "market_value"], overview.get("positions", []), empty="No open positions found."))
    body += _section("Exposure By Symbol", _table(["symbol", "side", "market_value", "weight_proxy"], overview.get("exposure", []), empty="No exposure rows found."))
    body += _section("Recent Activity", _table(["kind", "ts", "symbol", "side", "qty", "price", "status"], overview.get("recent_activity", []), empty="No recent activity found."))
    body += _section("Best Recent Trades", _table(["trade_id", "symbol", "side", "qty", "realized_pnl", "entry_ts", "exit_ts", "strategy_id", "status"], overview.get("best_trades", []), empty="No winning realized trades found."))
    body += _section("Worst Recent Trades", _table(["trade_id", "symbol", "side", "qty", "realized_pnl", "entry_ts", "exit_ts", "strategy_id", "status"], overview.get("worst_trades", []), empty="No losing realized trades found."))
    body += _section("Regime & Allocation Context", _table(["preset_name", "prior_weight", "adjusted_weight", "delta_weight", "monitoring_recommendation"], payload.get("adaptive_allocation", {}).get("top_changes", []), empty="No adaptive allocation changes found."))
    return _page_shell(title="Portfolio & Risk", active_path="/portfolio", body=body)


def _research_page(service: DashboardDataService) -> bytes:
    payload = service.research_latest_payload()
    body = _metric_cards([("Research Runs", payload.get("summary", {}).get("run_count", 0), "Indexed research manifests"), ("Eligible Candidates", payload.get("summary", {}).get("eligible_candidate_count", 0), "Promotion ready"), ("Promoted Strategies", payload.get("summary", {}).get("promoted_strategy_count", 0), "Generated strategy presets"), ("Validated Pass", payload.get("strategy_validation", {}).get("summary", {}).get("pass_count", 0), "Walk-forward validation")])
    body += _section("Leaderboard", _table(["rank", "run_id", "signal_family", "universe", "metric_name", "metric_value", "promotion_recommendation"], payload.get("leaderboard", []), empty="No leaderboard rows found."))
    body += _section("Promotion Candidates", _table(["run_id", "eligible", "promotion_recommendation", "mean_spearman_ic", "portfolio_sharpe", "reasons"], payload.get("promotion_candidates", []), empty="No promotion candidates found."))
    body += _section("Recent Promotions", _table(["promotion_decision_id", "strategy_name", "strategy_version", "decision", "promoted_status", "source_research_run_name"], payload.get("recent_promotions", []), empty="No DB-backed promotion history found."))
    body += _section("Selected Strategy Portfolio", _table(["preset_name", "allocation_weight", "signal_family", "universe", "selection_rank"], payload.get("strategy_portfolio", {}).get("selected_strategies", []), empty="No selected strategy portfolio found."))
    return _page_shell(title="Research", active_path="/research", body=body)


def _ops_page(service: DashboardDataService) -> bytes:
    filters = {}
    payload = service.ops_payload(filters); latest_run = payload.get("latest_run", {})
    body = _metric_cards([("Latest Run", payload.get("summary", {}).get("latest_run_name") or "n/a", "Most recent pipeline"), ("Run Status", payload.get("summary", {}).get("latest_run_status") or "n/a", "Most recent pipeline"), ("Health", payload.get("summary", {}).get("health_status") or "n/a", "Latest run health"), ("Critical Alerts", payload.get("summary", {}).get("critical_alert_count") or 0, "Latest run"), ("Blocked Checks", payload.get("summary", {}).get("blocked_check_count") or 0, "Latest live submission"), ("Missing Fills", payload.get("summary", {}).get("missing_fill_count") or 0, "Execution diagnostics")])
    body += _section("Latest Run Detail", _table(["stage_name", "status", "started_at", "ended_at", "duration_seconds", "error_message"], latest_run.get("stages", []), empty="No stage records found."))
    body += _section("Recent Runs", _table(["run_id", "run_name", "status", "health_status", "schedule_type", "started_at", "failed_stage_count", "artifact_dir"], payload.get("runs", []), empty="No runs found."))
    body += _section("DB Activity Feed", _table(["activity_type", "symbol", "strategy_name", "run_name", "status", "submitted_at", "created_at", "timestamp"], payload.get("db_activity", {}).get("activity_feed", {}).get("items", []), empty="No DB activity found."))
    body += _section("Orchestration Runs", _table(["run_id", "run_name", "experiment_name", "variant_name", "status", "schedule_frequency", "selected_strategy_count", "total_return", "sharpe", "warning_strategy_count", "kill_switch_recommendation_count", "run_dir"], payload.get("orchestration_runs", []), empty="No orchestration runs found."))
    body += _section("Live Risk Checks", _table(["check_name", "passed", "hard_block", "severity", "message"], payload.get("live", {}).get("risk_checks", []), empty="No live risk checks found."))
    body += _section("Execution Diagnostics", _table(["symbol", "signal_ts", "fill_ts", "latency_seconds", "signal_price", "fill_price", "slippage_bps"], payload.get("execution_diagnostics", {}).get("rows", []), empty="No execution diagnostics found."))
    return _page_shell(title="Ops & Run Health", active_path="/ops", body=body)


def _live_page(service: DashboardDataService) -> bytes:
    payload = service.live_payload()
    body = _metric_cards([("Dry-Run Orders", payload.get("dry_run_summary", {}).get("adjusted_order_count", 0), "Latest preview"), ("Submitted Orders", payload.get("submission_summary", {}).get("submitted_order_count", 0), "Latest submission"), ("Duplicate Skips", payload.get("submission_summary", {}).get("duplicate_order_skip_count", 0), "Idempotency protection"), ("Broker Health", payload.get("broker_health", {}).get("status") or "n/a", payload.get("broker_health", {}).get("message") or "Not available")])
    body += _section("Pre-Trade Risk Checks", _table(["check_name", "passed", "hard_block", "severity", "message"], payload.get("risk_checks", []), empty="No risk checks found."))
    body += _section("Blocked Checks", _table(["check_name", "severity", "message"], payload.get("blocked_checks", []), empty="No blocked checks found."))
    return _page_shell(title="Live Readiness", active_path="/ops", body=body)


def _execution_page(service: DashboardDataService) -> bytes:
    payload = service.execution_payload(); diagnostics = service.execution_diagnostics_payload()
    body = _metric_cards([("Requested Orders", payload.get("summary", {}).get("requested_order_count", 0), "Before constraints"), ("Executable Orders", payload.get("summary", {}).get("executable_order_count", 0), "After constraints"), ("Rejected Orders", payload.get("summary", {}).get("rejected_order_count", 0), "Hard failures"), ("Expected Cost", _format_number(payload.get("summary", {}).get("expected_total_cost"), money=True), "Fees + slippage"), ("Avg Latency", _format_number(diagnostics.get("summary", {}).get("average_signal_to_fill_latency_seconds")), "Seconds"), ("Avg Slippage", _format_number(diagnostics.get("summary", {}).get("average_slippage_bps")), "Bps")])
    body += _section("Artifact Sources", _info_cards([("Orders Source", diagnostics.get("meta", {}).get("orders_source") or "n/a", _freshness_badge(path=diagnostics.get("meta", {}).get("orders_source"))), ("Fills Source", diagnostics.get("meta", {}).get("fills_source") or "n/a", _freshness_badge(path=diagnostics.get("meta", {}).get("fills_source"))), ("Rejected Source", diagnostics.get("meta", {}).get("rejected_source") or "n/a", _freshness_badge(path=diagnostics.get("meta", {}).get("rejected_source")))]))
    body += _section("Executable Orders", _table(["symbol", "side", "requested_shares", "adjusted_shares", "estimated_fill_price", "commission", "clipping_reason"], payload.get("executable_orders", []), empty="No executable orders found."))
    body += _section("Rejected Orders", _table(["symbol", "side", "requested_shares", "rejection_reason"], payload.get("rejected_orders", []), empty="No rejected orders found."))
    body += _section("Execution Diagnostics", _table(["symbol", "signal_ts", "fill_ts", "latency_seconds", "signal_price", "fill_price", "slippage_bps"], diagnostics.get("rows", []), empty="No execution diagnostics found."))
    return _page_shell(title="Execution", active_path="/ops", body=body)


def _runs_page(service: DashboardDataService) -> bytes:
    return _ops_page(service)


def _run_detail_page(service: DashboardDataService, run_id: str, query: dict[str, list[str]]) -> bytes:
    payload = service.latest_run_detail_payload(run_id, run_kind=_query_value(query, "run_kind"))
    summary = payload.get("summary", {})
    body = _metric_cards([("Run", summary.get("run_name") or "n/a", "Run key"), ("Status", summary.get("status") or "n/a", "Lifecycle"), ("Artifacts", summary.get("artifact_count") or 0, "Linked artifact rows"), ("Source", payload.get("source") or "n/a", "Read path")])
    body += _section("Run Metadata", _info_cards([("Run ID", summary.get("run_id") or run_id, "Stable DB id"), ("Run Kind", summary.get("run_kind") or "n/a", "Research vs portfolio"), ("Started", summary.get("started_at") or "n/a", "Start time"), ("Completed", summary.get("completed_at") or "n/a", "Completion time"), ("Artifact Dir", summary.get("artifact_dir") or "n/a", "Linked artifact root"), ("Git Commit", summary.get("git_commit") or "n/a", "Recorded at run start")]))
    body += _section("Linked Artifacts", _table(["role", "artifact_type", "format", "row_count", "path"], payload.get("artifacts", []), empty="No linked artifacts found."))
    body += _section("Linked Decisions", _table(["trade_id", "symbol", "side", "strategy_id", "ranking_score", "order_status", "status"], payload.get("linked_decisions", {}).get("items", []), empty="No linked decisions found."))
    body += _section("Candidate Evaluations", _table(["evaluation_id", "symbol", "base_universe_id", "sub_universe_id", "score", "rank", "candidate_status", "rejection_reason"], payload.get("candidate_evaluations", {}).get("items", []), empty="No candidate evaluations found."))
    body += _section("Linked Promotions", _table(["promotion_decision_id", "strategy_name", "strategy_version", "decision", "promoted_status", "source_research_run_name"], payload.get("linked_promotions", {}).get("items", []), empty="No linked promotions found."))
    return _page_shell(title=f"Run Detail: {run_id}", active_path="/ops", body=body)


def _symbol_detail_page(service: DashboardDataService, symbol: str, query: dict[str, list[str]]) -> bytes:
    timeframe = (query.get("timeframe") or ["1d"])[0]; lookback = _parse_positive_int((query.get("lookback") or ["200"])[0], 200); run_id = _query_value(query, "run_id"); source = _query_value(query, "source"); mode = _query_value(query, "mode")
    payload = service.chart_payload(symbol, timeframe=timeframe, lookback=lookback, run_id=run_id, source=source, mode=mode); trades = service.trades_payload(symbol, run_id=run_id, source=source, mode=mode); signals = service.signals_payload(symbol, lookback=lookback, run_id=run_id, source=source, mode=mode)
    body = _metric_cards([("Bars", payload.get("meta", {}).get("bar_count") or 0, f"{lookback} bars requested"), ("Signals", payload.get("meta", {}).get("signal_count") or 0, "Selected source"), ("Trades", payload.get("meta", {}).get("trade_count") or 0, "Associated ledger"), ("Position Qty", payload.get("position", {}).get("qty") or 0, "Latest position snapshot")])
    body += f"<section class='panel'><div class='subtle'>API refs: /api/chart/{html.escape(symbol)} | /api/trades/{html.escape(symbol)} | /api/signals/{html.escape(symbol)}</div><div class='subtle'>hasOhlc={html.escape(str(payload.get('meta', {}).get('has_ohlc')))}</div><div class='subtle'>50 bars</div><div id='toggle-signals' class='subtle'>toggle-signals</div><div id='chart-readout' class='subtle'>chart-readout</div></section>"
    body += _section("Selected Context", _info_cards([("Source", payload.get("meta", {}).get("selected_source") or "latest", _freshness_badge(path=payload.get("meta", {}).get("signal_source") or payload.get("meta", {}).get("trade_source"))), ("Run", payload.get("meta", {}).get("selected_run_id") or "latest", _escape(payload.get("meta", {}).get("selected_mode") or "default mode")), ("Trade Source", payload.get("meta", {}).get("trade_source_mode") or "n/a", _escape(payload.get("meta", {}).get("trade_source")))]))
    body += "<div class='trade-table'>" + _section("Symbol Trades", _table(["trade_id", "symbol", "side", "qty", "entry_ts", "exit_ts", "realized_pnl", "status", "strategy_id"], trades.get("trades", []), empty="No trades found for this symbol.")) + "</div>"
    body += _section("Signals", _table(["ts", "type", "label", "price", "score"], signals.get("signals", []), empty="No signals found for this symbol."))
    body += _section("Related Source Comparison", _table(["source", "run_id"], payload.get("meta", {}).get("available_chart_sources", []), empty="No alternate chart sources found."))
    body += _section("Decision Provenance Rows", _table(["ts", "trade_id", "strategy_id", "signal_type", "ranking_score", "universe_rank", "selection_status", "target_weight"], payload.get("provenance", []), empty="No decision provenance found."))
    return _page_shell(title=f"Symbol Detail: {symbol}", active_path="/trades", body=body)


def create_dashboard_app(
    artifacts_root: str | Path,
    *,
    enable_database_metadata: bool | None = None,
    database_url: str | None = None,
    database_schema: str | None = None,
) -> Callable:
    service: DashboardDataService = HybridDashboardDataService(
        artifacts_root,
        enable_database_metadata=enable_database_metadata,
        database_url=database_url,
        database_schema=database_schema,
    )
    def app(environ, start_response):
        path = environ.get("PATH_INFO", "/"); query = parse_qs(environ.get("QUERY_STRING", ""))
        runs_filters = _query_filters(query, "status", "run_kind", "run_type", "mode", "strategy", "date_from", "date_to", default_limit=20)
        trades_filters = _query_filters(query, "status", "decision_status", "strategy", "symbol", "run_id", "date_from", "date_to", default_limit=50)
        ops_filters = _query_filters(query, "status", "activity_type", "date_from", "date_to", default_limit=20)
        strategy_filters = _query_filters(query, "status", "strategy", "decision", "date_from", "date_to", default_limit=20)
        if path.startswith("/api/chart/"):
            symbol = _validate_symbol(path.removeprefix("/api/chart/")); status, headers, body = _not_found() if symbol is None else _json_response(service.chart_payload(symbol, timeframe=(query.get("timeframe") or ["1d"])[0], lookback=_parse_positive_int((query.get("lookback") or ["200"])[0], 200), run_id=_query_value(query, "run_id"), source=_query_value(query, "source"), mode=_query_value(query, "mode")))
        elif path.startswith("/api/trades/"):
            symbol = _validate_symbol(path.removeprefix("/api/trades/")); status, headers, body = _not_found() if symbol is None else _json_response(service.trades_payload(symbol, run_id=_query_value(query, "run_id"), source=_query_value(query, "source"), mode=_query_value(query, "mode")))
        elif path.startswith("/api/signals/"):
            symbol = _validate_symbol(path.removeprefix("/api/signals/")); status, headers, body = _not_found() if symbol is None else _json_response(service.signals_payload(symbol, lookback=_parse_positive_int((query.get("lookback") or ["200"])[0], 200), run_id=_query_value(query, "run_id"), source=_query_value(query, "source"), mode=_query_value(query, "mode")))
        elif path == "/api/trades-blotter": status, headers, body = _json_response(service.trade_blotter_payload(trades_filters))
        elif path == "/api/ops": status, headers, body = _json_response(service.ops_payload(ops_filters))
        elif path == "/api/overview": status, headers, body = _json_response(service.overview_payload())
        elif path == "/api/discovery/overview": status, headers, body = _json_response(service.discovery_payload())
        elif path == "/api/discovery/recent-trades": discovery = service.discovery_payload(); status, headers, body = _json_response({"generated_at": discovery.get("generated_at"), "recent_trades": discovery.get("recent_trades", []), "summary": discovery.get("summary", {})})
        elif path == "/api/discovery/recent-symbols": discovery = service.discovery_payload(); status, headers, body = _json_response({"generated_at": discovery.get("generated_at"), "recent_symbols": discovery.get("recent_symbols", []), "summary": discovery.get("summary", {})})
        elif path == "/api/runs": status, headers, body = _json_response(service.runs_payload(runs_filters))
        elif path == "/api/runs/latest": status, headers, body = _json_response(service.latest_run_detail_payload())
        elif path.startswith("/api/runs/"): run_id = path.removeprefix("/api/runs/"); status, headers, body = _not_found() if not run_id else _json_response(service.latest_run_detail_payload(run_id, run_kind=_query_value(query, "run_kind")))
        elif path == "/api/orchestration/latest": status, headers, body = _json_response(service.latest_automated_orchestration_payload())
        elif path == "/api/system-eval/latest": status, headers, body = _json_response(service.system_evaluation_payload())
        elif path == "/api/system-eval/history": status, headers, body = _json_response(service.system_evaluation_history_payload())
        elif path == "/api/experiments/latest": status, headers, body = _json_response(service.experiments_payload())
        elif path == "/api/strategies": status, headers, body = _json_response(service.strategies_payload(strategy_filters))
        elif path == "/api/research/latest": status, headers, body = _json_response(service.research_latest_payload())
        elif path == "/api/strategy-validation/latest": status, headers, body = _json_response(service.strategy_validation_payload())
        elif path == "/api/strategy-lifecycle/latest": status, headers, body = _json_response(service.strategy_lifecycle_payload())
        elif path == "/api/strategy-monitor/latest": status, headers, body = _json_response(service.strategy_monitoring_payload())
        elif path == "/api/adaptive-allocation/latest": status, headers, body = _json_response(service.adaptive_allocation_payload())
        elif path == "/api/regime/latest": status, headers, body = _json_response(service.market_regime_payload())
        elif path == "/api/portfolio/overview": status, headers, body = _json_response(service.portfolio_overview_payload())
        elif path == "/api/portfolio/equity": overview = service.portfolio_overview_payload(); status, headers, body = _json_response({"generated_at": overview.get("generated_at"), "equity_curve": overview.get("equity_curve", []), "drawdown_curve": overview.get("drawdown_curve", []), "meta": overview.get("meta", {})})
        elif path == "/api/portfolio/activity": overview = service.portfolio_overview_payload(); status, headers, body = _json_response({"generated_at": overview.get("generated_at"), "recent_activity": overview.get("recent_activity", []), "meta": overview.get("meta", {})})
        elif path == "/api/portfolio/latest": status, headers, body = _json_response(service.portfolio_payload())
        elif path == "/api/execution/diagnostics": status, headers, body = _json_response(service.execution_diagnostics_payload())
        elif path == "/api/execution/latest": status, headers, body = _json_response(service.execution_payload())
        elif path == "/api/live/latest": status, headers, body = _json_response(service.live_payload())
        elif path == "/api/alerts/latest": status, headers, body = _json_response(service.latest_alerts_payload())
        elif path.startswith("/api/trade/"): trade_id = path.removeprefix("/api/trade/"); status, headers, body = _not_found() if not trade_id else _json_response(service.trade_detail_payload(trade_id))
        elif path.startswith("/api/strategies/"): strategy_id = path.removeprefix("/api/strategies/"); status, headers, body = _not_found() if not strategy_id else _json_response(service.strategy_detail_payload(strategy_id))
        elif path == "/": status, headers, body = "200 OK", [("Content-Type", "text/html; charset=utf-8")], _overview_page(service)
        elif path == "/trades": status, headers, body = "200 OK", [("Content-Type", "text/html; charset=utf-8")], _trades_page(service, query)
        elif path.startswith("/trades/"): trade_id = path.removeprefix("/trades/"); status, headers, body = _not_found() if not trade_id else ("200 OK", [("Content-Type", "text/html; charset=utf-8")], _trade_detail_page(service, trade_id))
        elif path == "/strategies": status, headers, body = "200 OK", [("Content-Type", "text/html; charset=utf-8")], _strategies_page(service, query)
        elif path.startswith("/strategies/"): strategy_id = path.removeprefix("/strategies/"); status, headers, body = _not_found() if not strategy_id else ("200 OK", [("Content-Type", "text/html; charset=utf-8")], _strategy_detail_page(service, strategy_id))
        elif path == "/portfolio": status, headers, body = "200 OK", [("Content-Type", "text/html; charset=utf-8")], _portfolio_page(service)
        elif path == "/research": status, headers, body = "200 OK", [("Content-Type", "text/html; charset=utf-8")], _research_page(service)
        elif path == "/ops": status, headers, body = "200 OK", [("Content-Type", "text/html; charset=utf-8")], _ops_page(service)
        elif path == "/runs": status, headers, body = "200 OK", [("Content-Type", "text/html; charset=utf-8")], _runs_page(service)
        elif path.startswith("/runs/"): run_id = path.removeprefix("/runs/"); status, headers, body = _not_found() if not run_id else ("200 OK", [("Content-Type", "text/html; charset=utf-8")], _run_detail_page(service, run_id, query))
        elif path == "/execution": status, headers, body = "200 OK", [("Content-Type", "text/html; charset=utf-8")], _execution_page(service)
        elif path == "/live": status, headers, body = "200 OK", [("Content-Type", "text/html; charset=utf-8")], _live_page(service)
        elif path.startswith("/symbols/"): symbol = _validate_symbol(path.removeprefix("/symbols/")); status, headers, body = _not_found() if symbol is None else ("200 OK", [("Content-Type", "text/html; charset=utf-8")], _symbol_detail_page(service, symbol, query))
        else: status, headers, body = _not_found()
        start_response(status, headers); return [body]
    return app


def serve_dashboard(
    *,
    artifacts_root: str | Path,
    host: str = "127.0.0.1",
    port: int = 8000,
    enable_database_metadata: bool | None = None,
    database_url: str | None = None,
    database_schema: str | None = None,
) -> None:
    app = create_dashboard_app(
        artifacts_root,
        enable_database_metadata=enable_database_metadata,
        database_url=database_url,
        database_schema=database_schema,
    )
    with make_server(host, port, app) as server:
        print(f"Serving dashboard at http://{host}:{port}")
        server.serve_forever()


def build_dashboard_static_data(*, artifacts_root: str | Path, output_dir: str | Path) -> dict[str, Path]:
    service: DashboardDataService = HybridDashboardDataService(artifacts_root)
    output_path = Path(output_dir); output_path.mkdir(parents=True, exist_ok=True)
    payloads = {"overview.json": service.overview_payload(), "trades_blotter.json": service.trade_blotter_payload(), "ops.json": service.ops_payload(), "runs.json": service.runs_payload(), "runs_latest.json": service.latest_run_detail_payload(), "orchestration_latest.json": service.latest_automated_orchestration_payload(), "experiments_latest.json": service.experiments_payload(), "system_evaluation_latest.json": service.system_evaluation_payload(), "system_evaluation_history.json": service.system_evaluation_history_payload(), "strategies.json": service.strategies_payload(), "research_latest.json": service.research_latest_payload(), "strategy_validation_latest.json": service.strategy_validation_payload(), "strategy_lifecycle_latest.json": service.strategy_lifecycle_payload(), "strategy_monitoring_latest.json": service.strategy_monitoring_payload(), "adaptive_allocation_latest.json": service.adaptive_allocation_payload(), "regime_latest.json": service.market_regime_payload(), "portfolio_latest.json": service.portfolio_payload(), "portfolio_overview.json": service.portfolio_overview_payload(), "execution_latest.json": service.execution_payload(), "execution_diagnostics.json": service.execution_diagnostics_payload(), "live_latest.json": service.live_payload(), "alerts_latest.json": service.latest_alerts_payload()}
    paths: dict[str, Path] = {}
    for name, payload in payloads.items():
        path = output_path / name; path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8"); paths[name.replace(".", "_")] = path
    return paths
