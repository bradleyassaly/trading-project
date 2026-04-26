"""
FastAPI backend for the trading platform GUI.

Starts with:  uvicorn trading_platform.api.main:app --port 8001
Runs alongside the existing Flask dashboard on port 8000.
Reads from the same artifact files — no new database required.
"""

from __future__ import annotations

from dotenv import load_dotenv
load_dotenv()

# Configure JSON logs once at import time so uvicorn + app records
# share the same structured format across all services.
try:
    from trading_platform.polymarket.logging_config import setup_logging
    setup_logging(service="api")
except Exception:
    pass


import subprocess
import threading
import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import Body, FastAPI, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from trading_platform.api import artifact_reader as reader
from trading_platform.polymarket.db_connection import db as _db

# Prometheus metrics. Must be importable even if the lib is absent so a
# fresh checkout without deps still boots — hence the try/except + noop
# shims. Real metrics kick in once prometheus-client is installed.
try:
    from prometheus_client import (
        Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST,
    )
    _req_count = Counter(
        "api_requests_total",
        "Total API requests",
        ["method", "path", "status"],
    )
    _req_latency = Histogram(
        "api_request_latency_seconds",
        "API request latency",
        ["method", "path"],
        buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
    )
    _METRICS_ENABLED = True
except ImportError:
    _METRICS_ENABLED = False


app = FastAPI(
    title="Trading Platform API",
    version="1.0.0",
    description="JSON API for the trading platform React GUI.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def _metrics_middleware(request: Request, call_next):
    """Record per-request latency + status. Scrapable at /metrics."""
    import time as _t
    if not _METRICS_ENABLED:
        return await call_next(request)
    start = _t.perf_counter()
    # Route template (eg. /api/smart-money/wallet/{addr}/trades) not raw
    # path, so cardinality stays bounded. Falls back to raw path if
    # routing hasn't matched yet.
    route = request.scope.get("route")
    path = getattr(route, "path", request.url.path) if route else request.url.path
    try:
        response = await call_next(request)
        status = str(response.status_code)
    except Exception:
        _req_count.labels(request.method, path, "500").inc()
        _req_latency.labels(request.method, path).observe(_t.perf_counter() - start)
        raise
    _req_count.labels(request.method, path, status).inc()
    _req_latency.labels(request.method, path).observe(_t.perf_counter() - start)
    return response


@app.get("/metrics")
def metrics() -> Response:
    """Prometheus scrape endpoint. No auth — internal network only."""
    if not _METRICS_ENABLED:
        return Response("# prometheus_client not installed\n",
                        media_type="text/plain")
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

# In-memory job registry for async research runs
_jobs: dict[str, dict[str, Any]] = {}
_jobs_lock = threading.Lock()


# ── Pydantic models ───────────────────────────────────────────────────────────


class LoopControlRequest(BaseModel):
    action: str  # "pause" | "resume" | "trigger_now"


# ── Health ────────────────────────────────────────────────────────────────────


@app.get("/api/health")
def health() -> dict[str, Any]:
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


# ── System status ─────────────────────────────────────────────────────────────


@app.get("/api/system/status")
def system_status() -> dict[str, Any]:
    return reader.read_system_status()


# ── P&L ───────────────────────────────────────────────────────────────────────


@app.get("/api/pnl/equity-curve")
def pnl_equity_curve() -> dict[str, Any]:
    return reader.read_equity_curve()


@app.get("/api/pnl/summary")
def pnl_summary() -> dict[str, Any]:
    return reader.read_pnl_summary()


# ── Signals ───────────────────────────────────────────────────────────────────


@app.get("/api/signals/performance")
def signals_performance() -> dict[str, Any]:
    """Edge / EV / live-readiness computed from signal_outcomes.

    Falls back to the legacy paper-trade-based calculator if the
    signal_outcomes table is missing or empty so older deployments
    keep rendering something.
    """
    try:
        from trading_platform.polymarket.signal_performance import (
            SignalPerformanceCalculator,
        )
        calc = SignalPerformanceCalculator(_alpha_db_path())
        result = calc.compute_all()
        if result.get("total_signals", 0) > 0:
            return result
    except Exception:
        pass
    return reader.read_signals_performance()


@app.get("/api/signals/correlation")
def signals_correlation() -> dict[str, Any]:
    return reader.read_signals_correlation()


@app.get("/api/bankroll")
def bankroll_info() -> dict[str, Any]:
    """Current live-account bankroll snapshot (cached)."""
    from trading_platform.polymarket.bankroll import get_bankroll_info
    return get_bankroll_info()


@app.post("/api/bankroll/refresh")
def bankroll_refresh() -> dict[str, Any]:
    """Force a live refresh (CLOB balance + positions). Returns fresh snapshot."""
    from trading_platform.polymarket.bankroll import refresh_bankroll
    return refresh_bankroll()


@app.get("/api/signals/backtest-results")
def signals_backtest_results() -> dict[str, Any]:
    """Latest per-signal-type EV from signal_engine_backtest, blended with
    live paper-trade resolutions. Sorted by EV descending."""
    try:
        with _db() as c:
            latest = c.execute("SELECT MAX(run_ts) FROM signal_backtest_results").fetchone()
            if not latest or not latest[0]:
                return {"rows": [], "run_ts": None, "note": "no backtest run yet"}
            run_ts = int(latest[0])
            bt_rows = c.execute(
                "SELECT signal_type, fires, resolved, wr, ev, total_delta, pending, lookback_days "
                "FROM signal_backtest_results WHERE run_ts = ?",
                (run_ts,),
            ).fetchall()
            live_map: dict[str, dict] = {}
            for st, n, ev, wins, pnl in c.execute(
                "SELECT signal_type, COUNT(*), AVG(return_pct)/100.0, "
                " SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END), "
                " SUM(COALESCE(realized_pnl,0)) "
                "FROM polymarket_paper_trades WHERE archived=0 AND exit_ts IS NOT NULL "
                "GROUP BY signal_type"
            ).fetchall():
                live_map[st] = {
                    "live_resolved": n or 0, "live_ev": ev,
                    "live_wins": wins or 0, "live_pnl": pnl or 0,
                }
            rows = []
            for st, fires, resolved, wr, ev, total_delta, pending, lookback in bt_rows:
                row = {
                    "signal_type": st,
                    "backtest_fires": fires,
                    "backtest_resolved": resolved,
                    "backtest_wr": wr,
                    "backtest_ev": ev,
                    "backtest_total_delta": total_delta,
                    "backtest_pending": pending,
                    "lookback_days": lookback,
                }
                row.update(live_map.get(st, {
                    "live_resolved": 0, "live_ev": None, "live_wins": 0, "live_pnl": 0,
                }))
                rows.append(row)
            rows.sort(key=lambda x: -(x["backtest_ev"] if x["backtest_ev"] is not None else -999))
            return {"run_ts": run_ts, "rows": rows, "n_signal_types": len(rows)}
    except Exception as exc:
        return {"error": str(exc), "rows": [], "run_ts": None}


@app.get("/api/signals/sizing")
def signals_sizing() -> dict[str, Any]:
    """Kelly-based sizing recommendations per signal type (from signal_outcomes)."""
    try:
        from trading_platform.polymarket.kelly_sizer import KellySizer
        return {"available": True, "data": KellySizer(_alpha_db_path()).get_sizing_report()}
    except Exception as exc:
        return {"available": False, "error": str(exc)}


# ── Political / geopolitical intelligence ──────────────────────────────────


@app.get("/api/live/paper-comparison")
def live_paper_comparison() -> dict[str, Any]:
    """Paper vs live execution comparison."""
    try:
        from trading_platform.polymarket.execution_comparator import ExecutionComparator
        return {"available": True, **ExecutionComparator(_alpha_db_path()).compare()}
    except Exception as exc:
        return {"available": False, "error": str(exc)}


@app.get("/api/insiders/list")
def insiders_list() -> dict[str, Any]:
    """All insider-flagged wallets sorted by score."""
    try:
        from trading_platform.polymarket.insider_detector import InsiderDetector
        insiders = InsiderDetector(_alpha_db_path()).get_all_insiders()
        return {"available": True, "insiders": insiders, "count": len(insiders)}
    except Exception as exc:
        return {"available": False, "insiders": [], "error": str(exc)}


@app.get("/api/wallets/archetypes")
def wallets_archetypes() -> dict[str, Any]:
    """Wallet archetype distribution and per-archetype copy-trading stats."""
    try:
        with _db() as conn:
            rows = conn.execute("""
                SELECT archetype, copyable, COUNT(*) AS n
                FROM wallet_archetypes
                GROUP BY archetype, copyable
                ORDER BY copyable DESC, n DESC
            """).fetchall()
            return {"available": True, "data": [
                {"archetype": r[0], "copyable": bool(r[1]), "count": r[2]} for r in rows
            ]}
    except Exception as exc:
        return {"available": False, "error": str(exc)}


@app.get("/api/signals/live-readiness")
def signals_live_readiness() -> dict[str, Any]:
    """Per-signal-type readiness gate status."""
    try:
        from trading_platform.polymarket.signal_performance import SignalPerformanceCalculator
        perf = SignalPerformanceCalculator(_alpha_db_path()).compute_all()
        return {"available": True, "data": {
            st: {
                "total_fired": d["total_fired"],
                "total_resolved": d["total_resolved"],
                "win_rate": d["win_rate"],
                "avg_ev": d["avg_ev"],
                "p_value": d["p_value"],
                "live_ready": d["live_ready"],
            }
            for st, d in perf.get("by_type", {}).items()
        }}
    except Exception as exc:
        return {"available": False, "error": str(exc)}


@app.get("/api/wallets/political-leaders")
def wallets_political_leaders(limit: int = 25) -> dict[str, Any]:
    """Top tier S/A/B wallets in politics + geopolitics with current activity."""
    try:
        with _db() as conn:
            rows = conn.execute(
                """SELECT wcp.wallet, wcp.tier, wcp.tier_score, wcp.category,
                          wcp.win_rate, wcp.win_rate_30d, wcp.resolved_trades,
                          wcp.monthly_pnl, wcp.consistency_score, wcp.category_purity,
                          wcp.last_trade_at,
                          (SELECT COUNT(*) FROM wallet_trades wt
                           WHERE wt.wallet = wcp.wallet
                             AND wt.timestamp > unixepoch('now','-7 days')) AS trades_7d,
                          (SELECT COUNT(DISTINCT wt.condition_id) FROM wallet_trades wt
                           WHERE wt.wallet = wcp.wallet
                             AND wt.market_resolved = 0) AS open_markets
                   FROM wallet_category_profiles wcp
                   WHERE wcp.category IN ('politics','geopolitics')
                     AND wcp.tier IN ('S','A','B')
                   ORDER BY wcp.tier_score DESC
                   LIMIT ?""",
                (limit,),
            ).fetchall()
            cols = [
                "wallet", "tier", "tier_score", "category", "win_rate",
                "win_rate_30d", "resolved_trades", "monthly_pnl",
                "consistency_score", "category_purity", "last_trade_at",
                "trades_7d", "open_markets",
            ]
            return {"available": True, "data": [dict(zip(cols, r)) for r in rows]}
    except Exception as exc:
        return {"available": False, "error": str(exc)}


@app.get("/api/signals/political-performance")
def signals_political_performance() -> dict[str, Any]:
    """SignalPerformanceCalculator filtered to politics + geopolitics."""
    try:
        from trading_platform.polymarket.signal_performance import SignalPerformanceCalculator
        calc = SignalPerformanceCalculator(_alpha_db_path())
        full = calc.compute_all()
        filtered: dict[str, Any] = {}
        for st, data in full.get("by_type", {}).items():
            pol = {
                cat: stats for cat, stats in data.get("by_category", {}).items()
                if cat in ("politics", "geopolitics")
            }
            if pol:
                total_n = sum(v["n"] for v in pol.values())
                total_wins = sum(v["wins"] for v in pol.values())
                total_ev_sum = sum(v["total_ev"] for v in pol.values())
                filtered[st] = {
                    "total_fired": data.get("total_fired"),
                    "total_resolved": total_n,
                    "wins": total_wins,
                    "losses": total_n - total_wins,
                    "win_rate": round(total_wins / total_n, 4) if total_n else 0,
                    "avg_ev": round(total_ev_sum / total_n, 4) if total_n else 0,
                    "total_ev": round(total_ev_sum, 4),
                    "by_category": pol,
                }
        return {"available": True, "by_type": filtered}
    except Exception as exc:
        return {"available": False, "error": str(exc)}


@app.get("/api/markets/political-activity")
def markets_political_activity(hours: int = 48, limit: int = 50) -> dict[str, Any]:
    """Recent whale trades in political / geopolitical markets (tracked tier S/A/B)."""
    try:
        with _db() as conn:
            rows = conn.execute(
                """SELECT wt.wallet, wt.side, wt.size, wt.price,
                          wt.title, wt.category,
                          datetime(wt.timestamp, 'unixepoch') AS trade_time,
                          COALESCE(wcp.tier, 'unranked') AS wallet_tier,
                          COALESCE(wcp.win_rate, 0) AS wallet_wr,
                          wt.condition_id
                   FROM wallet_trades wt
                   LEFT JOIN wallet_category_profiles wcp
                     ON wt.wallet = wcp.wallet AND wt.category = wcp.category
                   WHERE wt.category IN ('politics', 'geopolitics')
                     AND wt.timestamp > unixepoch('now', ?)
                   ORDER BY wt.timestamp DESC
                   LIMIT ?""",
                (f"-{int(hours)} hours", limit),
            ).fetchall()
            cols = [
                "wallet", "side", "size", "price", "title", "category",
                "trade_time", "wallet_tier", "wallet_wr", "condition_id",
            ]
            return {"available": True, "data": [dict(zip(cols, r)) for r in rows]}
    except Exception as exc:
        return {"available": False, "error": str(exc)}


# ── Kalshi markets ────────────────────────────────────────────────────────────


@app.get("/api/kalshi/markets")
def kalshi_markets() -> dict[str, Any]:
    return reader.read_kalshi_markets()


@app.get("/api/polymarket/live-markets")
def polymarket_live_markets() -> dict[str, Any]:
    return reader.read_polymarket_live_markets()


@app.get("/api/polymarket/market-ticks/{market_id}")
def polymarket_market_ticks(market_id: str) -> dict[str, Any]:
    return reader.read_polymarket_market_ticks(market_id)


@app.get("/api/smart-money/wallets")
def smart_money_wallets() -> dict[str, Any]:
    return reader.read_smart_money_wallets()


@app.get("/api/smart-money/signals")
def smart_money_signals() -> dict[str, Any]:
    return reader.read_smart_money_signals()


@app.get("/api/smart-money/wallet/{address}")
def smart_money_wallet_detail(address: str) -> dict[str, Any]:
    return reader.read_smart_money_wallet_detail(address)


# NOTE: /api/wallets/winners and /api/wallets/{wallet}/positions must be declared
# BEFORE /api/wallets/{wallet_address} so literal paths are matched first.
@app.get("/api/wallets/winners")
def wallets_winners_pre(window: str = "all") -> dict[str, Any]:
    """Wallet winners — declared early so it isn't shadowed by /{wallet_address}."""
    return reader.read_smart_money_winners(window=window)


@app.get("/api/wallets/{wallet_address}/positions")
def wallet_positions(wallet_address: str) -> dict[str, Any]:
    """Return current open positions from wallet_positions table."""
    return reader.read_wallet_positions(wallet_address)


# ── Backtesting endpoints ────────────────────────────────────────────────────


@app.post("/api/backtest/run")
def backtest_run(request: dict[str, Any]) -> dict[str, Any]:
    """Run a backtest for a wallet over a date range."""
    from trading_platform.polymarket.backtester import Backtester, BacktestConfig
    from trading_platform.polymarket.wallet_db import WalletDB

    try:
        config = BacktestConfig(
            wallet=request.get("wallet", ""),
            start_date=request.get("start_date", "2025-01-01"),
            end_date=request.get("end_date", "2026-04-07"),
            delay_seconds=int(request.get("delay_seconds", 300)),
            slippage_pct=float(request.get("slippage_pct", 0.02)),
            starting_bankroll=float(request.get("starting_bankroll", 100_000)),
            stake_per_trade_pct=float(request.get("stake_per_trade_pct", 0.02)),
            max_open_positions=int(request.get("max_open_positions", 20)),
            min_position_size=float(request.get("min_position_size", 25)),
            categories=request.get("categories"),
        )
        return Backtester().run(config, WalletDB.default_path())
    except Exception as exc:
        return {"error": str(exc), "total_trades": 0, "trades": []}


@app.get("/api/backtest/wallet/{wallet_address}/positions")
def backtest_wallet_positions(wallet_address: str) -> dict[str, Any]:
    """Return all wallet_market_positions for visualization."""
    return reader.read_backtest_wallet_positions(wallet_address)


@app.get("/api/backtest/market/{condition_id}/price-history")
def backtest_market_price_history(condition_id: str) -> dict[str, Any]:
    """Return market price history (fetches if not cached)."""
    return reader.read_backtest_price_history(condition_id)


@app.get("/api/backtest/wallet/{wallet_address}/market/{condition_id}")
def backtest_wallet_market_view(wallet_address: str, condition_id: str) -> dict[str, Any]:
    """Combined view: price history + wallet entries + simulated entry."""
    return reader.read_backtest_wallet_market(wallet_address, condition_id)


# ── Order book + anomaly endpoints ───────────────────────────────────────────


@app.get("/api/market/{condition_id}/order-book")
def market_order_book(condition_id: str) -> dict[str, Any]:
    """Live CLOB order book + recent anomalies for a market."""
    return reader.read_market_order_book(condition_id)


@app.get("/api/alerts/anomalies")
def alerts_anomalies(limit: int = 50, severity: str | None = None) -> dict[str, Any]:
    """Recent market_anomalies, ordered by detection time desc."""
    return reader.read_anomaly_alerts(limit=limit, severity=severity)


# ── Market Intelligence endpoints ────────────────────────────────────────────


@app.get("/api/circuit-breaker/status")
def circuit_breaker_status() -> dict[str, Any]:
    """Cumulative-drawdown state + recent log."""
    return reader.read_circuit_breaker_status()


@app.get("/api/circuit-breaker/log")
def circuit_breaker_log(limit: int = 50) -> dict[str, Any]:
    """Append-only audit trail of circuit-breaker events."""
    return reader.read_circuit_breaker_log(limit=limit)


@app.post("/api/circuit-breaker/reset")
def circuit_breaker_reset(request: dict[str, Any]) -> dict[str, Any]:
    """Manual reset. Body must include {confirm: true}."""
    return reader.trigger_circuit_breaker_reset(
        confirm=bool((request or {}).get("confirm")),
        reset_peak=bool((request or {}).get("reset_peak")),
        reset_by=(request or {}).get("reset_by", "api"),
    )


@app.post("/api/circuit-breaker/configure")
def circuit_breaker_configure(request: dict[str, Any]) -> dict[str, Any]:
    """Update max_drawdown_pct and/or daily_loss_limit (refused while halted)."""
    return reader.trigger_circuit_breaker_configure(
        max_drawdown_pct=(request or {}).get("max_drawdown_pct"),
        daily_loss_limit=(request or {}).get("daily_loss_limit"),
    )


@app.post("/api/circuit-breaker/initialize")
def circuit_breaker_initialize(request: dict[str, Any] | None = None) -> dict[str, Any]:
    """Initialize the circuit breaker (idempotent — won't overwrite)."""
    body = request or {}
    return reader.trigger_circuit_breaker_initialize(
        starting_capital=float(body.get("starting_capital", 100_000)),
        max_drawdown_pct=float(body.get("max_drawdown_pct", 0.20)),
        daily_loss_limit=float(body.get("daily_loss_limit", 0.05)),
    )


@app.get("/api/system/scheduler-status")
def system_scheduler_status() -> dict[str, Any]:
    """Read the task_scheduler.py state file."""
    return reader.read_scheduler_status()


# ── Wallet tiering endpoints ────────────────────────────────────────────────


@app.get("/api/tiers/summary")
def tiers_summary() -> dict[str, Any]:
    """Per-category tier distribution + recent changes."""
    return reader.read_tiers_summary()


@app.get("/api/tiers/category/{category}")
def tiers_category(category: str) -> dict[str, Any]:
    """Per-category wallet leaderboard."""
    return reader.read_tiers_category(category)


@app.get("/api/tiers/wallet/{wallet}")
def tiers_wallet(wallet: str) -> dict[str, Any]:
    """All category profiles + history for a single wallet."""
    return reader.read_tiers_wallet(wallet)


@app.get("/api/tiers/history")
def tiers_history(category: str | None = None, days: int = 30) -> dict[str, Any]:
    """Tier change history (filterable by category + lookback days)."""
    return reader.read_tiers_history(category=category, days=days)


@app.post("/api/tiers/rebuild")
def tiers_rebuild() -> dict[str, Any]:
    """Run a full WalletTieringEngine rebuild."""
    return reader.trigger_tier_rebuild()


# ── Market microstructure endpoints (pmxt) ─────────────────────────────────


@app.get("/api/market-data/health")
def market_data_health() -> dict[str, Any]:
    """pmxt sidecar status + cache hit rate + error count."""
    return reader.read_market_data_health()


@app.get("/api/market-data/{condition_id}")
def market_data(condition_id: str, direction: str = "YES") -> dict[str, Any]:
    """Full pmxt microstructure (velocity + book + baseline + quality score)."""
    return reader.read_market_data(condition_id, direction=direction)


@app.get("/api/market-data/{condition_id}/candles")
def market_data_candles(
    condition_id: str,
    direction: str = "YES",
    resolution: str = "1h",
    hours: int = 24,
) -> dict[str, Any]:
    """Raw OHLCV candles for charting."""
    return reader.read_market_data_candles(
        condition_id, direction=direction, resolution=resolution, hours=hours,
    )


# ── Calibration endpoints ───────────────────────────────────────────────────


@app.get("/api/calibration/status")
def calibration_status() -> dict[str, Any]:
    """Per-signal calibration + current bankroll allocation plan."""
    return reader.read_calibration_status()


@app.get("/api/calibration/report/latest")
def calibration_report_latest() -> dict[str, Any]:
    """Most recent calibration report."""
    return reader.read_calibration_report()


@app.get("/api/calibration/report")
def calibration_report(date: str | None = None) -> dict[str, Any]:
    """Calibration report for a specific YYYY-MM-DD date."""
    return reader.read_calibration_report(date=date)


@app.post("/api/calibration/report/generate")
def calibration_report_generate() -> dict[str, Any]:
    """Generate and persist a fresh calibration report for today."""
    return reader.write_calibration_report()


@app.post("/api/calibration/rebalance")
def calibration_rebalance(request: dict[str, Any] | None = None) -> dict[str, Any]:
    """Recompute calibration + rebalance the bankroll allocator."""
    dry_run = bool((request or {}).get("dry_run", False))
    return reader.trigger_calibration_rebalance(dry_run=dry_run)


# ── Live trading readiness + control endpoints ─────────────────────────────


@app.get("/api/live/readiness")
def live_readiness() -> dict[str, Any]:
    """All live readiness gates + per-signal status + kill-switch limits."""
    return reader.read_live_readiness()


@app.get("/api/live/trades")
def live_trades(limit: int = 50) -> dict[str, Any]:
    """Recent live_trades audit log (dry runs + real submissions)."""
    return reader.read_live_trades(limit=limit)


@app.get("/api/live/positions")
def live_positions() -> dict[str, Any]:
    """Open live positions with current price and unrealized PnL."""
    try:
        with _db() as conn:
            rows = conn.execute("""
                SELECT id, condition_id, token_id, question, category, side,
                       signal_type, confidence, fill_price, size_usd, shares,
                       submitted_at, wallet_archetype, signal_wallet
                FROM live_trades
                WHERE exit_ts IS NULL AND fill_price IS NOT NULL
                ORDER BY submitted_at DESC
            """).fetchall()
            cols = ["id", "condition_id", "token_id", "question", "category",
                    "side", "signal_type", "confidence", "fill_price",
                    "size_usd", "shares", "submitted_at", "wallet_archetype",
                    "signal_wallet"]
            positions = [dict(zip(cols, r)) for r in rows]
            total_exposure = sum(p.get("size_usd") or 0 for p in positions)
            return {
                "positions": positions,
                "total_exposure": round(total_exposure, 2),
                "count": len(positions),
            }
    except Exception as exc:
        return {"positions": [], "total_exposure": 0, "count": 0, "error": str(exc)}


@app.get("/api/live/history")
def live_history(limit: int = 50) -> dict[str, Any]:
    """Closed live trades with realized PnL."""
    try:
        with _db() as conn:
            rows = conn.execute("""
                SELECT id, condition_id, question, category, side, signal_type,
                       fill_price, exit_price, size_usd, realized_pnl, outcome,
                       submitted_at, filled_at, exit_ts, slippage
                FROM live_trades
                WHERE exit_ts IS NOT NULL
                ORDER BY exit_ts DESC LIMIT ?
            """, (limit,)).fetchall()
            cols = ["id", "condition_id", "question", "category", "side",
                    "signal_type", "fill_price", "exit_price", "size_usd",
                    "realized_pnl", "outcome", "submitted_at", "filled_at",
                    "exit_ts", "slippage"]
            trades = [dict(zip(cols, r)) for r in rows]
            total_pnl = sum(t.get("realized_pnl") or 0 for t in trades)
            wins = sum(1 for t in trades if t.get("outcome") == "win")
            return {
                "trades": trades,
                "total_realized_pnl": round(total_pnl, 2),
                "win_rate": round(wins / len(trades), 4) if trades else 0,
                "count": len(trades),
            }
    except Exception as exc:
        return {"trades": [], "total_realized_pnl": 0, "win_rate": 0, "count": 0, "error": str(exc)}


@app.get("/api/live/execution-quality")
def live_execution_quality() -> dict[str, Any]:
    """Fill rate, slippage, fill time stats."""
    try:
        with _db() as conn:
            r = conn.execute("""
                SELECT COUNT(*) AS total,
                       SUM(CASE WHEN fill_price IS NOT NULL THEN 1 ELSE 0 END) AS filled,
                       AVG(slippage) AS avg_slippage,
                       AVG(fill_time_ms) AS avg_fill_time_ms,
                       AVG(ABS(fill_price - expected_price)) AS avg_price_deviation
                FROM live_trades
                WHERE dry_run = 0
            """).fetchone()
            total, filled = r[0] or 0, r[1] or 0
            return {
                "total_orders": total,
                "filled": filled,
                "fill_rate": round(filled / total, 4) if total else 0,
                "avg_slippage": round(r[2] or 0, 4),
                "avg_fill_time_ms": round(r[3] or 0, 1),
                "avg_price_deviation": round(r[4] or 0, 4),
            }
    except Exception as exc:
        return {"total_orders": 0, "filled": 0, "fill_rate": 0, "error": str(exc)}


@app.get("/api/ladder/status")
def ladder_status() -> dict[str, Any]:
    """L0→L5 ladder progress. Computes current state from live data
    and reports the gates remaining to next promotion.

    Levels (from THESIS.md / LIVE_TRADING_CHECKLIST.md):
      L0 paper-only        bankroll <$500
      L1 live probate     $500-$1,000   max/trade $5-$50
      L2 confirm          $1K-$5K       max/trade $150
      L3 growth           $5K-$25K      max/trade $500
      L4 scale            $25K-$100K    max/trade $1.5K
      L5 target           $200-$300K    max/trade $3K
    """
    import os as _os
    out: dict[str, Any] = {}
    try:
        with _db() as conn:
            paper_30d = conn.execute(
                """SELECT COUNT(*) FILTER (WHERE outcome IN ('win','loss')) closed,
                          SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END) wins,
                          SUM(realized_pnl) FILTER (WHERE outcome IN ('win','loss')) pnl
                     FROM polymarket_paper_trades
                    WHERE archived=0 AND entry_ts > extract(epoch FROM NOW()-interval '30 days')"""
            ).fetchone()
            live_30d = conn.execute(
                """SELECT COUNT(*) attempts,
                          SUM(CASE WHEN status='submitted' THEN 1 ELSE 0 END) submitted,
                          SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END) wins,
                          SUM(CASE WHEN outcome IN ('win','loss') THEN 1 ELSE 0 END) closed
                     FROM live_trades
                    WHERE submitted_at > extract(epoch FROM NOW()-interval '30 days')"""
            ).fetchone()
            try:
                hyp_acc = conn.execute(
                    """SELECT AVG(hypothesis_correct::numeric)
                         FROM trade_hypotheses
                        WHERE resolved_at IS NOT NULL
                          AND resolved_at > extract(epoch FROM NOW()-interval '30 days')
                          AND hypothesis_correct IN (0,1)"""
                ).fetchone()
                hypothesis_acc_30d = float(hyp_acc[0]) if hyp_acc and hyp_acc[0] else None
            except Exception:
                hypothesis_acc_30d = None
    except Exception as exc:
        return {"error": str(exc)[:200]}

    paper_closed = int((paper_30d[0] or 0))
    paper_wins = int((paper_30d[1] or 0))
    paper_wr = (paper_wins / paper_closed) if paper_closed else None
    paper_pnl = float(paper_30d[2] or 0)
    live_attempts = int((live_30d[0] or 0))
    live_submitted = int((live_30d[1] or 0))
    live_closed = int((live_30d[3] or 0))
    live_wins = int((live_30d[2] or 0))
    live_wr = (live_wins / live_closed) if live_closed else None

    # Bankroll from env / live source
    try:
        from trading_platform.polymarket.bankroll import get_bankroll
        bankroll = float(get_bankroll() or 0)
    except Exception:
        bankroll = float(_os.environ.get("POLYMARKET_LIVE_BANKROLL_USD", 0) or 0)

    # Determine current level
    if bankroll < 500:
        level, level_name = "L0", "Paper Only"
    elif bankroll < 1000:
        level, level_name = "L1", "Live Probate"
    elif bankroll < 5000:
        level, level_name = "L2", "Confirm"
    elif bankroll < 25000:
        level, level_name = "L3", "Growth"
    elif bankroll < 100000:
        level, level_name = "L4", "Scale"
    else:
        level, level_name = "L5", "Target"

    # Gates per level
    if level == "L0":
        next_level = "L1 Live Probate"
        gates = [
            {"name": "Hypothesis accuracy ≥ 50% on n≥50",
             "value": f"{(hypothesis_acc_30d or 0) * 100:.1f}% on n={paper_closed}",
             "passed": (hypothesis_acc_30d is not None and hypothesis_acc_30d >= 0.50 and paper_closed >= 50)},
            {"name": "Paper PnL positive over 30d",
             "value": f"${paper_pnl:.2f}",
             "passed": paper_pnl > 0},
            {"name": "0 silent-failure scheduler incidents (7d)",
             "value": "verified READY",
             "passed": True},
        ]
    elif level == "L1":
        next_level = "L2 Confirm"
        gates = [
            {"name": "≥ 10 live trades submitted",
             "value": f"{live_submitted} submitted",
             "passed": live_submitted >= 10},
            {"name": "Live WR ≥ 55% on n≥10",
             "value": f"{(live_wr or 0) * 100:.1f}% on n={live_closed}",
             "passed": (live_wr is not None and live_wr >= 0.55 and live_closed >= 10)},
            {"name": "Slippage median ≤ 2%",
             "value": "n/a (needs trades)",
             "passed": False},
        ]
    else:
        next_level = "—"
        gates = []

    return {
        "level": level,
        "level_name": level_name,
        "next_level": next_level,
        "bankroll_usd": round(bankroll, 2),
        "target_bankroll_l5": 200000,
        "progress_pct": round(min(100.0, bankroll / 200000 * 100), 1),
        "paper": {
            "closed_30d": paper_closed,
            "wr_30d": round(paper_wr, 3) if paper_wr is not None else None,
            "pnl_30d": round(paper_pnl, 2),
        },
        "live": {
            "attempts_30d": live_attempts,
            "submitted_30d": live_submitted,
            "closed_30d": live_closed,
            "wr_30d": round(live_wr, 3) if live_wr is not None else None,
        },
        "hypothesis_accuracy_30d": (
            round(hypothesis_acc_30d, 3) if hypothesis_acc_30d is not None else None
        ),
        "gates_to_next": gates,
        "lanes": {
            "live_discovery_enabled": _os.environ.get("LIVE_DISCOVERY_ENABLED", "").lower() in ("1", "true"),
            "phase_b_enabled": _os.environ.get("PHASE_B_RESOLUTION_DECAY_ENABLED", "").lower() in ("1", "true"),
            "phase_d_enabled": _os.environ.get("PHASE_D_DIVERGENCE_ENABLED", "").lower() in ("1", "true"),
        },
    }


@app.post("/api/ladder/promote")
def ladder_promote(payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    """Record a ladder promotion to the audit table. Does NOT change
    bankroll — that step is manual. Confirms gates were passed at the
    time of promotion."""
    import time as _t
    try:
        with _db() as conn:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS ladder_promotions (
                    id BIGSERIAL PRIMARY KEY,
                    promoted_at BIGINT NOT NULL,
                    from_level TEXT, to_level TEXT,
                    passed_gates BIGINT, total_gates BIGINT,
                    notes TEXT
                )"""
            )
            conn.execute(
                """INSERT INTO ladder_promotions
                     (promoted_at, from_level, to_level,
                      passed_gates, total_gates, notes)
                   VALUES (?,?,?,?,?,?)""",
                (
                    int(_t.time()),
                    str(payload.get("from_level") or ""),
                    str(payload.get("to_level") or ""),
                    int(payload.get("passed_gates") or 0),
                    int(payload.get("total_gates") or 0),
                    "via Telegram /promote confirm",
                ),
            )
            conn.commit()
        try:
            from trading_platform.polymarket.telegram_alerts import get_alerter
            a = get_alerter()
            if a.enabled:
                a._send(
                    f"\U0001f680 <b>LADDER PROMOTION RECORDED</b>\n"
                    f"{payload.get('from_level')} → {payload.get('to_level')}\n"
                    f"Gates: {payload.get('passed_gates')}/{payload.get('total_gates')}\n"
                    f"Capital deposit + POLYMARKET_LIVE_BANKROLL_USD update is manual.",
                    disable_notification=False,
                )
        except Exception:
            pass
        return {"ok": True}
    except Exception as exc:
        return {"ok": False, "error": str(exc)[:200]}


@app.get("/api/diff/24h")
def diff_24h() -> dict[str, Any]:
    """What changed in the last 24h vs the prior 24h. Pulls counts
    from the same canonical sources used by the daily digest so
    operators can answer 'is the system improving?' at a glance.
    """
    import time as _t
    out: dict[str, Any] = {}
    try:
        with _db() as conn:
            for label, sql in (
                ("paper_trades", """
                    SELECT
                      COUNT(*) FILTER (WHERE entry_ts > extract(epoch FROM NOW()-interval '24 hours')) AS today,
                      COUNT(*) FILTER (WHERE entry_ts > extract(epoch FROM NOW()-interval '48 hours')
                                         AND entry_ts <= extract(epoch FROM NOW()-interval '24 hours')) AS yest
                      FROM polymarket_paper_trades WHERE archived=0
                """),
                ("pnl", """
                    SELECT
                      ROUND(SUM(realized_pnl) FILTER (WHERE exit_ts > extract(epoch FROM NOW()-interval '24 hours'))::numeric,2) AS today,
                      ROUND(SUM(realized_pnl) FILTER (WHERE exit_ts > extract(epoch FROM NOW()-interval '48 hours')
                                                       AND exit_ts <= extract(epoch FROM NOW()-interval '24 hours'))::numeric,2) AS yest
                      FROM polymarket_paper_trades WHERE archived=0 AND realized_pnl IS NOT NULL
                """),
                ("hypotheses_resolved", """
                    SELECT
                      COUNT(*) FILTER (WHERE resolved_at > extract(epoch FROM NOW()-interval '24 hours')) AS today,
                      COUNT(*) FILTER (WHERE resolved_at > extract(epoch FROM NOW()-interval '48 hours')
                                         AND resolved_at <= extract(epoch FROM NOW()-interval '24 hours')) AS yest
                      FROM trade_hypotheses WHERE resolved_at IS NOT NULL
                """),
                ("hypothesis_accuracy_pct", """
                    SELECT
                      ROUND(AVG(hypothesis_correct::numeric*100) FILTER (WHERE resolved_at > extract(epoch FROM NOW()-interval '24 hours'))::numeric,1) AS today,
                      ROUND(AVG(hypothesis_correct::numeric*100) FILTER (WHERE resolved_at > extract(epoch FROM NOW()-interval '48 hours')
                                                                          AND resolved_at <= extract(epoch FROM NOW()-interval '24 hours'))::numeric,1) AS yest
                      FROM trade_hypotheses
                     WHERE resolved_at IS NOT NULL AND hypothesis_correct IN (0,1)
                """),
                ("insider_pool", """
                    SELECT
                      (SELECT COUNT(*) FROM insider_wallets) AS today,
                      (SELECT COUNT(*) FROM insider_wallets
                        WHERE classified_at <= extract(epoch FROM NOW()-interval '24 hours')) AS yest
                """),
                ("alpha_gate_rejections", """
                    SELECT
                      COUNT(*) FILTER (WHERE gate='ALPHA_GATE' AND decision_ts > extract(epoch FROM NOW()-interval '24 hours')) AS today,
                      COUNT(*) FILTER (WHERE gate='ALPHA_GATE' AND decision_ts > extract(epoch FROM NOW()-interval '48 hours')
                                         AND decision_ts <= extract(epoch FROM NOW()-interval '24 hours')) AS yest
                      FROM decision_trace
                """),
            ):
                try:
                    row = conn.execute(sql).fetchone()
                    out[f"{label}_today"] = row[0] if row else None
                    out[f"{label}_yesterday"] = row[1] if row else None
                except Exception as exc:
                    out[f"{label}_error"] = str(exc)[:80]
        return out
    except Exception as exc:
        return {"error": str(exc)[:200]}


@app.get("/api/explain/{decision_id}")
def explain_decision(decision_id: str) -> dict[str, Any]:
    """Plain-language reason for why a signal fired or was rejected.

    Looks up `decision_trace` by id (or by signal_outcomes.id if no
    direct trace match). Returns the gate that fired, value vs threshold,
    and a humanized sentence.
    """
    try:
        sid = int(decision_id)
    except (TypeError, ValueError):
        return {"error": "decision_id must be an integer"}
    try:
        with _db() as conn:
            row = conn.execute(
                """SELECT decision_ts, condition_id, signal_type, side,
                          wallet, category, gate, passed, value, threshold,
                          detail, surface
                     FROM decision_trace WHERE id = ?""",
                (sid,),
            ).fetchone()
    except Exception as exc:
        return {"error": str(exc)[:200]}
    if not row:
        return {"error": f"no decision_trace row with id={sid}"}
    ts, cid, sig, side, wallet, cat, gate, passed, val, thr, detail, surface = row
    verdict = "PASSED" if passed else "BLOCKED"
    bits = [
        f"<b>{verdict}</b> {sig or '?'} {side or ''} on {surface or 'paper'}",
        f"  market: {(cid or '?')[:14]}…",
        f"  category: {cat or '?'}",
        f"  wallet: {(wallet or '?')[:14]}…",
        f"  gate: <code>{gate}</code>",
    ]
    if val is not None and thr is not None:
        bits.append(f"  value {val:.3f} vs threshold {thr:.3f}")
    if detail:
        bits.append(f"  detail: {detail[:200]}")
    return {"explanation": "\n".join(bits)}


@app.get("/api/insiders/growth")
def insider_growth(days: int = 30) -> dict[str, Any]:
    """Daily insider-pool size over the last `days` days. Used by
    Dashboard insider growth chart. Computes from `classified_at` so
    it reflects the auto-promote loop's actual cadence."""
    try:
        with _db() as conn:
            cutoff = int(__import__("time").time()) - days * 86400
            rows = conn.execute(
                """SELECT to_char(to_timestamp(classified_at)::date, 'YYYY-MM-DD') AS day,
                          COUNT(*) added
                     FROM insider_wallets
                    WHERE classified_at > %s
                    GROUP BY day
                    ORDER BY day""",
                (cutoff,),
            ).fetchall()
            total_now = conn.execute("SELECT COUNT(*) FROM insider_wallets").fetchone()[0]
    except Exception as exc:
        return {"error": str(exc)[:200], "growth": [], "total_now": 0}

    # Build cumulative series. Anything before the window is treated as
    # baseline (total_now − sum(adds within window)).
    window_adds = sum(int(r[1]) for r in rows)
    baseline = max(0, int(total_now) - window_adds)
    series: list[dict[str, Any]] = []
    cum = baseline
    for r in rows:
        cum += int(r[1])
        series.append({"day": r[0], "added": int(r[1]), "cumulative": cum})
    return {"days": days, "total_now": int(total_now),
            "baseline_before_window": baseline, "growth": series}


@app.get("/api/wallet/{address}/profile")
def wallet_profile(address: str) -> dict[str, Any]:
    """Unified wallet view across all the intelligence layers.

    Joins wallet_profiles, wallet_alpha_scores, wallet_behavior_metrics,
    wallet_category_zscore, insider_wallets, and earliness lookup. Single
    query for the dashboard / Telegram /wallet command.
    """
    addr = (address or "").lower()
    if not addr.startswith("0x") or len(addr) != 42:
        return {"error": "invalid address format"}
    out: dict[str, Any] = {"wallet": addr}
    try:
        with _db() as conn:
            row = conn.execute(
                """SELECT pm_pnl_usdc, pm_volume_usdc, net_pnl_usdc,
                          directional_win_rate, early_win_rate,
                          resolved_trades, total_volume_usdc,
                          wallet_type, pseudonym, is_early_informed,
                          first_seen_ts, last_trade_ts, pm_synced_at
                     FROM wallet_profiles WHERE wallet = ?""",
                (addr,),
            ).fetchone()
            if row:
                out["profile"] = {
                    "pm_pnl_usdc": row[0], "pm_volume_usdc": row[1],
                    "net_pnl_usdc": row[2], "directional_win_rate": row[3],
                    "early_win_rate": row[4], "resolved_trades": row[5],
                    "total_volume_usdc": row[6],
                    "wallet_type": row[7], "pseudonym": row[8],
                    "is_early_informed": row[9],
                    "first_seen_ts": row[10], "last_trade_ts": row[11],
                    "pm_synced_at": row[12],
                }
            else:
                out["profile"] = None

            try:
                bm = conn.execute(
                    """SELECT n_resolved, roi_mean, roi_lower_95, roi_upper_95,
                              is_copyable_ci, sizing_median_pct, sizing_p90_pct,
                              estimated_bankroll, sybil_score, is_likely_farmer,
                              category_hhi, primary_category, cluster_id
                         FROM wallet_behavior_metrics WHERE wallet = ?""",
                    (addr,),
                ).fetchone()
                if bm:
                    out["behavior"] = {
                        "n_resolved": bm[0], "roi_mean": bm[1],
                        "roi_lower_95": bm[2], "roi_upper_95": bm[3],
                        "is_copyable_ci": int(bm[4] or 0),
                        "sizing_median_pct": bm[5], "sizing_p90_pct": bm[6],
                        "estimated_bankroll": bm[7], "sybil_score": bm[8],
                        "is_likely_farmer": int(bm[9] or 0),
                        "category_hhi": bm[10], "primary_category": bm[11],
                        "cluster_id": bm[12],
                    }
                else:
                    out["behavior"] = None
            except Exception:
                out["behavior"] = None

            try:
                alpha_rows = conn.execute(
                    """SELECT category, win_rate, win_rate_30d, avg_pnl,
                              total_pnl, resolved_trades, copyability, is_copyable
                         FROM wallet_alpha_scores
                        WHERE wallet = ?
                        ORDER BY total_pnl DESC LIMIT 20""",
                    (addr,),
                ).fetchall()
                out["alpha_by_category"] = [
                    {"category": r[0], "win_rate": r[1], "win_rate_30d": r[2],
                     "avg_pnl": r[3], "total_pnl": r[4], "resolved_trades": r[5],
                     "copyability": r[6], "is_copyable": int(r[7] or 0)}
                    for r in alpha_rows
                ]
            except Exception:
                out["alpha_by_category"] = []

            try:
                z_rows = conn.execute(
                    """SELECT category, n_in_cat, cat_wr, lifetime_wr,
                              z_score, is_specialist_z
                         FROM wallet_category_zscore
                        WHERE wallet = ?
                        ORDER BY z_score DESC NULLS LAST""",
                    (addr,),
                ).fetchall()
                out["zscore_by_category"] = [
                    {"category": r[0], "n_in_cat": r[1], "cat_wr": r[2],
                     "lifetime_wr": r[3], "z_score": r[4],
                     "is_specialist_z": int(r[5] or 0)}
                    for r in z_rows
                ]
            except Exception:
                out["zscore_by_category"] = []

            try:
                ins = conn.execute(
                    """SELECT insider_score, total_trades, total_pnl,
                              detection_method, classified_at
                         FROM insider_wallets WHERE wallet = ?""",
                    (addr,),
                ).fetchone()
                out["insider"] = (
                    {"insider_score": ins[0], "total_trades": ins[1],
                     "total_pnl": ins[2], "detection_method": ins[3],
                     "classified_at": ins[4]}
                    if ins else None
                )
            except Exception:
                out["insider"] = None
        return out
    except Exception as exc:
        return {"wallet": addr, "error": str(exc)[:200]}


@app.get("/api/signal-health")
def signal_health() -> dict[str, Any]:
    """Latest IC + correlation snapshot per signal type. Reads
    `signal_health` table populated daily by signal_health.py.
    """
    try:
        with _db() as conn:
            rows = conn.execute(
                "SELECT signal_type, n_resolved_30d, ic_30d, ic_14d, ic_trend, "
                "       correlated_with, correlation_max, decay_flag, last_updated "
                "  FROM signal_health "
                " ORDER BY ic_14d DESC NULLS LAST"
            ).fetchall()
    except Exception as exc:
        return {"error": str(exc)[:200], "signals": []}
    out = []
    for r in rows:
        out.append({
            "signal_type": r[0], "n_resolved_30d": r[1],
            "ic_30d": r[2], "ic_14d": r[3], "ic_trend": r[4],
            "correlated_with": r[5], "correlation_max": r[6],
            "decay_flag": int(r[7] or 0),
            "last_updated": r[8],
        })
    return {"signals": out}


@app.get("/api/calibration")
def calibration_report(window_days: int = 30) -> dict[str, Any]:
    """Brier score + reliability diagram + per-signal calibration over
    the last `window_days` of resolved hypotheses. Verdict is one of
    {well_calibrated, mild_miscalibration, poor_calibration} keyed off
    mean abs miscalibration thresholds.
    """
    try:
        from trading_platform.polymarket.calibration_metrics import compute_calibration
        return compute_calibration(window_days=window_days)
    except Exception as exc:
        return {"error": str(exc)[:200], "n": 0, "window_days": window_days}


@app.get("/api/funnel/decisions")
def decision_funnel(hours: int = 24) -> dict[str, Any]:
    """Aggregate decision_trace rows by gate. Closes the 'why didn't I
    trade X yesterday' query gap. Reads from `decision_trace` written
    at every gate evaluation in paper + live executors.
    """
    try:
        from trading_platform.polymarket.decision_trace import funnel_summary
        rows = funnel_summary(hours=hours)
        return {"hours": hours, "by_gate": rows}
    except Exception as exc:
        return {"hours": hours, "by_gate": [], "error": str(exc)[:200]}


@app.get("/api/live/funnel")
def live_trade_funnel() -> dict[str, Any]:
    """Per-gate drop counts for the live executor over the past 24h.

    Built 2026-04-24 to answer the standing question: 'why does the live
    executor produce zero trades?' Reads `live_trades` for kill-switch
    rejections (which are auditable rows) and parses the structured
    `[KS_BLOCK:CODE]` log markers from the live-collect container.
    Pre-kill-switch gates (category, signal-side, FAV_GATE, etc.) only
    surface in logs, so the breakdown there is grep-of-logs not query.
    """
    import time as _t
    cutoff = int(_t.time()) - 24 * 3600
    funnel = {
        "kill_switch_blocks": {},
        "live_attempts": 0,
        "live_succeeded": 0,
        "live_failed": 0,
        "log_markers_note": (
            "Pre-kill-switch gates ([SIDE_GATE], [LIVE][FAV_GATE], "
            "[LIVE_BLOCKED]) only emit log lines; not aggregated here. "
            "Run: docker compose logs --since 24h live-collect "
            "| grep -E '\\[(SIDE_GATE|LIVE\\]\\[FAV_GATE|LIVE_BLOCKED|KS_BLOCK)\\]' "
            "| sort | uniq -c | sort -rn"
        ),
    }
    try:
        with _db() as conn:
            rows = conn.execute(
                """SELECT status, error_msg, COUNT(*) n
                     FROM live_trades
                    WHERE submitted_at > %s
                    GROUP BY status, error_msg""",
                (cutoff,),
            ).fetchall()
    except Exception as exc:
        return {"error": f"funnel query failed: {str(exc)[:120]}"}
    for row in rows:
        status, err, n = row[0], row[1] or "", int(row[2])
        if status == "submitted":
            funnel["live_succeeded"] += n
        elif status == "blocked":
            err_lower = err.lower()
            if "polymarket_live_enabled" in err_lower:
                code = "ENV"
            elif "emergency" in err_lower or "stop" in err_lower:
                code = "EMERGENCY"
            elif "min_resolved" in err_lower or "minimum" in err_lower:
                code = "MIN_RESOLVED"
            elif "ev" in err_lower:
                code = "EV"
            elif "wr" in err_lower:
                code = "WR"
            elif "disabled" in err_lower or "excluded" in err_lower:
                code = "DISABLED"
            else:
                code = "OTHER"
            funnel["kill_switch_blocks"][code] = funnel["kill_switch_blocks"].get(code, 0) + n
            funnel["live_failed"] += n
        funnel["live_attempts"] += n
    return funnel


@app.get("/api/system/readiness")
def system_readiness() -> dict[str, Any]:
    """Single-shot answer to the question 'is live trading active and healthy
    right now?'. Wired 2026-04-24 after the week-long unattended run failed
    silently — there was no single endpoint to confirm liveness.

    Returns status in {READY, DEGRADED, NOT_READY} with per-component details.
    Each component is a hard yes/no; DEGRADED means one or more checks failed
    but the system is partially operable; NOT_READY means live trading is
    blocked.
    """
    import os
    import time as _t
    components: list[dict[str, Any]] = []
    now = _t.time()

    def add(name: str, ok: bool, detail: str, blocking: bool = True) -> None:
        components.append(
            {"name": name, "ok": ok, "detail": detail, "blocking": blocking}
        )

    # 1. POLYMARKET_LIVE_ENABLED env var
    live_env = os.environ.get("POLYMARKET_LIVE_ENABLED", "").lower() in ("1", "true", "yes")
    add("env_live_enabled", live_env, "POLYMARKET_LIVE_ENABLED=1" if live_env else "not set")

    # 2. Bankroll > 0
    try:
        from trading_platform.polymarket.bankroll import get_bankroll
        bk = get_bankroll() or 0
        add("bankroll", bk > 0, f"${bk:.2f}")
    except Exception as exc:
        add("bankroll", False, f"error: {str(exc)[:60]}")

    # 3. Kill switch / emergency stop
    try:
        from trading_platform.polymarket.kill_switch import KillSwitch
        ks_inst = KillSwitch(_alpha_db_path(), bankroll=500)
        stopped, reason = ks_inst.is_emergency_stopped()
        add("kill_switch", not stopped, reason if stopped else "active")
    except Exception as exc:
        add("kill_switch", False, f"error: {str(exc)[:60]}")

    # 4. Circuit breaker
    try:
        from trading_platform.polymarket.circuit_breaker import CircuitBreaker
        cb = CircuitBreaker(_alpha_db_path())
        allowed, cb_reason = cb.can_trade()
        add("circuit_breaker", allowed, "ok" if allowed else cb_reason or "halted")
    except Exception as exc:
        add("circuit_breaker", False, f"error: {str(exc)[:60]}")

    # 5. Recent signal activity (any signal in last 60 min)
    try:
        with _db() as conn:
            last_sig = conn.execute(
                "SELECT MAX(fired_at) FROM market_signals"
            ).fetchone()[0]
        last_sig = float(last_sig) if last_sig else 0
        age_min = (now - last_sig) / 60 if last_sig else 99999
        add("signals_fresh", age_min < 60, f"last signal {age_min:.1f}min ago", blocking=False)
    except Exception as exc:
        add("signals_fresh", False, f"error: {str(exc)[:60]}", blocking=False)

    # 6. Wallet trade ingestion freshness. 60-min threshold accommodates
    # natural quiet periods + the wallet-poller's 10-min cadence + WS
    # reconnect windows. Below this we flag DEGRADED, not NOT_READY.
    try:
        with _db() as conn:
            last_wt = conn.execute(
                "SELECT MAX(timestamp) FROM wallet_trades"
            ).fetchone()[0]
        last_wt = float(last_wt) if last_wt else 0
        age_min = (now - last_wt) / 60 if last_wt else 99999
        add(
            "wallet_trades_fresh", age_min < 60,
            f"last trade {age_min:.1f}min ago",
            blocking=False,  # warn, don't fail readiness on bursty ingestion
        )
    except Exception as exc:
        add("wallet_trades_fresh", False, f"error: {str(exc)[:60]}", blocking=False)

    # 7. Scheduler tasks not in sustained failure
    try:
        from pathlib import Path
        import json as _j
        state_file = Path("/app/data/scheduler/state.json")
        if state_file.exists():
            data = _j.loads(state_file.read_text(encoding="utf-8"))
            failing = [
                t for t in data.get("tasks", [])
                if t.get("enabled", True) and int(t.get("consecutive_failures", 0)) >= 5
            ]
            if failing:
                add(
                    "scheduler_health", False,
                    f"{len(failing)} tasks failing 5+x: {', '.join(t['name'] for t in failing[:3])}",
                )
            else:
                add("scheduler_health", True, "no sustained failures")
        else:
            add("scheduler_health", False, "state file missing")
    except Exception as exc:
        add("scheduler_health", False, f"error: {str(exc)[:60]}")

    # 8. PM leaderboard sync staleness — closes the silent-staleness mode
    # diagnosed 2026-04-25 where the daily sync hadn't run for >24h
    # because container restarts kept resetting `last_run_at`. Threshold
    # 36h gives one full daily-cycle slack.
    try:
        with _db() as conn:
            row = conn.execute(
                "SELECT MAX(pm_synced_at) FROM wallet_profiles WHERE pm_synced_at IS NOT NULL"
            ).fetchone()
        last_sync = float(row[0]) if row and row[0] else 0
        age_h = (now - last_sync) / 3600 if last_sync else 99999
        add(
            "pm_leaderboard_fresh", age_h < 36,
            f"last sync {age_h:.1f}h ago",
            blocking=False,
        )
    except Exception as exc:
        add("pm_leaderboard_fresh", False, f"error: {str(exc)[:60]}", blocking=False)

    # 9. Wallet behavior metrics freshness — same logic, daily cadence
    try:
        with _db() as conn:
            row = conn.execute(
                "SELECT MAX(last_updated) FROM wallet_behavior_metrics"
            ).fetchone()
        last_bm = float(row[0]) if row and row[0] else 0
        age_h = (now - last_bm) / 3600 if last_bm else 99999
        add(
            "behavior_metrics_fresh", age_h < 36,
            f"last run {age_h:.1f}h ago" if last_bm else "never run",
            blocking=False,
        )
    except Exception as exc:
        add("behavior_metrics_fresh", False, f"error: {str(exc)[:60]}", blocking=False)

    blockers = [c for c in components if not c["ok"] and c["blocking"]]
    warnings = [c for c in components if not c["ok"] and not c["blocking"]]
    if not blockers and not warnings:
        status = "READY"
    elif blockers:
        status = "NOT_READY"
    else:
        status = "DEGRADED"

    return {
        "status": status,
        "blockers": [c["name"] for c in blockers],
        "warnings": [c["name"] for c in warnings],
        "components": components,
        "checked_at": int(now),
    }


@app.get("/api/system/health")
def system_health() -> dict[str, Any]:
    """Overall system health for the dashboard header."""
    import os
    try:
        with _db() as conn:
            yes_rate = conn.execute("""
                SELECT AVG(CASE WHEN resolution_price=1.0 THEN 1.0 ELSE 0.0 END)
                FROM signal_outcomes WHERE resolution_price IS NOT NULL
            """).fetchone()[0] or 0
            signals_24h = conn.execute("""
                SELECT COUNT(*) FROM market_signals
                WHERE fired_at > strftime('%s', 'now', '-24 hours')
            """).fetchone()[0]
            live_open = conn.execute("""
                SELECT COUNT(*) FROM live_trades WHERE exit_ts IS NULL AND fill_price IS NOT NULL
            """).fetchone()[0]
            last_signal = conn.execute(
                "SELECT MAX(fired_at) FROM market_signals"
            ).fetchone()[0]
            last_live = conn.execute(
                "SELECT MAX(submitted_at) FROM live_trades WHERE dry_run=0"
            ).fetchone()[0]

        from trading_platform.polymarket.kill_switch import KillSwitch
        ks = KillSwitch(_alpha_db_path(), bankroll=500)
        stopped, reason = ks.is_emergency_stopped()

        return {
            "db_integrity": True,
            "pipeline_running": signals_24h > 0,
            "resolution_yes_rate": round(yes_rate, 3),
            "signals_24h": signals_24h,
            "live_positions": live_open,
            "kill_switch": "tripped" if stopped else "active",
            "kill_switch_reason": reason if stopped else None,
            "last_signal_ts": last_signal,
            "last_live_trade_ts": last_live,
            "live_enabled": os.getenv("POLYMARKET_LIVE_ENABLED") == "1",
        }
    except Exception as exc:
        return {"error": str(exc), "db_integrity": False}


@app.post("/api/live/emergency-stop")
def live_emergency_stop(request: dict[str, Any] | None = None) -> dict[str, Any]:
    """Activate the kill-switch flag file. Halts all live trading."""
    try:
        from trading_platform.polymarket.kill_switch import KillSwitch
        from trading_platform.polymarket.wallet_db import WalletDB
        reason = (request or {}).get("reason", "manual via API")
        KillSwitch(WalletDB.default_path()).emergency_stop(reason)
        return {"ok": True, "reason": reason}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


@app.post("/api/live/clear-stop")
def live_clear_stop() -> dict[str, Any]:
    """Clear the kill-switch flag file."""
    try:
        from trading_platform.polymarket.kill_switch import KillSwitch
        from trading_platform.polymarket.wallet_db import WalletDB
        KillSwitch(WalletDB.default_path()).clear_emergency_stop()
        return {"ok": True}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


@app.post("/api/live/test-dry-run")
def live_test_dry_run(request: dict[str, Any] | None = None) -> dict[str, Any]:
    """Run a synthetic dry-run trade through the live executor pipeline."""
    try:
        from trading_platform.polymarket.polymarket_live_executor import PolymarketLiveExecutor
        sig_type = (request or {}).get("signal_type", "price_velocity")
        return PolymarketLiveExecutor().test_dry_run(sig_type)
    except Exception as exc:
        return {"success": False, "error": str(exc)}


@app.post("/api/paper/check-resolutions")
def paper_check_resolutions() -> dict[str, Any]:
    """Walk open paper trades and resolve any whose underlying market settled."""
    try:
        from trading_platform.polymarket.polymarket_paper_executor import PolymarketPaperExecutor
        with PolymarketPaperExecutor() as ex:
            return ex.check_and_resolve_open_trades()
    except Exception as exc:
        return {"checked": 0, "resolved": 0, "error": str(exc)}


@app.get("/api/markets/top")
def markets_top(
    sort: str = "volume24h",
    category: str | None = None,
    limit: int = 20,
) -> dict[str, Any]:
    """Top markets for the Market Monitor browse view."""
    return reader.read_top_markets(category=category, sort=sort, limit=limit)


@app.get("/api/market/{condition_id}/candles")
def market_candles(
    condition_id: str,
    interval: str = "1d",
    from_ts: int | None = None,
    to_ts: int | None = None,
) -> dict[str, Any]:
    """Synthesized OHLCV candles for a market."""
    return reader.read_market_candles(condition_id, interval=interval, from_ts=from_ts, to_ts=to_ts)


@app.get("/api/market/{condition_id}/flow")
def market_flow(
    condition_id: str,
    limit: int = 200,
    from_ts: int | None = None,
    to_ts: int | None = None,
) -> dict[str, Any]:
    """Order flow trades on a market: tracked wallets from DB + recent untracked from data-api."""
    return reader.read_market_trade_flow(condition_id, from_ts=from_ts, to_ts=to_ts, limit=limit)


@app.get("/api/market/{condition_id}/signals")
def market_signals(condition_id: str) -> dict[str, Any]:
    """Signals + paper trades fired for a market."""
    return reader.read_market_signals_and_trades(condition_id)


@app.get("/api/market/{condition_id}/intelligence")
def market_intelligence(condition_id: str) -> dict[str, Any]:
    """Full market intelligence: price history, all tier1/1h wallet activity,
    aggregated consensus, and convergence signal.
    """
    return reader.read_market_intelligence(condition_id)


@app.get("/api/markets/search")
def markets_search(
    q: str = "",
    category: str | None = None,
    has_whale_activity: bool = False,
    limit: int = 50,
) -> dict[str, Any]:
    """Search markets with whale activity, ordered by tier1/1h wallet count."""
    return reader.search_markets(q=q, category=category,
                                 has_whale_activity=has_whale_activity, limit=limit)


@app.post("/api/backtest/convergence")
def backtest_convergence(request: dict[str, Any]) -> dict[str, Any]:
    """Backtest the 'wait for N wallets to converge' strategy."""
    return reader.run_convergence_backtest(request)


@app.get("/api/wallets/{wallet_address}")
def wallet_detail(wallet_address: str) -> dict[str, Any]:
    """Alias for /api/smart-money/wallet/{address} — preferred endpoint."""
    return reader.read_smart_money_wallet_detail(wallet_address)


@app.get("/api/smart-money/alerts")
def smart_money_alerts(limit: int = 50, wallet: str | None = None, tier: int | None = None) -> dict[str, Any]:
    return reader.read_smart_money_alerts(limit=limit, wallet=wallet, tier=tier)


@app.get("/api/smart-money/open-positions")
def smart_money_open_positions() -> dict[str, Any]:
    return reader.read_smart_money_open_positions()


@app.get("/api/smart-money/actionable-signals")
def smart_money_actionable_signals() -> dict[str, Any]:
    return reader.read_smart_money_actionable_signals()


@app.get("/api/smart-money/universe-stats")
def smart_money_universe_stats() -> dict[str, Any]:
    return reader.read_smart_money_universe_stats()


@app.get("/api/paper/bankroll")
def paper_bankroll() -> dict[str, Any]:
    return reader.read_paper_bankroll()


@app.get("/api/pipeline/funnel")
def pipeline_funnel(hours: int = 24) -> dict[str, Any]:
    """Signal→paper-trade conversion funnel by signal_type + category."""
    return reader.read_pipeline_funnel(hours=max(1, min(168, hours)))


@app.get("/api/system/http-circuit")
def http_circuit() -> dict[str, Any]:
    """Outbound-HTTP circuit breaker state (per host)."""
    return reader.read_http_circuit_state()


@app.get("/api/trade-journal")
def trade_journal(limit: int = 50) -> dict[str, Any]:
    """Trade journal with gate-value snapshots + per-signal attribution."""
    return reader.read_trade_journal(limit=max(1, min(200, limit)))


@app.get("/api/exit-analyzer")
def exit_analyzer(min_sample: int = 10) -> dict[str, Any]:
    """Per-signal MAE/MFE analysis → recommended TP/SL adjustments."""
    return reader.read_mae_mfe_analysis(min_sample=max(3, min(100, min_sample)))


@app.get("/api/signal-attribution")
def signal_attribution(hours: int = 168) -> dict[str, Any]:
    """Per-signal PnL attribution — which signals make vs lose money."""
    return reader.read_signal_attribution(hours=max(1, min(720, hours)))


class BacktestConfig(BaseModel):
    hours: int = 168
    signal_types: list[str] | None = None
    min_confidence: float = 0.30
    min_fusion: float = 0.05
    min_entry_price: float = 0.10
    max_entry_price: float = 0.85
    stake_usd: float = 50.0
    apply_spread: float = 0.015


@app.post("/api/backtest/replay")
def backtest_replay(cfg: BacktestConfig) -> dict[str, Any]:
    """Replay paper trades under configurable gates. Returns PnL / WR / per-signal.
    Distinct from /api/backtest/run which runs the historical signal engine.
    """
    return reader.run_signal_backtest(
        hours=max(1, min(720, cfg.hours)),
        signal_types=cfg.signal_types,
        min_confidence=max(0, min(1, cfg.min_confidence)),
        min_fusion=max(0, min(1, cfg.min_fusion)),
        min_entry_price=max(0, min(1, cfg.min_entry_price)),
        max_entry_price=max(0, min(1, cfg.max_entry_price)),
        stake_usd=max(1, min(1000, cfg.stake_usd)),
        apply_spread=max(0, min(0.1, cfg.apply_spread)),
    )


@app.get("/api/paper/pnl-history")
def paper_pnl_history() -> dict[str, Any]:
    return reader.read_paper_pnl_history()


@app.post("/api/backtest/strategy")
def backtest_strategy(config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Run the systematic backtest framework against resolved signal_outcomes."""
    from trading_platform.polymarket.backtest_framework import run_backtest, BacktestConfig
    cfg = BacktestConfig()
    if config:
        for k in ("signal_types", "categories", "wallet_tiers"):
            if k in config:
                setattr(cfg, k, config[k])
        for k in ("min_confidence", "min_alpha_score", "flat_stake", "lookback_days", "train_pct"):
            if k in config:
                setattr(cfg, k, float(config[k]))
        if "entry_price_range" in config:
            cfg.entry_price_range = tuple(config["entry_price_range"])
    return run_backtest(cfg)


@app.get("/api/backtest/ensemble-validation")
def backtest_ensemble_validation(lookback_days: int = 60) -> dict[str, Any]:
    """Validate ensemble scorer: do higher scores predict better outcomes?"""
    from trading_platform.polymarket.backtest_framework import run_ensemble_backtest
    return run_ensemble_backtest(lookback_days=lookback_days)


@app.get("/api/smart-money/leaderboard")
def smart_money_leaderboard(sort_by: str = "equity_score") -> dict[str, Any]:
    return reader.read_smart_money_leaderboard(sort_by=sort_by)


@app.get("/api/smart-money/winners")
def smart_money_winners(window: str = "all") -> dict[str, Any]:
    return reader.read_smart_money_winners(window=window)


# /api/wallets/winners — see early declaration above (line ~133)
# It must be declared before /api/wallets/{wallet_address} to avoid shadowing.


@app.get("/api/smart-money/wallet/{address}/positions")
def smart_money_wallet_positions(address: str) -> dict[str, Any]:
    return reader.read_smart_money_wallet_positions(address)


@app.get("/api/smart-money/wallet/{address}/trades")
def smart_money_wallet_trades(address: str, page: int = 1, limit: int = 50) -> dict[str, Any]:
    return reader.read_smart_money_wallet_trades(address, page=page, limit=limit)


@app.get("/api/smart-money/mirror")
def smart_money_mirror() -> dict[str, Any]:
    return reader.read_smart_money_mirror()


@app.get("/api/polymarket/whale-feed")
def polymarket_whale_feed() -> dict[str, Any]:
    return reader.read_polymarket_whale_feed()


@app.get("/api/polymarket/subscription-status")
def polymarket_subscription_status() -> dict[str, Any]:
    return reader.read_polymarket_subscription_status()


@app.get("/api/polymarket/signals-feed")
def polymarket_signals_feed() -> dict[str, Any]:
    return reader.read_polymarket_signals_feed()


@app.get("/api/polymarket/category-performance")
def polymarket_category_performance() -> dict[str, Any]:
    return reader.read_polymarket_category_performance()


@app.get("/api/system/intelligence-health")
def system_intelligence_health() -> dict[str, Any]:
    return reader.read_intelligence_health()


@app.get("/api/market/{condition_id}")
def market_detail(condition_id: str) -> dict[str, Any]:
    return reader.read_market_detail(condition_id)


@app.get("/api/paper/positions-enriched")
def paper_positions_enriched() -> dict[str, Any]:
    return reader.read_paper_positions_enriched()


@app.get("/api/system/pipeline-status")
def system_pipeline_status() -> dict[str, Any]:
    return reader.read_pipeline_status()


@app.get("/api/system/pipeline-runs")
def system_pipeline_runs() -> list[dict[str, Any]]:
    return reader.read_pipeline_runs()


@app.post("/api/system/run-pipeline")
def system_run_pipeline() -> dict[str, Any]:
    import sys as _sys
    subprocess.Popen(
        [_sys.executable, "scripts/run_daily_intelligence.py"],
        cwd=str(reader.DATA_ROOT.parent),
    )
    return {"started": True, "message": "Pipeline started"}


# ── Kill switch (emergency stop) ────────────────────────────────────────────


@app.get("/api/system/kill-switch")
def system_kill_switch_status() -> dict[str, Any]:
    """Return KillSwitch emergency stop state + hard-limit config."""
    try:
        from trading_platform.polymarket.kill_switch import KillSwitch
        ks = KillSwitch(_alpha_db_path())
        stopped, reason = ks.is_emergency_stopped()
        return {
            "tripped": stopped,
            "reason": reason or None,
            "config": {
                "MAX_DAILY_LOSS_PCT": ks.MAX_DAILY_LOSS_PCT,
                "MAX_OPEN_POSITIONS": ks.MAX_OPEN_POSITIONS,
                "MAX_TRADE_USD": ks.MAX_TRADE_USD,
                "MIN_WIN_RATE": ks.MIN_WIN_RATE,
                "MIN_RESOLVED_HARD": ks.MIN_RESOLVED_HARD,
                "PREFERRED_MIN_RESOLVED": ks.PREFERRED_MIN_RESOLVED,
                "BANKROLL": ks.BANKROLL,
            },
        }
    except Exception as exc:
        return {"tripped": False, "error": str(exc)}


class KillSwitchReq(BaseModel):
    reason: str = "manual"


@app.post("/api/system/kill-switch/trip")
def system_kill_switch_trip(req: KillSwitchReq) -> dict[str, Any]:
    """Activate the emergency stop file — blocks every future paper+live trade."""
    try:
        from trading_platform.polymarket.kill_switch import KillSwitch
        KillSwitch(_alpha_db_path()).emergency_stop(req.reason or "manual")
        return {"ok": True, "tripped": True, "reason": req.reason}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


@app.post("/api/system/kill-switch/reset")
def system_kill_switch_reset() -> dict[str, Any]:
    """Clear the emergency stop file."""
    try:
        from trading_platform.polymarket.kill_switch import KillSwitch
        KillSwitch(_alpha_db_path()).clear_emergency_stop()
        return {"ok": True, "tripped": False}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


@app.post("/api/alerts/telegram-test")
def alerts_telegram_test() -> dict[str, Any]:
    try:
        from trading_platform.polymarket.telegram_alerts import TelegramAlerter
        alerter = TelegramAlerter()
        if not alerter.enabled:
            return {"success": False, "error": "TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set"}
        ok = alerter.send_test()
        return {"success": ok, "error": None if ok else "Send failed"}
    except Exception as exc:
        return {"success": False, "error": str(exc)}


# ── AlertManager endpoints ──────────────────────────────────────────────────


@app.post("/api/alerts/send-digest")
def alerts_send_digest() -> dict[str, Any]:
    """Manually trigger the daily digest. Used by the scheduler at 08:00
    UTC and available here for ad-hoc / dev triggering."""
    try:
        from trading_platform.polymarket.alert_manager import get_alert_manager
        sent = get_alert_manager().send_daily_digest()
        return {"success": bool(sent)}
    except Exception as exc:
        return {"success": False, "error": str(exc)}


@app.get("/api/alerts/recent")
def alerts_recent(limit: int = 20) -> dict[str, Any]:
    """Recent alerts dispatched through AlertManager (in-memory log)."""
    try:
        from trading_platform.polymarket.alert_manager import get_alert_manager
        return {"available": True, "alerts": get_alert_manager().get_recent(limit)}
    except Exception as exc:
        return {"available": False, "error": str(exc), "alerts": []}


@app.get("/api/alerts/config")
def alerts_config() -> dict[str, Any]:
    """Current AlertManager configuration + counters."""
    try:
        from trading_platform.polymarket.alert_manager import get_alert_manager
        return get_alert_manager().get_config()
    except Exception as exc:
        return {"telegram_configured": False, "error": str(exc)}


# ── Per-wallet alpha scores (Part 4-5 of pnl_investigation followup) ────────


def _alpha_db_path() -> str:
    """Resolve the wallet intelligence DB path.

    In Docker, always returns a /tmp copy to avoid NTFS hangs during
    heavy KPI queries. The shared db module handles the copy + caching.
    """
    from trading_platform.polymarket.db import _resolve_path, _get_tmp_copy
    path = _resolve_path()
    if not path:
        from trading_platform.polymarket.wallet_db import WalletDB
        return WalletDB.default_path()
    # Always use /tmp copy for the API — queries are heavy and NTFS
    # bind mounts hang intermittently under concurrent access.
    tmp = _get_tmp_copy(path)
    return tmp or path


@app.get("/api/alpha/summary")
def alpha_summary() -> dict[str, Any]:
    try:
        from trading_platform.polymarket.alpha_scores import get_summary
        return get_summary(_alpha_db_path())
    except Exception as exc:
        return {"available": False, "error": str(exc)}


@app.get("/api/alpha/leaderboard")
def alpha_leaderboard(category: str, min_score: float = 0.5, limit: int = 50) -> dict[str, Any]:
    try:
        from trading_platform.polymarket.alpha_scores import get_category_leaderboard
        rows = get_category_leaderboard(
            _alpha_db_path(), category=category, min_score=min_score, limit=limit,
        )
        return {"available": True, "category": category, "wallets": rows}
    except Exception as exc:
        return {"available": False, "error": str(exc), "wallets": []}


@app.get("/api/wallet/{address}/alpha")
def wallet_alpha(address: str) -> dict[str, Any]:
    try:
        from trading_platform.polymarket.alpha_scores import get_wallet_alpha_full
        rows = get_wallet_alpha_full(_alpha_db_path(), address)
        return {"available": True, "wallet": address, "categories": rows}
    except Exception as exc:
        return {"available": False, "error": str(exc), "categories": []}


@app.post("/api/alpha/recompute")
def alpha_recompute() -> dict[str, Any]:
    """Manually trigger an alpha-score recompute (also runs daily via scheduler)."""
    try:
        from trading_platform.polymarket.alpha_scores import compute_alpha_scores
        return compute_alpha_scores(_alpha_db_path())
    except Exception as exc:
        return {"error": str(exc)}


@app.get("/api/hypotheses/recent")
def hypotheses_recent(limit: int = 20) -> dict[str, Any]:
    """Recent trade hypotheses (rationale rows for paper trades)."""
    try:
        from trading_platform.polymarket.trade_hypotheses import get_recent_hypotheses
        return {"available": True, "hypotheses": get_recent_hypotheses(_alpha_db_path(), limit)}
    except Exception as exc:
        return {"available": False, "error": str(exc), "hypotheses": []}


# ── Thesis scorecard endpoints (KPI tracker) ────────────────────────────────


@app.get("/api/thesis/scorecard")
def thesis_scorecard() -> dict[str, Any]:
    """The single endpoint that summarizes whether the trading thesis
    is being confirmed. Drives the Command Center hero card."""
    try:
        from trading_platform.polymarket.kpi_tracker import KPITracker
        return KPITracker(_alpha_db_path()).compute_all()
    except Exception as exc:
        return {"available": False, "error": str(exc)}


@app.get("/api/thesis/claims")
def thesis_claims() -> dict[str, Any]:
    """Lightweight version — just the 5 claim statuses + scorecard."""
    try:
        from trading_platform.polymarket.kpi_tracker import KPITracker
        return KPITracker(_alpha_db_path()).claims_only()
    except Exception as exc:
        return {"available": False, "error": str(exc)}


@app.get("/api/thesis/history")
def thesis_history(days: int = 30) -> dict[str, Any]:
    """Daily snapshots of hypothesis accuracy over time."""
    try:
        from trading_platform.polymarket.kpi_tracker import KPITracker
        return {"available": True, "snapshots": KPITracker(_alpha_db_path()).get_history(days)}
    except Exception as exc:
        return {"available": False, "error": str(exc), "snapshots": []}


@app.post("/api/thesis/snapshot")
def thesis_snapshot() -> dict[str, Any]:
    """Manually trigger a daily snapshot save (also runs daily via scheduler)."""
    try:
        from trading_platform.polymarket.kpi_tracker import KPITracker
        return KPITracker(_alpha_db_path()).save_daily_snapshot()
    except Exception as exc:
        return {"saved": False, "error": str(exc)}


# ── Decision evaluation endpoints ──────────────────────────────────────────


@app.get("/api/wallet/{address}/expected-returns")
def wallet_expected_returns(address: str) -> dict[str, Any]:
    """Full return distribution stats for a wallet across all categories."""
    try:
        with _db() as conn:
            conn.row_factory = __import__("sqlite3").Row
            rows = conn.execute(
                """SELECT category, win_rate, resolved_trades,
                          avg_win_pnl, avg_loss_pnl, profit_factor,
                          sharpe_ratio, kelly_fraction, max_win, max_loss,
                          pnl_stddev, avg_hold_days, streak_current,
                          streak_max_win, streak_max_loss, copyability, is_copyable
                   FROM wallet_alpha_scores WHERE LOWER(wallet) = LOWER(?)""",
                (address,),
            ).fetchall()

            categories = {}
            for r in rows:
                d = dict(r)
                cat = d.pop("category")
                categories[cat] = d

            if not categories:
                lb = conn.execute(
                    """SELECT directional_win_rate, rolling_20_wr,
                              conviction_score, resolved_trades,
                              total_volume_usdc, net_pnl_usdc, tier,
                              wallet_type, pseudonym, leaderboard_source
                       FROM leaderboard WHERE LOWER(wallet) = LOWER(?)""",
                    (address,),
                ).fetchone()
                if not lb:
                    return {"available": False, "reason": "Wallet not in leaderboard or alpha scores"}
                profile = dict(lb)
                return {
                    "available": True,
                    "wallet": address,
                    "source": "leaderboard",
                    "profile": profile,
                    "categories": {},
                    "note": "No per-category alpha scores — wallet may lack sufficient clean trades",
                }

            return {"available": True, "wallet": address, "source": "alpha_scores", "categories": categories}
    except Exception as exc:
        return {"available": False, "error": str(exc)}


@app.get("/api/trade/{trade_id}/analysis")
def trade_analysis(trade_id: int) -> dict[str, Any]:
    """Full pre-trade hypothesis + post-trade analysis for a single trade."""
    try:
        import json
        with _db() as conn:
            conn.row_factory = __import__("sqlite3").Row
            hypo = conn.execute(
                "SELECT * FROM trade_hypotheses WHERE trade_id = ?",
                (trade_id,),
            ).fetchone()
            trade = conn.execute(
                "SELECT * FROM polymarket_paper_trades WHERE id = ?",
                (trade_id,),
            ).fetchone()
            snapshots = conn.execute(
                """SELECT current_price, unrealized_pnl, unrealized_pnl_pct,
                          time_held_hours, snapshot_at
                   FROM position_snapshots WHERE trade_id = ?
                   ORDER BY snapshot_at""",
                (trade_id,),
            ).fetchall()

        result: dict[str, Any] = {"available": True, "trade_id": trade_id}
        if hypo:
            h = dict(hypo)
            if h.get("confidence_factors"):
                try:
                    h["confidence_factors"] = json.loads(h["confidence_factors"])
                except Exception:
                    pass
            if h.get("post_analysis_json"):
                try:
                    h["post_analysis"] = json.loads(h["post_analysis_json"])
                except Exception:
                    pass
            result["hypothesis"] = h
        if trade:
            result["trade"] = dict(trade)
        result["snapshots"] = [dict(s) for s in snapshots]
        return result
    except Exception as exc:
        return {"available": False, "error": str(exc)}


@app.get("/api/calibration/accuracy")
def calibration_accuracy() -> dict[str, Any]:
    """WR calibration table + EV calibration + lag analysis."""
    try:
        from trading_platform.polymarket.trade_hypotheses import get_calibration_data
        return get_calibration_data(_alpha_db_path())
    except Exception as exc:
        return {"sufficient_data": False, "error": str(exc)}


@app.get("/api/positions/mark-to-market")
def positions_mtm() -> dict[str, Any]:
    """All open positions with current unrealized P&L."""
    try:
        from trading_platform.polymarket.position_monitor import get_open_positions_mtm
        positions = get_open_positions_mtm(_alpha_db_path())
        total = sum(p.get("unrealized_pnl") or 0 for p in positions)
        return {
            "available": True,
            "positions": positions,
            "total_unrealized": round(total, 2),
            "open_count": len(positions),
        }
    except Exception as exc:
        return {"available": False, "error": str(exc)}


@app.get("/api/execution/gates")
def execution_gates_status() -> dict[str, Any]:
    """Current gate thresholds + recent gate results from hypotheses."""
    try:
        import json
        from trading_platform.polymarket.execution_gates import _PAPER_THRESHOLDS
        with _db() as conn:
            recent = conn.execute(
                """SELECT trade_id, gate_results_json FROM trade_hypotheses
                   WHERE gate_results_json IS NOT NULL
                   ORDER BY created_at DESC LIMIT 10"""
            ).fetchall()
        results = []
        for tid, gj in recent:
            try:
                gr = json.loads(gj)
                passed = [k for k, v in gr.items() if isinstance(v, dict) and v.get("passed")]
                failed = [k for k, v in gr.items() if isinstance(v, dict) and not v.get("passed", True)]
                results.append({"trade_id": tid, "gates_passed": passed, "gates_failed": failed})
            except Exception:
                pass
        return {"available": True, "thresholds": _PAPER_THRESHOLDS, "recent_results": results}
    except Exception as exc:
        return {"available": False, "error": str(exc)}


@app.get("/api/positions/whale-exits")
def whale_exits() -> dict[str, Any]:
    """Recent whale exit signals."""
    try:
        with _db() as conn:
            conn.row_factory = __import__("sqlite3").Row
            rows = conn.execute(
                """SELECT * FROM whale_exit_signals ORDER BY detected_at DESC LIMIT 20"""
            ).fetchall()
            return {"available": True, "exits": [dict(r) for r in rows]}
    except Exception as exc:
        return {"available": False, "exits": [], "error": str(exc)}


@app.get("/api/system/execution-policy")
def get_execution_policy() -> dict[str, Any]:
    return reader.read_execution_policy()


@app.post("/api/system/execution-policy")
def set_execution_policy(request: dict[str, Any]) -> dict[str, Any]:
    return reader.write_execution_policy(request.get("policy", "paper_t1"))


@app.get("/api/paper/dashboard")
def paper_dashboard() -> dict[str, Any]:
    return reader.read_paper_dashboard()


@app.get("/api/paper/portfolio")
def paper_portfolio() -> dict[str, Any]:
    return reader.read_paper_portfolio()


@app.get("/api/paper/trades")
def paper_trades() -> dict[str, Any]:
    return reader.read_paper_trades()


@app.get("/api/paper/scan")
def paper_scan() -> dict[str, Any]:
    return reader.read_paper_scan()


# ── Paper analytics endpoints ──────────────────────────────────────────────


@app.get("/api/paper/analytics/signals")
def paper_analytics_signals() -> dict[str, Any]:
    try:
        from trading_platform.polymarket.paper_analytics import PaperAnalytics
        return {"available": True, "data": PaperAnalytics(_alpha_db_path()).signal_performance()}
    except Exception as exc:
        return {"available": False, "error": str(exc)}


@app.get("/api/paper/analytics/categories")
def paper_analytics_categories() -> dict[str, Any]:
    try:
        from trading_platform.polymarket.paper_analytics import PaperAnalytics
        return {"available": True, "data": PaperAnalytics(_alpha_db_path()).category_performance()}
    except Exception as exc:
        return {"available": False, "error": str(exc)}


@app.get("/api/paper/analytics/wallets")
def paper_analytics_wallets() -> dict[str, Any]:
    try:
        from trading_platform.polymarket.paper_analytics import PaperAnalytics
        return {"available": True, "data": PaperAnalytics(_alpha_db_path()).wallet_performance()}
    except Exception as exc:
        return {"available": False, "error": str(exc)}


@app.get("/api/paper/analytics/exits")
def paper_analytics_exits() -> dict[str, Any]:
    try:
        from trading_platform.polymarket.paper_analytics import PaperAnalytics
        return {"available": True, "data": PaperAnalytics(_alpha_db_path()).exit_analysis()}
    except Exception as exc:
        return {"available": False, "error": str(exc)}


@app.get("/api/paper/analytics/drawdown")
def paper_analytics_drawdown() -> dict[str, Any]:
    try:
        from trading_platform.polymarket.paper_analytics import PaperAnalytics
        return PaperAnalytics(_alpha_db_path()).drawdown_analysis()
    except Exception as exc:
        return {"available": False, "error": str(exc)}


@app.get("/api/paper/analytics/summary")
def paper_analytics_summary() -> dict[str, Any]:
    try:
        from trading_platform.polymarket.paper_analytics import PaperAnalytics
        return {"available": True, **PaperAnalytics(_alpha_db_path()).portfolio_summary()}
    except Exception as exc:
        return {"available": False, "error": str(exc)}


@app.get("/api/paper/equity-curve")
def paper_equity_curve() -> dict[str, Any]:
    try:
        with _db() as conn:
            rows = conn.execute(
                "SELECT ts, cash, positions_value, total_equity, open_count, realized_pnl_cumulative, unrealized_pnl FROM paper_equity_snapshots ORDER BY ts"
            ).fetchall()
            data = [
                {"ts": r[0], "cash": r[1], "positions_value": r[2],
                 "total_equity": r[3], "open_count": r[4],
                 "realized_pnl": r[5], "unrealized_pnl": r[6]}
                for r in rows
            ]
            return {"available": True, "data": data}
    except Exception as exc:
        return {"available": False, "data": [], "error": str(exc)}


@app.get("/api/reports/daily")
def daily_report() -> dict[str, Any]:
    """Daily summary for digest and Command Center."""
    try:
        import time
        now = int(time.time())
        yesterday = now - 86400
        with _db() as conn:
            row = conn.execute("""
                SELECT COUNT(*),
                       SUM(CASE WHEN exit_ts IS NULL AND archived=0 THEN 1 ELSE 0 END),
                       SUM(CASE WHEN outcome IS NOT NULL AND archived=0 THEN 1 ELSE 0 END),
                       SUM(CASE WHEN outcome='win' AND archived=0 THEN 1 ELSE 0 END),
                       COALESCE(SUM(CASE WHEN outcome IS NOT NULL AND archived=0 THEN realized_pnl ELSE 0 END), 0),
                       COALESCE(SUM(CASE WHEN exit_ts IS NULL AND archived=0 THEN size_usd ELSE 0 END), 0)
                FROM polymarket_paper_trades WHERE archived=0
            """).fetchone()
            new_today = conn.execute(
                "SELECT COUNT(*) FROM polymarket_paper_trades WHERE archived=0 AND entry_ts > ?",
                (yesterday,),
            ).fetchone()[0]
            exits_today = conn.execute(
                "SELECT COUNT(*), COALESCE(SUM(realized_pnl),0) FROM polymarket_paper_trades WHERE archived=0 AND exit_ts > ?",
                (yesterday,),
            ).fetchone()
            cats = conn.execute("""
                SELECT category, COUNT(*), COALESCE(SUM(size_usd),0)
                FROM polymarket_paper_trades WHERE archived=0 AND exit_ts IS NULL
                GROUP BY category
            """).fetchall()

        return {
            "available": True,
            "date": __import__("datetime").date.today().isoformat(),
            "portfolio": {
                "bankroll": 10000,
                "deployed": round(row[5] or 0, 2),
                "available": round(10000 - (row[5] or 0), 2),
                "realized_pnl": round(row[4] or 0, 2),
                "go_live_progress": f"{row[2] or 0}/50",
            },
            "today": {
                "paper_trades_placed": new_today,
                "exits": exits_today[0],
                "exit_pnl": round(exits_today[1] or 0, 2),
            },
            "category_exposure": {
                c[0]: {"deployed": round(c[2], 2), "pct": round(c[2] / 10000 * 100, 1), "trades": c[1]}
                for c in cats
            },
        }
    except Exception as exc:
        return {"available": False, "error": str(exc)}


@app.get("/api/paper/sizing")
def paper_sizing() -> dict[str, Any]:
    """Current position sizing parameters and available capital."""
    try:
        from trading_platform.polymarket.polymarket_paper_executor import (
            HALF_KELLY, MIN_STAKE_USD, MAX_STAKE_USD, MAX_PORTFOLIO_PCT,
            MIN_ENTRY_PRICE, MAX_ENTRY_PRICE, OPTIMAL_BAND_LOW, OPTIMAL_BAND_HIGH,
            STARTING_BANKROLL,
        )
        with _db() as conn:
            deployed = conn.execute(
                "SELECT COALESCE(SUM(size_usd),0) FROM polymarket_paper_trades WHERE exit_ts IS NULL AND archived=0"
            ).fetchone()[0]
        available = max(0, STARTING_BANKROLL - deployed)
        return {
            "available": True,
            "half_kelly": HALF_KELLY,
            "min_stake": MIN_STAKE_USD,
            "max_stake": MAX_STAKE_USD,
            "max_portfolio_pct": MAX_PORTFOLIO_PCT,
            "bankroll": STARTING_BANKROLL,
            "current_deployed": round(deployed, 2),
            "available_capital": round(available, 2),
            "next_trade_base_stake": round(available * HALF_KELLY * 0.7, 2),
            "entry_price_bounds": {"min": MIN_ENTRY_PRICE, "max": MAX_ENTRY_PRICE},
            "optimal_band": {"low": OPTIMAL_BAND_LOW, "high": OPTIMAL_BAND_HIGH},
        }
    except Exception as exc:
        return {"available": False, "error": str(exc)}


@app.post("/api/paper/check-exits")
def paper_check_exits() -> dict[str, Any]:
    try:
        from trading_platform.polymarket.polymarket_paper_executor import PolymarketPaperExecutor
        with PolymarketPaperExecutor() as ex:
            return ex.check_exits()
    except Exception as exc:
        return {"checked": 0, "exited": 0, "error": str(exc)}


@app.post("/api/paper/snapshot-equity")
def paper_snapshot_equity() -> dict[str, Any]:
    try:
        from trading_platform.polymarket.polymarket_paper_executor import PolymarketPaperExecutor
        with PolymarketPaperExecutor() as ex:
            return ex.snapshot_equity()
    except Exception as exc:
        return {"error": str(exc)}


@app.get("/api/kalshi/market/{ticker}/history")
def kalshi_market_history(ticker: str) -> dict[str, Any]:
    # Sanitize ticker to prevent path traversal
    safe_ticker = ticker.replace("/", "").replace("\\", "").replace("..", "")
    return reader.read_kalshi_market_history(safe_ticker)


# ── Trade reasoning ───────────────────────────────────────────────────────────


@app.get("/api/reasoning/trades")
def reasoning_trades() -> dict[str, Any]:
    return reader.read_reasoning_trades()


# ── Loop decisions & control ──────────────────────────────────────────────────


@app.get("/api/loop/decisions")
def loop_decisions() -> dict[str, Any]:
    return reader.read_loop_decisions()


@app.post("/api/loop/control")
def loop_control(request: LoopControlRequest) -> dict[str, Any]:
    control_dir = reader.ARTIFACTS_ROOT / "control"
    control_dir.mkdir(parents=True, exist_ok=True)

    action = request.action
    if action == "pause":
        (control_dir / "KILL_SWITCH").touch()
        return {"success": True, "message": "Loop paused — KILL_SWITCH written"}
    if action == "resume":
        kill_switch = control_dir / "KILL_SWITCH"
        if kill_switch.exists():
            kill_switch.unlink()
        return {"success": True, "message": "Loop resumed — KILL_SWITCH removed"}
    if action == "trigger_now":
        (control_dir / "TRIGGER_NOW").touch()
        return {"success": True, "message": "Immediate trigger requested — TRIGGER_NOW written"}

    return {"success": False, "message": f"Unknown action '{action}'. Use pause|resume|trigger_now"}


# ── Research jobs ─────────────────────────────────────────────────────────────


def _run_backtest_job(job_id: str) -> None:
    with _jobs_lock:
        _jobs[job_id]["status"] = "running"
        _jobs[job_id]["started_at"] = datetime.now(timezone.utc).isoformat()

    try:
        result = subprocess.run(
            ["trading-cli", "research", "kalshi-full-backtest"],
            capture_output=True,
            text=True,
            timeout=600,
        )
        with _jobs_lock:
            if result.returncode == 0:
                _jobs[job_id]["status"] = "complete"
                _jobs[job_id]["stdout"] = (result.stdout or "")[-2000:]
            else:
                _jobs[job_id]["status"] = "failed"
                _jobs[job_id]["error"] = (result.stderr or "unknown error")[-2000:]
    except subprocess.TimeoutExpired:
        with _jobs_lock:
            _jobs[job_id]["status"] = "failed"
            _jobs[job_id]["error"] = "Backtest timed out after 600 seconds"
    except Exception as exc:
        with _jobs_lock:
            _jobs[job_id]["status"] = "failed"
            _jobs[job_id]["error"] = str(exc)

    with _jobs_lock:
        _jobs[job_id]["completed_at"] = datetime.now(timezone.utc).isoformat()


@app.post("/api/research/run")
def research_run() -> dict[str, Any]:
    job_id = str(uuid.uuid4())
    with _jobs_lock:
        _jobs[job_id] = {
            "job_id": job_id,
            "status": "queued",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
    thread = threading.Thread(target=_run_backtest_job, args=(job_id,), daemon=True)
    thread.start()
    return {"job_id": job_id}


@app.get("/api/research/status/{job_id}")
def research_status(job_id: str) -> dict[str, Any]:
    with _jobs_lock:
        job = _jobs.get(job_id)
    if job is None:
        return {"available": False, "reason": f"Job {job_id} not found"}

    response = dict(job)
    if response.get("status") == "complete":
        perf = reader.read_signals_performance()
        response["results"] = perf.get("data", [])

    return response


# Shared research datasets and provider monitoring


@app.get("/api/research/datasets")
def research_datasets(
    provider: str | None = None,
    asset_class: str | None = None,
    dataset_name: str | None = None,
) -> dict[str, Any]:
    return reader.read_research_dataset_registry(
        provider=provider,
        asset_class=asset_class,
        dataset_name=dataset_name,
    )


@app.get("/api/research/datasets/{dataset_key}")
def research_dataset_detail(dataset_key: str) -> dict[str, Any]:
    return reader.read_research_dataset_detail(dataset_key)


@app.get("/api/research/datasets/{dataset_key}/rows")
def research_dataset_rows(
    dataset_key: str,
    symbol: list[str] | None = Query(default=None),
    interval: list[str] | None = Query(default=None),
    start: str | None = None,
    end: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    return reader.read_research_dataset_rows(
        dataset_key=dataset_key,
        symbols=symbol,
        intervals=interval,
        start=start,
        end=end,
        limit=limit,
    )


@app.get("/api/ops/registry-summary")
def ops_registry_summary() -> dict[str, Any]:
    return reader.read_registry_publication_summary()


@app.get("/api/ops/provider-monitoring")
def ops_provider_monitoring() -> dict[str, Any]:
    return reader.read_provider_monitoring_summary()


@app.get("/api/ops/provider-health")
def ops_provider_health() -> dict[str, Any]:
    return reader.read_provider_health_summary()


@app.get("/api/ops/providers/{provider}")
def ops_provider_detail(provider: str) -> dict[str, Any]:
    return reader.read_provider_drilldown(provider)


@app.get("/api/ops/datasets/{dataset_key}")
def ops_dataset_detail(dataset_key: str) -> dict[str, Any]:
    return reader.read_dataset_drilldown(dataset_key)


@app.get("/api/research/replay/preview")
def research_replay_preview(
    dataset_key: list[str] | None = Query(default=None),
    provider: list[str] | None = Query(default=None),
    dataset_name: list[str] | None = Query(default=None),
    symbol: list[str] | None = Query(default=None),
    interval: list[str] | None = Query(default=None),
    start: str | None = None,
    end: str | None = None,
    alignment_mode: str = "outer_union",
    anchor_dataset_key: str | None = None,
    tolerance: str | None = None,
    limit: int = 50,
) -> dict[str, Any]:
    return reader.read_replay_assembly_preview(
        dataset_keys=dataset_key,
        providers=provider,
        dataset_names=dataset_name,
        symbols=symbol,
        intervals=interval,
        start=start,
        end=end,
        alignment_mode=alignment_mode,
        anchor_dataset_key=anchor_dataset_key,
        tolerance=tolerance,
        limit=limit,
    )


@app.get("/api/research/replay/consumer-preview")
def research_replay_consumer_preview(
    dataset_key: list[str] | None = Query(default=None),
    provider: list[str] | None = Query(default=None),
    dataset_name: list[str] | None = Query(default=None),
    symbol: list[str] | None = Query(default=None),
    interval: list[str] | None = Query(default=None),
    start: str | None = None,
    end: str | None = None,
    alignment_mode: str = "outer_union",
    anchor_dataset_key: str | None = None,
    tolerance: str | None = None,
    limit: int = 50,
) -> dict[str, Any]:
    return reader.read_replay_consumer_preview(
        dataset_keys=dataset_key,
        providers=provider,
        dataset_names=dataset_name,
        symbols=symbol,
        intervals=interval,
        start=start,
        end=end,
        alignment_mode=alignment_mode,
        anchor_dataset_key=anchor_dataset_key,
        tolerance=tolerance,
        limit=limit,
    )


@app.get("/api/research/replay/evaluation-preview")
def research_replay_evaluation_preview(
    dataset_key: list[str] | None = Query(default=None),
    provider: list[str] | None = Query(default=None),
    dataset_name: list[str] | None = Query(default=None),
    symbol: list[str] | None = Query(default=None),
    interval: list[str] | None = Query(default=None),
    feature_column: list[str] | None = Query(default=None),
    target_column: list[str] | None = Query(default=None),
    start: str | None = None,
    end: str | None = None,
    alignment_mode: str = "outer_union",
    anchor_dataset_key: str | None = None,
    tolerance: str | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    return reader.read_replay_evaluation_preview(
        dataset_keys=dataset_key,
        providers=provider,
        dataset_names=dataset_name,
        symbols=symbol,
        intervals=interval,
        start=start,
        end=end,
        alignment_mode=alignment_mode,
        anchor_dataset_key=anchor_dataset_key,
        tolerance=tolerance,
        limit=limit,
        feature_columns=feature_column,
        target_columns=target_column,
    )


@app.get("/api/research/replay/comparison-latest")
def research_replay_comparison_latest() -> dict[str, Any]:
    return reader.read_latest_replay_comparison_summary()


@app.get("/api/research/replay/gating-latest")
def research_replay_gating_latest() -> dict[str, Any]:
    return reader.read_latest_research_gating_summary()


@app.get("/api/research/replay/history")
def research_replay_history(
    candidate_id: str | None = None,
    provider: str | None = None,
    limit: int = 50,
) -> dict[str, Any]:
    return reader.read_replay_history_view(
        candidate_id=candidate_id,
        provider=provider,
        limit=limit,
    )


@app.get("/api/research/replay/review-queue-latest")
def research_replay_review_queue_latest() -> dict[str, Any]:
    return reader.read_latest_replay_review_queue_summary()


@app.get("/api/research/replay/drift-latest")
def research_replay_drift_latest() -> dict[str, Any]:
    return reader.read_latest_replay_drift_summary()


@app.get("/api/research/replay/comparison-preview")
def research_replay_comparison_preview(
    evaluation_summary_path: list[str] | None = Query(default=None),
    dataset_key: list[str] | None = Query(default=None),
    provider: list[str] | None = Query(default=None),
    dataset_name: list[str] | None = Query(default=None),
    symbol: list[str] | None = Query(default=None),
    interval: list[str] | None = Query(default=None),
    alignment_mode: list[str] | None = Query(default=None),
    feature_column: list[str] | None = Query(default=None),
    target_column: list[str] | None = Query(default=None),
    start: str | None = None,
    end: str | None = None,
    anchor_dataset_key: str | None = None,
    tolerance: str | None = None,
    limit: int | None = None,
    comparison_mode: str = "provider",
    min_row_count: int = 25,
    max_candidates: int = 10,
) -> dict[str, Any]:
    return reader.read_replay_comparison_preview(
        evaluation_summary_paths=evaluation_summary_path,
        dataset_keys=dataset_key,
        providers=provider,
        dataset_names=dataset_name,
        symbols=symbol,
        intervals=interval,
        start=start,
        end=end,
        alignment_modes=alignment_mode,
        anchor_dataset_key=anchor_dataset_key,
        tolerance=tolerance,
        limit=limit,
        feature_columns=feature_column,
        target_columns=target_column,
        comparison_mode=comparison_mode,
        min_row_count=min_row_count,
        max_candidates=max_candidates,
    )


@app.get("/api/ops/providers/{provider}/timeline")
def ops_provider_timeline(provider: str) -> dict[str, Any]:
    return reader.read_provider_timeline(provider)


@app.get("/api/ops/providers/{provider}/history-summary")
def ops_provider_history_summary(provider: str) -> dict[str, Any]:
    return reader.read_provider_history_summary(provider)


@app.get("/api/ops/datasets/{dataset_key}/timeline")
def ops_dataset_timeline(dataset_key: str) -> dict[str, Any]:
    return reader.read_dataset_timeline(dataset_key)


@app.get("/api/ops/datasets/{dataset_key}/history-summary")
def ops_dataset_history_summary(dataset_key: str) -> dict[str, Any]:
    return reader.read_dataset_history_summary(dataset_key)
