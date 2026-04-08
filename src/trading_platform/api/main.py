"""
FastAPI backend for the trading platform GUI.

Starts with:  uvicorn trading_platform.api.main:app --port 8001
Runs alongside the existing Flask dashboard on port 8000.
Reads from the same artifact files — no new database required.
"""

from __future__ import annotations

from dotenv import load_dotenv
load_dotenv()


import subprocess
import threading
import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from trading_platform.api import artifact_reader as reader


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
    return reader.read_signals_performance()


@app.get("/api/signals/correlation")
def signals_correlation() -> dict[str, Any]:
    return reader.read_signals_correlation()


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
        return Backtester().run(config, str(WalletDB()._path))
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


@app.post("/api/live/emergency-stop")
def live_emergency_stop(request: dict[str, Any] | None = None) -> dict[str, Any]:
    """Activate the kill-switch flag file. Halts all live trading."""
    try:
        from trading_platform.polymarket.kill_switch import KillSwitch
        from trading_platform.polymarket.wallet_db import WalletDB
        reason = (request or {}).get("reason", "manual via API")
        KillSwitch(str(WalletDB()._path)).emergency_stop(reason)
        return {"ok": True, "reason": reason}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


@app.post("/api/live/clear-stop")
def live_clear_stop() -> dict[str, Any]:
    """Clear the kill-switch flag file."""
    try:
        from trading_platform.polymarket.kill_switch import KillSwitch
        from trading_platform.polymarket.wallet_db import WalletDB
        KillSwitch(str(WalletDB()._path)).clear_emergency_stop()
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
        return PolymarketPaperExecutor().check_and_resolve_open_trades()
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


@app.get("/api/paper/pnl-history")
def paper_pnl_history() -> dict[str, Any]:
    return reader.read_paper_pnl_history()


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
