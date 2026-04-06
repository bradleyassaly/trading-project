"""
FastAPI backend for the trading platform GUI.

Starts with:  uvicorn trading_platform.api.main:app --port 8001
Runs alongside the existing Flask dashboard on port 8000.
Reads from the same artifact files — no new database required.
"""
from __future__ import annotations

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
