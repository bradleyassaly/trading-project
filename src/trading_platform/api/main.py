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
