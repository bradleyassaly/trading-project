"""
In-process task scheduler for the trading platform.

Runs as a long-lived service inside Docker (or as a background
process via PowerShell). Each scheduled task is a small dict with a
``cmd`` (the shell command to invoke), a frequency in seconds, and
the last_run / next_run timestamps. After every run we capture stdout
and stderr to a per-task log file under ``logs/scheduler/`` and write
the result to ``data/scheduler/state.json`` so the health watchdog
and the GUI can see what's happening.

Failures fire a Telegram alert via the existing TelegramAlerter
(non-blocking; if Telegram is unconfigured the failure is just logged).

The schedule below has been verified against the actual CLI commands
that exist (see ``trading-cli`` polymarket subcommand list). Tasks
that reference commands that don't exist yet are commented out with
TODO markers.
"""
from __future__ import annotations

import json
import logging
import os
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass

LOG_DIR = PROJECT_ROOT / "logs" / "scheduler"
STATE_DIR = PROJECT_ROOT / "data" / "scheduler"
STATE_PATH = STATE_DIR / "state.json"
LOG_DIR.mkdir(parents=True, exist_ok=True)
STATE_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("scheduler")


@dataclass
class Task:
    name: str
    cmd: str
    interval_seconds: int
    description: str
    last_run_at: float | None = None
    last_status: str | None = None  # 'ok' | 'failed' | None
    last_duration_s: float | None = None
    last_error: str | None = None
    enabled: bool = True

    def next_run_at(self, now: float) -> float:
        if self.last_run_at is None:
            return now  # run immediately on startup
        return self.last_run_at + self.interval_seconds


# ── Schedule definitions ────────────────────────────────────────────────────
#
# Verified against the actual `trading-cli data polymarket *` commands:
#   backfill, blockchain_ingest, build_leaderboard, clob_fetch,
#   collect_history, daily_refresh, data_api_fetch, diagnose,
#   fetch_resolutions, goldsky_*, ingest, ingest_top_wallets, live,
#   mirror_scan, open_positions, orderflow, performance_review,
#   realtime_monitor, refresh_positions, refresh_universe,
#   smart_money_scan, smart_money_trade, test_telegram, wallet_profiles
#
# Tasks that depend on commands not present in the CLI are commented
# out with a TODO marker until the command is implemented.

SCHEDULE: list[Task] = [
    Task(
        name="paper_resolutions",
        cmd="curl -fsS -X POST http://api:8001/api/paper/check-resolutions",
        interval_seconds=30 * 60,
        description="Resolve any paper trades whose underlying market settled",
    ),
    Task(
        name="data_api_fetch",
        cmd="trading-cli data polymarket data-api-fetch --hours-back 6",
        interval_seconds=2 * 3600,
        description="Pull recent fills from the Polymarket data-api",
    ),
    Task(
        name="refresh_positions",
        cmd="trading-cli data polymarket refresh-positions",
        interval_seconds=3600,
        description="Sync open wallet positions",
    ),
    Task(
        name="open_positions",
        cmd="trading-cli data polymarket open-positions",
        interval_seconds=2 * 3600,
        description="Compute open-position P&L snapshots",
    ),
    Task(
        name="fetch_resolutions",
        cmd="trading-cli data polymarket fetch-resolutions",
        interval_seconds=24 * 3600,
        description="Mark resolved markets in wallet_trades (daily)",
    ),
    Task(
        name="wallet_profiles_rebuild",
        cmd="trading-cli data polymarket wallet-profiles --from-db",
        interval_seconds=24 * 3600,
        description="Recompute wallet metrics from the trade history",
    ),
    Task(
        name="build_leaderboard",
        cmd="trading-cli data polymarket build-leaderboard",
        interval_seconds=24 * 3600,
        description="Rebuild the static tier1h/tier1/tier2 leaderboard",
    ),
    Task(
        name="refresh_universe",
        cmd="trading-cli data polymarket refresh-universe",
        interval_seconds=24 * 3600,
        description="Refresh the active market universe (daily)",
    ),
    Task(
        name="calibration_rebalance",
        cmd="curl -fsS -X POST http://api:8001/api/calibration/rebalance -H 'Content-Type: application/json' -d '{}'",
        interval_seconds=24 * 3600,
        description="Recompute SignalEvaluator + bankroll allocator",
    ),
    Task(
        name="tiers_rebuild",
        cmd="curl -fsS -X POST http://api:8001/api/tiers/rebuild",
        interval_seconds=24 * 3600,
        description="Full WalletTieringEngine rebuild (daily)",
    ),
    Task(
        name="calibration_report",
        cmd="curl -fsS -X POST http://api:8001/api/calibration/report/generate",
        interval_seconds=24 * 3600,
        description="Persist daily calibration report snapshot",
    ),
    Task(
        name="circuit_breaker_daily_reset",
        # Inline Python so we don't depend on a CLI command — clears
        # daily_pnl + daily_halted on the cumulative-drawdown breaker.
        # Drawdown halt itself is NEVER auto-reset.
        cmd=(
            "python -c \""
            "from trading_platform.polymarket.circuit_breaker import CircuitBreaker; "
            "CircuitBreaker().reset_daily()\""
        ),
        interval_seconds=24 * 3600,
        description="Midnight reset of circuit breaker daily loss state",
    ),
    Task(
        name="categories_backfill_weekly",
        cmd="trading-cli data polymarket backfill-categories --no-reclassify-other",
        interval_seconds=7 * 24 * 3600,
        description="Weekly catch-up for any null-category rows",
    ),

    # ── TODO — wire when underlying commands are stable ────────────────
    # Task(
    #     name="signal_engine_loop",
    #     cmd="trading-cli data polymarket realtime-monitor",
    #     interval_seconds=3600,
    #     description="Run the live signal engine for an hour",
    #     enabled=False,
    # ),
    # Task(
    #     name="hot_market_scan",
    #     cmd="trading-cli data polymarket realtime-monitor --hot-scan-only",
    #     interval_seconds=300,
    #     description="HotMarketScanner cycle",
    #     enabled=False,
    # ),
]


def _persist_state(tasks: list[Task]) -> None:
    payload = {
        "updated_at": int(time.time()),
        "tasks": [asdict(t) for t in tasks],
    }
    try:
        STATE_PATH.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    except Exception as exc:
        logger.warning("scheduler state write failed: %s", exc)


def _send_failure_alert(task: Task, error: str) -> None:
    try:
        from trading_platform.polymarket.telegram_alerts import get_alerter
        alerter = get_alerter()
        if not alerter.enabled:
            return
        msg = (
            f"\u26a0\ufe0f <b>SCHEDULER TASK FAILED</b>\n"
            f"Task: <code>{task.name}</code>\n"
            f"Cmd: <code>{task.cmd[:80]}</code>\n"
            f"Error: {error[:300]}\n"
            f"────────\n\U0001f5a5 localhost:5173"
        )
        alerter._send(msg, disable_notification=True)
    except Exception:
        pass


def _run_task(task: Task) -> None:
    started = time.time()
    log_path = LOG_DIR / f"{task.name}.log"
    logger.info("[run] %s — %s", task.name, task.cmd[:80])
    try:
        result = subprocess.run(
            task.cmd,
            shell=True,
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=max(60, task.interval_seconds // 2),
        )
        elapsed = time.time() - started
        ts = datetime.now(tz=timezone.utc).isoformat()
        with log_path.open("a", encoding="utf-8") as f:
            f.write(f"\n=== {ts} (exit={result.returncode}, {elapsed:.1f}s) ===\n")
            f.write(result.stdout or "")
        task.last_run_at = started
        task.last_duration_s = round(elapsed, 1)
        if result.returncode == 0:
            task.last_status = "ok"
            task.last_error = None
            logger.info("[ok ] %s in %.1fs", task.name, elapsed)
        else:
            task.last_status = "failed"
            task.last_error = (result.stdout or "")[-500:]
            logger.warning("[fail] %s exit=%d", task.name, result.returncode)
            _send_failure_alert(task, task.last_error or f"exit {result.returncode}")
    except subprocess.TimeoutExpired:
        task.last_run_at = started
        task.last_status = "failed"
        task.last_error = "timeout"
        task.last_duration_s = time.time() - started
        logger.warning("[timeout] %s", task.name)
        _send_failure_alert(task, "timeout")
    except Exception as exc:
        task.last_run_at = started
        task.last_status = "failed"
        task.last_error = str(exc)[:500]
        task.last_duration_s = time.time() - started
        logger.exception("[error] %s", task.name)
        _send_failure_alert(task, str(exc))


def main() -> None:
    logger.info("scheduler starting with %d tasks", len([t for t in SCHEDULE if t.enabled]))
    _persist_state(SCHEDULE)
    while True:
        now = time.time()
        ran_any = False
        for task in SCHEDULE:
            if not task.enabled:
                continue
            if now >= task.next_run_at(now):
                _run_task(task)
                ran_any = True
        if ran_any:
            _persist_state(SCHEDULE)
        # Sleep until the next due task or 30s, whichever is sooner
        next_due = min(
            (t.next_run_at(now) for t in SCHEDULE if t.enabled),
            default=now + 30,
        )
        sleep_for = max(5.0, min(30.0, next_due - now))
        time.sleep(sleep_for)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("scheduler stopped")
