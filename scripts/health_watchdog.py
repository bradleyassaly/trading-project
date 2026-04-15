"""
Health watchdog for the trading platform.

Polls the API, scheduler state, and SQLite database every 5 minutes.
On any failure, fires a Telegram alert with specific component
status. Tracks each component's first-failure timestamp in memory so
we don't spam the same alert every cycle — alert once on degradation,
once on recovery.
"""
from __future__ import annotations

import json
import logging
import os
import shutil
import sqlite3
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import urllib.request
import urllib.error

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("watchdog")

API_URL = os.environ.get("WATCHDOG_API_URL", "http://api:8001")
SCHEDULER_STATE = PROJECT_ROOT / "data" / "scheduler" / "state.json"
DB_PATH = PROJECT_ROOT / "data" / "polymarket" / "wallet_intelligence.db"
POLL_INTERVAL_SECONDS = 5 * 60
FREE_DISK_WARN_GB = 5.0
# Dampen: require N consecutive failures before alerting. Stops spurious
# alerts from single-cycle blips (the 03:33 + 04:01 + 16:02 API alerts
# were all 1-cycle false positives during container startup).
CONSECUTIVE_FAILURES_TO_ALERT = 2
# Scheduler tasks are flagged stale only if overdue by more than this
# multiple of their interval. 2x was too tight — a 5-min poller that
# takes 140s on a crowded cycle would trip the alert.
SCHEDULER_STALE_MULTIPLIER = 3


@dataclass
class ComponentState:
    name: str
    healthy: bool
    detail: str
    failed_since: float | None = None


def _http_get(url: str, timeout: float = 5.0) -> tuple[int, str]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return resp.status, resp.read(2048).decode("utf-8", errors="ignore")
    except urllib.error.HTTPError as e:
        return e.code, str(e)
    except Exception as e:
        return 0, str(e)


def check_api() -> ComponentState:
    code, body = _http_get(f"{API_URL}/api/system/status")
    return ComponentState(
        name="api",
        healthy=(code == 200),
        detail=f"HTTP {code}" if code else body[:80],
    )


def check_db() -> ComponentState:
    """Check the canonical Postgres DB (the SQLite file is stale post-cutover).
    Falls back to the SQLite health check only if psycopg isn't installed.
    """
    backend = os.environ.get("DB_BACKEND", "postgres").lower()
    if backend == "postgres":
        try:
            import psycopg
            host = os.environ.get("POSTGRES_HOST", "postgres")
            port = int(os.environ.get("POSTGRES_PORT", "5432"))
            user = os.environ.get("POSTGRES_USER", "polymarket")
            pwd = os.environ.get("POSTGRES_PASSWORD", "polymarket_dev")
            dbn = os.environ.get("POSTGRES_DB", "polymarket")
            with psycopg.connect(host=host, port=port, user=user,
                                 password=pwd, dbname=dbn, connect_timeout=3) as conn:
                cur = conn.execute("SELECT COUNT(*) FROM wallet_trades")
                n = cur.fetchone()[0]
            return ComponentState("db", True, f"{n} wallet_trades (pg)")
        except Exception as e:
            return ComponentState("db", False, f"pg: {str(e)[:80]}")

    if not DB_PATH.exists():
        return ComponentState("db", False, f"missing: {DB_PATH}")
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=2.0)
        try:
            n = conn.execute("SELECT COUNT(*) FROM wallet_trades").fetchone()[0]
        finally:
            conn.close()
        return ComponentState("db", True, f"{n} wallet_trades (sqlite)")
    except Exception as e:
        return ComponentState("db", False, str(e)[:80])


def check_scheduler() -> ComponentState:
    if not SCHEDULER_STATE.exists():
        return ComponentState("scheduler", False, "no state file")
    try:
        data = json.loads(SCHEDULER_STATE.read_text(encoding="utf-8"))
    except Exception as e:
        return ComponentState("scheduler", False, f"bad state: {e}")
    now = time.time()
    stale: list[str] = []
    for t in data.get("tasks", []):
        if not t.get("enabled", True):
            continue
        last_run = t.get("last_run_at")
        interval = t.get("interval_seconds", 0)
        if last_run is None:
            continue  # not yet run, OK on first cycle
        # Stale if more than 2x its interval overdue
        if interval and now - float(last_run) > SCHEDULER_STALE_MULTIPLIER * interval:
            stale.append(t["name"])
    if stale:
        return ComponentState(
            "scheduler", False, f"stale tasks: {', '.join(stale[:5])}",
        )
    return ComponentState("scheduler", True, f"{len(data.get('tasks', []))} tasks tracked")


def check_disk() -> ComponentState:
    try:
        free_bytes = shutil.disk_usage(str(PROJECT_ROOT)).free
        free_gb = free_bytes / (1024 ** 3)
        if free_gb < FREE_DISK_WARN_GB:
            return ComponentState("disk", False, f"only {free_gb:.1f}GB free")
        return ComponentState("disk", True, f"{free_gb:.0f}GB free")
    except Exception as e:
        return ComponentState("disk", False, str(e)[:80])


def _send_alert(message: str, *, loud: bool = False) -> None:
    try:
        from trading_platform.polymarket.telegram_alerts import get_alerter
        alerter = get_alerter()
        if alerter.enabled:
            alerter._send(message, disable_notification=not loud)
    except Exception:
        pass


def format_status(states: list[ComponentState]) -> str:
    icons = {True: "\u2705", False: "\u274c"}
    lines = []
    for s in states:
        lines.append(f"{icons[s.healthy]} {s.name}: {s.detail}")
    return "\n".join(lines)


def main() -> None:
    logger.info("health watchdog starting (poll interval %ds, requires %d consecutive failures)",
                POLL_INTERVAL_SECONDS, CONSECUTIVE_FAILURES_TO_ALERT)
    failure_state: dict[str, float] = {}  # component_name → first_failure_ts
    fail_count: dict[str, int] = {}       # consecutive-failure counter
    while True:
        now = time.time()
        states = [check_api(), check_db(), check_scheduler(), check_disk()]

        # Compute new failures and recoveries. Require N consecutive cycles
        # to avoid alerting on single-cycle blips (container restart, slow
        # healthcheck on startup, etc.).
        for s in states:
            if not s.healthy:
                fail_count[s.name] = fail_count.get(s.name, 0) + 1
            else:
                fail_count.pop(s.name, None)

            if not s.healthy and fail_count.get(s.name, 0) >= CONSECUTIVE_FAILURES_TO_ALERT and s.name not in failure_state:
                failure_state[s.name] = now
                msg = (
                    f"\U0001f534 <b>SYSTEM ALERT</b>\n"
                    f"Component <b>{s.name}</b> failing\n"
                    f"Detail: {s.detail[:200]}\n\n"
                    + format_status(states)
                )
                logger.warning("[alert] %s failing: %s", s.name, s.detail)
                _send_alert(msg, loud=True)
            elif s.healthy and s.name in failure_state:
                duration_min = (now - failure_state[s.name]) / 60
                msg = (
                    f"\U0001f7e2 <b>RECOVERED</b>\n"
                    f"<b>{s.name}</b> back online (was down {duration_min:.0f} min)\n\n"
                    + format_status(states)
                )
                logger.info("[recover] %s back online", s.name)
                _send_alert(msg, loud=False)
                failure_state.pop(s.name, None)

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("watchdog stopped")
