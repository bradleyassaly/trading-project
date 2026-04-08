#!/usr/bin/env python
"""
Continuous monitor startup script with health checks and auto-restart.

Starts the Polymarket live-collect WebSocket process with whale detection.
Monitors health via ws_status.json and restarts on stale connections.

Usage:
  python scripts/run_live_monitor.py
  python scripts/run_live_monitor.py --once   (no restart loop)
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
WS_STATUS_PATH = PROJECT_ROOT / "data" / "polymarket" / "ws_status.json"
UNIVERSE_PATH = PROJECT_ROOT / "data" / "polymarket" / "market_universe.json"
DB_PATH = PROJECT_ROOT / "data" / "polymarket" / "wallet_intelligence.db"

MONITOR_STATUS_PATH = PROJECT_ROOT / "data" / "system" / "monitor_status.json"

MAX_RESTARTS_PER_HOUR = 10
STALE_THRESHOLD_SECONDS = 600  # 10 minutes
HEALTH_CHECK_INTERVAL = 60  # 1 minute (also writes monitor status)
RESTART_DELAY = 30

_started_at = int(time.time())
_restarts_today = 0
_consecutive_failures = 0


def check_prerequisites() -> list[str]:
    """Check system state before starting. Returns list of warnings."""
    warnings = []

    if not DB_PATH.exists():
        warnings.append(f"wallet_intelligence.db not found at {DB_PATH}")

    if UNIVERSE_PATH.exists():
        try:
            data = json.loads(UNIVERSE_PATH.read_text(encoding="utf-8"))
            age_hours = (time.time() - data.get("refreshed_at", 0)) / 3600
            if age_hours > 12:
                warnings.append(f"Market universe is {age_hours:.0f}h old — refreshing...")
                subprocess.run(
                    ["trading-cli", "data", "polymarket", "refresh-universe"],
                    cwd=str(PROJECT_ROOT),
                    timeout=180,
                )
        except Exception as exc:
            warnings.append(f"Could not check universe age: {exc}")
    else:
        warnings.append("No market_universe.json — will refresh on startup")
        try:
            subprocess.run(
                ["trading-cli", "data", "polymarket", "refresh-universe"],
                cwd=str(PROJECT_ROOT),
                timeout=180,
            )
        except Exception:
            pass

    # Check leaderboard validity
    if DB_PATH.exists():
        try:
            import sqlite3
            conn = sqlite3.connect(str(DB_PATH))
            meta = conn.execute("SELECT is_valid, wallets_tier1, wallets_tier2 FROM leaderboard_meta WHERE id=1").fetchone()
            conn.close()
            if meta:
                if not meta[0]:
                    warnings.append("Leaderboard is_valid=0 — will watch 0 wallets initially")
                else:
                    print(f"  Leaderboard: tier1={meta[1]} tier2={meta[2]} (valid)")
            else:
                warnings.append("No leaderboard_meta — run build-leaderboard first")
        except Exception:
            pass

    return warnings


def check_health() -> bool:
    """Check ws_status.json for stale connection. Returns True if healthy."""
    _write_monitor_status()

    if not WS_STATUS_PATH.exists():
        return True
    try:
        data = json.loads(WS_STATUS_PATH.read_text(encoding="utf-8"))
        written_at = data.get("written_at", 0)
        age = time.time() - written_at
        if age > STALE_THRESHOLD_SECONDS:
            print(f"  [HEALTH] Connection stale - last event {age:.0f}s ago")
            return False
        markets = data.get("markets_subscribed", 0)
        if markets < 50:
            print(f"  [HEALTH] WARNING: only {markets} markets subscribed")
        return True
    except Exception:
        return True


def _write_monitor_status() -> None:
    """Write current monitor state to data/system/monitor_status.json."""
    global _restarts_today
    MONITOR_STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
    now = int(time.time())

    # Read ws_status for WebSocket info
    ws_info = {"connected": False, "markets_subscribed": 0, "last_event_ts": 0}
    if WS_STATUS_PATH.exists():
        try:
            ws_data = json.loads(WS_STATUS_PATH.read_text(encoding="utf-8"))
            ws_info = {
                "connected": ws_data.get("connected", False),
                "markets_subscribed": ws_data.get("markets_subscribed", 0),
                "last_event_ts": ws_data.get("last_event_ts", 0),
                "last_event_age_seconds": now - (ws_data.get("last_event_ts") or now),
            }
        except Exception:
            pass

    status = {
        "started_at": _started_at,
        "last_heartbeat": now,
        "restarts_today": _restarts_today,
        "websocket": ws_info,
        "tier1_poll": {
            "last_poll_at": None,
            "last_poll_age_seconds": None,
            "wallets_polled": 0,
            "last_poll_success": True,
        },
        "signals": {
            "fired_today": ws_data.get("signals_today", 0) if WS_STATUS_PATH.exists() else 0,
            "last_signal_at": None,
        },
    }
    try:
        MONITOR_STATUS_PATH.write_text(json.dumps(status, indent=2), encoding="utf-8")
    except Exception:
        pass


def run_collector() -> subprocess.Popen:
    """Start the live-collect process."""
    print(f"  Starting live-collect at {datetime.now(tz=timezone.utc).isoformat()}")
    proc = subprocess.Popen(
        ["trading-cli", "data", "polymarket", "live-collect",
         "--config", "configs/polymarket.yaml"],
        cwd=str(PROJECT_ROOT),
        stdout=sys.stdout,
        stderr=sys.stderr,
    )
    return proc


def main() -> None:
    global _restarts_today, _consecutive_failures
    parser = argparse.ArgumentParser(description="Polymarket continuous monitor")
    parser.add_argument("--once", action="store_true", help="Run without restart loop")
    args = parser.parse_args()

    print("Polymarket Live Monitor")
    print("=" * 40)

    warnings = check_prerequisites()
    for w in warnings:
        print(f"  [WARN] {w}")

    if args.once:
        print("  Mode: single run (--once)")
        proc = run_collector()
        try:
            proc.wait()
        except KeyboardInterrupt:
            proc.terminate()
        sys.exit(proc.returncode or 0)

    # Restart loop
    print("  Mode: auto-restart")
    restart_times: list[float] = []

    while True:
        # Check restart rate
        now = time.time()
        restart_times = [t for t in restart_times if now - t < 3600]
        if len(restart_times) >= MAX_RESTARTS_PER_HOUR:
            print(f"[FATAL] {MAX_RESTARTS_PER_HOUR} restarts in last hour — stopping")
            sys.exit(1)

        proc = run_collector()
        restart_times.append(now)
        _consecutive_failures = 0

        try:
            while True:
                try:
                    proc.wait(timeout=HEALTH_CHECK_INTERVAL)
                    print(f"  [RESTART] Process exited with code {proc.returncode}")
                    _restarts_today += 1
                    _consecutive_failures += 1
                    if _consecutive_failures >= 3:
                        try:
                            from trading_platform.polymarket.telegram_alerts import get_alerter
                            get_alerter().send_pipeline_alert("websocket", f"Failed to reconnect ({_consecutive_failures} attempts)", "critical")
                        except Exception:
                            pass
                    break
                except subprocess.TimeoutExpired:
                    if not check_health():
                        print("  [RESTART] Killing stale process")
                        proc.terminate()
                        try:
                            proc.wait(timeout=10)
                        except subprocess.TimeoutExpired:
                            proc.kill()
                        _restarts_today += 1
                        break
        except KeyboardInterrupt:
            proc.terminate()
            print("\nMonitor stopped by user")
            sys.exit(0)

        print(f"  Restarting in {RESTART_DELAY}s...")
        time.sleep(RESTART_DELAY)


if __name__ == "__main__":
    main()
