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

MAX_RESTARTS_PER_HOUR = 10
STALE_THRESHOLD_SECONDS = 600  # 10 minutes
HEALTH_CHECK_INTERVAL = 300  # 5 minutes
RESTART_DELAY = 30


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
    if not WS_STATUS_PATH.exists():
        return True  # No status file yet, assume starting up
    try:
        data = json.loads(WS_STATUS_PATH.read_text(encoding="utf-8"))
        written_at = data.get("written_at", 0)
        age = time.time() - written_at
        if age > STALE_THRESHOLD_SECONDS:
            print(f"  [HEALTH] Connection stale — last event {age:.0f}s ago")
            return False
        markets = data.get("markets_subscribed", 0)
        if markets < 50:
            print(f"  [HEALTH] WARNING: only {markets} markets subscribed")
        return True
    except Exception:
        return True


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

        try:
            while True:
                try:
                    proc.wait(timeout=HEALTH_CHECK_INTERVAL)
                    # Process exited
                    print(f"  [RESTART] Process exited with code {proc.returncode}")
                    break
                except subprocess.TimeoutExpired:
                    # Process still running — check health
                    if not check_health():
                        print("  [RESTART] Killing stale process")
                        proc.terminate()
                        try:
                            proc.wait(timeout=10)
                        except subprocess.TimeoutExpired:
                            proc.kill()
                        break
        except KeyboardInterrupt:
            proc.terminate()
            print("\nMonitor stopped by user")
            sys.exit(0)

        print(f"  Restarting in {RESTART_DELAY}s...")
        time.sleep(RESTART_DELAY)


if __name__ == "__main__":
    main()
