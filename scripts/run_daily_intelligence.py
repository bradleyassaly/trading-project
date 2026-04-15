#!/usr/bin/env python
"""
Daily intelligence pipeline for Polymarket wallet tracking.

Runs the complete pipeline in sequence, writes detailed status to
data/system/pipeline_status.json and appends to pipeline_runs.jsonl.

Usage:
  python scripts/run_daily_intelligence.py
  python scripts/run_daily_intelligence.py --dry-run
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
STATUS_DIR = PROJECT_ROOT / "data" / "system"
PIPELINE_STATUS_PATH = STATUS_DIR / "pipeline_status.json"
PIPELINE_RUNS_PATH = STATUS_DIR / "pipeline_runs.jsonl"

STEP_KEYS = ["fetch_resolutions", "resolve_signals", "ingest_daily", "ingest_weekly", "ingest_monthly", "data_fetch", "profile_rebuild", "classify_buckets", "build_leaderboard", "refresh_universe"]

STEPS = [
    {"key": "fetch_resolutions", "name": "Fetching gamma market resolutions",
     "command": ["trading-cli", "data", "polymarket", "fetch-resolutions"],
     "timeout": 600, "fatal": False},
    {"key": "resolve_signals", "name": "Resolving signal outcomes",
     "command": [sys.executable, "-m", "trading_platform.polymarket.signal_resolver"],
     "timeout": 300, "fatal": False},
    {"key": "ingest_daily", "name": "Ingesting daily top wallets",
     "command": ["trading-cli", "data", "polymarket", "ingest-top-wallets",
                 "--limit", "50", "--window", "1d"],
     "timeout": 1800, "fatal": False},
    {"key": "ingest_weekly", "name": "Ingesting weekly top wallets",
     "command": ["trading-cli", "data", "polymarket", "ingest-top-wallets",
                 "--limit", "50", "--window", "7d"],
     "timeout": 1800, "fatal": False},
    {"key": "ingest_monthly", "name": "Ingesting monthly top wallets",
     "command": ["trading-cli", "data", "polymarket", "ingest-top-wallets",
                 "--limit", "50", "--window", "30d"],
     "timeout": 1800, "fatal": False},
    {"key": "data_fetch", "name": "Fetching new trades",
     "command": ["trading-cli", "data", "polymarket", "data-api-fetch", "--hours-back", "6"],
     "timeout": 300, "fatal": True},
    {"key": "profile_rebuild", "name": "Rebuilding wallet profiles",
     "command": ["trading-cli", "data", "polymarket", "wallet-profiles", "--from-db"],
     "timeout": 600, "fatal": True},
    {"key": "classify_buckets", "name": "Classifying buckets",
     "command": ["trading-cli", "data", "polymarket", "classify-wallet-buckets"],
     "timeout": 300, "fatal": False},
    {"key": "build_leaderboard", "name": "Building leaderboard",
     "command": ["trading-cli", "data", "polymarket", "build-leaderboard"],
     "timeout": 300, "fatal": True},
    {"key": "refresh_universe", "name": "Refreshing market universe",
     "command": ["trading-cli", "data", "polymarket", "refresh-universe"],
     "timeout": 900, "fatal": False},
]


def _try_telegram(component: str, message: str, level: str = "critical") -> None:
    try:
        from trading_platform.polymarket.telegram_alerts import get_alerter
        get_alerter().send_pipeline_alert(component, message, level)
    except Exception:
        pass


def run_pipeline(dry_run: bool = False) -> dict:
    run_id = str(uuid.uuid4())[:8]
    started_ts = int(time.time())
    started_iso = datetime.now(tz=timezone.utc).isoformat()
    t0 = time.time()
    steps_completed = 0
    error = None
    step_results: dict[str, dict] = {}

    STATUS_DIR.mkdir(parents=True, exist_ok=True)

    print(f"{'=' * 60}")
    print(f"Polymarket Intelligence Pipeline - run {run_id}")
    print(f"Started: {started_iso}")
    print(f"{'=' * 60}")
    print()

    for i, step in enumerate(STEPS, 1):
        key = step["key"]
        label = step["name"]
        step_t0 = time.time()
        print(f"[{i}/{len(STEPS)}] {label}...", end=" ", flush=True)

        if dry_run:
            print(f"DRY RUN - would run: {' '.join(step['command'])}")
            step_results[key] = {"status": "skipped", "detail": "dry run"}
            steps_completed += 1
            continue

        try:
            result = subprocess.run(
                step["command"], capture_output=True, text=True,
                timeout=step["timeout"], cwd=str(PROJECT_ROOT),
            )
            step_dur = round(time.time() - step_t0, 1)
            stdout = (result.stdout or "").strip()
            last_line = stdout.split("\n")[-1].strip()[:120] if stdout else ""

            if result.returncode != 0:
                stderr = (result.stderr or "").strip()[-200:]
                msg = stderr or last_line or f"exit code {result.returncode}"
                step_results[key] = {
                    "status": "failed", "started_at": int(step_t0),
                    "completed_at": int(time.time()), "duration_seconds": step_dur,
                    "detail": None, "error": msg[:200],
                }
                if step["fatal"]:
                    print(f"FAILED")
                    print(f"  Error: {msg[:100]}")
                    error = f"Step {i} ({label}) failed: {msg[:100]}"
                    _try_telegram(key, error, "critical")
                    break
                else:
                    print(f"WARN ({msg[:60]})")
                    steps_completed += 1
                    continue

            step_results[key] = {
                "status": "ok", "started_at": int(step_t0),
                "completed_at": int(time.time()), "duration_seconds": step_dur,
                "detail": last_line, "error": None,
            }
            print(f"OK ({last_line[:60]})" if last_line else "OK")
            steps_completed += 1

        except subprocess.TimeoutExpired:
            step_dur = round(time.time() - step_t0, 1)
            msg = f"Timed out after {step['timeout']}s"
            step_results[key] = {
                "status": "failed", "started_at": int(step_t0),
                "completed_at": int(time.time()), "duration_seconds": step_dur,
                "detail": None, "error": msg,
            }
            if step["fatal"]:
                print(f"FAILED ({msg})")
                error = f"Step {i} ({label}): {msg}"
                _try_telegram(key, error, "critical")
                break
            else:
                print(f"WARN ({msg})")
                steps_completed += 1

        except Exception as exc:
            step_dur = round(time.time() - step_t0, 1)
            step_results[key] = {
                "status": "failed", "started_at": int(step_t0),
                "completed_at": int(time.time()), "duration_seconds": step_dur,
                "detail": None, "error": str(exc)[:200],
            }
            if step["fatal"]:
                print(f"FAILED ({exc})")
                error = f"Step {i} ({label}): {exc}"
                _try_telegram(key, str(exc)[:100], "critical")
                break
            else:
                print(f"WARN ({exc})")
                steps_completed += 1

        # Write intermediate status
        _write_status(run_id, started_ts, step_results, steps_completed, None)

    elapsed = round(time.time() - t0, 1)
    elapsed_fmt = f"{int(elapsed // 60)}m {int(elapsed % 60)}s"
    success = error is None

    print()
    if success:
        print(f"Pipeline complete in {elapsed_fmt}")
    else:
        print(f"Pipeline FAILED at step {steps_completed + 1} after {elapsed_fmt}")
        print(f"  Error: {error}")

    # Write final status
    _write_status(run_id, started_ts, step_results, steps_completed, error)

    # Append to runs log
    summary = {
        "run_id": run_id,
        "started_at": started_ts,
        "completed_at": int(time.time()),
        "duration_seconds": elapsed,
        "steps_completed": steps_completed,
        "total_steps": len(STEPS),
        "success": success,
        "error": error,
    }
    if not dry_run:
        with PIPELINE_RUNS_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(summary) + "\n")

    # Send daily Telegram update on success
    if success and not dry_run:
        try:
            from trading_platform.polymarket.telegram_alerts import get_alerter
            alerter = get_alerter()

            # Open paper positions (with current prices) — prefer the live API
            # if it's running, fall back to direct DB read otherwise.
            positions: list = []
            try:
                import urllib.request as _ur
                with _ur.urlopen(
                    "http://localhost:8001/api/paper/positions-enriched",
                    timeout=10,
                ) as resp:
                    positions = json.loads(resp.read()).get("positions", [])
            except Exception:
                try:
                    from trading_platform.api import artifact_reader as _r
                    positions = _r.read_paper_positions_enriched().get("positions", [])
                except Exception:
                    positions = []

            # Today's signal / whale / hot-market counts straight from the DB.
            signals_today = 0
            whales_today = 0
            markets_discovered = 0
            try:
                import sqlite3 as _sq
                from trading_platform.polymarket.wallet_db import WalletDB
                db_path = str(WalletDB()._path)
                day_start = int(time.time()) - 86400
                _conn = _sq.connect(db_path)
                try:
                    signals_today = _conn.execute(
                        "SELECT COUNT(*) FROM market_signals WHERE fired_at >= ?",
                        (day_start,),
                    ).fetchone()[0]
                except Exception:
                    pass
                try:
                    whales_today = _conn.execute(
                        "SELECT COUNT(*) FROM wallet_alerts WHERE trade_ts >= ? AND tier = 1",
                        (day_start,),
                    ).fetchone()[0]
                except Exception:
                    pass
                try:
                    markets_discovered = _conn.execute(
                        """SELECT COUNT(DISTINCT condition_id) FROM market_anomalies
                           WHERE detected_at >= ? AND severity IN ('high', 'critical')""",
                        (day_start,),
                    ).fetchone()[0]
                except Exception:
                    pass
                _conn.close()
            except Exception:
                pass

            alerter.send_daily_update(positions, {
                "signals_today": signals_today,
                "whales_today": whales_today,
                "markets_discovered": markets_discovered,
                "pipeline_success": True,
                "pipeline_duration": elapsed,
            })
        except Exception:
            pass

    return summary


def _write_status(run_id: str, started_ts: int, steps: dict, completed: int, error: str | None) -> None:
    status = {
        "run_id": run_id,
        "started_at": started_ts,
        "completed_at": int(time.time()),
        "success": error is None,
        "duration_seconds": round(time.time() - started_ts, 1),
        "steps": steps,
        "next_run_at": started_ts + 4 * 3600,
    }
    PIPELINE_STATUS_PATH.write_text(json.dumps(status, indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Polymarket daily intelligence pipeline")
    parser.add_argument("--dry-run", action="store_true", help="Print steps without running")
    args = parser.parse_args()
    result = run_pipeline(dry_run=args.dry_run)
    sys.exit(0 if result["success"] else 1)


if __name__ == "__main__":
    main()
