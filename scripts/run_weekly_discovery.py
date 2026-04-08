#!/usr/bin/env python
"""
Weekly wallet discovery pipeline.

Runs deep discovery jobs that complement the daily intelligence pipeline:
  1. ingest-top-wallets --window all --limit 2000  (deep all-time)
  2. ingest-top-wallets --window all --limit 500 --by-volume  (volume discovery)
  3. discover-market-wallets --markets 50 --traders 100 --days-back 30
  4. backfill-top-wallets --limit 100
  5. run_daily_intelligence.py  (rebuild profiles + leaderboard)

Writes checkpoint at data/system/weekly_discovery_checkpoint.json so it
can resume after interruption. Logs to data/system/weekly_discovery_runs.jsonl.

Usage:
  python scripts/run_weekly_discovery.py
  python scripts/run_weekly_discovery.py --dry-run
  python scripts/run_weekly_discovery.py --resume   (skip completed steps)
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
SYSTEM_DIR = PROJECT_ROOT / "data" / "system"
CHECKPOINT_PATH = SYSTEM_DIR / "weekly_discovery_checkpoint.json"
RUNS_LOG_PATH = SYSTEM_DIR / "weekly_discovery_runs.jsonl"

STEPS = [
    {
        "key": "deep_alltime",
        "name": "Deep all-time leaderboard ingest (2000 wallets)",
        "command": ["trading-cli", "data", "polymarket", "ingest-top-wallets",
                    "--limit", "2000", "--window", "all"],
        "timeout": 3600,
    },
    {
        "key": "volume_discovery",
        "name": "Volume-based wallet discovery (500 wallets)",
        "command": ["trading-cli", "data", "polymarket", "ingest-top-wallets",
                    "--limit", "500", "--window", "all", "--by-volume"],
        "timeout": 3600,
    },
    {
        "key": "market_discovery",
        "name": "Market-based wallet discovery (50 markets, 100 traders each)",
        "command": ["trading-cli", "data", "polymarket", "discover-market-wallets",
                    "--markets", "50", "--traders", "100", "--days-back", "30"],
        "timeout": 3600,
    },
    {
        "key": "backfill_top",
        "name": "Backfill top 100 wallets by PnL",
        "command": ["trading-cli", "data", "polymarket", "backfill-top-wallets",
                    "--limit", "100"],
        "timeout": 3600,
    },
    {
        "key": "full_universe",
        "name": "Full-coverage market universe refresh",
        "command": ["trading-cli", "data", "polymarket", "refresh-universe", "--full"],
        "timeout": 1800,
    },
    {
        "key": "rebuild_pipeline",
        "name": "Rebuild profiles + leaderboard + universe",
        "command": [sys.executable, "scripts/run_daily_intelligence.py"],
        "timeout": 1800,
    },
]


def _read_checkpoint() -> dict:
    if not CHECKPOINT_PATH.exists():
        return {"completed_steps": [], "run_id": None}
    try:
        return json.loads(CHECKPOINT_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"completed_steps": [], "run_id": None}


def _write_checkpoint(checkpoint: dict) -> None:
    SYSTEM_DIR.mkdir(parents=True, exist_ok=True)
    CHECKPOINT_PATH.write_text(json.dumps(checkpoint, indent=2), encoding="utf-8")


def _clear_checkpoint() -> None:
    if CHECKPOINT_PATH.exists():
        CHECKPOINT_PATH.unlink()


def run_pipeline(dry_run: bool = False, resume: bool = False) -> dict:
    SYSTEM_DIR.mkdir(parents=True, exist_ok=True)

    checkpoint = _read_checkpoint() if resume else {"completed_steps": [], "run_id": None}
    if resume and checkpoint["run_id"]:
        run_id = checkpoint["run_id"]
        print(f"Resuming run {run_id}, completed steps: {checkpoint['completed_steps']}")
    else:
        run_id = str(uuid.uuid4())[:8]
        checkpoint = {"completed_steps": [], "run_id": run_id}

    started_at = datetime.now(tz=timezone.utc).isoformat()
    started_ts = int(time.time())
    t0 = time.time()
    error = None
    completed = list(checkpoint["completed_steps"])

    print(f"{'=' * 60}")
    print(f"Polymarket Weekly Discovery Pipeline — run {run_id}")
    print(f"Started: {started_at}")
    print(f"{'=' * 60}")
    print()

    for i, step in enumerate(STEPS, 1):
        key = step["key"]
        if key in completed:
            print(f"[{i}/{len(STEPS)}] {step['name']} — SKIPPED (already complete)")
            continue

        print(f"[{i}/{len(STEPS)}] {step['name']}...")
        if dry_run:
            print(f"  DRY RUN — would run: {' '.join(step['command'])}")
            completed.append(key)
            continue

        step_t0 = time.time()
        try:
            result = subprocess.run(
                step["command"],
                cwd=str(PROJECT_ROOT),
                timeout=step["timeout"],
                capture_output=False,
            )
            if result.returncode != 0:
                msg = f"exit code {result.returncode}"
                print(f"  FAILED: {msg}")
                error = f"Step {i} ({step['name']}): {msg}"
                break
            dur = round(time.time() - step_t0, 1)
            print(f"  OK ({dur}s)")
            completed.append(key)
            checkpoint["completed_steps"] = completed
            _write_checkpoint(checkpoint)
        except subprocess.TimeoutExpired:
            error = f"Step {i} ({step['name']}): timed out after {step['timeout']}s"
            print(f"  FAILED ({error})")
            break
        except KeyboardInterrupt:
            print()
            print("Interrupted by user — checkpoint saved, use --resume to continue")
            sys.exit(130)
        except Exception as exc:
            error = f"Step {i} ({step['name']}): {exc}"
            print(f"  FAILED ({exc})")
            break

    elapsed = round(time.time() - t0, 1)
    elapsed_fmt = f"{int(elapsed // 60)}m {int(elapsed % 60)}s"
    success = error is None

    print()
    if success:
        print(f"Weekly discovery complete in {elapsed_fmt}")
        if not dry_run:
            _clear_checkpoint()
    else:
        print(f"Weekly discovery FAILED after {elapsed_fmt}")
        print(f"  Error: {error}")
        print(f"  Use --resume to continue from step {len(completed) + 1}")

    summary = {
        "run_id": run_id,
        "started_at": started_ts,
        "completed_at": int(time.time()),
        "duration_seconds": elapsed,
        "steps_completed": len(completed),
        "total_steps": len(STEPS),
        "success": success,
        "error": error,
    }

    if not dry_run:
        with RUNS_LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(summary) + "\n")

    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Weekly wallet discovery pipeline")
    parser.add_argument("--dry-run", action="store_true", help="Print steps without running")
    parser.add_argument("--resume", action="store_true", help="Resume from last checkpoint")
    args = parser.parse_args()

    result = run_pipeline(dry_run=args.dry_run, resume=args.resume)
    sys.exit(0 if result["success"] else 1)


if __name__ == "__main__":
    main()
