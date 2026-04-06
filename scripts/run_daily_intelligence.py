#!/usr/bin/env python
"""
Daily intelligence pipeline for Polymarket wallet tracking.

Runs the complete pipeline in sequence:
  1. data-api-fetch (accumulate new trades)
  2. wallet-profiles --from-db (rebuild directional WR)
  3. classify-wallet-buckets
  4. build-leaderboard
  5. refresh-universe
  6. performance-review (optional)

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

STEPS = [
    {
        "name": "Fetching new trades",
        "command": ["trading-cli", "data", "polymarket", "data-api-fetch", "--hours-back", "6"],
        "timeout": 300,
        "fatal": True,
    },
    {
        "name": "Rebuilding wallet profiles",
        "command": ["trading-cli", "data", "polymarket", "wallet-profiles", "--from-db"],
        "timeout": 600,
        "fatal": True,
    },
    {
        "name": "Classifying buckets",
        "command": ["trading-cli", "data", "polymarket", "classify-wallet-buckets"],
        "timeout": 300,
        "fatal": False,
    },
    {
        "name": "Building leaderboard",
        "command": ["trading-cli", "data", "polymarket", "build-leaderboard"],
        "timeout": 300,
        "fatal": True,
    },
    {
        "name": "Refreshing market universe",
        "command": ["trading-cli", "data", "polymarket", "refresh-universe"],
        "timeout": 180,
        "fatal": False,
    },
]


def run_pipeline(dry_run: bool = False) -> dict:
    run_id = str(uuid.uuid4())[:8]
    started_at = datetime.now(tz=timezone.utc).isoformat()
    t0 = time.time()
    steps_completed = 0
    error = None

    print(f"{'=' * 60}")
    print(f"Polymarket Intelligence Pipeline — run {run_id}")
    print(f"Started: {started_at}")
    print(f"{'=' * 60}")
    print()

    for i, step in enumerate(STEPS, 1):
        label = step["name"]
        print(f"[{i}/{len(STEPS)}] {label}...", end=" ", flush=True)

        if dry_run:
            print(f"DRY RUN — would run: {' '.join(step['command'])}")
            steps_completed += 1
            continue

        try:
            result = subprocess.run(
                step["command"],
                capture_output=True,
                text=True,
                timeout=step["timeout"],
                cwd=str(PROJECT_ROOT),
            )
            if result.returncode != 0:
                stderr = (result.stderr or "").strip()[-200:]
                stdout = (result.stdout or "").strip()[-200:]
                msg = stderr or stdout or f"exit code {result.returncode}"
                if step["fatal"]:
                    print(f"FAILED")
                    print(f"  Error: {msg}")
                    error = f"Step {i} ({label}) failed: {msg}"
                    break
                else:
                    print(f"WARN ({msg[:60]})")
                    steps_completed += 1
                    continue
            # Extract useful info from stdout
            stdout = (result.stdout or "").strip()
            last_lines = stdout.split("\n")[-3:]
            summary = " | ".join(l.strip() for l in last_lines if l.strip())[:80]
            print(f"OK ({summary})" if summary else "OK")
            steps_completed += 1
        except subprocess.TimeoutExpired:
            msg = f"Timed out after {step['timeout']}s"
            if step["fatal"]:
                print(f"FAILED ({msg})")
                error = f"Step {i} ({label}): {msg}"
                break
            else:
                print(f"WARN ({msg})")
                steps_completed += 1
        except Exception as exc:
            if step["fatal"]:
                print(f"FAILED ({exc})")
                error = f"Step {i} ({label}): {exc}"
                break
            else:
                print(f"WARN ({exc})")
                steps_completed += 1

    elapsed = round(time.time() - t0, 1)
    elapsed_fmt = f"{int(elapsed // 60)}m {int(elapsed % 60)}s"
    success = error is None

    print()
    if success:
        print(f"Pipeline complete in {elapsed_fmt}")
    else:
        print(f"Pipeline FAILED at step {steps_completed + 1} after {elapsed_fmt}")
        print(f"  Error: {error}")

    # Write run summary
    summary = {
        "run_id": run_id,
        "started_at": started_at,
        "completed_at": datetime.now(tz=timezone.utc).isoformat(),
        "steps_completed": steps_completed,
        "total_steps": len(STEPS),
        "success": success,
        "error": error,
        "elapsed_seconds": elapsed,
    }

    if not dry_run:
        runs_path = PROJECT_ROOT / "data" / "polymarket" / "pipeline_runs.jsonl"
        runs_path.parent.mkdir(parents=True, exist_ok=True)
        with runs_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(summary) + "\n")

    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Polymarket daily intelligence pipeline")
    parser.add_argument("--dry-run", action="store_true", help="Print steps without running")
    args = parser.parse_args()

    result = run_pipeline(dry_run=args.dry_run)
    sys.exit(0 if result["success"] else 1)


if __name__ == "__main__":
    main()
