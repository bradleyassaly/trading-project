"""
CLI command: trading-cli data polymarket collect-history

Bootstraps historical data for the backtesting system.
Wrapper around scripts/run_history_collection.py.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[4]


def cmd_polymarket_collect_history(args: argparse.Namespace) -> None:
    cmd = [
        sys.executable,
        str(PROJECT_ROOT / "scripts" / "run_history_collection.py"),
        "--wallets", str(getattr(args, "wallets", 100)),
        "--days-back", str(getattr(args, "days_back", 180)),
    ]
    if getattr(args, "skip_trades", False):
        cmd.append("--skip-trades")
    if getattr(args, "skip_resolution", False):
        cmd.append("--skip-resolution")
    if getattr(args, "skip_prices", False):
        cmd.append("--skip-prices")
    if getattr(args, "skip_reconstruct", False):
        cmd.append("--skip-reconstruct")

    print(f"Running: {' '.join(cmd)}")
    subprocess.run(cmd, cwd=str(PROJECT_ROOT))
