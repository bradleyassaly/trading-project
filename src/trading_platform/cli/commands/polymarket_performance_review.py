"""
CLI command: trading-cli data polymarket performance-review

Runs advisory performance review on signal and category performance.
"""
from __future__ import annotations

import argparse


def cmd_polymarket_performance_review(args: argparse.Namespace) -> None:
    from trading_platform.polymarket.performance_monitor import run_performance_review

    print("Running performance review...")
    run_performance_review()
