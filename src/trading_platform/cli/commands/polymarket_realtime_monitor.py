# DEPRECATED: This module used EWR-based smart money detection.
# Replaced by whale_tripwire.py + whale_signal_engine.py (9-signal system).
# Do not run or import this module in new code.
# Preserved for reference only.
"""CLI: trading-cli data polymarket monitor-realtime"""
from __future__ import annotations

import argparse
import asyncio
import logging


def cmd_polymarket_monitor_realtime(args: argparse.Namespace) -> None:
    from trading_platform.polymarket.realtime_monitor import RealtimeMonitor
    from trading_platform.polymarket.alert_system import AlertSystem

    min_fill = getattr(args, "min_fill", 100)
    min_ewr = getattr(args, "min_ewr", 0.70)
    auto_trade = getattr(args, "auto_trade", False)
    tier1_only = getattr(args, "tier1_only", False)

    alert_system = AlertSystem()

    paper_executor = None
    if auto_trade:
        from trading_platform.polymarket.polymarket_paper_executor import PolymarketPaperExecutor
        paper_executor = PolymarketPaperExecutor()

    monitor = RealtimeMonitor(
        alert_system=alert_system,
        paper_executor=paper_executor,
        min_fill_usdc=min_fill,
        min_ewr=min_ewr,
        auto_trade=auto_trade,
        tier1_only=tier1_only,
    )

    try:
        asyncio.run(monitor.run())
    except KeyboardInterrupt:
        print("\nMonitor stopped.")
