# DEPRECATED: This module used EWR-based smart money detection.
# Replaced by whale_tripwire.py + whale_signal_engine.py (9-signal system).
# Do not run or import this module in new code.
# Preserved for reference only.
"""CLI: trading-cli data polymarket smart-money-trade"""
from __future__ import annotations

import argparse
import logging

logger = logging.getLogger(__name__)


def cmd_polymarket_smart_money_trade(args: argparse.Namespace) -> None:
    from trading_platform.kalshi.paper_executor import KalshiPaperExecutor
    from trading_platform.polymarket.smart_money_loop import run_smart_money_cycle

    executor = KalshiPaperExecutor()
    summary = run_smart_money_cycle(executor)

    print("Smart Money Trade Cycle")
    print(f"  Smart wallets   : {summary['smart_wallets']}")
    print(f"  Fills loaded    : {summary['fills_loaded']:,}")
    print(f"  Signals found   : {summary['signals_found']}")
    print(f"  Qualifying      : {summary['signals_qualifying']}")
    print(f"  Trades placed   : {summary['trades_placed']}")
    if summary["errors"]:
        print(f"  Errors          : {len(summary['errors'])}")

    port = executor.get_summary()
    print(f"\n  Portfolio: cash=${port['cash_usd']:.2f} total=${port['total_value']:.2f} "
          f"trades={port['total_trades']} wr={port['win_rate']:.1%}")
    executor.close()
