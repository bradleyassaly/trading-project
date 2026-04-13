"""CLI command: trading-cli data polymarket poll-wallet-trades"""
from __future__ import annotations

import argparse
from pathlib import Path


def register(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser(
        "poll-wallet-trades",
        help="Poll Data API for recent trades by copyable wallets and fire signals",
    )
    p.add_argument("--db", default=None, help="Path to wallet_intelligence.db")
    p.add_argument("--dry-run", action="store_true", help="Detect trades but don't fire signals")
    p.set_defaults(func=_run)


def _run(args: argparse.Namespace) -> None:
    from trading_platform.polymarket.wallet_trade_poller import main

    db_path = getattr(args, "db", None)
    dry_run = getattr(args, "dry_run", False)
    main(db_path=db_path, dry_run=dry_run)
