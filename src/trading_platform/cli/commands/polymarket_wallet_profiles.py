"""
CLI command: trading-cli data polymarket wallet-profiles

Builds wallet performance profiles from blockchain trade history.

Usage
-----
    trading-cli data polymarket wallet-profiles \
      --trades-csv data/polymarket/raw/blockchain_trades.csv \
      --resolution-csv data/polymarket/blockchain/resolution.csv
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[4]


def _project_relative(path_str: str) -> Path:
    p = Path(path_str)
    return p if p.is_absolute() else PROJECT_ROOT / p


def cmd_polymarket_wallet_profiles(args: argparse.Namespace) -> None:
    from trading_platform.polymarket.wallet_profiler import WalletProfiler

    trades_csv = Path(args.trades_csv).expanduser()
    resolution_csv = Path(args.resolution_csv).expanduser()
    output = _project_relative(getattr(args, "output", None) or "data/polymarket/wallet_profiles.parquet")

    if not trades_csv.exists():
        print(f"[ERROR] Trades CSV not found: {trades_csv}")
        return
    if not resolution_csv.exists():
        print(f"[ERROR] Resolution CSV not found: {resolution_csv}")
        return

    print("Polymarket Wallet Profiler")
    print(f"  trades csv   : {trades_csv}")
    print(f"  resolution   : {resolution_csv}")
    print(f"  output       : {output}")
    print()

    profiler = WalletProfiler()
    result = profiler.build_profiles(trades_csv, resolution_csv, output)

    print("[DONE] Wallet profiles built.")
    print(f"  Wallets analyzed            : {result.wallets_analyzed}")
    print(f"  With resolved trades        : {result.wallets_with_resolved_trades}")
    print(f"  Smart money flagged         : {result.smart_money_count}")
    print(f"  Output                      : {result.output_path}")
