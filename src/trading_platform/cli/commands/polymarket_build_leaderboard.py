"""
CLI command: trading-cli data polymarket build-leaderboard

Rebuilds the wallet leaderboard from profiles and trade data.
"""
from __future__ import annotations

import argparse


def cmd_polymarket_build_leaderboard(args: argparse.Namespace) -> None:
    from trading_platform.polymarket.leaderboard import build_leaderboard
    from trading_platform.polymarket.wallet_db import WalletDB

    print("Building wallet leaderboard...")
    result = build_leaderboard(WalletDB())

    if not result.get("committed"):
        print("[ERROR] Leaderboard build produced 0 wallets.")
        print("  Run wallet-profiles --from-db first to populate profiles.")
        raise SystemExit(1)

    print(f"[DONE] Leaderboard v{result['version']} committed.")
    print(f"  Tier 1 wallets : {result['tier1']}")
    print(f"  Tier 2 wallets : {result['tier2']}")
    print(f"  Demoted        : {result['demoted']}")
    print(f"  Insufficient   : {result['insufficient']}")
    print(f"  Elapsed        : {result['elapsed_seconds']}s")
