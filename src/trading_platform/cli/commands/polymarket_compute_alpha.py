"""CLI: trading-cli data polymarket compute-alpha-scores"""
from __future__ import annotations

import argparse


def cmd_polymarket_compute_alpha(args: argparse.Namespace) -> None:
    from trading_platform.polymarket.alpha_scores import compute_alpha_scores
    from trading_platform.polymarket.wallet_db import WalletDB

    db_path = str(WalletDB()._path)
    print(f"Computing alpha scores from {db_path}...")
    result = compute_alpha_scores(db_path)
    print(
        f"[DONE] scored={result['scored']} "
        f"copyable={result['copyable']} "
        f"elapsed={result['elapsed_seconds']}s"
    )
