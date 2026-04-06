"""CLI: trading-cli data polymarket compute-open-positions"""
from __future__ import annotations

import argparse
from pathlib import Path


def cmd_polymarket_compute_open_positions(args: argparse.Namespace) -> None:
    from trading_platform.polymarket.open_positions import compute_open_positions

    min_edge = float(getattr(args, "min_edge", None) or 0.20)
    output = "data/polymarket/wallet_open_positions.parquet"

    print(f"Computing open positions (min_edge={min_edge:.2f})...")
    df = compute_open_positions(min_edge=min_edge, output_path=output)

    if df.empty:
        print("[DONE] No open positions found.")
        return

    print(f"[DONE] {len(df)} open positions across {df['wallet'].nunique()} wallets")
    print(f"  Output: {output}")
    print()

    # Top positions by volume
    print(f"  {'Wallet':<16} {'Token':<24} {'Side':>4} {'$Net':>10} {'Edge':>6}")
    for _, r in df.head(15).iterrows():
        q = (r.get("market_question") or r["token_id"])[:24]
        print(f"  {r['wallet'][:16]:<16} {q:<24} {r['net_side']:>4} "
              f"${r['net_amount_usdc']:>9,.0f} {r['wallet_edge']:>6.1%}")
