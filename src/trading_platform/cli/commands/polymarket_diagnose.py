"""
CLI: trading-cli data polymarket diagnose-resolution
"""
from __future__ import annotations

import argparse
import csv
import logging
from pathlib import Path

logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[4]


def cmd_polymarket_diagnose_resolution(args: argparse.Namespace) -> None:
    from trading_platform.polymarket.resolution_resolver import ResolutionResolver, normalize_token_id

    resolution_path = PROJECT_ROOT / "data" / "polymarket" / "blockchain" / "resolution.csv"
    trades_dir = PROJECT_ROOT / "data" / "polymarket" / "data_api_trades"

    print("Polymarket Resolution Diagnostics")
    print(f"  resolution CSV : {resolution_path}")
    print(f"  trades dir     : {trades_dir}")
    print()

    # Load resolver
    resolver = ResolutionResolver(resolution_path)
    print(f"  Resolutions loaded: {resolver.count}")

    # Collect token_ids from trade CSVs
    trade_tokens: set[str] = set()
    if trades_dir.exists():
        for csv_file in trades_dir.glob("*.csv"):
            try:
                with csv_file.open(newline="", encoding="utf-8-sig") as fh:
                    for row in csv.DictReader(fh):
                        tid = row.get("token_id") or row.get("asset") or ""
                        if tid.strip():
                            trade_tokens.add(tid.strip())
            except Exception:
                continue

    print(f"  Unique trade token IDs: {len(trade_tokens)}")

    if not trade_tokens:
        print("  [WARN] No trade CSVs found")
        return

    overlap, unmatched_res, unmatched_tok = resolver.overlap_with(trade_tokens)
    print(f"  Overlap (matched): {overlap}")
    print(f"  Unmatched in resolution CSV: {len(unmatched_res)}")
    print(f"  Unmatched in trades: {len(unmatched_tok)}")

    if unmatched_res:
        sample = list(unmatched_res)[:3]
        print(f"  Sample unmatched resolution tickers: {sample}")
    if unmatched_tok:
        sample_tok = list(unmatched_tok)[:3]
        print(f"  Sample unmatched trade tokens: {sample_tok}")
        # Show normalized versions
        sample_raw = [t for t in trade_tokens if normalize_token_id(t) in {normalize_token_id(s) for s in sample_tok}][:3]
        print(f"  Sample raw trade tokens: {sample_raw}")
