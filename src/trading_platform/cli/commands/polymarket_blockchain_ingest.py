"""
CLI command: trading-cli data polymarket blockchain-ingest

Converts poly-trade-scan on-chain trade CSV into Kalshi-compatible
feature parquets and resolution CSV.

Usage
-----
    trading-cli data polymarket blockchain-ingest --trades-csv data/polymarket/raw/blockchain_trades.csv
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


def cmd_polymarket_blockchain_ingest(args: argparse.Namespace) -> None:
    from trading_platform.polymarket.blockchain_ingest import PolymarketBlockchainIngest

    csv_path = Path(args.trades_csv).expanduser()
    output_dir = _project_relative(getattr(args, "output_dir", None) or "data/polymarket/blockchain")
    min_trades = int(getattr(args, "min_trades", None) or 10)
    limit = getattr(args, "limit", None)
    if limit is not None:
        limit = int(limit)

    if not csv_path.exists():
        print(f"[ERROR] Trades CSV not found: {csv_path}")
        return

    print("Polymarket Blockchain Ingest")
    print(f"  trades csv : {csv_path}")
    print(f"  output dir : {output_dir}")
    print(f"  min trades : {min_trades}")
    print(f"  limit      : {limit or 'none'}")
    print()

    pipeline = PolymarketBlockchainIngest()
    result = pipeline.run(csv_path, output_dir, min_trades=min_trades, limit=limit)

    print("[DONE] Blockchain ingest complete.")
    print(f"  Rows loaded               : {result.rows_loaded}")
    print(f"  Token IDs found           : {result.token_ids_found}")
    print(f"  Markets matched           : {result.markets_matched}")
    print(f"  Skipped (few trades)      : {result.markets_skipped_few_trades}")
    print(f"  Skipped (feature error)   : {result.markets_skipped_feature_error}")
    print(f"  Markets processed         : {result.markets_processed}")
    print(f"  Feature files written     : {result.feature_files_written}")
    print(f"  Resolution records        : {result.resolution_records}")
    if result.date_range_start:
        print(f"  Date range                : {result.date_range_start} → {result.date_range_end}")
