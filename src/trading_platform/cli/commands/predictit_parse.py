"""
CLI command: trading-cli data predictit parse

Parses a PredictIt historical CSV into Kalshi-compatible feature parquets
and a resolution CSV.

PredictIt uses real USD (capped at $850/contract).

Usage
-----
    trading-cli data predictit parse --csv-path data/predictit/raw/market_data.csv
    trading-cli data predictit parse --csv-path data.csv --output-dir data/predictit --limit 100
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


def cmd_predictit_parse(args: argparse.Namespace) -> None:
    from trading_platform.predictit.parser import PredictItParser

    csv_path = Path(args.csv_path).expanduser()
    output_dir = _project_relative(getattr(args, "output_dir", None) or "data/predictit")
    min_bars = int(getattr(args, "min_bars", None) or 10)
    limit = getattr(args, "limit", None)
    if limit is not None:
        limit = int(limit)

    if not csv_path.exists():
        print(f"[ERROR] CSV file not found: {csv_path}")
        return

    print("PredictIt CSV Parser")
    print(f"  csv path   : {csv_path}")
    print(f"  output dir : {output_dir}")
    print(f"  min bars   : {min_bars}")
    print(f"  limit      : {limit or 'none'}")
    print()

    parser = PredictItParser()
    result = parser.parse(csv_path, output_dir, min_bars=min_bars, limit=limit)

    print("[DONE] PredictIt parse complete.")
    print(f"  Rows loaded               : {result.rows_loaded}")
    print(f"  Contracts found           : {result.contracts_found}")
    print(f"  Skipped (few bars)        : {result.contracts_skipped_few_bars}")
    print(f"  Skipped (feature error)   : {result.contracts_skipped_feature_error}")
    print(f"  Contracts processed       : {result.contracts_processed}")
    print(f"  Feature files written     : {result.feature_files_written}")
    print(f"  Resolution records        : {result.resolution_records}")
    if result.date_range_start:
        print(f"  Date range                : {result.date_range_start} → {result.date_range_end}")
