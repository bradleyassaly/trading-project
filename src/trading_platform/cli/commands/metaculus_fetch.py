"""
CLI command: trading-cli data metaculus fetch

Fetches resolved binary questions from the Metaculus public API and
converts forecast history into Kalshi-compatible feature parquets.

Usage
-----
    trading-cli data metaculus fetch --limit 2000
    trading-cli data metaculus fetch --output-dir data/metaculus --limit 500
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


def cmd_metaculus_fetch(args: argparse.Namespace) -> None:
    from trading_platform.metaculus.parser import MetaculusParser

    output_dir = _project_relative(getattr(args, "output_dir", None) or "data/metaculus")
    limit = int(getattr(args, "limit", None) or 2000)
    min_forecasts = int(getattr(args, "min_forecasts", None) or 5)

    print("Metaculus Resolved Question Fetcher")
    print(f"  output dir     : {output_dir}")
    print(f"  limit          : {limit}")
    print(f"  min forecasts  : {min_forecasts}")
    print()

    parser = MetaculusParser()
    result = parser.fetch_resolved(output_dir, limit=limit, min_forecasts=min_forecasts)

    print("[DONE] Metaculus fetch complete.")
    print(f"  Questions fetched         : {result.questions_fetched}")
    print(f"  Skipped (ambiguous)       : {result.questions_skipped_ambiguous}")
    print(f"  Skipped (no history)      : {result.questions_skipped_no_history}")
    print(f"  Skipped (few forecasts)   : {result.questions_skipped_few_forecasts}")
    print(f"  Skipped (feature error)   : {result.questions_skipped_feature_error}")
    print(f"  Questions processed       : {result.questions_processed}")
    print(f"  Feature files written     : {result.feature_files_written}")
    print(f"  Resolution records        : {result.resolution_records}")
    if result.date_range_start:
        print(f"  Date range                : {result.date_range_start} → {result.date_range_end}")
