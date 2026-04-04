"""
CLI command: trading-cli data manifold parse

Parses a Manifold Markets data dump (markets.json + bets.json) into
Kalshi-compatible feature parquets and a resolution CSV.

Manifold uses play money (Mana), not real USD.

Usage
-----
    trading-cli data manifold parse --dump-dir ~/Downloads/manifold_dump
    trading-cli data manifold parse --dump-dir ./dump --min-bets 20 --limit 1000
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


def cmd_manifold_parse(args: argparse.Namespace) -> None:
    from trading_platform.manifold.parser import ManifoldParser

    dump_dir = Path(args.dump_dir).expanduser()
    output_dir = _project_relative(getattr(args, "output_dir", None) or "data/manifold")
    min_bets = int(getattr(args, "min_bets", None) or 10)
    limit = getattr(args, "limit", None)
    if limit is not None:
        limit = int(limit)

    if not dump_dir.exists():
        print(f"[ERROR] Dump directory not found: {dump_dir}")
        return

    print("Manifold Markets Parser")
    print(f"  dump dir   : {dump_dir}")
    print(f"  output dir : {output_dir}")
    print(f"  min bets   : {min_bets}")
    print(f"  limit      : {limit or 'none'}")
    print()

    parser = ManifoldParser()
    result = parser.parse(dump_dir, output_dir, min_bets=min_bets, limit=limit)

    print("[DONE] Manifold parse complete.")
    print(f"  Markets loaded            : {result.markets_loaded}")
    print(f"  Qualifying (binary+resolved): {result.markets_filtered}")
    print(f"  Skipped (non-binary)      : {result.markets_skipped_type}")
    print(f"  Skipped (bad resolution)  : {result.markets_skipped_resolution}")
    print(f"  Skipped (few bets)        : {result.markets_skipped_few_bets}")
    print(f"  Skipped (feature error)   : {result.markets_skipped_feature_error}")
    print(f"  Markets processed         : {result.markets_processed}")
    print(f"  Feature files written     : {result.feature_files_written}")
    print(f"  Resolution records        : {result.resolution_records}")
    if result.date_range_start:
        print(f"  Date range                : {result.date_range_start} → {result.date_range_end}")
