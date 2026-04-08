"""
CLI command: trading-cli data polymarket backfill-categories

Walks every wallet_trades row whose category is NULL, ``other``, or the
legacy ``tweet_count`` bucket and reclassifies it via the keyword-based
:class:`MarketCategorizer`. Idempotent — running it twice produces the
same result.

Usage::

    trading-cli data polymarket backfill-categories
    trading-cli data polymarket backfill-categories --no-reclassify-other
"""
from __future__ import annotations

import argparse
import json


def cmd_polymarket_backfill_categories(args: argparse.Namespace) -> None:
    from trading_platform.polymarket.market_categorizer import MarketCategorizer

    mc = MarketCategorizer()
    summary = mc.backfill_wallet_trades(
        progress_every=int(getattr(args, "progress_every", 5000) or 5000),
        reclassify_other=not bool(getattr(args, "no_reclassify_other", False)),
    )
    print()
    print(json.dumps(summary, indent=2))

    other_pct = (
        summary.get("by_category", {}).get("other", {}).get("pct", 0.0)
    )
    if other_pct > 15:
        print(f"\nWARNING: 'other' category is {other_pct}% (target <10%).")
    elif other_pct > 10:
        print(f"\nNote: 'other' category is {other_pct}% — target is <10%.")
    else:
        print(f"\nOK: 'other' category is {other_pct}% (target <10%).")


def add_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--progress-every",
        type=int,
        default=5000,
        help="Log progress every N slugs (default: 5000)",
    )
    parser.add_argument(
        "--no-reclassify-other",
        action="store_true",
        help="Only fill in NULL rows; do not re-evaluate existing 'other'",
    )
