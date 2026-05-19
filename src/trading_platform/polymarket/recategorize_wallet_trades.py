"""
Recategorize wallet_trades rows where category = 'other'.

Runs classify_keywords() on each distinct (slug, title) pair and bulk-updates
wallet_trades with the correct category. Safe to run multiple times — only
updates rows still tagged 'other'.

Usage::
    python -m trading_platform.polymarket.recategorize_wallet_trades
    python -m trading_platform.polymarket.recategorize_wallet_trades --dry-run
"""
from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[3]


def run(dry_run: bool = False) -> dict:
    from trading_platform.polymarket.db_connection import get_connection
    from trading_platform.polymarket.market_categorizer import classify_keywords

    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT DISTINCT slug, title
            FROM wallet_trades
            WHERE (LOWER(category) = 'other' OR category IS NULL)
              AND slug IS NOT NULL AND slug != ''
        """).fetchall()
    finally:
        conn.close()

    logger.info("Found %d distinct (slug, title) pairs tagged 'other'", len(rows))

    # Group slugs by their new category
    by_category: dict[str, list[str]] = {}
    still_other = 0
    for slug, title in rows:
        if not slug:
            continue
        new_cat, _source = classify_keywords(slug, title)
        if new_cat != "other":
            by_category.setdefault(new_cat, []).append(slug)
        else:
            still_other += 1

    total_slugs = sum(len(v) for v in by_category.values())
    logger.info(
        "Reclassified %d slugs across %d categories — %d remain 'other'",
        total_slugs, len(by_category), still_other,
    )

    if dry_run:
        print("DRY RUN — no DB writes.")
        print(f"Would reclassify {total_slugs} unique slugs:")
        for cat, slugs in sorted(by_category.items(), key=lambda x: -len(x[1])):
            print(f"  {cat}: {len(slugs)} unique slugs")
            for s in slugs[:3]:
                print(f"    {s[:80]}")
        return {"dry_run": True, "reclassified_slugs": total_slugs,
                "by_category": {k: len(v) for k, v in by_category.items()}}

    if not by_category:
        print("Nothing to reclassify.")
        return {"updated_rows": 0}

    # One UPDATE per target category using ANY(%s::text[])
    total_updated = 0
    conn = get_connection()
    try:
        for category, slugs in by_category.items():
            result = conn.execute(
                """
                UPDATE wallet_trades
                SET category = %s
                WHERE slug = ANY(%s::text[])
                  AND (LOWER(category) = 'other' OR category IS NULL)
                """,
                (category, slugs),
            )
            conn.commit()
            rows_updated = result.rowcount
            total_updated += rows_updated
            logger.info("  %-16s: %d slugs → %d rows updated", category, len(slugs), rows_updated)
    finally:
        conn.close()

    print(f"Updated {total_updated} wallet_trades rows:")
    for cat, slugs in sorted(by_category.items(), key=lambda x: -len(x[1])):
        print(f"  {cat}: {len(slugs)} unique slugs")
    return {"updated_rows": total_updated, "by_category": {k: len(v) for k, v in by_category.items()}}


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    p = argparse.ArgumentParser(description="Recategorize wallet_trades 'other' rows")
    p.add_argument("--dry-run", action="store_true", help="Show what would change without writing")
    args = p.parse_args()

    t0 = time.monotonic()
    result = run(dry_run=args.dry_run)
    elapsed = time.monotonic() - t0
    print(f"Done in {elapsed:.1f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
