"""One-shot migration: correct size_usd/shares for SELL positions with partial fills.

GTC orders on near-resolution NO books partially fill (e.g. 1 token vs. 467
intended). The executor records size_usd = intended USDC allocation.  This
script reads the actual CONDITIONAL token balance and corrects size_usd and
shares in live_trades.

Run once: python scripts/fix_partial_fill_records.py
"""
from __future__ import annotations
import sys, os
sys.path.insert(0, "src")

from dotenv import load_dotenv
load_dotenv("C:/Users/bradl/PycharmProjects/trading_platform/.env")

import logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

from trading_platform.polymarket.db_connection import get_connection
from trading_platform.polymarket.clob_client import ClobClient

clob = ClobClient()
if not clob.is_configured:
    print("CLOB not configured — cannot check balances")
    sys.exit(1)

conn = get_connection()
rows = conn.execute("""
    SELECT id, token_id, fill_price, size_usd, shares
    FROM live_trades
    WHERE dry_run=0 AND direction='SELL' AND exit_ts IS NULL
      AND status NOT IN ('error','blocked','cancelled')
""").fetchall()
conn.close()

print(f"Checking {len(rows)} open SELL positions for partial fill discrepancies...")
print()

updated = skipped = zero_balance = 0
for lid, token_id, fill_price, size_usd, db_shares in rows:
    if not token_id:
        skipped += 1
        continue
    actual = clob.get_conditional_balance(token_id)
    if actual is None:
        logger.warning("#%d: balance check failed, skipping", lid)
        skipped += 1
        continue

    fill = float(fill_price) if fill_price else 0
    no_price = max(1.0 - fill, 0.01)
    old_size = float(size_usd) if size_usd else 0
    actual_size_usd = round(actual * no_price, 6)

    if actual < 0.001:
        zero_balance += 1
        logger.info("#%d: 0 tokens — market resolved, skipping DB update (monitor will close)", lid)
        continue

    ratio = old_size / max(actual_size_usd, 0.000001)
    if ratio < 1.5:
        # Within 50% — probably already correct or a BUY mistagged
        skipped += 1
        continue

    conn2 = get_connection()
    try:
        conn2.execute(
            "UPDATE live_trades SET size_usd=?, shares=? WHERE id=?",
            (actual_size_usd, round(actual, 6), lid),
        )
        conn2.commit()
    finally:
        try: conn2.close()
        except Exception: pass

    logger.info(
        "#%d: corrected size_usd $%.4f → $%.4f (%.0fx), shares %.4f",
        lid, old_size, actual_size_usd, ratio, actual,
    )
    updated += 1

print()
print(f"Done: updated={updated}, zero_balance={zero_balance}, skipped={skipped}")
