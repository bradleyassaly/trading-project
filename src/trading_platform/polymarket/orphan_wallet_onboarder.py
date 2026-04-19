"""Auto-onboard orphan wallets detected by live-collect into wallet_trades.

Live-collect fires signals for ANY whale seen on the WebSocket, even if the
wallet isn't in our `wallet_profiles` / `wallet_trades` tables. When a
repeat orphan accumulates enough activity, we should fetch their trade
history and let the rest of the pipeline (profile, classify, alpha-score)
pick them up.

Threshold chosen conservatively: 10+ orphan signals in the last 7 days.
That's meaningful recurrence, not one-off noise.

Run via task_scheduler at 12h cadence.
"""
from __future__ import annotations

import logging
from pathlib import Path

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_DEFAULT_DB = _PROJECT_ROOT / "data" / "polymarket" / "wallet_intelligence.db"

MIN_ORPHAN_SIGNALS = 10
LOOKBACK_DAYS = 7
# Synthetic wallet names that must NEVER be onboarded.
SYNTHETIC_WALLETS = {"velocity_detector", "order_book_monitor"}


def find_orphan_candidates(conn, *, min_signals: int = MIN_ORPHAN_SIGNALS) -> list[str]:
    """Return wallets with >= min_signals signals and no wallet_trades row."""
    cutoff_expr = f"strftime('%s','now','-{LOOKBACK_DAYS} days')"
    rows = conn.execute(
        f"""
        SELECT so.wallet, COUNT(*) AS n
        FROM signal_outcomes so
        WHERE so.signal_type != 'price_velocity'
          AND so.fired_at >= {cutoff_expr}
          AND so.wallet IS NOT NULL AND so.wallet != ''
          AND NOT EXISTS (
            SELECT 1 FROM wallet_trades wt WHERE wt.wallet = so.wallet
          )
        GROUP BY so.wallet
        HAVING COUNT(*) >= ?
        ORDER BY COUNT(*) DESC
        """,
        (min_signals,),
    ).fetchall()
    return [w for w, _ in rows if w not in SYNTHETIC_WALLETS]


def onboard(wallets: list[str]) -> dict:
    """Fetch each orphan wallet's trade history via the Data API and
    insert into wallet_trades + wallet_profiles. Reuses the existing
    sync pipeline so we inherit its error handling and rate limiting."""
    if not wallets:
        return {"candidates": 0, "onboarded": 0}
    from trading_platform.polymarket.wallet_db import WalletDB
    from trading_platform.polymarket.wallet_trade_sync import _sync_one_wallet
    from trading_platform.polymarket.address_map import AddressMap

    db = WalletDB()
    addr_map = AddressMap()
    onboarded = 0
    total_new = 0
    for w in wallets:
        try:
            # Seed a minimal profile so the sync loop can fill it out.
            db.upsert_profile(w, wallet_type="discovered_orphan")
            new, _ = _sync_one_wallet(db, w, addr_map)
            if new > 0:
                onboarded += 1
                total_new += new
            logger.info(
                "orphan onboarded: %s new=%d", w[:14], new,
            )
        except Exception as exc:
            logger.warning("orphan onboarding failed for %s: %s", w[:14], exc)
    return {
        "candidates": len(wallets),
        "onboarded": onboarded,
        "new_trades": total_new,
    }


def main() -> dict:
    conn = get_connection(str(_DEFAULT_DB))
    try:
        candidates = find_orphan_candidates(conn)
    finally:
        conn.close()
    print(f"orphan candidates ({LOOKBACK_DAYS}d, >={MIN_ORPHAN_SIGNALS} signals): {len(candidates)}")
    for w in candidates:
        print(f"  {w}")
    result = onboard(candidates)
    print(f"result: {result}")
    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    raise SystemExit(0 if main() else 1)
