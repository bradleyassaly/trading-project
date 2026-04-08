"""End-to-end signal engine test with simulated whale trade."""
from __future__ import annotations

import sqlite3
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass

from trading_platform.polymarket.wallet_db import WalletDB
from trading_platform.polymarket.market_universe import MarketUniverse
from trading_platform.polymarket.whale_tripwire import WhaleTripwire, WhaleTrade
from trading_platform.polymarket.whale_signal_engine import WhaleSignalEngine


def main() -> None:
    db = WalletDB()
    universe = MarketUniverse()
    if not universe.load_cached():
        print("Refreshing universe...")
        universe.refresh()

    tripwire = WhaleTripwire(db, universe)
    engine = WhaleSignalEngine(db, universe)

    print(f"Tripwire: tier1={len(tripwire.watched_tier1)} tier2={len(tripwire.watched_tier2)}")
    print(f"Universe: {len(universe.get_all_condition_ids())} markets")

    # Get real tier1 wallet
    conn = sqlite3.connect(str(db._path))
    row = conn.execute("""
        SELECT l.wallet, p.directional_win_rate, p.conviction_score,
               p.large_bet_threshold, l.rolling_20_wr, l.wallet_type, l.wallet_bucket
        FROM leaderboard l JOIN wallet_profiles p ON l.wallet = p.wallet
        WHERE l.tier='tier1' ORDER BY l.conviction_score DESC LIMIT 1
    """).fetchone()
    conn.close()

    assert row, "No tier1 wallets found - run build-leaderboard first"
    wallet, wr, conv, lbt, rolling, wtype, bucket = row
    print(f"Test wallet: {wallet[:16]}... WR={wr:.1%} conv={conv:.3f}")

    # Get a market from universe
    cids = list(universe.get_all_condition_ids())
    assert cids, "No markets in universe - run refresh-universe first"
    cid = cids[0]
    question = universe.get_question(cid) or "Test market"
    category = universe.get_category(cid) or "politics"
    print(f"Test market: {category} - {question[:60]}")

    # Build test WhaleTrade
    trade = WhaleTrade(
        wallet=wallet,
        condition_id=cid,
        side="BUY",
        price=0.45,
        size=500.0,
        outcome="YES",
        question=question,
        category=category,
        timestamp=int(time.time()),
        wallet_tier="tier1",
        directional_win_rate=wr or 0.6,
        conviction_score=conv or 1.0,
        total_volume_usdc=50000.0,
    )

    # Before counts
    conn2 = sqlite3.connect(str(db._path))
    before_signals = conn2.execute("SELECT COUNT(*) FROM market_signals").fetchone()[0]
    before_alerts = conn2.execute("SELECT COUNT(*) FROM wallet_alerts").fetchone()[0]
    print(f"Before: market_signals={before_signals} wallet_alerts={before_alerts}")

    # Fire the signal engine
    print("Firing signal engine...")
    result = engine.on_whale_trade(trade)
    print(f"Result: {result}")

    # After counts
    after_signals = conn2.execute("SELECT COUNT(*) FROM market_signals").fetchone()[0]
    after_alerts = conn2.execute("SELECT COUNT(*) FROM wallet_alerts").fetchone()[0]
    print(f"After: market_signals={after_signals} wallet_alerts={after_alerts}")

    if after_signals > before_signals:
        sig = conn2.execute("""
            SELECT signal_type, direction, confidence, category
            FROM market_signals ORDER BY fired_at DESC LIMIT 1
        """).fetchone()
        print(f"Signal fired: {sig}")
        print("OK SIGNAL ENGINE WORKING")
        conn2.close()
        sys.exit(0)
    else:
        print("FAIL: NO SIGNAL FIRED")
        print(f"  Trade size: ${trade.size} (min: $25)")
        conf = (wr or 0.6) * min((conv or 1.0) / 10.0, 1.0)
        print(f"  Computed confidence: {conf:.3f}")
        print(f"  Wallet in tripwire.watched_tier1: {wallet in tripwire.watched_tier1}")
        conn2.close()
        sys.exit(1)


if __name__ == "__main__":
    main()
