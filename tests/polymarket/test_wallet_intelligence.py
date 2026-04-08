"""Tests for the wallet intelligence system: DB, sync, rebuild, signals."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from trading_platform.polymarket.wallet_db import WalletDB, categorize_slug


class TestWalletDB:
    def test_create_tables(self, tmp_path: Path) -> None:
        db = WalletDB(tmp_path / "test.db")
        stats = db.stats()
        assert stats["profiles"] == 0
        assert stats["trades"] == 0

    def test_upsert_and_get_profile(self, tmp_path: Path) -> None:
        db = WalletDB(tmp_path / "test.db")
        db.upsert_profile("0xaaa", edge=0.30, wallet_type="directional")
        p = db.get_profile("0xaaa")
        assert p is not None
        assert p["edge"] == 0.30
        assert p["wallet_type"] == "directional"

    def test_upsert_profile_updates(self, tmp_path: Path) -> None:
        db = WalletDB(tmp_path / "test.db")
        db.upsert_profile("0xaaa", edge=0.30)
        db.upsert_profile("0xaaa", edge=0.50)
        assert db.get_profile("0xaaa")["edge"] == 0.50

    def test_upsert_trade_dedup(self, tmp_path: Path) -> None:
        db = WalletDB(tmp_path / "test.db")
        inserted = db.upsert_trade(wallet="0xa", asset="tok1", side="BUY",
                                    size=100, price=0.5, timestamp=1000,
                                    transaction_hash="tx1", category="crypto", synced_at=1000)
        assert inserted
        inserted2 = db.upsert_trade(wallet="0xa", asset="tok1", side="BUY",
                                     size=100, price=0.5, timestamp=1000,
                                     transaction_hash="tx1", category="crypto", synced_at=1000)
        assert not inserted2  # duplicate

    def test_get_wallet_trades(self, tmp_path: Path) -> None:
        db = WalletDB(tmp_path / "test.db")
        for i in range(5):
            db.upsert_trade(wallet="0xa", asset=f"tok{i}", side="BUY",
                            size=100, price=0.5, timestamp=1000 + i,
                            transaction_hash=f"tx{i}", category="crypto", synced_at=1000)
        trades = db.get_wallet_trades("0xa", limit=3)
        assert len(trades) == 3

    def test_upsert_position(self, tmp_path: Path) -> None:
        db = WalletDB(tmp_path / "test.db")
        db.upsert_position("0xa", "tok1", size=50, avg_price=0.5, title="Test")
        positions = db.get_wallet_positions("0xa")
        assert len(positions) == 1
        assert positions[0]["size"] == 50

    def test_upsert_position_updates(self, tmp_path: Path) -> None:
        db = WalletDB(tmp_path / "test.db")
        db.upsert_position("0xa", "tok1", size=50, avg_price=0.5)
        db.upsert_position("0xa", "tok1", size=75, avg_price=0.6)
        positions = db.get_wallet_positions("0xa")
        assert positions[0]["size"] == 75

    def test_alerts(self, tmp_path: Path) -> None:
        db = WalletDB(tmp_path / "test.db")
        db.insert_alert(wallet="0xa", token_id="t1", tier=1, side="YES", size=100)
        db.insert_alert(wallet="0xb", token_id="t2", tier=2, side="NO", size=200)
        all_alerts = db.get_alerts(limit=10)
        assert len(all_alerts) == 2
        tier1 = db.get_alerts(tier=1)
        assert len(tier1) == 1

    def test_leaderboard_empty(self, tmp_path: Path) -> None:
        db = WalletDB(tmp_path / "test.db")
        assert db.get_leaderboard() == []

    def test_leaderboard_sorted_by_equity(self, tmp_path: Path) -> None:
        db = WalletDB(tmp_path / "test.db")
        db.upsert_profile("0xa", equity_score=1.5, wallet_type="directional")
        db.upsert_profile("0xb", equity_score=2.0, wallet_type="directional")
        lb = db.get_leaderboard()
        assert lb[0]["wallet"] == "0xb"
        assert lb[1]["wallet"] == "0xa"


class TestCategorizeSlug:
    def test_crypto(self):
        assert categorize_slug("btc-above-70k") == "crypto"
        assert categorize_slug("will-bitcoin-hit-100k") == "crypto"
        assert categorize_slug("eth-above-4000") == "crypto"

    def test_politics(self):
        assert categorize_slug("2024-presidential-election") == "politics"
        assert categorize_slug("trump-wins-2024") == "politics"

    def test_sports(self):
        assert categorize_slug("nba-finals-winner") == "sports"
        assert categorize_slug("nfl-super-bowl-champion") == "sports"

    def test_tweet_count(self):
        assert categorize_slug("elon-musk-tweets-50") == "tweet_count"
        assert categorize_slug("musk-tweet-count") == "tweet_count"

    def test_crypto_expanded(self):
        assert categorize_slug("xrp-above-2") == "crypto"
        assert categorize_slug("coinbase-ipo-price") == "crypto"
        assert categorize_slug("dogecoin-hits-1") == "crypto"
        assert categorize_slug("defi-tvl-100b") == "crypto"
        assert categorize_slug("price-above-100k-btc") == "crypto"
        assert categorize_slug("market-cap-eth") == "crypto"

    def test_economics(self):
        assert categorize_slug("cpi-above-3") == "economics"
        assert categorize_slug("will-fed-cut") == "economics"
        assert categorize_slug("rate-hike-june") == "economics"
        assert categorize_slug("nonfarm-payroll-above-200k") == "economics"
        assert categorize_slug("basis-points-50") == "economics"
        assert categorize_slug("recession-2026") == "economics"
        assert categorize_slug("treasury-yield") == "economics"

    def test_politics_expanded(self):
        assert categorize_slug("harris-wins-primary") == "politics"
        assert categorize_slug("approval-rating-50") == "politics"
        assert categorize_slug("impeach-president") == "politics"
        assert categorize_slug("cabinet-secretary") == "politics"
        assert categorize_slug("ballot-measure") == "politics"

    def test_sports_expanded(self):
        assert categorize_slug("nascar-winner-2026") == "sports"
        assert categorize_slug("f1-race-winner") == "sports"
        assert categorize_slug("super-bowl-champion") == "sports"
        assert categorize_slug("stanley-cup-winner") == "sports"
        assert categorize_slug("playoffs-series") == "sports"

    def test_tweet_expanded(self):
        assert categorize_slug("twitter-user-count") == "tweet_count"
        assert categorize_slug("x-posts-elon") == "tweet_count"

    def test_other(self):
        assert categorize_slug("will-it-rain-tomorrow") == "other"


class TestProfileRebuild:
    def test_bot_detection(self, tmp_path: Path) -> None:
        from trading_platform.polymarket.wallet_profile_rebuild import _classify_wallet_type
        # 20 trades in under 5 minutes = arb_bot
        trades = [{"timestamp": 1000 + i, "side": "BUY", "size": 10} for i in range(25)]
        positions = {}
        assert _classify_wallet_type(trades, positions) == "arb_bot"

    def test_market_maker_detection(self, tmp_path: Path) -> None:
        from trading_platform.polymarket.wallet_profile_rebuild import _classify_wallet_type
        # >80% sells
        trades = [{"timestamp": 1000 + i * 100, "side": "SELL", "size": 10} for i in range(20)]
        trades += [{"timestamp": 3000, "side": "BUY", "size": 10}]
        positions = {}
        assert _classify_wallet_type(trades, positions) == "market_maker"

    def test_directional_detection(self, tmp_path: Path) -> None:
        from trading_platform.polymarket.wallet_profile_rebuild import _classify_wallet_type
        # Mixed buys/sells, avg position > $10
        trades = [{"timestamp": 1000 + i * 3600, "side": "BUY", "size": 50} for i in range(5)]
        trades += [{"timestamp": 20000 + i * 3600, "side": "SELL", "size": 20} for i in range(3)]
        positions = {"tok1": {"net_size": 30.0}}
        assert _classify_wallet_type(trades, positions) == "directional"

    def test_pnl_computation(self, tmp_path: Path) -> None:
        from trading_platform.polymarket.wallet_profile_rebuild import _compute_profile
        # Need >= 3 losses for non-None profit_factor (anti-outlier guard)
        trades = [
            {"asset": "t1", "side": "BUY", "size": 100, "price": 0.4, "category": "crypto",
             "timestamp": 1000, "market_resolved": True, "market_outcome": "YES",
             "event_slug": "", "outcome": "", "pnl": 60.0},
            {"asset": "t2", "side": "BUY", "size": 50, "price": 0.6, "category": "crypto",
             "timestamp": 2000, "market_resolved": True, "market_outcome": "NO",
             "event_slug": "", "outcome": "", "pnl": -10.0},
            {"asset": "t3", "side": "BUY", "size": 50, "price": 0.6, "category": "crypto",
             "timestamp": 3000, "market_resolved": True, "market_outcome": "NO",
             "event_slug": "", "outcome": "", "pnl": -10.0},
            {"asset": "t4", "side": "BUY", "size": 50, "price": 0.6, "category": "crypto",
             "timestamp": 4000, "market_resolved": True, "market_outcome": "NO",
             "event_slug": "", "outcome": "", "pnl": -10.0},
        ]
        p = _compute_profile(trades)
        assert p["total_won_usdc"] == 60.0
        assert p["total_lost_usdc"] == 30.0
        assert p["net_pnl_usdc"] == 30.0
        assert p["profit_factor"] == 2.0
        assert p["avg_win_size_usdc"] == 60.0
        assert p["avg_loss_size_usdc"] == 10.0

    def test_pnl_insufficient_losses_returns_null_pf(self) -> None:
        """Profit factor should be None when fewer than 3 losses (anti-outlier)."""
        from trading_platform.polymarket.wallet_profile_rebuild import _compute_profile
        trades = [
            {"asset": "t1", "side": "BUY", "size": 100, "price": 0.4, "category": "crypto",
             "timestamp": 1000, "market_resolved": True, "market_outcome": "YES",
             "event_slug": "", "outcome": "", "pnl": 100.0},
        ]
        p = _compute_profile(trades)
        assert p["profit_factor"] is None

    def test_conviction_score(self, tmp_path: Path) -> None:
        from trading_platform.polymarket.wallet_profile_rebuild import _compute_profile
        import math
        trades = [
            {"asset": f"t{i}", "side": "BUY", "size": 500, "price": 0.5, "category": "crypto",
             "timestamp": 1000 + i * 3600, "market_resolved": True, "market_outcome": "YES",
             "event_slug": "", "outcome": "", "pnl": 250.0}
            for i in range(5)
        ] + [
            {"asset": f"l{i}", "side": "BUY", "size": 500, "price": 0.5, "category": "crypto",
             "timestamp": 6000 + i * 3600, "market_resolved": True, "market_outcome": "NO",
             "event_slug": "", "outcome": "", "pnl": -250.0}
            for i in range(5)
        ]
        p = _compute_profile(trades)
        # equity_score > 0 for 50% wr, 10 trades
        assert p["equity_score"] > 0
        # conviction = equity * log10(avg_pos + 1)
        assert p["conviction_score"] > 0
        assert p["conviction_score"] > p["equity_score"]  # avg_pos > 1 so log > 0

    def test_volume_tier(self, tmp_path: Path) -> None:
        from trading_platform.polymarket.wallet_profile_rebuild import _compute_profile
        # Whale: total_size >= 100k
        trades = [{"asset": "t1", "side": "BUY", "size": 100_000, "price": 0.5,
                    "category": "other", "timestamp": 1000, "market_resolved": False,
                    "market_outcome": None, "event_slug": "", "outcome": ""}]
        assert _compute_profile(trades)["volume_tier"] == "whale"
        # Small
        trades2 = [{"asset": "t1", "side": "BUY", "size": 50, "price": 0.5,
                     "category": "other", "timestamp": 1000, "market_resolved": False,
                     "market_outcome": None, "event_slug": "", "outcome": ""}]
        assert _compute_profile(trades2)["volume_tier"] == "small"

    def test_large_bet_threshold(self, tmp_path: Path) -> None:
        from trading_platform.polymarket.wallet_profile_rebuild import _compute_profile
        trades = [
            {"asset": f"t{i}", "side": "BUY", "size": (i + 1) * 10, "price": 0.5,
             "category": "other", "timestamp": 1000 + i, "market_resolved": False,
             "market_outcome": None, "event_slug": "", "outcome": ""}
            for i in range(20)  # sizes: 10, 20, 30, ..., 200
        ]
        p = _compute_profile(trades)
        # 75th percentile of [10,20,...,200] = 155 (index 15 of 20)
        assert p["large_bet_threshold"] >= 140
        assert p["large_bet_threshold"] <= 160

    def test_directional_win_rate_excludes_tweets(self, tmp_path: Path) -> None:
        from trading_platform.polymarket.wallet_profile_rebuild import _compute_profile
        trades = [
            {"asset": "t1", "side": "BUY", "size": 100, "category": "crypto",
             "timestamp": 1000, "market_resolved": True, "market_outcome": "YES",
             "event_slug": "", "outcome": ""},
            {"asset": "t2", "side": "BUY", "size": 100, "category": "tweet_count",
             "timestamp": 2000, "market_resolved": True, "market_outcome": "YES",
             "event_slug": "", "outcome": ""},
            {"asset": "t3", "side": "BUY", "size": 100, "category": "crypto",
             "timestamp": 3000, "market_resolved": True, "market_outcome": "NO",
             "event_slug": "", "outcome": ""},
        ]
        profile = _compute_profile(trades)
        # t1 win (BUY+YES), t3 loss (BUY+NO), t2 excluded (tweet_count)
        assert profile["directional_win_rate"] == 0.5  # 1/2
        assert profile["crypto_win_rate"] == 0.5


class TestWinners:
    def test_winners_all_time(self, tmp_path: Path) -> None:
        db = WalletDB(tmp_path / "test.db")
        db.upsert_profile("0xa", net_pnl_usdc=500.0, total_volume_usdc=10000, resolved_trades=20,
                           wallet_type="directional", equity_score=1.0)
        db.upsert_profile("0xb", net_pnl_usdc=-200.0, total_volume_usdc=5000, resolved_trades=10,
                           wallet_type="directional", equity_score=0.5)
        winners = db.get_winners(window="all")
        assert len(winners) == 2
        assert winners[0]["wallet"] == "0xa"  # highest profit first
        assert winners[0]["profit"] == 500.0

    def test_winners_weekly_from_trades(self, tmp_path: Path) -> None:
        import time
        db = WalletDB(tmp_path / "test.db")
        db.upsert_profile("0xa", equity_score=1.0)
        now = int(time.time())
        # Recent trade (within last 7 days)
        db.upsert_trade(wallet="0xa", asset="t1", side="BUY", size=100, price=0.5,
                         timestamp=now - 3600, transaction_hash="tx1", category="crypto",
                         synced_at=now, market_resolved=1, market_outcome="YES", pnl=50.0)
        # Old trade (>7 days ago)
        db.upsert_trade(wallet="0xa", asset="t2", side="BUY", size=100, price=0.5,
                         timestamp=now - 15 * 86400, transaction_hash="tx2", category="crypto",
                         synced_at=now, market_resolved=1, market_outcome="YES", pnl=200.0)
        winners = db.get_winners(window="weekly")
        assert len(winners) == 1
        assert winners[0]["profit"] == 50.0  # only recent trade counts

    def test_winners_empty(self, tmp_path: Path) -> None:
        db = WalletDB(tmp_path / "test.db")
        assert db.get_winners(window="all") == []


class TestSignalEngine:
    def test_compute_requires_directional_wallets(self, tmp_path: Path) -> None:
        from trading_platform.polymarket.signal_engine import SignalEngine
        db = WalletDB(tmp_path / "test.db")
        engine = SignalEngine(db=db)
        signals = engine.compute_market_signals()
        assert signals == []

    def test_actionable_filters(self, tmp_path: Path) -> None:
        from trading_platform.polymarket.signal_engine import SignalEngine
        db = WalletDB(tmp_path / "test.db")
        # Insert a signal directly
        db.insert_signal(token_id="tok1", market_title="Test", category="crypto",
                         direction="YES", confidence=0.8, net_smart_volume=1000,
                         weighted_net_volume=500, smart_wallet_count=3,
                         directional_wallet_count=3, top_wallet_edge=1.5,
                         top_wallet_address="0xa")
        # Insert a low-confidence signal
        db.insert_signal(token_id="tok2", market_title="Weak", category="crypto",
                         direction="NO", confidence=0.3, net_smart_volume=100,
                         weighted_net_volume=50, smart_wallet_count=1,
                         directional_wallet_count=1, top_wallet_edge=0.5,
                         top_wallet_address="0xb")
        engine = SignalEngine(db=db)
        actionable = engine.get_actionable_signals(min_wallets=2, min_confidence=0.6)
        assert len(actionable) == 1
        assert actionable[0]["token_id"] == "tok1"
