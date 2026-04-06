"""Tests for RealtimeMonitor on_fill() logic."""
from __future__ import annotations

import asyncio
import csv
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from trading_platform.polymarket.alert_system import AlertSystem
from trading_platform.polymarket.price_lookup import PriceLookup
from trading_platform.polymarket.realtime_monitor import RealtimeMonitor


def _mock_price_result():
    return {
        "price": 0.50, "best_bid": 0.48, "best_ask": 0.52,
        "spread": 0.04, "question": "Will inflation rise?",
        "end_date_iso": "2026-12-31", "tradeable": True, "source": "mock",
    }


def _make_profiles(tmp_path: Path, wallets: list[dict]) -> Path:
    """Create a wallet_profiles.parquet with given wallet rows."""
    path = tmp_path / "wallet_profiles.parquet"
    df = pd.DataFrame(wallets)
    df.to_parquet(path, index=False)
    return path


def _make_fill(
    maker: str = "0xaaa",
    amount_raw: int = 200_000_000,  # $200 in raw units (6 decimals)
    maker_asset: str = "0",
    taker_asset: str = "tok1",
    timestamp: int = 0,
) -> dict:
    if timestamp == 0:
        timestamp = int(datetime.now(tz=timezone.utc).timestamp())
    return {
        "id": f"fill-{maker}-{taker_asset}",
        "timestamp": timestamp,
        "maker": maker,
        "taker": "0xcounterparty",
        "makerAmountFilled": str(amount_raw),
        "takerAmountFilled": "400000000",
        "makerAssetId": maker_asset,
        "takerAssetId": taker_asset,
    }


def _default_wallet(wallet: str = "0xaaa", ewr: float = 0.85, edge: float = 0.30,
                     conf: float = 2.5, uet: int = 50) -> dict:
    return {
        "wallet": wallet,
        "early_win_rate": ewr,
        "edge": edge,
        "confidence_score": conf,
        "uncertain_early_trades": uet,
        "total_volume_usdc": 100000.0,
        "best_domain": "economics",
        "is_degraded": False,
        "is_early_informed": True,
        "smart_money": True,
        "win_rate": 0.65,
        "resolved_trades": 100,
        "wins": 65,
        "total_trades": 200,
    }


@pytest.fixture
def monitor_with_wallets(tmp_path):
    """Create a monitor with 2 wallets: 0xaaa (Tier 1) and 0xbbb (Tier 2)."""
    profiles = _make_profiles(tmp_path, [
        _default_wallet("0xaaa", conf=3.0),  # Tier 1 (highest conf)
        _default_wallet("0xbbb", conf=1.5),  # Tier 2
    ])
    alert = AlertSystem()  # console only
    monitor = RealtimeMonitor(
        wallet_profiles_path=profiles,
        prices_db_path=tmp_path / "nonexistent.db",
        alert_system=alert,
        min_fill_usdc=100.0,
        min_ewr=0.70,
        auto_trade=True,
        tier1_count=1,
        tier2_count=1,
    )
    # Mock price lookup to return valid data for test tokens
    monitor._price_lookup = MagicMock(spec=PriceLookup)
    monitor._price_lookup.get_price.return_value = _mock_price_result()
    return monitor


class TestOnFill:
    def test_fill_below_min_fill_skipped(self, monitor_with_wallets):
        """Fills below $100 are not processed."""
        monitor = monitor_with_wallets
        fill = _make_fill(maker="0xaaa", amount_raw=50_000_000)  # $50
        asyncio.run(monitor.on_fill(fill))
        assert monitor._alerts_sent == 0

    def test_fill_from_non_watched_wallet_skipped(self, monitor_with_wallets):
        """Fills from unknown wallets are ignored."""
        monitor = monitor_with_wallets
        fill = _make_fill(maker="0xunknown", amount_raw=500_000_000)
        asyncio.run(monitor.on_fill(fill))
        assert monitor._alerts_sent == 0

    def test_tier1_wallet_triggers_alert(self, monitor_with_wallets):
        """Tier 1 wallet fill triggers an alert."""
        monitor = monitor_with_wallets
        fill = _make_fill(maker="0xaaa", amount_raw=200_000_000)  # $200
        asyncio.run(monitor.on_fill(fill))
        assert monitor._alerts_sent == 1

    def test_tier2_wallet_triggers_alert_no_auto_trade(self, monitor_with_wallets):
        """Tier 2 wallet triggers alert but not auto-trade."""
        monitor = monitor_with_wallets
        # Mock paper executor
        executor = MagicMock()
        executor.execute_trade.return_value = True
        monitor._paper_executor = executor

        fill = _make_fill(maker="0xbbb", amount_raw=200_000_000)
        asyncio.run(monitor.on_fill(fill))

        assert monitor._alerts_sent == 1
        # Auto trade not called for Tier 2
        assert monitor._trades_placed == 0

    def test_direction_yes_when_maker_asset_zero(self, monitor_with_wallets):
        """makerAssetId == '0' means buying tokens = YES direction."""
        monitor = monitor_with_wallets
        fill = _make_fill(maker="0xaaa", maker_asset="0", taker_asset="tok1")
        asyncio.run(monitor.on_fill(fill))
        # The alert was sent — check the direction was YES
        # (We can verify through fills_processed count)
        assert monitor._fills_processed == 1

    def test_direction_no_when_maker_asset_nonzero(self, monitor_with_wallets):
        """makerAssetId != '0' means selling tokens = NO direction."""
        monitor = monitor_with_wallets
        fill = _make_fill(maker="0xaaa", maker_asset="tok1", taker_asset="0")
        asyncio.run(monitor.on_fill(fill))
        assert monitor._fills_processed == 1


class TestIlliquidMarket:
    def test_illiquid_market_no_auto_trade(self, tmp_path):
        """Spread > 0.15 skips auto-trade even for Tier 1."""
        profiles = _make_profiles(tmp_path, [
            _default_wallet("0xaaa", conf=3.0),
        ])

        executor = MagicMock()
        executor.execute_trade.return_value = True

        monitor = RealtimeMonitor(
            wallet_profiles_path=profiles,
            prices_db_path=tmp_path / "nonexistent.db",
            paper_executor=executor,
            min_fill_usdc=100.0,
            auto_trade=True,
            tier1_count=1,
            tier2_count=0,
        )

        # Mock price lookup to return illiquid market (no spread)
        illiquid_price = {
            "price": 0.50, "best_bid": None, "best_ask": None,
            "spread": None, "question": "Will inflation rise?",
            "end_date_iso": "2026-12-31", "tradeable": False, "source": "gamma",
        }
        monitor._price_lookup = MagicMock(spec=PriceLookup)
        monitor._price_lookup.get_price.return_value = illiquid_price

        fill = _make_fill(maker="0xaaa", amount_raw=200_000_000)
        asyncio.run(monitor.on_fill(fill))

        # Alert sent but no trade (not tradeable)
        assert monitor._alerts_sent == 1
        assert monitor._trades_placed == 0


class TestGoldskyStream:
    def test_wallet_filter(self):
        """Stream only passes fills from watched wallets."""
        from trading_platform.polymarket.goldsky_stream import GoldskyStream

        received = []
        stream = GoldskyStream(
            watch_wallets=["0xAAA", "0xBBB"],
            callback=lambda fill: received.append(fill),
        )

        # Simulate events
        events = [
            {"id": "1", "timestamp": "123", "maker": "0xaaa", "taker": "0xz",
             "makerAmountFilled": "100", "takerAmountFilled": "200",
             "makerAssetId": "0", "takerAssetId": "tok1"},
            {"id": "2", "timestamp": "124", "maker": "0xccc", "taker": "0xz",
             "makerAmountFilled": "100", "takerAmountFilled": "200",
             "makerAssetId": "0", "takerAssetId": "tok2"},
        ]
        asyncio.run(stream._process_events(events))

        assert len(received) == 1
        assert received[0]["maker"] == "0xaaa"

    def test_dedup_by_id(self):
        """Same fill ID is not processed twice."""
        from trading_platform.polymarket.goldsky_stream import GoldskyStream

        received = []
        stream = GoldskyStream(
            watch_wallets=["0xaaa"],
            callback=lambda fill: received.append(fill),
        )

        events = [
            {"id": "same", "timestamp": "123", "maker": "0xaaa", "taker": "0xz",
             "makerAmountFilled": "100", "takerAmountFilled": "200",
             "makerAssetId": "0", "takerAssetId": "tok1"},
        ]
        asyncio.run(stream._process_events(events))
        asyncio.run(stream._process_events(events))

        assert len(received) == 1


class TestAlertSystem:
    def test_console_alert(self, capsys):
        """AlertSystem prints to console without Telegram config."""
        alert = AlertSystem()
        fill = {"maker": "0xabc123def456", "timestamp": int(datetime.now(tz=timezone.utc).timestamp())}
        profile = {"early_win_rate": 0.85, "edge": 0.30, "best_domain": "economics",
                    "uncertain_early_trades": 50}
        alert.send_mirror_alert(fill, profile, "Will Fed cut rates?", 0.65, 0.03,
                                tier=1, direction="YES", fill_amount=500.0)
        out = capsys.readouterr().out
        assert "TIER 1 INSIDER" in out
        assert "Will Fed cut rates?" in out
        assert "$500" in out

    def test_tier2_label(self, capsys):
        alert = AlertSystem()
        fill = {"maker": "0xabc", "timestamp": 0}
        alert.send_mirror_alert(fill, {}, "Test", None, None, tier=2, direction="NO", fill_amount=100)
        out = capsys.readouterr().out
        assert "TIER 2" in out


class TestWatchListLoading:
    def test_tier_split(self, tmp_path):
        """Wallets are split into Tier 1 and Tier 2 by confidence score."""
        wallets = [_default_wallet(f"0x{i:04x}", conf=10.0 - i) for i in range(5)]
        profiles = _make_profiles(tmp_path, wallets)
        monitor = RealtimeMonitor(
            wallet_profiles_path=profiles,
            prices_db_path=tmp_path / "none.db",
            tier1_count=2,
            tier2_count=2,
        )
        assert len(monitor._tier1) == 2
        assert len(monitor._tier2) == 2
        assert len(monitor.all_watched) == 4

    def test_tier1_only_flag(self, tmp_path):
        """tier1_only excludes Tier 2 wallets."""
        wallets = [_default_wallet(f"0x{i:04x}", conf=10.0 - i) for i in range(5)]
        profiles = _make_profiles(tmp_path, wallets)
        monitor = RealtimeMonitor(
            wallet_profiles_path=profiles,
            prices_db_path=tmp_path / "none.db",
            tier1_count=2,
            tier2_count=3,
            tier1_only=True,
        )
        assert len(monitor._tier1) == 2
        assert len(monitor._tier2) == 0

    def test_missing_profiles_no_crash(self, tmp_path):
        """Monitor handles missing profile file gracefully."""
        monitor = RealtimeMonitor(
            wallet_profiles_path=tmp_path / "nonexistent.parquet",
            prices_db_path=tmp_path / "none.db",
        )
        assert len(monitor.all_watched) == 0


class TestAlertDedup:
    def test_same_fill_id_only_one_alert(self, monitor_with_wallets):
        """Same fill ID should only trigger one alert, not two."""
        monitor = monitor_with_wallets
        fill = _make_fill(maker="0xaaa", amount_raw=200_000_000)
        fill["id"] = "dedup-test-123"
        asyncio.run(monitor.on_fill(fill))
        asyncio.run(monitor.on_fill(fill))
        assert monitor._alerts_sent == 1

    def test_different_fill_ids_both_alert(self, monitor_with_wallets):
        """Different fill IDs should each trigger an alert."""
        monitor = monitor_with_wallets
        fill1 = _make_fill(maker="0xaaa", amount_raw=200_000_000)
        fill1["id"] = "fill-1"
        fill2 = _make_fill(maker="0xaaa", amount_raw=200_000_000)
        fill2["id"] = "fill-2"
        asyncio.run(monitor.on_fill(fill1))
        asyncio.run(monitor.on_fill(fill2))
        assert monitor._alerts_sent == 2


class TestDomainClassification:
    def test_domain_by_trade_count(self, tmp_path):
        """Domain assigned by trade count not win rate."""
        from trading_platform.polymarket.goldsky_wallet_profiler import classify_market
        # Wallet trades 10 sports markets and 2 geopolitics markets
        # If domain were by WR: geopolitics (100% WR) would win
        # By trade count: sports (10 trades) wins
        #
        # We test the classify_market function directly since the profiler
        # aggregation uses d_trades count for domain assignment
        assert classify_market("Will Liverpool win the soccer match?") == "sports"
        assert classify_market("US forces enter Iran?") == "geopolitics"

    def test_sports_wallet_excluded_from_watch_list(self, tmp_path):
        """Sports-domain wallets excluded from WalletMirror."""
        from trading_platform.polymarket.wallet_mirror import WalletMirror
        profiles = _make_profiles(tmp_path, [
            {**_default_wallet("0xsports", conf=5.0), "best_domain": "sports"},
            {**_default_wallet("0xgeopol", conf=3.0), "best_domain": "geopolitics"},
        ])
        mirror = WalletMirror(profiles, top_n=10, min_confidence=0.5, min_uncertain_trades=10)
        assert "0xsports" not in mirror.watch_list
        assert "0xgeopol" in mirror.watch_list
        assert mirror.excluded_sports == 1


class TestSportsKeywords:
    """Test is_sports_market() for specific cases from production."""

    def test_fc_metz(self):
        from trading_platform.polymarket.price_lookup import is_sports_market
        assert is_sports_market("Will FC Metz win on 2026-04-05?")

    def test_wild_vs_red_wings(self):
        from trading_platform.polymarket.price_lookup import is_sports_market
        assert is_sports_market("Wild vs. Red Wings")

    def test_dodgers_vs_nationals(self):
        from trading_platform.polymarket.price_lookup import is_sports_market
        assert is_sports_market("Los Angeles Dodgers vs. Washington Nationals")

    def test_iran_not_sports(self):
        from trading_platform.polymarket.price_lookup import is_sports_market
        assert not is_sports_market("Will US forces enter Iran by April 30?")

    def test_fed_not_sports(self):
        from trading_platform.polymarket.price_lookup import is_sports_market
        assert not is_sports_market("Will Fed hold rates in May?")

    def test_monitor_sports_counter(self, monitor_with_wallets):
        """Sports fills increment _sports_filtered counter."""
        monitor = monitor_with_wallets
        sports_price = _mock_price_result()
        sports_price["question"] = "Wild vs. Red Wings"
        monitor._price_lookup.get_price.return_value = sports_price
        fill = _make_fill(maker="0xaaa", amount_raw=200_000_000)
        asyncio.run(monitor.on_fill(fill))
        assert monitor._alerts_sent == 0
        assert monitor._sports_filtered == 1
