"""Tests for WalletMirror."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from trading_platform.polymarket.wallet_mirror import WalletMirror, MirrorSignal
from trading_platform.polymarket.price_lookup import PriceLookup


def _profiles(tmp_path: Path, wallets: list[dict]) -> Path:
    path = tmp_path / "profiles.parquet"
    pd.DataFrame(wallets).to_parquet(path, index=False)
    return path


def _smart(name: str, edge: float = 0.70, uet: int = 25) -> dict:
    return {
        "wallet": name, "total_trades": 100, "resolved_trades": 50,
        "wins": 40, "win_rate": 0.80, "early_trades": 30, "early_wins": 22,
        "uncertain_early_trades": uet, "uncertain_early_wins": 18,
        "early_win_rate": 0.82, "baseline_win_rate": 0.52,
        "edge": edge, "confidence_score": 1.0,
        "avg_hours_before_close": 48.0,
        "total_volume_usdc": 50000, "avg_trade_size": 500,
        "markets_traded": 20, "best_domain": "economics",
        "is_early_informed": True, "is_degraded": False,
        "is_highly_informed": True, "smart_money": True,
    }


def _fill(maker: str, token: str = "tok1", amount: float = 100.0,
          maker_asset: str = "0") -> dict:
    return {
        "id": f"f-{maker}-{token}",
        "timestamp": datetime.now(tz=timezone.utc),
        "maker_wallet": maker, "taker_wallet": "other",
        "maker_amount": amount, "taker_amount": amount * 2,
        "maker_asset_id": maker_asset, "taker_asset_id": token,
        "fee": 0.5,
    }


def _mock_price():
    return {
        "price": 0.50, "best_bid": 0.48, "best_ask": 0.52,
        "spread": 0.04, "question": "Will inflation rise?",
        "end_date_iso": "2026-12-31", "tradeable": True, "source": "mock",
    }


class TestWalletMirror:
    def test_combines_multiple_wallets(self, tmp_path: Path) -> None:
        profiles = _profiles(tmp_path, [_smart("w1"), _smart("w2")])
        mirror = WalletMirror(profiles, top_n=10)

        mock_client = MagicMock()
        mock_client.fetch_fills_by_makers.return_value = pd.DataFrame([_fill("w1")])

        fills = mirror.fetch_recent_trades(goldsky_client=mock_client)
        assert mock_client.fetch_fills_by_makers.call_count == 1  # single batched call

    @patch.object(PriceLookup, "get_price", return_value=_mock_price())
    def test_direction_yes_when_maker_asset_0(self, mock_gp, tmp_path: Path) -> None:
        profiles = _profiles(tmp_path, [_smart("w1")])
        mirror = WalletMirror(profiles, top_n=10)

        mock_client = MagicMock()
        mock_client.fetch_fills_by_makers.return_value = pd.DataFrame([
            _fill("w1", "tok1", maker_asset="0"),
        ])

        signals = mirror.get_mirror_signals(goldsky_client=mock_client)
        assert len(signals) >= 1
        assert signals[0].direction == "YES"

    @patch.object(PriceLookup, "get_price", return_value=_mock_price())
    def test_skip_when_price_moved(self, mock_gp, tmp_path: Path) -> None:
        profiles = _profiles(tmp_path, [_smart("w1")])
        mirror = WalletMirror(profiles, top_n=10)

        mock_client = MagicMock()
        mock_client.fetch_fills_by_makers.return_value = pd.DataFrame([
            _fill("w1", "tok1", amount=200.0),
        ])
        signals = mirror.get_mirror_signals(goldsky_client=mock_client, min_fill_usdc=100)
        assert len(signals) >= 1

    @patch.object(PriceLookup, "get_price", return_value=_mock_price())
    def test_sorted_by_edge(self, mock_gp, tmp_path: Path) -> None:
        profiles = _profiles(tmp_path, [_smart("w1", edge=0.70), _smart("w2", edge=0.90)])
        mirror = WalletMirror(profiles, top_n=10)

        mock_client = MagicMock()
        mock_client.fetch_fills_by_makers.return_value = pd.DataFrame([
            _fill("w1", "tok1"), _fill("w2", "tok2"),
        ])

        signals = mirror.get_mirror_signals(goldsky_client=mock_client)
        if len(signals) >= 2:
            assert signals[0].wallet_edge >= signals[1].wallet_edge

    def test_empty_when_no_profiles(self, tmp_path: Path) -> None:
        mirror = WalletMirror(tmp_path / "missing.parquet")
        assert mirror.watch_list == []

    @patch.object(PriceLookup, "get_price", return_value=_mock_price())
    def test_fills_below_min_excluded(self, mock_gp, tmp_path: Path) -> None:
        profiles = _profiles(tmp_path, [_smart("w1")])
        mirror = WalletMirror(profiles, top_n=10)

        mock_client = MagicMock()
        mock_client.fetch_fills_by_makers.return_value = pd.DataFrame([
            _fill("w1", "tok1", amount=50.0),
        ])

        signals = mirror.get_mirror_signals(goldsky_client=mock_client, min_fill_usdc=100)
        assert signals == []

    def test_not_tracked_when_no_price(self, tmp_path: Path) -> None:
        """Signal skipped when PriceLookup returns None."""
        profiles = _profiles(tmp_path, [_smart("w1")])
        mirror = WalletMirror(profiles, top_n=10)

        mock_client = MagicMock()
        mock_client.fetch_fills_by_makers.return_value = pd.DataFrame([
            _fill("w1", "tok1", amount=200.0),
        ])

        with patch.object(PriceLookup, "get_price", return_value=None):
            signals = mirror.get_mirror_signals(
                goldsky_client=mock_client, min_fill_usdc=100,
            )
        assert len(signals) == 0

    @patch.object(PriceLookup, "get_price")
    def test_sports_market_filtered(self, mock_gp, tmp_path: Path) -> None:
        """Sports markets filtered by question content."""
        sports_price = _mock_price()
        sports_price["question"] = "Will Arsenal win the match?"
        mock_gp.return_value = sports_price

        profiles = _profiles(tmp_path, [_smart("w1")])
        mirror = WalletMirror(profiles, top_n=10)

        mock_client = MagicMock()
        mock_client.fetch_fills_by_makers.return_value = pd.DataFrame([
            _fill("w1", "tok1", amount=200.0),
        ])

        signals = mirror.get_mirror_signals(goldsky_client=mock_client, min_fill_usdc=100)
        assert len(signals) == 0
        assert mirror._last_sports_count == 1

    @patch.object(PriceLookup, "get_price", return_value=_mock_price())
    def test_dedup_same_wallet_token(self, mock_gp, tmp_path: Path) -> None:
        """Multiple fills for same wallet+token deduped to one signal."""
        profiles = _profiles(tmp_path, [_smart("w1")])
        mirror = WalletMirror(profiles, top_n=10)

        mock_client = MagicMock()
        fills = [_fill("w1", "tok1", amount=100.0),
                 _fill("w1", "tok1", amount=200.0),
                 _fill("w1", "tok1", amount=150.0)]
        # Give each fill a unique ID so they aren't deduped at the fill level
        for i, f in enumerate(fills):
            f["id"] = f"fill-{i}"
        mock_client.fetch_fills_by_makers.return_value = pd.DataFrame(fills)

        signals = mirror.get_mirror_signals(goldsky_client=mock_client, min_fill_usdc=50)
        assert len(signals) == 1
        # Largest fill kept as representative
        assert signals[0].fill_amount_usdc == 200.0
        assert signals[0].fill_count == 3
