"""Tests for SmartMoneySignalGenerator (Goldsky fill format)."""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from trading_platform.polymarket.smart_money_signal import SmartMoneySignalGenerator


def _profiles(tmp_path: Path, wallets: list[dict]) -> Path:
    path = tmp_path / "profiles.parquet"
    pd.DataFrame(wallets).to_parquet(path, index=False)
    return path


def _smart_wallet(name: str = "smart1", edge: float = 0.30, uet: int = 20) -> dict:
    return {
        "wallet": name, "total_trades": 100, "resolved_trades": 50,
        "wins": 40, "win_rate": 0.80, "early_trades": 30, "early_wins": 22,
        "uncertain_early_trades": uet, "uncertain_early_wins": 18,
        "early_win_rate": 0.82, "baseline_win_rate": 0.52,
        "edge": edge, "avg_hours_before_close": 48.0,
        "total_volume_usdc": 50000, "avg_trade_size": 500,
        "markets_traded": 20, "is_early_informed": True,
        "is_highly_informed": True, "smart_money": True,
    }


def _goldsky_fill(maker: str, token: str, amount: float = 100.0,
                  maker_asset: str = "0") -> dict:
    """Goldsky fill format. maker_asset_id=0 means maker BOUGHT tokens."""
    from datetime import datetime, timezone
    return {
        "id": f"f-{maker}-{token}",
        "timestamp": datetime.now(tz=timezone.utc),
        "maker_wallet": maker, "taker_wallet": "counterparty",
        "maker_amount": amount, "taker_amount": amount * 2,
        "maker_asset_id": maker_asset, "taker_asset_id": token,
        "fee": 0.5,
    }


class TestSmartMoneySignal:
    def test_maker_asset_0_is_buy(self, tmp_path: Path) -> None:
        """maker_asset_id == '0' means maker paid USDC = bought tokens (bullish)."""
        profiles = _profiles(tmp_path, [_smart_wallet("s1")])
        gen = SmartMoneySignalGenerator(profiles)
        fills = pd.DataFrame([_goldsky_fill("s1", "tok1", 100.0, maker_asset="0")])
        signals = gen.compute(fills)
        assert len(signals) == 1
        assert signals[0].smart_buy_volume == 100.0
        assert signals[0].smart_sell_volume == 0.0
        assert signals[0].direction == "YES"

    def test_maker_asset_nonzero_is_sell(self, tmp_path: Path) -> None:
        """maker_asset_id != '0' means maker sold tokens (bearish)."""
        profiles = _profiles(tmp_path, [_smart_wallet("s1")])
        gen = SmartMoneySignalGenerator(profiles)
        fills = pd.DataFrame([_goldsky_fill("s1", "tok1", 100.0, maker_asset="tok1")])
        signals = gen.compute(fills)
        assert len(signals) == 1
        assert signals[0].smart_sell_volume == 100.0
        assert signals[0].smart_buy_volume == 0.0
        assert signals[0].direction == "NO"

    def test_below_50_excluded(self, tmp_path: Path) -> None:
        profiles = _profiles(tmp_path, [_smart_wallet("s1")])
        gen = SmartMoneySignalGenerator(profiles)
        fills = pd.DataFrame([_goldsky_fill("s1", "tok1", 30.0)])
        signals = gen.compute(fills)
        assert signals == []

    def test_top_wallet_edge(self, tmp_path: Path) -> None:
        profiles = _profiles(tmp_path, [
            _smart_wallet("s1", edge=0.30, uet=20),
            _smart_wallet("s2", edge=0.80, uet=50),
        ])
        gen = SmartMoneySignalGenerator(profiles)
        fills = pd.DataFrame([
            _goldsky_fill("s1", "tok1", 60.0),
            _goldsky_fill("s2", "tok1", 40.0),
        ])
        signals = gen.compute(fills)
        assert len(signals) == 1
        assert signals[0].top_wallet_edge == pytest.approx(0.80)
        assert signals[0].top_wallet_uncertain_early_trades == 50

    def test_non_smart_excluded(self, tmp_path: Path) -> None:
        profiles = _profiles(tmp_path, [_smart_wallet("s1")])
        gen = SmartMoneySignalGenerator(profiles)
        fills = pd.DataFrame([_goldsky_fill("regular_wallet", "tok1", 200.0)])
        signals = gen.compute(fills)
        assert signals == []

    def test_neutral_balanced(self, tmp_path: Path) -> None:
        profiles = _profiles(tmp_path, [_smart_wallet("s1")])
        gen = SmartMoneySignalGenerator(profiles)
        fills = pd.DataFrame([
            _goldsky_fill("s1", "tok1", 55.0, maker_asset="0"),
            _goldsky_fill("s1", "tok1", 45.0, maker_asset="tok1"),
        ])
        signals = gen.compute(fills)
        assert len(signals) == 1
        assert signals[0].direction == "NEUTRAL"

    def test_sorted_by_abs_net(self, tmp_path: Path) -> None:
        profiles = _profiles(tmp_path, [_smart_wallet("s1")])
        gen = SmartMoneySignalGenerator(profiles)
        fills = pd.DataFrame([
            _goldsky_fill("s1", "tok1", 100.0),
            _goldsky_fill("s1", "tok2", 500.0),
        ])
        signals = gen.compute(fills)
        assert len(signals) == 2
        assert abs(signals[0].net_smart_volume) >= abs(signals[1].net_smart_volume)
