"""Tests for smart money trading loop."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from trading_platform.polymarket.smart_money_loop import run_smart_money_cycle


def _profiles(tmp_path: Path) -> Path:
    path = tmp_path / "profiles.parquet"
    pd.DataFrame([{
        "wallet": "smart1", "total_trades": 100, "resolved_trades": 50,
        "wins": 40, "win_rate": 0.80, "early_trades": 30, "early_wins": 22,
        "uncertain_early_trades": 20, "uncertain_early_wins": 18,
        "early_win_rate": 0.82, "baseline_win_rate": 0.52,
        "edge": 0.60, "avg_hours_before_close": 48.0,
        "total_volume_usdc": 50000, "avg_trade_size": 500,
        "markets_traded": 20, "is_early_informed": True,
        "is_highly_informed": True, "smart_money": True,
    }]).to_parquet(path, index=False)
    return path


def _fills(tmp_path: Path, maker: str = "smart1", amount: float = 10000.0) -> Path:
    fills_dir = tmp_path / "fills"
    fills_dir.mkdir()
    pd.DataFrame([{
        "id": "f1", "timestamp": datetime.now(tz=timezone.utc),
        "maker_wallet": maker, "taker_wallet": "other",
        "maker_amount": amount, "taker_amount": amount * 2,
        "maker_asset_id": "0", "taker_asset_id": "tok_live_1",
        "fee": 0.5,
    }]).to_parquet(fills_dir / "tok_live_1.parquet", index=False)
    return fills_dir


class TestSmartMoneyLoop:
    def test_qualifying_signal_places_trade(self, tmp_path: Path) -> None:
        profiles = _profiles(tmp_path)
        fills_dir = _fills(tmp_path, amount=10000.0)

        executor = MagicMock()
        executor.execute_trade.return_value = True

        summary = run_smart_money_cycle(
            executor,
            profiles_path=profiles,
            fills_dir=fills_dir,
            min_confidence=0.50,
            min_net_volume=50,
            min_edge=0.50,
        )
        assert summary["trades_placed"] == 1
        executor.execute_trade.assert_called_once()

    def test_below_confidence_skipped(self, tmp_path: Path) -> None:
        profiles = _profiles(tmp_path)
        fills_dir = _fills(tmp_path, amount=10000.0)

        executor = MagicMock()

        summary = run_smart_money_cycle(
            executor,
            profiles_path=profiles,
            fills_dir=fills_dir,
            min_confidence=2.0,  # impossible threshold
            min_net_volume=50,
            min_edge=0.50,
        )
        assert summary["trades_placed"] == 0
        executor.execute_trade.assert_not_called()

    def test_duplicate_positions_not_reentered(self, tmp_path: Path) -> None:
        profiles = _profiles(tmp_path)
        fills_dir = _fills(tmp_path, amount=10000.0)

        executor = MagicMock()
        executor.execute_trade.return_value = False  # already has position

        summary = run_smart_money_cycle(
            executor,
            profiles_path=profiles,
            fills_dir=fills_dir,
            min_confidence=0.50,
            min_net_volume=50,
            min_edge=0.50,
        )
        assert summary["trades_placed"] == 0  # executor returned False
