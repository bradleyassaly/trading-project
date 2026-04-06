"""Tests for PolymarketPaperExecutor."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from trading_platform.polymarket.polymarket_paper_executor import PolymarketPaperExecutor
from trading_platform.polymarket.smart_money_signal import SmartMoneySignal


def _signal(
    token: str = "tok1_long_id_here_78_chars",
    direction: str = "YES",
    confidence: float = 0.80,
    edge: float = 0.75,
    net_vol: float = 10000.0,
) -> SmartMoneySignal:
    return SmartMoneySignal(
        token_id=token,
        net_smart_volume=net_vol,
        weighted_net_volume=net_vol * 0.8,
        smart_buy_volume=net_vol,
        smart_sell_volume=0.0,
        smart_trade_count=50,
        direction=direction,
        confidence=confidence,
        top_wallet="0xsmart",
        top_wallet_edge=edge,
        top_wallet_uncertain_early_trades=20,
        most_recent_smart_trade="2026-04-04T21:00:00Z",
        hours_since_last_smart_trade=1.0,
    )


class TestPolymarketPaperExecutor:
    def test_trade_placed(self, tmp_path: Path) -> None:
        executor = PolymarketPaperExecutor(tmp_path / "paper.db")
        placed = executor.execute_trade(_signal(), market_price=0.65)
        assert placed is True
        summary = executor.get_summary()
        assert summary["total_trades"] == 1
        assert summary["platform"] == "polymarket"
        executor.close()

    def test_skipped_low_confidence(self, tmp_path: Path) -> None:
        executor = PolymarketPaperExecutor(tmp_path / "paper.db")
        assert executor.execute_trade(_signal(confidence=0.40), market_price=0.65) is False
        executor.close()

    def test_skipped_low_edge(self, tmp_path: Path) -> None:
        executor = PolymarketPaperExecutor(tmp_path / "paper.db")
        assert executor.execute_trade(_signal(edge=0.30), market_price=0.65) is False
        executor.close()

    def test_skipped_extreme_price(self, tmp_path: Path) -> None:
        executor = PolymarketPaperExecutor(tmp_path / "paper.db")
        assert executor.execute_trade(_signal(), market_price=0.97) is False
        assert executor.execute_trade(_signal(), market_price=0.02) is False
        executor.close()

    def test_no_duplicate_positions(self, tmp_path: Path) -> None:
        executor = PolymarketPaperExecutor(tmp_path / "paper.db")
        assert executor.execute_trade(_signal("tok_dup"), market_price=0.65) is True
        assert executor.execute_trade(_signal("tok_dup"), market_price=0.65) is False
        executor.close()

    def test_kelly_sizing(self, tmp_path: Path) -> None:
        executor = PolymarketPaperExecutor(tmp_path / "paper.db")
        executor.execute_trade(_signal(edge=0.75), market_price=0.65)
        trades = executor._conn.execute("SELECT size_usd FROM trades WHERE platform='polymarket'").fetchall()
        stake = trades[0][0]
        assert 5.0 <= stake <= 15.0
        executor.close()

    def test_check_resolutions_win(self, tmp_path: Path) -> None:
        executor = PolymarketPaperExecutor(tmp_path / "paper.db")
        executor.execute_trade(_signal("winning_tok"), market_price=0.65)

        resolver = MagicMock()
        resolver.resolve.return_value = 100.0  # YES won

        resolved = executor.check_resolutions(resolver)
        assert len(resolved) == 1
        assert resolved[0]["outcome"] == "win"
        assert executor.get_summary()["wins"] == 1
        executor.close()

    def test_check_resolutions_loss(self, tmp_path: Path) -> None:
        executor = PolymarketPaperExecutor(tmp_path / "paper.db")
        executor.execute_trade(_signal("losing_tok", direction="YES"), market_price=0.65)

        resolver = MagicMock()
        resolver.resolve.return_value = 0.0  # NO won

        resolved = executor.check_resolutions(resolver)
        assert len(resolved) == 1
        assert resolved[0]["outcome"] == "loss"
        executor.close()
