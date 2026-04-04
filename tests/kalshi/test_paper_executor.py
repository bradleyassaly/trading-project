"""Tests for Kalshi paper trade executor."""
from __future__ import annotations

from pathlib import Path

import pytest

from trading_platform.kalshi.paper_executor import KalshiPaperExecutor
from trading_platform.kalshi.market_scanner import ScanResult


def _scan_result(
    ticker: str = "KXFED-27APR-T4.25",
    confidence: float = 0.5,
    side: str = "YES",
    news: str = "unscheduled",
) -> ScanResult:
    return ScanResult(
        ticker=ticker, yes_price=55.0, days_to_close=10,
        signal_scores={"cal_drift": 1.5}, strongest_signal="cal_drift",
        recommended_side=side, confidence=confidence,
        news_context=news, kelly_fraction=min(0.05, confidence * 0.1),
    )


class TestKalshiPaperExecutor:
    def test_creates_db(self, tmp_path: Path) -> None:
        db = tmp_path / "paper.db"
        executor = KalshiPaperExecutor(db)
        assert db.exists()
        summary = executor.get_summary()
        assert summary["cash_usd"] == 500.0
        assert summary["total_trades"] == 0
        executor.close()

    def test_execute_trade(self, tmp_path: Path) -> None:
        executor = KalshiPaperExecutor(tmp_path / "paper.db")
        result = _scan_result(confidence=0.5)
        executed = executor.execute_trade(result)
        assert executed is True

        summary = executor.get_summary()
        assert summary["total_trades"] == 1
        assert summary["open_trades"] == 1
        assert summary["cash_usd"] < 500.0
        executor.close()

    def test_no_duplicate_positions(self, tmp_path: Path) -> None:
        executor = KalshiPaperExecutor(tmp_path / "paper.db")
        r = _scan_result()
        executor.execute_trade(r)
        second = executor.execute_trade(r)
        assert second is False
        assert executor.get_summary()["total_trades"] == 1
        executor.close()

    def test_skip_scheduled_release(self, tmp_path: Path) -> None:
        executor = KalshiPaperExecutor(tmp_path / "paper.db")
        r = _scan_result(news="scheduled_release")
        assert executor.execute_trade(r) is False
        executor.close()

    def test_get_recent_trades(self, tmp_path: Path) -> None:
        executor = KalshiPaperExecutor(tmp_path / "paper.db")
        executor.execute_trade(_scan_result("T1"))
        executor.execute_trade(_scan_result("T2"))
        trades = executor.get_recent_trades()
        assert len(trades) == 2
        assert trades[0]["ticker"] == "T2"  # most recent first
        executor.close()

    def test_check_resolutions_win(self, tmp_path: Path) -> None:
        executor = KalshiPaperExecutor(tmp_path / "paper.db")
        executor.execute_trade(_scan_result("WINNER", side="YES"))

        class FakeClient:
            def get_markets_raw(self, **kw):
                return [{"ticker": "WINNER", "status": "settled", "result": "yes"}], None

        resolved = executor.check_resolutions(FakeClient())
        assert resolved == 1
        summary = executor.get_summary()
        assert summary["wins"] == 1
        assert summary["closed_trades"] == 1
        assert summary["cash_usd"] > 500.0  # profit credited
        executor.close()

    def test_check_resolutions_loss(self, tmp_path: Path) -> None:
        executor = KalshiPaperExecutor(tmp_path / "paper.db")
        executor.execute_trade(_scan_result("LOSER", side="YES"))

        class FakeClient:
            def get_markets_raw(self, **kw):
                return [{"ticker": "LOSER", "status": "settled", "result": "no"}], None

        resolved = executor.check_resolutions(FakeClient())
        assert resolved == 1
        summary = executor.get_summary()
        assert summary["wins"] == 0
        assert summary["cash_usd"] < 500.0  # lost the trade
        executor.close()
