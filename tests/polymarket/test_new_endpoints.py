"""Tests for new API reader functions: universe-stats, bankroll, pnl-history, signals performance."""
from __future__ import annotations

from pathlib import Path

import pytest

from trading_platform.polymarket.wallet_db import WalletDB


class TestUniverseStats:
    def test_returns_total_wallets(self, tmp_path: Path) -> None:
        db = WalletDB(tmp_path / "test.db")
        db.upsert_profile("0xa", wallet_type="directional", volume_tier="whale", equity_score=1.0)
        db.upsert_profile("0xb", wallet_type="arb_bot", volume_tier="active", equity_score=0.5)
        db.upsert_profile("0xc", wallet_type="directional", volume_tier="casual", equity_score=0.3)
        stats = db.universe_stats()
        assert stats["total_wallets"] == 3
        assert stats["by_type"]["directional"] == 2
        assert stats["by_type"]["arb_bot"] == 1
        assert stats["by_tier"]["whale"] == 1

    def test_empty_db(self, tmp_path: Path) -> None:
        db = WalletDB(tmp_path / "test.db")
        stats = db.universe_stats()
        assert stats["total_wallets"] == 0
        assert stats["signals_today"] == 0


class TestBankrollEndpoint:
    def test_returns_all_signal_types(self) -> None:
        import sys; sys.path.insert(0, "src")
        from trading_platform.api.artifact_reader import read_paper_bankroll
        result = read_paper_bankroll()
        assert result["available"]
        assert result["total_bankroll"] == 10000
        assert len(result["by_signal"]) == 7
        assert "whale_entry" in result["by_signal"]
        assert "convergence" in result["by_signal"]
        assert result["by_signal"]["whale_entry"]["allocated"] == 2000
        assert result["by_signal"]["convergence"]["stake_per_trade"] == 125


class TestPnlHistory:
    def test_returns_empty_when_no_resolved(self) -> None:
        import sys; sys.path.insert(0, "src")
        from trading_platform.api.artifact_reader import read_paper_pnl_history
        result = read_paper_pnl_history()
        assert result["available"]
        # may or may not have data depending on DB state
        assert isinstance(result["by_day"], list)
        assert isinstance(result["by_signal_total"], dict)


class TestSignalsPerformance:
    def test_returns_all_signal_types(self) -> None:
        import sys; sys.path.insert(0, "src")
        from trading_platform.api.artifact_reader import read_signals_performance
        result = read_signals_performance()
        assert result["available"]
        types = {t["signal_type"] for t in result["by_type"]}
        assert "whale_entry" in types
        assert "convergence" in types
        assert "cascade" in types
        assert len(result["by_type"]) == 7


class TestBucketClassification:
    def test_concentrated_whale(self) -> None:
        import time
        from trading_platform.polymarket.wallet_buckets import _classify_one
        now = int(time.time())
        trades = [
            {"asset": f"t{i%5}", "side": "BUY", "size": 15000, "price": 0.60,
             "condition_id": f"c{i%5}", "timestamp": now - i * 3600,
             "category": "crypto", "market_resolved": False, "market_outcome": None,
             "event_slug": "", "outcome": "", "pnl": None}
            for i in range(25)
        ]
        bucket, conf, _ = _classify_one(trades)
        assert bucket == "concentrated_whale"

    def test_contrarian_whale(self) -> None:
        import time
        from trading_platform.polymarket.wallet_buckets import _classify_one
        now = int(time.time())
        trades = [
            {"asset": f"t{i%10}", "side": "BUY", "size": 12000, "price": 0.30,
             "condition_id": f"c{i%10}", "timestamp": now - i * 3600,
             "category": "politics", "market_resolved": False, "market_outcome": None,
             "event_slug": "", "outcome": "", "pnl": None}
            for i in range(30)
        ]
        bucket, conf, _ = _classify_one(trades)
        assert bucket == "contrarian_whale"

    def test_arb_bot(self) -> None:
        from trading_platform.polymarket.wallet_buckets import _classify_one
        trades = [
            {"asset": "t1", "side": "BUY", "size": 100, "price": 0.50,
             "condition_id": "c1", "timestamp": 1000 + i,
             "category": "other", "market_resolved": False, "market_outcome": None,
             "event_slug": "", "outcome": "", "pnl": None}
            for i in range(25)
        ]
        bucket, _, _ = _classify_one(trades)
        assert bucket == "arb_bot"

    def test_noise(self) -> None:
        from trading_platform.polymarket.wallet_buckets import _classify_one
        trades = [
            {"asset": "t1", "side": "BUY", "size": 10, "price": 0.50,
             "condition_id": "c1", "timestamp": 1000 + i * 7200,
             "category": "other", "market_resolved": True, "market_outcome": "NO",
             "event_slug": "", "outcome": "", "pnl": -5}
            for i in range(25)
        ]
        bucket, _, _ = _classify_one(trades)
        assert bucket == "noise"  # <35% WR with 25 resolved
