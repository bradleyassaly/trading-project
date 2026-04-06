"""Tests for AlertLog and open positions computation."""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from trading_platform.polymarket.alert_log import AlertLog


class TestAlertLog:
    def test_append_and_query(self, tmp_path: Path) -> None:
        log = AlertLog(tmp_path / "alerts.jsonl")
        log.append(wallet="0xaaa", token_id="tok1", market_question="Test?",
                   side="YES", amount_usdc=500.0, wallet_edge=0.30,
                   early_win_rate=0.85, tier=1)
        log.append(wallet="0xbbb", token_id="tok2", market_question="Test2?",
                   side="NO", amount_usdc=200.0, wallet_edge=0.20,
                   early_win_rate=0.70, tier=2)

        # Query all
        results = log.query(limit=10)
        assert len(results) == 2
        # Most recent first
        assert results[0]["wallet"] == "0xbbb"
        assert results[1]["wallet"] == "0xaaa"

    def test_query_by_wallet(self, tmp_path: Path) -> None:
        log = AlertLog(tmp_path / "alerts.jsonl")
        log.append(wallet="0xaaa", token_id="t1", market_question="Q1",
                   side="YES", amount_usdc=100, wallet_edge=0.3,
                   early_win_rate=0.8, tier=1)
        log.append(wallet="0xbbb", token_id="t2", market_question="Q2",
                   side="NO", amount_usdc=200, wallet_edge=0.2,
                   early_win_rate=0.7, tier=2)

        results = log.query(wallet="0xaaa")
        assert len(results) == 1
        assert results[0]["wallet"] == "0xaaa"

    def test_query_by_tier(self, tmp_path: Path) -> None:
        log = AlertLog(tmp_path / "alerts.jsonl")
        log.append(wallet="0xa", token_id="t1", market_question="Q",
                   side="YES", amount_usdc=100, wallet_edge=0.3,
                   early_win_rate=0.8, tier=1)
        log.append(wallet="0xb", token_id="t2", market_question="Q",
                   side="NO", amount_usdc=200, wallet_edge=0.2,
                   early_win_rate=0.7, tier=2)

        assert len(log.query(tier=1)) == 1
        assert len(log.query(tier=2)) == 1

    def test_query_limit(self, tmp_path: Path) -> None:
        log = AlertLog(tmp_path / "alerts.jsonl")
        for i in range(10):
            log.append(wallet=f"0x{i}", token_id=f"t{i}", market_question="Q",
                       side="YES", amount_usdc=100, wallet_edge=0.3,
                       early_win_rate=0.8, tier=1)
        assert len(log.query(limit=3)) == 3

    def test_query_empty_file(self, tmp_path: Path) -> None:
        log = AlertLog(tmp_path / "nonexistent.jsonl")
        assert log.query() == []

    def test_schema_fields(self, tmp_path: Path) -> None:
        log = AlertLog(tmp_path / "alerts.jsonl")
        log.append(wallet="0xw", token_id="tid", market_question="MQ?",
                   side="YES", amount_usdc=500.0, wallet_edge=0.30,
                   early_win_rate=0.85, tier=1, hours_since_close=48.5)
        entry = log.query(limit=1)[0]
        assert "ts" in entry
        assert entry["wallet"] == "0xw"
        assert entry["token_id"] == "tid"
        assert entry["market_question"] == "MQ?"
        assert entry["side"] == "YES"
        assert entry["amount_usdc"] == 500.0
        assert entry["wallet_edge"] == 0.30
        assert entry["early_win_rate"] == 0.85
        assert entry["tier"] == 1
        assert entry["hours_since_close"] == 48.5


class TestOpenPositions:
    def test_basic_computation(self, tmp_path: Path) -> None:
        import csv
        from trading_platform.polymarket.open_positions import compute_open_positions

        # Create profiles
        profiles = pd.DataFrame([{
            "wallet": "0xaaa", "edge": 0.30, "early_win_rate": 0.80,
            "uncertain_early_trades": 20, "total_volume_usdc": 50000,
            "is_early_informed": True, "smart_money": True,
            "win_rate": 0.65, "resolved_trades": 50, "wins": 32,
        }])
        profiles_path = tmp_path / "profiles.parquet"
        profiles.to_parquet(profiles_path, index=False)

        # Create fills — wallet buys 2 tokens, one resolved and one not
        from datetime import datetime, timezone
        fills = pd.DataFrame([
            {"id": "f1", "timestamp": datetime(2026, 4, 1, tzinfo=timezone.utc),
             "maker_wallet": "0xaaa", "taker_wallet": "cp",
             "maker_amount": 200.0, "taker_amount": 400.0,
             "maker_asset_id": "0", "taker_asset_id": "tok_open", "fee": 0},
            {"id": "f2", "timestamp": datetime(2026, 4, 1, tzinfo=timezone.utc),
             "maker_wallet": "0xaaa", "taker_wallet": "cp",
             "maker_amount": 100.0, "taker_amount": 200.0,
             "maker_asset_id": "0", "taker_asset_id": "tok_resolved", "fee": 0},
        ])
        fills_dir = tmp_path / "fills"
        fills_dir.mkdir()
        fills.to_parquet(fills_dir / "test.parquet", index=False)

        # Resolution CSV — tok_resolved is resolved, tok_open is NOT
        res_path = tmp_path / "resolution.csv"
        with res_path.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["ticker", "resolution_price", "question", "close_time"])
            w.writeheader()
            w.writerow({"ticker": "tok_resolved", "resolution_price": "100.0",
                        "question": "Resolved?", "close_time": "2026-04-01"})
            w.writerow({"ticker": "tok_open", "resolution_price": "",
                        "question": "Still open?", "close_time": "2026-12-01"})

        df = compute_open_positions(
            profiles_path=profiles_path,
            fills_dir=fills_dir,
            resolution_csv=res_path,
            min_edge=0.20,
        )
        # Only tok_open should be in results (tok_resolved is excluded)
        assert len(df) == 1
        assert df.iloc[0]["token_id"] == "tok_open"
        assert df.iloc[0]["net_side"] == "YES"
        assert df.iloc[0]["net_amount_usdc"] == 200.0

    def test_empty_when_no_profiles(self, tmp_path: Path) -> None:
        from trading_platform.polymarket.open_positions import compute_open_positions
        df = compute_open_positions(profiles_path=tmp_path / "missing.parquet")
        assert df.empty

    def test_filters_by_min_edge(self, tmp_path: Path) -> None:
        from trading_platform.polymarket.open_positions import compute_open_positions
        profiles = pd.DataFrame([
            {"wallet": "0xlow", "edge": 0.10, "early_win_rate": 0.55,
             "uncertain_early_trades": 20, "total_volume_usdc": 50000,
             "is_early_informed": False, "smart_money": False,
             "win_rate": 0.55, "resolved_trades": 50, "wins": 27},
        ])
        profiles_path = tmp_path / "profiles.parquet"
        profiles.to_parquet(profiles_path, index=False)
        df = compute_open_positions(profiles_path=profiles_path, min_edge=0.20)
        assert df.empty  # edge 0.10 < 0.20 threshold
