"""Tests for MarketCloseTimeMap."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from trading_platform.polymarket.market_close_times import MarketCloseTimeMap


def _make_db(tmp_path: Path, rows: list[tuple[str, str, str]]) -> Path:
    """Create a minimal SQLite DB with markets table."""
    import sqlite3
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE markets (
            market_id TEXT PRIMARY KEY,
            question TEXT,
            volume REAL,
            yes_token_id TEXT,
            end_date_iso TEXT
        )
    """)
    for market_id, token_id, end_date in rows:
        conn.execute(
            "INSERT INTO markets (market_id, question, volume, yes_token_id, end_date_iso) VALUES (?, ?, ?, ?, ?)",
            (market_id, "Q?", 1000, token_id, end_date),
        )
    conn.commit()
    conn.close()
    return db_path


class TestMarketCloseTimeMap:
    def test_get_returns_none_for_unknown(self, tmp_path: Path) -> None:
        db_path = _make_db(tmp_path, [("m1", "tok1", "2026-04-15")])
        ctm = MarketCloseTimeMap.from_live_db(db_path)
        assert ctm.get("unknown_token") is None

    def test_get_normalizes_token_id(self, tmp_path: Path) -> None:
        """Token stored as decimal, queried as hex → should match."""
        db_path = _make_db(tmp_path, [("m1", "255", "2026-05-01")])
        ctm = MarketCloseTimeMap.from_live_db(db_path)
        result = ctm.get("0xff")  # hex for 255
        assert result is not None
        assert result.year == 2026
        assert result.month == 5

    def test_get_returns_datetime(self, tmp_path: Path) -> None:
        db_path = _make_db(tmp_path, [("m1", "12345", "2026-06-15T12:00:00Z")])
        ctm = MarketCloseTimeMap.from_live_db(db_path)
        result = ctm.get("12345")
        assert result is not None
        assert isinstance(result, datetime)
        assert result.tzinfo is not None

    def test_coverage_keys(self, tmp_path: Path) -> None:
        db_path = _make_db(tmp_path, [
            ("m1", "t1", "2026-04-15"),
            ("m2", "t2", "2026-05-01"),
        ])
        ctm = MarketCloseTimeMap.from_live_db(db_path)
        cov = ctm.coverage()
        assert "total_tokens" in cov
        assert "with_close_time" in cov
        assert "pct" in cov
        assert cov["total_tokens"] == 2

    def test_missing_db(self, tmp_path: Path) -> None:
        ctm = MarketCloseTimeMap.from_live_db(tmp_path / "missing.db")
        assert ctm.get("anything") is None
        assert ctm.coverage()["total_tokens"] == 0
