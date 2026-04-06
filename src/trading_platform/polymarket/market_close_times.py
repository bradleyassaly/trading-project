"""
Market close time map for Polymarket.

Maps canonical token IDs to their close/end dates, used by the
wallet profiler to compute ``hours_before_close`` per trade.

Usage::

    from trading_platform.polymarket.market_close_times import MarketCloseTimeMap
    ctm = MarketCloseTimeMap.from_live_db("data/polymarket/live/prices.db")
    close_dt = ctm.get("104173...")  # returns datetime or None
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from trading_platform.polymarket.resolution_resolver import normalize_token_id

logger = logging.getLogger(__name__)


class MarketCloseTimeMap:
    """Maps token IDs to market close times."""

    def __init__(self) -> None:
        self._map: dict[str, datetime] = {}

    @classmethod
    def from_gamma_csv(cls, csv_path: str | Path) -> "MarketCloseTimeMap":
        """Load close times from gamma_resolution.csv (highest priority source)."""
        import csv
        instance = cls()
        csv_path = Path(csv_path)
        if not csv_path.exists():
            return instance
        with csv_path.open(newline="", encoding="utf-8-sig") as fh:
            for row in csv.DictReader(fh):
                ticker = row.get("ticker", "").strip()
                ct = row.get("close_time", "").strip()
                if not ticker or not ct:
                    continue
                key = normalize_token_id(ticker)
                dt = cls._parse_datetime(ct)
                if dt:
                    instance._map[key] = dt
        logger.info("Loaded %d close times from %s", len(instance._map), csv_path)
        return instance

    @classmethod
    def from_all_sources(cls, gamma_csv: str | Path | None = None,
                         live_db: str | Path | None = None) -> "MarketCloseTimeMap":
        """Load close times from all available sources (gamma CSV first, then live DB)."""
        instance = cls()
        # Priority 1: gamma resolution CSV
        if gamma_csv:
            gamma = cls.from_gamma_csv(gamma_csv)
            instance._map.update(gamma._map)
        # Priority 2: live DB (fills gaps)
        if live_db:
            db = cls.from_live_db(live_db)
            for k, v in db._map.items():
                if k not in instance._map:
                    instance._map[k] = v
        logger.info("Total close times from all sources: %d", len(instance._map))
        return instance

    @staticmethod
    def _parse_datetime(s: str) -> datetime | None:
        """Parse a datetime string to UTC datetime."""
        if not s:
            return None
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
        except (ValueError, AttributeError):
            pass
        try:
            return datetime.strptime(s.strip(), "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except (ValueError, AttributeError):
            return None

    @classmethod
    def from_live_db(cls, db_path: str | Path) -> "MarketCloseTimeMap":
        """Load close times from the live collector SQLite database."""
        instance = cls()
        db_path = Path(db_path)
        if not db_path.exists():
            logger.warning("Live DB not found: %s", db_path)
            return instance

        try:
            conn = sqlite3.connect(str(db_path), check_same_thread=False)
            rows = conn.execute(
                "SELECT yes_token_id, end_date_iso FROM markets "
                "WHERE yes_token_id IS NOT NULL AND end_date_iso IS NOT NULL"
            ).fetchall()
            conn.close()
        except Exception as exc:
            logger.warning("Failed to read live DB: %s", exc)
            return instance

        for token_id, end_date_str in rows:
            key = normalize_token_id(token_id)
            try:
                dt = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                instance._map[key] = dt
            except (ValueError, AttributeError):
                # Try date-only format (e.g. "2026-03-31")
                try:
                    dt = datetime.strptime(end_date_str.strip(), "%Y-%m-%d").replace(tzinfo=timezone.utc)
                    instance._map[key] = dt
                except (ValueError, AttributeError):
                    continue

        logger.info("Loaded %d close times from %s", len(instance._map), db_path)
        return instance

    def get(self, token_id: str) -> datetime | None:
        """Return close time for a token ID (any format), or None."""
        key = normalize_token_id(token_id)
        return self._map.get(key)

    def coverage(self) -> dict[str, Any]:
        """Return coverage statistics."""
        return {
            "total_tokens": len(self._map),
            "with_close_time": len(self._map),
            "pct": 100.0 if self._map else 0.0,
        }
