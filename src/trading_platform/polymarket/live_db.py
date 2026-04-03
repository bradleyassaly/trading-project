"""
SQLite tick storage for the Polymarket live collector.

Stores raw price ticks in a WAL-mode SQLite database for fast concurrent
writes (from the WebSocket collector) and reads (from the FastAPI endpoint
and hourly parquet exporter).
"""
from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class LiveTickStore:
    """Thread-safe SQLite tick store for Polymarket live price data."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._create_tables()

    def _create_tables(self) -> None:
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS ticks (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                asset_id    TEXT    NOT NULL,
                market_id   TEXT    NOT NULL,
                price       REAL    NOT NULL,
                side        TEXT,
                size        REAL,
                timestamp   TEXT    NOT NULL,
                received_at TEXT    NOT NULL,
                msg_type    TEXT    NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_ticks_asset_ts
                ON ticks(asset_id, timestamp);
            CREATE INDEX IF NOT EXISTS idx_ticks_market_ts
                ON ticks(market_id, timestamp);
        """)

    def insert_tick(
        self,
        *,
        asset_id: str,
        market_id: str,
        price: float,
        timestamp: str,
        msg_type: str,
        side: str | None = None,
        size: float | None = None,
    ) -> None:
        received_at = datetime.now(tz=timezone.utc).isoformat()
        with self._lock:
            self._conn.execute(
                """INSERT INTO ticks
                   (asset_id, market_id, price, side, size, timestamp, received_at, msg_type)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (asset_id, market_id, price, side, size, timestamp, received_at, msg_type),
            )
            self._conn.commit()

    def insert_ticks_batch(self, rows: list[tuple[str, str, float, str | None, float | None, str, str]]) -> int:
        """Batch insert ticks. Each tuple: (asset_id, market_id, price, side, size, timestamp, msg_type)."""
        received_at = datetime.now(tz=timezone.utc).isoformat()
        with self._lock:
            self._conn.executemany(
                """INSERT INTO ticks
                   (asset_id, market_id, price, side, size, timestamp, received_at, msg_type)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                [(a, m, p, sd, sz, ts, received_at, mt) for a, m, p, sd, sz, ts, mt in rows],
            )
            self._conn.commit()
        return len(rows)

    def get_ticks_for_hour(
        self,
        market_id: str,
        hour_start: str,
        hour_end: str,
        *,
        msg_type: str | None = "last_trade_price",
    ) -> list[dict[str, Any]]:
        """Return ticks for a market within [hour_start, hour_end)."""
        query = """
            SELECT price, timestamp FROM ticks
            WHERE market_id = ? AND timestamp >= ? AND timestamp < ?
        """
        params: list[Any] = [market_id, hour_start, hour_end]
        if msg_type is not None:
            query += " AND msg_type = ?"
            params.append(msg_type)
        query += " ORDER BY timestamp"
        with self._lock:
            rows = self._conn.execute(query, params).fetchall()
        return [{"price": r[0], "timestamp": r[1]} for r in rows]

    def get_latest_prices(self) -> dict[str, dict[str, Any]]:
        """Return the most recent tick per market_id."""
        with self._lock:
            rows = self._conn.execute("""
                SELECT market_id, price, timestamp
                FROM ticks
                WHERE id IN (
                    SELECT MAX(id) FROM ticks GROUP BY market_id
                )
            """).fetchall()
        return {
            r[0]: {"price": r[1], "timestamp": r[2]}
            for r in rows
        }

    def close(self) -> None:
        with self._lock:
            self._conn.close()
