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
                msg_type    TEXT    NOT NULL,
                best_bid    REAL,
                best_ask    REAL,
                spread      REAL,
                trade_size  REAL
            );
            CREATE INDEX IF NOT EXISTS idx_ticks_asset_ts
                ON ticks(asset_id, timestamp);
            CREATE INDEX IF NOT EXISTS idx_ticks_market_ts
                ON ticks(market_id, timestamp);
            CREATE TABLE IF NOT EXISTS markets (
                market_id      TEXT PRIMARY KEY,
                question       TEXT NOT NULL,
                volume         REAL,
                yes_token_id   TEXT,
                end_date_iso   TEXT
            );
        """)
        # Migrate old databases that lack newer columns
        for col, table in [
            ("end_date_iso TEXT", "markets"),
            ("best_bid REAL", "ticks"),
            ("best_ask REAL", "ticks"),
            ("spread REAL", "ticks"),
            ("trade_size REAL", "ticks"),
        ]:
            try:
                self._conn.execute(f"ALTER TABLE {table} ADD COLUMN {col}")
            except sqlite3.OperationalError:
                pass  # column already exists

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
        best_bid: float | None = None,
        best_ask: float | None = None,
        spread: float | None = None,
        trade_size: float | None = None,
    ) -> None:
        received_at = datetime.now(tz=timezone.utc).isoformat()
        with self._lock:
            self._conn.execute(
                """INSERT INTO ticks
                   (asset_id, market_id, price, side, size, timestamp, received_at, msg_type,
                    best_bid, best_ask, spread, trade_size)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (asset_id, market_id, price, side, size, timestamp, received_at, msg_type,
                 best_bid, best_ask, spread, trade_size),
            )
            self._conn.commit()

    def insert_ticks_batch(
        self,
        rows: list[tuple],
    ) -> int:
        """Batch insert ticks.

        Each tuple: ``(asset_id, market_id, price, side, size, timestamp, msg_type)``
        or extended: ``(..., best_bid, best_ask, spread, trade_size)`` (11 elements).
        """
        received_at = datetime.now(tz=timezone.utc).isoformat()
        expanded = []
        for row in rows:
            if len(row) >= 11:
                a, m, p, sd, sz, ts, mt, bb, ba, sp, tsz = row[:11]
            elif len(row) >= 10:
                a, m, p, sd, sz, ts, mt, bb, ba, sp = row[:10]
                tsz = None
            else:
                a, m, p, sd, sz, ts, mt = row[:7]
                bb, ba, sp, tsz = None, None, None, None
            expanded.append((a, m, p, sd, sz, ts, received_at, mt, bb, ba, sp, tsz))
        with self._lock:
            self._conn.executemany(
                """INSERT INTO ticks
                   (asset_id, market_id, price, side, size, timestamp, received_at, msg_type,
                    best_bid, best_ask, spread, trade_size)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                expanded,
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
        """Return the most recent last_trade_price tick per market_id."""
        with self._lock:
            rows = self._conn.execute("""
                SELECT market_id, price, timestamp
                FROM ticks
                WHERE msg_type = 'last_trade_price'
                  AND id IN (
                    SELECT MAX(id) FROM ticks
                    WHERE msg_type = 'last_trade_price'
                    GROUP BY market_id
                  )
            """).fetchall()
        return {
            r[0]: {"price": r[1], "timestamp": r[2]}
            for r in rows
        }

    def upsert_market_info(self, market_id: str, question: str, volume: float,
                           yes_token_id: str, end_date_iso: str | None = None) -> None:
        with self._lock:
            self._conn.execute(
                """INSERT OR REPLACE INTO markets
                   (market_id, question, volume, yes_token_id, end_date_iso)
                   VALUES (?, ?, ?, ?, ?)""",
                (market_id, question, volume, yes_token_id, end_date_iso),
            )
            self._conn.commit()

    def upsert_markets_batch(self, rows: list[tuple[str, str, float, str, str | None]]) -> None:
        """Batch upsert market metadata.

        Each tuple: ``(market_id, question, volume, yes_token_id, end_date_iso)``.
        """
        with self._lock:
            self._conn.executemany(
                """INSERT OR REPLACE INTO markets
                   (market_id, question, volume, yes_token_id, end_date_iso)
                   VALUES (?, ?, ?, ?, ?)""",
                rows,
            )
            self._conn.commit()

    def get_market_info(self) -> dict[str, dict[str, Any]]:
        """Return all market metadata keyed by market_id."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT market_id, question, volume, yes_token_id, end_date_iso FROM markets"
            ).fetchall()
        return {
            r[0]: {"question": r[1], "volume": r[2], "yes_token_id": r[3], "end_date_iso": r[4]}
            for r in rows
        }

    def get_ticks_for_market(self, market_id: str, *, limit: int = 500) -> list[dict[str, Any]]:
        """Return the most recent ticks for a market in ascending timestamp order."""
        with self._lock:
            rows = self._conn.execute(
                """SELECT price, timestamp FROM ticks
                   WHERE market_id = ?
                   ORDER BY id DESC LIMIT ?""",
                (market_id, limit),
            ).fetchall()
        # Reverse to ascending order
        return [{"price": r[0], "timestamp": r[1]} for r in reversed(rows)]

    def get_tick_counts(self) -> dict[str, int]:
        """Return tick count per market_id."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT market_id, COUNT(*) FROM ticks GROUP BY market_id"
            ).fetchall()
        return {r[0]: r[1] for r in rows}

    def close(self) -> None:
        with self._lock:
            self._conn.close()
