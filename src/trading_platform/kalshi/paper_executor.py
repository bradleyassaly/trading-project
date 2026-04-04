"""
Kalshi paper trade executor.

Tracks paper trades in SQLite, manages a simulated portfolio,
and records outcomes when markets resolve.

Usage::

    from trading_platform.kalshi.paper_executor import KalshiPaperExecutor
    executor = KalshiPaperExecutor("data/kalshi/paper_trades.db")
    executor.execute_trade(scan_result)
    executor.check_resolutions(client)
    print(executor.get_summary())
"""
from __future__ import annotations

import json
import sqlite3
import threading
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import logging

logger = logging.getLogger(__name__)

_STARTING_CASH = 500.0
_MAX_TRADE_SIZE = 15.0
_MIN_TRADE_SIZE = 5.0


class KalshiPaperExecutor:
    """SQLite-backed paper trade tracker for Kalshi markets."""

    def __init__(self, db_path: str | Path = "data/kalshi/paper_trades.db") -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._create_tables()

    def _create_tables(self) -> None:
        self._conn.executescript(f"""
            CREATE TABLE IF NOT EXISTS trades (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker          TEXT NOT NULL,
                side            TEXT NOT NULL,
                entry_price     REAL NOT NULL,
                size_usd        REAL NOT NULL,
                signal_family   TEXT,
                confidence      REAL,
                news_context    TEXT,
                entry_ts        TEXT NOT NULL,
                exit_price      REAL,
                exit_ts         TEXT,
                outcome         TEXT,
                return_pct      REAL,
                status          TEXT NOT NULL DEFAULT 'open'
            );
            CREATE TABLE IF NOT EXISTS portfolio (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                ts              TEXT NOT NULL,
                cash_usd        REAL NOT NULL,
                open_value      REAL NOT NULL DEFAULT 0,
                total_value     REAL NOT NULL,
                realized_pnl    REAL NOT NULL DEFAULT 0
            );
            INSERT OR IGNORE INTO portfolio (id, ts, cash_usd, open_value, total_value, realized_pnl)
                SELECT 1, '{datetime.now(tz=timezone.utc).isoformat()}', {_STARTING_CASH}, 0, {_STARTING_CASH}, 0
                WHERE NOT EXISTS (SELECT 1 FROM portfolio);
        """)

    def _get_cash(self) -> float:
        row = self._conn.execute(
            "SELECT cash_usd FROM portfolio ORDER BY id DESC LIMIT 1"
        ).fetchone()
        return float(row[0]) if row else _STARTING_CASH

    def _get_realized_pnl(self) -> float:
        row = self._conn.execute(
            "SELECT realized_pnl FROM portfolio ORDER BY id DESC LIMIT 1"
        ).fetchone()
        return float(row[0]) if row else 0.0

    def execute_trade(self, result: Any) -> bool:
        """Place a paper trade from a ScanResult. Returns True if executed."""
        from trading_platform.kalshi.market_scanner import ScanResult

        with self._lock:
            # Check no duplicate open position
            existing = self._conn.execute(
                "SELECT id FROM trades WHERE ticker = ? AND status = 'open'",
                (result.ticker,),
            ).fetchone()
            if existing:
                logger.debug("Skipping %s: already have open position", result.ticker)
                return False

            # Don't trade into scheduled releases
            if result.news_context == "scheduled_release":
                logger.debug("Skipping %s: scheduled_release context", result.ticker)
                return False

            cash = self._get_cash()
            if cash < 10.0:
                logger.debug("Skipping %s: insufficient cash ($%.2f)", result.ticker, cash)
                return False

            total_value = cash  # simplified — open positions counted separately
            size = min(_MAX_TRADE_SIZE, result.kelly_fraction * total_value)
            size = max(size, _MIN_TRADE_SIZE)  # floor at minimum
            size = min(size, cash)
            if size < _MIN_TRADE_SIZE:
                return False

            now = datetime.now(tz=timezone.utc).isoformat()
            self._conn.execute(
                """INSERT INTO trades
                   (ticker, side, entry_price, size_usd, signal_family,
                    confidence, news_context, entry_ts, status)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'open')""",
                (result.ticker, result.recommended_side, result.yes_price,
                 round(size, 2), result.strongest_signal, result.confidence,
                 result.news_context, now),
            )
            # Deduct cash
            new_cash = cash - size
            realized = self._get_realized_pnl()
            self._conn.execute(
                "INSERT INTO portfolio (ts, cash_usd, open_value, total_value, realized_pnl) VALUES (?, ?, ?, ?, ?)",
                (now, round(new_cash, 2), round(size, 2), round(new_cash + size, 2), realized),
            )
            self._conn.commit()
            logger.info("Paper trade: %s %s @ %.1f¢ size=$%.2f signal=%s",
                         result.recommended_side, result.ticker, result.yes_price,
                         size, result.strongest_signal)
            return True

    def check_resolutions(self, client: Any) -> int:
        """Check open trades for resolved markets. Returns count resolved."""
        with self._lock:
            open_trades = self._conn.execute(
                "SELECT id, ticker, side, entry_price, size_usd FROM trades WHERE status = 'open'"
            ).fetchall()

        resolved = 0
        for trade_id, ticker, side, entry_price, size_usd in open_trades:
            try:
                markets, _ = client.get_markets_raw(tickers=[ticker], limit=1)
                if not markets:
                    continue
                market = markets[0]
            except Exception:
                continue

            status = market.get("status", "")
            result_str = market.get("result", "")
            if status != "settled" or not result_str:
                continue

            # Determine outcome
            resolved_yes = result_str.lower() == "yes"
            won = (side == "YES" and resolved_yes) or (side == "NO" and not resolved_yes)
            outcome = "win" if won else "loss"

            # Calculate return
            if won:
                exit_price = 100.0
                return_pct = (100.0 - entry_price) / entry_price if entry_price > 0 else 0
            else:
                exit_price = 0.0
                return_pct = -1.0  # total loss

            payout = size_usd * (1 + return_pct) if won else 0.0
            now = datetime.now(tz=timezone.utc).isoformat()

            with self._lock:
                self._conn.execute(
                    """UPDATE trades SET exit_price=?, exit_ts=?, outcome=?,
                       return_pct=?, status='closed' WHERE id=?""",
                    (exit_price, now, outcome, round(return_pct, 4), trade_id),
                )
                cash = self._get_cash() + payout
                realized = self._get_realized_pnl() + (payout - size_usd)
                self._conn.execute(
                    "INSERT INTO portfolio (ts, cash_usd, open_value, total_value, realized_pnl) VALUES (?, ?, 0, ?, ?)",
                    (now, round(cash, 2), round(cash, 2), round(realized, 2)),
                )
                self._conn.commit()

            resolved += 1
            logger.info("Resolved: %s %s → %s (return=%.1f%%)", side, ticker, outcome, return_pct * 100)

        return resolved

    def get_summary(self) -> dict[str, Any]:
        with self._lock:
            port = self._conn.execute(
                "SELECT cash_usd, open_value, total_value, realized_pnl FROM portfolio ORDER BY id DESC LIMIT 1"
            ).fetchone()
            total_trades = self._conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
            wins = self._conn.execute("SELECT COUNT(*) FROM trades WHERE outcome='win'").fetchone()[0]
            closed = self._conn.execute("SELECT COUNT(*) FROM trades WHERE status='closed'").fetchone()[0]
            open_count = self._conn.execute("SELECT COUNT(*) FROM trades WHERE status='open'").fetchone()[0]

        cash = port[0] if port else _STARTING_CASH
        open_val = port[1] if port else 0
        total_val = port[2] if port else _STARTING_CASH
        realized = port[3] if port else 0

        return {
            "cash_usd": round(cash, 2),
            "open_positions_value": round(open_val, 2),
            "total_value": round(total_val, 2),
            "realized_pnl": round(realized, 2),
            "total_trades": total_trades,
            "open_trades": open_count,
            "closed_trades": closed,
            "wins": wins,
            "win_rate": round(wins / closed, 3) if closed > 0 else 0.0,
        }

    def get_recent_trades(self, limit: int = 50) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """SELECT id, ticker, side, entry_price, size_usd, signal_family,
                          confidence, news_context, entry_ts, exit_price, exit_ts,
                          outcome, return_pct, status
                   FROM trades ORDER BY id DESC LIMIT ?""",
                (limit,),
            ).fetchall()
        cols = ["id", "ticker", "side", "entry_price", "size_usd", "signal_family",
                "confidence", "news_context", "entry_ts", "exit_price", "exit_ts",
                "outcome", "return_pct", "status"]
        return [dict(zip(cols, row)) for row in rows]

    def close(self) -> None:
        with self._lock:
            self._conn.close()
