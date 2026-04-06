"""
Polymarket paper trade executor using smart money signals.

Shares the same SQLite database as KalshiPaperExecutor but tracks
Polymarket trades separately via a ``platform`` column.

Usage::

    from trading_platform.polymarket.polymarket_paper_executor import PolymarketPaperExecutor
    executor = PolymarketPaperExecutor()
    executor.execute_trade(signal, market_price=0.65)
"""
from __future__ import annotations

import logging
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_STARTING_CASH = 500.0


class PolymarketPaperExecutor:
    """Paper trade executor for Polymarket smart money signals."""

    def __init__(self, db_path: str | Path = "data/kalshi/paper_trades.db") -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._migrate()

    def _migrate(self) -> None:
        """Create tables if missing, add columns for Polymarket fields."""
        now = datetime.now(tz=timezone.utc).isoformat()
        self._conn.executescript(f"""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL, side TEXT NOT NULL,
                entry_price REAL NOT NULL, size_usd REAL NOT NULL,
                signal_family TEXT, confidence REAL, news_context TEXT,
                entry_ts TEXT NOT NULL, exit_price REAL, exit_ts TEXT,
                outcome TEXT, return_pct REAL, status TEXT NOT NULL DEFAULT 'open'
            );
            CREATE TABLE IF NOT EXISTS portfolio (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL, cash_usd REAL NOT NULL,
                open_value REAL NOT NULL DEFAULT 0, total_value REAL NOT NULL,
                realized_pnl REAL NOT NULL DEFAULT 0
            );
        """)
        # Seed portfolio if empty
        if not self._conn.execute("SELECT 1 FROM portfolio LIMIT 1").fetchone():
            self._conn.execute(
                "INSERT INTO portfolio (ts, cash_usd, open_value, total_value, realized_pnl) VALUES (?, ?, 0, ?, 0)",
                (now, _STARTING_CASH, _STARTING_CASH),
            )
            self._conn.commit()
        for col, default in [
            ("platform TEXT DEFAULT 'kalshi'", None),
            ("full_token_id TEXT", None),
            ("signal_type TEXT", None),
            ("smart_money_confidence REAL", None),
            ("smart_money_edge REAL", None),
            ("weighted_net_volume REAL", None),
        ]:
            try:
                self._conn.execute(f"ALTER TABLE trades ADD COLUMN {col}")
            except sqlite3.OperationalError:
                pass

    def execute_trade(self, signal: Any, market_price: float) -> bool:
        """Place a paper trade from a SmartMoneySignal. Returns True if placed."""
        # Validation
        if signal.confidence < 0.60:
            return False
        if signal.top_wallet_edge < 0.50:
            return False
        if market_price < 0.05 or market_price > 0.95:
            return False
        if signal.direction == "NEUTRAL":
            return False

        token_short = signal.token_id[:20]

        with self._lock:
            # Check for existing position
            existing = self._conn.execute(
                "SELECT id FROM trades WHERE ticker = ? AND status = 'open'",
                (token_short,),
            ).fetchone()
            if existing:
                return False

            # Kelly sizing
            edge = signal.top_wallet_edge
            kelly = max(0, (edge - (1 - edge)))
            stake = min(max(kelly * _STARTING_CASH * 0.25, 5.0), 15.0)

            # Check cash
            cash_row = self._conn.execute(
                "SELECT cash_usd FROM portfolio ORDER BY id DESC LIMIT 1"
            ).fetchone()
            cash = float(cash_row[0]) if cash_row else _STARTING_CASH
            if cash < stake:
                return False

            now = datetime.now(tz=timezone.utc).isoformat()
            self._conn.execute(
                """INSERT INTO trades
                   (ticker, side, entry_price, size_usd, signal_family, confidence,
                    news_context, entry_ts, status, platform, full_token_id,
                    signal_type, smart_money_confidence, smart_money_edge,
                    weighted_net_volume)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'open', 'polymarket', ?, ?, ?, ?, ?)""",
                (token_short, signal.direction, market_price, round(stake, 2),
                 "smart_money", signal.confidence, "smart_money_scan", now,
                 signal.token_id, "smart_money",
                 signal.confidence, signal.top_wallet_edge,
                 signal.weighted_net_volume),
            )
            # Update portfolio
            new_cash = cash - stake
            self._conn.execute(
                "INSERT INTO portfolio (ts, cash_usd, open_value, total_value, realized_pnl) "
                "SELECT ?, ?, open_value + ?, total_value, realized_pnl "
                "FROM portfolio ORDER BY id DESC LIMIT 1",
                (now, round(new_cash, 2), round(stake, 2)),
            )
            self._conn.commit()

        logger.info(
            "Polymarket paper trade: %s %s @ %.2f size=$%.2f edge=%.1f%% wt_vol=$%.0f",
            signal.direction, token_short, market_price, stake,
            signal.top_wallet_edge * 100, signal.weighted_net_volume,
        )
        return True

    def check_resolutions(self, resolver: Any) -> list[dict[str, Any]]:
        """Check open Polymarket trades for resolution. Returns resolved trades."""
        with self._lock:
            open_trades = self._conn.execute(
                "SELECT id, ticker, side, entry_price, size_usd, full_token_id "
                "FROM trades WHERE platform = 'polymarket' AND status = 'open'"
            ).fetchall()

        resolved: list[dict[str, Any]] = []
        for trade_id, ticker, side, entry_price, size_usd, full_token_id in open_trades:
            token = full_token_id or ticker
            rp = resolver.resolve(token)
            if rp is None:
                continue

            resolved_yes = rp >= 99.0
            won = (side == "YES" and resolved_yes) or (side == "NO" and not resolved_yes)
            if won:
                payout = size_usd * (1.0 / entry_price) if entry_price > 0 else size_usd
                return_pct = (payout - size_usd) / size_usd
            else:
                payout = 0.0
                return_pct = -1.0

            now = datetime.now(tz=timezone.utc).isoformat()
            with self._lock:
                self._conn.execute(
                    "UPDATE trades SET exit_price=?, exit_ts=?, outcome=?, return_pct=?, status='closed' WHERE id=?",
                    (100.0 if resolved_yes else 0.0, now, "win" if won else "loss",
                     round(return_pct, 4), trade_id),
                )
                cash_row = self._conn.execute("SELECT cash_usd, realized_pnl FROM portfolio ORDER BY id DESC LIMIT 1").fetchone()
                new_cash = float(cash_row[0]) + payout
                new_pnl = float(cash_row[1]) + (payout - size_usd)
                self._conn.execute(
                    "INSERT INTO portfolio (ts, cash_usd, open_value, total_value, realized_pnl) VALUES (?, ?, 0, ?, ?)",
                    (now, round(new_cash, 2), round(new_cash, 2), round(new_pnl, 2)),
                )
                self._conn.commit()

            resolved.append({
                "ticker": ticker, "side": side, "outcome": "win" if won else "loss",
                "return_pct": round(return_pct, 4), "payout": round(payout, 2),
            })

        return resolved

    def get_summary(self) -> dict[str, Any]:
        """Summary of Polymarket paper trades."""
        with self._lock:
            total = self._conn.execute("SELECT COUNT(*) FROM trades WHERE platform='polymarket'").fetchone()[0]
            open_count = self._conn.execute("SELECT COUNT(*) FROM trades WHERE platform='polymarket' AND status='open'").fetchone()[0]
            closed = self._conn.execute("SELECT COUNT(*) FROM trades WHERE platform='polymarket' AND status='closed'").fetchone()[0]
            wins = self._conn.execute("SELECT COUNT(*) FROM trades WHERE platform='polymarket' AND outcome='win'").fetchone()[0]
            avg_conf = self._conn.execute("SELECT AVG(smart_money_confidence) FROM trades WHERE platform='polymarket'").fetchone()[0]
            avg_edge = self._conn.execute("SELECT AVG(smart_money_edge) FROM trades WHERE platform='polymarket'").fetchone()[0]

        return {
            "platform": "polymarket",
            "total_trades": total,
            "open_trades": open_count,
            "closed_trades": closed,
            "wins": wins,
            "win_rate": round(wins / closed, 3) if closed > 0 else 0.0,
            "avg_confidence": round(avg_conf, 3) if avg_conf else 0.0,
            "avg_edge": round(avg_edge, 3) if avg_edge else 0.0,
        }

    def close(self) -> None:
        with self._lock:
            self._conn.close()
