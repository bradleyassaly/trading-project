"""
Position monitor — tracks mark-to-market for open paper trades.

Runs every 30 minutes via the scheduler. For each open position, fetches
the current mid-price and stores a snapshot with unrealized P&L. This
gives us a time series of how each trade evolved between entry and exit.

Not used for exit decisions in paper trading (we hold to resolution for
clean hypothesis validation). The data feeds the dashboard, the daily
digest, and post-trade analysis.
"""
from __future__ import annotations

import logging
import sqlite3
import time
from pathlib import Path
from typing import Any

from trading_platform.polymarket.db import connect_wallet_db

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_DEFAULT_DB = str(_PROJECT_ROOT / "data" / "polymarket" / "wallet_intelligence.db")


_SNAPSHOT_SCHEMA = """
CREATE TABLE IF NOT EXISTS position_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id INTEGER NOT NULL,
    condition_id TEXT,
    entry_price REAL,
    current_price REAL,
    unrealized_pnl REAL,
    unrealized_pnl_pct REAL,
    time_held_hours REAL,
    snapshot_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ps_trade ON position_snapshots(trade_id);
CREATE INDEX IF NOT EXISTS idx_ps_time ON position_snapshots(snapshot_at DESC);
"""


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(_SNAPSHOT_SCHEMA)
    conn.commit()


def mark_to_market(db_path: str | None = None) -> dict[str, Any]:
    """Snapshot all open positions with current unrealized P&L.

    Returns summary stats for the scheduler log.
    """
    db_path = db_path or _DEFAULT_DB
    conn = connect_wallet_db(db_path)
    _ensure_schema(conn)

    now = int(time.time())

    # Get open paper trades
    open_trades = conn.execute(
        """SELECT id, condition_id, side, entry_price, size_usd, entry_ts
           FROM polymarket_paper_trades
           WHERE exit_ts IS NULL AND archived = 0"""
    ).fetchall()

    if not open_trades:
        conn.close()
        return {"open_positions": 0, "snapshots_created": 0}

    # Try to fetch current prices from the live ticks store or CLOB
    snapshots = 0
    total_unrealized = 0.0
    warnings = []

    for trade_id, cid, side, entry_price, size, entry_ts in open_trades:
        current_price = _get_current_price(conn, cid)
        if current_price is None:
            continue

        entry = float(entry_price or 0)
        cur = float(current_price)
        sz = float(size or 0)

        # Compute unrealized P&L
        if side in ("YES", "BUY"):
            unrealized_pnl = (cur - entry) * sz / max(entry, 0.01)
            unrealized_pct = (cur - entry) / max(entry, 0.01)
        else:
            unrealized_pnl = (entry - cur) * sz / max(1 - entry, 0.01)
            unrealized_pct = (entry - cur) / max(1 - entry, 0.01)

        hours_held = (now - (entry_ts or now)) / 3600

        conn.execute(
            """INSERT INTO position_snapshots
               (trade_id, condition_id, entry_price, current_price,
                unrealized_pnl, unrealized_pnl_pct, time_held_hours, snapshot_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (trade_id, cid, entry, cur,
             round(unrealized_pnl, 2), round(unrealized_pct, 4),
             round(hours_held, 1), now),
        )
        snapshots += 1
        total_unrealized += unrealized_pnl

        if unrealized_pct < -0.50:
            warnings.append(
                f"Trade #{trade_id}: {unrealized_pct*100:.0f}% unrealized loss"
            )

    conn.commit()
    conn.close()

    for w in warnings:
        logger.warning("[POSITION_MONITOR] %s", w)
        print(f"[POSITION_MONITOR] WARNING: {w}")

    return {
        "open_positions": len(open_trades),
        "snapshots_created": snapshots,
        "total_unrealized": round(total_unrealized, 2),
        "warnings": len(warnings),
    }


def _get_current_price(conn: sqlite3.Connection, condition_id: str) -> float | None:
    """Get the latest price for a market. Tries live_ticks, then cached data."""
    if not condition_id:
        return None

    # Try live_ticks (populated by live-collect's price_change handler)
    try:
        row = conn.execute(
            """SELECT mid_price FROM live_ticks
               WHERE condition_id = ?
               ORDER BY ts DESC LIMIT 1""",
            (condition_id,),
        ).fetchone()
        if row and row[0]:
            return float(row[0])
    except Exception:
        pass

    # Fallback: market_signals last known price
    try:
        row = conn.execute(
            """SELECT price FROM market_signals
               WHERE condition_id = ?
               ORDER BY fired_at DESC LIMIT 1""",
            (condition_id,),
        ).fetchone()
        if row and row[0]:
            return float(row[0])
    except Exception:
        pass

    return None


def get_open_positions_mtm(db_path: str | None = None) -> list[dict[str, Any]]:
    """Return all open positions with latest snapshot data. For the API."""
    db_path = db_path or _DEFAULT_DB
    conn = connect_wallet_db(db_path)
    _ensure_schema(conn)

    positions = conn.execute(
        """SELECT pt.id, pt.condition_id, pt.question, pt.side,
                  pt.entry_price, pt.size_usd, pt.signal_type,
                  pt.wallet, pt.entry_ts,
                  ps.current_price, ps.unrealized_pnl,
                  ps.unrealized_pnl_pct, ps.time_held_hours, ps.snapshot_at
           FROM polymarket_paper_trades pt
           LEFT JOIN (
               SELECT trade_id, current_price, unrealized_pnl,
                      unrealized_pnl_pct, time_held_hours, snapshot_at,
                      ROW_NUMBER() OVER (PARTITION BY trade_id ORDER BY snapshot_at DESC) AS rn
               FROM position_snapshots
           ) ps ON ps.trade_id = pt.id AND ps.rn = 1
           WHERE pt.exit_ts IS NULL AND pt.archived = 0
           ORDER BY pt.entry_ts DESC"""
    ).fetchall()

    cols = [
        "trade_id", "condition_id", "question", "side",
        "entry_price", "size_usd", "signal_type",
        "wallet", "entry_ts",
        "current_price", "unrealized_pnl",
        "unrealized_pnl_pct", "time_held_hours", "snapshot_at",
    ]
    conn.close()
    return [dict(zip(cols, r)) for r in positions]
