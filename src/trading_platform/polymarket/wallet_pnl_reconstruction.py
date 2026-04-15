"""FIFO lot-matching PnL reconstruction from wallet_trades.

Our legacy PnL only counted resolved markets. Wallets that sell before
resolution — the dominant pattern for active traders — had their early
exits skipped entirely, causing 100-1000x undercounts vs Polymarket's
authoritative leaderboard.

This module reconstructs trade-level realized PnL by FIFO-matching SELL
fills against earlier BUY fills per (wallet, asset). At market
resolution, remaining open lots settle at the outcome price (1.0 if the
held outcome won, 0.0 otherwise). The resulting per-fill realized_pnl
is written back to ``wallet_trades.realized_pnl_closed`` and summed to
``wallet_profiles.realized_pnl_total``.

Usage::

    from trading_platform.polymarket.wallet_pnl_reconstruction import (
        reconstruct_all, reconstruct_wallet,
    )
    reconstruct_all()                     # full backfill
    reconstruct_wallet("0x514550b8...")   # single wallet refresh
"""
from __future__ import annotations

import logging
import time
from collections import defaultdict, deque
from typing import Any

from trading_platform.polymarket.db_connection import db, get_connection

logger = logging.getLogger(__name__)


def _ensure_columns() -> None:
    """Idempotent ALTERs — new columns on existing tables.

    Uses a fresh connection per ALTER because Postgres aborts the
    whole transaction when any statement errors, so a single batch
    would silently skip everything after the first "already exists".
    """
    for table, col, typ in (
        ("wallet_trades", "realized_pnl_closed", "DOUBLE PRECISION"),
        ("wallet_trades", "lot_remaining", "DOUBLE PRECISION"),
        ("wallet_profiles", "realized_pnl_total", "DOUBLE PRECISION"),
        ("wallet_profiles", "unrealized_pnl_estimate", "DOUBLE PRECISION"),
        ("wallet_profiles", "pnl_reconstructed_at", "BIGINT"),
        ("wallet_profiles", "closed_trades_count", "INTEGER"),
    ):
        c = get_connection()
        try:
            c.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typ}")
            c.commit()
        except Exception:
            pass  # already exists
        finally:
            c.close()


def _fifo_match(trades: list[dict]) -> tuple[list[tuple[int, float, float]], float, float]:
    """Run FIFO matching for one (wallet, asset) tape of fills.

    Each trade dict needs: id, side, size, price, timestamp,
    market_resolved (0/1), market_outcome (YES/NO/None), outcome (the
    token side this row holds — YES or NO).

    Returns: (updates, realized_total, open_cost)
      - updates: list of (trade_id, realized_pnl_closed, lot_remaining)
      - realized_total: sum of realized PnL from SELLs + resolutions
      - open_cost: cost basis of still-open lots (unrealized reference)
    """
    trades = sorted(trades, key=lambda t: (t["timestamp"] or 0, t["id"]))
    lots: deque[list[float]] = deque()  # each lot: [remaining_size, buy_price, buy_trade_id]
    updates: list[tuple[int, float, float]] = []
    realized_total = 0.0

    # What outcome (YES/NO) does this tape hold? wallet_trades.outcome
    # is set per fill; we key FIFO by asset so it should be consistent.
    held_outcome = ""
    for t in trades:
        if t.get("outcome"):
            held_outcome = (t["outcome"] or "").upper()
            break

    for t in trades:
        side = (t.get("side") or "").upper()
        size = float(t.get("size") or 0)
        price = float(t.get("price") or 0)
        tid = t["id"]
        if size <= 0:
            updates.append((tid, 0.0, 0.0))
            continue

        if side == "BUY":
            lots.append([size, price, tid])
            updates.append((tid, 0.0, size))  # full size remains open at BUY time
        elif side == "SELL":
            remaining = size
            pnl_closed = 0.0
            while remaining > 1e-9 and lots:
                lot = lots[0]
                matched = min(lot[0], remaining)
                pnl_closed += (price - lot[1]) * matched
                lot[0] -= matched
                remaining -= matched
                if lot[0] <= 1e-9:
                    lots.popleft()
            # If we still have remaining after exhausting lots, the
            # wallet had a short (unusual on PM) — treat as zero-cost open.
            if remaining > 1e-9:
                pnl_closed += price * remaining  # book the proceeds
            realized_total += pnl_closed
            updates.append((tid, pnl_closed, 0.0))
        else:
            updates.append((tid, 0.0, 0.0))

    # Resolution settlement — if market is resolved, close all open lots
    # at outcome price. wallet_trades rows all share market_resolved /
    # market_outcome per condition, so read from any trade.
    resolved = any(t.get("market_resolved") for t in trades)
    if resolved and lots:
        outcome_str = ""
        for t in trades:
            if t.get("market_outcome"):
                outcome_str = (t["market_outcome"] or "").upper()
                break
        # Settlement price: 1.0 if the held outcome won, else 0.0.
        # If market_outcome unknown, skip settlement (leave as open).
        if outcome_str in ("YES", "NO") and held_outcome in ("YES", "NO"):
            settle_price = 1.0 if outcome_str == held_outcome else 0.0
            for lot in lots:
                realized_total += (settle_price - lot[1]) * lot[0]
            lots.clear()

    open_cost = sum(lot[0] * lot[1] for lot in lots)
    return updates, realized_total, open_cost


def reconstruct_wallet(wallet: str, *, conn: Any = None) -> dict[str, Any]:
    """Rebuild PnL for one wallet. Returns summary dict."""
    owns_conn = conn is None
    if owns_conn:
        conn = get_connection()
    try:
        rows = conn.execute(
            """SELECT id, asset, side, size, price, timestamp,
                      market_resolved, market_outcome, outcome
               FROM wallet_trades
               WHERE wallet = ?
               ORDER BY asset, timestamp ASC""",
            (wallet,),
        ).fetchall()

        by_asset: dict[str, list[dict]] = defaultdict(list)
        for r in rows:
            asset = r[1] or ""
            by_asset[asset].append({
                "id": r[0], "asset": asset, "side": r[2], "size": r[3],
                "price": r[4], "timestamp": r[5], "market_resolved": r[6],
                "market_outcome": r[7], "outcome": r[8],
            })

        all_updates: list[tuple[int, float, float]] = []
        realized_total = 0.0
        open_cost_total = 0.0
        closed_count = 0
        for asset, trades in by_asset.items():
            updates, realized, open_cost = _fifo_match(trades)
            all_updates.extend(updates)
            realized_total += realized
            open_cost_total += open_cost
            closed_count += sum(1 for u in updates if abs(u[1]) > 1e-9)

        # Write per-trade realized_pnl_closed + lot_remaining
        for tid, pnl_closed, lot_rem in all_updates:
            conn.execute(
                "UPDATE wallet_trades SET realized_pnl_closed = ?, lot_remaining = ? WHERE id = ?",
                (pnl_closed, lot_rem, tid),
            )

        # Update wallet_profiles aggregate
        conn.execute(
            """UPDATE wallet_profiles
               SET realized_pnl_total = ?,
                   unrealized_pnl_estimate = ?,
                   pnl_reconstructed_at = ?,
                   closed_trades_count = ?
               WHERE wallet = ?""",
            (realized_total, open_cost_total, int(time.time()), closed_count, wallet),
        )
        conn.commit()
        return {
            "wallet": wallet,
            "trades": len(rows),
            "assets": len(by_asset),
            "realized_pnl_total": round(realized_total, 2),
            "open_cost_basis": round(open_cost_total, 2),
            "closed_fills": closed_count,
        }
    finally:
        if owns_conn:
            conn.close()


def reconstruct_all(*, min_trades: int = 1, limit: int | None = None) -> dict[str, Any]:
    """Full backfill — reconstruct PnL for every wallet with trades."""
    _ensure_columns()
    t0 = time.time()
    conn = get_connection()
    try:
        wallets = [r[0] for r in conn.execute(
            """SELECT wallet FROM wallet_trades
               GROUP BY wallet
               HAVING COUNT(*) >= ?
               ORDER BY COUNT(*) DESC""",
            (min_trades,),
        ).fetchall()]
        if limit:
            wallets = wallets[:limit]

        total_realized = 0.0
        processed = 0
        for w in wallets:
            try:
                res = reconstruct_wallet(w, conn=conn)
                total_realized += res["realized_pnl_total"]
                processed += 1
                if processed % 100 == 0:
                    logger.info("pnl reconstruct: %d/%d wallets (%.1fs)",
                                processed, len(wallets), time.time() - t0)
            except Exception as exc:
                logger.warning("reconstruct failed for %s: %s", w[:12], exc)

        return {
            "wallets_processed": processed,
            "wallets_total": len(wallets),
            "aggregate_realized_pnl": round(total_realized, 2),
            "elapsed_seconds": round(time.time() - t0, 1),
        }
    finally:
        conn.close()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    result = reconstruct_all()
    print(f"pnl reconstruction: {result}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
