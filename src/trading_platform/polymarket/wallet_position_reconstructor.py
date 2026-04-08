"""
Wallet position reconstructor.

Aggregates raw wallet_trades into position-level rows in
wallet_market_positions. One row per (wallet, condition_id, side) where
``side`` is the uppercased outcome name (YES / NO / team name / OVER /
UNDER / etc.). Each outcome token traded by a wallet on a market becomes
a distinct position; BUY and SELL fills on the same outcome are netted.

Usage::

    from trading_platform.polymarket.wallet_position_reconstructor import WalletPositionReconstructor
    r = WalletPositionReconstructor()
    n = r.reconstruct_wallet(wallet_address, db_path)
"""
from __future__ import annotations

import logging
import sqlite3
from collections import defaultdict
from typing import Any

logger = logging.getLogger(__name__)


def _normalize_side(outcome: str | None) -> str:
    """Normalize an outcome name into a side label.

    Polymarket uses 'Yes'/'No' for binary markets and free-form names
    (team names, 'Over'/'Under', 'Up'/'Down', candidate names) for
    multi-outcome markets. We uppercase and trim, leaving the literal
    name intact so multi-outcome positions stay distinguishable.
    """
    if not outcome:
        return "YES"
    return str(outcome).strip().upper() or "YES"


class WalletPositionReconstructor:
    """Group raw fills into per-market positions, netting BUYs and SELLs."""

    def reconstruct_wallet(self, wallet: str, db_path: str) -> int:
        """Reconstruct all market positions for one wallet. Returns count stored."""
        conn = sqlite3.connect(db_path)
        try:
            trade_rows = conn.execute(
                """SELECT condition_id, side, price, size, timestamp, pnl,
                          market_resolved, market_outcome, title, category,
                          asset, outcome
                   FROM wallet_trades
                   WHERE wallet = ? AND condition_id IS NOT NULL AND condition_id != ''
                   ORDER BY timestamp ASC""",
                (wallet,),
            ).fetchall()

            if not trade_rows:
                return 0

            cols = ["cid", "side", "price", "size", "ts", "pnl",
                    "resolved", "market_outcome", "question", "category",
                    "asset", "outcome"]
            trades = [dict(zip(cols, t)) for t in trade_rows]

            # Group by (cid, asset) so YES vs NO tokens (and per-team multi-outcome
            # tokens) become distinct positions. asset is the ERC1155 token id.
            groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
            for t in trades:
                key = (t["cid"], t.get("asset") or "")
                groups[key].append(t)

            stored = 0
            for (cid, asset), fills in groups.items():
                buys = [f for f in fills if (f.get("side") or "").upper() == "BUY"]
                sells = [f for f in fills if (f.get("side") or "").upper() == "SELL"]

                # Skip groups with no buys (orphan sells = data quality issue)
                if not buys:
                    continue

                buy_size = sum((f.get("size") or 0) for f in buys)
                if buy_size <= 0:
                    continue

                sell_size = sum((f.get("size") or 0) for f in sells)
                net_shares = buy_size - sell_size
                is_fully_exited = 1 if net_shares <= max(0.01, buy_size * 0.001) else 0

                avg_entry = sum((f["price"] or 0) * (f["size"] or 0) for f in buys) / buy_size
                total_cost = sum((f["price"] or 0) * (f["size"] or 0) for f in buys)

                avg_exit = None
                sell_volume = None
                if sell_size > 0:
                    avg_exit = sum((f["price"] or 0) * (f["size"] or 0) for f in sells) / sell_size
                    sell_volume = sum((f["price"] or 0) * (f["size"] or 0) for f in sells)

                first_ts = min((f["ts"] for f in buys), default=None)
                last_ts = max((f["ts"] for f in buys), default=None)
                exit_ts = max((f["ts"] for f in sells), default=None)

                # Outcome name = the human label of this asset/token. Take from
                # any fill (they all share the same asset → same outcome).
                outcome_name = next((f.get("outcome") for f in fills if f.get("outcome")), "")
                side = _normalize_side(outcome_name)

                resolved = any((f.get("resolved") or 0) == 1 for f in fills)
                market_outcome = None
                res_price = None
                if resolved:
                    market_outcome = next(
                        (f.get("market_outcome") for f in fills if f.get("market_outcome")),
                        None,
                    )
                    if market_outcome:
                        # Long this token: resolution_price = 1 if this outcome won, else 0
                        won = _normalize_side(market_outcome) == side
                        res_price = 1.0 if won else 0.0

                pnl_vals = [f["pnl"] for f in fills if f.get("pnl") is not None]
                realized_pnl = sum(pnl_vals) if pnl_vals else None

                end_ts_row = conn.execute(
                    "SELECT MAX(t) FROM market_price_history WHERE condition_id = ?",
                    (cid,),
                ).fetchone()
                end_ts = end_ts_row[0] if end_ts_row and end_ts_row[0] else None
                hours_before = round((end_ts - first_ts) / 3600, 1) if (end_ts and first_ts) else None

                question = next((f.get("question") for f in fills if f.get("question")), "")
                category = next((f.get("category") for f in fills if f.get("category")), "")

                conn.execute(
                    """INSERT OR REPLACE INTO wallet_market_positions
                       (wallet, condition_id, asset, question, category, side,
                        first_entry_ts, last_entry_ts, exit_ts,
                        avg_entry_price, avg_exit_price,
                        total_shares, total_cost_usdc, total_proceeds_usdc,
                        net_shares, total_sold, sell_volume_usdc, is_fully_exited,
                        realized_pnl, market_resolved, market_outcome,
                        resolution_price, end_date_ts, hours_before_close)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        wallet, cid, asset, question, category, side,
                        first_ts, last_ts, exit_ts,
                        round(avg_entry, 4), round(avg_exit, 4) if avg_exit else None,
                        round(buy_size, 2), round(total_cost, 2),
                        round(sell_volume, 2) if sell_volume else None,
                        round(net_shares, 2),
                        round(sell_size, 2),
                        round(sell_volume, 2) if sell_volume else None,
                        is_fully_exited,
                        round(realized_pnl, 2) if realized_pnl is not None else None,
                        1 if resolved else 0, market_outcome, res_price,
                        end_ts, hours_before,
                    ),
                )
                stored += 1

            conn.commit()
            return stored
        finally:
            conn.close()

    def reconstruct_batch(self, wallets: list[str], db_path: str) -> dict[str, int]:
        results: dict[str, int] = {}
        for wallet in wallets:
            try:
                results[wallet] = self.reconstruct_wallet(wallet, db_path)
            except Exception as exc:
                logger.debug("reconstruct failed for %s: %s", wallet[:16], exc)
                results[wallet] = 0
        return results
