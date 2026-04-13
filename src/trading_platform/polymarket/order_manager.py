"""
Order lifecycle manager for live trading on Polymarket CLOB.

Handles: submit → monitor → fill/cancel/timeout with retry logic.
Records every attempt in ``trade_fills`` for post-trade slippage analysis.

Not active until the live executor is enabled (DRY_RUN=False +
POLYMARKET_LIVE_ENABLED=1).
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


class OrderState:
    PENDING = "pending"
    OPEN = "open"
    PARTIAL = "partial"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    EXPIRED = "expired"
    ERROR = "error"


class OrderManager:
    """Manages the lifecycle of a live order: submit → monitor → fill/cancel."""

    ORDER_TIMEOUT_SECONDS = 120
    POLL_INTERVAL_SECONDS = 5
    MAX_RETRIES = 2

    def __init__(self, live_executor: Any, db_path: str | None = None) -> None:
        self.executor = live_executor
        self.db_path = db_path or _DEFAULT_DB

    def execute_with_monitoring(
        self,
        token_id: str,
        side: str,
        price: float,
        size: float,
        trade_id: int | None = None,
    ) -> dict[str, Any]:
        """Place an order and monitor until filled, cancelled, or timed out."""
        result: dict[str, Any] = {
            "trade_id": trade_id,
            "attempts": [],
            "final_status": None,
            "total_filled": 0.0,
            "avg_fill_price": None,
        }

        remaining = size
        for attempt in range(self.MAX_RETRIES + 1):
            order_result = self.executor.place_order(
                token_id, side, price, remaining,
            ) if hasattr(self.executor, "place_order") else {
                "success": False, "error": "place_order not implemented",
            }

            record: dict[str, Any] = {
                "attempt": attempt + 1,
                "order_id": order_result.get("order_id"),
                "submitted_at": time.time(),
                "result": order_result,
            }

            if not order_result.get("success"):
                record["status"] = OrderState.REJECTED
                record["error"] = order_result.get("error", "unknown")
                result["attempts"].append(record)

                error = str(order_result.get("error", "")).lower()
                if "insufficient" in error or "balance" in error:
                    break
                time.sleep(2)
                continue

            # Monitor for fill
            order_id = order_result["order_id"]
            start = time.time()

            while time.time() - start < self.ORDER_TIMEOUT_SECONDS:
                status = (
                    self.executor.get_order_status(order_id)
                    if hasattr(self.executor, "get_order_status")
                    else {"status": "unknown"}
                )

                if status.get("status") in ("matched", "filled"):
                    filled = float(status.get("filled_size") or size)
                    record["status"] = OrderState.FILLED
                    record["filled_size"] = filled
                    record["fill_price"] = float(status.get("price") or price)
                    record["fill_time_ms"] = int((time.time() - start) * 1000)
                    result["total_filled"] += filled
                    remaining -= filled
                    break

                if status.get("status") in ("cancelled", "expired"):
                    record["status"] = OrderState.CANCELLED
                    break

                time.sleep(self.POLL_INTERVAL_SECONDS)
            else:
                # Timed out
                if hasattr(self.executor, "cancel_order"):
                    self.executor.cancel_order(order_id)
                record["status"] = OrderState.EXPIRED
                record["error"] = f"Timed out after {self.ORDER_TIMEOUT_SECONDS}s"

            result["attempts"].append(record)

            if result["total_filled"] >= size * 0.95:
                break

        # Compute final
        filled_attempts = [
            a for a in result["attempts"] if a.get("status") == OrderState.FILLED
        ]
        if filled_attempts:
            total_cost = sum(
                a["filled_size"] * a["fill_price"] for a in filled_attempts
            )
            result["avg_fill_price"] = round(total_cost / result["total_filled"], 6)
            result["final_status"] = (
                OrderState.FILLED
                if result["total_filled"] >= size * 0.95
                else OrderState.PARTIAL
            )
        else:
            last = result["attempts"][-1] if result["attempts"] else {}
            result["final_status"] = last.get("status", OrderState.ERROR)

        self._record_fills(result, price)
        return result

    def _record_fills(self, result: dict, intended_price: float) -> None:
        """Persist fill data to trade_fills table."""
        try:
            conn = connect_wallet_db(self.db_path)
            try:
                for attempt in result["attempts"]:
                    fill_price = attempt.get("fill_price")
                    slippage = (
                        round(fill_price - intended_price, 6)
                        if fill_price is not None
                        else None
                    )
                    slippage_pct = (
                        round(slippage / intended_price, 6)
                        if slippage is not None and intended_price > 0
                        else None
                    )
                    conn.execute(
                        """INSERT INTO trade_fills
                           (trade_id, order_id, order_type, intended_price,
                            actual_fill_price, intended_size, actual_fill_size,
                            slippage, slippage_pct, fill_time_ms,
                            order_status, raw_response, created_at)
                           VALUES (?, ?, 'limit', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            result.get("trade_id"),
                            attempt.get("order_id"),
                            intended_price,
                            fill_price,
                            attempt.get("result", {}).get("intended_size"),
                            attempt.get("filled_size"),
                            slippage,
                            slippage_pct,
                            attempt.get("fill_time_ms"),
                            attempt.get("status"),
                            str(attempt.get("result", {}))[:500],
                            int(time.time()),
                        ),
                    )
                conn.commit()
            finally:
                conn.close()
        except Exception as exc:
            logger.debug("Fill recording failed: %s", exc)
