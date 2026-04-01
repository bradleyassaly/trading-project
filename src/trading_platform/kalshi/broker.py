"""
Kalshi live trading broker.

Implements the same interface as AlpacaBroker so it can drop into the
existing execution pipeline.

Prediction market conventions used here:
  BrokerOrder.symbol   = Kalshi ticker  (e.g. "PRES-24-D.TR")
  BrokerOrder.side     = "BUY_YES" | "SELL_YES" | "BUY_NO" | "SELL_NO"
  BrokerOrder.quantity = number of contracts
  limit_price          = YES price as a float in [0.01, 0.99]
                         (converted to "0.XXXX" dollar string on submission)

Risk controls:
  - Kill switch: cancels all open orders and blocks new ones.
  - Max drawdown: auto-activates kill switch if equity drops by N%.
"""
from __future__ import annotations

import logging
import uuid

from trading_platform.broker.live_models import (
    BrokerAccount,
    LiveBrokerFill,
    LiveBrokerOrderRequest,
    LiveBrokerOrderStatus,
    LiveBrokerPosition,
)
from trading_platform.kalshi.auth import KalshiConfig
from trading_platform.kalshi.client import KalshiClient
from trading_platform.kalshi.models import KalshiOrderRequest, float_to_price

logger = logging.getLogger(__name__)

_DEFAULT_MAX_DRAWDOWN = 0.20   # 20%


class KalshiBroker:
    """
    Live Kalshi prediction market broker.

    Usage::

        config = KalshiConfig.from_env()
        broker = KalshiBroker(config, max_drawdown_pct=0.15)

        account = broker.get_account()
        positions = broker.get_positions()
        broker.submit_orders([order1, order2])

        # Emergency stop:
        broker.activate_kill_switch()
    """

    def __init__(
        self,
        config: KalshiConfig,
        max_drawdown_pct: float = _DEFAULT_MAX_DRAWDOWN,
    ) -> None:
        self.config = config
        self._client = KalshiClient(config)
        self._max_drawdown_pct = max_drawdown_pct
        self._starting_equity: float | None = None
        self._killed = False

    # ── Account ───────────────────────────────────────────────────────────────

    def get_account(self) -> BrokerAccount:
        data = self._client.get_balance()
        # Kalshi returns balance in cents
        cash = float(data.get("balance", 0)) / 100.0
        equity = float(data.get("portfolio_value", 0)) / 100.0
        return BrokerAccount(
            account_id=None,
            cash=cash,
            equity=equity,
            buying_power=cash,
            currency="USD",
        )

    def get_cash(self) -> float:
        return self.get_account().cash

    # ── Positions ─────────────────────────────────────────────────────────────

    def get_positions(self) -> dict[str, LiveBrokerPosition]:
        raw = self._client.get_positions()
        return {
            p.ticker: LiveBrokerPosition(
                symbol=p.ticker,
                quantity=p.position,
                avg_price=0.0,       # Kalshi doesn't return avg entry in positions endpoint
                market_price=0.0,
                market_value=0.0,
            )
            for p in raw
            if p.position != 0
        }

    # ── Orders ────────────────────────────────────────────────────────────────

    def submit_orders(self, orders: list[LiveBrokerOrderRequest]) -> list[LiveBrokerOrderStatus]:
        if self._killed:
            raise RuntimeError("Kill switch is active — no orders accepted.")
        self._check_drawdown()

        results: list[LiveBrokerOrderStatus] = []
        for order in orders:
            side, action = _parse_side_action(order.side)
            yes_price = float_to_price(order.limit_price) if order.limit_price else None
            client_id = order.client_order_id or str(uuid.uuid4())

            req = KalshiOrderRequest(
                ticker=order.symbol,
                side=side,
                action=action,
                count=order.quantity,
                yes_price=yes_price,
                time_in_force="good_till_canceled",
                client_order_id=client_id,
            )

            status = self._client.create_order(req)
            results.append(
                LiveBrokerOrderStatus(
                    broker_order_id=status.order_id,
                    client_order_id=status.client_order_id,
                    symbol=status.ticker,
                    side=order.side,
                    quantity=order.quantity,
                    filled_quantity=order.quantity - status.remaining_count,
                    order_type="limit",
                    time_in_force="good_till_canceled",
                    status=status.status,
                    submitted_at=status.created_time,
                )
            )

        return results

    def list_open_orders(self) -> list[LiveBrokerOrderStatus]:
        orders, _ = self._client.get_orders(status="resting")
        return [
            LiveBrokerOrderStatus(
                broker_order_id=o.order_id,
                client_order_id=o.client_order_id,
                symbol=o.ticker,
                side=f"{o.action.upper()}_{o.side.upper()}",
                quantity=o.count,
                filled_quantity=o.count - o.remaining_count,
                order_type=o.order_type,
                time_in_force="good_till_canceled",
                status=o.status,
                submitted_at=o.created_time,
            )
            for o in orders
        ]

    def cancel_open_orders(self, ticker: str | None = None) -> None:
        self._client.cancel_all_orders(ticker=ticker)

    def list_recent_fills(self) -> list[LiveBrokerFill]:
        fills, _ = self._client.get_fills()
        return [
            LiveBrokerFill(
                broker_order_id=f.order_id,
                symbol=f.ticker,
                side=f"{f.action.upper()}_{f.side.upper()}",
                quantity=f.count,
                fill_price=float(f.yes_price or 0),
                notional=float(f.yes_price or 0) * f.count,
                commission=float(f.fees or 0),
            )
            for f in fills
        ]

    # ── Kill Switch & Risk ────────────────────────────────────────────────────

    def activate_kill_switch(self) -> None:
        """Cancel all open orders and permanently block new submissions until reset."""
        logger.warning("KALSHI KILL SWITCH ACTIVATED — canceling all open orders.")
        self._killed = True
        try:
            self.cancel_open_orders()
        except Exception as exc:
            logger.error("Error during kill switch cancellation: %s", exc)

    def reset_kill_switch(self) -> None:
        self._killed = False
        logger.info("Kill switch reset.")

    def _check_drawdown(self) -> None:
        account = self.get_account()
        if self._starting_equity is None:
            self._starting_equity = account.equity
            return
        if self._starting_equity <= 0:
            return
        drawdown = (self._starting_equity - account.equity) / self._starting_equity
        if drawdown >= self._max_drawdown_pct:
            logger.error(
                "Max drawdown breached: %.1f%% >= limit %.1f%%. Kill switch activating.",
                drawdown * 100,
                self._max_drawdown_pct * 100,
            )
            self.activate_kill_switch()
            raise RuntimeError(
                f"Max drawdown of {self._max_drawdown_pct:.0%} breached. Kill switch activated."
            )


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_side_action(order_side: str) -> tuple[str, str]:
    """
    Map a BrokerOrder.side string to Kalshi (side, action).

    Expected inputs: BUY_YES, SELL_YES, BUY_NO, SELL_NO
    Returns: ("yes"/"no", "buy"/"sell")
    """
    parts = order_side.upper().split("_", 1)
    if len(parts) == 2:
        return parts[1].lower(), parts[0].lower()
    # Fallback: treat ambiguous side as YES buy/sell
    return "yes", parts[0].lower()
