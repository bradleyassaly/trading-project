from __future__ import annotations

from dataclasses import dataclass

from trading_platform.broker.base import Broker, BrokerFill, BrokerOrder, BrokerPosition
from trading_platform.paper.models import PaperPortfolioState, PaperPosition

@dataclass
class PaperBrokerConfig:
    commission_per_order: float = 0.0
    slippage_bps: float = 0.0


class PaperBroker(Broker):
    def __init__(
        self,
        *,
        state: PaperPortfolioState,
        config: PaperBrokerConfig | None = None,
    ) -> None:
        self.state = state
        self.config = config or PaperBrokerConfig()

    def get_cash(self) -> float:
        return float(self.state.cash)

    def get_positions(self) -> dict[str, BrokerPosition]:
        return {
            symbol: BrokerPosition(
                symbol=position.symbol,
                quantity=position.quantity,
                avg_price=position.avg_price,
                last_price=position.last_price,
            )
            for symbol, position in self.state.positions.items()
        }

    def submit_orders(self, orders: list[BrokerOrder]) -> list[BrokerFill]:
        fills: list[BrokerFill] = []

        for order in orders:
            fill_price = self._apply_slippage(
                side=order.side,
                reference_price=order.reference_price,
            )
            notional = float(order.quantity) * float(fill_price)
            commission = float(self.config.commission_per_order)

            signed_qty = order.quantity if order.side == "BUY" else -order.quantity
            cash_change = -signed_qty * fill_price - commission
            self.state.cash += cash_change

            current = self.state.positions.get(order.symbol)
            prior_quantity = current.quantity if current else 0
            new_quantity = prior_quantity + signed_qty

            if new_quantity == 0:
                self.state.positions.pop(order.symbol, None)
            else:
                avg_price = self._compute_avg_price(
                    current=current,
                    signed_qty=signed_qty,
                    fill_price=fill_price,
                    new_quantity=new_quantity,
                )
                self.state.positions[order.symbol] = PaperPosition(
                    symbol=order.symbol,
                    quantity=new_quantity,
                    avg_price=avg_price,
                    last_price=fill_price,
                )

            fills.append(
                BrokerFill(
                    symbol=order.symbol,
                    side=order.side,
                    quantity=order.quantity,
                    fill_price=fill_price,
                    notional=notional,
                    commission=commission,
                    slippage_bps=float(self.config.slippage_bps),
                )
            )

        return fills

    def _apply_slippage(self, *, side: str, reference_price: float) -> float:
        bps = float(self.config.slippage_bps) / 10_000.0
        if side == "BUY":
            return float(reference_price) * (1.0 + bps)
        return float(reference_price) * (1.0 - bps)

    @staticmethod
    def _compute_avg_price(
        *,
        current: PaperPosition | None,
        signed_qty: int,
        fill_price: float,
        new_quantity: int,
    ) -> float:
        if current is None:
            return float(fill_price)

        prior_quantity = current.quantity
        if signed_qty > 0 and prior_quantity >= 0:
            return float(
                ((current.avg_price * prior_quantity) + (fill_price * signed_qty))
                / new_quantity
            )

        return float(current.avg_price)