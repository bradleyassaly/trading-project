from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

import pandas as pd

from trading_platform.broker.alpaca_broker import AlpacaBroker, AlpacaBrokerConfig
from trading_platform.broker.live_models import LiveBrokerOrderRequest
from trading_platform.broker.models import (
    BrokerAccountSnapshot,
    BrokerConfig,
    BrokerOpenOrder,
    BrokerOrderRequest,
    BrokerOrderResult,
    BrokerPosition,
)


class BrokerAdapter(Protocol):
    def get_account_snapshot(self) -> BrokerAccountSnapshot: ...

    def get_positions(self) -> dict[str, BrokerPosition]: ...

    def get_open_orders(self) -> list[BrokerOpenOrder]: ...

    def submit_orders(self, orders: list[BrokerOrderRequest]) -> list[BrokerOrderResult]: ...

    def cancel_order(self, broker_order_id: str) -> BrokerOrderResult: ...

    def cancel_all_orders(self) -> list[BrokerOrderResult]: ...

    def health_check(self) -> tuple[bool, str]: ...


def _load_mock_positions(path: str | None) -> dict[str, BrokerPosition]:
    if not path:
        return {}
    df = pd.read_csv(path)
    if df.empty:
        return {}
    positions: dict[str, BrokerPosition] = {}
    for row in df.to_dict(orient="records"):
        quantity = int(row["quantity"])
        market_price = float(row["market_price"])
        positions[str(row["symbol"])] = BrokerPosition(
            symbol=str(row["symbol"]),
            quantity=quantity,
            avg_price=float(row["avg_price"]),
            market_price=market_price,
            market_value=float(row.get("market_value", quantity * market_price)),
        )
    return positions


def _load_mock_open_orders(path: str | None) -> list[BrokerOpenOrder]:
    if not path:
        return []
    df = pd.read_csv(path)
    if df.empty:
        return []
    rows: list[BrokerOpenOrder] = []
    for row in df.to_dict(orient="records"):
        rows.append(
            BrokerOpenOrder(
                broker_order_id=str(row.get("broker_order_id") or "") or None,
                client_order_id=str(row.get("client_order_id") or "") or None,
                symbol=str(row["symbol"]),
                side=str(row["side"]).upper(),
                quantity=int(row["quantity"]),
                filled_quantity=int(row.get("filled_quantity", 0)),
                order_type=str(row.get("order_type", "market")),
                time_in_force=str(row.get("time_in_force", "day")),
                status=str(row.get("status", "open")),
                submitted_at=str(row.get("submitted_at")) if row.get("submitted_at") is not None else None,
            )
        )
    return rows


@dataclass
class MockBrokerAdapter:
    config: BrokerConfig

    def __post_init__(self) -> None:
        self._positions = _load_mock_positions(self.config.mock_positions_path)
        self._open_orders = _load_mock_open_orders(self.config.mock_open_orders_path)
        self._next_order_index = 1

    def get_account_snapshot(self) -> BrokerAccountSnapshot:
        return BrokerAccountSnapshot(
            account_id="mock-account",
            cash=float(self.config.mock_cash),
            equity=float(self.config.mock_equity),
            buying_power=float(self.config.mock_cash),
            currency="USD",
        )

    def get_positions(self) -> dict[str, BrokerPosition]:
        return dict(self._positions)

    def get_open_orders(self) -> list[BrokerOpenOrder]:
        return list(self._open_orders)

    def submit_orders(self, orders: list[BrokerOrderRequest]) -> list[BrokerOrderResult]:
        results: list[BrokerOrderResult] = []
        reject_symbols = {symbol.upper() for symbol in self.config.mock_reject_symbols}
        for order in orders:
            if order.symbol.upper() in reject_symbols:
                results.append(
                    BrokerOrderResult(
                        symbol=order.symbol,
                        side=order.side,
                        quantity=order.quantity,
                        order_type=order.order_type,
                        time_in_force=order.time_in_force,
                        status="rejected",
                        submitted=False,
                        client_order_id=order.client_order_id,
                        message="mock symbol rejection",
                    )
                )
                continue
            broker_order_id = f"mock-{self._next_order_index}"
            self._next_order_index += 1
            submitted_at = datetime.now(UTC).isoformat()
            open_order = BrokerOpenOrder(
                broker_order_id=broker_order_id,
                client_order_id=order.client_order_id,
                symbol=order.symbol,
                side=order.side,
                quantity=order.quantity,
                filled_quantity=0,
                order_type=order.order_type,
                time_in_force=order.time_in_force,
                status="accepted",
                submitted_at=submitted_at,
            )
            self._open_orders.append(open_order)
            results.append(
                BrokerOrderResult(
                    symbol=order.symbol,
                    side=order.side,
                    quantity=order.quantity,
                    order_type=order.order_type,
                    time_in_force=order.time_in_force,
                    status="accepted",
                    submitted=True,
                    client_order_id=order.client_order_id,
                    broker_order_id=broker_order_id,
                    submitted_at=submitted_at,
                    message="accepted by mock broker",
                )
            )
        return results

    def cancel_order(self, broker_order_id: str) -> BrokerOrderResult:
        for index, order in enumerate(list(self._open_orders)):
            if order.broker_order_id == broker_order_id:
                self._open_orders.pop(index)
                return BrokerOrderResult(
                    symbol=order.symbol,
                    side=order.side,
                    quantity=order.quantity,
                    order_type=order.order_type,
                    time_in_force=order.time_in_force,
                    status="cancelled",
                    submitted=False,
                    client_order_id=order.client_order_id,
                    broker_order_id=order.broker_order_id,
                    message="cancelled by mock broker",
                )
        return BrokerOrderResult(
            symbol="",
            side="BUY",
            quantity=0,
            order_type="market",
            time_in_force="day",
            status="failed",
            submitted=False,
            broker_order_id=broker_order_id,
            message="order not found",
        )

    def cancel_all_orders(self) -> list[BrokerOrderResult]:
        results = [
            BrokerOrderResult(
                symbol=order.symbol,
                side=order.side,
                quantity=order.quantity,
                order_type=order.order_type,
                time_in_force=order.time_in_force,
                status="cancelled",
                submitted=False,
                client_order_id=order.client_order_id,
                broker_order_id=order.broker_order_id,
                message="cancelled by mock broker",
            )
            for order in self._open_orders
        ]
        self._open_orders = []
        return results

    def health_check(self) -> tuple[bool, str]:
        return True, "mock broker healthy"


@dataclass
class AlpacaBrokerAdapter:
    config: BrokerConfig

    def __post_init__(self) -> None:
        api_key = os.getenv(self.config.alpaca_api_key_env_var)
        secret_key = os.getenv(self.config.alpaca_secret_key_env_var)
        if not api_key or not secret_key:
            raise ValueError(
                f"Missing Alpaca credentials. Set {self.config.alpaca_api_key_env_var} and {self.config.alpaca_secret_key_env_var}."
            )
        self._broker = AlpacaBroker(
            AlpacaBrokerConfig(
                api_key=api_key,
                secret_key=secret_key,
                paper=bool(self.config.alpaca_paper),
                base_url=self.config.alpaca_base_url,
            )
        )

    def get_account_snapshot(self) -> BrokerAccountSnapshot:
        account = self._broker.get_account()
        return BrokerAccountSnapshot(
            account_id=account.account_id,
            cash=account.cash,
            equity=account.equity,
            buying_power=account.buying_power,
            currency=account.currency,
        )

    def get_positions(self) -> dict[str, BrokerPosition]:
        positions = self._broker.get_positions()
        return {
            symbol: BrokerPosition(
                symbol=item.symbol,
                quantity=item.quantity,
                avg_price=item.avg_price,
                market_price=item.market_price,
                market_value=item.market_value,
            )
            for symbol, item in positions.items()
        }

    def get_open_orders(self) -> list[BrokerOpenOrder]:
        return [
            BrokerOpenOrder(
                broker_order_id=item.broker_order_id,
                client_order_id=item.client_order_id,
                symbol=item.symbol,
                side=item.side,
                quantity=item.quantity,
                filled_quantity=item.filled_quantity,
                order_type=item.order_type,
                time_in_force=item.time_in_force,
                status=item.status,
                submitted_at=item.submitted_at,
            )
            for item in self._broker.list_open_orders()
        ]

    def submit_orders(self, orders: list[BrokerOrderRequest]) -> list[BrokerOrderResult]:
        submitted = self._broker.submit_orders(
            [
                LiveBrokerOrderRequest(
                    symbol=order.symbol,
                    side=order.side,
                    quantity=order.quantity,
                    order_type=order.order_type,
                    time_in_force=order.time_in_force,
                    limit_price=order.limit_price,
                    client_order_id=order.client_order_id,
                    reason=order.reason,
                )
                for order in orders
            ]
        )
        return [
            BrokerOrderResult(
                symbol=item.symbol,
                side=item.side,
                quantity=item.quantity,
                order_type=item.order_type,
                time_in_force=item.time_in_force,
                status="submitted" if item.status.lower() in {"accepted", "new", "held", "pending_new"} else item.status.lower(),
                submitted=item.status.lower() not in {"rejected", "failed", "canceled", "cancelled"},
                client_order_id=item.client_order_id,
                broker_order_id=item.broker_order_id,
                filled_quantity=item.filled_quantity,
                submitted_at=item.submitted_at,
                message=item.status,
            )
            for item in submitted
        ]

    def cancel_order(self, broker_order_id: str) -> BrokerOrderResult:
        raise NotImplementedError("Alpaca single-order cancel is not implemented yet.")

    def cancel_all_orders(self) -> list[BrokerOrderResult]:
        raise NotImplementedError("Alpaca cancel_all_orders is not implemented yet.")

    def health_check(self) -> tuple[bool, str]:
        try:
            account = self.get_account_snapshot()
        except Exception as exc:  # pragma: no cover - network/client specific
            return False, str(exc)
        return True, f"connected account {account.account_id or 'unknown'}"


def resolve_broker_adapter(config: BrokerConfig) -> BrokerAdapter:
    if config.broker_name == "mock":
        return MockBrokerAdapter(config)
    if config.broker_name == "alpaca":
        return AlpacaBrokerAdapter(config)
    raise ValueError(f"Unsupported broker_name: {config.broker_name}")
