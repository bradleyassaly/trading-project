from __future__ import annotations

import os
from dataclasses import dataclass

from trading_platform.broker.live_models import (
    BrokerAccount,
    LiveBrokerFill,
    LiveBrokerOrderRequest,
    LiveBrokerOrderStatus,
    LiveBrokerPosition,
)


@dataclass(frozen=True)
class AlpacaBrokerConfig:
    api_key: str
    secret_key: str
    paper: bool = True
    base_url: str | None = None

    @classmethod
    def from_env(cls) -> "AlpacaBrokerConfig":
        api_key = os.getenv("ALPACA_API_KEY")
        secret_key = os.getenv("ALPACA_SECRET_KEY")
        if not api_key or not secret_key:
            raise ValueError(
                "Missing Alpaca credentials. Set ALPACA_API_KEY and ALPACA_SECRET_KEY."
            )
        paper_value = os.getenv("ALPACA_PAPER", "true").strip().lower()
        paper = paper_value in {"1", "true", "yes", "y"}
        base_url = os.getenv("ALPACA_BASE_URL")
        return cls(
            api_key=api_key,
            secret_key=secret_key,
            paper=paper,
            base_url=base_url,
        )


class AlpacaBroker:
    def __init__(self, config: AlpacaBrokerConfig) -> None:
        self.config = config

    def get_account(self) -> BrokerAccount:
        raise NotImplementedError(
            "AlpacaBroker.get_account is a scaffold. Wire this to the Alpaca SDK or REST API."
        )

    def get_positions(self) -> dict[str, LiveBrokerPosition]:
        raise NotImplementedError(
            "AlpacaBroker.get_positions is a scaffold. Wire this to the Alpaca SDK or REST API."
        )

    def list_open_orders(self) -> list[LiveBrokerOrderStatus]:
        raise NotImplementedError(
            "AlpacaBroker.list_open_orders is a scaffold. Wire this to the Alpaca SDK or REST API."
        )

    def cancel_open_orders(self) -> None:
        raise NotImplementedError(
            "AlpacaBroker.cancel_open_orders is a scaffold. Wire this to the Alpaca SDK or REST API."
        )

    def submit_orders(
        self,
        orders: list[LiveBrokerOrderRequest],
    ) -> list[LiveBrokerOrderStatus]:
        raise NotImplementedError(
            "AlpacaBroker.submit_orders is a scaffold. Wire this to the Alpaca SDK or REST API."
        )

    def list_recent_fills(self) -> list[LiveBrokerFill]:
        raise NotImplementedError(
            "AlpacaBroker.list_recent_fills is a scaffold. Wire this to the Alpaca SDK or REST API."
        )