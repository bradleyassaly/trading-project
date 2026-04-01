"""
Kalshi WebSocket streaming client.

Supports real-time subscription to:
  orderbook_delta   — bid/ask changes
  ticker            — price/volume ticks
  trade             — public trades
  fill              — your personal fills
  market_positions  — your position changes
  user_orders       — your order updates

Usage::

    import asyncio
    from trading_platform.kalshi.auth import KalshiConfig
    from trading_platform.kalshi.stream import KalshiStream

    async def main():
        config = KalshiConfig.from_env()
        async with KalshiStream(config) as stream:
            await stream.subscribe("orderbook_delta", ["TICKER-1"])
            async for msg in stream.messages():
                print(msg)

    asyncio.run(main())
"""
from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from typing import Any

try:
    from websockets.asyncio.client import connect as ws_connect
except ImportError:  # pragma: no cover
    try:
        from websockets.legacy.client import connect as ws_connect  # type: ignore[assignment]
    except ImportError:
        ws_connect = None  # type: ignore[assignment]

from trading_platform.kalshi.auth import KalshiConfig, build_auth_headers

logger = logging.getLogger(__name__)

WS_PATH = "/trade-api/ws/v2"


class KalshiStream:
    """
    Async context manager for a Kalshi WebSocket connection.

    :param config:          KalshiConfig with credentials.
    :param reconnect:       If True, transparently reconnect on disconnect.
    :param reconnect_delay: Seconds to wait before reconnecting.
    """

    def __init__(
        self,
        config: KalshiConfig,
        reconnect: bool = True,
        reconnect_delay: float = 2.0,
    ) -> None:
        if ws_connect is None:
            raise ImportError(
                "websockets >= 10 is required for KalshiStream.\n"
                "Install it: pip install 'websockets>=10'"
            )
        self.config = config
        self.reconnect = reconnect
        self.reconnect_delay = reconnect_delay
        self._ws: Any = None
        self._sub_id = 0
        self._active_subs: dict[int, dict[str, Any]] = {}

    async def __aenter__(self) -> "KalshiStream":
        await self._connect()
        return self

    async def __aexit__(self, *args: Any) -> None:
        if self._ws:
            await self._ws.close()
            self._ws = None

    async def _connect(self) -> None:
        headers = build_auth_headers(self.config, "GET", WS_PATH)
        url = f"{self.config.ws_base}{WS_PATH}"
        self._ws = await ws_connect(url, additional_headers=headers)
        logger.info("KalshiStream connected to %s", url)

    async def subscribe(self, channel: str, markets: list[str]) -> int:
        """
        Subscribe to a channel for the given market tickers.
        Returns the subscription ID for later unsubscription.
        """
        self._sub_id += 1
        sid = self._sub_id
        msg = {
            "id": sid,
            "cmd": "subscribe",
            "params": {
                "channels": [channel],
                "market_tickers": markets,
            },
        }
        self._active_subs[sid] = msg
        await self._ws.send(json.dumps(msg))
        return sid

    async def unsubscribe(self, sub_id: int) -> None:
        await self._ws.send(json.dumps({"id": sub_id, "cmd": "unsubscribe"}))
        self._active_subs.pop(sub_id, None)

    async def messages(self) -> AsyncIterator[dict[str, Any]]:
        """
        Async generator that yields parsed WebSocket messages.
        If reconnect=True, re-subscribes and continues on disconnect.
        """
        while True:
            try:
                async for raw in self._ws:
                    try:
                        yield json.loads(raw)
                    except json.JSONDecodeError:
                        logger.warning("Non-JSON WS message: %s", raw)
            except Exception as exc:
                if not self.reconnect:
                    raise
                logger.warning("KalshiStream disconnected (%s). Reconnecting in %.1fs...", exc, self.reconnect_delay)
                await asyncio.sleep(self.reconnect_delay)
                await self._connect()
                for msg in self._active_subs.values():
                    await self._ws.send(json.dumps(msg))
