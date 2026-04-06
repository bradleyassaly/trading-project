"""
Real-time Goldsky fill stream via batched HTTP polling.

Polls the Goldsky orderbook subgraph every 30 seconds for fills
from watched wallets using batched maker_in queries.

Usage::

    import asyncio
    from trading_platform.polymarket.goldsky_stream import GoldskyStream

    async def on_fill(fill):
        print(f"Fill: {fill['maker_wallet']} bought {fill['taker_asset_id']}")

    stream = GoldskyStream(watch_wallets=["0xabc..."], callback=on_fill)
    asyncio.run(stream.run())
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Callable

logger = logging.getLogger(__name__)

POLL_INTERVAL = 30  # seconds
LOOKBACK_MINUTES = 2  # slightly more than one poll interval
WALLET_BATCH_SIZE = 10


class GoldskyStream:
    """Poll Goldsky for fills from watched wallets."""

    def __init__(
        self,
        watch_wallets: list[str],
        callback: Callable[[dict[str, Any]], Any],
        *,
        poll_interval: float = POLL_INTERVAL,
    ) -> None:
        self._wallets = [w.lower() for w in watch_wallets]
        self._callback = callback
        self._poll_interval = poll_interval
        self._seen_ids: set[str] = set()

    async def run(self) -> None:
        """Poll Goldsky every poll_interval seconds."""
        from trading_platform.polymarket.goldsky_client import GoldskyClient
        client = GoldskyClient()

        print(f"Polling Goldsky every {self._poll_interval:.0f}s "
              f"for {len(self._wallets)} watched wallets "
              f"(batches of {WALLET_BATCH_SIZE})...")

        while True:
            try:
                new_fills = 0
                fills = client.fetch_fills_by_makers(
                    self._wallets,
                    since_hours=LOOKBACK_MINUTES / 60,
                    batch_size=WALLET_BATCH_SIZE,
                )

                for _, row in fills.iterrows():
                    fill_id = str(row.get("id", ""))
                    if not fill_id or fill_id in self._seen_ids:
                        continue
                    self._seen_ids.add(fill_id)
                    new_fills += 1

                    fill_dict = row.to_dict()
                    # Normalize field names for callback compatibility
                    fill_dict["maker"] = fill_dict.get("maker_wallet", "")
                    fill_dict["taker"] = fill_dict.get("taker_wallet", "")
                    fill_dict["makerAmountFilled"] = str(int(fill_dict.get("maker_amount", 0) * 1e6))
                    fill_dict["takerAmountFilled"] = str(int(fill_dict.get("taker_amount", 0) * 1e6))
                    fill_dict["makerAssetId"] = fill_dict.get("maker_asset_id", "")
                    fill_dict["takerAssetId"] = fill_dict.get("taker_asset_id", "")

                    # Convert timestamp to epoch int if it's a datetime
                    ts = fill_dict.get("timestamp")
                    if hasattr(ts, "timestamp"):
                        fill_dict["timestamp"] = int(ts.timestamp())

                    result = self._callback(fill_dict)
                    if asyncio.iscoroutine(result):
                        await result

                if new_fills > 0:
                    now = datetime.now(tz=timezone.utc).strftime("%H:%M:%S")
                    print(f"[{now}] Found {new_fills} new fills from watched wallets")

                # Trim seen IDs to prevent memory growth
                if len(self._seen_ids) > 50_000:
                    self._seen_ids = set(list(self._seen_ids)[-25_000:])

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Poll cycle error: %s, retrying in %.0fs", exc, self._poll_interval)

            await asyncio.sleep(self._poll_interval)

    async def _process_events(self, events: list[dict[str, Any]]) -> None:
        """Filter events to watched wallets and invoke callback. Used by tests."""
        wallet_set = set(self._wallets)
        for ev in events:
            ev_id = ev.get("id", "")
            if ev_id in self._seen_ids:
                continue
            self._seen_ids.add(ev_id)

            maker = ev.get("maker", "")
            if isinstance(maker, dict):
                maker = maker.get("id", "")
            maker = maker.lower()

            if maker not in wallet_set:
                continue

            fill = {
                "id": ev_id,
                "timestamp": int(ev.get("timestamp", 0)),
                "maker": maker,
                "taker": ev.get("taker", ""),
                "makerAmountFilled": ev.get("makerAmountFilled", "0"),
                "takerAmountFilled": ev.get("takerAmountFilled", "0"),
                "makerAssetId": ev.get("makerAssetId", ""),
                "takerAssetId": ev.get("takerAssetId", ""),
            }
            if isinstance(fill["taker"], dict):
                fill["taker"] = fill["taker"].get("id", "")

            result = self._callback(fill)
            if asyncio.iscoroutine(result):
                await result
