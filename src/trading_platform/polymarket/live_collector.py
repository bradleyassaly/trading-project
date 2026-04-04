"""
Polymarket live WebSocket price collector.

Connects to the Polymarket CLOB WebSocket, subscribes to market price
updates for a set of YES token IDs, stores raw ticks in SQLite, and
periodically exports hourly OHLCV bars to parquet files compatible
with the existing feature pipeline.

Usage::

    import asyncio
    from trading_platform.polymarket.live_collector import (
        PolymarketLiveCollector, PolymarketLiveCollectorConfig, LiveMarketInfo,
    )

    markets = [LiveMarketInfo(market_id="123", question="...", yes_token_id="10417...", volume=50000)]
    collector = PolymarketLiveCollector(PolymarketLiveCollectorConfig(), markets)
    asyncio.run(collector.run())
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

try:
    from websockets.asyncio.client import connect as ws_connect
except ImportError:
    try:
        from websockets.legacy.client import connect as ws_connect  # type: ignore[assignment]
    except ImportError:
        ws_connect = None  # type: ignore[assignment]

from trading_platform.polymarket.live_db import LiveTickStore

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LiveMarketInfo:
    market_id: str
    question: str
    yes_token_id: str
    volume: float
    end_date_iso: str | None = None


@dataclass
class PolymarketLiveCollectorConfig:
    db_path: str = "data/polymarket/live/prices.db"
    hourly_bars_dir: str = "data/polymarket/live/hourly_bars"
    stats_path: str = "artifacts/polymarket_live/stats.json"
    ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    reconnect_delay: float = 2.0
    reconnect_backoff_max: float = 60.0
    ping_interval_sec: float = 20.0
    export_interval_sec: float = 3600.0
    stats_interval_sec: float = 60.0
    heartbeat_interval_sec: float = 300.0


@dataclass
class _CollectorState:
    messages_received: int = 0
    ticks_stored: int = 0
    reconnect_count: int = 0
    last_export_at: datetime | None = None
    started_at: datetime | None = None
    latest_prices: dict[str, float] = field(default_factory=dict)


class PolymarketLiveCollector:
    """
    WebSocket-based live price collector for Polymarket markets.

    Subscribes to market price updates via the CLOB WebSocket, stores
    ticks in SQLite, and exports hourly OHLCV bars to parquet.
    """

    def __init__(
        self,
        config: PolymarketLiveCollectorConfig,
        markets: list[LiveMarketInfo],
    ) -> None:
        if ws_connect is None:
            raise ImportError(
                "websockets >= 10 is required for PolymarketLiveCollector.\n"
                "Install it: pip install 'websockets>=10'"
            )
        self.config = config
        self.markets = markets
        # Lookup: yes_token_id → LiveMarketInfo
        self._by_asset: dict[str, LiveMarketInfo] = {
            m.yes_token_id: m for m in markets
        }
        self.state = _CollectorState()
        self._store: LiveTickStore | None = None
        self._ws: Any = None

    # ── Public API ───────────────────────────────────────────────────────────

    async def run(self) -> None:
        """Run the collector indefinitely (Ctrl+C to stop)."""
        self._store = LiveTickStore(self.config.db_path)
        self.state.started_at = datetime.now(tz=timezone.utc)
        # Persist market metadata so the API can join question text
        self._store.upsert_markets_batch([
            (m.market_id, m.question, m.volume, m.yes_token_id, m.end_date_iso)
            for m in self.markets
        ])
        try:
            export_task = asyncio.create_task(self._export_loop())
            stats_task = asyncio.create_task(self._stats_loop())
            heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            await self._ws_loop()
        except (KeyboardInterrupt, asyncio.CancelledError):
            logger.info("Collector stopping gracefully")
        finally:
            export_task.cancel()
            stats_task.cancel()
            heartbeat_task.cancel()
            # Final export before exit
            try:
                await self._export_hourly_bars()
            except Exception as exc:
                logger.warning("Final export failed: %s", exc)
            if self._store:
                self._store.close()
            logger.info(
                "Collector stopped. msgs=%d ticks=%d reconnects=%d",
                self.state.messages_received,
                self.state.ticks_stored,
                self.state.reconnect_count,
            )

    def get_state_snapshot(self) -> dict[str, Any]:
        """Return current collector state for API/monitoring."""
        return {
            "messages_received": self.state.messages_received,
            "ticks_stored": self.state.ticks_stored,
            "reconnect_count": self.state.reconnect_count,
            "markets_subscribed": len(self.markets),
            "started_at": self.state.started_at.isoformat() if self.state.started_at else None,
            "last_export_at": self.state.last_export_at.isoformat() if self.state.last_export_at else None,
            "latest_prices": {k: round(v * 100, 2) for k, v in self.state.latest_prices.items()},
        }

    # ── WebSocket loop ───────────────────────────────────────────────────────

    async def _ws_loop(self) -> None:
        delay = self.config.reconnect_delay
        while True:
            try:
                await self._connect_and_subscribe()
                delay = self.config.reconnect_delay  # reset on success
                await self._message_loop()
            except (KeyboardInterrupt, asyncio.CancelledError):
                raise
            except Exception as exc:
                self.state.reconnect_count += 1
                logger.warning(
                    "WebSocket disconnected (%s). Reconnecting in %.1fs... (attempt %d)",
                    exc, delay, self.state.reconnect_count,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, self.config.reconnect_backoff_max)

    async def _connect_and_subscribe(self) -> None:
        self._ws = await ws_connect(
            self.config.ws_url,
            ping_interval=self.config.ping_interval_sec,
            ping_timeout=self.config.ping_interval_sec,
        )
        token_ids = [m.yes_token_id for m in self.markets]
        subscribe_msg = json.dumps({
            "type": "market",
            "assets_ids": token_ids,
        })
        await self._ws.send(subscribe_msg)
        logger.info(
            "Subscribed to %d markets on %s", len(token_ids), self.config.ws_url,
        )

    async def _message_loop(self) -> None:
        async for raw in self._ws:
            self.state.messages_received += 1
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                logger.warning("Non-JSON WS message: %s", raw[:200])
                continue
            # Polymarket WS sends messages as JSON arrays, e.g.
            # [{"event_type": "price_change", ...}]
            items: list[dict[str, Any]]
            if isinstance(parsed, list):
                items = parsed
            elif isinstance(parsed, dict):
                items = [parsed]
            else:
                continue
            for msg in items:
                if not isinstance(msg, dict):
                    continue
                msg_type = msg.get("event_type") or msg.get("type", "")
                if msg_type == "price_change":
                    self._handle_price_change(msg)
                elif msg_type == "last_trade_price":
                    self._handle_last_trade_price(msg)
                elif msg_type == "book":
                    self._handle_book(msg)

    # ── Message handlers ─────────────────────────────────────────────────────

    def _handle_price_change(self, msg: dict[str, Any]) -> None:
        asset_id = msg.get("asset_id", "")
        info = self._by_asset.get(asset_id)
        if not info:
            return
        timestamp = msg.get("timestamp") or datetime.now(tz=timezone.utc).isoformat()
        changes = msg.get("changes", [])
        batch: list[tuple[str, str, float, str | None, float | None, str, str]] = []
        for change in changes:
            try:
                price = float(change.get("price", 0))
            except (TypeError, ValueError):
                continue
            side = change.get("side")
            try:
                size = float(change.get("size", 0))
            except (TypeError, ValueError):
                size = None
            batch.append((asset_id, info.market_id, price, side, size, str(timestamp), "price_change"))
        if batch and self._store:
            self._store.insert_ticks_batch(batch)
            self.state.ticks_stored += len(batch)

    def _handle_book(self, msg: dict[str, Any]) -> None:
        """Extract midpoint from orderbook snapshot as initial price point."""
        asset_id = msg.get("asset_id", "")
        info = self._by_asset.get(asset_id)
        if not info:
            return
        try:
            bids = msg.get("bids", [])
            asks = msg.get("asks", [])
            best_bid = float(bids[0].get("price", 0)) if bids else 0.0
            best_ask = float(asks[0].get("price", 0)) if asks else 0.0
            if best_bid > 0 and best_ask > 0:
                price = (best_bid + best_ask) / 2.0
            elif best_bid > 0:
                price = best_bid
            elif best_ask > 0:
                price = best_ask
            else:
                return
        except (TypeError, ValueError, IndexError):
            return
        spread = best_ask - best_bid if best_ask > 0 and best_bid > 0 else None
        timestamp = msg.get("timestamp") or datetime.now(tz=timezone.utc).isoformat()
        if self._store:
            self._store.insert_tick(
                asset_id=asset_id,
                market_id=info.market_id,
                price=price,
                timestamp=str(timestamp),
                msg_type="book",
                best_bid=best_bid if best_bid > 0 else None,
                best_ask=best_ask if best_ask > 0 else None,
                spread=spread,
            )
            self.state.ticks_stored += 1
            self.state.latest_prices[info.market_id] = price

    def _handle_last_trade_price(self, msg: dict[str, Any]) -> None:
        asset_id = msg.get("asset_id", "")
        info = self._by_asset.get(asset_id)
        if not info:
            return
        try:
            price = float(msg.get("price", 0))
        except (TypeError, ValueError):
            return
        timestamp = msg.get("timestamp") or datetime.now(tz=timezone.utc).isoformat()
        if self._store:
            self._store.insert_tick(
                asset_id=asset_id,
                market_id=info.market_id,
                price=price,
                timestamp=str(timestamp),
                msg_type="last_trade_price",
            )
            self.state.ticks_stored += 1
            self.state.latest_prices[info.market_id] = price

    # ── Hourly export ────────────────────────────────────────────────────────

    async def _export_loop(self) -> None:
        while True:
            await asyncio.sleep(self.config.export_interval_sec)
            try:
                await self._export_hourly_bars()
            except Exception as exc:
                logger.warning("Hourly export failed: %s", exc)

    async def _export_hourly_bars(self) -> None:
        if not self._store:
            return

        import pandas as pd

        now = datetime.now(tz=timezone.utc)
        hour_end = now.replace(minute=0, second=0, microsecond=0)
        hour_start = hour_end - timedelta(hours=1)
        hour_start_str = hour_start.isoformat()
        hour_end_str = hour_end.isoformat()

        bars_dir = Path(self.config.hourly_bars_dir)
        bars_dir.mkdir(parents=True, exist_ok=True)
        exported = 0

        for market in self.markets:
            ticks = self._store.get_ticks_for_hour(
                market.market_id, hour_start_str, hour_end_str,
            )
            if not ticks:
                continue

            prices = [t["price"] for t in ticks]
            bar = pd.DataFrame([{
                "timestamp": hour_start,
                "symbol": market.market_id,
                "open": prices[0],
                "high": max(prices),
                "low": min(prices),
                "close": prices[-1],
                "volume": float(len(prices)),
                "dollar_volume": sum(prices) * 100,
            }])

            path = bars_dir / f"{market.market_id}.parquet"
            existing = pd.read_parquet(path) if path.exists() else pd.DataFrame()
            if not existing.empty:
                combined = pd.concat([existing, bar], ignore_index=True)
                combined = combined.drop_duplicates(subset=["timestamp"], keep="last")
                combined = combined.sort_values("timestamp")
            else:
                combined = bar
            combined.to_parquet(path, index=False)
            exported += 1

        self.state.last_export_at = now
        if exported:
            logger.info("Exported %d hourly bar files for %s", exported, hour_start_str)

    # ── Stats file ───────────────────────────────────────────────────────────

    async def _stats_loop(self) -> None:
        while True:
            await asyncio.sleep(self.config.stats_interval_sec)
            try:
                self._write_stats()
            except Exception as exc:
                logger.warning("Stats write failed: %s", exc)

    def _write_stats(self) -> None:
        stats_path = Path(self.config.stats_path)
        stats_path.parent.mkdir(parents=True, exist_ok=True)
        now = datetime.now(tz=timezone.utc)
        stats = {
            "started_at": self.state.started_at.isoformat() if self.state.started_at else None,
            "markets_subscribed": len(self.markets),
            "markets_active": len(self.state.latest_prices),
            "total_ticks": self.state.ticks_stored,
            "messages_received": self.state.messages_received,
            "reconnect_count": self.state.reconnect_count,
            "last_tick_at": now.isoformat(),
            "last_export_at": self.state.last_export_at.isoformat() if self.state.last_export_at else None,
        }
        stats_path.write_text(json.dumps(stats, indent=2), encoding="utf-8")

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self.config.heartbeat_interval_sec)
            logger.info(
                "[heartbeat] %d ticks collected, %d markets active",
                self.state.ticks_stored,
                len(self.state.latest_prices),
            )
