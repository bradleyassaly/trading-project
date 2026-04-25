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
    condition_id: str | None = None


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
    Optionally integrates whale detection via WhaleTripwire.
    """

    def __init__(
        self,
        config: PolymarketLiveCollectorConfig,
        markets: list[LiveMarketInfo],
        whale_tripwire: Any | None = None,
        signal_engine: Any | None = None,
        paper_executor: Any | None = None,
        market_universe: Any | None = None,
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
        self._tripwire = whale_tripwire
        self._signal_engine = signal_engine
        self._paper_executor = paper_executor
        self._universe = market_universe
        # Per-token rolling price buffer for velocity detection.
        # {token_id: [(ts, price), ...]} — trimmed to last 30 minutes on insert.
        self._price_buffer: dict[str, list[tuple[int, float]]] = {}
        # Pending ticks waiting to be batch-written to market_ticks.
        # Each entry: (condition_id, token_id, ts, price). Flushed every 50 ticks.
        self._tick_batch: list[tuple[str, str, int, float]] = []
        self._tick_batch_size = 50
        # Background polling threads (started in run())
        self._ob_monitor: Any = None
        self._ob_thread: Any = None
        self._hot_scanner: Any = None
        self._hot_thread: Any = None

    # ── Public API ───────────────────────────────────────────────────────────

    async def run(self) -> None:
        """Run the collector indefinitely (Ctrl+C to stop)."""
        # Cap pyarrow's internal thread pools. The ARROW_IO_THREADS env
        # var only controls the IO pool, not the CPU pool, and even then
        # only if read before pyarrow is imported — we can't rely on
        # that. Setting these explicitly at startup is the only robust
        # cap. On containers with many visible CPUs (32 on this host)
        # pyarrow would otherwise spawn a thread per core, each with
        # its own jemalloc arena — the primary driver of the ~130MB/h
        # RSS climb fixed in 2026-04.
        try:
            import pyarrow as _pa
            _pa.set_cpu_count(2)
            _pa.set_io_thread_count(2)
        except Exception:
            pass
        # Optional tracemalloc profiling — gated by env so normal runs
        # pay no overhead. Set LIVE_COLLECT_TRACEMALLOC=1 to turn on;
        # top allocations are dumped every 15 min into the log.
        # The task ref must be held on self — asyncio.create_task only
        # holds a weak reference, so a fire-and-forget task whose only
        # ref is the create_task return value gets GC'd the moment it
        # hits its first await. Python 3.11 made that more aggressive.
        import os as _os
        self._tracemalloc_task = None
        _tm_env = _os.environ.get("LIVE_COLLECT_TRACEMALLOC")
        print(f"[startup] LIVE_COLLECT_TRACEMALLOC={_tm_env!r}", flush=True)
        if _tm_env == "1":
            import tracemalloc
            tracemalloc.start(25)
            self._tracemalloc_task = asyncio.create_task(self._tracemalloc_dump_loop())
            print(f"[startup] tracemalloc started, task={self._tracemalloc_task!r}", flush=True)
        self._store = LiveTickStore(self.config.db_path)
        self.state.started_at = datetime.now(tz=timezone.utc)
        # Persist market metadata so the API can join question text
        self._store.upsert_markets_batch([
            (m.market_id, m.question, m.volume, m.yes_token_id, m.end_date_iso, m.condition_id)
            for m in self.markets
        ])

        # Resolve any pending paper trades from previous runs
        if self._paper_executor and hasattr(self._paper_executor, "check_resolutions_v2"):
            try:
                resolved = self._paper_executor.check_resolutions_v2()
                open_count = len(self._paper_executor.get_open_positions())
                logger.info("[PAPER] Resolved %d trades on startup. Open positions: %d", len(resolved), open_count)
                print(f"[PAPER] Resolved {len(resolved)} trades on startup. Open positions: {open_count}")
            except Exception as exc:
                logger.warning("[PAPER] Resolution check failed: %s", exc)
        try:
            # Start optional background monitors (gated by env vars)
            self._start_background_monitors()
            export_task = asyncio.create_task(self._export_loop())
            stats_task = asyncio.create_task(self._stats_loop())
            heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            poll_task = asyncio.create_task(self._tier1_poll_loop())
            self._fundamental_task = asyncio.create_task(self._fundamental_scan_loop())
            await self._ws_loop()
        except (KeyboardInterrupt, asyncio.CancelledError):
            logger.info("Collector stopping gracefully")
        finally:
            export_task.cancel()
            stats_task.cancel()
            heartbeat_task.cancel()
            poll_task.cancel()
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
                # Whale detection on every event
                self._check_whale(msg)

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
            self._update_velocity_buffer(asset_id, price, info)

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
            self._update_velocity_buffer(asset_id, price, info)

    # ── Price velocity tracking ───────────────────────────────────────────────

    def _update_velocity_buffer(
        self,
        token_id: str,
        price: float,
        info: LiveMarketInfo,
    ) -> None:
        """Append a price tick to the rolling 30-min buffer and emit a
        ``price_velocity`` signal if the move qualifies (>=10¢ in <30min).

        Also enqueues the tick for batched persistence into ``market_ticks``
        so the candle synthesis backend can build denser charts than the
        Gamma price-history snapshot allows.
        """
        import time as _t
        now = int(_t.time())
        buf = self._price_buffer.setdefault(token_id, [])
        buf.append((now, price))
        # Trim to last 30 minutes
        cutoff = now - 1800
        buf[:] = [(t, p) for t, p in buf if t >= cutoff]

        # Bound the _price_buffer outer dict — otherwise token_ids from
        # resolved/delisted markets stay forever. Prune any buffer that's
        # now empty + evict its key. Also cap total keys at 5000 (roughly
        # 2× our peak subscription count); when over, drop the least-
        # recently-updated keys. Prevents unbounded dict growth across
        # weeks of operation.
        if not buf:
            self._price_buffer.pop(token_id, None)
        elif len(self._price_buffer) > 5000:
            # Evict tokens whose newest tick is older than the cutoff
            stale_keys = [
                k for k, v in self._price_buffer.items()
                if not v or v[-1][0] < cutoff
            ]
            for k in stale_keys[:500]:
                self._price_buffer.pop(k, None)

        # Bound state.latest_prices the same way. Each new market_id adds
        # an entry and nothing ever removes it — over weeks this dict can
        # carry thousands of resolved markets we'll never trade again.
        # Cap at the subscribed universe size × 2 as a safety ceiling;
        # when over, evict the first 500 keys (insertion-order FIFO, so
        # we drop the oldest subscribed ones which are likely resolved).
        if len(self.state.latest_prices) > max(len(self.markets) * 2, 2000):
            for k in list(self.state.latest_prices.keys())[:500]:
                self.state.latest_prices.pop(k, None)

        # Persist tick (batched to keep write overhead minimal)
        cid = info.condition_id or ""
        if cid:
            self._tick_batch.append((cid, token_id, now, float(price)))
            if len(self._tick_batch) >= self._tick_batch_size:
                self._flush_tick_batch()

        # Velocity-signal firing must live in this method's scope so ``buf``
        # is defined. A previous refactor accidentally moved this block past
        # the next ``def`` and into ``_flush_tick_batch``, causing every
        # WebSocket message to crash with ``NameError: name 'buf' is not
        # defined`` and the live-collector to reconnect-loop forever.
        if len(buf) < 3 or self._signal_engine is None:
            return
        oldest_ts, oldest_price = buf[0]
        newest_ts, newest_price = buf[-1]
        price_change = newest_price - oldest_price
        if abs(price_change) < 0.10:
            return
        minutes_elapsed = (newest_ts - oldest_ts) / 60.0
        if minutes_elapsed <= 0:
            return
        velocity = price_change / max(minutes_elapsed, 0.5)
        # Classify the market at fire time so price_velocity signals carry
        # a real category. Previously hardcoded to "other", which is why
        # every velocity-fired signal was mis-tagged and the political /
        # geopolitical wallet tiering path was never consulted.
        try:
            from trading_platform.polymarket.market_categorizer import classify_keywords
            cat, _ = classify_keywords("", info.question or "")
        except Exception:
            cat = "other"
        try:
            self._signal_engine.on_price_velocity(
                condition_id=info.condition_id or "",
                token_id=token_id,
                price_change=price_change,
                velocity=velocity,
                current_price=newest_price,
                minutes_elapsed=minutes_elapsed,
                question=info.question or "",
                category=cat,
            )
        except Exception as exc:
            logger.debug("velocity signal failed: %s", exc)

    def _flush_tick_batch(self) -> None:
        """Write queued ticks to market_ticks in a single transaction.

        Uses the pooled db_connection helper — the previous raw
        ``sqlite3.connect(WalletDB()._path)`` per-batch pattern both
        violated our Postgres cutover (writes were going to a phantom
        SQLite file nobody reads) and was the primary allocator in the
        live-collect hot path — each batch opened a fresh connection
        with its own parser/prepared-statement buffers, at 50 ticks
        per flush that was tens of thousands of allocations per day
        never reclaimed by the sqlite3 module's internal caches.
        """
        if not self._tick_batch:
            return
        try:
            from trading_platform.polymarket.db_connection import db as _db
            with _db() as conn:
                conn.executemany(
                    """INSERT OR IGNORE INTO market_ticks
                       (condition_id, token_id, timestamp, price)
                       VALUES (?, ?, ?, ?)""",
                    self._tick_batch,
                )
            self._tick_batch = []
        except Exception as exc:
            logger.debug("tick batch flush failed: %s", exc)
            # Drop the batch on failure to avoid unbounded memory growth
            self._tick_batch = []

    # ── Background pollers (order book monitor + hot market scanner) ─────────

    def _start_background_monitors(self) -> None:
        """Optionally start the order book monitor and hot market scanner.

        Both are opt-in via the ``POLYMARKET_ENABLE_OB_MONITOR`` and
        ``POLYMARKET_ENABLE_HOT_SCANNER`` env vars to avoid surprising
        existing deployments.
        """
        import os as _os
        import threading as _th
        import time as _t

        if _os.environ.get("POLYMARKET_ENABLE_OB_MONITOR", "").lower() in ("1", "true", "yes"):
            try:
                from trading_platform.polymarket.order_book_monitor import OrderBookMonitor
                from trading_platform.polymarket.wallet_db import WalletDB
                self._ob_monitor = OrderBookMonitor(db_path=str(WalletDB()._path))

                def _ob_loop() -> None:
                    while True:
                        try:
                            import requests as _req
                            import json as _json
                            r = _req.get(
                                "https://gamma-api.polymarket.com/markets",
                                params={"active": "true", "closed": "false",
                                        "limit": 50, "order": "volume24hr",
                                        "ascending": "false"},
                                timeout=10,
                            )
                            top = r.json() if isinstance(r.json(), list) else []
                            ob_markets = []
                            for m in top:
                                tids_raw = m.get("clobTokenIds", "[]")
                                tids = _json.loads(tids_raw) if isinstance(tids_raw, str) else tids_raw
                                if not tids:
                                    continue
                                ob_markets.append({
                                    "condition_id": m.get("conditionId", ""),
                                    "token_id": tids[0],
                                    "question": m.get("question", ""),
                                    "category": m.get("category", "other"),
                                })
                            signals = self._ob_monitor.run_scan(ob_markets)
                            if self._signal_engine:
                                for s in signals:
                                    if s.get("severity") in ("high", "critical"):
                                        self._signal_engine.on_order_book_signal(s)
                        except Exception as exc:
                            logger.error("[OB_POLL] cycle failed: %s", exc)
                        _t.sleep(120)

                self._ob_thread = _th.Thread(target=_ob_loop, daemon=True, name="ob_monitor")
                self._ob_thread.start()
                logger.info("[INIT] Order book monitor started (2-min interval, top 50 markets)")
            except Exception as exc:
                logger.warning("OB monitor failed to start: %s", exc)

        if _os.environ.get("POLYMARKET_ENABLE_HOT_SCANNER", "").lower() in ("1", "true", "yes"):
            if self._universe is None:
                logger.warning("Hot scanner requested but no market_universe provided")
            else:
                try:
                    from trading_platform.polymarket.hot_market_scanner import HotMarketScanner
                    from trading_platform.polymarket.wallet_db import WalletDB
                    self._hot_scanner = HotMarketScanner(self._universe, db_path=str(WalletDB()._path))
                    self._hot_thread = self._hot_scanner.run_background(interval=300)
                    logger.info("[INIT] Hot market scanner started (5-min interval)")
                except Exception as exc:
                    logger.warning("Hot scanner failed to start: %s", exc)

    # ── Whale detection ───────────────────────────────────────────────────────

    def _check_whale(self, event: dict[str, Any]) -> None:
        """Check event for watched wallet activity."""
        if not self._tripwire:
            return
        try:
            whale = self._tripwire.check_event(event)
            if whale and self._signal_engine:
                signal = self._signal_engine.on_whale_trade(whale)
                if signal and self._paper_executor:
                    self._paper_executor.on_signal(signal)
        except Exception as exc:
            logger.debug("Whale check error: %s", exc)

    # ── Tier-1 wallet polling ──────────────────────────────────────────────

    async def _tier1_poll_loop(self) -> None:
        """Poll Data API every TIER1_POLL_INTERVAL_SEC seconds for tier-1
        wallet trades on markets outside the WebSocket universe.

        2026-04-24: dropped from 900s → 300s. The 15min cadence was
        leaving up to 14m59s of alpha window unmonitored on each tier-1
        wallet's off-WS markets. 300s is still well below any tier-1
        Data API rate limits (~6 calls/min/wallet * 343 wallets =
        ~2k/min — Polymarket's Data API absorbs this trivially).
        Override via TIER1_POLL_INTERVAL_SEC env if needed.
        """
        import os
        _POLL_INTERVAL = int(os.environ.get("TIER1_POLL_INTERVAL_SEC", "300"))
        _last_seen: dict[str, int] = {}

        # Initial delay (let WebSocket settle), then run immediately
        await asyncio.sleep(60)

        if self._tripwire and self._tripwire.watched_tier1:
            n_wallets = len(self._tripwire.watched_tier1)
            logger.info("Tier-1 poll loop starting: %d wallets", n_wallets)
            print(f"[TIER1-POLL] Starting loop with {n_wallets} wallets")

        while True:
            try:
                if self._tripwire and self._tripwire.watched_tier1:
                    polled = await self._poll_tier1_wallets(_last_seen)
                    self._write_tier1_poll_status(polled, success=True)
                else:
                    self._write_tier1_poll_status(0, success=True)
            except Exception as exc:
                logger.warning("Tier-1 poll error: %s", exc)
                print(f"[TIER1-POLL] Error: {exc}")
                self._write_tier1_poll_status(0, success=False)
            await asyncio.sleep(_POLL_INTERVAL)

    def _write_tier1_poll_status(self, wallets_polled: int, success: bool) -> None:
        """Update monitor_status.json with tier-1 poll status."""
        try:
            _proj_root = Path(__file__).resolve().parents[3]
            status_path = _proj_root / "data" / "system" / "monitor_status.json"
            status_path.parent.mkdir(parents=True, exist_ok=True)
            existing = {}
            if status_path.exists():
                try:
                    existing = json.loads(status_path.read_text(encoding="utf-8"))
                except Exception:
                    pass
            now = int(datetime.now(tz=timezone.utc).timestamp())
            existing["tier1_poll"] = {
                "last_poll_at": now,
                "last_poll_age_seconds": 0,
                "wallets_polled": wallets_polled,
                "last_poll_success": success,
            }
            status_path.write_text(json.dumps(existing, indent=2), encoding="utf-8")
        except Exception as exc:
            logger.debug("Failed to write tier1_poll_status: %s", exc)

    async def _poll_tier1_wallets(self, last_seen: dict[str, int]) -> int:
        """Fetch recent trades for tier-1 wallets from Data API. Returns count polled."""
        try:
            from trading_platform.polymarket.data_api_fetcher import PolymarketDataApiFetcher
        except ImportError as exc:
            logger.warning("Tier-1 poll: cannot import fetcher: %s", exc)
            return 0

        fetcher = PolymarketDataApiFetcher()
        polled = 0
        for wallet in list(self._tripwire.watched_tier1)[:50]:
            try:
                # Use _fetch_page directly for non-blocking poll
                trades = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda w=wallet: fetcher._fetch_page({"user": w, "limit": 10}),
                )
            except Exception as exc:
                logger.debug("Poll failed for %s: %s", wallet[:10], exc)
                continue

            polled += 1
            if not trades:
                continue

            cutoff = last_seen.get(wallet, 0)
            for trade in trades:
                ts = trade.get("timestamp") or trade.get("createdAt") or 0
                if isinstance(ts, str):
                    try:
                        from datetime import datetime as _dt
                        ts = int(_dt.fromisoformat(ts.replace("Z", "+00:00")).timestamp())
                    except Exception:
                        ts = 0
                try:
                    ts = int(ts)
                except (TypeError, ValueError):
                    ts = 0
                if ts <= cutoff:
                    continue

                event = {
                    "maker": wallet,
                    "side": trade.get("side", "BUY"),
                    "price": trade.get("price", 0),
                    "size": trade.get("size") or trade.get("amount", 0),
                    "asset_id": trade.get("asset") or trade.get("token_id", ""),
                    # Data API returns camelCase "conditionId", not "condition_id"
                    "condition_id": (
                        trade.get("conditionId")
                        or trade.get("condition_id")
                        or ""
                    ),
                    "question": trade.get("title", ""),
                    "outcome": trade.get("outcome", ""),
                    "timestamp": ts,
                }
                self._check_whale(event)

            all_ts = [t.get("timestamp") or t.get("createdAt") or 0 for t in trades]
            int_ts = [int(t) for t in all_ts if isinstance(t, (int, float))]
            if int_ts:
                last_seen[wallet] = max(int_ts)

            await asyncio.sleep(0.2)

        return polled

    # ── Hourly export ────────────────────────────────────────────────────────

    async def _export_loop(self) -> None:
        while True:
            await asyncio.sleep(self.config.export_interval_sec)
            try:
                await self._export_hourly_bars()
            except Exception as exc:
                logger.warning("Hourly export failed: %s", exc)
            # Prune ticks for resolved markets older than 7 days.
            # Keeps prices.db from growing unbounded (~600 MB at 50k
            # ticks/day with 1000 subscribed markets).
            try:
                if self._store:
                    pruned = self._store.prune_old_ticks(keep_days=7)
                    if pruned:
                        print(f"[prune] deleted {pruned} old ticks from resolved markets", flush=True)
            except Exception as exc:
                logger.debug("tick prune failed: %s", exc)

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
        now_ts = int(now.timestamp())
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

        # Write ws_status.json for the API
        _proj_root = Path(__file__).resolve().parents[3]
        ws_status_path = _proj_root / "data" / "polymarket" / "ws_status.json"
        ws_status_path.parent.mkdir(parents=True, exist_ok=True)
        t1 = len(self._tripwire.watched_tier1) if self._tripwire else 0
        t2 = len(self._tripwire.watched_tier2) if self._tripwire else 0

        # Category counts — use MarketUniverse for accurate classification
        categories: dict[str, int] = {}
        _universe = None
        if self._tripwire and getattr(self._tripwire, "universe", None):
            _universe = self._tripwire.universe
        else:
            try:
                from trading_platform.polymarket.market_universe import MarketUniverse
                _u = MarketUniverse()
                if _u.load_cached() and len(_u.get_all_condition_ids()) > 0:
                    _universe = _u
            except Exception:
                pass

        if _universe and len(_universe.get_all_condition_ids()) > 0:
            for m in self.markets:
                cid = m.condition_id or m.market_id
                cat = _universe.get_category(cid) or "other"
                categories[cat] = categories.get(cat, 0) + 1
        else:
            categories["unknown"] = len(self.markets)

        # Count signals today
        signals_today = 0
        if self._signal_engine:
            try:
                signals_today = len(self._signal_engine.get_recent_signals(hours=24.0))
            except Exception:
                pass

        # Count wallet-derived markets if tripwire universe is loaded
        wallet_derived_count = 0
        if self._tripwire and getattr(self._tripwire, "universe", None):
            try:
                wallet_cids = self._tripwire.universe.get_wallet_derived_markets(days_back=14)
                subscribed_cids = {m.condition_id for m in self.markets if m.condition_id}
                wallet_derived_count = len(wallet_cids & subscribed_cids)
            except Exception:
                pass

        # Track tier1h count from tripwire
        t1h = sum(1 for p in (self._tripwire.leaderboard.values() if self._tripwire else [])
                  if p.get("tier") == "tier1h")

        ws_status = {
            "connected": True,
            "markets_subscribed": len(self.markets),
            "wallet_derived_markets": wallet_derived_count,
            "watched_wallets": t1 + t2,
            "tier1_wallets": t1,
            "tier1h_wallets": t1h,
            "tier2_wallets": t2,
            "signals_today": signals_today,
            "last_event_ts": now_ts,
            "categories": categories,
            "written_at": now_ts,
        }
        ws_status_path.write_text(json.dumps(ws_status, indent=2), encoding="utf-8")

        # Heartbeat into service_health so the watchdog can detect a silent
        # WS death. Without this, a stalled WS only shows up indirectly
        # through wallet_trades freshness — often hours later. This table
        # is the ground truth for "is the collector alive right now?".
        try:
            from trading_platform.polymarket.db_connection import get_connection
            conn = get_connection()
            try:
                detail = f"markets={len(self.markets)} ticks={self.state.ticks_stored}"
                conn.execute(
                    "INSERT INTO service_health (service, status, error_message, checked_at) VALUES (?, ?, ?, ?)",
                    ("live_collect", "ok", detail, now_ts),
                )
                conn.commit()
            finally:
                try: conn.close()
                except Exception: pass
        except Exception as exc:
            logger.debug("heartbeat write failed: %s", exc)

        # Periodic tripwire reload
        if self._tripwire:
            self._tripwire.maybe_reload()

    async def _tracemalloc_dump_loop(self) -> None:
        """Dump top memory allocators every 15 min (when LIVE_COLLECT_TRACEMALLOC=1).

        Writes with ``print(flush=True)`` rather than ``logger.info`` because
        the live-collect CLI entry point doesn't configure a logging handler,
        so INFO-level logger calls get swallowed — the prints here reach
        docker logs unconditionally.
        """
        import tracemalloc
        print("[tracemalloc-loop] started, sleeping 300s before first dump", flush=True)
        await asyncio.sleep(300)  # let the collector warm up 5 min first
        print("[tracemalloc-loop] 5-min warmup complete, entering dump loop", flush=True)
        while True:
            try:
                import resource as _resource
                import threading as _threading
                rss_kb = _resource.getrusage(_resource.RUSAGE_SELF).ru_maxrss
                try:
                    os_thread_count = len(list((Path("/proc/1/task")).iterdir()))
                except Exception:
                    os_thread_count = -1
                py_thread_count = _threading.active_count()
                snap = tracemalloc.take_snapshot()
                # Filter out our own tracemalloc frames + standard lib noise
                snap = snap.filter_traces((
                    tracemalloc.Filter(False, "<frozen importlib._bootstrap>"),
                    tracemalloc.Filter(False, "<frozen importlib._bootstrap_external>"),
                    tracemalloc.Filter(False, tracemalloc.__file__),
                ))
                stats = snap.statistics("lineno")[:15]
                total_traced = sum(s.size for s in snap.statistics("lineno"))
                print(
                    f"[tracemalloc] rss_max={rss_kb/1024:.0f}MB "
                    f"traced={total_traced/1024/1024:.1f}MB "
                    f"os_threads={os_thread_count} py_threads={py_thread_count} "
                    f"price_buf={len(self._price_buffer)} "
                    f"latest_prices={len(self.state.latest_prices)} "
                    f"tick_batch={len(self._tick_batch)}",
                    flush=True,
                )
                print("[tracemalloc] top 15 allocators by size:", flush=True)
                for i, s in enumerate(stats, 1):
                    frame = s.traceback[0] if s.traceback else None
                    loc = f"{Path(frame.filename).name}:{frame.lineno}" if frame else "?"
                    print(f"  {i:2d}. {loc:40s}  {s.size/1024:7.1f} KB  ({s.count} allocs)", flush=True)
            except Exception as exc:
                print(f"[tracemalloc] dump failed: {exc}", flush=True)
            await asyncio.sleep(15 * 60)

    async def _fundamental_scan_loop(self) -> None:
        """Scan for fundamental (non-wallet) signals every 15 min.

        These signals are uncorrelated with the wallet cluster — they
        derive from price patterns and market lifecycle, providing true
        diversification in the ensemble scorer.
        """
        await asyncio.sleep(180)
        while True:
            try:
                from trading_platform.polymarket.fundamental_signals import (
                    scan_price_momentum, scan_resolution_proximity,
                )
                signals = scan_price_momentum() + scan_resolution_proximity()
                placed = 0
                for sig in signals:
                    if self._paper_executor:
                        try:
                            result = self._paper_executor.execute_signal(sig)
                            if result:
                                placed += 1
                        except Exception:
                            pass
                if placed:
                    print(f"[FUNDAMENTAL] Placed {placed} trades from {len(signals)} signals", flush=True)
            except Exception as exc:
                logger.debug("fundamental scan failed: %s", exc)
            await asyncio.sleep(900)

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self.config.heartbeat_interval_sec)
            logger.info(
                "[heartbeat] %d ticks collected, %d markets active",
                self.state.ticks_stored,
                len(self.state.latest_prices),
            )
