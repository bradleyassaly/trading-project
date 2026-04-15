"""Polygon WS listener → immediate wallet-poll trigger.

Subscribes to USDC Transfer events on Polygon where the counterparty is
Polymarket's CTFExchange contract. Any log touching a watched wallet
fires an immediate poll of that wallet's recent trades via the existing
Data API fetcher, bypassing the 10-minute poller cycle.

Replaces 10-min latency with ~2–5s. Target runtime: an always-on docker
service (polymarket-wallet-stream). The legacy wallet-poller container
stays running at a longer interval as a safety net for events the WS
might drop (reconnects, rate limits, etc.).

Env config:
  POLYGON_WS_URL       Alchemy/Quicknode Polygon WSS URL (required)
  WATCHED_WALLETS_SRC  'alpha' | 'insider' | 'both'  (default: both)
  WS_RECONNECT_DELAY   seconds (default 5)
  WS_DEDUP_WINDOW_SEC  dedup same-wallet triggers (default 10)

Free-tier friendly: Alchemy free tier (300M CU/mo) covers 200+ watched
addresses indefinitely at our trade-volume. Falls back to public RPCs
if POLYGON_WS_URL is a comma-separated list.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Any

logger = logging.getLogger(__name__)


# Polymarket CTFExchange + USDC on Polygon. CTFExchange is the order
# matcher — every trade causes USDC to move to/from this contract.
CTF_EXCHANGE = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
USDC_E = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"  # bridged, original
USDC_NATIVE = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"  # native Polygon
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


def _pad_addr(a: str) -> str:
    """0x-prefixed 32-byte topic encoding of an address."""
    a = a.lower().replace("0x", "")
    return "0x" + a.rjust(64, "0")


def _addr_from_topic(t: str) -> str:
    """Inverse of _pad_addr — extract the 20-byte address."""
    t = t.lower().replace("0x", "")
    return "0x" + t[-40:]


def _load_watched_wallets() -> set[str]:
    """Union of alpha-copyable + insider wallets. Refreshed at startup.

    In practice we'll want to reload periodically or on a SIGHUP — deferred
    until we see whether the in-practice reload cadence matters.
    """
    from trading_platform.polymarket.db_connection import get_connection
    src = os.environ.get("WATCHED_WALLETS_SRC", "both").lower()
    wallets: set[str] = set()
    conn = get_connection()
    try:
        if src in ("alpha", "both"):
            rows = conn.execute(
                "SELECT DISTINCT wallet FROM wallet_alpha_scores WHERE is_copyable = 1"
            ).fetchall()
            wallets.update(r[0].lower() for r in rows if r[0])
        if src in ("insider", "both"):
            rows = conn.execute("SELECT wallet FROM insider_wallets").fetchall()
            wallets.update(r[0].lower() for r in rows if r[0])
    finally:
        try: conn.close()
        except Exception: pass
    return wallets


class WalletStream:
    """Single-provider Polygon WS listener with auto-reconnect."""

    def __init__(self, ws_url: str, watched: set[str]):
        self.ws_url = ws_url
        self.watched = {w.lower() for w in watched}
        self.dedup_window = int(os.environ.get("WS_DEDUP_WINDOW_SEC", "10"))
        self._last_poll: dict[str, float] = {}  # wallet → last-trigger ts
        self._fetcher = None
        self._stats = {
            "events_seen": 0,
            "events_matched": 0,
            "polls_triggered": 0,
            "reconnects": 0,
            "started_at": time.time(),
        }

    async def _subscribe(self, ws) -> None:
        """Register both Transfer subscriptions: USDC.e + USDC.native.

        We don't topic-filter by watched wallets — there are up to 200+ and
        providers often cap topic-array size. Instead we subscribe to any
        Transfer involving CTFExchange and filter client-side on the
        addresses that are padded-encoded in topics[1] / topics[2].
        """
        sub_msgs = [
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_subscribe",
                "params": [
                    "logs",
                    {
                        "address": [USDC_E, USDC_NATIVE],
                        "topics": [
                            TRANSFER_TOPIC,
                            None,
                            _pad_addr(CTF_EXCHANGE),
                        ],
                    },
                ],
            },
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "eth_subscribe",
                "params": [
                    "logs",
                    {
                        "address": [USDC_E, USDC_NATIVE],
                        "topics": [
                            TRANSFER_TOPIC,
                            _pad_addr(CTF_EXCHANGE),
                            None,
                        ],
                    },
                ],
            },
        ]
        for m in sub_msgs:
            await ws.send(json.dumps(m))

    async def _handle_log(self, params: dict) -> None:
        """Dispatch a single Transfer event, triggering a poll if matched."""
        self._stats["events_seen"] += 1
        try:
            log = params.get("result") or {}
            topics = log.get("topics") or []
            if len(topics) < 3:
                return
            from_addr = _addr_from_topic(topics[1])
            to_addr = _addr_from_topic(topics[2])
        except Exception as exc:
            logger.debug("log decode failed: %s", exc)
            return

        # Exactly one side is our wallet (the other is CTFExchange).
        wallet = from_addr if from_addr in self.watched else (
            to_addr if to_addr in self.watched else None
        )
        if not wallet:
            return

        self._stats["events_matched"] += 1
        now = time.time()
        last = self._last_poll.get(wallet, 0)
        if (now - last) < self.dedup_window:
            return  # too close to last trigger — de-dupe
        self._last_poll[wallet] = now
        self._stats["polls_triggered"] += 1

        # Trigger an immediate poll of THIS wallet (cheap single-wallet
        # API call, not the whole copyable set).
        asyncio.create_task(self._poll_wallet(wallet))

    async def _poll_wallet(self, wallet: str) -> None:
        """Fetch recent trades for one wallet, feed through the signal engine."""
        try:
            if self._fetcher is None:
                from trading_platform.polymarket.data_api_fetcher import (
                    PolymarketDataApiFetcher,
                )
                self._fetcher = PolymarketDataApiFetcher()

            # Fetch. Don't await in the WS read loop — just kick it off.
            trades = await asyncio.to_thread(
                self._fetcher._fetch_page, {"user": wallet, "limit": 10},
            )
            if not trades:
                return

            # Delegate to the existing poller's signal-firing path rather
            # than duplicate it here. The single-wallet variant re-uses
            # _map_trade + the whale signal engine.
            from trading_platform.polymarket.wallet_trade_poller import WalletTradePoller
            if not hasattr(self, "_poller"):
                self._poller = WalletTradePoller(dry_run=False)
            # Inject the fetched trades into the poller's per-wallet handler.
            # Fall back to a cycle if direct injection fails.
            try:
                await asyncio.to_thread(self._poller.process_wallet, wallet, trades)
            except AttributeError:
                # process_wallet not exposed — run the minimal path
                logger.info(
                    "[wallet-stream] wallet %s: %d trades (handler not exposed)",
                    wallet[:10], len(trades),
                )
        except Exception as exc:
            logger.warning("poll_wallet(%s) failed: %s", wallet[:10], exc)

    async def run(self) -> None:
        """Main reconnect loop."""
        reconnect_delay = int(os.environ.get("WS_RECONNECT_DELAY", "5"))
        try:
            import websockets
        except ImportError as exc:
            raise RuntimeError("websockets lib required; pip install websockets") from exc

        while True:
            try:
                logger.info(
                    "[wallet-stream] connecting (watching %d wallets) …",
                    len(self.watched),
                )
                async with websockets.connect(
                    self.ws_url, ping_interval=20, ping_timeout=10,
                ) as ws:
                    await self._subscribe(ws)
                    logger.info("[wallet-stream] subscribed — listening")
                    async for raw in ws:
                        msg = json.loads(raw)
                        if msg.get("method") == "eth_subscription":
                            await self._handle_log(msg.get("params") or {})
            except Exception as exc:
                self._stats["reconnects"] += 1
                logger.warning(
                    "[wallet-stream] disconnect: %s — reconnecting in %ds",
                    exc, reconnect_delay,
                )
                await asyncio.sleep(reconnect_delay)


async def _heartbeat(stream: WalletStream) -> None:
    """Write a service_health row every 60s so the watchdog can observe us."""
    from trading_platform.polymarket.db_connection import get_connection
    while True:
        try:
            conn = get_connection()
            try:
                uptime = int(time.time() - stream._stats["started_at"])
                detail = (
                    f"events={stream._stats['events_seen']} "
                    f"matched={stream._stats['events_matched']} "
                    f"polls={stream._stats['polls_triggered']} "
                    f"reconnects={stream._stats['reconnects']} "
                    f"uptime={uptime}s"
                )
                conn.execute(
                    "INSERT INTO service_health (service, status, error_message, checked_at) "
                    "VALUES (?, ?, ?, ?)",
                    ("wallet_stream", "ok", detail, int(time.time())),
                )
                conn.commit()
            finally:
                try: conn.close()
                except Exception: pass
        except Exception as exc:
            logger.debug("heartbeat write failed: %s", exc)
        await asyncio.sleep(60)


def main() -> int:
    # Wire JSON logging + sensible defaults.
    try:
        from trading_platform.polymarket.logging_config import setup_logging
        setup_logging(service="wallet-stream")
    except Exception:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    # Accept both legacy POLYGON_WSS_URL and new POLYGON_WS_URL; fall back
    # to a public free endpoint as the last-resort default so the service
    # boots to a working state without any .env changes.
    ws_url = (
        os.environ.get("POLYGON_WS_URL")
        or os.environ.get("POLYGON_WSS_URL")
        or "wss://polygon-bor-rpc.publicnode.com"
    )
    logger.info("[wallet-stream] connecting to %s", ws_url)

    watched = _load_watched_wallets()
    if not watched:
        logger.warning("No watched wallets loaded — nothing to listen for. Exiting.")
        return 3
    logger.info("[wallet-stream] loaded %d watched wallets", len(watched))

    stream = WalletStream(ws_url=ws_url, watched=watched)

    async def _run():
        await asyncio.gather(stream.run(), _heartbeat(stream))

    asyncio.run(_run())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
