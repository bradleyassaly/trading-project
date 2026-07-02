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
NEG_RISK_CTF_EXCHANGE = "0xC5d563A36AE78145C45a50134d48A1215220f80a"
USDC_E = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"  # bridged, original
USDC_NATIVE = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"  # native Polygon
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
# OrderFilled(bytes32 orderHash, address maker, address taker,
#   uint256 makerAssetId, uint256 takerAssetId, uint256 makerAmountFilled,
#   uint256 takerAmountFilled, uint256 fee)
# Indexed: orderHash, maker, taker  → topics[1], topics[2], topics[3]
# Non-indexed: 5 uint256 in data (makerAssetId, takerAssetId, makerAmount, takerAmount, fee)
ORDER_FILLED_TOPIC = "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"


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
        self._reconnect_requested = False
        # token_id → (cached_at, meta|None). Local markets-table lookups for
        # chain-direct dispatch; None is cached too so unknown tokens don't
        # hammer the DB on every fill.
        self._token_meta_cache: dict[str, tuple[float, dict | None]] = {}
        self._stats = {
            "events_seen": 0,
            "events_matched": 0,
            "polls_triggered": 0,
            "chain_direct_dispatches": 0,
            "chain_direct_misses": 0,
            "reconnects": 0,
            "started_at": time.time(),
        }

    async def _subscribe(self, ws) -> None:
        """Register narrow Transfer subscriptions filtered to watched wallets.

        Critical for free tier survival: passing the watched-wallet set
        into the topic filter means the provider only delivers events
        matching our wallets (~1–3/sec) instead of the full CTFExchange
        flow (~168/sec). First version of this file used the broad filter
        and burned 300M CU of Alchemy credits in ~25 min.

        Each JSON-RPC log filter supports arrays at topic positions. We
        still need two subscriptions because we can't AND (from=wallet
        AND to=CTFExchange) OR (from=CTFExchange AND to=wallet) in a
        single filter — the AND is across positions, not OR.
        """
        watched_padded = [_pad_addr(w) for w in self.watched]
        ctf_padded = _pad_addr(CTF_EXCHANGE)
        logger.info(
            "[wallet-stream] subscribing with %d watched addresses in topic filter",
            len(watched_padded),
        )
        sub_msgs = [
            # Wallet → CTFExchange (BUY side: we're sending USDC)
            {
                "jsonrpc": "2.0", "id": 1, "method": "eth_subscribe",
                "params": [
                    "logs",
                    {
                        "address": [USDC_E, USDC_NATIVE],
                        "topics": [TRANSFER_TOPIC, watched_padded, ctf_padded],
                    },
                ],
            },
            # CTFExchange → Wallet (SELL side or payouts)
            {
                "jsonrpc": "2.0", "id": 2, "method": "eth_subscribe",
                "params": [
                    "logs",
                    {
                        "address": [USDC_E, USDC_NATIVE],
                        "topics": [TRANSFER_TOPIC, ctf_padded, watched_padded],
                    },
                ],
            },
            # OrderFilled where MAKER is a watched wallet (they placed
            # a resting order that got filled). 2-5s chain-native latency.
            {
                "jsonrpc": "2.0", "id": 3, "method": "eth_subscribe",
                "params": [
                    "logs",
                    {
                        "address": [CTF_EXCHANGE, NEG_RISK_CTF_EXCHANGE],
                        "topics": [ORDER_FILLED_TOPIC, None, watched_padded],
                    },
                ],
            },
            # OrderFilled where TAKER is a watched wallet (they took
            # liquidity — active market buy/sell).
            {
                "jsonrpc": "2.0", "id": 4, "method": "eth_subscribe",
                "params": [
                    "logs",
                    {
                        "address": [CTF_EXCHANGE, NEG_RISK_CTF_EXCHANGE],
                        "topics": [ORDER_FILLED_TOPIC, None, None, watched_padded],
                    },
                ],
            },
        ]
        for m in sub_msgs:
            await ws.send(json.dumps(m))

    async def _handle_log(self, params: dict) -> None:
        """Route incoming log event by topic0 to the right handler."""
        self._stats["events_seen"] += 1
        try:
            log = params.get("result") or {}
            topics = log.get("topics") or []
            if not topics:
                return
            topic0 = topics[0].lower()
        except Exception as exc:
            logger.debug("log decode failed: %s", exc)
            return

        if topic0 == ORDER_FILLED_TOPIC.lower():
            await self._handle_order_filled(log, topics)
        elif topic0 == TRANSFER_TOPIC.lower():
            await self._handle_transfer(log, topics)

    async def _handle_transfer(self, log: dict, topics: list) -> None:
        """Transfer event → trigger single-wallet REST poll. Legacy path."""
        if len(topics) < 3:
            return
        from_addr = _addr_from_topic(topics[1])
        to_addr = _addr_from_topic(topics[2])
        wallet = from_addr if from_addr in self.watched else (
            to_addr if to_addr in self.watched else None
        )
        if not wallet:
            return
        self._stats["events_matched"] += 1
        now = time.time()
        last = self._last_poll.get(wallet, 0)
        if (now - last) < self.dedup_window:
            return
        self._last_poll[wallet] = now
        self._stats["polls_triggered"] += 1
        asyncio.create_task(self._poll_wallet(wallet))

    async def _handle_order_filled(self, log: dict, topics: list) -> None:
        """Chain-native trade: decode OrderFilled directly — no REST lag.

        Topic layout (3 indexed):
          topics[0] = event signature
          topics[1] = orderHash
          topics[2] = maker
          topics[3] = taker
        Data layout (5 × 32 bytes, non-indexed):
          [0]  makerAssetId
          [1]  takerAssetId
          [2]  makerAmountFilled
          [3]  takerAmountFilled
          [4]  fee

        For a BUY: maker's asset = USDC (id=0), taker's asset = token.
        For a SELL: maker's asset = token, taker's asset = USDC (id=0).
        We infer direction from whichever side has asset_id=0.
        """
        if len(topics) < 4:
            return
        maker = _addr_from_topic(topics[2])
        taker = _addr_from_topic(topics[3])
        if maker in self.watched:
            wallet, role = maker, "maker"
        elif taker in self.watched:
            wallet, role = taker, "taker"
        else:
            return
        self._stats["events_matched"] += 1
        self._stats["order_filled_decoded"] = self._stats.get("order_filled_decoded", 0) + 1

        data_hex = (log.get("data") or "0x").lower().replace("0x", "")
        if len(data_hex) < 64 * 5:
            logger.debug("OrderFilled data too short: %d", len(data_hex))
            return
        def _u256(i: int) -> int:
            return int(data_hex[i * 64:(i + 1) * 64], 16)
        maker_asset_id = _u256(0)
        taker_asset_id = _u256(1)
        maker_amount = _u256(2)
        taker_amount = _u256(3)

        # asset_id=0 represents USDC (collateral). The other side is the
        # binary-outcome conditional token.
        if maker_asset_id == 0 and taker_asset_id != 0:
            # Maker paid USDC → maker is BUYING the token
            # Price ≈ maker_amount (USDC) / taker_amount (shares)
            side = "BUY" if role == "maker" else "SELL"
            token_id = taker_asset_id
            usdc = maker_amount / 1e6
            shares = taker_amount / 1e6
        elif taker_asset_id == 0 and maker_asset_id != 0:
            # Maker sold token for USDC → maker is SELLING
            side = "SELL" if role == "maker" else "BUY"
            token_id = maker_asset_id
            usdc = taker_amount / 1e6
            shares = maker_amount / 1e6
        else:
            # Token-to-token — rare; skip
            return
        price = usdc / shares if shares > 0 else 0
        tx_hash = log.get("transactionHash") or ""

        logger.info(
            "[chain-trade] %s %s %s %.2f shares @ %.4f ($%.2f) tx=%s",
            wallet[:10], role, side, shares, price, usdc, tx_hash[:10] + "..." if tx_hash else "?",
        )

        # 2026-07-02 CHAIN-DIRECT DISPATCH — the missing last mile.
        # Previously we decoded the fill here and then WAITED on a Data API
        # poll for market metadata ("chain tells us WHEN, REST tells us WHAT
        # MARKET"). Data-api indexing lag made p50 whale-fill→our-attempt
        # detection latency 8.8 MINUTES (daily review section 14, day 1).
        # The local markets table already maps token_id → (condition_id,
        # question, YES/NO side) for the tracked universe, so for known
        # tokens we synthesize the data-api-shaped trade dict and feed
        # process_wallet() IMMEDIATELY — seconds, not minutes. Dedup is
        # safe: process_wallet keys on transaction_hash + last_checked_ts,
        # so the later hydration poll re-delivering this fill is a no-op.
        meta: dict | None = None
        if os.environ.get("CHAIN_DIRECT_DISPATCH", "1").lower() in ("1", "true", "yes"):
            meta = self._market_for_token(str(token_id))
            if meta:
                synthetic = {
                    "proxyWallet": wallet,
                    "side": side,
                    "asset": str(token_id),
                    "conditionId": meta["condition_id"],
                    "size": shares,
                    "price": price,
                    # Receipt time ≈ block time on Polygon (2s blocks);
                    # becomes whale_trade_ts downstream, so section 14's
                    # latency metric measures this exact path.
                    "timestamp": int(time.time()),
                    "title": meta.get("question") or "",
                    "outcome": meta.get("outcome") or "YES",
                    "transactionHash": tx_hash,
                }
                try:
                    from trading_platform.polymarket.wallet_trade_poller import WalletTradePoller
                    if not hasattr(self, "_poller"):
                        self._poller = WalletTradePoller(dry_run=False)
                    res = await asyncio.to_thread(
                        self._poller.process_wallet, wallet, [synthetic]
                    )
                    self._stats["chain_direct_dispatches"] += 1
                    logger.info(
                        "[chain-direct] dispatched %s %s %.2f@%.4f on %s → %s",
                        wallet[:10], side, shares, price,
                        (meta.get("question") or "?")[:40], res,
                    )
                except Exception as exc:
                    logger.warning("[chain-direct] dispatch failed (poll will cover): %s", exc)
            else:
                self._stats["chain_direct_misses"] += 1
                logger.info(
                    "[chain-direct] token %s… not in local markets table — "
                    "falling back to REST poll only", str(token_id)[:16],
                )

        # 2026-07-02 INSTANT MIRROR EXIT. Entry latency is fixed above; exits
        # still waited for the 5-min monitor cycle — and whale_mirror_exit
        # had the best per-trade exit economics in the April audit, half of
        # which evaporates if we exit minutes after the whale. When a watched
        # wallet SELLS a market we currently hold live, run the exit monitor
        # immediately (it processes all open positions — cheap at our size).
        if side == "SELL":
            asyncio.create_task(self._maybe_trigger_exit_check(wallet, meta))

        # Hydration poll (also the only path for unknown tokens): fetches the
        # full data-api record (category enrichment, canonical fields). The
        # direct dispatch above already fired any signal; this is bookkeeping.
        now = time.time()
        last = self._last_poll.get(wallet, 0)
        if (now - last) < 3:  # tighter dedup for chain events
            return
        self._last_poll[wallet] = now
        self._stats["polls_triggered"] += 1
        asyncio.create_task(self._poll_wallet(wallet))

    async def _maybe_trigger_exit_check(self, wallet: str, meta: dict | None) -> None:
        """Run the live exit monitor now if this whale-sell touches our book.

        Trigger conditions: we hold an open live position either on the same
        market (condition_id match) or sourced from this wallet (mirror
        semantics). 60s debounce — the monitor sweeps all open positions,
        so one run covers a burst of sells.
        """
        now = time.time()
        if (now - getattr(self, "_last_exit_check", 0)) < 60:
            return
        try:
            def _holds() -> bool:
                from trading_platform.polymarket.db_connection import get_connection
                conn = get_connection()
                try:
                    cid = (meta or {}).get("condition_id") or ""
                    row = conn.execute(
                        """SELECT 1 FROM live_trades
                            WHERE dry_run = 0 AND exit_ts IS NULL
                              AND (LOWER(COALESCE(signal_wallet, '')) = ?
                                   OR (? != '' AND condition_id = ?))
                            LIMIT 1""",
                        (wallet.lower(), cid, cid),
                    ).fetchone()
                    return bool(row)
                finally:
                    try: conn.close()
                    except Exception: pass
            if not await asyncio.to_thread(_holds):
                return
            self._last_exit_check = now
            self._stats["mirror_exit_triggers"] = self._stats.get("mirror_exit_triggers", 0) + 1
            logger.info("[chain-direct] whale SELL touches our book — running exit monitor now")
            from trading_platform.polymarket.live_position_monitor import check_live_exits
            res = await asyncio.to_thread(check_live_exits)
            logger.info("[chain-direct] triggered exit check → %s", res)
        except Exception as exc:
            logger.warning("[chain-direct] exit-check trigger failed: %s", exc)

    def _market_for_token(self, token_id: str) -> dict | None:
        """Local markets-table lookup: token → condition/question/side.

        1h TTL cache including negative results (unknown tokens are usually
        markets outside our tracked universe; they stay unknown for a while).
        """
        now = time.time()
        cached = self._token_meta_cache.get(token_id)
        if cached and (now - cached[0]) < 3600:
            return cached[1]
        meta: dict | None = None
        try:
            # Local markets table first; falls back to a one-shot Gamma
            # by-token fetch (~200ms) and persists the result — a whale
            # fill on an unindexed market is still worth copying.
            from trading_platform.polymarket.markets_table import get_by_token_id
            meta = get_by_token_id(token_id)
        except Exception as exc:
            logger.debug("token→market lookup failed for %s: %s", token_id[:16], exc)
        self._token_meta_cache[token_id] = (now, meta)
        return meta

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

    def request_reconnect(self) -> None:
        """Signal `run()` to drop the current WS and re-subscribe.
        Used by the watched-list reloader after new wallets are added —
        the topic filter has to be re-issued to cover them.
        """
        self._reconnect_requested = True

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
                    self._reconnect_requested = False
                    logger.info("[wallet-stream] subscribed — listening")
                    async for raw in ws:
                        if self._reconnect_requested:
                            logger.info("[wallet-stream] reconnect requested — closing")
                            await ws.close()
                            break
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


async def _watched_reloader(stream: "WalletStream") -> None:
    """Hot-reload the watched-wallet set every WS_RELOAD_SEC.

    Wired 2026-04-24 — closes the gap where new tier1h wallets discovered
    by `pm_leaderboard_sync` (daily) and `orphan_wallet_onboarder` (12h)
    weren't picked up until the next process restart, sometimes 12-24h
    after they qualified.

    Strategy:
      * Re-read the union (alpha_copyable + insiders) every interval.
      * Update `stream.watched` in-place so the routing logic sees the
        new set immediately for any matching event already covered by
        the broad CTFExchange topic filter.
      * If new wallets were added, raise a connection-reset to force
        the WS reconnect loop in `run()` to re-subscribe with the
        expanded topic-filter (only newly-included wallets need that).
    """
    interval = int(os.environ.get("WS_RELOAD_SEC", "300"))
    while True:
        try:
            await asyncio.sleep(interval)
            new = _load_watched_wallets()
            if not new:
                continue
            new_lower = {w.lower() for w in new}
            added = new_lower - stream.watched
            removed = stream.watched - new_lower
            if not added and not removed:
                continue
            stream.watched = new_lower
            logger.info(
                "[wallet-stream] watched-list reload: +%d -%d (now %d)",
                len(added), len(removed), len(new_lower),
            )
            if added:
                # Force reconnect so the topic filter expands to cover
                # the newly added wallets. Existing connection won't
                # see their events otherwise.
                stream.request_reconnect()
        except Exception as exc:
            logger.debug("[wallet-stream] reloader error: %s", exc)


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
        await asyncio.gather(
            stream.run(),
            _heartbeat(stream),
            _watched_reloader(stream),
        )

    asyncio.run(_run())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
