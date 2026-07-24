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


# Polymarket exchange contracts on Polygon. The exchange is the order
# matcher — every trade emits OrderFilled from one of these.
#
# V1 (dead): Polymarket migrated off these ~mid-June 2026 — eth_getLogs shows
# ZERO logs on them while every live fill routes through the V2 vanity
# deploys below. This is what actually darkened the fast lane for a week
# (discovered 2026-07-07 by pulling receipts of our own fills).
CTF_EXCHANGE = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
NEG_RISK_CTF_EXCHANGE = "0xC5d563A36AE78145C45a50134d48A1215220f80a"
# V2 (live): vanity-address redeploys ("e111…" exchange, "e2222…" neg-risk),
# verified 2026-07-07 against receipts of our watched wallets' fills.
CTF_EXCHANGE_V2 = "0xe111180000d2663c0091e4f400237545b87b996b"
NEG_RISK_CTF_EXCHANGE_V2 = "0xe2222d279d744050d28e00520010520000310f59"
# All four, lowercase: public bor endpoints match `address` filters
# case-sensitively (logs carry lowercase addresses), so checksummed
# addresses in a filter silently match nothing.
EXCHANGES = [a.lower() for a in (
    CTF_EXCHANGE, NEG_RISK_CTF_EXCHANGE,
    CTF_EXCHANGE_V2, NEG_RISK_CTF_EXCHANGE_V2,
)]
USDC_E = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"  # bridged, original
USDC_NATIVE = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"  # native Polygon
# V2 collateral ("c011a7e" ≈ collate[ral]): fills move this token, not USDC.
COLLATERAL_V2 = "0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb"
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
# OrderFilled(bytes32 orderHash, address maker, address taker,
#   uint256 makerAssetId, uint256 takerAssetId, uint256 makerAmountFilled,
#   uint256 takerAmountFilled, uint256 fee)
# Indexed: orderHash, maker, taker  → topics[1], topics[2], topics[3]
# Non-indexed: 5 uint256 in data (makerAssetId, takerAssetId, makerAmount, takerAmount, fee)
ORDER_FILLED_TOPIC = "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"
# V2 OrderFilled: same indexed layout (orderHash, maker, taker) and the same
# first four data words (makerAssetId, takerAssetId, makerAmountFilled,
# takerAmountFilled); 7 data words total (verified against a known fill:
# BUY 10 @ 0.99 decoded as its 0.01 complement leg on the neg-risk V2).
ORDER_FILLED_TOPIC_V2 = "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"
ORDER_FILLED_TOPICS = [ORDER_FILLED_TOPIC, ORDER_FILLED_TOPIC_V2]

# Endpoint failover pool. POLYGON_WS_URL/POLYGON_WSS_URL may be a comma-
# separated list; these defaults are appended so a single flaky public
# endpoint (drpc dropped every ~5 min for a week, 2026-06-30 → 07-07,
# delivering ZERO events the whole time) can't silently kill the lane.
DEFAULT_WS_URLS = [
    "wss://polygon-bor-rpc.publicnode.com",
    "wss://polygon.drpc.org",
]

# If a provider accepts our subscriptions but delivers nothing for this
# long, assume the subscription is broken (some public RPCs ack
# eth_subscribe filters they never serve) and rotate/escalate.
SILENCE_REBOOT_SEC = int(os.environ.get("WS_SILENCE_REBOOT_SEC", "900"))
# Broad mode only: max seconds with ZERO OrderFilled events before we treat
# the (heavy) fill subscription as silently dead and rotate — all of
# Polymarket fills every few seconds, so 300s of nothing is unambiguous.
FILL_SILENCE_REBOOT_SEC = int(os.environ.get("WS_FILL_SILENCE_REBOOT_SEC", "300"))

# ── Chain firehose persist (2026-07-20) ──────────────────────────────────────
# We already receive EVERY OrderFilled on both exchange contracts (broad
# subscription) and used to discard all non-tracked wallets — the root cause
# of the 2% realtime coverage vs Polymarket's own /activity. Now every decoded
# fill is persisted to wallet_trades from BOTH perspectives (maker row + taker
# row) with the BLOCK timestamp, so rows dedup exactly against data-api-sourced
# rows on the composite fill key. Measured volume 2026-07-20: ~450k events/day
# => ~900k wallet-rows/day (~27M/month; retention policy is a known follow-up).
#   all    — persist every wallet's fills (the data-product goal; default)
#   roster — persist only wallets present in wallet_profiles
#   off    — legacy behavior (tracked-wallet dispatch only)
FIREHOSE_MODE = os.environ.get("CHAIN_FIREHOSE_PERSIST", "all").strip().lower()
FIREHOSE_RPC_HTTP = os.environ.get(
    "POLYGON_RPC_HTTP", "https://polygon-bor-rpc.publicnode.com")

# CTF (ConditionalTokens) lifecycle events — SPLIT mints a full outcome set
# for $1 collateral, MERGE burns one back, REDEMPTION settles after
# resolution. Invisible to /activity TRADE feeds and to OrderFilled — the
# one-leg accounting blindness that made arb/MM wallets look like pure
# losers (2026-07-16 fade-test artifact). Topic hashes derived at build time
# via eth_utils.keccak of the canonical Gnosis CTF signatures (verified in
# the prod container 2026-07-20); we subscribe by TOPIC ONLY (no address)
# and self-verify relevance by requiring condition_id to exist in our
# markets table — no trusted-address assumption needed.
CTF_SPLIT_TOPIC = "0x2e6bb91f8cbcda0c93623c54d0403a43514fabc40084ec96b6d5379a74786298"
CTF_MERGE_TOPIC = "0x6f13ca62553fcc2bcd2372180a43949c1e4cebba603901ede2f4e14f36b282ca"
CTF_REDEEM_TOPIC = "0x2682012a4a4f1973119f1c9b90745d1bd91fa2bab387344f044cb3586864d18d"
_CTF_TOPIC_TYPE = {CTF_SPLIT_TOPIC: "split", CTF_MERGE_TOPIC: "merge",
                   CTF_REDEEM_TOPIC: "redeem"}
# Polymarket collateral addresses (lowercase) — the intrinsic per-event filter
# for CTF events; see _handle_ctf_event.
_CTF_COLLATERAL = {a.lower() for a in ("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
                                       "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",
                                       "0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb")}


def _parse_ws_urls() -> list[str]:
    """Endpoint pool: env list first (priority order), defaults appended."""
    raw = os.environ.get("POLYGON_WS_URL") or os.environ.get("POLYGON_WSS_URL") or ""
    urls = [u.strip() for u in raw.split(",") if u.strip()]
    for d in DEFAULT_WS_URLS:
        if d not in urls:
            urls.append(d)
    return urls


def _health_status(stats: dict, now: float, silence_sec: int = SILENCE_REBOOT_SEC) -> str:
    """Heartbeat status. The old heartbeat wrote 'ok' unconditionally, so a
    week of events=0 looked healthy to the watchdog. Degraded means: the
    process is alive but the lane is not delivering."""
    if stats.get("subscribe_errors", 0) > 0 and stats.get("events_seen", 0) == 0:
        return "degraded"
    last = stats.get("last_event_ts") or stats.get("started_at", now)
    if (now - last) > silence_sec:
        return "degraded"
    return "ok"


def _pad_addr(a: str) -> str:
    """0x-prefixed 32-byte topic encoding of an address."""
    a = a.lower().replace("0x", "")
    return "0x" + a.rjust(64, "0")


def _addr_from_topic(t: str) -> str:
    """Inverse of _pad_addr — extract the 20-byte address."""
    t = t.lower().replace("0x", "")
    return "0x" + t[-40:]


# ── Pure log decoders (extracted from the async handlers, 2026-07-23) ────────
# These are the ONLY place the on-chain byte layouts live. They are pure and
# synchronous (no self, no I/O), so every event type/orientation can be pinned
# by a unit test against a real captured log — the 2026-07-21 REDEEM layout bug
# (100% of redemptions silently dropped on garbage fields) was undetectable
# precisely because the decode was inline, untested code inside an async
# handler. The handlers now call these and apply policy (watched-wallet
# routing, collateral filter, persistence) around the returned dict.

def decode_order_filled(topics: list, data: str | None) -> dict | None:
    """Decode an OrderFilled (V1 or V2) log into its economic fields.

    Topic layout (3 indexed, shared V1/V2):
      topics[0]=signature  topics[1]=orderHash  topics[2]=maker  topics[3]=taker
    Data words (non-indexed; V1/V2 agree on the first four, V2 appends extras):
      [0] makerAssetId  [1] takerAssetId
      [2] makerAmountFilled  [3] takerAmountFilled  [4] fee (+ V2 extras)

    asset_id 0 == USDC collateral; the non-zero side is the binary-outcome
    conditional token. Direction is inferred from whichever leg is USDC:
      * makerAssetId==0  → the maker paid USDC, i.e. maker is BUYING the token.
      * takerAssetId==0  → the maker received USDC, i.e. maker is SELLING.
    (Empirically the live V2 exchange always encodes the USDC leg on the maker
    side, so a taker who is the watched wallet on a maker-BUY log is SELLING —
    that inversion is the handler's job, not this decoder's.)

    Returns None for a malformed log (fewer than 4 topics / fewer than 5 data
    words) or a token↔token swap with no USDC leg (nothing to price). Amounts
    are USDC/share units scaled by 1e6.
    """
    if len(topics) < 4:
        return None
    data_hex = (data or "0x").lower().replace("0x", "")
    if len(data_hex) < 64 * 5:
        return None
    maker = _addr_from_topic(topics[2])
    taker = _addr_from_topic(topics[3])

    def _u256(i: int) -> int:
        return int(data_hex[i * 64:(i + 1) * 64], 16)

    maker_asset_id = _u256(0)
    taker_asset_id = _u256(1)
    maker_amount = _u256(2)
    taker_amount = _u256(3)

    if maker_asset_id == 0 and taker_asset_id != 0:
        maker_side = "BUY"
        token_id = taker_asset_id
        usdc = maker_amount / 1e6
        shares = taker_amount / 1e6
    elif taker_asset_id == 0 and maker_asset_id != 0:
        maker_side = "SELL"
        token_id = maker_asset_id
        usdc = taker_amount / 1e6
        shares = maker_amount / 1e6
    else:
        return None
    price = usdc / shares if shares > 0 else 0
    return {
        "maker": maker,
        "taker": taker,
        "maker_side": maker_side,
        "token_id": token_id,
        "shares": shares,
        "usdc": usdc,
        "price": price,
        "maker_asset_id": maker_asset_id,
        "taker_asset_id": taker_asset_id,
    }


def decode_ctf_event(etype: str, topics: list, data: str | None) -> dict | None:
    """Decode a CTF PositionSplit / PositionsMerge / PayoutRedemption log.

    CRITICAL: the layouts DIFFER per event (verified on-chain 2026-07-21) —
    this is the exact bug that silently dropped 100% of redemptions:

      split / merge:  topics=[sig, stakeholder, parentCollectionId, conditionId]
                      data  =[collateralToken, partition/indexSets, amount]
      redeem:         topics=[sig, redeemer, collateralToken, parentCollectionId]
                      data  =[conditionId, indexSets, payout]

    i.e. PayoutRedemption INDEXES the collateral token (topics[2]) and carries
    the conditionId in DATA word 0 — the OPPOSITE of split/merge. Decoding a
    redeem with the split layout reads conditionId=parentCollection (≈0x0) and
    collateral=a conditionId fragment, so it never matches the Polymarket
    collateral set and every redeem is rejected. The amount/payout is DATA
    word 2 in USDC units (1e6) for all three.

    Returns None for a malformed log (fewer than 4 topics / fewer than 3 data
    words). The amount>0 check and the Polymarket-collateral membership test
    are policy applied by the caller, not part of the decode.
    """
    if len(topics) < 4:
        return None
    data_hex = (data or "0x").lower().replace("0x", "")
    if len(data_hex) < 64 * 3:
        return None
    wallet = _addr_from_topic(topics[1])
    if etype == "redeem":
        collateral = "0x" + topics[2][-40:].lower()
        cid = "0x" + data_hex[0:64]
    else:
        collateral = "0x" + data_hex[24:64]
        cid = topics[3].lower()
    amount = int(data_hex[128:192], 16) / 1e6
    return {
        "wallet": wallet,
        "event_type": etype,
        "condition_id": cid,
        "collateral": collateral,
        "amount": amount,
    }


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

    def __init__(self, ws_url: str | list[str], watched: set[str]):
        self.ws_urls = [ws_url] if isinstance(ws_url, str) else list(ws_url)
        self.ws_url = self.ws_urls[0]  # legacy attr, kept for callers/tests
        self.watched = {w.lower() for w in watched}
        self.dedup_window = int(os.environ.get("WS_DEDUP_WINDOW_SEC", "10"))
        self._last_poll: dict[str, float] = {}  # wallet → last-trigger ts
        self._fetcher = None
        self._reconnect_requested = False
        # Escalation state: once a provider proves it won't serve the narrow
        # 450-address topic filter, subscribe broad (OrderFilled on the two
        # exchange contracts only) and filter client-side. Broad is free on
        # public endpoints; metered users set WS_ALLOW_BROAD=0.
        self._force_broad = os.environ.get("WS_FORCE_BROAD", "0").lower() in ("1", "true", "yes")
        self._allow_broad = os.environ.get("WS_ALLOW_BROAD", "1").lower() in ("1", "true", "yes")
        self._consec_failures = 0
        self._events_this_conn = 0
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
            "subscribe_errors": 0,
            "silence_reboots": 0,
            "last_event_ts": time.time(),
            "mode": "narrow",
            "endpoint": self.ws_url,
            "started_at": time.time(),
        }

    def _broad_sub_msgs(self) -> list[dict]:
        """Broad-mode subscription: OrderFilled on the two exchange contracts
        with NO wallet topic filter — the handlers already filter maker/taker
        against self.watched client-side. This is the fallback for providers
        that reject or silently ignore the 450-address narrow filter.

        Deliberately does NOT include the broad USDC Transfer filter (all of
        Polygon's USDC flow — that is what burned 300M Alchemy CU in 25 min).
        OrderFilled on the exchanges is all Polymarket trades (~tens/sec),
        which the chain-direct path needs anyway; Transfer only added
        payout/redeem poll triggers, which the REST poller still covers.
        """
        return [
            {
                "jsonrpc": "2.0", "id": 10, "method": "eth_subscribe",
                "params": [
                    "logs",
                    {
                        "address": EXCHANGES,
                        "topics": [ORDER_FILLED_TOPICS],
                    },
                ],
            },
            # newHeads: one small frame per block (~0.5/s) carrying number +
            # timestamp — feeds the block-ts cache so the firehose never has
            # to fetch timestamps over HTTP (the HTTP fetcher got our IP
            # 429-limited at publicnode within hours on 2026-07-20).
            {
                "jsonrpc": "2.0", "id": 11, "method": "eth_subscribe",
                "params": ["newHeads"],
            },
            # CTF split/merge/redeem — topic-only (see _CTF_TOPIC_TYPE note).
            {
                "jsonrpc": "2.0", "id": 12, "method": "eth_subscribe",
                "params": [
                    "logs",
                    {"topics": [[CTF_SPLIT_TOPIC, CTF_MERGE_TOPIC,
                                 CTF_REDEEM_TOPIC]]},
                ],
            },
        ]

    async def _await_sub_confirms(self, ws, expected_ids: list,
                                  timeout: float = 10.0) -> dict:
        """Read frames until every eth_subscribe request id has a response.

        The original code fired the subscribe messages and never read the
        responses, so a provider rejecting the filter (or acking with an
        error) left us 'listening' on nothing — the exact failure mode that
        kept the fast lane dark for a week. Subscription events that arrive
        interleaved are routed to the normal handler.

        Returns {request_id: error_dict} — empty means all confirmed.
        """
        pending = set(expected_ids)
        errors: dict = {}
        deadline = time.time() + timeout
        while pending and time.time() < deadline:
            try:
                raw = await asyncio.wait_for(
                    ws.recv(), timeout=max(0.1, deadline - time.time()))
            except asyncio.TimeoutError:
                break
            try:
                msg = json.loads(raw)
            except (ValueError, TypeError):
                continue
            if msg.get("method") == "eth_subscription":
                await self._handle_log(msg.get("params") or {})
                continue
            mid = msg.get("id")
            if mid in pending:
                pending.discard(mid)
                if "error" in msg:
                    errors[mid] = msg["error"]
                elif not msg.get("result"):
                    errors[mid] = {"message": f"empty result: {msg!r}"[:200]}
        for mid in pending:
            errors[mid] = {"message": "no response before timeout"}
        return errors

    async def _subscribe(self, ws, broad: bool = False) -> dict:
        """Register subscriptions; validate every response.

        Narrow mode filters to watched wallets in the topic arrays —
        critical for free tier survival on metered providers: the provider
        only delivers events matching our wallets (~1–3/sec) instead of the
        full CTFExchange flow (~168/sec). First version of this file used
        the broad filter and burned 300M CU of Alchemy credits in ~25 min.

        Each JSON-RPC log filter supports arrays at topic positions. We
        still need two subscriptions because we can't AND (from=wallet
        AND to=CTFExchange) OR (from=CTFExchange AND to=wallet) in a
        single filter — the AND is across positions, not OR.

        Returns {request_id: error} for rejected/unanswered subscriptions.
        """
        if broad:
            logger.info(
                "[wallet-stream] subscribing BROAD (OrderFilled on both "
                "exchanges, client-side filter over %d wallets)",
                len(self.watched),
            )
            sub_msgs = self._broad_sub_msgs()
            for m in sub_msgs:
                await ws.send(json.dumps(m))
            return await self._await_sub_confirms(ws, [m["id"] for m in sub_msgs])

        watched_padded = [_pad_addr(w) for w in self.watched]
        exchanges_padded = [_pad_addr(a) for a in EXCHANGES]
        collateral = [a.lower() for a in (USDC_E, USDC_NATIVE, COLLATERAL_V2)]
        logger.info(
            "[wallet-stream] subscribing with %d watched addresses in topic filter",
            len(watched_padded),
        )
        sub_msgs = [
            # Wallet → exchange (BUY side: wallet sends collateral)
            {
                "jsonrpc": "2.0", "id": 1, "method": "eth_subscribe",
                "params": [
                    "logs",
                    {
                        "address": collateral,
                        "topics": [TRANSFER_TOPIC, watched_padded, exchanges_padded],
                    },
                ],
            },
            # Exchange → wallet (SELL side or payouts)
            {
                "jsonrpc": "2.0", "id": 2, "method": "eth_subscribe",
                "params": [
                    "logs",
                    {
                        "address": collateral,
                        "topics": [TRANSFER_TOPIC, exchanges_padded, watched_padded],
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
                        "address": EXCHANGES,
                        "topics": [ORDER_FILLED_TOPICS, None, watched_padded],
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
                        "address": EXCHANGES,
                        "topics": [ORDER_FILLED_TOPICS, None, None, watched_padded],
                    },
                ],
            },
        ]
        for m in sub_msgs:
            await ws.send(json.dumps(m))
        return await self._await_sub_confirms(ws, [m["id"] for m in sub_msgs])

    async def _handle_log(self, params: dict) -> None:
        """Route incoming log event by topic0 to the right handler."""
        self._stats["events_seen"] += 1
        try:
            log = params.get("result") or {}
            topics = log.get("topics") or []
            if not topics:
                # newHeads frame: a block header, not a log. Feed the
                # block-ts cache (number/timestamp are hex strings).
                num, ts = log.get("number"), log.get("timestamp")
                if num and ts:
                    cache = getattr(self, "_blk_ts_cache", None)
                    if cache is None:
                        cache = self._blk_ts_cache = {}
                    if len(cache) > 4000:
                        cache.clear()
                    try:
                        cache[num] = int(ts, 16)
                    except (TypeError, ValueError):
                        pass
                return
            topic0 = topics[0].lower()
        except Exception as exc:
            logger.debug("log decode failed: %s", exc)
            return

        if topic0 in _CTF_TOPIC_TYPE:
            await self._handle_ctf_event(log, topics, _CTF_TOPIC_TYPE[topic0])
            return
        if topic0 in (ORDER_FILLED_TOPIC.lower(), ORDER_FILLED_TOPIC_V2.lower()):
            # V1 and V2 share the decode: same indexed (orderHash, maker,
            # taker) and same first four data words; V2 just appends extras.
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

        The byte-level decode lives in the pure ``decode_order_filled`` so it
        can be unit-tested against real captured logs; this handler applies
        the tracked-wallet routing/dispatch policy around it.
        """
        # last_fill_ts feeds the fill-silence rotation clock: an OrderFilled
        # log arrived (topics>=4) even if the specific decode is skipped.
        if len(topics) >= 4:
            self._stats["last_fill_ts"] = time.time()
        # Decode BEFORE the tracked-wallet filter — the firehose persists
        # every fill; only the fast-lane dispatch below is tracked-only.
        decoded = decode_order_filled(topics, log.get("data"))
        if decoded is None:
            return
        maker = decoded["maker"]
        taker = decoded["taker"]
        maker_side = decoded["maker_side"]
        token_id = decoded["token_id"]
        usdc = decoded["usdc"]
        shares = decoded["shares"]
        price = decoded["price"]
        tx_hash = log.get("transactionHash") or ""

        if FIREHOSE_MODE != "off":
            asyncio.create_task(self._persist_firehose(
                log, maker, taker, maker_side, token_id, shares, price))

        if maker in self.watched:
            wallet, role = maker, "maker"
        elif taker in self.watched:
            wallet, role = taker, "taker"
        else:
            return
        self._stats["events_matched"] += 1
        self._stats["order_filled_decoded"] = self._stats.get("order_filled_decoded", 0) + 1
        side = maker_side if role == "maker" else ("SELL" if maker_side == "BUY" else "BUY")

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
                    # P3: real lane tag, threaded through PolledTrade →
                    # WhaleTrade → signal dict → live_trades.source_lane.
                    "source_lane": "chain_direct",
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

    async def _block_timestamp(self, block_hex: str | None) -> int:
        """Block timestamp (cached per block; ~30 lookups/min worst case).
        Falls back to arrival time — Polygon blocks are 2s, and the composite
        fill key tolerates the rare mismatch as one duplicate row."""
        import time as _t
        if not block_hex:
            return int(_t.time())
        cache = getattr(self, "_blk_ts_cache", None)
        if cache is None:
            cache = self._blk_ts_cache = {}
        if block_hex in cache:
            return cache[block_hex]

        def _fetch() -> int | None:
            import json as _j
            import urllib.request
            body = _j.dumps({"jsonrpc": "2.0", "id": 1,
                             "method": "eth_getBlockByNumber",
                             "params": [block_hex, False]}).encode()
            req = urllib.request.Request(
                FIREHOSE_RPC_HTTP, data=body,
                headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=6) as r:
                res = _j.loads(r.read()).get("result") or {}
            ts = int(res.get("timestamp", "0x0"), 16)
            return ts or None

        try:
            ts = await asyncio.to_thread(_fetch)
        except Exception:
            ts = None
        if not ts:
            ts = int(_t.time())
        if len(cache) > 4000:
            cache.clear()
        cache[block_hex] = ts
        return ts

    def _firehose_roster(self) -> set:
        """wallet_profiles roster, cached 10 min (roster mode only)."""
        import time as _t
        now = _t.time()
        if now - getattr(self, "_roster_ts", 0) < 600:
            return getattr(self, "_roster_set", set())
        try:
            from trading_platform.polymarket.db_connection import get_connection
            conn = get_connection()
            try:
                rows = conn.execute("SELECT wallet FROM wallet_profiles").fetchall()
            finally:
                conn.close()
            self._roster_set = {r[0] for r in rows}
        except Exception as exc:
            logger.debug("[firehose] roster load failed: %s", exc)
            self._roster_set = getattr(self, "_roster_set", set())
        self._roster_ts = now
        return self._roster_set

    async def _persist_firehose(self, log: dict, maker: str, taker: str,
                                maker_side: str, token_id: int,
                                shares: float, price: float) -> None:
        """Persist one decoded fill from BOTH wallet perspectives with the
        block timestamp, so rows dedup exactly against data-api-sourced rows
        on the composite fill key. This is the realtime completeness layer —
        per-wallet API polling structurally cannot keep up with HF wallets."""
        try:
            tx = log.get("transactionHash") or ""
            if not tx:
                return
            rows = [(maker, maker_side),
                    (taker, "SELL" if maker_side == "BUY" else "BUY")]
            if FIREHOSE_MODE == "roster":
                roster = self._firehose_roster()
                rows = [r for r in rows if r[0] in roster]
                if not rows:
                    return
            ts = await self._block_timestamp(log.get("blockNumber"))
            meta = self._market_for_token(str(token_id)) or {}

            def _write() -> int:
                from trading_platform.polymarket.wallet_db import WalletDB
                if not hasattr(self, "_firehose_db"):
                    self._firehose_db = WalletDB()
                n = 0
                import time as _t
                for w, s in rows:
                    if self._firehose_db.upsert_trade(
                        wallet=w, proxy_wallet=w, asset=str(token_id),
                        condition_id=meta.get("condition_id"),
                        side=s, size=shares, price=price, timestamp=ts,
                        title=meta.get("question") or "",
                        slug=meta.get("slug") or "",
                        outcome=meta.get("outcome") or "",
                        event_slug="", transaction_hash=tx,
                        synced_at=int(_t.time()),
                    ):
                        n += 1
                return n

            sem = getattr(self, "_persist_sem", None)
            if sem is None:
                # Bound in-flight DB writes so a slow/stuck DB back-pressures
                # instead of spawning unbounded tasks (stream must stay alive).
                sem = self._persist_sem = asyncio.Semaphore(8)
            async with sem:
                try:
                    n = await asyncio.to_thread(_write)
                except Exception:
                    # 2026-07-20: the cached WalletDB pinned ONE pooled
                    # connection; when it died (pool max_lifetime / PG hiccup)
                    # every write failed FOREVER and the DEBUG-level log hid a
                    # 4.5h persist outage. Self-heal: drop the instance, retry
                    # once on a fresh connection.
                    if hasattr(self, "_firehose_db"):
                        try:
                            self._firehose_db._conn.close()
                        except Exception:
                            pass
                        del self._firehose_db
                    n = await asyncio.to_thread(_write)
            self._stats["firehose_rows"] = self._stats.get("firehose_rows", 0) + n
        except Exception as exc:
            self._stats["firehose_errors"] = self._stats.get("firehose_errors", 0) + 1
            if self._stats["firehose_errors"] % 50 == 1:  # loud but not spammy
                logger.warning("[firehose] persist failed (%d total): %s",
                               self._stats["firehose_errors"], str(exc)[:120])

    def _known_condition(self, cid: str) -> bool:
        """Is this condition_id a Polymarket market we know? (10-min cached
        set — the self-verifying filter for the topic-only CTF subscription:
        non-Polymarket CTF deployments fail this and are dropped.)

        2026-07-21: markets alone is NOT enough — redemptions happen on
        RESOLVED markets, which Gamma DELISTS, so a markets-only filter
        silently dropped 100% of PayoutRedemption events (2,484 flowed
        on-chain in one 3-min sample; we persisted zero). Union with
        market_resolutions and conditions already seen in wallet_ctf_events
        (a prior split/merge proves it's Polymarket)."""
        import time as _t
        now = _t.time()
        if now - getattr(self, "_cid_cache_ts", 0) > 600:
            try:
                from trading_platform.polymarket.db_connection import get_connection
                conn = get_connection()
                try:
                    rows = conn.execute("""
                        SELECT condition_id FROM markets WHERE condition_id IS NOT NULL
                        UNION SELECT condition_id FROM market_resolutions
                        UNION SELECT DISTINCT condition_id FROM wallet_ctf_events
                    """).fetchall()
                finally:
                    conn.close()
                self._cid_cache = {r[0].lower() for r in rows if r[0]}
            except Exception as exc:
                logger.debug("[ctf] cid cache load failed: %s", exc)
                self._cid_cache = getattr(self, "_cid_cache", set())
            self._cid_cache_ts = now
        return cid.lower() in getattr(self, "_cid_cache", set())

    async def _handle_ctf_event(self, log: dict, topics: list, etype: str) -> None:
        """Persist SPLIT/MERGE/REDEEM for Polymarket markets.

        Layout (all three events share it): topics = [sig, stakeholder,
        parentCollectionId, conditionId]; data head = [collateralToken,
        offset(partition/indexSets), amount|payout]. Amounts are collateral
        units (USDC 1e6). We store the stakeholder's cash-flow event keyed by
        condition — the missing legs for FIFO on arb/MM wallets.
        """
        try:
            if FIREHOSE_MODE == "off":
                return
            # LAYOUTS DIFFER per event (verified on-chain 2026-07-21) — the
            # redeem-vs-split layout bug that dropped 100% of redemptions lives
            # in decode_ctf_event, now pure and unit-tested against real logs.
            decoded = decode_ctf_event(etype, topics, log.get("data"))
            if decoded is None:
                return
            wallet = decoded["wallet"]
            collateral = decoded["collateral"]
            cid = decoded["condition_id"]
            amount = decoded["amount"]
            # Polymarket filter: collateral must be a Polymarket USDC —
            # intrinsic to the event, so it survives Gamma delisting resolved
            # markets (which silently killed the markets-table cid filter for
            # redeems). cid-known fallback for any exotic-collateral market.
            if collateral not in _CTF_COLLATERAL:
                if not self._known_condition(cid):
                    return
            if amount <= 0:
                return
            tx = log.get("transactionHash") or ""
            ts = await self._block_timestamp(log.get("blockNumber"))

            def _write() -> None:
                from trading_platform.polymarket.db_connection import get_connection
                conn = get_connection()
                try:
                    conn.execute("""
                        CREATE TABLE IF NOT EXISTS wallet_ctf_events (
                            id BIGSERIAL PRIMARY KEY,
                            wallet TEXT NOT NULL,
                            event_type TEXT NOT NULL,
                            condition_id TEXT NOT NULL,
                            amount_usdc DOUBLE PRECISION,
                            transaction_hash TEXT,
                            timestamp BIGINT,
                            synced_at BIGINT
                        )""")
                    conn.execute("""
                        CREATE UNIQUE INDEX IF NOT EXISTS wallet_ctf_events_key
                        ON wallet_ctf_events (transaction_hash, wallet,
                                              event_type, condition_id, amount_usdc)
                    """)
                    import time as _t
                    conn.execute(
                        "INSERT INTO wallet_ctf_events (wallet, event_type, "
                        "condition_id, amount_usdc, transaction_hash, timestamp, "
                        "synced_at) VALUES (?,?,?,?,?,?,?) ON CONFLICT DO NOTHING",
                        (wallet, etype, cid, round(amount, 6), tx, ts, int(_t.time())))
                    conn.commit()
                finally:
                    conn.close()

            sem = getattr(self, "_persist_sem", None)
            if sem is None:
                sem = self._persist_sem = asyncio.Semaphore(8)
            async with sem:
                await asyncio.to_thread(_write)
            self._stats["ctf_events"] = self._stats.get("ctf_events", 0) + 1
        except Exception as exc:
            self._stats["ctf_errors"] = self._stats.get("ctf_errors", 0) + 1
            if self._stats["ctf_errors"] % 50 == 1:
                logger.warning("[ctf] persist failed (%d total): %s",
                               self._stats["ctf_errors"], str(exc)[:120])

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

    def _delivery_proven(self) -> None:
        """First frame on this connection: the endpoint actually delivers.
        Only THIS resets the barren-connection counter — resetting on a mere
        successful subscribe would let drpc (acks everything, delivers
        nothing, drops every ~5 min) evade rotation forever."""
        if self._events_this_conn == 0:
            self._consec_failures = 0
        self._events_this_conn += 1

    def _on_conn_lost(self, url: str) -> bool:
        """Connection ended in error. Returns True if we should rotate.

        A connection that died WITHOUT delivering a single frame is barren;
        3 barren cycles in a row = the endpoint is broken for us regardless
        of how politely it acked the subscriptions (the observed drpc mode:
        subscribe ok → zero events → drop at ~5 min, forever). Escalates to
        broad-mode client-side filtering so delivery becomes continuously
        observable on the next endpoint."""
        self._stats["reconnects"] += 1
        if self._events_this_conn > 0:
            return False
        self._consec_failures += 1
        if self._consec_failures < 3:
            return False
        logger.error(
            "[wallet-stream] %d consecutive BARREN connections on %s "
            "(subscribed fine, delivered nothing) — rotating endpoint%s",
            self._consec_failures, url,
            " and escalating to BROAD" if (self._allow_broad and not self._force_broad) else "",
        )
        if self._allow_broad and not self._force_broad:
            self._force_broad = True
        self._consec_failures = 0
        return True

    async def run(self) -> None:
        """Main reconnect loop with endpoint rotation + delivery watchdogs.

        Failure modes handled (all observed live 2026-06-30 → 07-07, when
        the lane sat dark for a week on wss://polygon.drpc.org):
          * subscribe rejected → retry BROAD on same endpoint, else rotate
          * 3 consecutive barren connections (subscribe ok, zero frames
            delivered, then dropped) → rotate endpoint + escalate broad
          * connected but ZERO frames for SILENCE_REBOOT_SEC on a
            long-lived connection → rotate endpoint + escalate broad
        """
        reconnect_delay = int(os.environ.get("WS_RECONNECT_DELAY", "5"))
        try:
            import websockets
        except ImportError as exc:
            raise RuntimeError("websockets lib required; pip install websockets") from exc

        idx = 0
        while True:
            url = self.ws_urls[idx % len(self.ws_urls)]
            self.ws_url = url
            self._stats["endpoint"] = url
            self._events_this_conn = 0
            mode = "broad" if (self._force_broad and self._allow_broad) else "narrow"
            try:
                logger.info(
                    "[wallet-stream] connecting to %s (watching %d wallets, mode=%s) …",
                    url, len(self.watched), mode,
                )
                async with websockets.connect(
                    url, ping_interval=20, ping_timeout=10,
                    # Default 1MB frame cap killed live_collector when its
                    # snapshot outgrew it (see live_collector 2026-07-06);
                    # same headroom here so a burst of matched logs can't
                    # 1009-loop the chain-direct lane.
                    max_size=16 * 1024 * 1024,
                ) as ws:
                    errors = await self._subscribe(ws, broad=(mode == "broad"))
                    if errors and mode == "narrow" and self._allow_broad:
                        self._stats["subscribe_errors"] += len(errors)
                        logger.error(
                            "[wallet-stream] narrow subscribe REJECTED on %s: %s "
                            "— retrying broad with client-side filtering",
                            url, errors,
                        )
                        mode = "broad"
                        errors = await self._subscribe(ws, broad=True)
                    if errors:
                        self._stats["subscribe_errors"] += len(errors)
                        raise RuntimeError(f"subscribe failed: {errors}")
                    self._stats["mode"] = mode
                    self._reconnect_requested = False
                    # Start the silence clock at subscribe, not process start.
                    self._stats["last_event_ts"] = time.time()
                    self._stats["last_fill_ts"] = time.time()
                    logger.info("[wallet-stream] subscribed (%s) — listening", mode)
                    while True:
                        if self._reconnect_requested:
                            logger.info("[wallet-stream] reconnect requested — closing")
                            await ws.close()
                            break
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=30)
                        except asyncio.TimeoutError:
                            silent_for = time.time() - self._stats["last_event_ts"]
                            # Broad mode: total silence should rotate on the
                            # FILL clock (300s), not the general 900s one —
                            # a hard-throttled endpoint (publicnode 01:25
                            # 07-21: subscribed fine, zero frames ever) would
                            # otherwise burn 15 dead minutes per attempt.
                            if mode == "broad" and (
                                    time.time() - self._stats.get("last_fill_ts", 0)
                                    > FILL_SILENCE_REBOOT_SEC):
                                self._stats["silence_reboots"] += 1
                                logger.error(
                                    "[wallet-stream] no OrderFilled for %.0fs "
                                    "on %s (broad, total-silence path) — "
                                    "rotating endpoint",
                                    time.time() - self._stats.get("last_fill_ts", 0),
                                    url)
                                await ws.close()
                                idx += 1
                                break
                            if silent_for > SILENCE_REBOOT_SEC:
                                self._stats["silence_reboots"] += 1
                                logger.error(
                                    "[wallet-stream] SILENT %.0fs on %s (mode=%s) — "
                                    "rotating endpoint%s", silent_for, url, mode,
                                    " and escalating to BROAD"
                                    if (self._allow_broad and not self._force_broad) else "",
                                )
                                if self._allow_broad and not self._force_broad:
                                    self._force_broad = True
                                await ws.close()
                                idx += 1
                                break
                            continue
                        self._delivery_proven()
                        self._stats["last_event_ts"] = time.time()
                        # FILL-silence check (2026-07-21): in broad mode the
                        # newHeads/CTF subs keep frames flowing forever, so the
                        # TimeoutError silence-reboot NEVER fires even when the
                        # provider silently stops serving the (heavy) OrderFilled
                        # sub — exactly what publicnode did at 16:41 on 07-20,
                        # killing fill ingestion for 8.5h while CTF flowed. All
                        # of Polymarket produces fills every few seconds; zero
                        # OrderFilled for FILL_SILENCE_S on a broad sub = dead
                        # subscription. Rotate.
                        if mode == "broad" and (
                                time.time() - self._stats.get("last_fill_ts", 0)
                                > FILL_SILENCE_REBOOT_SEC):
                            self._stats["silence_reboots"] += 1
                            logger.error(
                                "[wallet-stream] OrderFilled SILENT %.0fs on %s "
                                "while other subs flow — rotating endpoint",
                                time.time() - self._stats.get("last_fill_ts", 0), url)
                            await ws.close()
                            idx += 1
                            break
                        msg = json.loads(raw)
                        if msg.get("method") == "eth_subscription":
                            await self._handle_log(msg.get("params") or {})
            except Exception as exc:
                if self._on_conn_lost(url):
                    idx += 1
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
                now = time.time()
                uptime = int(now - stream._stats["started_at"])
                event_age = int(now - stream._stats.get("last_event_ts", now))
                status = _health_status(stream._stats, now)
                detail = (
                    f"events={stream._stats['events_seen']} "
                    f"matched={stream._stats['events_matched']} "
                    f"polls={stream._stats['polls_triggered']} "
                    f"chain_direct={stream._stats['chain_direct_dispatches']} "
                    f"chain_misses={stream._stats['chain_direct_misses']} "
                    f"reconnects={stream._stats['reconnects']} "
                    f"sub_errors={stream._stats['subscribe_errors']} "
                    f"silence_reboots={stream._stats['silence_reboots']} "
                    f"mode={stream._stats['mode']} "
                    f"last_event_age={event_age}s "
                    f"endpoint={stream._stats['endpoint']} "
                    f"uptime={uptime}s"
                )
                conn.execute(
                    "INSERT INTO service_health (service, status, error_message, checked_at) "
                    "VALUES (?, ?, ?, ?)",
                    ("wallet_stream", status, detail, int(time.time())),
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

    # Accept both legacy POLYGON_WSS_URL and new POLYGON_WS_URL (comma-
    # separated lists supported); public defaults are appended so a single
    # flaky endpoint can't strand the lane.
    ws_urls = _parse_ws_urls()
    logger.info("[wallet-stream] endpoint pool: %s", ws_urls)

    watched = _load_watched_wallets()
    if not watched:
        logger.warning("No watched wallets loaded — nothing to listen for. Exiting.")
        return 3
    logger.info("[wallet-stream] loaded %d watched wallets", len(watched))

    stream = WalletStream(ws_url=ws_urls, watched=watched)

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
