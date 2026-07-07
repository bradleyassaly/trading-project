"""P3.0: the fast lane sat dark for a week (2026-06-30 → 07-07) because
eth_subscribe responses were never validated, the flaky endpoint was never
rotated, and the heartbeat wrote 'ok' unconditionally. These tests pin the
restored behavior: subscribe confirmation, broad fallback shape, endpoint
pool parsing, silence-aware health status, and the paper staleness-gate key.
"""
import asyncio
import inspect
import json

from trading_platform.polymarket import wallet_stream as wst


class FakeWS:
    """Scripted websocket: .send captures frames, .recv replays responses
    then hangs (so _await_sub_confirms hits its timeout branch)."""

    def __init__(self, responses=None):
        self.sent: list[dict] = []
        self._in = list(responses or [])

    async def send(self, raw):
        self.sent.append(json.loads(raw))

    async def recv(self):
        if not self._in:
            await asyncio.sleep(3600)
        return json.dumps(self._in.pop(0))


def _stream(watched=("0x" + "a" * 40,)):
    return wst.WalletStream(ws_url="wss://test", watched=set(watched))


# ---------------------------------------------------------------------------
# Endpoint pool
# ---------------------------------------------------------------------------

def test_parse_ws_urls_env_priority(monkeypatch):
    monkeypatch.setenv("POLYGON_WS_URL", "wss://primary, wss://secondary")
    monkeypatch.delenv("POLYGON_WSS_URL", raising=False)
    urls = wst._parse_ws_urls()
    assert urls[0] == "wss://primary" and urls[1] == "wss://secondary"
    # public defaults appended for failover
    for d in wst.DEFAULT_WS_URLS:
        assert d in urls


def test_parse_ws_urls_defaults_when_unset(monkeypatch):
    monkeypatch.delenv("POLYGON_WS_URL", raising=False)
    monkeypatch.delenv("POLYGON_WSS_URL", raising=False)
    assert wst._parse_ws_urls() == wst.DEFAULT_WS_URLS


# ---------------------------------------------------------------------------
# Health status (the honest heartbeat)
# ---------------------------------------------------------------------------

def test_health_status_ok_when_events_flow():
    now = 1_783_000_000.0
    stats = {"subscribe_errors": 0, "events_seen": 50,
             "last_event_ts": now - 30, "started_at": now - 3600}
    assert wst._health_status(stats, now) == "ok"


def test_health_status_degraded_on_silence():
    now = 1_783_000_000.0
    stats = {"subscribe_errors": 0, "events_seen": 50,
             "last_event_ts": now - 2000, "started_at": now - 86400}
    assert wst._health_status(stats, now, silence_sec=900) == "degraded"


def test_health_status_degraded_on_rejected_subscribe():
    # The exact week-long failure: subscriptions never confirmed, zero events,
    # old heartbeat said 'ok'. Now: degraded.
    now = 1_783_000_000.0
    stats = {"subscribe_errors": 4, "events_seen": 0,
             "last_event_ts": now - 10, "started_at": now - 100}
    assert wst._health_status(stats, now) == "degraded"


# ---------------------------------------------------------------------------
# Subscribe confirmation reading
# ---------------------------------------------------------------------------

def test_await_sub_confirms_all_ok():
    s = _stream()
    ws = FakeWS(responses=[
        {"jsonrpc": "2.0", "id": 1, "result": "0xsub1"},
        {"jsonrpc": "2.0", "id": 2, "result": "0xsub2"},
    ])
    errors = asyncio.run(s._await_sub_confirms(ws, [1, 2], timeout=1.0))
    assert errors == {}


def test_await_sub_confirms_captures_error():
    s = _stream()
    ws = FakeWS(responses=[
        {"jsonrpc": "2.0", "id": 1, "result": "0xsub1"},
        {"jsonrpc": "2.0", "id": 2,
         "error": {"code": -32602, "message": "filter too large"}},
    ])
    errors = asyncio.run(s._await_sub_confirms(ws, [1, 2], timeout=1.0))
    assert 2 in errors and "filter too large" in errors[2]["message"]


def test_await_sub_confirms_timeout_is_an_error():
    # Provider never answers (the drpc failure mode): every unanswered id
    # must come back as an error, not silently pass.
    s = _stream()
    ws = FakeWS(responses=[{"jsonrpc": "2.0", "id": 1, "result": "0xsub1"}])
    errors = asyncio.run(s._await_sub_confirms(ws, [1, 2], timeout=0.3))
    assert 1 not in errors and 2 in errors


def test_await_sub_confirms_routes_interleaved_events():
    # A subscription event arriving between confirmations must be handled,
    # not dropped or mistaken for a confirmation.
    s = _stream()
    ws = FakeWS(responses=[
        {"jsonrpc": "2.0", "method": "eth_subscription",
         "params": {"result": {"topics": []}}},
        {"jsonrpc": "2.0", "id": 1, "result": "0xsub1"},
    ])
    errors = asyncio.run(s._await_sub_confirms(ws, [1], timeout=1.0))
    assert errors == {}
    assert s._stats["events_seen"] == 1


# ---------------------------------------------------------------------------
# Subscription message shapes
# ---------------------------------------------------------------------------

def test_narrow_subscribe_includes_watched_topics():
    s = _stream()
    ws = FakeWS(responses=[{"id": i, "result": f"0x{i}"} for i in (1, 2, 3, 4)])
    errors = asyncio.run(s._subscribe(ws, broad=False))
    assert errors == {}
    assert len(ws.sent) == 4
    padded = wst._pad_addr(next(iter(s.watched)))
    # every narrow filter carries the watched set at some topic position
    assert all(
        any(isinstance(t, list) and padded in t
            for t in m["params"][1]["topics"])
        for m in ws.sent
    )


def test_broad_subscribe_has_no_wallet_filter():
    s = _stream()
    ws = FakeWS(responses=[{"id": 10, "result": "0xsub"}])
    errors = asyncio.run(s._subscribe(ws, broad=True))
    assert errors == {}
    assert len(ws.sent) == 1
    params = ws.sent[0]["params"][1]
    # both event generations OR'd at topic position 0
    assert params["topics"] == [[wst.ORDER_FILLED_TOPIC, wst.ORDER_FILLED_TOPIC_V2]]
    # V1 + V2 exchanges, all LOWERCASE (public bor endpoints match address
    # filters case-sensitively — checksummed addresses match nothing)
    assert set(params["address"]) == set(wst.EXCHANGES)
    assert all(a == a.lower() for a in params["address"])
    # broad mode must NOT subscribe the raw collateral Transfer firehose
    assert wst.USDC_E not in params["address"]


def test_narrow_covers_v2_exchange_and_collateral():
    s = _stream()
    ws = FakeWS(responses=[{"id": i, "result": f"0x{i}"} for i in (1, 2, 3, 4)])
    asyncio.run(s._subscribe(ws, broad=False))
    of_msgs = [m for m in ws.sent
               if m["params"][1]["topics"][0] != wst.TRANSFER_TOPIC]
    for m in of_msgs:
        p = m["params"][1]
        assert wst.CTF_EXCHANGE_V2 in p["address"]
        assert wst.ORDER_FILLED_TOPIC_V2 in p["topics"][0]
    tr_msgs = [m for m in ws.sent
               if m["params"][1]["topics"][0] == wst.TRANSFER_TOPIC]
    for m in tr_msgs:
        assert wst.COLLATERAL_V2 in m["params"][1]["address"]


def test_broad_events_still_filtered_client_side():
    # In broad mode unmatched maker/taker events must be dropped by the
    # handler (watched-set check), not dispatched.
    s = _stream(watched=("0x" + "a" * 40,))
    unwatched = wst._pad_addr("0x" + "b" * 40)
    log = {"topics": [wst.ORDER_FILLED_TOPIC, "0x" + "0" * 64, unwatched, unwatched],
           "data": "0x" + "0" * 320}
    asyncio.run(s._handle_order_filled(log, log["topics"]))
    assert s._stats["events_matched"] == 0


def test_v2_order_filled_decodes(monkeypatch):
    # V2 event: 4 topics (sig, orderHash, maker, taker), 7 data words with
    # the V1 meaning in the first four (makerAssetId, takerAssetId,
    # makerAmount, takerAmount). Verified live 2026-07-07 against a known
    # fill. The router must accept topic0=V2 and the decoder must match the
    # watched taker.
    monkeypatch.setenv("CHAIN_DIRECT_DISPATCH", "0")
    wallet = "0x" + "a" * 40
    s = _stream(watched=(wallet,))
    s._last_poll[wallet] = 10**12  # suppress the hydration poll (network)
    token_id = 123456789
    def w(v): return hex(v)[2:].rjust(64, "0")
    # maker gives 50100 collateral units, taker gives 5010000 token units
    data = "0x" + w(0) + w(token_id) + w(50100) + w(5010000) + w(0) + w(0) + w(0)
    log = {"topics": [wst.ORDER_FILLED_TOPIC_V2, "0x" + "0" * 64,
                      wst._pad_addr("0x" + "c" * 40), wst._pad_addr(wallet)],
           "data": data, "transactionHash": "0xdead"}
    asyncio.run(s._handle_log({"result": log}))
    assert s._stats["events_matched"] == 1
    assert s._stats["order_filled_decoded"] == 1


# ---------------------------------------------------------------------------
# Barren-connection rotation (the exact drpc failure pattern)
# ---------------------------------------------------------------------------

def test_three_barren_connections_rotate_and_escalate(monkeypatch):
    # drpc pattern: subscribe acked, ZERO frames delivered, dropped at ~5min.
    # A naive consecutive-failure counter that resets on successful subscribe
    # never rotates. Barren counting must rotate on the 3rd dead cycle and
    # escalate to broad so delivery becomes observable.
    s = _stream()
    assert s._on_conn_lost("wss://x") is False   # barren #1
    assert s._on_conn_lost("wss://x") is False   # barren #2
    assert s._on_conn_lost("wss://x") is True    # barren #3 → rotate
    assert s._force_broad is True
    assert s._consec_failures == 0               # reset after rotation


def test_delivered_connection_does_not_count_barren():
    s = _stream()
    s._on_conn_lost("wss://x")
    s._on_conn_lost("wss://x")
    # a frame arrives on the next connection → endpoint proven healthy
    s._events_this_conn = 0
    s._delivery_proven()
    assert s._consec_failures == 0
    # its later disconnect is a normal blip, not barren
    assert s._on_conn_lost("wss://x") is False
    assert s._consec_failures == 0
    assert s._force_broad is False


def test_barren_rotation_respects_allow_broad(monkeypatch):
    monkeypatch.setenv("WS_ALLOW_BROAD", "0")
    s = _stream()
    for _ in range(2):
        s._on_conn_lost("wss://x")
    assert s._on_conn_lost("wss://x") is True    # still rotates
    assert s._force_broad is False               # but never goes broad (metered)


# ---------------------------------------------------------------------------
# Staleness-gate key regression (paper executor)
# ---------------------------------------------------------------------------

def test_paper_executor_passes_whale_trade_ts():
    # Signal dicts carry "whale_trade_ts" (whale_signal_engine._fire_signal);
    # the executor used to read "timestamp"/"trade_ts" — keys that never
    # existed — so check_price_staleness never ran.
    from trading_platform.polymarket import polymarket_paper_executor as ppe
    src = inspect.getsource(ppe)
    assert 'whale_trade_ts=signal.get("whale_trade_ts")' in src
