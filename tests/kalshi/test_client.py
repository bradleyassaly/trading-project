"""Tests for KalshiClient — all HTTP calls are mocked."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from trading_platform.kalshi.auth import KalshiConfig
from trading_platform.kalshi.client import KalshiClient, _parse_market, _parse_order_status
from trading_platform.kalshi.models import KalshiMarket, KalshiOrderBook, KalshiOrderRequest


# ── Fixtures ──────────────────────────────────────────────────────────────────

DUMMY_PEM = "-----BEGIN RSA PRIVATE KEY-----\ndummy\n-----END RSA PRIVATE KEY-----"


@pytest.fixture()
def config() -> KalshiConfig:
    return KalshiConfig(api_key_id="test-id", private_key_pem=DUMMY_PEM, demo=True)


@pytest.fixture()
def client(config: KalshiConfig) -> KalshiClient:
    return KalshiClient(config)


# ── Parser unit tests (no HTTP) ───────────────────────────────────────────────

def test_parse_market_dollar_prices():
    raw = {
        "ticker": "FOO-BAR",
        "title": "Will X happen?",
        "status": "open",
        "yes_bid_dollars": "0.6200",
        "yes_ask_dollars": "0.6500",
        "volume": 1234,
    }
    market = _parse_market(raw)
    assert market.ticker == "FOO-BAR"
    assert market.yes_bid == "0.6200"
    assert market.yes_ask == "0.6500"
    assert market.volume == 1234


def test_parse_market_fallback_to_cent_prices():
    raw = {"ticker": "OLD", "title": "Old market", "status": "closed", "yes_bid": "62", "yes_ask": "65"}
    market = _parse_market(raw)
    assert market.yes_bid == "62"


def test_parse_order_status():
    raw = {
        "order_id": "ord-123",
        "ticker": "FOO",
        "side": "yes",
        "action": "buy",
        "status": "resting",
        "type": "limit",
        "yes_price_dollars": "0.5500",
        "count": 10,
        "remaining_count": 10,
        "amend_count": 0,
    }
    status = _parse_order_status(raw)
    assert status.order_id == "ord-123"
    assert status.yes_price == "0.5500"
    assert status.remaining_count == 10


# ── KalshiClient integration tests (mocked HTTP) ─────────────────────────────

def _make_mock_response(json_data: dict) -> MagicMock:
    resp = MagicMock()
    resp.json.return_value = json_data
    resp.raise_for_status.return_value = None
    return resp


def test_get_market(client: KalshiClient):
    payload = {"market": {"ticker": "TEST-MKT", "title": "Test", "status": "open"}}
    with patch.object(client._session, "get", return_value=_make_mock_response(payload)), \
         patch("trading_platform.kalshi.client.build_auth_headers", return_value={}), \
         patch("trading_platform.kalshi.client.time") as mock_time:
        mock_time.sleep.return_value = None
        market = client.get_market("TEST-MKT")

    assert market.ticker == "TEST-MKT"
    assert market.status == "open"


def test_get_orderbook(client: KalshiClient):
    payload = {
        "orderbook": {
            "yes_dollars": [["0.6500", 50], ["0.6400", 100]],
            "no_dollars": [["0.3500", 30]],
        }
    }
    with patch.object(client._session, "get", return_value=_make_mock_response(payload)), \
         patch("trading_platform.kalshi.client.build_auth_headers", return_value={}), \
         patch("trading_platform.kalshi.client.time") as mock_time:
        mock_time.sleep.return_value = None
        ob = client.get_orderbook("TEST-MKT", depth=5)

    assert isinstance(ob, KalshiOrderBook)
    assert ob.best_yes_bid == pytest.approx(0.65)
    assert len(ob.yes_bids) == 2
    assert ob.mid_price is not None


def test_get_all_markets_pagination(client: KalshiClient):
    page1 = {"markets": [{"ticker": "A", "title": "A", "status": "open"}], "cursor": "next"}
    page2 = {"markets": [{"ticker": "B", "title": "B", "status": "open"}], "cursor": None}

    responses = [_make_mock_response(page1), _make_mock_response(page2)]
    with patch.object(client._session, "get", side_effect=responses), \
         patch("trading_platform.kalshi.client.build_auth_headers", return_value={}), \
         patch("trading_platform.kalshi.client.time") as mock_time:
        mock_time.sleep.return_value = None
        markets = client.get_all_markets(status="open")

    assert len(markets) == 2
    assert {m.ticker for m in markets} == {"A", "B"}


def test_create_order(client: KalshiClient):
    payload = {
        "order": {
            "order_id": "ord-999",
            "ticker": "SOME-MKT",
            "side": "yes",
            "action": "buy",
            "status": "resting",
            "type": "limit",
            "count": 5,
            "remaining_count": 5,
            "amend_count": 0,
        }
    }
    req = KalshiOrderRequest(ticker="SOME-MKT", side="yes", action="buy", count=5, yes_price="0.6000")
    with patch.object(client._session, "post", return_value=_make_mock_response(payload)), \
         patch("trading_platform.kalshi.client.build_auth_headers", return_value={}), \
         patch("trading_platform.kalshi.client.time") as mock_time:
        mock_time.sleep.return_value = None
        status = client.create_order(req)

    assert status.order_id == "ord-999"
    assert status.status == "resting"


def test_cancel_order(client: KalshiClient):
    payload = {"order": {"order_id": "ord-999", "ticker": "X", "side": "yes", "action": "buy",
                         "status": "canceled", "type": "limit", "count": 5, "remaining_count": 5, "amend_count": 0}}
    with patch.object(client._session, "delete", return_value=_make_mock_response(payload)), \
         patch("trading_platform.kalshi.client.build_auth_headers", return_value={}), \
         patch("trading_platform.kalshi.client.time") as mock_time:
        mock_time.sleep.return_value = None
        status = client.cancel_order("ord-999")

    assert status.status == "canceled"
