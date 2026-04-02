"""Tests for KalshiClient — all HTTP calls are mocked."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
import requests

from trading_platform.kalshi.auth import KalshiConfig
from trading_platform.kalshi.client import KalshiClient, KalshiGetRetryPolicy, _parse_market, _parse_order_status
from trading_platform.kalshi.models import KalshiOrderBook, KalshiOrderRequest


# ── Fixtures ──────────────────────────────────────────────────────────────────

DUMMY_PEM = "-----BEGIN RSA PRIVATE KEY-----\ndummy\n-----END RSA PRIVATE KEY-----"


@pytest.fixture()
def config() -> KalshiConfig:
    return KalshiConfig(api_key_id="test-id", private_key_pem=DUMMY_PEM, demo=True)


@pytest.fixture()
def client(config: KalshiConfig) -> KalshiClient:
    return KalshiClient(config)


@pytest.fixture()
def live_retry_policy() -> KalshiGetRetryPolicy:
    return KalshiGetRetryPolicy(
        max_retries=3,
        backoff_base_sec=0.5,
        backoff_max_sec=8.0,
        jitter_max_sec=0.25,
    )


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
    resp.status_code = 200
    resp.headers = {}
    resp.json.return_value = json_data
    resp.raise_for_status.return_value = None
    return resp


def _make_http_error_response(status_code: int, *, headers: dict[str, str] | None = None) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.headers = headers or {}
    resp.json.return_value = {}
    resp.raise_for_status.side_effect = requests.HTTPError(f"{status_code} error", response=resp)
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


def test_authenticated_get_immediate_success_returns_payload(config: KalshiConfig, live_retry_policy: KalshiGetRetryPolicy):
    client = KalshiClient(
        config,
        authenticated_sleep_sec=0.0,
        authenticated_retry_policy=live_retry_policy,
    )
    payload = {"markets": [{"ticker": "A"}], "cursor": None}
    with patch.object(client._session, "get", return_value=_make_mock_response(payload)) as mock_get, \
         patch("trading_platform.kalshi.client.build_auth_headers", return_value={"Auth": "x"}), \
         patch("trading_platform.kalshi.client.time.sleep") as mock_sleep:
        markets, cursor = client.get_markets_raw(status="settled", limit=5)

    assert markets == [{"ticker": "A"}]
    assert cursor is None
    assert mock_get.call_count == 1
    mock_sleep.assert_called_once_with(0.0)


def test_authenticated_get_retries_429_then_succeeds_with_retry_after_header(
    config: KalshiConfig,
    live_retry_policy: KalshiGetRetryPolicy,
):
    client = KalshiClient(
        config,
        authenticated_sleep_sec=0.0,
        authenticated_retry_policy=live_retry_policy,
    )
    retry_response = _make_http_error_response(429, headers={"Retry-After": "1.5"})
    success_response = _make_mock_response({"markets": [{"ticker": "A"}], "cursor": None})
    sleep_calls: list[float] = []

    with patch.object(client._session, "get", side_effect=[retry_response, success_response]) as mock_get, \
         patch("trading_platform.kalshi.client.build_auth_headers", return_value={"Auth": "x"}), \
         patch("trading_platform.kalshi.client.time.sleep", side_effect=lambda seconds: sleep_calls.append(seconds)), \
         patch("trading_platform.kalshi.client.random.uniform", return_value=0.0):
        result = client.get_markets_raw(limit=5)

    assert result == ([{"ticker": "A"}], None)
    assert mock_get.call_count == 2
    assert sleep_calls == [0.0, 1.5]


def test_authenticated_get_retries_multiple_429s_then_succeeds(
    config: KalshiConfig,
    live_retry_policy: KalshiGetRetryPolicy,
):
    client = KalshiClient(
        config,
        authenticated_sleep_sec=0.0,
        authenticated_retry_policy=live_retry_policy,
    )
    retry_response = _make_http_error_response(429)
    success_response = _make_mock_response({"markets": [{"ticker": "A"}], "cursor": None})
    sleep_calls: list[float] = []

    with patch.object(client._session, "get", side_effect=[retry_response, retry_response, success_response]) as mock_get, \
         patch("trading_platform.kalshi.client.build_auth_headers", return_value={"Auth": "x"}), \
         patch("trading_platform.kalshi.client.time.sleep", side_effect=lambda seconds: sleep_calls.append(seconds)), \
         patch("trading_platform.kalshi.client.random.uniform", side_effect=[0.1, 0.2]):
        result = client.get_markets_raw(limit=5)

    assert result == ([{"ticker": "A"}], None)
    assert mock_get.call_count == 3
    assert sleep_calls == [0.0, 0.6, 1.2]


def test_authenticated_get_repeated_429_raises_after_max_retries(
    config: KalshiConfig,
    live_retry_policy: KalshiGetRetryPolicy,
):
    client = KalshiClient(
        config,
        authenticated_sleep_sec=0.0,
        authenticated_retry_policy=KalshiGetRetryPolicy(
            max_retries=2,
            backoff_base_sec=0.5,
            backoff_max_sec=8.0,
            jitter_max_sec=0.25,
        ),
    )
    retry_response = _make_http_error_response(429)
    sleep_calls: list[float] = []

    with patch.object(client._session, "get", side_effect=[retry_response] * 3) as mock_get, \
         patch("trading_platform.kalshi.client.build_auth_headers", return_value={"Auth": "x"}), \
         patch("trading_platform.kalshi.client.time.sleep", side_effect=lambda seconds: sleep_calls.append(seconds)), \
         patch("trading_platform.kalshi.client.random.uniform", return_value=0.0):
        with pytest.raises(requests.HTTPError, match="429 error") as exc_info:
            client.get_markets_raw(limit=5)

    assert mock_get.call_count == 3
    assert sleep_calls == [0.0, 0.5, 1.0]
    if hasattr(exc_info.value, "__notes__"):
        assert any("after 2 retries" in note for note in exc_info.value.__notes__)


def test_authenticated_rate_limit_logging_is_clearly_live(
    config: KalshiConfig,
    live_retry_policy: KalshiGetRetryPolicy,
    caplog: pytest.LogCaptureFixture,
):
    client = KalshiClient(
        config,
        authenticated_sleep_sec=0.0,
        authenticated_retry_policy=live_retry_policy,
    )
    retry_response = _make_http_error_response(429)
    success_response = _make_mock_response({"markets": [{"ticker": "A"}], "cursor": None})

    with patch.object(client._session, "get", side_effect=[retry_response, success_response]), \
         patch("trading_platform.kalshi.client.build_auth_headers", return_value={"Auth": "x"}), \
         patch("trading_platform.kalshi.client.time.sleep", return_value=None), \
         patch("trading_platform.kalshi.client.random.uniform", return_value=0.0), \
         caplog.at_level("WARNING", logger="trading_platform.kalshi.client"):
        client.get_markets_raw(limit=5)

    assert "Kalshi live/authenticated GET rate limited" in caplog.text


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


def test_get_public_uses_client_historical_sleep_by_default(config: KalshiConfig):
    client = KalshiClient(config, historical_sleep_sec=0.05)
    payload = {"markets": [], "cursor": None}
    with patch.object(client._session, "get", return_value=_make_mock_response(payload)), \
         patch("trading_platform.kalshi.client.time") as mock_time:
        mock_time.sleep.return_value = None
        client.get_historical_markets()

    mock_time.sleep.assert_called_once_with(0.05)


def test_get_all_historical_methods_forward_limit_and_sleep(client: KalshiClient):
    with patch.object(
        client,
        "get_historical_markets",
        side_effect=[([{"ticker": "A"}], "next"), ([{"ticker": "B"}], None)],
    ) as mock_markets:
        markets = client.get_all_historical_markets(limit=100, sleep=0.05)

    assert markets == [{"ticker": "A"}, {"ticker": "B"}]
    assert mock_markets.call_args_list[0].kwargs["limit"] == 100
    assert mock_markets.call_args_list[0].kwargs["sleep"] == 0.05
    assert mock_markets.call_args_list[1].kwargs["limit"] == 100
    assert mock_markets.call_args_list[1].kwargs["sleep"] == 0.05

    with patch.object(
        client,
        "get_historical_trades",
        side_effect=[([{"trade_id": "t1"}], "next"), ([{"trade_id": "t2"}], None)],
    ) as mock_trades:
        trades = client.get_all_historical_trades("TEST", limit=250, sleep=0.05)

    assert trades == [{"trade_id": "t1"}, {"trade_id": "t2"}]
    assert mock_trades.call_args_list[0].kwargs["limit"] == 250
    assert mock_trades.call_args_list[0].kwargs["sleep"] == 0.05
    assert mock_trades.call_args_list[1].kwargs["limit"] == 250
    assert mock_trades.call_args_list[1].kwargs["sleep"] == 0.05


def test_get_public_immediate_success_returns_payload(config: KalshiConfig):
    client = KalshiClient(config, historical_sleep_sec=0.0)
    payload = {"markets": [{"ticker": "A"}], "cursor": None}
    with patch.object(client._session, "get", return_value=_make_mock_response(payload)) as mock_get, \
         patch("trading_platform.kalshi.client.time.sleep") as mock_sleep:
        result = client.get_historical_markets(limit=5, sleep=0.0)

    assert result == ([{"ticker": "A"}], None)
    assert mock_get.call_count == 1
    mock_sleep.assert_called_once_with(0.0)


def test_get_public_retries_429_then_succeeds_with_retry_after_header(config: KalshiConfig):
    client = KalshiClient(config, historical_sleep_sec=0.0)
    retry_response = _make_http_error_response(429, headers={"Retry-After": "1.5"})
    success_response = _make_mock_response({"markets": [{"ticker": "A"}], "cursor": None})
    sleep_calls: list[float] = []

    with patch.object(client._session, "get", side_effect=[retry_response, success_response]) as mock_get, \
         patch("trading_platform.kalshi.client.time.sleep", side_effect=lambda seconds: sleep_calls.append(seconds)), \
         patch("trading_platform.kalshi.client.random.uniform", return_value=0.0):
        result = client.get_historical_markets(limit=5, sleep=0.0)

    assert result == ([{"ticker": "A"}], None)
    assert mock_get.call_count == 2
    assert sleep_calls == [0.0, 1.5]


def test_get_public_retries_429_with_exponential_backoff_and_jitter(config: KalshiConfig):
    client = KalshiClient(config, historical_sleep_sec=0.0)
    retry_response = _make_http_error_response(429)
    success_response = _make_mock_response({"markets": [{"ticker": "A"}], "cursor": None})
    sleep_calls: list[float] = []

    with patch.object(client._session, "get", side_effect=[retry_response, retry_response, success_response]) as mock_get, \
         patch("trading_platform.kalshi.client.time.sleep", side_effect=lambda seconds: sleep_calls.append(seconds)), \
         patch("trading_platform.kalshi.client.random.uniform", side_effect=[0.1, 0.2]):
        result = client.get_historical_markets(limit=5, sleep=0.0)

    assert result == ([{"ticker": "A"}], None)
    assert mock_get.call_count == 3
    assert sleep_calls == [0.0, 0.6, 1.2]


def test_get_public_repeated_429_raises_after_max_retries(config: KalshiConfig):
    client = KalshiClient(config, historical_sleep_sec=0.0)
    retry_response = _make_http_error_response(429)
    sleep_calls: list[float] = []

    with patch.object(client._session, "get", side_effect=[retry_response] * 6) as mock_get, \
         patch("trading_platform.kalshi.client.time.sleep", side_effect=lambda seconds: sleep_calls.append(seconds)), \
         patch("trading_platform.kalshi.client.random.uniform", return_value=0.0):
        with pytest.raises(requests.HTTPError, match="429 error") as exc_info:
            client.get_historical_markets(limit=5, sleep=0.0)

    assert mock_get.call_count == 6
    assert sleep_calls == [0.0, 0.5, 1.0, 2.0, 4.0, 8.0]
    if hasattr(exc_info.value, "__notes__"):
        assert any("after 5 retries" in note for note in exc_info.value.__notes__)


def test_get_historical_markets_filters_close_time_client_side(config: KalshiConfig):
    client = KalshiClient(config, historical_sleep_sec=0.0)
    now = datetime.now(UTC)
    payload = {
        "markets": [
            {"ticker": "IN", "close_time": now.isoformat()},
            {"ticker": "OUT", "close_time": (now - timedelta(days=90)).isoformat()},
        ],
        "cursor": "next",
    }

    with patch.object(client._session, "get", return_value=_make_mock_response(payload)) as mock_get, \
         patch("trading_platform.kalshi.client.time.sleep", return_value=None):
        markets, cursor = client.get_historical_markets(
            limit=5,
            sleep=0.0,
            min_close_ts=int((now - timedelta(days=30)).timestamp()),
            max_close_ts=int(now.timestamp()),
        )

    assert [market["ticker"] for market in markets] == ["IN"]
    assert cursor == "next"
    request_url = mock_get.call_args.args[0]
    assert "min_close_ts" not in request_url
    assert "max_close_ts" not in request_url


def test_get_historical_cutoff(config: KalshiConfig):
    client = KalshiClient(config, historical_sleep_sec=0.0)
    payload = {"market_settled_ts": "2026-01-01T00:00:00Z", "trades_created_ts": "2026-01-01T00:00:00Z"}

    with patch.object(client._session, "get", return_value=_make_mock_response(payload)), \
         patch("trading_platform.kalshi.client.time.sleep", return_value=None):
        result = client.get_historical_cutoff()

    assert result["market_settled_ts"] == "2026-01-01T00:00:00Z"


def test_get_markets_raw_and_candlesticks_raw(config: KalshiConfig):
    client = KalshiClient(config)
    markets_payload = {"markets": [{"ticker": "X", "result": "yes"}], "cursor": None}
    candles_payload = {"candlesticks": [{"close_price_dollars": 0.55}]}

    with patch.object(client, "_get", side_effect=[markets_payload, candles_payload]), \
         patch("trading_platform.kalshi.client.build_auth_headers", return_value={}):
        markets, cursor = client.get_markets_raw(status="settled", limit=200)
        candles = client.get_market_candlesticks_raw("X", start_ts=1, end_ts=2, period_interval=60)

    assert markets == [{"ticker": "X", "result": "yes"}]
    assert cursor is None
    assert candles == [{"close_price_dollars": 0.55}]
