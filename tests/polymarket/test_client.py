"""Tests for PolymarketClient."""
from __future__ import annotations

import json
import pytest
import responses as resp_lib

from trading_platform.polymarket.client import PolymarketClient, PolymarketConfig


GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"


def _config(sleep: float = 0.0) -> PolymarketConfig:
    return PolymarketConfig(request_sleep_sec=sleep)


def _make_market(i: int) -> dict:
    return {"id": str(i), "question": f"Q{i}", "closed": True, "resolved": True}


# ── PolymarketConfig ──────────────────────────────────────────────────────────

class TestPolymarketConfig:
    def test_default_urls(self):
        cfg = PolymarketConfig()
        assert "gamma-api.polymarket.com" in cfg.gamma_base_url
        assert "clob.polymarket.com" in cfg.clob_base_url

    def test_negative_sleep_raises(self):
        with pytest.raises(ValueError):
            PolymarketConfig(request_sleep_sec=-1.0)


# ── get_markets ───────────────────────────────────────────────────────────────

class TestGetMarkets:
    @resp_lib.activate
    def test_returns_list_from_array_response(self):
        markets = [_make_market(1), _make_market(2)]
        resp_lib.add(
            resp_lib.GET,
            f"{GAMMA_BASE}/markets",
            json=markets,
            status=200,
        )
        client = PolymarketClient(_config())
        page, next_offset = client.get_markets(tag_slug="politics", closed=True, limit=100, offset=0)
        assert len(page) == 2
        assert page[0]["id"] == "1"
        assert next_offset is None  # len(page) < limit → last page

    @resp_lib.activate
    def test_next_offset_when_full_page(self):
        markets = [_make_market(i) for i in range(3)]
        resp_lib.add(resp_lib.GET, f"{GAMMA_BASE}/markets", json=markets, status=200)
        client = PolymarketClient(_config())
        page, next_offset = client.get_markets(limit=3, offset=0)
        assert next_offset == 3  # full page → more data expected

    @resp_lib.activate
    def test_handles_dict_response_with_markets_key(self):
        markets = [_make_market(1)]
        resp_lib.add(
            resp_lib.GET,
            f"{GAMMA_BASE}/markets",
            json={"markets": markets},
            status=200,
        )
        client = PolymarketClient(_config())
        page, _ = client.get_markets()
        assert len(page) == 1


class TestGetAllMarkets:
    @resp_lib.activate
    def test_paginates_until_partial_page(self):
        page1 = [_make_market(i) for i in range(3)]
        page2 = [_make_market(i) for i in range(3, 5)]
        resp_lib.add(resp_lib.GET, f"{GAMMA_BASE}/markets", json=page1, status=200)
        resp_lib.add(resp_lib.GET, f"{GAMMA_BASE}/markets", json=page2, status=200)
        client = PolymarketClient(_config())
        # _page_size=3 so page1 is a full page (triggers another fetch) and page2 is partial
        all_m = client.get_all_markets(tag_slug="economics", closed=True, _page_size=3)
        assert len(all_m) == 5

    @resp_lib.activate
    def test_returns_empty_on_first_empty_page(self):
        resp_lib.add(resp_lib.GET, f"{GAMMA_BASE}/markets", json=[], status=200)
        client = PolymarketClient(_config())
        assert client.get_all_markets() == []


class TestGetMarket:
    @resp_lib.activate
    def test_returns_single_market(self):
        resp_lib.add(
            resp_lib.GET,
            f"{GAMMA_BASE}/markets/42",
            json={"id": "42", "question": "Single?"},
            status=200,
        )
        client = PolymarketClient(_config())
        m = client.get_market("42")
        assert m["id"] == "42"


class TestGetPriceHistory:
    @resp_lib.activate
    def test_returns_history_list(self):
        history = [{"t": 1700000000, "p": 0.72}, {"t": 1700003600, "p": 0.75}]
        resp_lib.add(
            resp_lib.GET,
            f"{CLOB_BASE}/prices-history",
            json={"history": history},
            status=200,
        )
        client = PolymarketClient(_config())
        result = client.get_price_history("0xdeadbeef")
        assert result == history

    @resp_lib.activate
    def test_empty_history_returns_empty_list(self):
        resp_lib.add(
            resp_lib.GET,
            f"{CLOB_BASE}/prices-history",
            json={"history": []},
            status=200,
        )
        client = PolymarketClient(_config())
        assert client.get_price_history("0xfoo") == []


class TestGetMarketsOrderParams:
    @resp_lib.activate
    def test_order_and_ascending_sent_as_params(self):
        resp_lib.add(resp_lib.GET, f"{GAMMA_BASE}/markets", json=[], status=200)
        client = PolymarketClient(_config())
        client.get_markets(order="endDate", ascending=False)
        request = resp_lib.calls[0].request
        assert "order=endDate" in request.url
        assert "ascending=false" in request.url

    @resp_lib.activate
    def test_order_params_omitted_when_none(self):
        resp_lib.add(resp_lib.GET, f"{GAMMA_BASE}/markets", json=[], status=200)
        client = PolymarketClient(_config())
        client.get_markets()
        request = resp_lib.calls[0].request
        assert "order=" not in request.url
        assert "ascending=" not in request.url

    @resp_lib.activate
    def test_end_date_min_sent_as_param(self):
        resp_lib.add(resp_lib.GET, f"{GAMMA_BASE}/markets", json=[], status=200)
        client = PolymarketClient(_config())
        client.get_markets(end_date_min="2026-01-01")
        request = resp_lib.calls[0].request
        assert "end_date_min=2026-01-01" in request.url

    @resp_lib.activate
    def test_end_date_min_omitted_when_none(self):
        resp_lib.add(resp_lib.GET, f"{GAMMA_BASE}/markets", json=[], status=200)
        client = PolymarketClient(_config())
        client.get_markets()
        request = resp_lib.calls[0].request
        assert "end_date_min" not in request.url


class TestGetMarketsFlattensNestedLists:
    @resp_lib.activate
    def test_nested_list_response_is_flattened(self):
        """API occasionally returns [[{...}, {...}]] — client should flatten."""
        nested = [[_make_market(1), _make_market(2)], [_make_market(3)]]
        resp_lib.add(resp_lib.GET, f"{GAMMA_BASE}/markets", json=nested, status=200)
        client = PolymarketClient(_config())
        page, _ = client.get_markets()
        assert len(page) == 3
        assert all(isinstance(m, dict) for m in page)

    @resp_lib.activate
    def test_flat_list_response_unchanged(self):
        markets = [_make_market(1), _make_market(2)]
        resp_lib.add(resp_lib.GET, f"{GAMMA_BASE}/markets", json=markets, status=200)
        client = PolymarketClient(_config())
        page, _ = client.get_markets()
        assert len(page) == 2
        assert all(isinstance(m, dict) for m in page)
