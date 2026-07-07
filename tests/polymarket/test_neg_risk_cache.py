"""P4: neg_risk read-through cache + order-book threading.

The hot lane paid 4 CLOB REST round-trips per live entry (/midpoint, /book
x2, /neg-risk) and 2 redundant per exit. neg_risk is immutable per market
(it selects the exchange contract orders are signed against), so it caches
forever; the depth-check book threads into the order call with a 10s
freshness stamp.
"""
import inspect
import os
import sqlite3
import tempfile

import pytest

from trading_platform.polymarket import markets_table as mt


@pytest.fixture()
def tmp_markets_db(monkeypatch):
    fd, path = tempfile.mkstemp(suffix=".db"); os.close(fd)
    monkeypatch.setenv("DB_BACKEND", "sqlite")
    from trading_platform.polymarket import db_connection as dbc
    monkeypatch.setattr(dbc, "DB_BACKEND", "sqlite")
    monkeypatch.setattr(dbc, "DEFAULT_DB_PATH", path)
    mt.ensure_schema()
    yield path
    os.unlink(path)


def _seed_market(path, cid="c1", yes="tok_yes", no="tok_no", neg_risk=None):
    c = sqlite3.connect(path)
    c.execute(
        "INSERT INTO markets (condition_id, yes_token_id, no_token_id, "
        "neg_risk, last_fetched_at) VALUES (?,?,?,?,1)",
        (cid, yes, no, neg_risk))
    c.commit()
    c.close()


class _NoHTTP:
    def get(self, *a, **k):
        raise AssertionError("HTTP call made on a cache hit")


def test_cache_hit_no_http(tmp_markets_db, monkeypatch):
    _seed_market(tmp_markets_db, neg_risk=1)
    import requests
    monkeypatch.setattr(requests, "get", _NoHTTP().get)
    assert mt.get_neg_risk_cached("tok_yes") is True
    assert mt.get_neg_risk_cached("tok_no") is True


def test_cache_hit_false_value(tmp_markets_db, monkeypatch):
    _seed_market(tmp_markets_db, neg_risk=0)
    import requests
    monkeypatch.setattr(requests, "get", _NoHTTP().get)
    assert mt.get_neg_risk_cached("tok_yes") is False


def test_null_miss_fetches_and_writes_back(tmp_markets_db, monkeypatch):
    _seed_market(tmp_markets_db, neg_risk=None)
    calls = []

    class _R:
        status_code = 200
        def json(self): return {"neg_risk": True}

    import requests
    monkeypatch.setattr(requests, "get", lambda *a, **k: (calls.append(a), _R())[1])
    assert mt.get_neg_risk_cached("tok_yes") is True
    assert len(calls) == 1
    # write-back: second call is a pure cache hit
    monkeypatch.setattr(requests, "get", _NoHTTP().get)
    assert mt.get_neg_risk_cached("tok_yes") is True


def test_no_row_returns_rest_value_without_skeleton(tmp_markets_db, monkeypatch):
    class _R:
        status_code = 200
        def json(self): return {"neg_risk": False}

    import requests
    monkeypatch.setattr(requests, "get", lambda *a, **k: _R())
    assert mt.get_neg_risk_cached("unknown_tok") is False
    # UPDATE-only write-back must not create skeleton rows
    c = sqlite3.connect(tmp_markets_db)
    n = c.execute("SELECT COUNT(*) FROM markets").fetchone()[0]
    c.close()
    assert n == 0


def test_rest_failure_returns_none(tmp_markets_db, monkeypatch):
    import requests
    def _boom(*a, **k): raise OSError("net down")
    monkeypatch.setattr(requests, "get", _boom)
    assert mt.get_neg_risk_cached("unknown_tok") is None


def test_extract_row_carries_neg_risk():
    row = mt._extract_row("c1", {"negRisk": True, "question": "q"})
    assert row[-1] == 1
    row = mt._extract_row("c1", {"negRisk": False})
    assert row[-1] == 0
    row = mt._extract_row("c1", {})       # key absent → NULL (unknown)
    assert row[-1] is None


# ---------------------------------------------------------------------------
# Book threading
# ---------------------------------------------------------------------------

def test_get_order_book_stamps_freshness(monkeypatch):
    from trading_platform.polymarket.clob_client import ClobClient

    class _R:
        status_code = 200
        def json(self):
            return {"asks": [{"price": "0.5"}], "bids": [{"price": "0.4"}]}

    c = ClobClient()
    monkeypatch.setattr(c._session, "get", lambda *a, **k: _R())
    book = c.get_order_book("tok")
    assert book.get("_fetched_at", 0) > 0


def test_order_methods_accept_book_param():
    from trading_platform.polymarket.clob_client import ClobClient
    for meth in ("place_market_order", "place_limit_order"):
        sig = inspect.signature(getattr(ClobClient, meth))
        assert "book" in sig.parameters, meth
    src = inspect.getsource(ClobClient.place_market_order)
    # staleness guard present: stale/missing stamp → re-fetch
    assert "_fetched_at" in src
    # neg_risk cache consulted before the REST client
    assert "get_neg_risk_cached" in src


def test_call_sites_thread_the_book():
    from trading_platform.polymarket import polymarket_live_executor as ple
    from trading_platform.polymarket import live_position_monitor as lpm
    assert "book=book" in inspect.getsource(ple)
    assert "book=_book" in inspect.getsource(lpm)
