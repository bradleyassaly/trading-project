"""#6 Stage A: resolution_decay captures UMA oracle state at fire time.

Instrument-only — nothing gates on uma_status yet. These tests pin the
plumbing: _candidate_markets must SELECT and carry uma_status through to the
candidate dict (guards against the column-count unpacking bug), and the
emitted payload must stamp uma_status_at_fire so it lands in
polymarket_paper_trades.features_at_fire for later measurement.

_candidate_markets' SQL is Postgres-only (to_char/interval), so we exercise
the Python logic with a fake connection returning canned rows rather than a
SQLite fixture.
"""
from __future__ import annotations

import inspect
from datetime import datetime, timedelta, timezone

from trading_platform.polymarket import resolution_decay_signal as rds


class _FakeCur:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _FakeConn:
    """Routes the two queries in _candidate_markets by table name."""

    def __init__(self, market_rows):
        self._market_rows = market_rows

    def execute(self, sql, params=None):
        if "FROM live_trades" in sql:
            return _FakeCur([])          # no recently-failed cids
        if "FROM markets" in sql:
            return _FakeCur(self._market_rows)
        return _FakeCur([])


def _market_row(uma_status, *, hours=12.0, yes_price=0.18, vol=5000.0):
    """A markets row in the exact SELECT column order of _candidate_markets:
    condition_id, slug, question, end_date_iso, yes_token_id, no_token_id,
    outcome_prices, volume_24h, subcategory, uma_status."""
    end_dt = datetime.now(timezone.utc) + timedelta(hours=hours)
    end_iso = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return (
        "0xabc123", "some-slug", "Will X happen?", end_iso,
        "yes_tok", "no_tok", f'["{yes_price}", "{1 - yes_price:.2f}"]',
        vol, "sports/nba", uma_status,
    )


def test_candidate_markets_carries_uma_status():
    out = rds._candidate_markets(_FakeConn([_market_row("proposed")]))
    assert len(out) == 1, "market in-band should survive the filters"
    assert out[0]["uma_status"] == "proposed"
    # sanity: the rest of the dict is still intact (no unpack drift)
    assert out[0]["condition_id"] == "0xabc123"
    assert out[0]["yes_price"] == 0.18


def test_candidate_markets_handles_null_uma_status():
    # Most open near-resolution markets have no oracle proposal yet; a null
    # must pass through cleanly (measurement will tell us how often).
    out = rds._candidate_markets(_FakeConn([_market_row(None)]))
    assert len(out) == 1
    assert out[0]["uma_status"] is None


def test_candidate_row_has_expected_arity():
    # The SELECT and the unpack must agree. Build a row one column short and
    # assert _candidate_markets raises — i.e. the arity is pinned at 10.
    short_row = _market_row("resolved")[:-1]  # drop uma_status
    try:
        rds._candidate_markets(_FakeConn([short_row]))
    except ValueError:
        return  # expected: "not enough values to unpack"
    raise AssertionError("expected an unpack error on a 9-column row")


def test_payload_stamps_uma_status_at_fire():
    # Source-inspection (the executor invocations in _emit_signal make a full
    # functional call heavy). Mirrors the repo's existing wiring-assertion
    # style in test_decay_curve.py.
    src = inspect.getsource(rds)
    assert '"uma_status_at_fire": market.get("uma_status")' in src
    # and the candidate dict must expose it for the payload to read
    assert '"uma_status": uma_status' in src
