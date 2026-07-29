"""parse_clob_resolution: turn CLOB /markets winner flags into resolution truth.

Only a cleanly-settled binary market (closed=True, a Yes+No token, exactly one
winner) yields a resolution; anything else returns None so we never record a
half-settled or ambiguous outcome.

discover_missing: the backfill's target universe. 2026-07-28 gap: it saw
only live trades + resolved signal_outcomes, so OPEN paper positions on
ended (Gamma-delisted) markets were resolved by nobody — 311/313 open
paper positions jammed the 300 cap. Ended-market open paper books are in
the universe now.
"""
from __future__ import annotations

import os
import sqlite3
import tempfile

from trading_platform.polymarket import clob_resolution_backfill as crb
from trading_platform.polymarket.clob_resolution_backfill import parse_clob_resolution


def test_discover_missing_includes_open_paper_on_ended_markets(monkeypatch):
    fd, path = tempfile.mkstemp(suffix=".db"); os.close(fd)
    try:
        monkeypatch.setenv("DB_BACKEND", "sqlite")
        from trading_platform.polymarket import db_connection as dbc
        monkeypatch.setattr(dbc, "DB_BACKEND", "sqlite")
        # discovery must not depend on the bulk resolution lookup here
        monkeypatch.setattr(crb, "get_resolutions_bulk", lambda cids: {})
        c = sqlite3.connect(path)
        c.execute("CREATE TABLE live_trades (condition_id TEXT, realized_pnl REAL)")
        c.execute("CREATE TABLE signal_outcomes (condition_id TEXT, resolved_at INT)")
        c.execute("CREATE TABLE polymarket_paper_trades "
                  "(condition_id TEXT, exit_ts INT, archived INT)")
        c.execute("CREATE TABLE markets (condition_id TEXT, end_date_iso TEXT)")
        # open paper position on an ended market → must be discovered
        c.execute("INSERT INTO polymarket_paper_trades VALUES ('c_ended', NULL, 0)")
        c.execute("INSERT INTO markets VALUES ('c_ended', '2000-01-01')")
        # open paper position on a live market → must NOT be discovered
        c.execute("INSERT INTO polymarket_paper_trades VALUES ('c_live', NULL, 0)")
        c.execute("INSERT INTO markets VALUES ('c_live', '2999-01-01')")
        # closed paper position on an ended market → not the target
        c.execute("INSERT INTO polymarket_paper_trades VALUES ('c_closed', 123, 0)")
        c.execute("INSERT INTO markets VALUES ('c_closed', '2000-01-01')")
        c.commit()
        try:
            missing = crb.discover_missing(c)
        finally:
            c.close()
        assert "c_ended" in missing
        assert "c_live" not in missing
        assert "c_closed" not in missing
    finally:
        os.unlink(path)


def _mkt(closed, tokens, question="Q?"):
    return {"closed": closed, "question": question, "tokens": tokens}


def _tok(outcome, winner, price, tid):
    return {"outcome": outcome, "winner": winner, "price": price, "token_id": tid}


def test_resolves_no():
    m = _mkt(True, [_tok("Yes", False, 0, "111"), _tok("No", True, 1, "222")])
    r = parse_clob_resolution(m)
    assert r["resolves_yes"] == 0
    assert r["payout_yes"] == 0.0
    assert r["winning_outcome"] == "No"
    assert r["yes_token_id"] == "111" and r["no_token_id"] == "222"
    assert r["question"] == "Q?"


def test_resolves_yes():
    m = _mkt(True, [_tok("Yes", True, 1, "111"), _tok("No", False, 0, "222")])
    r = parse_clob_resolution(m)
    assert r["resolves_yes"] == 1 and r["payout_yes"] == 1.0
    assert r["winning_outcome"] == "Yes"


def test_not_closed_returns_none():
    assert parse_clob_resolution(
        _mkt(False, [_tok("Yes", False, 0, "1"), _tok("No", True, 1, "2")])) is None


def test_no_winner_returns_none():
    assert parse_clob_resolution(
        _mkt(True, [_tok("Yes", False, 0, "1"), _tok("No", False, 0, "2")])) is None


def test_two_winners_returns_none():
    assert parse_clob_resolution(
        _mkt(True, [_tok("Yes", True, 1, "1"), _tok("No", True, 1, "2")])) is None


def test_missing_yes_or_no_token_returns_none():
    assert parse_clob_resolution(_mkt(True, [_tok("Yes", True, 1, "1")])) is None
    assert parse_clob_resolution(_mkt(True, [])) is None


def test_none_and_empty():
    assert parse_clob_resolution(None) is None
    assert parse_clob_resolution({}) is None


def test_outcome_casing_tolerant():
    # CLOB returns "Yes"/"No"; be tolerant of casing/whitespace
    m = _mkt(True, [_tok(" YES ", False, 0, "1"), _tok("no", True, 1, "2")])
    r = parse_clob_resolution(m)
    assert r["resolves_yes"] == 0
