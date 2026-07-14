"""parse_clob_resolution: turn CLOB /markets winner flags into resolution truth.

Only a cleanly-settled binary market (closed=True, a Yes+No token, exactly one
winner) yields a resolution; anything else returns None so we never record a
half-settled or ambiguous outcome.
"""
from __future__ import annotations

from trading_platform.polymarket.clob_resolution_backfill import parse_clob_resolution


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
