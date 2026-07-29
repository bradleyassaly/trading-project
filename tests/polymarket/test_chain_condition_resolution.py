"""ConditionResolution → market_resolutions: the handler policy layer.

decode_condition_resolution itself is pinned against a REAL captured log in
test_chain_decoders.py. This file covers what the handler does with it:
the canonical-contract pin, the markets-table YES/NO mapping, tie handling,
the kill switch, and that the WS actually subscribes to the topic in BOTH
modes (a decoder nothing subscribes to is a decoder that never runs — the
2026-06-30 dark-lane failure mode).

DB-free: get_connection and record_resolution are patched at their module
attributes (the handler imports them lazily inside the write closure).
"""
from __future__ import annotations

import asyncio
import json
from unittest.mock import MagicMock

import pytest

from trading_platform.polymarket import db_connection as dbc
from trading_platform.polymarket import resolutions as rs
from trading_platform.polymarket.wallet_stream import (
    CONDITION_RESOLUTION_TOPIC,
    CONDITIONAL_TOKENS,
    WalletStream,
    _pad_addr,
)

CID = "0x" + "11" * 32
QID = "0x" + "22" * 32
ORACLE = "0x65070be91477460d8a7aeeb94ef92fe056c2f2a7"
YES_TOK, NO_TOK = "111222333", "444555666"
BLOCK = "0x1234"
BLOCK_TS = 1785000000


def _u256(v: int) -> str:
    return hex(v)[2:].rjust(64, "0")


def _log(numerators: list[int], *, address: str = CONDITIONAL_TOKENS) -> dict:
    data = "0x" + "".join([
        _u256(len(numerators)),   # outcomeSlotCount
        _u256(0x40),              # offset of the array head
        _u256(len(numerators)),   # array length
        *[_u256(n) for n in numerators],
    ])
    return {
        "address": address,
        "topics": [CONDITION_RESOLUTION_TOPIC, CID, _pad_addr(ORACLE), QID],
        "data": data,
        "blockNumber": BLOCK,
        "transactionHash": "0x" + "ef" * 32,
    }


@pytest.fixture()
def stream(monkeypatch):
    monkeypatch.setenv("CHAIN_RESOLUTION_PERSIST", "1")
    s = WalletStream("wss://example.invalid", set())
    s._blk_ts_cache = {BLOCK: BLOCK_TS}   # no HTTP for the block timestamp
    return s


@pytest.fixture()
def recorded(monkeypatch):
    """Patch the DB + writer; return the list of record_resolution kwargs."""
    calls: list[dict] = []

    def _fake_record(condition_id, **kw):
        calls.append({"condition_id": condition_id, **kw})
        return "inserted"

    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = (
        YES_TOK, NO_TOK, "Will X happen?")
    monkeypatch.setattr(dbc, "get_connection", lambda *a, **k: conn)
    monkeypatch.setattr(rs, "record_resolution", _fake_record)
    return calls


def _run(stream, log):
    asyncio.run(stream._handle_condition_resolution(log, log["topics"]))


# ---------------------------------------------------------------------------
# The mapping that matters: numerators → YES/NO in our canonical price space
# ---------------------------------------------------------------------------

def test_yes_win_records_resolves_yes_1(stream, recorded):
    _run(stream, _log([1, 0]))
    assert len(recorded) == 1
    c = recorded[0]
    assert c["condition_id"] == CID
    assert c["source"] == "chain_condition_resolution"
    assert c["resolves_yes"] == 1
    assert c["payout_yes"] == 1.0
    assert c["winning_outcome"] == "Yes"
    assert (c["yes_token_id"], c["no_token_id"]) == (YES_TOK, NO_TOK)
    assert c["resolved_at"] == BLOCK_TS
    assert c["question"] == "Will X happen?"
    assert c["details"]["numerators"] == [1, 0]
    assert c["details"]["oracle"] == ORACLE


def test_no_win_records_resolves_yes_0(stream, recorded):
    _run(stream, _log([0, 1]))
    assert recorded[0]["resolves_yes"] == 0
    assert recorded[0]["payout_yes"] == 0.0
    assert recorded[0]["winning_outcome"] == "No"


def test_tie_records_half_payout_and_null_verdict(stream, recorded):
    """A [1,1] void pays both sides $0.50. Forcing that to YES or NO would
    invent a winner; payout_yes=0.5 with a NULL verdict is the honest row."""
    _run(stream, _log([1, 1]))
    assert recorded[0]["payout_yes"] == 0.5
    assert recorded[0]["resolves_yes"] is None
    assert recorded[0]["winning_outcome"] is None


def test_scaled_numerators_are_not_confused_for_a_tie(stream, recorded):
    # UMA can report large equal-scale numerators; only the RATIO matters.
    _run(stream, _log([10_000, 0]))
    assert recorded[0]["resolves_yes"] == 1 and recorded[0]["payout_yes"] == 1.0


def test_non_binary_condition_is_skipped(stream, recorded):
    _run(stream, _log([0, 1, 0]))
    assert recorded == []
    assert stream._stats["chain_res_nonbinary"] == 1


# ---------------------------------------------------------------------------
# Policy guards
# ---------------------------------------------------------------------------

def test_non_canonical_contract_is_ignored(stream, recorded):
    """conditionId = keccak(oracle, questionId, slotCount) is portable across
    CTF deployments, so a clone could emit an INVERTED payout for a real
    Polymarket condition. Only the canonical contract is trusted."""
    _run(stream, _log([1, 0], address="0x" + "de" * 20))
    assert recorded == []


def test_unknown_condition_is_counted_not_written(stream, monkeypatch):
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = None   # not in markets
    monkeypatch.setattr(dbc, "get_connection", lambda *a, **k: conn)
    monkeypatch.setattr(rs, "record_resolution",
                        lambda *a, **k: pytest.fail("must not write"))
    _run(stream, _log([1, 0]))
    assert stream._stats["chain_res_unknown_cid"] == 1


def test_market_row_without_token_ids_is_skipped(stream, monkeypatch):
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = (None, None, "Q?")
    monkeypatch.setattr(dbc, "get_connection", lambda *a, **k: conn)
    monkeypatch.setattr(rs, "record_resolution",
                        lambda *a, **k: pytest.fail("must not write"))
    _run(stream, _log([1, 0]))
    assert stream._stats["chain_res_unknown_cid"] == 1


def test_kill_switch_disables_persist(stream, recorded, monkeypatch):
    monkeypatch.setenv("CHAIN_RESOLUTION_PERSIST", "0")
    _run(stream, _log([1, 0]))
    assert recorded == []


def test_write_failure_is_counted_not_raised(stream, monkeypatch):
    def _boom(*a, **k):
        raise RuntimeError("db down")
    monkeypatch.setattr(dbc, "get_connection", _boom)
    _run(stream, _log([1, 0]))            # must not propagate
    assert stream._stats["chain_res_errors"] == 1


def test_successful_write_increments_counter(stream, recorded):
    _run(stream, _log([1, 0]))
    assert stream._stats["chain_resolutions"] == 1


# ---------------------------------------------------------------------------
# Wiring: subscribed in both modes, and routed by topic0
# ---------------------------------------------------------------------------

def _assert_resolution_sub(msgs):
    # params is ["logs", filter] for log subs and ["newHeads"] for the
    # header sub — hence the length guard.
    subs = [m for m in msgs
            if len(m["params"]) > 1
            and (m["params"][1] or {}).get("topics") == [[CONDITION_RESOLUTION_TOPIC]]]
    assert len(subs) == 1, "exactly one ConditionResolution subscription"
    assert subs[0]["params"][1]["address"] == [CONDITIONAL_TOKENS]


def test_broad_mode_subscribes_to_condition_resolution(stream):
    _assert_resolution_sub(stream._broad_sub_msgs())


def test_narrow_mode_subscribes_to_condition_resolution(stream):
    class FakeWS:
        def __init__(self):
            self.sent = []
            self._pending = []

        async def send(self, raw):
            msg = json.loads(raw)
            self.sent.append(msg)
            self._pending.append(msg["id"])

        async def recv(self):
            return json.dumps({"id": self._pending.pop(0), "result": "0xsub"})

    ws = FakeWS()
    errors = asyncio.run(stream._subscribe(ws, broad=False))
    assert errors == {}
    _assert_resolution_sub(ws.sent)


def test_subscription_ids_are_unique(stream):
    """A duplicate request id would make _await_sub_confirms mark two
    subscriptions confirmed on one response — a silently dead filter."""
    ids = [m["id"] for m in stream._broad_sub_msgs()]
    assert len(ids) == len(set(ids))


def test_router_dispatches_topic0_to_the_handler(stream, monkeypatch):
    seen = []

    async def _spy(log, topics):
        seen.append(log)

    monkeypatch.setattr(stream, "_handle_condition_resolution", _spy)
    log = _log([1, 0])
    asyncio.run(stream._handle_log({"result": log}))
    assert seen == [log]
