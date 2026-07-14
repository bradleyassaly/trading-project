"""#1 Stage A: per-wallet USDC cash-flow logic.

net_cash_out = withdrawals - deposits, classifying each USDC Transfer by
whether its counterparty is a Polymarket contract (internal) or external
(real cash in/out). These tests pin the pure decode/classify/aggregate/cohort
logic; the RPC fetch + backfill layer needs live Polygon RPC and is not
exercised here.
"""
from __future__ import annotations

import pytest

from trading_platform.polymarket import wallet_cash_flow as cf

W = "0x" + "1" * 40          # our wallet
EXT = "0x" + "2" * 40        # an external address (CEX, EOA, bridge)
EXT2 = "0x" + "3" * 40
INTERNAL = cf._CTF_EXCHANGE_V2  # a known Polymarket contract


def _log(frm, to, usdc, *, topic=cf.TRANSFER_TOPIC, contract=cf.USDC_E):
    raw = int(round(usdc * cf.USDC_DECIMALS))
    return {
        "address": contract,
        "topics": [topic, cf._pad_addr(frm), cf._pad_addr(to)],
        "data": "0x" + format(raw, "064x"),
    }


# ── decode ──────────────────────────────────────────────────────────────────

def test_decode_valid_transfer():
    d = cf.decode_usdc_transfer(_log(EXT, W, 100.0))
    assert d == {"from": EXT, "to": W, "amount_usdc": 100.0}


def test_decode_rejects_wrong_topic():
    assert cf.decode_usdc_transfer(_log(EXT, W, 100.0, topic="0x" + "a" * 64)) is None


def test_decode_rejects_malformed():
    assert cf.decode_usdc_transfer({"topics": [], "data": "0x0"}) is None
    assert cf.decode_usdc_transfer({"topics": [cf.TRANSFER_TOPIC, cf._pad_addr(W),
                                               cf._pad_addr(EXT)], "data": "nope"}) is None
    # zero-value transfer is dropped (no cash moved)
    assert cf.decode_usdc_transfer(_log(EXT, W, 0.0)) is None


# ── classify ─────────────────────────────────────────────────────────────────

def test_classify_deposit_and_withdrawal():
    assert cf.classify_transfer(EXT, W, W) == "deposit"
    assert cf.classify_transfer(W, EXT, W) == "withdrawal"


def test_classify_polymarket_internal_both_directions():
    assert cf.classify_transfer(INTERNAL, W, W) == "internal"
    assert cf.classify_transfer(W, INTERNAL, W) == "internal"


def test_classify_self_and_unrelated():
    assert cf.classify_transfer(W, W, W) == "self"
    assert cf.classify_transfer(EXT, EXT2, W) == "unrelated"


def test_classify_is_case_insensitive():
    # checksummed / upper wallet still matches the lowercase log addresses
    assert cf.classify_transfer(EXT.upper(), W.upper(), W) == "deposit"


def test_classify_zero_addr_is_external():
    # bridge mint (0x0 → wallet) counts as a real deposit
    assert cf.classify_transfer(cf.ZERO_ADDR, W, W) == "deposit"


# ── aggregate ────────────────────────────────────────────────────────────────

def test_aggregate_net_cash_out():
    logs = [
        _log(EXT, W, 1000.0),     # deposit  +1000 in
        _log(EXT2, W, 500.0),     # deposit  +500 in
        _log(W, EXT, 2000.0),     # withdrawal 2000 out
        _log(W, INTERNAL, 750.0), # internal (buying) — ignored
        _log(INTERNAL, W, 300.0), # internal (settle) — ignored
        _log(EXT, EXT2, 999.0),   # unrelated — ignored
    ]
    out = cf.aggregate_cash_flow(logs, W)
    assert out["deposits_usdc"] == pytest.approx(1500.0)
    assert out["withdrawals_usdc"] == pytest.approx(2000.0)
    assert out["net_cash_out"] == pytest.approx(500.0)   # 2000 - 1500
    assert out["n_deposits"] == 2
    assert out["n_withdrawals"] == 1
    assert out["n_internal"] == 2


def test_aggregate_accepts_predecoded():
    logs = [{"from": EXT, "to": W, "amount_usdc": 42.0}]
    out = cf.aggregate_cash_flow(logs, W)
    assert out["deposits_usdc"] == pytest.approx(42.0)
    assert out["net_cash_out"] == pytest.approx(-42.0)


def test_aggregate_empty():
    out = cf.aggregate_cash_flow([], W)
    assert out["net_cash_out"] == 0.0 and out["n_deposits"] == 0


# ── cohort selection ─────────────────────────────────────────────────────────

class _Cur:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _Conn:
    def __init__(self, mapping):
        self._mapping = mapping

    def execute(self, q):
        for key, val in self._mapping.items():
            if key in q:
                if isinstance(val, Exception):
                    raise val
                return _Cur(val)
        return _Cur([])


def test_select_cohort_unions_and_dedupes():
    conn = _Conn({
        "FROM leaderboard": [("0xAAA",), ("0xბ",)],  # note mixed case handled by _norm
        "wallet_alpha_scores": [("0xaaa",), ("0xbbb",)],
        "insider_wallets": [("0xccc",)],
        "wallet_archetypes": [("0xDDD",)],
    })
    out = cf.select_cohort(conn)
    assert out == sorted({"0xaaa", "0xბ".lower(), "0xbbb", "0xccc", "0xddd"})


def test_select_cohort_survives_missing_table():
    conn = _Conn({
        "FROM leaderboard": [("0xaaa",)],
        "wallet_alpha_scores": RuntimeError("no such table"),
        "insider_wallets": [("0xbbb",)],
        "wallet_archetypes": [],
    })
    out = cf.select_cohort(conn)
    assert out == ["0xaaa", "0xbbb"]


def test_select_cohort_respects_limit():
    conn = _Conn({"FROM leaderboard": [(f"0x{i:040x}",) for i in range(10)]})
    assert len(cf.select_cohort(conn, limit=3)) == 3
