"""P6: cost/slippage recording on EVERY order attempt (record-only).

The execution comparator filtered out rejects/errors, so the trades we
didn't get were invisible. Now every attempt carries decision-time market
context (mid, book top, spread) and, when filled, realized slippage — in
held-token cents, positive = adverse (entries are always a CLOB BUY of the
held token, so the sign is uniform).
"""
import os
import sqlite3
import tempfile

import pytest

from trading_platform.polymarket.clob_client import OrderResult
from trading_platform.polymarket.polymarket_live_executor import PolymarketLiveExecutor


@pytest.fixture()
def executor(monkeypatch):
    """Skeleton executor: only _db_path is needed by the recording path."""
    fd, path = tempfile.mkstemp(suffix=".db"); os.close(fd)
    monkeypatch.setenv("DB_BACKEND", "sqlite")
    from trading_platform.polymarket import db_connection as dbc
    monkeypatch.setattr(dbc, "DB_BACKEND", "sqlite")
    ex = object.__new__(PolymarketLiveExecutor)
    ex._db_path = path
    ex._ensure_live_trades_table()
    yield ex
    os.unlink(path)


def _row(path, cols):
    c = sqlite3.connect(path)
    r = c.execute(f"SELECT {cols} FROM live_trades ORDER BY id DESC LIMIT 1").fetchone()
    c.close()
    return r


def _order(fill=None, target=None, ask=None, bid=None, status="matched"):
    return OrderResult(
        success=fill is not None, order_id="o1", status=status,
        filled_price=fill, filled_size=5.0, error_msg=None, raw={},
        target_price=target, best_ask_at_submit=ask, best_bid_at_submit=bid)


def test_filled_attempt_records_slippage_and_spread(executor):
    # fill 0.34, mid 0.32, ask 0.33 → slippage_c = +2.0, spread_paid_c = +1.0
    signal = {"signal_type": "resolution_decay", "condition_id": "c1",
              "direction": "BUY", "confidence": 0.6,
              "_mid_at_decision": 0.32, "_yes_mid_at_decision": 0.32,
              "_best_ask_at_decision": 0.33, "_best_bid_at_decision": 0.31,
              "_spread_at_decision": 0.02}
    executor._record_attempt(signal, 5.0, 0.32, _order(fill=0.34, target=0.33),
                             dry_run=False, status="matched", error_msg=None,
                             token_id="tok")
    r = _row(executor._db_path,
             "mid_at_decision, slippage_c, spread_paid_c, book_target_price, "
             "best_ask_at_decision, spread_at_decision")
    assert r == (0.32, pytest.approx(2.0), pytest.approx(1.0), 0.33, 0.33, 0.02)


def test_reject_records_spread_but_null_slippage(executor):
    # A depth-blocked attempt: book context present, NO fill → spread_paid_c
    # populated (the new info), slippage_c NULL.
    signal = {"signal_type": "whale_entry_filtered", "condition_id": "c2",
              "direction": "BUY", "confidence": 0.5,
              "_mid_at_decision": 0.50,
              "_best_ask_at_decision": 0.55, "_best_bid_at_decision": 0.45,
              "_spread_at_decision": 0.10}
    executor._record_attempt(signal, 5.0, 0.50, None,
                             dry_run=False, status="blocked",
                             error_msg="thin book", token_id="tok")
    r = _row(executor._db_path, "spread_paid_c, slippage_c, status")
    assert r[0] == pytest.approx(5.0)   # (0.55-0.50)*100
    assert r[1] is None
    assert r[2] == "blocked"


def test_early_gate_block_records_nulls(executor):
    # Gates before the price fetch (kill switch, horizon) have no market
    # context — columns stay NULL, no exception.
    signal = {"signal_type": "resolution_decay", "condition_id": "c3",
              "direction": "BUY", "confidence": 0.6}
    executor._record_attempt(signal, 5.0, None, None,
                             dry_run=False, status="blocked",
                             error_msg="kill switch", token_id=None)
    r = _row(executor._db_path,
             "mid_at_decision, spread_paid_c, slippage_c, book_target_price")
    assert r == (None, None, None, None)


def test_order_result_ask_overrides_decision_snapshot(executor):
    # When the order method returns its own book top (fresher), it wins over
    # the executor's earlier decision snapshot.
    signal = {"signal_type": "resolution_decay", "condition_id": "c4",
              "direction": "BUY", "confidence": 0.6,
              "_mid_at_decision": 0.30, "_best_ask_at_decision": 0.31}
    executor._record_attempt(signal, 5.0, 0.30,
                             _order(fill=0.33, target=0.33, ask=0.32, bid=0.29),
                             dry_run=False, status="matched", error_msg=None,
                             token_id="tok")
    r = _row(executor._db_path, "best_ask_at_decision, spread_paid_c")
    assert r[0] == 0.32
    assert r[1] == pytest.approx(2.0)   # (0.32-0.30)*100


def test_order_result_new_fields_default_none():
    o = OrderResult(success=True, order_id="x", status="matched",
                    filled_price=0.5, filled_size=1.0, error_msg=None, raw={})
    assert o.target_price is None
    assert o.best_ask_at_submit is None
    assert o.best_bid_at_submit is None
