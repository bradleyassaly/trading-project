"""live_mark_logger: measurement-only fine-mark capture for open live positions.

Verifies it (1) captures a mark for a near-resolution open position, (2) skips
far-from-resolution ones, (3) never crashes on a book error, and (4) ignores
closed / dry-run rows. All in-memory sqlite; no CLOB, no live trading.
"""
import sqlite3
import time

import trading_platform.polymarket.live_mark_logger as lml


class FakeClient:
    def __init__(self, book):
        self._book = book
        self.calls = 0

    def get_order_book(self, token_id):
        self.calls += 1
        return dict(self._book)


def _mk_conn():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """CREATE TABLE live_trades (
            id INTEGER PRIMARY KEY, token_id TEXT, condition_id TEXT,
            signal_type TEXT, direction TEXT, entry_price REAL,
            resolution_date INTEGER, dry_run INTEGER, exit_ts INTEGER,
            status TEXT)"""
    )
    conn.commit()
    return conn


def _seed(conn, tid, token, res_in_h, *, dry_run=0, exit_ts=None, status="matched"):
    now = int(time.time())
    conn.execute(
        """INSERT INTO live_trades
           (id, token_id, condition_id, signal_type, direction, entry_price,
            resolution_date, dry_run, exit_ts, status)
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (tid, token, "c" + str(tid), "resolution_decay", "BUY", 0.24,
         now + int(res_in_h * 3600), dry_run, exit_ts, status),
    )
    conn.commit()


_BOOK = {"bids": [{"price": "0.20", "size": "100"}],
         "asks": [{"price": "0.26", "size": "100"}]}


def test_captures_mark_for_near_resolution_position():
    lml._schema_ready = False
    conn = _mk_conn()
    _seed(conn, 1, "tokA", res_in_h=2.0)  # inside the 12h window
    fake = FakeClient(_BOOK)
    r = lml.capture_marks_once(conn=conn, client=fake)
    assert r == {"open": 1, "eligible": 1, "marks": 1, "errors": 0}
    row = conn.execute(
        "SELECT trade_id, mid, best_bid, best_ask, spread FROM live_position_marks"
    ).fetchone()
    assert row[0] == 1
    assert abs(row[1] - 0.23) < 1e-9    # mid = (0.20 + 0.26) / 2
    assert abs(row[2] - 0.20) < 1e-9
    assert abs(row[3] - 0.26) < 1e-9
    assert abs(row[4] - 0.06) < 1e-9    # spread
    conn.close()


def test_skips_far_from_resolution():
    lml._schema_ready = False
    conn = _mk_conn()
    _seed(conn, 2, "tokB", res_in_h=48.0)  # well outside 12h
    fake = FakeClient(_BOOK)
    r = lml.capture_marks_once(conn=conn, client=fake, near_hours=12.0)
    assert r["open"] == 1 and r["eligible"] == 0 and r["marks"] == 0
    assert fake.calls == 0  # never fetched the book
    conn.close()


def test_safe_on_book_error():
    lml._schema_ready = False
    conn = _mk_conn()
    _seed(conn, 3, "tokC", res_in_h=1.0)

    class Boom:
        def get_order_book(self, token_id):
            raise RuntimeError("clob down")

    r = lml.capture_marks_once(conn=conn, client=Boom())
    assert r["eligible"] == 1 and r["marks"] == 0 and r["errors"] == 1
    conn.close()


def test_ignores_closed_and_dry_run():
    lml._schema_ready = False
    conn = _mk_conn()
    _seed(conn, 4, "tokD", res_in_h=1.0, exit_ts=int(time.time()))  # closed
    _seed(conn, 5, "tokE", res_in_h=1.0, dry_run=1)                 # dry-run
    r = lml.capture_marks_once(conn=conn, client=FakeClient(_BOOK))
    assert r["open"] == 0 and r["marks"] == 0
    conn.close()
