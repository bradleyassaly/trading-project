"""N7: complementary-leg guaranteed-loss gate helper.

Two mutually-exclusive YES legs on one event whose combined YES cost per share
> 1.0 lock a loss. Tests the extracted _open_event_yes_cost helper + the
threshold decision, including the '-more-markets' overflow-page normalization
that distinguishes this from a naive event-slug equality check.
"""
import sqlite3

from trading_platform.polymarket.polymarket_live_executor import _open_event_yes_cost


def _seed(conn):
    conn.execute(
        """CREATE TABLE markets (condition_id TEXT PRIMARY KEY, event_slug TEXT)"""
    )
    conn.execute(
        """CREATE TABLE live_trades (
             id INTEGER PRIMARY KEY, condition_id TEXT, direction TEXT,
             entry_price REAL, dry_run INTEGER, exit_ts INTEGER, status TEXT)"""
    )
    conn.commit()


def _add_leg(conn, cid, slug, price, direction="BUY", status="matched", dry=0, exit_ts=None):
    conn.execute("INSERT INTO markets VALUES (?,?)", (cid, slug))
    conn.execute(
        "INSERT INTO live_trades (condition_id, direction, entry_price, dry_run, exit_ts, status) "
        "VALUES (?,?,?,?,?,?)", (cid, direction, price, dry, exit_ts, status))
    conn.commit()


def test_blocks_when_combined_yes_exceeds_one():
    conn = sqlite3.connect(":memory:")
    _seed(conn)
    _add_leg(conn, "c1", "game-x", 0.55)
    _add_leg(conn, "c2", "game-x", 0.50)
    conn.execute("INSERT INTO markets VALUES ('c3','game-x')")  # candidate market
    conn.commit()
    open_yes, n = _open_event_yes_cost(conn, "c3", "game-x")
    combined = open_yes + 0.10  # candidate YES price
    assert n == 2
    assert combined > 1.0  # 1.15 -> guaranteed loss, blocks
    conn.close()


def test_allows_when_under_one():
    conn = sqlite3.connect(":memory:")
    _seed(conn)
    _add_leg(conn, "c1", "game-x", 0.30)
    _add_leg(conn, "c2", "game-x", 0.30)
    open_yes, n = _open_event_yes_cost(conn, "c3", "game-x")
    assert open_yes + 0.30 <= 1.0  # 0.90 -> allowed
    conn.close()


def test_normalizes_more_markets_overflow_page():
    conn = sqlite3.connect(":memory:")
    _seed(conn)
    _add_leg(conn, "c1", "game-x-more-markets", 0.60)  # overflow page, same event
    open_yes, n = _open_event_yes_cost(conn, "c3", "game-x")
    assert n == 1 and abs(open_yes - 0.60) < 1e-9  # grouped despite suffix
    conn.close()


def test_ignores_sell_legs():
    conn = sqlite3.connect(":memory:")
    _seed(conn)
    _add_leg(conn, "c1", "game-x", 0.90, direction="SELL")
    open_yes, n = _open_event_yes_cost(conn, "c3", "game-x")
    assert n == 0 and open_yes == 0.0  # SELL not part of YES sum
    conn.close()


def test_ignores_closed_and_other_events():
    conn = sqlite3.connect(":memory:")
    _seed(conn)
    _add_leg(conn, "c1", "game-x", 0.80, exit_ts=123)   # closed
    _add_leg(conn, "c2", "game-y", 0.80)                 # different event
    open_yes, n = _open_event_yes_cost(conn, "c3", "game-x")
    assert n == 0 and open_yes == 0.0
    conn.close()
