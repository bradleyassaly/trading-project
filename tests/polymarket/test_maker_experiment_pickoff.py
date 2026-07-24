"""Tests for maker-experiment pickoff measurability on late-booked live fills.

Regression cover for the 2026-07-23 bug: pickoff was only computed inside the
+5m mark window, but live fills are booked LATE (reconcile_fills_from_activity
stamps filled_at = the on-chain time), so by the time the row is 'filled' its
age already exceeds the window and pickoff was never computed (0 of 29 live
fills marked, while the reported "29%" was entirely dry-run).

These exercise the layered fallback: 5m -> 30m -> late, each tagged in
pickoff_basis, plus the honest UNMEASURED path when a fill is booked too late
to mark. Uses a tmp-path SQLite DB (like test_circuit_breaker) so nothing
touches the shared Postgres.
"""
from __future__ import annotations

import time

import pytest

from trading_platform.polymarket import db_connection as dbc
from trading_platform.polymarket import maker_experiment as me


# ── Fixtures ────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _sqlite_backend(monkeypatch, tmp_path):
    """Pin get_connection() to a tmp-path SQLite DB.

    DB_BACKEND / DEFAULT_DB_PATH are read as module globals inside
    db_connection, so setenv alone is not enough — patch the attributes.
    """
    db = tmp_path / "wi.db"
    monkeypatch.setattr(dbc, "DB_BACKEND", "sqlite")
    monkeypatch.setattr(dbc, "DEFAULT_DB_PATH", str(db))
    conn = dbc.get_connection()
    conn.executescript(
        """
        CREATE TABLE maker_experiment_orders (
            id INTEGER PRIMARY KEY,
            posted_at INTEGER, live INTEGER DEFAULT 1,
            condition_id TEXT, yes_token_id TEXT, no_token_id TEXT,
            fill_price REAL, filled_at INTEGER, shares REAL DEFAULT 5,
            quote_price REAL, hours_to_resolve REAL,
            status TEXT DEFAULT 'filled',
            mark_5m REAL, mark_30m REAL, mark_late REAL,
            resolved_at INTEGER, resolves_yes INTEGER, realized_pnl REAL,
            pickoff INTEGER, pickoff_basis TEXT
        );
        CREATE TABLE markets (condition_id TEXT, end_date_iso TEXT, outcome_prices TEXT);
        CREATE TABLE market_resolutions (condition_id TEXT, resolves_yes INTEGER);
        CREATE TABLE market_ticks (condition_id TEXT, price REAL, timestamp INTEGER);
        """
    )
    conn.commit()
    conn.close()
    return db


class _FakeClob:
    """Minimal CLOB double: returns a fixed YES price for every token.

    A non-None get_last_price keeps _yes_price_now off the market_ticks
    fallback, so the mark is exactly ``1 - yes_price``.
    """

    def __init__(self, yes_price):
        self.yes_price = yes_price

    def get_last_price(self, token_id):
        return self.yes_price


def _insert_fill(*, oid, fill_price, age_s, htr=24.0, **cols):
    """Insert one 'filled' order whose fill happened ``age_s`` seconds ago.

    posted_at is kept recent with a long hours_to_resolve so _resolve_lookup
    treats the market as still-live (returns None) unless the caller seeds
    market_resolutions.
    """
    now = int(time.time())
    conn = dbc.get_connection()
    row = {
        "id": oid, "posted_at": now - 30, "live": 1,
        "condition_id": f"cid{oid}", "yes_token_id": f"yes{oid}",
        "no_token_id": f"no{oid}", "fill_price": fill_price,
        "filled_at": now - age_s, "shares": 5.0, "hours_to_resolve": htr,
        "status": "filled",
    }
    row.update(cols)
    keys = ", ".join(row)
    ph = ", ".join("?" for _ in row)
    conn.execute(f"INSERT INTO maker_experiment_orders ({keys}) VALUES ({ph})",
                 tuple(row.values()))
    conn.commit()
    conn.close()


def _get(oid):
    conn = dbc.get_connection()
    r = conn.execute(
        "SELECT status, mark_5m, mark_30m, mark_late, pickoff, pickoff_basis, "
        "resolves_yes FROM maker_experiment_orders WHERE id=?", (oid,)).fetchone()
    conn.close()
    return dict(zip(
        ("status", "mark_5m", "mark_30m", "mark_late", "pickoff",
         "pickoff_basis", "resolves_yes"), r))


def _run(yes_price):
    conn = dbc.get_connection()
    try:
        me.update_fill_marks_and_resolutions(conn, _FakeClob(yes_price))
    finally:
        conn.close()


# ── _is_pickoff formula ─────────────────────────────────────────────────────

def test_is_pickoff_formula():
    # pickoff iff mark <= 80% of fill price
    assert me._is_pickoff(0.48, 0.61) == 1     # 0.48 <= 0.488
    assert me._is_pickoff(0.49, 0.61) == 0     # 0.49  > 0.488
    assert me._is_pickoff(0.80, 1.00) == 1     # exactly at the boundary
    assert me._is_pickoff(0.81, 1.00) == 0


# ── 5m window (unchanged gold standard) ─────────────────────────────────────

def test_5m_window_sets_basis_5m():
    # age in [300,1800): mark from 5m. yes=0.40 -> mark 0.60 vs fill 0.80 -> pickoff
    _insert_fill(oid=1, fill_price=0.80, age_s=600)
    _run(yes_price=0.40)
    r = _get(1)
    assert r["mark_5m"] == pytest.approx(0.60)
    assert r["pickoff"] == 1
    assert r["pickoff_basis"] == "5m"
    assert r["mark_30m"] is None and r["mark_late"] is None


# ── 30m fallback: the late-booked live fix (option a) ───────────────────────

def test_late_booked_fill_marks_via_30m():
    # No 5m mark (booked past the window). age in [1800,7200). This mirrors the
    # real row 170: mark ~0.01, fill 0.61 -> textbook pickoff, now flagged.
    _insert_fill(oid=2, fill_price=0.61, age_s=3600)
    _run(yes_price=0.99)  # mark = 0.01
    r = _get(2)
    assert r["mark_30m"] == pytest.approx(0.01)
    assert r["mark_5m"] is None
    assert r["pickoff"] == 1
    assert r["pickoff_basis"] == "30m"


def test_30m_does_not_overwrite_existing_5m_pickoff():
    # A row already marked via 5m (pickoff 0, basis '5m'): the 30m pass records
    # mark_30m but must NOT change pickoff/basis (5m is authoritative).
    _insert_fill(oid=3, fill_price=0.65, age_s=3600,
                 mark_5m=0.64, pickoff=0, pickoff_basis="5m")
    _run(yes_price=0.10)  # would-be mark 0.90, irrelevant
    r = _get(3)
    assert r["mark_30m"] == pytest.approx(0.90)
    assert r["pickoff"] == 0
    assert r["pickoff_basis"] == "5m"


# ── late first-observation capture (option c) ───────────────────────────────

def test_late_capture_when_both_windows_missed():
    # Booked ~2.7h late: past both windows but within LATE_MARK_MAX_AGE_S and
    # still unresolved -> one 'late' mark carries pickoff.
    _insert_fill(oid=4, fill_price=0.70, age_s=10000)
    _run(yes_price=0.95)  # mark 0.05 <= 0.56 -> pickoff
    r = _get(4)
    assert r["mark_late"] == pytest.approx(0.05)
    assert r["mark_5m"] is None and r["mark_30m"] is None
    assert r["pickoff"] == 1
    assert r["pickoff_basis"] == "late"


def test_late_skipped_when_too_old_stays_unmeasured():
    # Beyond the staleness cap: the mark has decayed toward settlement, so we
    # keep pickoff UNMEASURED (NULL) rather than fabricate one.
    _insert_fill(oid=5, fill_price=0.70, age_s=me.LATE_MARK_MAX_AGE_S + 5000)
    _run(yes_price=0.95)
    r = _get(5)
    assert r["mark_late"] is None
    assert r["pickoff"] is None
    assert r["pickoff_basis"] is None


def test_late_capture_skipped_once_resolved():
    # Same late age, but the market has resolved: no 'late' mark (settlement
    # price is not adverse selection); the row is booked resolved with pickoff
    # left UNMEASURED — resolution-adverse covers this cohort separately.
    _insert_fill(oid=6, fill_price=0.70, age_s=10000)
    conn = dbc.get_connection()
    conn.execute("INSERT INTO market_resolutions (condition_id, resolves_yes) "
                 "VALUES (?, ?)", ("cid6", 1))
    conn.commit()
    conn.close()
    _run(yes_price=0.95)
    r = _get(6)
    assert r["status"] == "resolved"
    assert r["resolves_yes"] == 1
    assert r["mark_late"] is None
    assert r["pickoff"] is None


def test_before_5m_window_no_mark():
    # Freshly booked (age < 5m): too early to mark, nothing set.
    _insert_fill(oid=7, fill_price=0.70, age_s=60)
    _run(yes_price=0.95)
    r = _get(7)
    assert r["pickoff"] is None and r["pickoff_basis"] is None
    assert r["mark_5m"] is None and r["mark_30m"] is None and r["mark_late"] is None
