"""Schema-ensure must not convoy readers behind an ACCESS EXCLUSIVE lock.

2026-08-02 incident: the nightly pg_dump held ACCESS SHARE on every table
(an 84M-row wallet_trades COPY takes minutes). A schema-ensure fired
``ALTER TABLE markets ADD COLUMN event_slug TEXT`` — for a column that had
existed for weeks — which queued for ACCESS EXCLUSIVE behind the dump. In
Postgres a queued ACCESS EXCLUSIVE request blocks every LATER reader too,
so all readers of `markets` and `live_trades` stalled 5+ minutes on a DDL
statement that could only ever fail with "column already exists".

Two independent defences, both tested here:

  1. ``ensure_columns`` reads the catalog and emits NO DDL when nothing is
     missing. This is the load-bearing fix. ``ADD COLUMN IF NOT EXISTS``
     is NOT sufficient — verified against the live database, an
     IF-NOT-EXISTS ALTER on an already-existing column still takes ACCESS
     EXCLUSIVE and still convoys. It suppresses the error, not the lock.

  2. Every lock-taking DDL statement runs under a short ``lock_timeout``,
     so anything that slips past defence 1 (or any future call site) fails
     fast and retries instead of freezing the platform.
"""
from __future__ import annotations

import sqlite3

import pytest

from trading_platform.polymarket import db_connection as dbc


@pytest.fixture(autouse=True)
def _clear_ensure_cache():
    """The ensured-columns cache is process-global; isolate each test."""
    dbc._ENSURED_COLUMNS_CACHE.clear()
    yield
    dbc._ENSURED_COLUMNS_CACHE.clear()


@pytest.fixture
def sqlite_db(tmp_path, monkeypatch):
    """A real SQLite DB routed through the same code path as production."""
    monkeypatch.setattr(dbc, "DB_BACKEND", "sqlite")
    path = tmp_path / "ensure.db"
    conn = sqlite3.connect(str(path))
    conn.execute("CREATE TABLE markets (condition_id TEXT PRIMARY KEY, slug TEXT)")
    conn.commit()
    conn.close()
    return str(path)


class _RecordingConn:
    """Delegating proxy that logs every statement.

    sqlite3.Connection.execute is read-only, so the spy has to wrap rather
    than patch.
    """

    def __init__(self, conn, log: list[str]):
        self._conn = conn
        self._log = log

    def execute(self, sql, params=None):
        self._log.append(str(sql))
        return self._conn.execute(sql, params) if params is not None \
            else self._conn.execute(sql)

    def __getattr__(self, name):
        return getattr(self._conn, name)


def spy_statements(monkeypatch) -> list[str]:
    """Record every statement issued through db_connection.get_connection()."""
    log: list[str] = []
    real_get = dbc.get_connection
    monkeypatch.setattr(
        dbc, "get_connection",
        lambda *a, **kw: _RecordingConn(real_get(*a, **kw), log),
    )
    return log


def sqlite_columns(path: str, table: str) -> set[str]:
    """Read columns directly, bypassing any active spy."""
    conn = sqlite3.connect(path)
    try:
        return {str(r[1]).lower()
                for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    finally:
        conn.close()


# ── ensure_columns: the no-DDL-when-nothing-missing property ──────────────

class TestEnsureColumnsEmitsNoDDL:
    def test_no_alter_when_every_column_present(self, sqlite_db, monkeypatch):
        """The incident case: all columns exist → not one ALTER statement."""
        executed = spy_statements(monkeypatch)
        added = dbc.ensure_columns(
            "markets", [("condition_id", "TEXT"), ("slug", "TEXT")], db_path=sqlite_db
        )

        assert added == []
        assert not [s for s in executed if "ALTER" in s.upper()], executed

    def test_adds_only_the_missing_column(self, sqlite_db):
        added = dbc.ensure_columns(
            "markets",
            [("slug", "TEXT"), ("event_slug", "TEXT"), ("neg_risk", "INTEGER")],
            db_path=sqlite_db,
        )
        assert sorted(added) == ["event_slug", "neg_risk"]

        cols = dbc.table_columns("markets", db_path=sqlite_db)
        assert {"condition_id", "slug", "event_slug", "neg_risk"} <= cols

    def test_second_call_is_a_no_op(self, sqlite_db):
        dbc.ensure_columns("markets", [("event_slug", "TEXT")], db_path=sqlite_db)
        assert dbc.ensure_columns(
            "markets", [("event_slug", "TEXT")], db_path=sqlite_db
        ) == []

    def test_cache_short_circuits_even_the_catalog_read(self, sqlite_db, monkeypatch):
        """Steady state costs zero queries, not just zero DDL."""
        dbc.ensure_columns("markets", [("slug", "TEXT")], db_path=sqlite_db)

        def boom(*a, **kw):
            raise AssertionError("cached ensure_columns must not touch the DB")

        monkeypatch.setattr(dbc, "get_connection", boom)
        assert dbc.ensure_columns("markets", [("slug", "TEXT")], db_path=sqlite_db) == []

    def test_missing_table_is_skipped_and_not_cached(self, sqlite_db):
        """A table the caller's CREATE TABLE will define must stay re-checkable.

        markets_table.ensure_schema() runs ensure_columns BEFORE its
        CREATE TABLE (the event_slug index needs the column), so on a fresh
        deployment the table does not exist yet.
        """
        assert dbc.ensure_columns("not_yet", [("c", "TEXT")], db_path=sqlite_db) == []
        assert not dbc._ENSURED_COLUMNS_CACHE

        conn = sqlite3.connect(sqlite_db)
        conn.execute("CREATE TABLE not_yet (id INTEGER)")
        conn.commit()
        conn.close()

        assert dbc.ensure_columns("not_yet", [("c", "TEXT")], db_path=sqlite_db) == ["c"]

    def test_accepts_both_spec_forms(self, sqlite_db):
        """Call sites use "col TYPE" strings and (col, type) pairs alike."""
        added = dbc.ensure_columns(
            "markets", ["a TEXT", ("b", "INTEGER"), "c DOUBLE PRECISION"],
            db_path=sqlite_db,
        )
        assert sorted(added) == ["a", "b", "c"]

    def test_column_matching_is_case_insensitive(self, sqlite_db):
        """Postgres folds unquoted identifiers to lower case."""
        assert dbc.ensure_columns(
            "markets", [("SLUG", "TEXT")], db_path=sqlite_db
        ) == []


# ── The lock_timeout guard on the Postgres path ───────────────────────────

class _FakeCur:
    """Minimal psycopg-cursor stand-in that records statements."""

    def __init__(self):
        self.statements: list[str] = []
        self.description = None
        self.rowcount = 0

    def execute(self, sql, params=None):
        self.statements.append(str(sql))
        return self

    def fetchone(self):
        return None

    def fetchall(self):
        return []


def _run(sql: str) -> list[str]:
    cur = _FakeCur()
    dbc._PgCursorWrapper(cur, {}).execute(sql)
    return cur.statements


class TestDDLLockTimeoutGuard:
    def test_alter_table_is_wrapped_in_lock_timeout(self):
        stmts = _run("ALTER TABLE markets ADD COLUMN event_slug TEXT")
        assert stmts[0] == f"SET lock_timeout = '{dbc.DDL_LOCK_TIMEOUT_MS}ms'"
        assert "ALTER TABLE" in stmts[1]
        assert stmts[-1] == "SET lock_timeout = DEFAULT"

    def test_timeout_is_reset_even_when_the_ddl_fails(self):
        """A lock timeout must not leave the pooled connection altered."""
        cur = _FakeCur()

        def failing(sql, params=None):
            cur.statements.append(str(sql))
            if "ALTER" in str(sql).upper():
                raise RuntimeError("canceling statement due to lock timeout")
            return cur

        cur.execute = failing
        with pytest.raises(RuntimeError):
            dbc._PgCursorWrapper(cur, {}).execute("ALTER TABLE markets ADD COLUMN x TEXT")
        assert cur.statements[-1] == "SET lock_timeout = DEFAULT"

    @pytest.mark.parametrize("sql", [
        "ALTER TABLE live_trades ADD COLUMN outcome TEXT",
        "DROP TABLE wallet_copy_graph",
        "DROP INDEX idx_markets_event",
        "TRUNCATE firehose_fills",
        "CREATE INDEX idx_markets_event ON markets(event_slug)",
        "CREATE UNIQUE INDEX ux_x ON t(a)",
        "REINDEX TABLE markets",
    ])
    def test_lock_taking_ddl_is_guarded(self, sql):
        assert _run(sql)[0].startswith("SET lock_timeout")

    @pytest.mark.parametrize("sql", [
        "SELECT yes_token_id FROM markets WHERE condition_id = 'x'",
        "INSERT INTO markets (condition_id) VALUES ('x')",
        "UPDATE markets SET slug = 'y'",
        "CREATE TABLE IF NOT EXISTS markets (a TEXT)",
    ])
    def test_non_locking_statements_are_untouched(self, sql):
        """CREATE TABLE IF NOT EXISTS takes no lock on an existing relation."""
        stmts = _run(sql)
        assert not any("lock_timeout" in s for s in stmts), stmts

    def test_guard_can_be_disabled(self, monkeypatch):
        monkeypatch.setattr(dbc, "DDL_LOCK_TIMEOUT_MS", 0)
        stmts = _run("ALTER TABLE markets ADD COLUMN x TEXT")
        assert not any("lock_timeout" in s for s in stmts)

    def test_ddl_still_runs_when_the_timeout_cannot_be_armed(self):
        """Failing to SET must not stop the DDL itself."""
        cur = _FakeCur()

        def selective(sql, params=None):
            if "lock_timeout" in str(sql):
                raise RuntimeError("cannot SET here")
            cur.statements.append(str(sql))
            return cur

        cur.execute = selective
        dbc._PgCursorWrapper(cur, {}).execute("ALTER TABLE markets ADD COLUMN x TEXT")
        assert cur.statements == ["ALTER TABLE markets ADD COLUMN x TEXT"]


# ── Call-site regressions ─────────────────────────────────────────────────

class TestCallSitesEmitNoDDLWhenCurrent:
    def test_markets_ensure_schema_issues_no_alter(self, tmp_path, monkeypatch):
        """markets_table.ensure_schema() ran two no-op ALTERs on every call.

        Those two statements are what queued for 303s behind the pg_dump.
        """
        from trading_platform.polymarket import markets_table

        monkeypatch.setattr(dbc, "DB_BACKEND", "sqlite")
        path = str(tmp_path / "m.db")
        monkeypatch.setattr(dbc, "DEFAULT_DB_PATH", path)

        markets_table.ensure_schema()          # creates the table
        dbc._ENSURED_COLUMNS_CACHE.clear()     # forget, to force a real re-check

        executed = spy_statements(monkeypatch)
        markets_table.ensure_schema()

        assert not [s for s in executed if "ALTER" in s.upper()], executed
        assert {"event_slug", "neg_risk"} <= sqlite_columns(path, "markets")

    def test_live_trades_ensure_issues_no_alter(self, tmp_path, monkeypatch):
        """The 33-ALTER loop: every column is in the CREATE TABLE or added once."""
        from trading_platform.polymarket.polymarket_live_executor import (
            PolymarketLiveExecutor,
        )

        monkeypatch.setattr(dbc, "DB_BACKEND", "sqlite")
        path = str(tmp_path / "lt.db")

        ex = object.__new__(PolymarketLiveExecutor)
        ex._db_path = path
        ex._ensure_live_trades_table()
        dbc._ENSURED_COLUMNS_CACHE.clear()

        executed = spy_statements(monkeypatch)
        ex._ensure_live_trades_table()

        assert not [s for s in executed if "ALTER" in s.upper()], executed

        cols = sqlite_columns(path, "live_trades")
        declared = {
            spec.split()[0].lower()
            for spec in PolymarketLiveExecutor._LIVE_TRADES_COLUMNS
        }
        assert declared <= cols, sorted(declared - cols)
