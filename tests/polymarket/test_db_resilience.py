"""
Tests for the DB resilience layer (db_connection.py).

Covers the retry wrapper, the context manager, and the WAL-mode
default that fixed the multi-process locking issue. Mocks `sqlite3.connect`
or uses a real tmp DB so we don't touch the production wallet DB.
"""
from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from trading_platform.polymarket.db_connection import (
    get_connection,
    db,
    execute_with_retry,
    commit_with_retry,
    RETRY_MAX_ATTEMPTS,
)


def _bootstrap(tmp_path: Path) -> Path:
    p = tmp_path / "wi.db"
    conn = sqlite3.connect(str(p))
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
    conn.commit()
    conn.close()
    return p


# ── get_connection ──────────────────────────────────────────────────────────


class TestGetConnection:
    def test_returns_wal_mode_on_fresh_db(self, tmp_path):
        db_path = _bootstrap(tmp_path)
        conn = get_connection(str(db_path))
        try:
            mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
            assert mode == "wal"
        finally:
            conn.close()

    def test_busy_timeout_set(self, tmp_path):
        db_path = _bootstrap(tmp_path)
        conn = get_connection(str(db_path))
        try:
            ms = conn.execute("PRAGMA busy_timeout").fetchone()[0]
            assert ms == 60000
        finally:
            conn.close()

    def test_foreign_keys_on(self, tmp_path):
        db_path = _bootstrap(tmp_path)
        conn = get_connection(str(db_path))
        try:
            assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        finally:
            conn.close()


# ── context manager ─────────────────────────────────────────────────────────


class TestContextManager:
    def test_commits_on_clean_exit(self, tmp_path):
        db_path = _bootstrap(tmp_path)
        with db(str(db_path)) as conn:
            conn.execute("INSERT INTO t (v) VALUES (?)", (42,))
        # Re-open and verify the row landed
        conn2 = get_connection(str(db_path))
        try:
            n = conn2.execute("SELECT COUNT(*) FROM t WHERE v = 42").fetchone()[0]
            assert n == 1
        finally:
            conn2.close()

    def test_rollback_on_exception(self, tmp_path):
        db_path = _bootstrap(tmp_path)
        with pytest.raises(ValueError):
            with db(str(db_path)) as conn:
                conn.execute("INSERT INTO t (v) VALUES (?)", (99,))
                raise ValueError("trigger rollback")
        # Re-open and verify nothing landed
        conn2 = get_connection(str(db_path))
        try:
            n = conn2.execute("SELECT COUNT(*) FROM t WHERE v = 99").fetchone()[0]
            assert n == 0
        finally:
            conn2.close()


# ── retry wrapper ───────────────────────────────────────────────────────────


class _LockedThenOK:
    """Mock connection: raises 'database is locked' N times then succeeds."""

    def __init__(self, fail_count: int):
        self.fail_count = fail_count
        self.attempts = 0

    def execute(self, sql, params=None):
        self.attempts += 1
        if self.attempts <= self.fail_count:
            raise sqlite3.OperationalError("database is locked")
        return MagicMock()  # success — pretend cursor

    def commit(self):
        self.attempts += 1
        if self.attempts <= self.fail_count:
            raise sqlite3.OperationalError("database is locked")


class TestRetryWrapper:
    def test_retry_succeeds_on_third_try(self):
        conn = _LockedThenOK(fail_count=2)
        # Use a tiny base_delay so the test runs in ms not seconds
        cur = execute_with_retry(conn, "SELECT 1", base_delay=0.001)
        assert cur is not None
        assert conn.attempts == 3

    def test_retry_exhausts_then_raises(self):
        conn = _LockedThenOK(fail_count=999)
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            execute_with_retry(conn, "SELECT 1", base_delay=0.001)
        assert conn.attempts == RETRY_MAX_ATTEMPTS

    def test_non_lock_error_raises_immediately(self):
        conn = MagicMock()
        conn.execute.side_effect = sqlite3.OperationalError("syntax error")
        with pytest.raises(sqlite3.OperationalError, match="syntax"):
            execute_with_retry(conn, "MALFORMED", base_delay=0.001)
        assert conn.execute.call_count == 1  # no retry on real errors

    def test_commit_with_retry_succeeds(self):
        conn = _LockedThenOK(fail_count=1)
        commit_with_retry(conn, base_delay=0.001)
        assert conn.attempts == 2

    def test_commit_with_retry_exhausts(self):
        conn = _LockedThenOK(fail_count=999)
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            commit_with_retry(conn, base_delay=0.001)
        assert conn.attempts == RETRY_MAX_ATTEMPTS


# ── round-trip: real WAL DB with concurrent writes ──────────────────────────


class TestConcurrentWalDB:
    def test_reader_does_not_block_writer(self, tmp_path):
        # WAL allows one writer + many concurrent readers (it does NOT
        # allow two simultaneous writers — that's still serialized).
        db_path = _bootstrap(tmp_path)
        # Pre-load a row so the reader has something to read
        with db(str(db_path)) as c:
            c.execute("INSERT INTO t (v) VALUES (?)", (1,))

        # Open a long-lived reader, then write from a different conn
        reader = get_connection(str(db_path))
        try:
            n = reader.execute("SELECT COUNT(*) FROM t").fetchone()[0]
            assert n == 1
            # Now write from a separate conn while the reader is still open
            with db(str(db_path)) as writer:
                writer.execute("INSERT INTO t (v) VALUES (?)", (2,))
            # Reader should still work without "is locked"
            n2 = reader.execute("SELECT COUNT(*) FROM t").fetchone()[0]
            assert n2 == 2
        finally:
            reader.close()
