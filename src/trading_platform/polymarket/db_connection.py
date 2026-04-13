"""
Standard SQLite connection helper for Polymarket modules.

Uses ``journal_mode=DELETE`` (rollback journal) rather than WAL. WAL
mode was tried twice and broke both times on the Docker Desktop Windows
bind-mount: the ``-shm``/``-wal`` sidecars don't survive cross-container
opens cleanly, and a single orphaned handle from live-collect takes an
exclusive lock that blocks every short-lived scheduler task with
"unable to open database file". See reports/system_diagnostic_2026-04-11.md
for the full post-mortem. DELETE mode serializes writers through the
existing ``busy_timeout`` path, which is fine for this workload — the
only hot writer is live-collect, everything else writes in short bursts.

Generic usage::

    from trading_platform.polymarket.db_connection import get_connection, db, execute_with_retry
    conn = get_connection()                          # one-shot
    with db() as conn: ...                            # context manager (auto-commit + close)
    cur = execute_with_retry(conn, "SELECT ...")     # retries on lock

Pass an explicit ``db_path`` whenever the caller knows it. The
``WALLET_INTELLIGENCE_DB`` env var can override the default for ad-hoc
container deployments.
"""
from __future__ import annotations

import logging
import os
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = os.environ.get(
    "WALLET_INTELLIGENCE_DB",
    "data/polymarket/wallet_intelligence.db",
)

# Tunables — agreed defaults across the stack.
DEFAULT_TIMEOUT_SEC = 60         # Python sqlite3 connect() timeout
DEFAULT_BUSY_TIMEOUT_MS = 60000  # SQLite-side busy_timeout
RETRY_MAX_ATTEMPTS = 5
RETRY_BASE_DELAY = 1.0


def get_connection(
    db_path: str | Path | None = None,
    *,
    check_same_thread: bool = True,
    row_factory: bool = False,
) -> sqlite3.Connection:
    """Open a SQLite connection with DELETE journal + busy_timeout pragmas.

    The connection is returned with:

    * ``journal_mode = DELETE`` — rollback journal; no -wal/-shm sidecars
      on the Windows bind mount, so fresh opens always succeed
    * ``busy_timeout = 60000``  — wait up to 60s for a lock
    * ``synchronous = NORMAL``  — faster commits, still durable across crashes
    * ``foreign_keys = ON``     — match the schema migrations

    PRAGMA failures (e.g. another process is checkpointing) are
    swallowed — the connection remains usable on whatever the
    persisted journal_mode is. Real schema bugs still raise.
    """
    path = str(db_path) if db_path is not None else DEFAULT_DB_PATH
    conn = sqlite3.connect(path, timeout=DEFAULT_TIMEOUT_SEC, check_same_thread=check_same_thread)
    for stmt in (
        f"PRAGMA busy_timeout={DEFAULT_BUSY_TIMEOUT_MS}",
        "PRAGMA journal_mode=DELETE",
        "PRAGMA synchronous=NORMAL",
        "PRAGMA foreign_keys=ON",
    ):
        try:
            conn.execute(stmt)
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower():
                raise
            logger.debug("PRAGMA skipped (db locked): %s", stmt)
    if row_factory:
        conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def db(db_path: str | Path | None = None, *, row_factory: bool = False):
    """Context manager that opens, yields, commits, and closes a connection.

    Use this in any code path that does a small number of writes:

        with db() as conn:
            conn.execute("INSERT INTO ...", (...))
            conn.execute("UPDATE ...", (...))
        # auto-commit on clean exit, auto-rollback on exception, always closes
    """
    conn = get_connection(db_path, row_factory=row_factory)
    try:
        yield conn
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass


def execute_with_retry(
    conn: sqlite3.Connection,
    sql: str,
    params: tuple | list | dict | None = None,
    *,
    max_retries: int = RETRY_MAX_ATTEMPTS,
    base_delay: float = RETRY_BASE_DELAY,
) -> sqlite3.Cursor:
    """Execute a single SQL statement with exponential-backoff retry on lock.

    Use this on the hot write paths (live_collector, paper_executor) where
    a lock contention should NOT crash the caller. Logs every retry at
    WARNING level so contention shows up in the operator's view.
    """
    last_exc: Exception | None = None
    for attempt in range(max_retries):
        try:
            if params is None:
                return conn.execute(sql)
            return conn.execute(sql, params)
        except sqlite3.OperationalError as exc:
            last_exc = exc
            msg = str(exc).lower()
            if "locked" not in msg and "busy" not in msg:
                raise
            if attempt == max_retries - 1:
                break
            delay = base_delay * (2 ** attempt)
            logger.warning(
                "[DB_RETRY] locked, attempt %d/%d, waiting %.1fs: %s",
                attempt + 1, max_retries, delay, sql[:60].replace("\n", " "),
            )
            time.sleep(delay)
    # Re-raise so callers can fail fast or queue.
    assert last_exc is not None
    raise last_exc


def commit_with_retry(
    conn: sqlite3.Connection,
    *,
    max_retries: int = RETRY_MAX_ATTEMPTS,
    base_delay: float = RETRY_BASE_DELAY,
) -> None:
    """Commit with retry on lock — pairs with execute_with_retry."""
    last_exc: Exception | None = None
    for attempt in range(max_retries):
        try:
            conn.commit()
            return
        except sqlite3.OperationalError as exc:
            last_exc = exc
            if "locked" not in str(exc).lower() and "busy" not in str(exc).lower():
                raise
            if attempt == max_retries - 1:
                break
            delay = base_delay * (2 ** attempt)
            logger.warning(
                "[DB_RETRY] commit locked, attempt %d/%d, waiting %.1fs",
                attempt + 1, max_retries, delay,
            )
            time.sleep(delay)
    assert last_exc is not None
    raise last_exc


def force_journal_mode_wal(db_path: str | Path) -> str:
    """One-shot helper: open the DB and force WAL mode persistently."""
    conn = sqlite3.connect(str(db_path), timeout=DEFAULT_TIMEOUT_SEC)
    try:
        result = conn.execute("PRAGMA journal_mode=WAL").fetchone()
        conn.commit()
        return result[0] if result else ""
    finally:
        conn.close()


def force_journal_mode_delete(db_path: str | Path) -> str:
    """One-shot helper: force DELETE mode (legacy migration helper)."""
    conn = sqlite3.connect(str(db_path), timeout=DEFAULT_TIMEOUT_SEC)
    try:
        result = conn.execute("PRAGMA journal_mode=DELETE").fetchone()
        conn.commit()
        return result[0] if result else ""
    finally:
        conn.close()
