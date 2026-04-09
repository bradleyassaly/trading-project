"""
Standard SQLite connection helper for Polymarket modules.

Uses ``journal_mode=DELETE`` instead of WAL because WAL requires
shared-memory mmap, which does not work on NTFS bind-mounts inside
WSL2 Docker containers (the api/scheduler/watchdog stack). DELETE
mode works on every filesystem we run on (host Windows, host Linux,
WSL2 bind-mount, ext4 volume) at the cost of slightly more contention
on the writer — acceptable since this codebase has a single writer
(scheduler) and many readers (API + frontend).

Generic usage::

    from trading_platform.polymarket.db_connection import get_connection
    conn = get_connection("data/polymarket/wallet_intelligence.db")

Pass an explicit ``db_path`` whenever the caller knows it. The
``WALLET_INTELLIGENCE_DB`` env var can override the default for ad-hoc
container deployments.
"""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path

DEFAULT_DB_PATH = os.environ.get(
    "WALLET_INTELLIGENCE_DB",
    "data/polymarket/wallet_intelligence.db",
)


def get_connection(
    db_path: str | Path | None = None,
    *,
    check_same_thread: bool = True,
    row_factory: bool = False,
) -> sqlite3.Connection:
    """Open a SQLite connection with bind-mount-safe pragmas.

    Sets:
    * ``journal_mode=DELETE`` — works on NTFS bind-mounts (WAL does not).
    * ``busy_timeout=30000`` — 30s before SQLITE_BUSY is raised.
    * ``foreign_keys=ON``  — match the schema migrations.
    """
    path = str(db_path) if db_path is not None else DEFAULT_DB_PATH
    conn = sqlite3.connect(path, timeout=30, check_same_thread=check_same_thread)
    conn.execute("PRAGMA journal_mode=DELETE")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    if row_factory:
        conn.row_factory = sqlite3.Row
    return conn


def force_journal_mode_delete(db_path: str | Path) -> str:
    """One-shot helper: open the DB and force DELETE mode persistently.

    Returns the resulting journal_mode (string). Useful for migration
    scripts and the host-side fix that has to convert an existing WAL
    DB before the Docker containers attempt to read it.
    """
    conn = sqlite3.connect(str(db_path), timeout=30)
    try:
        result = conn.execute("PRAGMA journal_mode=DELETE").fetchone()
        conn.commit()
        return result[0] if result else ""
    finally:
        conn.close()
