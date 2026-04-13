"""
Centralized SQLite connection module for the wallet intelligence DB.

Replaces all bare ``sqlite3.connect`` calls across the codebase with a
single function that handles:

1. **WAL mode** — enables concurrent readers + single writer
2. **Busy timeout** — waits 10s instead of failing immediately on lock
3. **Path resolution** — tries WALLET_DB_PATH env, then standard paths
4. **Retry with backoff** — 3 attempts on OperationalError
5. **Docker NTFS fallback** — copies to /tmp when bind mount fails

Every module that needs the DB should::

    from trading_platform.polymarket.db import connect_wallet_db

    conn = connect_wallet_db()
    rows = conn.execute("SELECT ...").fetchall()
    conn.close()
"""
from __future__ import annotations

import logging
import os
import shutil
import sqlite3
import tempfile
import time
from pathlib import Path

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[3]

# Ordered list of paths to try when connecting to the wallet DB.
_CANDIDATE_PATHS = [
    lambda: os.environ.get("WALLET_DB_PATH"),
    lambda: str(_PROJECT_ROOT / "data" / "polymarket" / "wallet_intelligence.db"),
    lambda: "/app/data/polymarket/wallet_intelligence.db",
    lambda: "data/polymarket/wallet_intelligence.db",
]

# /tmp copy cache: path → (src_mtime, tmp_path)
_TMP_CACHE: dict[str, tuple[float, str]] = {}

MAX_RETRIES = 3
RETRY_DELAY = 1.0  # seconds


def connect_wallet_db(
    path: str | Path | None = None,
    *,
    timeout: int = 30,
    check_same_thread: bool = False,
    readonly: bool = False,
) -> sqlite3.Connection:
    """Connect to the wallet intelligence DB with WAL + retry + NTFS fallback.

    Parameters
    ----------
    path : optional explicit DB path (overrides env/defaults)
    timeout : SQLite timeout in seconds
    check_same_thread : passed to sqlite3.connect
    readonly : if True, don't attempt WAL mode switch

    Returns
    -------
    sqlite3.Connection with WAL mode and busy_timeout set.

    Raises
    ------
    sqlite3.OperationalError if all attempts fail.
    """
    resolved = _resolve_path(path)
    if not resolved:
        raise sqlite3.OperationalError(
            "wallet_intelligence.db not found. Set WALLET_DB_PATH or check data directory."
        )

    last_error: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            conn = _try_connect(resolved, timeout, check_same_thread, readonly)
            return conn
        except sqlite3.OperationalError as exc:
            last_error = exc
            err = str(exc).lower()
            if "unable to open" in err:
                # NTFS bind mount issue — try /tmp copy
                tmp_path = _get_tmp_copy(resolved)
                if tmp_path:
                    try:
                        conn = _try_connect(tmp_path, timeout, check_same_thread, readonly)
                        return conn
                    except sqlite3.OperationalError:
                        pass
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))

    raise sqlite3.OperationalError(
        f"Failed to connect after {MAX_RETRIES} attempts: {last_error}"
    )


def _resolve_path(explicit: str | Path | None = None) -> str | None:
    """Find the first DB path that exists."""
    if explicit:
        p = Path(explicit)
        if p.exists():
            return str(p)
    for candidate_fn in _CANDIDATE_PATHS:
        try:
            p = candidate_fn()
            if p and Path(p).exists():
                return str(p)
        except Exception:
            pass
    return None


def _try_connect(
    path: str,
    timeout: int,
    check_same_thread: bool,
    readonly: bool,
) -> sqlite3.Connection:
    """Open a connection and set pragmas."""
    conn = sqlite3.connect(path, timeout=timeout, check_same_thread=check_same_thread)
    conn.execute("SELECT 1")  # verify the connection works
    conn.execute("PRAGMA busy_timeout=10000")
    if not readonly:
        try:
            conn.execute("PRAGMA journal_mode=DELETE")
        except sqlite3.OperationalError:
            pass  # read-only FS
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _get_tmp_copy(src: str) -> str | None:
    """Copy the DB to /tmp if the source is newer than the cached copy."""
    try:
        src_path = Path(src)
        src_mtime = src_path.stat().st_mtime
        key = src

        cached = _TMP_CACHE.get(key)
        if cached:
            tmp_path = cached[1]
            if Path(tmp_path).exists() and cached[0] >= src_mtime - 30:
                return tmp_path

        tmp_path = str(Path(tempfile.gettempdir()) / f"_wal_{src_path.name}")
        shutil.copy2(src, tmp_path)
        _TMP_CACHE[key] = (src_mtime, tmp_path)
        return tmp_path
    except Exception as exc:
        logger.debug("tmp copy failed: %s", exc)
        return None
