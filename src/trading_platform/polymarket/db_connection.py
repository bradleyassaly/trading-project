"""Database connection helper — PostgreSQL backend with SQLite fallback.

Migrated from SQLite to PostgreSQL on 2026-04-14 after repeated corruption
under concurrent-writer load. Previous SQLite deployment kept hitting
"database disk image is malformed" under Docker bind-mount + multi-process
writes. Postgres's MVCC makes that class of error impossible.

Public API unchanged — all callers keep using:

    from trading_platform.polymarket.db_connection import get_connection, db
    conn = get_connection()
    with db() as conn: ...
    cur = execute_with_retry(conn, "SELECT ...")

Backward-compat details:
  * `?` placeholders in callers are auto-converted to `$N` for Postgres.
    (psycopg v3 uses `%s`, but we wrap to preserve the dominant ? style.)
  * `INSERT OR REPLACE INTO` is the only SQLite-specific upsert pattern
    callers use; we auto-rewrite to Postgres ON CONFLICT semantics
    when the target table's primary key can be inferred.
  * PRAGMA calls become no-ops under Postgres.
  * `.execute` / `.executemany` / `.fetchone` / `.fetchall` preserved.

Env toggle: DB_BACKEND=sqlite keeps the legacy path for rollback / tests.
"""
from __future__ import annotations

import logging
import os
import re
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


# ── Config ────────────────────────────────────────────────────────────────

DEFAULT_DB_PATH = os.environ.get(
    "WALLET_INTELLIGENCE_DB",
    "data/polymarket/wallet_intelligence.db",
)
DB_BACKEND = os.environ.get("DB_BACKEND", "postgres").lower()

PG_HOST = os.environ.get("POSTGRES_HOST", "localhost")
PG_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
PG_USER = os.environ.get("POSTGRES_USER", "polymarket")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "polymarket_dev")
PG_DB = os.environ.get("POSTGRES_DB", "polymarket")

# SQLite legacy tunables (only used when DB_BACKEND=sqlite)
DEFAULT_TIMEOUT_SEC = 60
DEFAULT_BUSY_TIMEOUT_MS = 60000
RETRY_MAX_ATTEMPTS = 5
RETRY_BASE_DELAY = 1.0


# ── Postgres adapter ──────────────────────────────────────────────────────
# The adapter provides a sqlite3-compatible surface so existing callers
# work unchanged (``conn.execute(sql, params)``, ``fetchone``, ``fetchall``,
# ``cursor()``, ``commit()``, ``close()``, ``row_factory`` attribute).

try:
    import psycopg
    from psycopg.rows import tuple_row, dict_row
    _psycopg_available = True
except ImportError:
    _psycopg_available = False

try:
    from psycopg_pool import ConnectionPool
    _psycopg_pool_available = True
except ImportError:
    _psycopg_pool_available = False


_pool_instance = None
_pool_lock = __import__("threading").Lock()

POOL_MIN_SIZE = int(os.environ.get("PG_POOL_MIN_SIZE", "2"))
POOL_MAX_SIZE = int(os.environ.get("PG_POOL_MAX_SIZE", "20"))
POOL_TIMEOUT = float(os.environ.get("PG_POOL_TIMEOUT", "60.0"))
POOL_MAX_IDLE = float(os.environ.get("PG_POOL_MAX_IDLE", "60.0"))
POOL_MAX_LIFETIME = float(os.environ.get("PG_POOL_MAX_LIFETIME", "1800.0"))


def _get_pool():
    """Return a process-wide Postgres connection pool (lazy-init).

    A single pool per process replaces per-call psycopg.connect() which
    was adding 10-50ms of TCP + auth + role-setup overhead to every API
    request. Connections are returned via wrapper.close() → pool.putconn.

    2026-04-24: hardened against silent idle-disconnects after the
    week-long unattended run died at +6h with `terminating connection
    due to idle-session timeout`. Three layers of defense now:
      1. TCP keepalives so a dropped backend surfaces fast.
      2. `check=check_connection` so the pool runs SELECT 1 on each
         getconn() and recycles a dead connection transparently.
      3. `max_idle` + `max_lifetime` so the pool retires conns before
         server-side timeouts fire.
    """
    global _pool_instance
    if _pool_instance is not None:
        return _pool_instance
    with _pool_lock:
        if _pool_instance is not None:
            return _pool_instance
        if not _psycopg_pool_available:
            raise RuntimeError(
                "psycopg_pool not installed. Run: pip install 'psycopg-pool>=3.2'"
            )
        conninfo = (
            f"host={PG_HOST} port={PG_PORT} user={PG_USER} "
            f"password={PG_PASSWORD} dbname={PG_DB} "
            "keepalives=1 keepalives_idle=30 "
            "keepalives_interval=10 keepalives_count=5"
        )
        _pool_instance = ConnectionPool(
            conninfo=conninfo,
            min_size=POOL_MIN_SIZE,
            max_size=POOL_MAX_SIZE,
            kwargs={"autocommit": True},
            open=True,
            timeout=POOL_TIMEOUT,
            max_idle=POOL_MAX_IDLE,
            max_lifetime=POOL_MAX_LIFETIME,
            check=ConnectionPool.check_connection,
        )
        logger.info(
            "[pg_pool] opened min=%d max=%d max_idle=%.0fs max_lifetime=%.0fs host=%s",
            POOL_MIN_SIZE, POOL_MAX_SIZE, POOL_MAX_IDLE, POOL_MAX_LIFETIME, PG_HOST,
        )
        return _pool_instance


_PLACEHOLDER_RE = re.compile(r"\?")
_INSERT_OR_REPLACE_RE = re.compile(r"\bINSERT\s+OR\s+REPLACE\s+INTO\b", re.IGNORECASE)
_INSERT_OR_IGNORE_RE = re.compile(r"\bINSERT\s+OR\s+IGNORE\s+INTO\b", re.IGNORECASE)
_STRFTIME_NOW_RE = re.compile(
    r"strftime\s*\(\s*['\"]%s['\"]\s*,\s*['\"]now['\"](?:\s*,\s*['\"][^'\"]+['\"])*\s*\)",
    re.IGNORECASE,
)
_UNIXEPOCH_RE = re.compile(r"\bunixepoch\s*\(\s*\)", re.IGNORECASE)
# GROUP_CONCAT(col)          → STRING_AGG(col::text, ',')
# GROUP_CONCAT(col, 'sep')   → STRING_AGG(col::text, 'sep')
# GROUP_CONCAT(DISTINCT col) → STRING_AGG(DISTINCT col::text, ',')
_GROUP_CONCAT_RE = re.compile(
    r"\bGROUP_CONCAT\s*\(\s*(DISTINCT\s+)?([^,)]+?)\s*(?:,\s*(['\"][^'\"]*['\"]))?\s*\)",
    re.IGNORECASE,
)


def _convert_create_ddl(sql: str) -> str:
    """Convert SQLite-flavored CREATE TABLE/INDEX to Postgres equivalents.
    Mirrors the migration-tool logic so new modules calling
    ``ensure_schema()`` succeed under Postgres without changes.
    """
    sql = re.sub(
        r"INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT",
        "BIGSERIAL PRIMARY KEY", sql, flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"INTEGER\s+PRIMARY\s+KEY", "BIGINT PRIMARY KEY", sql, flags=re.IGNORECASE,
    )
    sql = re.sub(r"COLLATE\s+\w+", "", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\)\s*(WITHOUT\s+ROWID|STRICT)\s*;?\s*$", ")", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bINTEGER\b", "BIGINT", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bREAL\b", "DOUBLE PRECISION", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bBLOB\b", "BYTEA", sql, flags=re.IGNORECASE)
    sql = re.sub(
        r"unixepoch\s*\(\s*('now')?\s*\)",
        "(EXTRACT(EPOCH FROM NOW())::BIGINT)", sql, flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"strftime\s*\(\s*['\"]\%s['\"]\s*,\s*['\"]now['\"]\s*\)",
        "(EXTRACT(EPOCH FROM NOW())::BIGINT)", sql, flags=re.IGNORECASE,
    )
    return sql


def _convert_sqlite_sql(sql: str, table_pk_cache: dict | None = None) -> str:
    """Convert SQLite-flavored SQL to Postgres-compatible.

    - `?` placeholders → `%s`
    - `strftime('%s','now','-N days')` → time arithmetic
    - `unixepoch()` → `EXTRACT(EPOCH FROM NOW())::BIGINT`
    - `INSERT OR REPLACE INTO foo(...)` → `INSERT INTO foo(...) ON CONFLICT DO UPDATE ...`
      (only works when the target PK is known at runtime; otherwise leaves
      the statement unchanged and relies on the caller to have handled it)
    """
    # Time expressions: handle these BEFORE placeholder conversion because
    # the rewrites may introduce new strings.
    def _strftime_sub(m):
        # Grab optional modifier group like ',''-30 days'''
        raw = m.group(0)
        # Extract a '-N days' modifier if present
        mod = re.search(r",\s*['\"]([+-]?\d+\s+\w+)['\"]", raw)
        if mod:
            return f"(EXTRACT(EPOCH FROM (NOW() + INTERVAL '{mod.group(1)}'))::BIGINT)"
        return "(EXTRACT(EPOCH FROM NOW())::BIGINT)"

    sql = _STRFTIME_NOW_RE.sub(_strftime_sub, sql)
    sql = _UNIXEPOCH_RE.sub("(EXTRACT(EPOCH FROM NOW())::BIGINT)", sql)

    def _gc_sub(m):
        distinct = (m.group(1) or "").strip()
        expr = m.group(2).strip()
        sep = m.group(3) or "','"
        return f"STRING_AGG({distinct} ({expr})::text, {sep})"
    sql = _GROUP_CONCAT_RE.sub(_gc_sub, sql)

    # `INSERT OR IGNORE INTO` → `INSERT INTO … ON CONFLICT DO NOTHING`
    if _INSERT_OR_IGNORE_RE.search(sql):
        sql = _INSERT_OR_IGNORE_RE.sub("INSERT INTO", sql)
        # Append ON CONFLICT DO NOTHING if not already present
        if not re.search(r"\bON\s+CONFLICT\b", sql, flags=re.IGNORECASE):
            sql = sql.rstrip(";") + " ON CONFLICT DO NOTHING"

    # `INSERT OR REPLACE` — very common in our codebase.
    # Translate to INSERT ... ON CONFLICT DO UPDATE using the PK detected
    # at runtime via a cached lookup.
    if _INSERT_OR_REPLACE_RE.search(sql):
        sql = _rewrite_insert_or_replace(sql, table_pk_cache)

    # `?` → `%s`. Cheap and safe as long as no `?` appears inside quoted
    # strings in the SQL. Our codebase doesn't use `?` in string literals.
    sql = _PLACEHOLDER_RE.sub("%s", sql)
    return sql


def _rewrite_insert_or_replace(sql: str, pk_cache: dict | None) -> str:
    """INSERT OR REPLACE INTO foo (a,b,c) VALUES (?,?,?) →
       INSERT INTO foo (a,b,c) VALUES (?,?,?) ON CONFLICT (pk) DO UPDATE
         SET a=EXCLUDED.a, b=EXCLUDED.b, c=EXCLUDED.c
    """
    # Match: INSERT OR REPLACE INTO <table> (col1, col2, ...) VALUES ...
    m = re.search(
        r"INSERT\s+OR\s+REPLACE\s+INTO\s+['\"]?(\w+)['\"]?\s*\(\s*([^)]+)\s*\)",
        sql, flags=re.IGNORECASE,
    )
    if not m:
        return sql
    table, cols_str = m.group(1), m.group(2)
    cols = [c.strip().strip('"') for c in cols_str.split(",")]
    pk_cols = None
    if pk_cache and table in pk_cache:
        pk_cols = pk_cache[table]
    if not pk_cols:
        # Ask Postgres what the PK is, cache
        try:
            with _pg_raw_conn() as _c:
                pk_cols = _lookup_pk(_c, table)
            if pk_cache is not None:
                pk_cache[table] = pk_cols
        except Exception:
            return sql  # give up; caller deals with it
    if not pk_cols:
        return sql

    new_sql = _INSERT_OR_REPLACE_RE.sub("INSERT INTO", sql)
    # Append ON CONFLICT clause (only if not already present)
    if re.search(r"\bON\s+CONFLICT\b", new_sql, flags=re.IGNORECASE):
        return new_sql
    non_pk_cols = [c for c in cols if c not in pk_cols]
    set_clause = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in non_pk_cols)
    conflict_cols = ", ".join(f'"{c}"' for c in pk_cols)
    if non_pk_cols:
        new_sql = f"{new_sql} ON CONFLICT ({conflict_cols}) DO UPDATE SET {set_clause}"
    else:
        new_sql = f"{new_sql} ON CONFLICT ({conflict_cols}) DO NOTHING"
    return new_sql


def _pg_raw_conn():
    """Underlying psycopg connection, for internal uses only."""
    return psycopg.connect(
        host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD,
        dbname=PG_DB, autocommit=False,
    )


def _lookup_pk(pg_conn, table: str) -> list[str]:
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT a.attname FROM pg_index i "
            "JOIN pg_attribute a ON a.attrelid = i.indrelid "
            "  AND a.attnum = ANY(i.indkey) "
            "WHERE i.indrelid = %s::regclass AND i.indisprimary",
            (table,),
        )
        return [r[0] for r in cur.fetchall()]


class _PgCursorWrapper:
    """Mimics sqlite3.Cursor. Auto-rewrites SQL on execute/executemany.

    Tracks rowcount + lastrowid; returns tuples by default (set
    row_factory='dict' for dict rows)."""

    def __init__(self, pg_cur, rewrite_cache: dict, row_factory=None):
        self._cur = pg_cur
        self._rewrite_cache = rewrite_cache
        self._row_factory = row_factory
        self._last_rewritten = None

    @property
    def description(self):
        return self._cur.description

    @property
    def rowcount(self):
        return self._cur.rowcount

    @property
    def lastrowid(self):
        # psycopg doesn't auto-populate lastrowid; callers needing it must
        # use RETURNING id.
        return None

    def execute(self, sql, params=None):
        # PRAGMA statements: most no-op under Postgres. One is special —
        # `PRAGMA table_info(tbl)` is used widely in our codebase to
        # enumerate columns; translate to information_schema.columns.
        stripped = sql.lstrip()
        if stripped[:6].upper() == "PRAGMA":
            m = re.match(
                r"PRAGMA\s+table_info\s*\(\s*['\"]?(\w+)['\"]?\s*\)",
                stripped, flags=re.IGNORECASE,
            )
            if m:
                table = m.group(1)
                self._cur.execute(
                    "SELECT ordinal_position - 1 AS cid, column_name AS name, "
                    "data_type AS type, "
                    "CASE WHEN is_nullable = 'NO' THEN 1 ELSE 0 END AS notnull, "
                    "column_default AS dflt_value, "
                    "0 AS pk "
                    "FROM information_schema.columns "
                    "WHERE table_schema = 'public' AND table_name = %s "
                    "ORDER BY ordinal_position",
                    (table,),
                )
                return self
            # Other PRAGMAs are pure no-ops on postgres (busy_timeout,
            # journal_mode, synchronous, foreign_keys, integrity_check…)
            return self
        self._last_rewritten = _convert_sqlite_sql(sql, self._rewrite_cache)
        # CREATE TABLE / CREATE INDEX from legacy code uses SQLite type
        # keywords — route through the column-type converter so new
        # modules can still call ensure_schema() under Postgres.
        if re.match(r"\s*CREATE\s+(TABLE|INDEX|UNIQUE\s+INDEX)", self._last_rewritten, re.IGNORECASE):
            self._last_rewritten = _convert_create_ddl(self._last_rewritten)
        try:
            if params is None:
                self._cur.execute(self._last_rewritten)
            else:
                self._cur.execute(self._last_rewritten, params)
        except psycopg.errors.InFailedSqlTransaction:
            raise
        return self

    def executemany(self, sql, param_seq):
        self._last_rewritten = _convert_sqlite_sql(sql, self._rewrite_cache)
        self._cur.executemany(self._last_rewritten, list(param_seq))
        return self

    def fetchone(self):
        return self._cur.fetchone()

    def fetchall(self):
        return self._cur.fetchall()

    def fetchmany(self, size=None):
        return self._cur.fetchmany(size)

    def __iter__(self):
        return iter(self._cur)

    def close(self):
        self._cur.close()


class _PgConnectionWrapper:
    """sqlite3.Connection-like wrapper around psycopg.Connection.
    When `_pool` is provided, close() returns the connection to the pool
    instead of actually closing it. The wrapper still presents a
    sqlite3.Connection-like surface to callers.
    """

    def __init__(self, pg_conn, row_factory=None, _pool=None):
        self._conn = pg_conn
        self._rewrite_cache: dict = {}
        self.row_factory = row_factory  # attr exists for compat; sqlite3.Row becomes dict rows
        self._pool = _pool

    def cursor(self):
        kw = {}
        if self.row_factory is not None and hasattr(sqlite3, "Row") and self.row_factory is sqlite3.Row:
            kw["row_factory"] = dict_row
        cur = self._conn.cursor(**kw) if kw else self._conn.cursor()
        return _PgCursorWrapper(cur, self._rewrite_cache, row_factory=self.row_factory)

    def execute(self, sql, params=None):
        cur = self.cursor()
        return cur.execute(sql, params)

    def executemany(self, sql, param_seq):
        cur = self.cursor()
        return cur.executemany(sql, param_seq)

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def executescript(self, script: str):
        """Multi-statement SQLite shim. Under Postgres, schema is already
        migrated via migrate_sqlite_to_postgres.py — legacy SCHEMA blobs
        from WalletDB init would re-declare SQLite-flavored DDL which
        Postgres doesn't accept. Silently no-op; schema is authoritative
        in Postgres. Callers needing ad-hoc DDL should use conn.execute.
        """
        logger.debug("executescript() is a no-op under Postgres backend (schema is migrated authoritatively)")
        return self

    def close(self):
        # Return the underlying connection to the pool rather than closing
        # it outright — keeps the TCP + auth setup warm across API calls.
        try:
            if self._pool is not None:
                self._pool.putconn(self._conn)
            else:
                self._conn.close()
        except Exception:
            pass

    # ── SQLite-compat no-ops ────────────────────────────────────────────
    def create_function(self, *a, **kw):
        logger.debug("create_function() is a no-op under Postgres backend")

    # ── SQLite-specific lock emulation (match WalletDB expectation) ────
    # Some legacy code uses `with self._lock:` — this doesn't need to be
    # a DB-side lock; a thread lock suffices. WalletDB provides its own
    # _lock attribute separately, so this connection doesn't need one.


def get_connection(
    db_path: str | Path | None = None,
    *,
    check_same_thread: bool = True,
    row_factory: bool = False,
) -> Any:
    """Open a connection. Returns a wrapper that looks like sqlite3.Connection.

    Postgres is the only supported backend post-cutover. The DB_BACKEND=sqlite
    fallback was removed to eliminate split-brain risk — two writers to two
    databases was the root cause of multiple data-drift incidents.
    """
    if not _psycopg_available:
        raise RuntimeError(
            "psycopg is not installed. Run: pip install 'psycopg[binary]>=3.1'"
        )
    # Pooled connection. See _get_pool() for why pooling matters — prior
    # per-call psycopg.connect added 10-50ms overhead per API endpoint.
    pg = _get_pool().getconn()
    rf = sqlite3.Row if row_factory else None
    return _PgConnectionWrapper(pg, row_factory=rf, _pool=_get_pool())


def _get_sqlite_connection(
    db_path: str | Path | None,
    *,
    check_same_thread: bool,
    row_factory: bool,
) -> sqlite3.Connection:
    """Legacy SQLite path — unused post-cutover but retained for tests."""
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
    """Context manager: open, yield, commit on clean exit, rollback on error, close.

    Under Postgres, temporarily disables autocommit inside the block so
    the whole block is one transaction. Mirrors the SQLite semantics
    callers expect.
    """
    conn = get_connection(db_path, row_factory=row_factory)
    # For the Postgres wrapper, scope a transaction by toggling autocommit.
    raw = getattr(conn, "_conn", None)
    was_autocommit = getattr(raw, "autocommit", None)
    if raw is not None and was_autocommit is True:
        raw.autocommit = False
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
        if raw is not None and was_autocommit is True:
            try:
                raw.autocommit = True
            except Exception:
                pass
        try:
            conn.close()
        except Exception:
            pass


def execute_with_retry(
    conn, sql, params=None, *,
    max_retries: int = RETRY_MAX_ATTEMPTS,
    base_delay: float = RETRY_BASE_DELAY,
):
    """Execute with retry on transient lock/busy errors.

    Under Postgres these errors are vanishingly rare (MVCC) so this is
    effectively a pass-through except for deadlocks. Retained for API
    compatibility with all the call-sites that import it.
    """
    last_exc = None
    for attempt in range(max_retries):
        try:
            if params is None:
                return conn.execute(sql)
            return conn.execute(sql, params)
        except Exception as exc:  # psycopg.OperationalError or sqlite3.OperationalError
            last_exc = exc
            msg = str(exc).lower()
            if "locked" not in msg and "busy" not in msg and "deadlock" not in msg:
                raise
            if attempt == max_retries - 1:
                break
            delay = base_delay * (2 ** attempt)
            logger.warning(
                "[DB_RETRY] transient error, attempt %d/%d, waiting %.1fs: %s",
                attempt + 1, max_retries, delay, msg[:100],
            )
            time.sleep(delay)
    assert last_exc is not None
    raise last_exc


def commit_with_retry(conn, **_kwargs) -> None:
    """Commit with no-op retry — Postgres commit either succeeds or raises."""
    conn.commit()


def force_journal_mode_wal(db_path: str | Path) -> str:
    """No-op under Postgres; retained for API compat."""
    if DB_BACKEND == "sqlite":
        conn = sqlite3.connect(str(db_path), timeout=DEFAULT_TIMEOUT_SEC)
        try:
            result = conn.execute("PRAGMA journal_mode=WAL").fetchone()
            conn.commit()
            return result[0] if result else ""
        finally:
            conn.close()
    return "n/a (postgres)"


def force_journal_mode_delete(db_path: str | Path) -> str:
    if DB_BACKEND == "sqlite":
        conn = sqlite3.connect(str(db_path), timeout=DEFAULT_TIMEOUT_SEC)
        try:
            result = conn.execute("PRAGMA journal_mode=DELETE").fetchone()
            conn.commit()
            return result[0] if result else ""
        finally:
            conn.close()
    return "n/a (postgres)"
