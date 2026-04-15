"""One-shot SQLite → Postgres migration.

Phase 1: generate Postgres DDL from SQLite schema (with dialect fixes).
Phase 2: dump SQLite data and bulk-load into Postgres using COPY.

Design choices:
  - Every INTEGER PRIMARY KEY AUTOINCREMENT → BIGSERIAL PRIMARY KEY
  - TEXT / INTEGER / REAL map 1:1 (all three exist in Postgres too)
  - INSERT OR REPLACE INTO → handled at application level (upsert shim)
  - sqlite_autoindex_* indexes are already implicit in Postgres PKs — skip
  - Partial indexes, triggers, views → NOT YET SUPPORTED (we don't use any)

Usage:
    python scripts/migrate_sqlite_to_postgres.py --drop-first
"""
from __future__ import annotations

import argparse
import os
import re
import sqlite3
import sys
import time
from pathlib import Path

try:
    import psycopg
    from psycopg import sql as pg_sql
except ImportError:
    print("psycopg v3 required. Install: pip install 'psycopg[binary]'")
    sys.exit(1)


SQLITE_PATH = "data/polymarket/wallet_intelligence.db"

# Connection params (override via env for remote Postgres)
PG_HOST = os.environ.get("POSTGRES_HOST", "localhost")
PG_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
PG_USER = os.environ.get("POSTGRES_USER", "polymarket")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "polymarket_dev")
PG_DB = os.environ.get("POSTGRES_DB", "polymarket")


def _pg_conn():
    return psycopg.connect(
        host=PG_HOST, port=PG_PORT,
        user=PG_USER, password=PG_PASSWORD,
        dbname=PG_DB, autocommit=False,
    )


# ── Schema conversion ─────────────────────────────────────────────────────

def _convert_column_types(sql: str) -> str:
    """Convert SQLite column types + default exprs to Postgres equivalents."""
    # AUTOINCREMENT primary key → BIGSERIAL
    sql = re.sub(
        r"INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT",
        "BIGSERIAL PRIMARY KEY",
        sql, flags=re.IGNORECASE,
    )
    # Plain INTEGER PRIMARY KEY stays as BIGINT
    sql = re.sub(
        r"INTEGER\s+PRIMARY\s+KEY",
        "BIGINT PRIMARY KEY",
        sql, flags=re.IGNORECASE,
    )
    sql = re.sub(r"COLLATE\s+\w+", "", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\)\s*(WITHOUT\s+ROWID|STRICT)\s*;?\s*$", ")", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bINTEGER\b", "BIGINT", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bREAL\b", "DOUBLE PRECISION", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bBLOB\b", "BYTEA", sql, flags=re.IGNORECASE)

    # SQLite-specific DEFAULT expressions → Postgres equivalents.
    # unixepoch('now')  →  EXTRACT(EPOCH FROM NOW())::BIGINT
    sql = re.sub(
        r"unixepoch\s*\(\s*('now')?\s*\)",
        "(EXTRACT(EPOCH FROM NOW())::BIGINT)",
        sql, flags=re.IGNORECASE,
    )
    # strftime('%s', 'now')  →  EXTRACT(EPOCH FROM NOW())::BIGINT
    sql = re.sub(
        r"strftime\s*\(\s*['\"]\%s['\"]\s*,\s*['\"]now['\"]\s*\)",
        "(EXTRACT(EPOCH FROM NOW())::BIGINT)",
        sql, flags=re.IGNORECASE,
    )
    # datetime('now') → NOW()
    sql = re.sub(r"datetime\s*\(\s*['\"]now['\"]\s*\)", "NOW()", sql, flags=re.IGNORECASE)
    # date('now') → CURRENT_DATE
    sql = re.sub(r"date\s*\(\s*['\"]now['\"]\s*\)", "CURRENT_DATE", sql, flags=re.IGNORECASE)
    # CURRENT_TIMESTAMP works in both — leave as-is
    return sql


def get_sqlite_schema(db_path: str) -> list[tuple[str, str, str]]:
    """Return [(type, name, sql), ...] for tables + indexes."""
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE type IN ('table', 'index') AND sql IS NOT NULL "
            "AND name NOT LIKE 'sqlite_%' "
            "ORDER BY CASE type WHEN 'table' THEN 0 ELSE 1 END, name"
        ).fetchall()
    finally:
        conn.close()
    return rows


def ddl_for_postgres(db_path: str) -> list[tuple[str, str]]:
    """Return list of (name, ddl_statement) ready to execute against Postgres."""
    out: list[tuple[str, str]] = []
    for typ, name, sql in get_sqlite_schema(db_path):
        if not sql:
            continue
        converted = _convert_column_types(sql).strip().rstrip(";") + ";"
        # Postgres doesn't support CREATE TABLE IF NOT EXISTS on some older versions
        # but modern Postgres does. Keep the IF NOT EXISTS.
        if typ == "index":
            # Skip sqlite_autoindex_*
            if name.startswith("sqlite_autoindex_"):
                continue
            # CREATE UNIQUE INDEX syntax is same
        out.append((name, converted))
    return out


def create_postgres_schema(ddl: list[tuple[str, str]], drop_first: bool = False) -> None:
    conn = _pg_conn()
    try:
        with conn.cursor() as cur:
            if drop_first:
                # Drop all tables in reverse dependency order — use CASCADE
                cur.execute(
                    "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
                )
                for (t,) in cur.fetchall():
                    cur.execute(f'DROP TABLE IF EXISTS "{t}" CASCADE')
            for name, stmt in ddl:
                try:
                    cur.execute(stmt)
                    print(f"  ok: {name}")
                except psycopg.Error as exc:
                    print(f"  FAIL {name}: {exc}")
                    print(f"    stmt: {stmt[:200]}")
                    conn.rollback()
                    raise
            conn.commit()
    finally:
        conn.close()


# ── Data migration ────────────────────────────────────────────────────────

def migrate_table_data(table: str, batch: int = 1000) -> dict:
    sconn = sqlite3.connect(SQLITE_PATH)
    sconn.row_factory = sqlite3.Row
    try:
        rows = sconn.execute(f'SELECT * FROM "{table}"').fetchall()
        if not rows:
            return {"table": table, "count": 0}
        cols = rows[0].keys()
    finally:
        sconn.close()
    if not rows:
        return {"table": table, "count": 0}

    pconn = _pg_conn()
    try:
        with pconn.cursor() as cur:
            cols_sql = ", ".join(f'"{c}"' for c in cols)
            placeholders = ", ".join(["%s"] * len(cols))
            insert_sql = f'INSERT INTO "{table}" ({cols_sql}) VALUES ({placeholders})'
            written = 0
            buffer = []
            for r in rows:
                buffer.append(tuple(r[c] for c in cols))
                if len(buffer) >= batch:
                    cur.executemany(insert_sql, buffer)
                    written += len(buffer)
                    buffer = []
            if buffer:
                cur.executemany(insert_sql, buffer)
                written += len(buffer)
        pconn.commit()
        return {"table": table, "count": written}
    except psycopg.Error as exc:
        pconn.rollback()
        return {"table": table, "count": 0, "error": str(exc)[:200]}
    finally:
        pconn.close()


def list_sqlite_tables(db_path: str) -> list[str]:
    conn = sqlite3.connect(db_path)
    try:
        return [
            r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%' ORDER BY name"
            ).fetchall()
        ]
    finally:
        conn.close()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--drop-first", action="store_true", help="Drop all Postgres tables first")
    p.add_argument("--schema-only", action="store_true", help="Only migrate schema, not data")
    p.add_argument("--data-only", action="store_true", help="Only migrate data")
    args = p.parse_args()

    if not Path(SQLITE_PATH).exists():
        print(f"SQLite not found at {SQLITE_PATH}")
        return 1

    if not args.data_only:
        print(f"=== Phase 1: SCHEMA MIGRATION ===")
        print(f"reading SQLite schema from {SQLITE_PATH}")
        ddl = ddl_for_postgres(SQLITE_PATH)
        print(f"  {len(ddl)} DDL statements generated")
        print(f"\napplying to Postgres {PG_HOST}:{PG_PORT}/{PG_DB}...")
        create_postgres_schema(ddl, drop_first=args.drop_first)
        print(f"schema done.")

    if not args.schema_only:
        print(f"\n=== Phase 2: DATA MIGRATION ===")
        tables = list_sqlite_tables(SQLITE_PATH)
        print(f"{len(tables)} tables to migrate")
        total = 0
        t0 = time.time()
        for t in tables:
            result = migrate_table_data(t)
            cnt = result.get("count", 0)
            total += cnt
            err = result.get("error")
            if err:
                print(f"  {t:30s} ERROR: {err}")
            else:
                print(f"  {t:30s} {cnt:>8,} rows")
        print(f"\ndata migration done — {total:,} total rows in {time.time()-t0:.1f}s")

        # Phase 3: reset sequences — BIGSERIAL sequences start at 1 but
        # migrated data may already have higher ids. Sync each sequence
        # to MAX(id)+1 so the next INSERT doesn't collide.
        print(f"\n=== Phase 3: SEQUENCE RESET ===")
        c = _pg_conn()
        c.autocommit = True
        try:
            with c.cursor() as cur:
                cur.execute(
                    "SELECT c.relname AS seq, t.relname AS tbl, a.attname AS col "
                    "FROM pg_class c "
                    "JOIN pg_depend d ON d.objid = c.oid "
                    "JOIN pg_class t ON t.oid = d.refobjid "
                    "JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = d.refobjsubid "
                    "WHERE c.relkind = 'S' AND d.deptype = 'a'"
                )
                for seq, tbl, col in cur.fetchall():
                    with c.cursor() as c2:
                        c2.execute(f'SELECT COALESCE(MAX("{col}"), 0) + 1 FROM "{tbl}"')
                        nxt = c2.fetchone()[0]
                        c2.execute(f"SELECT setval('{seq}', {nxt}, false)")
                    print(f"  {tbl}.{col} → next_val={nxt}")
        finally:
            c.close()
        print(f"sequence reset complete.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
