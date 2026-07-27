"""Firehose retention: archive-then-prune non-roster wallet fills.

The chain firehose (2026-07-20) writes ~300-380k rows/hour (~8M/day, ~2.5GB/day
in Postgres) — full-history-for-everyone is not viable on this host. Policy:

  KEEP IN DB forever : fills for roster wallets (wallet_profiles) — the
                       analytics working set; new roster members get API
                       backfill anyway.
  KEEP IN DB 7 days  : everyone else (discovery jobs scan recent activity).
  ARCHIVE then DELETE: non-roster rows older than RETAIN_DAYS are appended to
                       daily parquet files (data/polymarket/firehose_archive/
                       fills_YYYY-MM-DD.parquet, ~10x compression) BEFORE
                       deletion — the data-product history survives on disk,
                       the DB stays lean.

Idempotent; batched deletes (100k/batch) to avoid long locks. Reports rows
archived/deleted + table size to data_quality_metrics.

Run: python scripts/firehose_retention.py [--days 7] [--dry-run]
Scheduled: task 'firehose_retention' (daily).
"""
from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from trading_platform.polymarket.db_connection import get_connection

ARCHIVE_DIR = Path("data/polymarket/firehose_archive")
BATCH = int(__import__("os").environ.get("FIREHOSE_PRUNE_BATCH", "100000"))
# Roll to a new parquet part once the day file exceeds this: the append path
# is read-whole-file + concat + rewrite, which is quadratic in file size and
# holds the entire file in RAM — fatal on a multi-day catch-up backlog.
ROLL_BYTES = int(__import__("os").environ.get("FIREHOSE_ARCHIVE_ROLL_BYTES",
                                              str(256 * 1024 * 1024)))


def _archive_path(day: str) -> Path:
    """Day file, rolling to fills_{day}_pN.parquet when the current part
    is full (bounds the concat-rewrite cost and RAM per batch)."""
    out = ARCHIVE_DIR / f"fills_{day}.parquet"
    n = 0
    while out.exists() and out.stat().st_size >= ROLL_BYTES:
        n += 1
        out = ARCHIVE_DIR / f"fills_{day}_p{n}.parquet"
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=int(
        __import__("os").environ.get("FIREHOSE_RETAIN_DAYS", "7")))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    cutoff = int(time.time()) - args.days * 86400
    conn = get_connection()
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

    n_candidates = conn.execute(
        """SELECT COUNT(*) FROM wallet_trades wt
           WHERE wt.synced_at < ?
             AND NOT EXISTS (SELECT 1 FROM wallet_profiles wp
                             WHERE wp.wallet = wt.wallet)
             AND NOT EXISTS (SELECT 1 FROM wallet_alpha_scores was
                             WHERE was.wallet = wt.wallet)""",
        (cutoff,),
    ).fetchone()[0]
    print(f"[retention] non-roster rows older than {args.days}d: {n_candidates:,}")
    if args.dry_run or not n_candidates:
        conn.close()
        return

    import polars as pl
    total_archived = total_deleted = 0
    while True:
        rows = conn.execute(
            """SELECT wt.id, wt.wallet, wt.proxy_wallet, wt.asset, wt.condition_id,
                      wt.side, wt.size, wt.price, wt.timestamp, wt.title, wt.slug,
                      wt.outcome, wt.event_slug, wt.transaction_hash, wt.category,
                      wt.synced_at
               FROM wallet_trades wt
               WHERE wt.synced_at < ?
                 AND NOT EXISTS (SELECT 1 FROM wallet_profiles wp
                                 WHERE wp.wallet = wt.wallet)
                 AND NOT EXISTS (SELECT 1 FROM wallet_alpha_scores was
                                 WHERE was.wallet = wt.wallet)
               ORDER BY wt.id LIMIT ?""",
            (cutoff, BATCH),
        ).fetchall()
        if not rows:
            break
        cols = ["id", "wallet", "proxy_wallet", "asset", "condition_id", "side",
                "size", "price", "timestamp", "title", "slug", "outcome",
                "event_slug", "transaction_hash", "category", "synced_at"]
        df = pl.DataFrame([dict(zip(cols, r)) for r in rows],
                          infer_schema_length=None)
        day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        out = _archive_path(day)
        if out.exists():  # append via concat (parts stay under ROLL_BYTES)
            df = pl.concat([pl.read_parquet(out), df], how="vertical_relaxed")
        df.write_parquet(out, compression="zstd")
        ids = [r[0] for r in rows]
        # Single array bind — psycopg3 binds server-side with a hard 65,535
        # parameter cap, so the old 100k-way "IN (?,?,...)" failed on EVERY
        # batch (4 silent no-prune days; table hit 52 GB / 48M rows before
        # the 2026-07-27 eval caught it).
        conn.execute("DELETE FROM wallet_trades WHERE id = ANY(?)", (ids,))
        conn.commit()
        total_archived += len(rows)
        total_deleted += len(rows)
        print(f"[retention] archived+deleted {total_deleted:,}/{n_candidates:,}")

    size = conn.execute(
        "SELECT pg_total_relation_size('wallet_trades')").fetchone()[0]
    now = int(time.time())
    conn.execute("""
        CREATE TABLE IF NOT EXISTS data_quality_metrics (
            ts BIGINT NOT NULL, metric TEXT NOT NULL, wallet TEXT,
            value DOUBLE PRECISION, detail TEXT)""")
    for metric, value in (("firehose_rows_pruned", float(total_deleted)),
                          ("wallet_trades_bytes", float(size))):
        conn.execute(
            "INSERT INTO data_quality_metrics (ts, metric, wallet, value, detail) "
            "VALUES (?,?,NULL,?,?)",
            (now, metric, value, json.dumps({"retain_days": args.days})))
    conn.commit()
    conn.close()
    print(f"[retention] done: {total_deleted:,} rows -> {ARCHIVE_DIR}, "
          f"table now {size/1e9:.2f} GB")


if __name__ == "__main__":
    main()
