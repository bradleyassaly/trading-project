"""Export trading records for external consumers (e.g. the Zeno project).

2026-07-10: first consumer is Zeno, which needs the real-money trade
ledger with honest outcomes. Two consumption modes are supported:

  1. File export (this script): CSV + JSONL snapshots under exports/,
     with a manifest.json recording filters, row counts, and the export
     timestamp. Suitable for one-off sharing or a nightly scheduler job.
  2. Live access (created by --create-view): a stable Postgres view
     `zeno_trades_v1` over live_trades. External projects should read
     the VIEW, never the base table — the view's column set is a
     compatibility contract; internal schema churn doesn't break it.
     Grant access with a read-only role (run once, pick your password):
       CREATE ROLE zeno_ro LOGIN PASSWORD '...';
       GRANT CONNECT ON DATABASE polymarket TO zeno_ro;
       GRANT USAGE ON SCHEMA public TO zeno_ro;
       GRANT SELECT ON zeno_trades_v1 TO zeno_ro;
     Postgres listens on 127.0.0.1:5432 (compose port map), so a
     same-host project connects with: postgresql://zeno_ro:...@127.0.0.1:5432/polymarket

All timestamps are exported as ISO-8601 UTC strings; epoch columns from
the base table are converted. Prices are in probability space (0-1 USDC
per share). realized_pnl is net of the actual exit channel (see
reconciled-EV docs); is_probe rows are $1 execution probes and should be
EXCLUDED from any EV/alpha analysis downstream (that is their contract
here, same as in our own evidence streams).

Usage:
    python scripts/export_trading_records.py                 # real trades only
    python scripts/export_trading_records.py --include-dry   # + dry-run/blocked rows
    python scripts/export_trading_records.py --create-view   # (re)create zeno_trades_v1
    python scripts/export_trading_records.py --out DIR       # default exports/
"""
import argparse
import csv
import datetime as dt
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
from trading_platform.polymarket.db_connection import get_connection

# The compatibility contract: column name -> SQL expression over live_trades.
# Add columns freely; never rename or remove one without versioning the
# view (zeno_trades_v2) and the export.
COLUMNS: dict[str, str] = {
    "trade_id": "id",
    "is_real_money": "CASE WHEN dry_run = 0 THEN 1 ELSE 0 END",
    "is_probe": "COALESCE(is_probe, 0)",
    "signal_type": "signal_type",
    "category": "category",
    "direction": "direction",
    "side": "side",
    "confidence": "confidence",
    "question": "question",
    "condition_id": "condition_id",
    "token_id": "token_id",
    "status": "status",
    "attempted_at_utc": "to_char(to_timestamp(attempted_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')",
    "filled_at_utc": "to_char(to_timestamp(filled_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')",
    "exit_at_utc": "to_char(to_timestamp(exit_ts) AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')",
    "entry_price": "entry_price",
    "expected_price": "expected_price",
    "fill_price": "fill_price",
    "shares": "shares",
    "size_usd": "size_usd",
    "slippage": "slippage",
    "slippage_signed": "slippage_signed",
    "slippage_cost_usd": "slippage_cost_usd",
    "fill_time_ms": "fill_time_ms",
    "exit_price": "exit_price",
    "exit_reason": "exit_reason",
    "outcome": "outcome",
    "realized_pnl": "realized_pnl",
    "unrealized_pnl": "unrealized_pnl",
    "last_mark_price": "last_mark_price",
    "resolution_date_utc": "to_char(to_timestamp(resolution_date) AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')",
    "error_msg": "error_msg",
}

VIEW_NAME = "zeno_trades_v1"


def _select_sql(where: str) -> str:
    cols = ",\n       ".join(f"{expr} AS {name}" for name, expr in COLUMNS.items())
    return f"SELECT {cols}\n  FROM live_trades\n WHERE {where}\n ORDER BY id"


def create_view(conn) -> None:
    # The view always exposes every row; consumers filter on
    # is_real_money/is_probe themselves. CREATE OR REPLACE keeps grants.
    conn.execute(f"CREATE OR REPLACE VIEW {VIEW_NAME} AS\n{_select_sql('TRUE')}")
    conn.commit()
    print(f"view {VIEW_NAME} created/updated "
          f"({len(COLUMNS)} columns; grant SELECT to a read-only role for external access)")


def export_files(conn, out_dir: str, include_dry: bool) -> None:
    where = "TRUE" if include_dry else "dry_run = 0"
    rows = conn.execute(_select_sql(where)).fetchall()
    names = list(COLUMNS.keys())

    os.makedirs(out_dir, exist_ok=True)
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
    base = os.path.join(out_dir, f"trading_records_{stamp}")

    csv_path = base + ".csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(names)
        w.writerows(rows)

    jsonl_path = base + ".jsonl"
    with open(jsonl_path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(dict(zip(names, r)), default=str) + "\n")

    manifest = {
        "exported_at_utc": stamp,
        "source": "live_trades",
        "schema_version": VIEW_NAME,
        "filter": "all rows" if include_dry else "real money only (dry_run=0)",
        "row_count": len(rows),
        "columns": names,
        "files": [os.path.basename(csv_path), os.path.basename(jsonl_path)],
        "notes": [
            "prices are probability-space USDC per share (0-1)",
            "is_probe=1 rows are $1 execution probes — exclude from EV/alpha analysis",
            "realized_pnl is net of the actual exit channel",
            "status='blocked' rows (only present with --include-dry) never reached order placement",
        ],
    }
    with open(base + ".manifest.json", "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    print(f"exported {len(rows)} rows -> {csv_path}, {jsonl_path} (+manifest)")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default=os.path.join(os.path.dirname(__file__), "..", "exports"))
    ap.add_argument("--include-dry", action="store_true",
                    help="include dry-run/blocked rows (default: real money only)")
    ap.add_argument("--create-view", action="store_true",
                    help=f"(re)create the {VIEW_NAME} Postgres view and exit")
    args = ap.parse_args()

    conn = get_connection()
    try:
        if args.create_view:
            create_view(conn)
        else:
            export_files(conn, args.out, args.include_dry)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
