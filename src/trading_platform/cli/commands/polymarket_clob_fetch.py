"""
CLI: trading-cli data polymarket clob-fetch
     trading-cli data polymarket orderbook-fetch
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[4]


def _project_relative(p: str) -> Path:
    path = Path(p)
    return path if path.is_absolute() else PROJECT_ROOT / path


def cmd_polymarket_clob_fetch(args: argparse.Namespace) -> None:
    from trading_platform.polymarket.clob_trades_fetcher import ClobTradesFetcher

    output_dir = _project_relative(getattr(args, "output_dir", None) or "data/polymarket/clob_trades")
    hours_back = int(getattr(args, "hours_back", None) or 168)

    print(f"Polymarket CLOB Trade Fetcher")
    print(f"  output dir : {output_dir}")
    print(f"  hours back : {hours_back}")
    print()

    fetcher = ClobTradesFetcher()
    results = fetcher.fetch_all_active_markets(output_dir, hours_back=hours_back)

    total = sum(results.values())
    print(f"[DONE] Fetched {total} trades across {len(results)} markets")


def cmd_polymarket_orderbook_fetch(args: argparse.Namespace) -> None:
    from trading_platform.polymarket.graph_orderbook import GraphOrderbookFetcher

    print("Polymarket Orderbook Fetcher (Goldsky subgraph)")

    fetcher = GraphOrderbookFetcher()
    schema = fetcher.get_schema()
    print(f"  Schema fields: {len(schema)}")
    if schema:
        print(f"  Available: {', '.join(schema[:10])}...")

    # Load token IDs from live DB
    db_path = PROJECT_ROOT / "data" / "polymarket" / "live" / "prices.db"
    if not db_path.exists():
        print("[WARN] No live collector DB found")
        return

    import sqlite3
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    rows = conn.execute("SELECT yes_token_id FROM markets WHERE yes_token_id IS NOT NULL").fetchall()
    conn.close()
    token_ids = [r[0] for r in rows if r[0]]

    if not token_ids:
        print("[WARN] No token IDs found in DB")
        return

    print(f"  Fetching orderbooks for {len(token_ids)} markets...")
    df = fetcher.fetch_orderbooks(token_ids)
    print(f"  Got {len(df)} orderbook entries")

    if not df.empty:
        output = _project_relative("data/polymarket/orderbooks.parquet")
        output.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output, index=False)
        print(f"  Written to {output}")
