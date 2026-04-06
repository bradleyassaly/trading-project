"""
CLI: trading-cli data polymarket collect-orderflow
     trading-cli data polymarket orderflow-status
"""
from __future__ import annotations

import argparse
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml

logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[4]


def cmd_polymarket_collect_orderflow(args: argparse.Namespace) -> None:
    from trading_platform.polymarket.goldsky_client import GoldskyClient
    from trading_platform.polymarket.orderflow_collector import OrderFlowCollector

    # Load Polymarket token_ids from live DB
    db_path = PROJECT_ROOT / "data" / "polymarket" / "live" / "prices.db"
    poly_ids: list[str] = []
    if db_path.exists():
        try:
            conn = sqlite3.connect(str(db_path), check_same_thread=False)
            rows = conn.execute(
                "SELECT yes_token_id FROM markets WHERE yes_token_id IS NOT NULL"
            ).fetchall()
            conn.close()
            poly_ids = [r[0] for r in rows if r[0]]
        except Exception as exc:
            print(f"[WARN] Could not load token_ids from {db_path}: {exc}")
    else:
        print(f"[WARN] Live DB not found at {db_path} — no Polymarket markets to collect")

    # Load Kalshi tickers from config
    kalshi_tickers: list[str] = []
    config_path = PROJECT_ROOT / "configs" / "kalshi.yaml"
    if config_path.exists():
        try:
            cfg = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
            kalshi_tickers = list(cfg.get("historical_ingest", {}).get("direct_series_tickers") or [])
        except Exception:
            pass

    interval = int(getattr(args, "interval", None) or 300)
    once = getattr(args, "once", False)
    since_days = int(getattr(args, "since_days", None) or 7)
    since_hours = since_days * 24

    goldsky = GoldskyClient()
    # Kalshi orderbook collector is optional
    kalshi_collector = None

    collector = OrderFlowCollector(
        goldsky_client=goldsky,
        kalshi_collector=kalshi_collector,
        fills_dir=str(PROJECT_ROOT / "data" / "polymarket" / "orderflow"),
        polymarket_token_ids=poly_ids,
        kalshi_tickers=kalshi_tickers,
    )

    print("Polymarket Orderflow Collector")
    print(f"  Polymarket markets : {len(poly_ids)}")
    print(f"  Kalshi series      : {len(kalshi_tickers)}")
    print(f"  Fills dir          : data/polymarket/orderflow/")
    print(f"  Since              : {since_days} days")
    print()

    if once:
        summary = collector.run_once(since_hours=since_hours)
        print(f"[DONE] Orderflow collection complete.")
        print(f"  Polymarket updated : {summary['polymarket_markets_updated']}")
        print(f"  New fills          : {summary['new_fills_total']}")
        print(f"  Kalshi snapshots   : {summary['kalshi_snapshots_taken']}")
        if summary["errors"]:
            print(f"  Errors             : {len(summary['errors'])}")
            for e in summary["errors"][:5]:
                print(f"    {e}")
    else:
        print(f"Running every {interval}s. Ctrl+C to stop.")
        collector.run_loop(interval_seconds=interval, since_hours=since_hours)


def cmd_polymarket_orderflow_status(args: argparse.Namespace) -> None:
    fills_dir = PROJECT_ROOT / "data" / "polymarket" / "orderflow"

    if not fills_dir.exists():
        print("No orderflow data found. Run: trading-cli data polymarket collect-orderflow --once")
        return

    parquets = sorted(fills_dir.glob("*.parquet"))
    if not parquets:
        print("No fill parquet files found.")
        return

    now = datetime.now(tz=timezone.utc)
    print(f"{'Token ID':<24} {'Fills':>8} {'Latest':>20} {'Age (min)':>10}")
    print(f"{'-'*24} {'-'*8} {'-'*20} {'-'*10}")

    oldest_age = 0.0
    newest_age = float("inf")

    for path in parquets:
        try:
            df = pd.read_parquet(path)
            count = len(df)
            if count == 0:
                continue
            if "timestamp" in df.columns:
                max_ts = df["timestamp"].max()
                if hasattr(max_ts, "to_pydatetime"):
                    max_dt = max_ts.to_pydatetime()
                    age_min = (now - max_dt).total_seconds() / 60
                    ts_str = max_dt.strftime("%Y-%m-%d %H:%M")
                else:
                    age_min = 0
                    ts_str = str(max_ts)[:20]
            else:
                age_min = 0
                ts_str = "—"

            oldest_age = max(oldest_age, age_min)
            newest_age = min(newest_age, age_min)

            print(f"{path.stem[:24]:<24} {count:>8,} {ts_str:>20} {age_min:>10,.0f}")
        except Exception as exc:
            print(f"{path.stem[:24]:<24} ERROR: {exc}")

    if newest_age == float("inf"):
        newest_age = 0

    print(f"\nSummary: {len(parquets)} markets tracked")
    print(f"  Oldest fill: {oldest_age:,.0f} min ago")
    print(f"  Newest fill: {newest_age:,.0f} min ago")
