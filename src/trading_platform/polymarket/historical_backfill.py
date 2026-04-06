"""
Backfill historical trades for already-resolved Polymarket markets.

Fetches trade history from the Data API for markets that have already
resolved (from gamma_resolution.csv), enabling immediate wallet profiling
without waiting weeks for active markets to close.

Usage:
    python scripts/backfill_historical_trades.py --limit 200 --min-volume 5000
    trading-cli data polymarket backfill-historical-trades --limit 200
"""
from __future__ import annotations

import csv
import logging
import time
from pathlib import Path
from typing import Any

import requests

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
_BASE_URL = "https://data-api.polymarket.com"
_SLEEP = 0.5


def backfill(
    *,
    resolution_csv: str | Path | None = None,
    output_dir: str | Path | None = None,
    limit: int = 200,
    min_volume: float = 5000.0,
    max_trades_per_market: int = 10000,
) -> dict[str, Any]:
    resolution_csv = Path(resolution_csv or PROJECT_ROOT / "data" / "polymarket" / "gamma_resolution.csv")
    output_dir = Path(output_dir or PROJECT_ROOT / "data" / "polymarket" / "historical_trades")
    output_dir.mkdir(parents=True, exist_ok=True)

    if not resolution_csv.exists():
        print(f"[ERROR] Resolution CSV not found: {resolution_csv}")
        return {"markets_fetched": 0, "total_trades": 0}

    # Load resolved markets, filter by volume
    markets: list[dict[str, str]] = []
    with resolution_csv.open(newline="", encoding="utf-8-sig") as fh:
        for row in csv.DictReader(fh):
            try:
                vol = float(row.get("volume") or 0)
            except (TypeError, ValueError):
                vol = 0
            if vol < min_volume:
                continue
            ticker = row.get("ticker", "").strip()
            if not ticker:
                continue
            markets.append(row)

    markets = markets[:limit]
    print(f"Backfilling trades for {len(markets)} resolved markets (min_vol={min_volume:,.0f})")

    session = requests.Session()
    session.headers.update({"Accept": "application/json"})

    csv_fields = ["timestamp", "wallet", "token_id", "condition_id",
                  "side", "price", "total_usdc", "outcome", "title"]
    total_trades = 0
    markets_fetched = 0

    for i, market in enumerate(markets):
        token_id = market["ticker"]
        if (i + 1) % 10 == 0 or i == 0:
            print(f"  Market {i+1}/{len(markets)}: {token_id[:24]}... ", end="", flush=True)

        market_csv = output_dir / f"{token_id[:32]}.csv"
        market_trades = 0

        with market_csv.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=csv_fields)
            writer.writeheader()

            offset = 0
            max_pages = max_trades_per_market // 500
            for page in range(max_pages):
                time.sleep(_SLEEP)
                try:
                    resp = session.get(
                        f"{_BASE_URL}/trades",
                        params={"market": token_id, "limit": 500, "offset": offset},
                        timeout=15,
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    trades = data if isinstance(data, list) else data.get("data", data.get("trades", []))
                except Exception as exc:
                    logger.debug("Fetch failed for %s page %d: %s", token_id[:16], page, exc)
                    break

                if not trades:
                    break

                for trade in trades:
                    try:
                        price = float(trade.get("price") or 0)
                        size = float(trade.get("size") or 0)
                        writer.writerow({
                            "timestamp": trade.get("timestamp", ""),
                            "wallet": trade.get("proxyWallet") or trade.get("maker_address") or "",
                            "token_id": trade.get("asset") or token_id,
                            "condition_id": trade.get("conditionId") or market.get("condition_id") or "",
                            "side": trade.get("side", ""),
                            "price": str(price),
                            "total_usdc": str(round(size * price, 2)),
                            "outcome": trade.get("outcome") or "",
                            "title": trade.get("title") or market.get("question") or "",
                        })
                        market_trades += 1
                    except (TypeError, ValueError):
                        continue

                if len(trades) < 500:
                    break
                offset += len(trades)

        total_trades += market_trades
        markets_fetched += 1
        if (i + 1) % 10 == 0 or i == 0:
            print(f"{market_trades} trades")

    # Combine all per-market CSVs
    combined_path = output_dir.parent / "data_api_trades" / "historical_combined.csv"
    combined_path.parent.mkdir(parents=True, exist_ok=True)
    with combined_path.open("w", newline="", encoding="utf-8") as out:
        writer = csv.DictWriter(out, fieldnames=csv_fields)
        writer.writeheader()
        for csv_file in sorted(output_dir.glob("*.csv")):
            with csv_file.open(newline="", encoding="utf-8-sig") as inp:
                for row in csv.DictReader(inp):
                    writer.writerow(row)

    print(f"\n[DONE] Backfill complete.")
    print(f"  Markets fetched: {markets_fetched}")
    print(f"  Total trades: {total_trades}")
    print(f"  Combined CSV: {combined_path}")

    return {"markets_fetched": markets_fetched, "total_trades": total_trades}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--min-volume", type=float, default=5000.0)
    args = parser.parse_args()
    backfill(limit=args.limit, min_volume=args.min_volume)
