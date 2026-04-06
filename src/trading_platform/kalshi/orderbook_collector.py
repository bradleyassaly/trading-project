"""
Kalshi orderbook snapshot collector.

Fetches authenticated orderbook snapshots and computes depth features.

Usage::

    from trading_platform.kalshi.orderbook_collector import KalshiOrderBookCollector
    collector = KalshiOrderBookCollector(client)
    df = collector.snapshot("KXCPI-26APR-T0.3")
    features = collector.compute_book_features("KXCPI-26APR-T0.3")
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class KalshiOrderBookCollector:
    """Collect and analyze Kalshi orderbook snapshots."""

    def __init__(self, client: Any, snapshot_dir: str = "data/kalshi/orderbook_snapshots") -> None:
        self.client = client
        self.snapshot_dir = Path(snapshot_dir)
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)

    def snapshot(self, ticker: str) -> pd.DataFrame:
        """Fetch orderbook for ticker and save snapshot. Returns DataFrame."""
        try:
            data = self.client._get(f"/markets/{ticker}/orderbook", {})
        except Exception as exc:
            logger.debug("Orderbook fetch failed for %s: %s", ticker, exc)
            return pd.DataFrame()

        orderbook = data.get("orderbook", {})
        yes_levels = orderbook.get("yes", [])
        no_levels = orderbook.get("no", [])

        rows: list[dict[str, Any]] = []
        now = datetime.now(tz=timezone.utc)
        for price_cents, size in yes_levels:
            rows.append({"ticker": ticker, "side": "YES", "price": float(price_cents) / 100.0,
                         "size": int(size), "ts": now})
        for price_cents, size in no_levels:
            rows.append({"ticker": ticker, "side": "NO", "price": float(price_cents) / 100.0,
                         "size": int(size), "ts": now})

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)

        # Save snapshot
        ticker_dir = self.snapshot_dir / ticker
        ticker_dir.mkdir(parents=True, exist_ok=True)
        ts_label = now.strftime("%Y%m%d%H%M%S%f")
        df.to_parquet(ticker_dir / f"{ts_label}.parquet", index=False)

        return df

    def compute_book_features(self, ticker: str, lookback: int = 12) -> dict[str, float]:
        """Compute orderbook features from recent snapshots."""
        ticker_dir = self.snapshot_dir / ticker
        if not ticker_dir.exists():
            return {}

        snapshots = sorted(ticker_dir.glob("*.parquet"))[-lookback:]
        if len(snapshots) < 2:
            return {}

        try:
            latest = pd.read_parquet(snapshots[-1])
            if latest.empty:
                return {}

            yes_df = latest[latest["side"] == "YES"]
            no_df = latest[latest["side"] == "NO"]

            best_yes_bid = float(yes_df["price"].max()) if not yes_df.empty else 0.0
            best_no_bid = float(no_df["price"].max()) if not no_df.empty else 0.0
            spread = 1.0 - best_yes_bid - best_no_bid
            yes_depth = float(yes_df["size"].sum()) if not yes_df.empty else 0.0
            no_depth = float(no_df["size"].sum()) if not no_df.empty else 0.0
            total_depth = yes_depth + no_depth
            depth_imbalance = (yes_depth - no_depth) / total_depth if total_depth > 0 else 0.0

            # Trends over snapshots
            imbalances: list[float] = []
            spreads: list[float] = []
            for sp in snapshots:
                try:
                    sdf = pd.read_parquet(sp)
                    y = sdf[sdf["side"] == "YES"]
                    n = sdf[sdf["side"] == "NO"]
                    yd = float(y["size"].sum()) if not y.empty else 0
                    nd = float(n["size"].sum()) if not n.empty else 0
                    t = yd + nd
                    imbalances.append((yd - nd) / t if t > 0 else 0)
                    yb = float(y["price"].max()) if not y.empty else 0
                    nb = float(n["price"].max()) if not n.empty else 0
                    spreads.append(1.0 - yb - nb)
                except Exception:
                    continue

            # Linear slope
            def _slope(values: list[float]) -> float:
                if len(values) < 2:
                    return 0.0
                x = np.arange(len(values), dtype=float)
                y = np.array(values, dtype=float)
                return float(np.polyfit(x, y, 1)[0])

            return {
                "best_yes_bid": round(best_yes_bid, 4),
                "best_no_bid": round(best_no_bid, 4),
                "spread": round(spread, 4),
                "yes_depth": yes_depth,
                "no_depth": no_depth,
                "depth_imbalance": round(depth_imbalance, 4),
                "depth_imbalance_trend": round(_slope(imbalances), 6),
                "spread_trend": round(_slope(spreads), 6),
            }
        except Exception as exc:
            logger.warning("Book feature computation failed for %s: %s", ticker, exc)
            return {}

    def snapshot_all(self, tickers: list[str]) -> dict[str, pd.DataFrame]:
        """Snapshot orderbooks for all tickers. Returns {ticker: df}."""
        results: dict[str, pd.DataFrame] = {}
        for ticker in tickers:
            try:
                df = self.snapshot(ticker)
                results[ticker] = df
                if not df.empty:
                    logger.debug("Snapshot %s: %d levels", ticker, len(df))
            except Exception as exc:
                logger.warning("Snapshot failed for %s: %s", ticker, exc)
                results[ticker] = pd.DataFrame()
        return results
