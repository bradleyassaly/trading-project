"""
Continuous orderflow collector for Polymarket and Kalshi.

Runs alongside the WebSocket price collector, periodically fetching
fills from Goldsky and orderbook snapshots from Kalshi.

Usage::

    from trading_platform.polymarket.orderflow_collector import OrderFlowCollector
    collector = OrderFlowCollector(goldsky_client, kalshi_collector)
    collector.run_once()
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


class OrderFlowCollector:
    """Collect fills and orderbook snapshots on a schedule."""

    def __init__(
        self,
        goldsky_client: Any = None,
        kalshi_collector: Any = None,
        *,
        fills_dir: str = "data/polymarket/orderflow/",
        polymarket_token_ids: list[str] | None = None,
        kalshi_tickers: list[str] | None = None,
    ) -> None:
        self._goldsky = goldsky_client
        self._kalshi = kalshi_collector
        self._fills_dir = Path(fills_dir)
        self._fills_dir.mkdir(parents=True, exist_ok=True)
        self._poly_ids = polymarket_token_ids or []
        self._kalshi_tickers = kalshi_tickers or []

    def run_once(self, *, since_hours: int = 24) -> dict[str, Any]:
        """Fetch fills + snapshots once. Returns summary."""
        summary: dict[str, Any] = {
            "polymarket_markets_updated": 0,
            "new_fills_total": 0,
            "kalshi_snapshots_taken": 0,
            "errors": [],
        }

        # Polymarket fills
        if self._goldsky and self._poly_ids:
            for token_id in self._poly_ids:
                try:
                    path = self._fills_dir / f"{token_id[:32]}.parquet"
                    # Check existing data to avoid re-fetching
                    effective_hours = since_hours
                    if path.exists():
                        try:
                            existing = pd.read_parquet(path)
                            if not existing.empty and "timestamp" in existing.columns:
                                max_ts = existing["timestamp"].max()
                                if hasattr(max_ts, "timestamp"):
                                    age_hours = (datetime.now(tz=timezone.utc) - max_ts.to_pydatetime()).total_seconds() / 3600
                                    effective_hours = max(1, int(age_hours) + 1)
                        except Exception:
                            pass

                    fills = self._goldsky.fetch_fills(token_id, since_hours=effective_hours)
                    if fills.empty:
                        continue

                    # Merge with existing
                    if path.exists():
                        try:
                            existing = pd.read_parquet(path)
                            fills = pd.concat([existing, fills], ignore_index=True)
                            if "id" in fills.columns:
                                fills = fills.drop_duplicates(subset=["id"], keep="last")
                            fills = fills.sort_values("timestamp")
                        except Exception:
                            pass

                    fills.to_parquet(path, index=False)
                    summary["polymarket_markets_updated"] += 1
                    summary["new_fills_total"] += len(fills)
                except Exception as exc:
                    summary["errors"].append(f"{token_id[:16]}: {exc}")
                    logger.debug("Fill fetch error for %s: %s", token_id[:16], exc)

        # Kalshi orderbook snapshots
        if self._kalshi and self._kalshi_tickers:
            results = self._kalshi.snapshot_all(self._kalshi_tickers)
            summary["kalshi_snapshots_taken"] = sum(1 for df in results.values() if not df.empty)

        return summary

    def run_loop(self, *, interval_seconds: int = 300, since_hours: int = 24) -> None:
        """Run collection in a loop."""
        while True:
            start = time.time()
            try:
                summary = self.run_once(since_hours=since_hours)
                elapsed = time.time() - start
                logger.info(
                    "Orderflow collection: poly=%d markets (%d fills), kalshi=%d snapshots, errors=%d, %.1fs",
                    summary["polymarket_markets_updated"],
                    summary["new_fills_total"],
                    summary["kalshi_snapshots_taken"],
                    len(summary["errors"]),
                    elapsed,
                )
            except Exception as exc:
                logger.error("Orderflow collection failed: %s", exc)
            time.sleep(interval_seconds)
