"""
Polymarket feature generator.

Converts CLOB price history into a canonical feature parquet compatible
with the Kalshi backtest pipeline.  The feature schema is identical to
Kalshi feature parquets so the same KalshiBacktester and signal families
can be applied unchanged.

Price convention
----------------
CLOB ``p`` values are 0–1 floats (YES token price).  The Kalshi feature
builder auto-scales to 0–100 when it detects max ≤ 1.0, so we pass them
through as-is.

Synthetic trades
----------------
CLOB prices-history gives snapshots, not individual trades.  Each snapshot
becomes one "synthetic trade" with count=1.  Volume-based signals will be
flat (no real order flow), but calibration_drift and time_decay signals
will have meaningful values.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    from trading_platform.polymarket.models import PolymarketMarket

logger = logging.getLogger(__name__)


def build_polymarket_features(
    market: "PolymarketMarket",
    price_history: list[dict],
    *,
    ticker: str | None = None,
    period: str = "1h",
) -> pl.DataFrame:
    """
    Build a Kalshi-compatible feature DataFrame for a Polymarket market.

    :param market:        Parsed PolymarketMarket.
    :param price_history: List of ``{"t": unix_ts, "p": price_0_to_1}`` dicts
                          from the CLOB prices-history endpoint.
    :param ticker:        Override for the symbol name (defaults to market.id).
    :param period:        Resampling period (default ``"1h"``).
    :returns:             Feature DataFrame with the same schema as Kalshi features.
    :raises ValueError:   If price_history is empty or yields no valid rows.
    """
    from trading_platform.kalshi.features import build_kalshi_features

    ticker = ticker or market.id

    close_time: datetime | None = None
    if market.end_date_iso:
        try:
            end = market.end_date_iso
            if end.endswith("Z"):
                end = end[:-1] + "+00:00"
            close_time = datetime.fromisoformat(end)
        except (ValueError, AttributeError):
            pass

    rows = []
    for point in price_history:
        ts = point.get("t") or point.get("timestamp")
        p = point.get("p") or point.get("price")
        if ts is None or p is None:
            continue
        try:
            rows.append({
                "traded_at": datetime.fromtimestamp(float(ts), tz=timezone.utc),
                "yes_price": float(p),
                "count": 1,
            })
        except (TypeError, ValueError, OSError):
            continue

    if not rows:
        raise ValueError(f"No valid price history points for market {market.id}")

    trades = pl.DataFrame(rows).with_columns(
        pl.col("traded_at").cast(pl.Datetime("us", "UTC")),
        pl.col("yes_price").cast(pl.Float64),
        pl.col("count").cast(pl.Int64),
    )

    return build_kalshi_features(
        trades,
        ticker=ticker,
        period=period,
        close_time=close_time,
        feature_groups=[
            "probability_calibration",
            "volume_activity",
            "time_decay",
        ],
        timestamp_col="traded_at",
        price_col="yes_price",
        count_col="count",
    )


class PolymarketFeatureGenerator:
    """
    Thin wrapper around :func:`build_polymarket_features` that also handles
    writing the output parquet to disk.
    """

    def __init__(self, features_dir: str | Path) -> None:
        self.features_dir = Path(features_dir)
        self.features_dir.mkdir(parents=True, exist_ok=True)

    def generate_and_write(
        self,
        market: "PolymarketMarket",
        price_history: list[dict],
        *,
        period: str = "1h",
    ) -> Path:
        """
        Generate features for *market* and write to
        ``<features_dir>/<market.id>.parquet``.

        :raises ValueError: If price_history is empty or invalid.
        """
        df = build_polymarket_features(market, price_history, period=period)
        out_path = self.features_dir / f"{market.id}.parquet"
        df.write_parquet(out_path)
        return out_path
