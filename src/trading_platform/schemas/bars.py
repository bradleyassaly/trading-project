from __future__ import annotations

from dataclasses import dataclass

BAR_COLUMNS = [
    "timestamp",
    "symbol",
    "open",
    "high",
    "low",
    "close",
    "volume",
]

REQUIRED_BAR_COLUMNS = tuple(BAR_COLUMNS)

OPTIONAL_BAR_COLUMNS = (
    "adj_close",
    "vwap",
    "trade_count",
)

NUMERIC_BAR_COLUMNS = (
    "open",
    "high",
    "low",
    "close",
    "volume",
)


@dataclass(frozen=True)
class BarSchema:
    timestamp: str = "timestamp"
    symbol: str = "symbol"
    open: str = "open"
    high: str = "high"
    low: str = "low"
    close: str = "close"
    volume: str = "volume"


BAR_SCHEMA = BarSchema()