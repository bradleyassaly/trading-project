from __future__ import annotations

from typing import Any

import pandas as pd

from trading_platform.ingestion.contracts import (
    CANONICAL_MARKET_DATA_COLUMNS,
    MARKET_DATA_SCHEMA_VERSION,
)


def normalize_timeframe(value: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError("timeframe must be a non-empty string")
    mapping = {
        "1Day": "1d",
        "1D": "1d",
        "day": "1d",
        "daily": "1d",
        "1Hour": "1h",
        "1H": "1h",
        "hour": "1h",
        "1Min": "1m",
        "1T": "1m",
        "minute": "1m",
    }
    return mapping.get(normalized, normalized.lower())


def normalize_symbol(symbol: str) -> str:
    value = str(symbol or "").strip().upper()
    if not value:
        raise ValueError("symbol must be a non-empty string")
    return value


def normalize_market_data_frame(
    frame: pd.DataFrame,
    *,
    symbol: str,
    timeframe: str,
    provider: str,
    asset_class: str,
    metadata: dict[str, Any] | None = None,
) -> pd.DataFrame:
    if frame.empty:
        raise ValueError(f"No normalized market data available for {symbol}")

    normalized = frame.copy()
    if "date" in normalized.columns and "timestamp" not in normalized.columns:
        normalized["timestamp"] = normalized["date"]

    normalized["timestamp"] = pd.to_datetime(normalized["timestamp"], errors="coerce", utc=False)
    normalized["symbol"] = normalize_symbol(symbol)
    normalized["timeframe"] = normalize_timeframe(timeframe)
    normalized["provider"] = str(provider)
    normalized["asset_class"] = str(asset_class)
    normalized["schema_version"] = MARKET_DATA_SCHEMA_VERSION

    missing = [column for column in CANONICAL_MARKET_DATA_COLUMNS if column not in normalized.columns]
    if missing:
        raise ValueError(f"Missing required canonical market-data columns: {missing}")

    normalized = normalized[CANONICAL_MARKET_DATA_COLUMNS].copy()
    normalized = normalized.sort_values(["timestamp", "symbol"]).reset_index(drop=True)
    normalized.attrs["metadata"] = {} if metadata is None else {str(key): metadata[key] for key in sorted(metadata)}
    return normalized
