from __future__ import annotations

from pathlib import Path

import pandas as pd


CANONICAL_SCHEMA_DESCRIPTION = {
    "required": ["timestamp", "close", "symbol"],
    "optional": ["open", "high", "low", "volume", "dollar_volume"],
    "notes": [
        "timestamp is timezone-naive pandas datetime64[ns]",
        "close is numeric and required for research-ready frames",
        "symbol is injected when missing",
        "extra columns are preserved",
    ],
}

_TIMESTAMP_CANDIDATES = ("timestamp", "date", "Date", "Datetime")
_CANONICAL_ALIASES = {
    "close": ("close", "Close", "adj_close", "Adj Close", "adj close", "adjusted_close", "Adjusted Close"),
    "open": ("open", "Open"),
    "high": ("high", "High"),
    "low": ("low", "Low"),
    "volume": ("volume", "Volume"),
    "dollar_volume": ("dollar_volume", "Dollar Volume", "dollarVolume", "avg_dollar_volume", "Avg Dollar Volume"),
}


def _normalize_timestamp_column(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    timestamp_source: str | None = None
    for candidate in _TIMESTAMP_CANDIDATES:
        if candidate in normalized.columns:
            timestamp_source = candidate
            break

    if timestamp_source is not None:
        normalized["timestamp"] = pd.to_datetime(normalized[timestamp_source], errors="coerce")
    elif isinstance(normalized.index, pd.DatetimeIndex):
        normalized = normalized.reset_index()
        index_name = str(normalized.columns[0])
        normalized["timestamp"] = pd.to_datetime(normalized[index_name], errors="coerce")
        if index_name != "timestamp":
            normalized = normalized.drop(columns=[index_name])
    else:
        raise ValueError(
            "feature data must include a 'timestamp', 'date', 'Date', or 'Datetime' column, "
            "or use a DatetimeIndex."
        )

    return (
        normalized.dropna(subset=["timestamp"])
        .sort_values("timestamp")
        .reset_index(drop=True)
    )


def _first_available_column(df: pd.DataFrame, candidates: tuple[str, ...]) -> str | None:
    for column in candidates:
        if column in df.columns:
            return column
    return None


def _normalize_standard_columns(
    df: pd.DataFrame,
    *,
    require_close: bool,
) -> pd.DataFrame:
    normalized = df.copy()
    for canonical_name, aliases in _CANONICAL_ALIASES.items():
        if canonical_name in normalized.columns:
            normalized[canonical_name] = pd.to_numeric(normalized[canonical_name], errors="coerce")
            continue

        source_column = _first_available_column(normalized, aliases)
        if source_column is None:
            continue
        normalized[canonical_name] = pd.to_numeric(normalized[source_column], errors="coerce")

    if require_close and "close" not in normalized.columns:
        raise ValueError(
            "feature data must include a 'close' column or a supported close-like variant."
        )

    return normalized


def normalize_research_frame(
    df: pd.DataFrame,
    *,
    symbol: str | None = None,
    require_close: bool = True,
) -> pd.DataFrame:
    normalized = _normalize_timestamp_column(df)
    normalized = _normalize_standard_columns(normalized, require_close=require_close)

    if symbol and "symbol" not in normalized.columns:
        normalized["symbol"] = symbol
    elif "symbol" in normalized.columns:
        normalized["symbol"] = normalized["symbol"].astype(str)

    return normalized


def load_research_frame_from_parquet(
    path: str | Path,
    *,
    symbol: str | None = None,
    require_close: bool = True,
) -> pd.DataFrame:
    parquet_path = Path(path)
    return normalize_research_frame(
        pd.read_parquet(parquet_path),
        symbol=symbol,
        require_close=require_close,
    )


def load_research_symbol_frame(
    feature_dir: str | Path,
    symbol: str,
    *,
    require_close: bool = True,
) -> pd.DataFrame:
    parquet_path = Path(feature_dir) / f"{symbol}.parquet"
    if not parquet_path.exists():
        raise FileNotFoundError(f"Feature file not found for {symbol}: {parquet_path}")
    return load_research_frame_from_parquet(
        parquet_path,
        symbol=symbol,
        require_close=require_close,
    )
