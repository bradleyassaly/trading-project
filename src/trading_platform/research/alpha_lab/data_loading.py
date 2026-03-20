from __future__ import annotations

from pathlib import Path

import pandas as pd


def normalize_timestamp_column(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    if "timestamp" in normalized.columns:
        normalized["timestamp"] = pd.to_datetime(normalized["timestamp"], errors="coerce")
    elif "date" in normalized.columns:
        normalized["timestamp"] = pd.to_datetime(normalized["date"], errors="coerce")
    elif "Date" in normalized.columns:
        normalized["timestamp"] = pd.to_datetime(normalized["Date"], errors="coerce")
    elif isinstance(normalized.index, pd.DatetimeIndex):
        normalized = normalized.reset_index()
        index_name = normalized.columns[0]
        normalized["timestamp"] = pd.to_datetime(normalized[index_name], errors="coerce")
        if index_name != "timestamp":
            normalized = normalized.drop(columns=[index_name])
    else:
        raise ValueError("feature data must include a 'timestamp', 'date', or 'Date' column, or use a DatetimeIndex.")

    normalized = normalized.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    return normalized


def normalize_close_column(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    if "close" in normalized.columns:
        normalized["close"] = pd.to_numeric(normalized["close"], errors="coerce")
        return normalized

    close_candidates = ("Close", "adj_close", "Adj Close", "adj close", "adjusted_close", "Adjusted Close")
    for column in close_candidates:
        if column in normalized.columns:
            normalized["close"] = pd.to_numeric(normalized[column], errors="coerce")
            return normalized

    raise ValueError("feature data must include a 'close' column or a supported close-like variant.")


def load_symbol_feature_data(feature_dir: Path, symbol: str) -> pd.DataFrame:
    parquet_path = feature_dir / f"{symbol}.parquet"
    if not parquet_path.exists():
        raise FileNotFoundError(f"Feature file not found for {symbol}: {parquet_path}")

    df = normalize_timestamp_column(pd.read_parquet(parquet_path))
    df = normalize_close_column(df)

    if "symbol" not in df.columns:
        df["symbol"] = symbol

    return df
