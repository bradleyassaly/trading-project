from __future__ import annotations

import pandas as pd

from trading_platform.ingestion.normalize import normalize_market_data_frame
from trading_platform.schemas.bars import BAR_COLUMNS


YAHOO_COLUMN_RENAMES = {
    "Date": "timestamp",
    "Datetime": "timestamp",
    "Open": "open",
    "High": "high",
    "Low": "low",
    "Close": "close",
    "Adj Close": "adj_close",
    "Volume": "volume",
}


def _flatten_yahoo_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Flatten yfinance MultiIndex columns into a simple one-level column index.

    Tries to preserve the OHLCV level if present.
    """
    if not isinstance(df.columns, pd.MultiIndex):
        return df

    for level in range(df.columns.nlevels):
        values = set(df.columns.get_level_values(level))
        if {"Open", "High", "Low", "Close", "Volume"}.issubset(values):
            out = df.copy()
            out.columns = df.columns.get_level_values(level)
            return out

    out = df.copy()
    out.columns = [
        col[0] if isinstance(col, tuple) else col
        for col in df.columns
    ]
    return out


def normalize_yahoo_bars(df: pd.DataFrame, symbol: str, *, timeframe: str = "1d") -> pd.DataFrame:
    """
    Convert raw yfinance output into the platform's canonical bar schema.
    """
    if df.empty:
        raise ValueError(f"No data returned for {symbol}")

    df = _flatten_yahoo_columns(df).copy()
    df = df.rename(columns=YAHOO_COLUMN_RENAMES)

    if "timestamp" not in df.columns:
        df["timestamp"] = pd.to_datetime(df.index)

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=False)
    df["symbol"] = symbol

    required = {"open", "high", "low", "close", "volume"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(
            f"Missing required columns after normalization for {symbol}: {missing}. "
            f"Available columns: {list(df.columns)}"
        )

    out = df.reset_index(drop=True)

    # Keep only canonical columns for now.
    out = out[BAR_COLUMNS].copy()

    out["symbol"] = out["symbol"].astype(str)
    out = out.sort_values(["timestamp", "symbol"]).reset_index(drop=True)

    return normalize_market_data_frame(
        out,
        symbol=symbol,
        timeframe=timeframe,
        provider="yahoo",
        asset_class="equity",
    )

def normalize_bars(
    df: pd.DataFrame,
    symbol: str,
    provider_name: str,
) -> pd.DataFrame:
    if provider_name == "yahoo":
        return normalize_yahoo_bars(df, symbol=symbol)

    raise ValueError(f"Unsupported provider_name for normalization: {provider_name}")
