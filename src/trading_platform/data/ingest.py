from __future__ import annotations

from pathlib import Path

from trading_platform.data.normalize import normalize_yahoo_bars
from trading_platform.data.providers.base import BarDataProvider
from trading_platform.data.providers.yahoo import YahooBarDataProvider
from trading_platform.data.validate import validate_bars
from trading_platform.settings import NORMALIZED_DATA_DIR, RAW_DATA_DIR


def ingest_symbol(
    symbol: str,
    start: str = "2010-01-01",
    end: str | None = None,
    interval: str = "1d",
    provider: BarDataProvider | None = None,
) -> Path:
    """
    Fetch raw bar data from a provider, save the raw snapshot, normalize it into
    canonical schema, validate it, and save normalized output.

    Returns the normalized parquet path.
    """
    provider = provider or YahooBarDataProvider()

    raw_df = provider.fetch_bars(
        symbol=symbol,
        start=start,
        end=end,
        interval=interval,
    )

    raw_path = RAW_DATA_DIR / f"{symbol}.parquet"
    raw_df.to_parquet(raw_path)

    # For now, normalization is Yahoo-specific because Yahoo is the only provider.
    # Later we can generalize this by adding provider-specific normalizers.
    normalized_df = normalize_yahoo_bars(raw_df, symbol=symbol)
    normalized_df = validate_bars(normalized_df)

    normalized_path = NORMALIZED_DATA_DIR / f"{symbol}.parquet"
    normalized_df.to_parquet(normalized_path, index=False)

    return normalized_path