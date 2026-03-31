from __future__ import annotations

from pathlib import Path

from trading_platform.data.providers.base import BarDataProvider
from trading_platform.data.validate import validate_bars
from trading_platform.ingestion.framework import (
    YahooEquityDailyIngestionAdapter,
    write_market_data_artifacts,
)
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
    adapter = YahooEquityDailyIngestionAdapter(provider=provider)
    raw_df = adapter.fetch_raw_bars(
        symbol=symbol,
        start=start,
        end=end,
        timeframe=interval,
    )

    raw_path = RAW_DATA_DIR / f"{symbol}.parquet"
    raw_df.to_parquet(raw_path)

    normalized_df = adapter.normalize_raw_bars(
        raw_frame=raw_df,
        symbol=symbol,
        timeframe=interval,
    )
    normalized_df = validate_bars(normalized_df)

    normalized_path = NORMALIZED_DATA_DIR / f"{symbol}.parquet"
    normalized_df.to_parquet(normalized_path, index=False)
    write_market_data_artifacts(
        raw_frame=raw_df.reset_index(drop=True),
        normalized_frame=normalized_df,
        symbol=symbol,
        provider=adapter.provider_name,
        asset_class=adapter.asset_class,
        timeframe=interval,
        metadata={
            "legacy_raw_path": str(raw_path),
            "legacy_normalized_path": str(normalized_path),
        },
    )

    return normalized_path
