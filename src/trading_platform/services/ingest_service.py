from __future__ import annotations

from pathlib import Path

from trading_platform.config.models import IngestConfig
from trading_platform.data.ingest import ingest_symbol
from trading_platform.data.providers.base import BarDataProvider


def run_ingest(
    config: IngestConfig,
    provider: BarDataProvider | None = None,
) -> Path:
    """
    Application/service-layer entry point for bar ingestion.
    """
    normalized_path = ingest_symbol(
        symbol=config.symbol,
        start=config.start,
        end=config.end,
        interval=config.interval,
        provider=provider,
    )
    return normalized_path