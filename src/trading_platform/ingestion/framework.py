from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import pandas as pd

from trading_platform.data.normalize import normalize_yahoo_bars
from trading_platform.data.providers.base import BarDataProvider
from trading_platform.data.providers.yahoo import YahooBarDataProvider
from trading_platform.ingestion.contracts import MarketDataArtifactManifest
from trading_platform.ingestion.normalize import (
    normalize_market_data_frame as base_normalize_market_data_frame,
    normalize_symbol,
    normalize_timeframe,
)
from trading_platform.ingestion.validation import (
    validate_market_data_frame,
    write_market_data_validation_report,
)
from trading_platform.settings import DATA_DIR


MARKET_DATA_ARTIFACTS_DIR = DATA_DIR / "market_data"

def _dataset_name(symbol: str, timeframe: str) -> str:
    normalized_symbol = normalize_symbol(symbol)
    normalized_timeframe = normalize_timeframe(timeframe)
    if normalized_timeframe == "1d":
        return normalized_symbol
    return f"{normalized_symbol}__{normalized_timeframe}"


def _artifact_root(*, provider: str, asset_class: str, timeframe: str) -> Path:
    return MARKET_DATA_ARTIFACTS_DIR / str(asset_class) / str(provider) / normalize_timeframe(timeframe)


def build_market_data_artifact_paths(
    *,
    symbol: str,
    provider: str,
    asset_class: str,
    timeframe: str,
) -> dict[str, Path]:
    root = _artifact_root(provider=provider, asset_class=asset_class, timeframe=timeframe)
    root.mkdir(parents=True, exist_ok=True)
    stem = _dataset_name(symbol, timeframe)
    return {
        "raw_path": root / f"{stem}.raw.parquet",
        "normalized_path": root / f"{stem}.parquet",
        "manifest_path": root / f"{stem}.manifest.json",
        "validation_report_path": root / f"{stem}.validation.json",
    }


def _coerce_metadata(metadata: dict[str, Any] | None) -> dict[str, Any]:
    if metadata is None:
        return {}
    return {str(key): metadata[key] for key in sorted(metadata)}


def normalize_market_data_frame(
    frame: pd.DataFrame,
    *,
    symbol: str,
    timeframe: str,
    provider: str,
    asset_class: str,
    metadata: dict[str, Any] | None = None,
) -> pd.DataFrame:
    normalized = base_normalize_market_data_frame(
        frame,
        symbol=symbol,
        timeframe=timeframe,
        provider=provider,
        asset_class=asset_class,
        metadata=metadata,
    )
    normalized.attrs["metadata"] = _coerce_metadata(metadata)
    return normalized


def build_market_data_manifest(
    *,
    normalized_frame: pd.DataFrame,
    symbol: str,
    provider: str,
    asset_class: str,
    timeframe: str,
    raw_path: str | Path | None = None,
    normalized_path: str | Path | None = None,
    manifest_path: str | Path | None = None,
    metadata: dict[str, Any] | None = None,
    validation_report_path: str | Path | None = None,
) -> MarketDataArtifactManifest:
    timestamps = pd.to_datetime(normalized_frame["timestamp"], errors="coerce")
    start_timestamp = None if timestamps.empty else timestamps.min()
    end_timestamp = None if timestamps.empty else timestamps.max()
    merged_metadata = dict(normalized_frame.attrs.get("metadata") or {})
    merged_metadata.update(_coerce_metadata(metadata))
    return MarketDataArtifactManifest(
        symbol=normalize_symbol(symbol),
        timeframe=normalize_timeframe(timeframe),
        provider=str(provider),
        asset_class=str(asset_class),
        row_count=int(len(normalized_frame.index)),
        start_timestamp=start_timestamp.isoformat() if pd.notna(start_timestamp) else None,
        end_timestamp=end_timestamp.isoformat() if pd.notna(end_timestamp) else None,
        raw_path=str(raw_path) if raw_path is not None else None,
        normalized_path=str(normalized_path) if normalized_path is not None else None,
        manifest_path=str(manifest_path) if manifest_path is not None else None,
        validation_report_path=str(validation_report_path) if validation_report_path is not None else None,
        metadata=merged_metadata,
    )


def write_market_data_artifacts(
    *,
    raw_frame: pd.DataFrame,
    normalized_frame: pd.DataFrame,
    symbol: str,
    provider: str,
    asset_class: str,
    timeframe: str,
    metadata: dict[str, Any] | None = None,
) -> MarketDataArtifactManifest:
    paths = build_market_data_artifact_paths(
        symbol=symbol,
        provider=provider,
        asset_class=asset_class,
        timeframe=timeframe,
    )
    raw_frame.to_parquet(paths["raw_path"], index=False)
    normalized_frame.to_parquet(paths["normalized_path"], index=False)
    validation_report = validate_market_data_frame(
        normalized_frame,
        symbol=normalize_symbol(symbol),
        timeframe=normalize_timeframe(timeframe),
        provider=str(provider),
        asset_class=str(asset_class),
    )
    write_market_data_validation_report(
        output_path=paths["validation_report_path"],
        report=validation_report,
    )
    manifest = build_market_data_manifest(
        normalized_frame=normalized_frame,
        symbol=symbol,
        provider=provider,
        asset_class=asset_class,
        timeframe=timeframe,
        raw_path=paths["raw_path"],
        normalized_path=paths["normalized_path"],
        manifest_path=paths["manifest_path"],
        validation_report_path=paths["validation_report_path"],
        metadata=metadata,
    )
    paths["manifest_path"].write_text(json.dumps(manifest.to_dict(), indent=2), encoding="utf-8")
    return manifest


class MarketDataIngestionAdapter(Protocol):
    @property
    def provider_name(self) -> str:
        ...

    @property
    def asset_class(self) -> str:
        ...

    def fetch_raw_bars(
        self,
        *,
        symbol: str,
        start: str,
        end: str | None = None,
        timeframe: str = "1d",
    ) -> pd.DataFrame:
        ...

    def normalize_raw_bars(
        self,
        *,
        raw_frame: pd.DataFrame,
        symbol: str,
        timeframe: str,
    ) -> pd.DataFrame:
        ...


@dataclass
class YahooEquityDailyIngestionAdapter:
    provider: BarDataProvider | None = None

    @property
    def provider_name(self) -> str:
        return "yahoo"

    @property
    def asset_class(self) -> str:
        return "equity"

    def fetch_raw_bars(
        self,
        *,
        symbol: str,
        start: str,
        end: str | None = None,
        timeframe: str = "1d",
    ) -> pd.DataFrame:
        provider = self.provider or YahooBarDataProvider()
        return provider.fetch_bars(symbol=symbol, start=start, end=end, interval=timeframe)

    def normalize_raw_bars(
        self,
        *,
        raw_frame: pd.DataFrame,
        symbol: str,
        timeframe: str,
    ) -> pd.DataFrame:
        return normalize_yahoo_bars(raw_frame, symbol=symbol, timeframe=timeframe)


@dataclass
class CryptoIntradayIngestionScaffoldAdapter:
    provider_name_value: str = "crypto_scaffold"

    @property
    def provider_name(self) -> str:
        return self.provider_name_value

    @property
    def asset_class(self) -> str:
        return "crypto"

    def fetch_raw_bars(
        self,
        *,
        symbol: str,
        start: str,
        end: str | None = None,
        timeframe: str = "1m",
    ) -> pd.DataFrame:
        raise NotImplementedError(
            "Crypto intraday fetch is scaffold-only in G-01. Provide raw bars and call normalize_raw_bars()."
        )

    def normalize_raw_bars(
        self,
        *,
        raw_frame: pd.DataFrame,
        symbol: str,
        timeframe: str,
    ) -> pd.DataFrame:
        frame = raw_frame.copy()
        if "date" in frame.columns and "timestamp" not in frame.columns:
            frame["timestamp"] = frame["date"]
        return normalize_market_data_frame(
            frame,
            symbol=symbol,
            timeframe=timeframe,
            provider=self.provider_name,
            asset_class=self.asset_class,
        )
