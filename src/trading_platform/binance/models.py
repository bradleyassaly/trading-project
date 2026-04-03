from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import yaml


def _as_str_list(values: list[Any] | tuple[Any, ...] | None) -> list[str]:
    return [str(value) for value in (values or [])]


def _project_relative(project_root: Path, path: str | Path) -> str:
    value = Path(path)
    if value.is_absolute():
        return str(value)
    return str(project_root / value)


@dataclass(frozen=True)
class BinanceHistoricalIngestConfig:
    symbols: tuple[str, ...] = ("BTCUSDT", "ETHUSDT")
    intervals: tuple[str, ...] = ("1m", "5m")
    start: str = "2024-01-01T00:00:00Z"
    end: str | None = None
    kline_limit: int = 1000
    agg_trade_limit: int = 1000
    request_sleep_sec: float = 0.1
    max_retries: int = 5
    backoff_base_sec: float = 0.5
    backoff_max_sec: float = 8.0
    websocket_incremental_enabled: bool = False
    capture_book_ticker: bool = True
    normalize_after_ingest: bool = True
    raw_root: str = "data/binance/raw"
    normalized_root: str = "data/binance/normalized"
    checkpoint_path: str = "data/binance/raw/ingest_checkpoint.json"
    summary_path: str = "data/binance/raw/ingest_summary.json"
    exchange_info_path: str = "data/binance/raw/exchange_info.json"

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbols", tuple(sorted({str(symbol).upper() for symbol in self.symbols})))
        object.__setattr__(self, "intervals", tuple(str(interval) for interval in self.intervals))
        if not self.symbols:
            raise ValueError("At least one Binance symbol is required")
        if not self.intervals:
            raise ValueError("At least one Binance kline interval is required")
        if self.kline_limit <= 0 or self.agg_trade_limit <= 0:
            raise ValueError("Pagination limits must be positive")
        if self.max_retries < 0:
            raise ValueError("max_retries must be >= 0")

    def to_cli_defaults(self) -> dict[str, Any]:
        return {
            "symbols": list(self.symbols),
            "intervals": list(self.intervals),
            "start": self.start,
            "end": self.end,
            "kline_limit": self.kline_limit,
            "agg_trade_limit": self.agg_trade_limit,
            "request_sleep_sec": self.request_sleep_sec,
            "max_retries": self.max_retries,
            "backoff_base_sec": self.backoff_base_sec,
            "backoff_max_sec": self.backoff_max_sec,
            "websocket_incremental_enabled": self.websocket_incremental_enabled,
            "capture_book_ticker": self.capture_book_ticker,
            "normalize_after_ingest": self.normalize_after_ingest,
            "raw_root": self.raw_root,
            "normalized_root": self.normalized_root,
            "checkpoint_path": self.checkpoint_path,
            "summary_path": self.summary_path,
            "exchange_info_path": self.exchange_info_path,
        }

    @classmethod
    def from_mapping(cls, mapping: dict[str, Any], *, project_root: Path) -> "BinanceHistoricalIngestConfig":
        crypto_cfg = dict(mapping.get("crypto", {}) or {})
        binance_cfg = dict(crypto_cfg.get("binance", {}) or {})
        ingest_cfg = dict(binance_cfg.get("historical_ingest", {}) or {})
        provider_cfg = dict(binance_cfg.get("provider", {}) or {})
        output_cfg = dict(binance_cfg.get("outputs", {}) or {})
        websocket_cfg = dict(binance_cfg.get("websocket", {}) or {})
        return cls(
            symbols=tuple(_as_str_list(provider_cfg.get("symbols") or ingest_cfg.get("symbols") or ("BTCUSDT", "ETHUSDT"))),
            intervals=tuple(_as_str_list(provider_cfg.get("intervals") or ingest_cfg.get("intervals") or ("1m", "5m"))),
            start=str(ingest_cfg.get("start") or "2024-01-01T00:00:00Z"),
            end=str(ingest_cfg["end"]) if ingest_cfg.get("end") is not None else None,
            kline_limit=int(ingest_cfg.get("kline_limit", 1000)),
            agg_trade_limit=int(ingest_cfg.get("agg_trade_limit", 1000)),
            request_sleep_sec=float(provider_cfg.get("request_sleep_sec", 0.1)),
            max_retries=int(provider_cfg.get("max_retries", 5)),
            backoff_base_sec=float(provider_cfg.get("backoff_base_sec", 0.5)),
            backoff_max_sec=float(provider_cfg.get("backoff_max_sec", 8.0)),
            websocket_incremental_enabled=bool(websocket_cfg.get("enabled", False)),
            capture_book_ticker=bool(ingest_cfg.get("capture_book_ticker", True)),
            normalize_after_ingest=bool(ingest_cfg.get("normalize_after_ingest", True)),
            raw_root=_project_relative(project_root, output_cfg.get("raw_root", "data/binance/raw")),
            normalized_root=_project_relative(project_root, output_cfg.get("normalized_root", "data/binance/normalized")),
            checkpoint_path=_project_relative(
                project_root,
                output_cfg.get("checkpoint_path", "data/binance/raw/ingest_checkpoint.json"),
            ),
            summary_path=_project_relative(
                project_root,
                output_cfg.get("summary_path", "data/binance/raw/ingest_summary.json"),
            ),
            exchange_info_path=_project_relative(
                project_root,
                output_cfg.get("exchange_info_path", "data/binance/raw/exchange_info.json"),
            ),
        )

    @classmethod
    def from_yaml(cls, path: str | Path, *, project_root: Path) -> "BinanceHistoricalIngestConfig":
        payload = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
        return cls.from_mapping(payload, project_root=project_root)


@dataclass(frozen=True)
class BinanceNormalizeConfig:
    raw_root: str = "data/binance/raw"
    normalized_root: str = "data/binance/normalized"
    symbols: tuple[str, ...] = ()
    intervals: tuple[str, ...] = ()
    summary_path: str = "data/binance/normalized/normalization_summary.json"

    def to_cli_defaults(self) -> dict[str, Any]:
        return {
            "raw_root": self.raw_root,
            "normalized_root": self.normalized_root,
            "symbols": list(self.symbols),
            "intervals": list(self.intervals),
            "summary_path": self.summary_path,
        }

    @classmethod
    def from_ingest_config(cls, config: BinanceHistoricalIngestConfig) -> "BinanceNormalizeConfig":
        return cls(
            raw_root=config.raw_root,
            normalized_root=config.normalized_root,
            symbols=config.symbols,
            intervals=config.intervals,
            summary_path=str(Path(config.normalized_root) / "normalization_summary.json"),
        )

    @classmethod
    def from_yaml(cls, path: str | Path, *, project_root: Path) -> "BinanceNormalizeConfig":
        ingest = BinanceHistoricalIngestConfig.from_yaml(path, project_root=project_root)
        return cls.from_ingest_config(ingest)


@dataclass(frozen=True)
class BinanceWebsocketIngestConfig:
    enabled: bool = False
    stream_families: tuple[str, ...] = ("kline", "agg_trade", "book_ticker")
    symbols: tuple[str, ...] = ("BTCUSDT", "ETHUSDT")
    intervals: tuple[str, ...] = ("1m", "5m")
    combined_stream: bool = True
    base_url: str = "wss://data-stream.binance.vision"
    max_streams_per_connection: int = 1024
    ping_interval_sec: float = 20.0
    ping_timeout_sec: float = 20.0
    receive_timeout_sec: float = 30.0
    reconnect_backoff_base_sec: float = 1.0
    reconnect_backoff_max_sec: float = 30.0
    max_reconnect_attempts: int = 10
    max_runtime_seconds: int | None = None
    max_messages: int | None = None
    raw_incremental_root: str = "data/binance/raw/websocket"
    normalized_incremental_root: str = "data/binance/normalized/incremental"
    checkpoint_path: str = "data/binance/raw/websocket_checkpoint.json"
    summary_path: str = "data/binance/raw/websocket_summary.json"
    projection_output_root: str = "data/binance/projections"
    refresh_projection_after_ingest: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbols", tuple(sorted({str(symbol).upper() for symbol in self.symbols})))
        object.__setattr__(self, "intervals", tuple(str(interval) for interval in self.intervals))
        object.__setattr__(self, "stream_families", tuple(str(family) for family in self.stream_families))
        if not self.symbols:
            raise ValueError("At least one Binance websocket symbol is required")
        if "kline" in self.stream_families and not self.intervals:
            raise ValueError("Kline websocket ingest requires at least one interval")
        if len(self.stream_names()) > self.max_streams_per_connection:
            raise ValueError("Configured Binance websocket streams exceed max_streams_per_connection")

    def stream_names(self) -> list[str]:
        names: list[str] = []
        symbol_stream = [symbol.lower() for symbol in self.symbols]
        if "agg_trade" in self.stream_families:
            names.extend(f"{symbol}@aggTrade" for symbol in symbol_stream)
        if "book_ticker" in self.stream_families:
            names.extend(f"{symbol}@bookTicker" for symbol in symbol_stream)
        if "kline" in self.stream_families:
            for symbol in symbol_stream:
                for interval in self.intervals:
                    names.append(f"{symbol}@kline_{interval}")
        return names

    def to_cli_defaults(self) -> dict[str, Any]:
        return {
            "websocket_enabled": self.enabled,
            "stream_families": list(self.stream_families),
            "symbols": list(self.symbols),
            "intervals": list(self.intervals),
            "combined_stream": self.combined_stream,
            "max_runtime_seconds": self.max_runtime_seconds,
            "max_messages": self.max_messages,
            "raw_incremental_root": self.raw_incremental_root,
            "normalized_incremental_root": self.normalized_incremental_root,
            "checkpoint_path": self.checkpoint_path,
            "summary_path": self.summary_path,
            "projection_output_root": self.projection_output_root,
            "refresh_projection_after_ingest": self.refresh_projection_after_ingest,
            "reconnect_backoff_base_sec": self.reconnect_backoff_base_sec,
            "reconnect_backoff_max_sec": self.reconnect_backoff_max_sec,
            "max_reconnect_attempts": self.max_reconnect_attempts,
            "receive_timeout_sec": self.receive_timeout_sec,
        }

    @classmethod
    def from_mapping(cls, mapping: dict[str, Any], *, project_root: Path) -> "BinanceWebsocketIngestConfig":
        crypto_cfg = dict(mapping.get("crypto", {}) or {})
        binance_cfg = dict(crypto_cfg.get("binance", {}) or {})
        provider_cfg = dict(binance_cfg.get("provider", {}) or {})
        websocket_cfg = dict(binance_cfg.get("websocket", {}) or {})
        output_cfg = dict(binance_cfg.get("outputs", {}) or {})
        return cls(
            enabled=bool(websocket_cfg.get("enabled", False)),
            stream_families=tuple(_as_str_list(websocket_cfg.get("stream_families") or ("kline", "agg_trade", "book_ticker"))),
            symbols=tuple(_as_str_list(provider_cfg.get("symbols") or websocket_cfg.get("symbols") or ("BTCUSDT", "ETHUSDT"))),
            intervals=tuple(_as_str_list(provider_cfg.get("intervals") or websocket_cfg.get("intervals") or ("1m", "5m"))),
            combined_stream=bool(websocket_cfg.get("combined_stream", True)),
            base_url=str(websocket_cfg.get("base_url", "wss://data-stream.binance.vision")),
            max_streams_per_connection=int(websocket_cfg.get("max_streams_per_connection", 1024)),
            ping_interval_sec=float(websocket_cfg.get("ping_interval_sec", 20.0)),
            ping_timeout_sec=float(websocket_cfg.get("ping_timeout_sec", 20.0)),
            receive_timeout_sec=float(websocket_cfg.get("receive_timeout_sec", 30.0)),
            reconnect_backoff_base_sec=float(websocket_cfg.get("reconnect_backoff_base_sec", 1.0)),
            reconnect_backoff_max_sec=float(websocket_cfg.get("reconnect_backoff_max_sec", 30.0)),
            max_reconnect_attempts=int(websocket_cfg.get("max_reconnect_attempts", 10)),
            max_runtime_seconds=int(websocket_cfg["max_runtime_seconds"]) if websocket_cfg.get("max_runtime_seconds") is not None else None,
            max_messages=int(websocket_cfg["max_messages"]) if websocket_cfg.get("max_messages") is not None else None,
            raw_incremental_root=_project_relative(project_root, output_cfg.get("raw_incremental_root", "data/binance/raw/websocket")),
            normalized_incremental_root=_project_relative(project_root, output_cfg.get("normalized_incremental_root", "data/binance/normalized/incremental")),
            checkpoint_path=_project_relative(project_root, output_cfg.get("websocket_checkpoint_path", "data/binance/raw/websocket_checkpoint.json")),
            summary_path=_project_relative(project_root, output_cfg.get("websocket_summary_path", "data/binance/raw/websocket_summary.json")),
            projection_output_root=_project_relative(project_root, output_cfg.get("projection_output_root", "data/binance/projections")),
            refresh_projection_after_ingest=bool(websocket_cfg.get("refresh_projection_after_ingest", True)),
        )

    @classmethod
    def from_yaml(cls, path: str | Path, *, project_root: Path) -> "BinanceWebsocketIngestConfig":
        payload = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
        return cls.from_mapping(payload, project_root=project_root)


@dataclass(frozen=True)
class BinanceProjectionConfig:
    historical_normalized_root: str = "data/binance/normalized"
    incremental_normalized_root: str = "data/binance/normalized/incremental"
    output_root: str = "data/binance/projections"
    summary_path: str = "data/binance/projections/projection_summary.json"
    symbols: tuple[str, ...] = ()
    intervals: tuple[str, ...] = ()

    def to_cli_defaults(self) -> dict[str, Any]:
        return {
            "historical_normalized_root": self.historical_normalized_root,
            "incremental_normalized_root": self.incremental_normalized_root,
            "output_root": self.output_root,
            "summary_path": self.summary_path,
            "symbols": list(self.symbols),
            "intervals": list(self.intervals),
        }

    @classmethod
    def from_mapping(cls, mapping: dict[str, Any], *, project_root: Path) -> "BinanceProjectionConfig":
        historical = BinanceHistoricalIngestConfig.from_mapping(mapping, project_root=project_root)
        websocket = BinanceWebsocketIngestConfig.from_mapping(mapping, project_root=project_root)
        return cls(
            historical_normalized_root=historical.normalized_root,
            incremental_normalized_root=websocket.normalized_incremental_root,
            output_root=websocket.projection_output_root,
            summary_path=str(Path(websocket.projection_output_root) / "projection_summary.json"),
            symbols=historical.symbols,
            intervals=historical.intervals,
        )

    @classmethod
    def from_yaml(cls, path: str | Path, *, project_root: Path) -> "BinanceProjectionConfig":
        payload = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
        return cls.from_mapping(payload, project_root=project_root)


@dataclass
class BinanceHistoricalIngestSummary:
    symbols_requested: list[str]
    symbols_validated: list[str]
    intervals: list[str]
    start: str
    end: str | None
    exchange_info_path: str
    raw_root: str
    normalized_root: str
    request_count: int = 0
    retry_count: int = 0
    pages_fetched: int = 0
    raw_artifacts_written: int = 0
    kline_rows_fetched: int = 0
    agg_trade_rows_fetched: int = 0
    book_ticker_snapshots_fetched: int = 0
    skipped_symbols: list[str] = field(default_factory=list)
    failures: list[dict[str, Any]] = field(default_factory=list)
    rate_limits: list[dict[str, Any]] = field(default_factory=list)
    per_symbol: dict[str, dict[str, Any]] = field(default_factory=dict)
    normalization_summary_path: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return json.loads(json.dumps(asdict(self), default=str))


@dataclass(frozen=True)
class BinanceNormalizeResult:
    kline_files_written: int
    agg_trade_files_written: int
    book_ticker_files_written: int
    total_kline_rows: int
    total_agg_trade_rows: int
    total_book_ticker_rows: int
    summary_path: str
    output_paths: dict[str, list[str]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "kline_files_written": self.kline_files_written,
            "agg_trade_files_written": self.agg_trade_files_written,
            "book_ticker_files_written": self.book_ticker_files_written,
            "total_kline_rows": self.total_kline_rows,
            "total_agg_trade_rows": self.total_agg_trade_rows,
            "total_book_ticker_rows": self.total_book_ticker_rows,
            "summary_path": self.summary_path,
            "output_paths": {key: list(value) for key, value in self.output_paths.items()},
        }


@dataclass(frozen=True)
class BinanceWebsocketIngestResult:
    summary_path: str
    checkpoint_path: str
    raw_incremental_root: str
    normalized_incremental_root: str
    messages_processed: int
    messages_written: int
    duplicates_dropped: int
    reconnect_count: int
    warnings: list[str]
    failures: list[dict[str, Any]]
    projection_summary_path: str | None = None


@dataclass(frozen=True)
class BinanceProjectionResult:
    summary_path: str
    output_paths: dict[str, str]
    row_counts: dict[str, int]


@dataclass(frozen=True)
class BinanceFeatureConfig:
    projection_root: str = "data/binance/projections"
    features_root: str = "data/binance/features"
    feature_store_root: str = "data/feature_store"
    summary_path: str = "data/binance/features/feature_refresh_summary.json"
    symbols: tuple[str, ...] = ()
    intervals: tuple[str, ...] = ()
    return_horizons: tuple[int, ...] = (1, 5, 15)
    volatility_windows: tuple[int, ...] = (5, 15, 30)
    volume_windows: tuple[int, ...] = (5, 15, 30)
    order_book_windows: tuple[int, ...] = (5, 15)
    trade_intensity_windows: tuple[int, ...] = (5, 15)
    rebuild_lookback_rows: int = 5000
    incremental_refresh: bool = True

    def to_cli_defaults(self) -> dict[str, Any]:
        return {
            "projection_root": self.projection_root,
            "features_root": self.features_root,
            "feature_store_root": self.feature_store_root,
            "summary_path": self.summary_path,
            "symbols": list(self.symbols),
            "intervals": list(self.intervals),
            "return_horizons": list(self.return_horizons),
            "volatility_windows": list(self.volatility_windows),
            "volume_windows": list(self.volume_windows),
            "order_book_windows": list(self.order_book_windows),
            "trade_intensity_windows": list(self.trade_intensity_windows),
            "rebuild_lookback_rows": self.rebuild_lookback_rows,
            "incremental_refresh": self.incremental_refresh,
        }

    @classmethod
    def from_mapping(cls, mapping: dict[str, Any], *, project_root: Path) -> "BinanceFeatureConfig":
        projection = BinanceProjectionConfig.from_mapping(mapping, project_root=project_root)
        crypto_cfg = dict(mapping.get("crypto", {}) or {})
        binance_cfg = dict(crypto_cfg.get("binance", {}) or {})
        features_cfg = dict(binance_cfg.get("features", {}) or {})
        return cls(
            projection_root=projection.output_root,
            features_root=_project_relative(project_root, features_cfg.get("features_root", "data/binance/features")),
            feature_store_root=_project_relative(project_root, features_cfg.get("feature_store_root", "data/feature_store")),
            summary_path=_project_relative(project_root, features_cfg.get("summary_path", "data/binance/features/feature_refresh_summary.json")),
            symbols=tuple(_as_str_list(features_cfg.get("symbols") or projection.symbols)),
            intervals=tuple(_as_str_list(features_cfg.get("intervals") or projection.intervals)),
            return_horizons=tuple(int(value) for value in (features_cfg.get("return_horizons") or (1, 5, 15))),
            volatility_windows=tuple(int(value) for value in (features_cfg.get("volatility_windows") or (5, 15, 30))),
            volume_windows=tuple(int(value) for value in (features_cfg.get("volume_windows") or (5, 15, 30))),
            order_book_windows=tuple(int(value) for value in (features_cfg.get("order_book_windows") or (5, 15))),
            trade_intensity_windows=tuple(int(value) for value in (features_cfg.get("trade_intensity_windows") or (5, 15))),
            rebuild_lookback_rows=int(features_cfg.get("rebuild_lookback_rows", 5000)),
            incremental_refresh=bool(features_cfg.get("incremental_refresh", True)),
        )

    @classmethod
    def from_yaml(cls, path: str | Path, *, project_root: Path) -> "BinanceFeatureConfig":
        payload = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
        return cls.from_mapping(payload, project_root=project_root)


@dataclass(frozen=True)
class BinanceFeatureResult:
    summary_path: str
    features_path: str
    slice_paths: list[str]
    feature_store_manifest_paths: list[str]
    rows_written: int
    artifacts_written: int
    symbols: list[str]
    intervals: list[str]


@dataclass(frozen=True)
class BinanceSyncConfig:
    websocket: BinanceWebsocketIngestConfig
    projection: BinanceProjectionConfig
    features: BinanceFeatureConfig
    symbols: tuple[str, ...]
    intervals: tuple[str, ...]
    stream_families: tuple[str, ...]
    skip_projection: bool = False
    skip_features: bool = False
    max_runtime_seconds: int | None = None
    max_messages: int | None = None
    full_feature_rebuild: bool = False
    sync_summary_path: str = "data/binance/sync/sync_summary.json"

    def to_cli_defaults(self) -> dict[str, Any]:
        payload = {}
        payload.update(self.websocket.to_cli_defaults())
        payload.update(self.projection.to_cli_defaults())
        payload.update(self.features.to_cli_defaults())
        payload["skip_projection"] = self.skip_projection
        payload["skip_features"] = self.skip_features
        payload["symbols"] = list(self.symbols)
        payload["intervals"] = list(self.intervals)
        payload["stream_families"] = list(self.stream_families)
        payload["max_runtime_seconds"] = self.max_runtime_seconds
        payload["max_messages"] = self.max_messages
        payload["full_feature_rebuild"] = self.full_feature_rebuild
        payload["sync_summary_path"] = self.sync_summary_path
        return payload

    @classmethod
    def from_mapping(cls, mapping: dict[str, Any], *, project_root: Path) -> "BinanceSyncConfig":
        crypto_cfg = dict(mapping.get("crypto", {}) or {})
        binance_cfg = dict(crypto_cfg.get("binance", {}) or {})
        sync_cfg = dict(binance_cfg.get("sync", {}) or {})
        symbol_override = tuple(_as_str_list(sync_cfg.get("symbols") or ()))
        interval_override = tuple(_as_str_list(sync_cfg.get("intervals") or ()))
        stream_override = tuple(_as_str_list(sync_cfg.get("stream_families") or ()))
        websocket = BinanceWebsocketIngestConfig.from_mapping(mapping, project_root=project_root)
        if symbol_override:
            websocket = BinanceWebsocketIngestConfig(**{**websocket.__dict__, "symbols": symbol_override})
        if interval_override:
            websocket = BinanceWebsocketIngestConfig(**{**websocket.__dict__, "intervals": interval_override})
        if stream_override:
            websocket = BinanceWebsocketIngestConfig(**{**websocket.__dict__, "stream_families": stream_override})
        websocket = BinanceWebsocketIngestConfig(
            **{
                **websocket.__dict__,
                "max_runtime_seconds": int(sync_cfg["max_runtime_seconds"])
                if sync_cfg.get("max_runtime_seconds") is not None
                else websocket.max_runtime_seconds,
                "max_messages": int(sync_cfg["max_messages"]) if sync_cfg.get("max_messages") is not None else websocket.max_messages,
            }
        )
        projection = BinanceProjectionConfig.from_mapping(mapping, project_root=project_root)
        if symbol_override:
            projection = BinanceProjectionConfig(**{**projection.__dict__, "symbols": symbol_override})
        if interval_override:
            projection = BinanceProjectionConfig(**{**projection.__dict__, "intervals": interval_override})
        features = BinanceFeatureConfig.from_mapping(mapping, project_root=project_root)
        if symbol_override:
            features = BinanceFeatureConfig(**{**features.__dict__, "symbols": symbol_override})
        if interval_override:
            features = BinanceFeatureConfig(**{**features.__dict__, "intervals": interval_override})
        return cls(
            websocket=websocket,
            projection=projection,
            features=features,
            symbols=tuple(symbol_override or websocket.symbols),
            intervals=tuple(interval_override or projection.intervals),
            stream_families=tuple(stream_override or websocket.stream_families),
            skip_projection=bool(sync_cfg.get("skip_projection", False)),
            skip_features=bool(sync_cfg.get("skip_features", False)),
            max_runtime_seconds=int(sync_cfg["max_runtime_seconds"]) if sync_cfg.get("max_runtime_seconds") is not None else websocket.max_runtime_seconds,
            max_messages=int(sync_cfg["max_messages"]) if sync_cfg.get("max_messages") is not None else websocket.max_messages,
            full_feature_rebuild=bool(sync_cfg.get("full_feature_rebuild", False)),
            sync_summary_path=_project_relative(project_root, sync_cfg.get("summary_path", "data/binance/sync/sync_summary.json")),
        )

    @classmethod
    def from_yaml(cls, path: str | Path, *, project_root: Path) -> "BinanceSyncConfig":
        payload = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
        return cls.from_mapping(payload, project_root=project_root)


@dataclass(frozen=True)
class BinanceSyncResult:
    summary_path: str
    websocket_summary_path: str | None
    projection_summary_path: str | None
    feature_summary_path: str | None
    status: str
    step_statuses: dict[str, str]
