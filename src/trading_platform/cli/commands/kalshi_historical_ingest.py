"""
CLI command: trading-cli data kalshi historical-ingest

Downloads all resolved Kalshi markets from the past year via the public
historical API (no auth required), builds trade parquets, computes feature
parquets (including base rate and optional Metaculus features), and writes
a run manifest.

Usage
-----
    trading-cli data kalshi historical-ingest
    trading-cli data kalshi historical-ingest --config configs/kalshi.yaml
    trading-cli data kalshi historical-ingest --lookback-days 180
    trading-cli data kalshi historical-ingest --no-base-rate
    trading-cli data kalshi historical-ingest --metaculus --sleep 0.3
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import yaml
from trading_platform.kalshi.client import KalshiGetRetryPolicy
from trading_platform.kalshi.auth import KalshiConfig
from trading_platform.kalshi.historical_ingest import HistoricalIngestConfig
from trading_platform.cli.commands.kalshi_validate_dataset import build_kalshi_validation_config
from trading_platform.kalshi.validation import run_kalshi_data_validation

logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[4]


def _load_yaml(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _project_path(*parts: str) -> Path:
    return PROJECT_ROOT.joinpath(*parts)


def _project_relative_path(path_str: str) -> Path:
    path = Path(path_str)
    return path if path.is_absolute() else PROJECT_ROOT / path


def _prepare_output_paths(config: HistoricalIngestConfig) -> None:
    for directory in (
        Path(config.raw_markets_dir),
        Path(config.raw_trades_dir),
        Path(config.raw_candles_dir),
        Path(config.trades_parquet_dir),
        Path(config.normalized_candles_dir),
        Path(config.features_dir),
        Path(config.normalized_markets_path).parent,
        Path(config.resolution_csv_path).parent,
        Path(config.legacy_resolution_csv_path).parent,
        Path(config.manifest_path).parent,
        Path(config.checkpoint_path).parent,
        Path(config.summary_path).parent,
        Path(config.base_rate_db_path).parent,
        Path(config.status_artifacts_root),
    ):
        directory.mkdir(parents=True, exist_ok=True)


def _force_live_reads(config: KalshiConfig) -> KalshiConfig:
    if not config.demo:
        return config
    return type(config)(
        api_key_id=config.api_key_id,
        private_key_pem=config.private_key_pem,
        demo=False,
        private_key_path=config.private_key_path,
    )


def cmd_kalshi_historical_ingest(args: argparse.Namespace) -> None:
    from trading_platform.kalshi.client import KalshiClient
    from trading_platform.kalshi.historical_ingest import HistoricalIngestConfig, HistoricalIngestPipeline

    # Load YAML config for defaults; CLI flags override
    cfg_raw: dict[str, Any] = {}
    if getattr(args, "config", None):
        try:
            cfg_raw = _load_yaml(args.config)
        except (OSError, Exception) as exc:
            print(f"[WARN] Could not load config {args.config}: {exc}")

    hist_cfg = cfg_raw.get("historical_ingest", {})
    ingestion_cfg = cfg_raw.get("ingestion", {})

    # Build HistoricalIngestConfig from project-root-relative paths + CLI overrides.
    config = HistoricalIngestConfig(
        raw_markets_dir=str(_project_path("data", "kalshi", "raw", "markets")),
        raw_trades_dir=str(_project_path("data", "kalshi", "raw", "trades")),
        raw_candles_dir=str(_project_path("data", "kalshi", "raw", "candles")),
        trades_parquet_dir=str(_project_path("data", "kalshi", "normalized", "trades")),
        normalized_candles_dir=str(_project_path("data", "kalshi", "normalized", "candles")),
        normalized_markets_path=str(_project_path("data", "kalshi", "normalized", "markets.parquet")),
        features_dir=(
            str(_project_relative_path(getattr(args, "output_dir")))
            if getattr(args, "output_dir", None)
            else str(_project_path("data", "kalshi", "features", "real"))
        ),
        resolution_csv_path=str(_project_path("data", "kalshi", "normalized", "resolution.csv")),
        legacy_resolution_csv_path=str(_project_path("data", "kalshi", "resolution.csv")),
        manifest_path=str(_project_path("data", "kalshi", "raw", "ingest_manifest.json")),
        checkpoint_path=str(_project_path("data", "kalshi", "raw", "ingest_checkpoint.json")),
        summary_path=str(_project_path("data", "kalshi", "raw", "ingest_summary.json")),
        status_artifacts_root=str(_project_relative_path(hist_cfg.get("status_artifacts_root", "artifacts/kalshi_ingest"))),
        checkpoint_backup_path=str(_project_path("data", "kalshi", "raw", "ingest_checkpoint.bak.json")),
        lookback_days=int(getattr(args, "lookback_days", None) or ingestion_cfg.get("backfill_days", 365)),
        feature_period=getattr(args, "period", None) or hist_cfg.get("feature_period", "1h"),
        min_trades=5,
        request_sleep_sec=float(getattr(args, "sleep", None) or ingestion_cfg.get("request_sleep_sec", 0.05)),
        authenticated_request_sleep_sec=float(hist_cfg.get("authenticated_request_sleep_sec", 0.072)),
        authenticated_rate_limit_max_retries=int(hist_cfg.get("authenticated_rate_limit_max_retries", 5)),
        authenticated_rate_limit_backoff_base_sec=float(hist_cfg.get("authenticated_rate_limit_backoff_base_sec", 0.5)),
        authenticated_rate_limit_backoff_max_sec=float(hist_cfg.get("authenticated_rate_limit_backoff_max_sec", 8.0)),
        authenticated_rate_limit_jitter_max_sec=float(hist_cfg.get("authenticated_rate_limit_jitter_max_sec", 0.25)),
        max_live_pages_without_retained_markets=int(hist_cfg.get("max_live_pages_without_retained_markets", 25)),
        max_raw_markets_without_processing=int(hist_cfg.get("max_raw_markets_without_processing", 2000)),
        resume_cursor_max_retries=int(hist_cfg.get("resume_cursor_max_retries", 3)),
        resume_cursor_backoff_base_sec=float(hist_cfg.get("resume_cursor_backoff_base_sec", 1.0)),
        resume_cursor_backoff_max_sec=float(hist_cfg.get("resume_cursor_backoff_max_sec", 10.0)),
        resume_cursor_jitter_max_sec=float(hist_cfg.get("resume_cursor_jitter_max_sec", 0.5)),
        resume_recovery_mode=str(getattr(args, "resume_recovery_mode", None) or hist_cfg.get("resume_recovery_mode", "automatic")),
        run_base_rate=not getattr(args, "no_base_rate", False),
        base_rate_db_path=str(_project_path("data", "kalshi", "base_rates", "base_rate_db.json")),
        run_metaculus=getattr(args, "metaculus", False),
        metaculus_matches_path=str(_project_path("data", "kalshi", "metaculus", "matches.json")),
        metaculus_min_confidence=float(hist_cfg.get("metaculus_min_confidence", 0.70)),
        market_page_size=1000,
        trade_page_size=1000,
        ticker_filter=list(getattr(args, "tickers", None) or []),
        resume=not getattr(args, "fresh_run", False),
        resume_mode=(
            "fresh"
            if getattr(args, "fresh_run", False)
            else ("explicit" if getattr(args, "resume_from_checkpoint", None) else "latest")
        ),
        resume_checkpoint_path=(
            str(_project_relative_path(getattr(args, "resume_from_checkpoint")))
            if getattr(args, "resume_from_checkpoint", None)
            else None
        ),
        excluded_series_patterns=list(hist_cfg.get("excluded_series_patterns") or []),
        max_markets_per_event=int(hist_cfg.get("max_markets_per_event") or 0),
        min_volume=float(hist_cfg.get("min_volume") or 0.0),
        preferred_categories=list(hist_cfg.get("preferred_categories") or []),
        use_events_for_category_filter=bool(hist_cfg.get("use_events_for_category_filter", True)),
        skip_historical_pagination=bool(hist_cfg.get("skip_historical_pagination", True)),
        use_direct_series_fetch=bool(hist_cfg.get("use_direct_series_fetch", True)),
        direct_series_tickers=list(hist_cfg.get("direct_series_tickers") or []),
    )
    _prepare_output_paths(config)

    print("Kalshi Historical Ingest")
    print(f"  lookback : {config.lookback_days} days")
    print(f"  period   : {config.feature_period}")
    print(f"  sleep    : {config.request_sleep_sec}s/request")
    print(f"  live sleep: {config.authenticated_request_sleep_sec}s/request")
    print(f"  live retries: {config.authenticated_rate_limit_max_retries}")
    print(f"  fail-fast pages: {config.max_live_pages_without_retained_markets}")
    print(f"  fail-fast retained/raw: {config.max_raw_markets_without_processing}")
    print(f"  resume cursor retries: {config.resume_cursor_max_retries}")
    print(f"  resume recovery: {config.resume_recovery_mode}")
    print(f"  base rate: {'enabled' if config.run_base_rate else 'disabled'}")
    print(f"  metaculus: {'enabled' if config.run_metaculus else 'disabled'}")
    print(f"  raw      : {config.raw_markets_dir}")
    print(f"  normalized: {config.trades_parquet_dir}")
    print(f"  features : {config.features_dir}")
    print(f"  status   : {config.status_artifacts_root}")
    print(f"  resume   : {config.resume_mode}")
    if config.resume_checkpoint_path:
        print(f"  resume checkpoint: {config.resume_checkpoint_path}")
    if config.ticker_filter:
        print(f"  tickers  : {', '.join(config.ticker_filter)}")
    if config.preferred_categories:
        print(f"  categories: {', '.join(config.preferred_categories)}")
    if config.excluded_series_patterns:
        print(f"  excluded : {', '.join(config.excluded_series_patterns)}")
    if config.min_volume > 0:
        print(f"  min vol  : {config.min_volume:.0f} contracts")
    if config.max_markets_per_event > 0:
        print(f"  max/event: {config.max_markets_per_event} markets per event")

    # Build a minimal client that can hit public historical endpoints.
    # Historical endpoints don't require auth, but the client still needs
    # a config object.  We pass dummy credentials; they are never used
    # because historical methods use _get_public() which skips auth.
    loaded_config = KalshiConfig.from_mapping(
        cfg_raw.get("auth", {}),
        env_fallback=True,
        demo=bool(cfg_raw.get("environment", {}).get("demo", False)),
        allow_missing=True,
        source_label=f"Kalshi auth ({getattr(args, 'config', None) or 'config'})",
    )
    kalshi_config = _force_live_reads(loaded_config) if loaded_config is not None else _force_live_reads(_build_public_only_config(cfg_raw))

    client = KalshiClient(
        kalshi_config,
        historical_sleep_sec=config.request_sleep_sec,
        authenticated_sleep_sec=config.authenticated_request_sleep_sec,
        authenticated_retry_policy=KalshiGetRetryPolicy(
            max_retries=config.authenticated_rate_limit_max_retries,
            backoff_base_sec=config.authenticated_rate_limit_backoff_base_sec,
            backoff_max_sec=config.authenticated_rate_limit_backoff_max_sec,
            jitter_max_sec=config.authenticated_rate_limit_jitter_max_sec,
        ),
    )

    pipeline = HistoricalIngestPipeline(client, config)

    print(f"\nStarting download... (this may take several minutes for {config.lookback_days} days of data)")
    result = pipeline.run()

    print("\n[DONE] Historical ingest complete.")
    print(f"  Markets downloaded         : {result.markets_downloaded}")
    print(f"  Markets with trades        : {result.markets_with_trades}")
    print(f"  Markets skipped (no trades): {result.markets_skipped_no_trades}")
    print(f"  Markets failed             : {result.markets_failed}")
    print(f"  Total trades               : {result.total_trades}")
    print(f"  Total candlesticks         : {result.total_candlesticks}")
    print(f"  Resolution records         : {result.resolution_count}")
    print(f"  Normalized markets written : {result.normalized_markets_written}")
    print(f"  Feature files written      : {result.feature_files_written}")
    if result.date_range_start:
        print(f"  Date range                 : {result.date_range_start} → {result.date_range_end}")
    print(f"  Manifest                   : {result.manifest_path}")
    print(f"  Summary                    : {result.summary_path}")
    if getattr(result, "status_artifact_path", None):
        print(f"  Status artifact            : {result.status_artifact_path}")
    if getattr(result, "run_summary_artifact_path", None):
        print(f"  Run summary artifact       : {result.run_summary_artifact_path}")

    if not getattr(args, "skip_validation", False):
        validation_args = SimpleNamespace(
            markets_path=None,
            trades_path=None,
            candles_path=None,
            resolution_path=None,
            ingest_summary_path=None,
            ingest_manifest_path=None,
            ingest_checkpoint_path=None,
            features_dir=None,
            output_dir=None,
        )
        validation_config = build_kalshi_validation_config(cfg_raw, validation_args)
        validation_result = run_kalshi_data_validation(validation_config)
        print("\nPost-ingest validation complete.")
        print(f"  Validation status          : {validation_result.status}")
        print(f"  Validation summary         : {validation_result.artifacts.summary_path}")
        print(f"  Validation details         : {validation_result.artifacts.details_path}")
        print(f"  Validation report          : {validation_result.artifacts.report_path}")


def _build_public_only_config(cfg_raw: dict[str, Any]):
    """Build a KalshiConfig with dummy credentials for public-only access."""
    from trading_platform.kalshi.auth import KalshiConfig
    demo = cfg_raw.get("environment", {}).get("demo", True)
    # Use a placeholder private key that will never be called
    # (historical methods use _get_public which skips auth)
    placeholder_pem = (
        "-----BEGIN RSA PRIVATE KEY-----\n"
        "PLACEHOLDER_NOT_USED_FOR_PUBLIC_ENDPOINTS\n"
        "-----END RSA PRIVATE KEY-----"
    )
    return KalshiConfig(
        api_key_id="public-only",
        private_key_pem=placeholder_pem,
        demo=bool(demo),
    )
