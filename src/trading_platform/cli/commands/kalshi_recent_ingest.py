"""
CLI command: trading-cli data kalshi recent-ingest

Uses the live authenticated ``/markets`` endpoint as the primary discovery
path for recent filtered Kalshi research ingestion and optionally augments the
dataset with direct historical ticker lookups.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from trading_platform.kalshi.auth import KalshiConfig
from trading_platform.kalshi.client import KalshiGetRetryPolicy
from trading_platform.cli.commands.kalshi_historical_ingest import (
    PROJECT_ROOT as _HISTORICAL_PROJECT_ROOT,
    _build_public_only_config,
    _force_live_reads,
    _load_yaml,
    _prepare_output_paths,
)
from trading_platform.cli.commands.kalshi_validate_dataset import build_kalshi_validation_config
from trading_platform.kalshi.recent_ingest import RecentIngestConfig
from trading_platform.kalshi.validation import run_kalshi_data_validation

PROJECT_ROOT = _HISTORICAL_PROJECT_ROOT


def _project_path(*parts: str) -> Path:
    return PROJECT_ROOT.joinpath(*parts)


def _project_relative_path(path_str: str) -> Path:
    path = Path(path_str)
    return path if path.is_absolute() else PROJECT_ROOT / path


def cmd_kalshi_recent_ingest(args: argparse.Namespace) -> None:
    from trading_platform.kalshi.client import KalshiClient
    from trading_platform.kalshi.recent_ingest import RecentIngestPipeline

    cfg_raw: dict[str, Any] = {}
    if getattr(args, "config", None):
        try:
            cfg_raw = _load_yaml(args.config)
        except (OSError, Exception) as exc:
            print(f"[WARN] Could not load config {args.config}: {exc}")

    ingestion_cfg = cfg_raw.get("ingestion", {})
    hist_cfg = cfg_raw.get("historical_ingest", {})
    recent_cfg = cfg_raw.get("recent_ingest", {})

    config = RecentIngestConfig(
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
        manifest_path=str(_project_path("data", "kalshi", "raw", "recent_ingest_manifest.json")),
        checkpoint_path=str(_project_path("data", "kalshi", "raw", "recent_ingest_checkpoint.json")),
        checkpoint_backup_path=str(_project_path("data", "kalshi", "raw", "recent_ingest_checkpoint.bak.json")),
        summary_path=str(_project_path("data", "kalshi", "raw", "recent_ingest_summary.json")),
        status_artifacts_root=str(
            _project_relative_path(
                recent_cfg.get("status_artifacts_root", hist_cfg.get("status_artifacts_root", "artifacts/kalshi_ingest"))
            )
        ),
        lookback_days=int(getattr(args, "lookback_days", None) or ingestion_cfg.get("backfill_days", 365)),
        feature_period=getattr(args, "period", None) or hist_cfg.get("feature_period", "1h"),
        min_trades=int(recent_cfg.get("min_trades", 5)),
        request_sleep_sec=float(getattr(args, "sleep", None) or ingestion_cfg.get("request_sleep_sec", 0.05)),
        authenticated_request_sleep_sec=float(recent_cfg.get("authenticated_request_sleep_sec", hist_cfg.get("authenticated_request_sleep_sec", 0.072))),
        authenticated_rate_limit_max_retries=int(recent_cfg.get("authenticated_rate_limit_max_retries", hist_cfg.get("authenticated_rate_limit_max_retries", 5))),
        authenticated_rate_limit_backoff_base_sec=float(recent_cfg.get("authenticated_rate_limit_backoff_base_sec", hist_cfg.get("authenticated_rate_limit_backoff_base_sec", 0.5))),
        authenticated_rate_limit_backoff_max_sec=float(recent_cfg.get("authenticated_rate_limit_backoff_max_sec", hist_cfg.get("authenticated_rate_limit_backoff_max_sec", 8.0))),
        authenticated_rate_limit_jitter_max_sec=float(recent_cfg.get("authenticated_rate_limit_jitter_max_sec", hist_cfg.get("authenticated_rate_limit_jitter_max_sec", 0.25))),
        max_live_pages_without_retained_markets=int(hist_cfg.get("max_live_pages_without_retained_markets", 25)),
        max_raw_markets_without_processing=int(hist_cfg.get("max_raw_markets_without_processing", 2000)),
        resume_cursor_max_retries=int(hist_cfg.get("resume_cursor_max_retries", 3)),
        resume_cursor_backoff_base_sec=float(hist_cfg.get("resume_cursor_backoff_base_sec", 1.0)),
        resume_cursor_backoff_max_sec=float(hist_cfg.get("resume_cursor_backoff_max_sec", 10.0)),
        resume_cursor_jitter_max_sec=float(hist_cfg.get("resume_cursor_jitter_max_sec", 0.5)),
        resume_recovery_mode="fail_fast",
        run_base_rate=not getattr(args, "no_base_rate", False),
        base_rate_db_path=str(_project_path("data", "kalshi", "base_rates", "base_rate_db.json")),
        run_metaculus=getattr(args, "metaculus", False),
        metaculus_matches_path=str(_project_path("data", "kalshi", "metaculus", "matches.json")),
        metaculus_min_confidence=float(hist_cfg.get("metaculus_min_confidence", 0.70)),
        market_page_size=min(int(recent_cfg.get("recent_ingest_limit", 200) or 200), 200),
        trade_page_size=1000,
        ticker_filter=[],
        resume=bool(getattr(args, "resume", False)),
        resume_mode="latest" if getattr(args, "resume", False) else "fresh",
        preferred_categories=list(getattr(args, "category", None) or recent_cfg.get("recent_ingest_categories") or []),
        excluded_series_patterns=list(hist_cfg.get("excluded_series_patterns") or []),
        max_markets_per_event=int(hist_cfg.get("max_markets_per_event") or 0),
        min_volume=float(
            getattr(args, "min_volume", None)
            if getattr(args, "min_volume", None) is not None
            else recent_cfg.get("min_volume", hist_cfg.get("min_volume") or 0.0)
        ),
        recent_ingest_enabled=bool(recent_cfg.get("recent_ingest_enabled", True)),
        recent_ingest_statuses=list(getattr(args, "status", None) or recent_cfg.get("recent_ingest_statuses") or ["settled"]),
        recent_ingest_categories=list(getattr(args, "category", None) or recent_cfg.get("recent_ingest_categories") or []),
        recent_ingest_limit=int(getattr(args, "limit", None) or recent_cfg.get("recent_ingest_limit", 200)),
        preferred_research_ingest_mode=str(recent_cfg.get("preferred_research_ingest_mode", "live_recent_filtered")),
        direct_historical_tickers=list(getattr(args, "direct_historical_tickers", None) or recent_cfg.get("direct_historical_tickers") or []),
        recent_ingest_series_tickers=list(getattr(args, "series", None) or recent_cfg.get("recent_ingest_series_tickers") or []),
        recent_ingest_event_tickers=list(getattr(args, "event", None) or recent_cfg.get("recent_ingest_event_tickers") or []),
        economics_series=list(recent_cfg.get("economics_series") or []),
        politics_series=list(recent_cfg.get("politics_series") or []),
        exclude_market_type_patterns=list(recent_cfg.get("exclude_market_type_patterns") or []),
        disable_market_type_filter=bool(getattr(args, "disable_market_type_filter", False)),
    )
    _prepare_output_paths(config)

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

    print("Kalshi Recent Ingest")
    print(f"  preferred mode : {config.preferred_research_ingest_mode}")
    print(f"  lookback       : {config.lookback_days} days")
    print(f"  statuses       : {', '.join(config.recent_ingest_statuses)}")
    print(f"  categories     : {', '.join(config.recent_ingest_categories) if config.recent_ingest_categories else 'all'}")
    print(f"  series         : {', '.join(config.recent_ingest_series_tickers) if config.recent_ingest_series_tickers else 'all'}")
    print(f"  events         : {', '.join(config.recent_ingest_event_tickers) if config.recent_ingest_event_tickers else 'all'}")
    print(f"  recent limit   : {config.recent_ingest_limit}")
    print(f"  min volume     : {config.min_volume:.0f}" if config.min_volume > 0 else "  min volume     : disabled")
    print(f"  market types   : {'disabled' if config.disable_market_type_filter else ', '.join(config.exclude_market_type_patterns) if config.exclude_market_type_patterns else 'disabled'}")
    print(f"  direct history : {', '.join(config.direct_historical_tickers) if config.direct_historical_tickers else 'none'}")
    print(f"  features       : {config.features_dir}")
    print(f"  status         : {config.status_artifacts_root}")
    print(f"  resume         : {'enabled' if config.resume else 'disabled'}")

    result = RecentIngestPipeline(client, config).run()

    print("\n[DONE] Recent ingest complete.")
    print(f"  Markets downloaded         : {result.markets_downloaded}")
    print(f"  Markets with trades        : {result.markets_with_trades}")
    print(f"  Markets skipped (no trades): {result.markets_skipped_no_trades}")
    print(f"  Markets failed             : {result.markets_failed}")
    print(f"  Total trades               : {result.total_trades}")
    print(f"  Total candlesticks         : {result.total_candlesticks}")
    print(f"  Resolution records         : {result.resolution_count}")
    print(f"  Normalized markets written : {result.normalized_markets_written}")
    print(f"  Feature files written      : {result.feature_files_written}")
    print(f"  Manifest                   : {result.manifest_path}")
    print(f"  Summary                    : {result.summary_path}")
    if getattr(result, "status_artifact_path", None):
        print(f"  Status artifact            : {result.status_artifact_path}")
    if getattr(result, "run_summary_artifact_path", None):
        print(f"  Run summary artifact       : {result.run_summary_artifact_path}")
    try:
        summary_payload = json.loads(Path(result.summary_path).read_text(encoding="utf-8"))
        filter_diagnostics = summary_payload.get("filter_diagnostics", {})
        recent_diagnostics = summary_payload.get("recent_ingest", {})
        print(f"  Excluded missing core      : {filter_diagnostics.get('excluded_missing_core_fields', 0)}")
        print(f"  Excluded by category       : {filter_diagnostics.get('excluded_by_category', 0)}")
        print(f"  Excluded by series         : {filter_diagnostics.get('excluded_by_series', 0)}")
        print(f"  Excluded by market type    : {filter_diagnostics.get('excluded_by_market_type', 0)}")
        print(f"  Excluded by lookback       : {filter_diagnostics.get('excluded_by_lookback', 0)}")
        print(f"  Excluded no trade data     : {filter_diagnostics.get('excluded_no_trade_data', 0)}")
        filter_resolution = recent_diagnostics.get("filter_resolution", {})
        print(f"  Market-type filter         : {'enabled' if recent_diagnostics.get('market_type_filter_enabled') else 'disabled'}")
        if filter_resolution:
            print(f"  Filter mode                : {filter_resolution.get('mode', 'unknown')}")
            print(f"  Filter conflicts           : {len(filter_resolution.get('filter_conflicts', []))}")
        if recent_diagnostics.get("zero_results_due_to_filter_conflicts"):
            print("  Zero-result cause          : incompatible category/series filters were corrected or ignored")
    except (OSError, json.JSONDecodeError):
        pass

    if not getattr(args, "skip_validation", False):
        validation_args = SimpleNamespace(
            markets_path=None,
            trades_path=None,
            candles_path=None,
            resolution_path=None,
            ingest_summary_path=str(result.summary_path),
            ingest_manifest_path=str(result.manifest_path),
            ingest_checkpoint_path=str(config.checkpoint_path),
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
