from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import yaml

from trading_platform.kalshi.validation import (
    KalshiDataValidationConfig,
    KalshiValidationThresholds,
    run_kalshi_data_validation,
)

PROJECT_ROOT = Path(__file__).resolve().parents[4]


def _load_yaml(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    config_path = Path(path)
    if not config_path.exists():
        return {}
    with config_path.open(encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def _project_relative_path(path_str: str | None, default: str) -> str:
    raw = path_str or default
    path = Path(raw)
    return str(path if path.is_absolute() else PROJECT_ROOT / path)


def build_kalshi_validation_config(raw_cfg: dict[str, Any], args: argparse.Namespace) -> KalshiDataValidationConfig:
    validation_cfg = raw_cfg.get("data_validation", {})
    thresholds_cfg = validation_cfg.get("thresholds", {})
    output_dir = _project_relative_path(
        getattr(args, "output_dir", None) or validation_cfg.get("output_dir"),
        "data/kalshi/validation",
    )
    return KalshiDataValidationConfig(
        normalized_markets_path=_project_relative_path(
            getattr(args, "markets_path", None) or validation_cfg.get("normalized_markets_path"),
            "data/kalshi/normalized/markets.parquet",
        ),
        normalized_trades_path=_project_relative_path(
            getattr(args, "trades_path", None) or validation_cfg.get("normalized_trades_path"),
            "data/kalshi/normalized/trades",
        ),
        normalized_candles_path=_project_relative_path(
            getattr(args, "candles_path", None) or validation_cfg.get("normalized_candles_path"),
            "data/kalshi/normalized/candles",
        ),
        resolution_csv_path=_project_relative_path(
            getattr(args, "resolution_path", None) or validation_cfg.get("resolution_csv_path"),
            "data/kalshi/normalized/resolution.csv",
        ),
        ingest_summary_path=_project_relative_path(
            getattr(args, "ingest_summary_path", None) or validation_cfg.get("ingest_summary_path"),
            "data/kalshi/raw/ingest_summary.json",
        ),
        ingest_manifest_path=_project_relative_path(
            getattr(args, "ingest_manifest_path", None) or validation_cfg.get("ingest_manifest_path"),
            "data/kalshi/raw/ingest_manifest.json",
        ),
        ingest_checkpoint_path=_project_relative_path(
            getattr(args, "ingest_checkpoint_path", None) or validation_cfg.get("ingest_checkpoint_path"),
            "data/kalshi/raw/ingest_checkpoint.json",
        ),
        features_dir=_project_relative_path(
            getattr(args, "features_dir", None) or validation_cfg.get("features_dir"),
            "data/kalshi/features/real",
        ),
        output_dir=output_dir,
        thresholds=KalshiValidationThresholds(
            min_resolution_coverage_warn_pct=float(
                thresholds_cfg.get("min_resolution_coverage_warn_pct", 0.90)
            ),
            min_resolution_coverage_fail_pct=float(
                thresholds_cfg.get("min_resolution_coverage_fail_pct", 0.75)
            ),
            min_trade_coverage_warn_pct=float(thresholds_cfg.get("min_trade_coverage_warn_pct", 0.80)),
            min_trade_coverage_fail_pct=float(thresholds_cfg.get("min_trade_coverage_fail_pct", 0.60)),
            min_candle_coverage_warn_pct=float(thresholds_cfg.get("min_candle_coverage_warn_pct", 0.80)),
            min_candle_coverage_fail_pct=float(thresholds_cfg.get("min_candle_coverage_fail_pct", 0.60)),
            max_duplicate_ticker_warn_rate=float(thresholds_cfg.get("max_duplicate_ticker_warn_rate", 0.0)),
            max_duplicate_ticker_fail_rate=float(thresholds_cfg.get("max_duplicate_ticker_fail_rate", 0.01)),
            max_duplicate_market_id_warn_rate=float(
                thresholds_cfg.get("max_duplicate_market_id_warn_rate", 0.0)
            ),
            max_duplicate_market_id_fail_rate=float(
                thresholds_cfg.get("max_duplicate_market_id_fail_rate", 0.01)
            ),
            max_invalid_timestamp_warn_rate=float(
                thresholds_cfg.get("max_invalid_timestamp_warn_rate", 0.01)
            ),
            max_invalid_timestamp_fail_rate=float(
                thresholds_cfg.get("max_invalid_timestamp_fail_rate", 0.05)
            ),
            allowed_missing_category_warn_rate=float(
                thresholds_cfg.get("allowed_missing_category_warn_rate", 0.05)
            ),
            allowed_missing_category_fail_rate=float(
                thresholds_cfg.get("allowed_missing_category_fail_rate", 0.10)
            ),
            synthetic_markers_hard_fail=bool(thresholds_cfg.get("synthetic_markers_hard_fail", True)),
        ),
    )


def cmd_kalshi_validate_dataset(args: argparse.Namespace) -> None:
    raw_cfg = _load_yaml(getattr(args, "config", None))
    config = build_kalshi_validation_config(raw_cfg, args)

    print("Kalshi Dataset Validation")
    print(f"  markets path        : {config.normalized_markets_path}")
    print(f"  trades path         : {config.normalized_trades_path}")
    print(f"  candles path        : {config.normalized_candles_path}")
    print(f"  resolution path     : {config.resolution_csv_path}")
    print(f"  ingest summary      : {config.ingest_summary_path}")
    print(f"  ingest manifest     : {config.ingest_manifest_path}")
    print(f"  ingest checkpoint   : {config.ingest_checkpoint_path}")
    print(f"  features dir        : {config.features_dir}")
    print(f"  output dir          : {config.output_dir}")

    result = run_kalshi_data_validation(config)

    print("\nValidation complete.")
    print(f"  Status   : {result.status}")
    print(f"  Passed   : {result.passed}")
    print(f"  Summary  : {result.artifacts.summary_path}")
    print(f"  Details  : {result.artifacts.details_path}")
    print(f"  Report   : {result.artifacts.report_path}")
