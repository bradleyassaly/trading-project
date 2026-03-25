from __future__ import annotations

from pathlib import Path

from trading_platform.config.models import FeatureConfig
from trading_platform.features.build import build_features
from trading_platform.settings import FEATURES_DIR


def run_feature_build(config: FeatureConfig) -> Path:
    """
    Application/service-layer entry point for feature generation.
    """
    return run_feature_build_with_dirs(config=config)


def run_feature_build_with_dirs(
    *,
    config: FeatureConfig,
    normalized_dir: Path | None = None,
    features_dir: Path | None = None,
) -> Path:
    """
    Application/service-layer entry point for feature generation with optional directory overrides.
    """
    output_path = build_features(
        symbol=config.symbol,
        feature_groups=config.feature_groups,
        normalized_data_dir=normalized_dir,
        features_dir=features_dir,
    )
    return Path(output_path)


def feature_output_path(symbol: str) -> Path:
    return FEATURES_DIR / f"{symbol}.parquet"
