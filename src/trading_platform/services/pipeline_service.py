from __future__ import annotations

from pathlib import Path

from trading_platform.config.models import FeatureConfig, IngestConfig
from trading_platform.data.providers.base import BarDataProvider
from trading_platform.services.feature_service import run_feature_build
from trading_platform.services.ingest_service import run_ingest


def run_research_prep_pipeline(
    ingest_config: IngestConfig,
    feature_config: FeatureConfig,
    provider: BarDataProvider | None = None,
) -> dict[str, Path]:
    """
    Run the research-preparation pipeline for a symbol:

    1. ingest raw data
    2. normalize + validate
    3. build features
    """
    normalized_path = run_ingest(
        config=ingest_config,
        provider=provider,
    )

    features_path = run_feature_build(
        config=feature_config,
    )

    return {
        "normalized_path": normalized_path,
        "features_path": features_path,
    }