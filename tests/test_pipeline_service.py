from __future__ import annotations

from pathlib import Path

from trading_platform.config.models import FeatureConfig, IngestConfig
from trading_platform.services.pipeline_service import run_research_prep_pipeline


def test_run_research_prep_pipeline_calls_ingest_then_feature_build(monkeypatch) -> None:
    calls: list[tuple[str, dict]] = []

    normalized_out = Path("/tmp/normalized/AAPL.parquet")
    features_out = Path("/tmp/features/AAPL.parquet")

    def fake_run_ingest(**kwargs):
        calls.append(("ingest", kwargs))
        return normalized_out

    def fake_run_feature_build(**kwargs):
        calls.append(("feature", kwargs))
        return features_out

    monkeypatch.setattr(
        "trading_platform.services.pipeline_service.run_ingest",
        fake_run_ingest,
    )
    monkeypatch.setattr(
        "trading_platform.services.pipeline_service.run_feature_build",
        fake_run_feature_build,
    )

    ingest_config = IngestConfig(
        symbol="AAPL",
        start="2024-01-01",
        end="2024-12-31",
        interval="1d",
    )
    feature_config = FeatureConfig(
        symbol="AAPL",
        feature_groups=["trend", "momentum"],
    )

    out = run_research_prep_pipeline(
        ingest_config=ingest_config,
        feature_config=feature_config,
        provider=None,
    )

    assert out["normalized_path"] == normalized_out
    assert out["features_path"] == features_out

    assert calls[0][0] == "ingest"
    assert calls[1][0] == "feature"

    assert calls[0][1]["config"] == ingest_config
    assert calls[1][1]["config"] == feature_config