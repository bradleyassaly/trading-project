from __future__ import annotations

from pathlib import Path

from trading_platform.config.models import FeatureConfig
from trading_platform.services.feature_service import run_feature_build


def test_run_feature_build_delegates_to_build_features(monkeypatch) -> None:
    expected = "C:/tmp/aapl_features.parquet"
    captured: dict[str, object] = {}

    def fake_build_features(*, symbol: str, feature_groups: list[str] | None = None):
        captured["symbol"] = symbol
        captured["feature_groups"] = feature_groups
        return expected

    monkeypatch.setattr(
        "trading_platform.services.feature_service.build_features",
        fake_build_features,
    )

    config = FeatureConfig(
        symbol="AAPL",
        feature_groups=["trend", "momentum"],
    )

    out = run_feature_build(config=config)

    assert out == Path(expected)
    assert captured["symbol"] == "AAPL"
    assert captured["feature_groups"] == ["trend", "momentum"]