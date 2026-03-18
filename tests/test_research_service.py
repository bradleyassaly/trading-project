from __future__ import annotations

from pathlib import Path

from trading_platform.config.models import ResearchWorkflowConfig
from trading_platform.services.research_service import run_research_workflow


def test_run_research_workflow_calls_prep_then_backtest(monkeypatch) -> None:
    calls: list[tuple[str, dict]] = []

    prep_result = {
        "normalized_path": Path("/tmp/normalized/AAPL.parquet"),
        "features_path": Path("/tmp/features/AAPL.parquet"),
    }
    backtest_result = {
        "stats": {"Return [%]": 10.0},
        "experiment_id": "exp-456",
    }

    def fake_run_research_prep_pipeline(**kwargs):
        calls.append(("prep", kwargs))
        return prep_result

    def fake_run_backtest_workflow(**kwargs):
        calls.append(("backtest", kwargs))
        return backtest_result

    monkeypatch.setattr(
        "trading_platform.services.research_service.run_research_prep_pipeline",
        fake_run_research_prep_pipeline,
    )
    monkeypatch.setattr(
        "trading_platform.services.research_service.run_backtest_workflow",
        fake_run_backtest_workflow,
    )

    config = ResearchWorkflowConfig(
        symbol="AAPL",
        start="2024-01-01",
        end="2024-12-31",
        interval="1d",
        feature_groups=["trend", "momentum"],
        strategy="sma_cross",
        fast=20,
        slow=50,
        cash=10000,
        commission=0.001,
    )

    out = run_research_workflow(config=config, provider=None)

    assert out["normalized_path"] == prep_result["normalized_path"]
    assert out["features_path"] == prep_result["features_path"]
    assert out["stats"] == backtest_result["stats"]
    assert out["experiment_id"] == backtest_result["experiment_id"]

    assert calls[0][0] == "prep"
    assert calls[1][0] == "backtest"