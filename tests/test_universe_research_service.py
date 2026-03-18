from __future__ import annotations

import pytest

from trading_platform.config.models import ResearchWorkflowConfig
from trading_platform.services.universe_research_service import (
    run_universe_research_workflow,
)


def test_run_universe_research_workflow_runs_all_symbols(monkeypatch) -> None:
    calls: list[str] = []

    def fake_run_research_workflow(*, config, provider=None):
        calls.append(config.symbol)
        return {
            "normalized_path": f"/tmp/{config.symbol}_normalized.parquet",
            "features_path": f"/tmp/{config.symbol}_features.parquet",
            "stats": {"Return [%]": 10.0},
            "experiment_id": f"exp-{config.symbol}",
        }

    monkeypatch.setattr(
        "trading_platform.services.universe_research_service.run_research_workflow",
        fake_run_research_workflow,
    )

    base_config = ResearchWorkflowConfig(
        symbol="PLACEHOLDER",
        strategy="sma_cross",
        fast=20,
        slow=50,
    )

    out = run_universe_research_workflow(
        symbols=["AAPL", "MSFT"],
        base_config=base_config,
    )

    assert calls == ["AAPL", "MSFT"]
    assert set(out["results"].keys()) == {"AAPL", "MSFT"}
    assert out["errors"] == {}


def test_run_universe_research_workflow_collects_errors_when_continue_on_error(
    monkeypatch,
) -> None:
    def fake_run_research_workflow(*, config, provider=None):
        if config.symbol == "MSFT":
            raise ValueError("boom")
        return {
            "normalized_path": f"/tmp/{config.symbol}_normalized.parquet",
            "features_path": f"/tmp/{config.symbol}_features.parquet",
            "stats": {"Return [%]": 10.0},
            "experiment_id": f"exp-{config.symbol}",
        }

    monkeypatch.setattr(
        "trading_platform.services.universe_research_service.run_research_workflow",
        fake_run_research_workflow,
    )

    base_config = ResearchWorkflowConfig(
        symbol="PLACEHOLDER",
        strategy="sma_cross",
        fast=20,
        slow=50,
    )

    out = run_universe_research_workflow(
        symbols=["AAPL", "MSFT"],
        base_config=base_config,
        continue_on_error=True,
    )

    assert "AAPL" in out["results"]
    assert "MSFT" in out["errors"]
    assert "boom" in out["errors"]["MSFT"]


def test_run_universe_research_workflow_raises_when_symbols_empty() -> None:
    base_config = ResearchWorkflowConfig(
        symbol="PLACEHOLDER",
        strategy="sma_cross",
        fast=20,
        slow=50,
    )

    with pytest.raises(ValueError, match="non-empty"):
        run_universe_research_workflow(
            symbols=[],
            base_config=base_config,
        )