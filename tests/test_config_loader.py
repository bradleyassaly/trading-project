from __future__ import annotations

import json

from trading_platform.config.loader import load_pipeline_run_config, load_research_workflow_config


def test_load_research_workflow_config_from_json(tmp_path) -> None:
    path = tmp_path / "job.json"
    payload = {
        "symbol": "AAPL",
        "start": "2024-01-01",
        "interval": "1d",
        "strategy": "sma_cross",
        "fast": 20,
        "slow": 50,
        "cash": 10000,
        "commission": 0.001,
    }
    path.write_text(json.dumps(payload), encoding="utf-8")

    config = load_research_workflow_config(path)

    assert config.symbol == "AAPL"
    assert config.strategy == "sma_cross"
    assert config.fast == 20
    assert config.slow == 50


def test_load_research_workflow_config_from_yaml(tmp_path) -> None:
    path = tmp_path / "job.yaml"
    path.write_text(
        """
symbol: AAPL
start: "2024-01-01"
interval: "1d"
strategy: sma_cross
fast: 20
slow: 50
cash: 10000
commission: 0.001
""".strip(),
        encoding="utf-8",
    )

    config = load_research_workflow_config(path)

    assert config.symbol == "AAPL"
    assert config.strategy == "sma_cross"
    assert config.fast == 20
    assert config.slow == 50


def test_load_pipeline_run_config_from_yaml(tmp_path) -> None:
    path = tmp_path / "pipeline.yaml"
    path.write_text(
        """
run_name: daily_governance
schedule_type: daily
universes:
  - nasdaq100
output_root_dir: artifacts/orchestration
registry_path: artifacts/registry.json
governance_config_path: configs/governance.yaml
multi_strategy_output_path: artifacts/generated_multi_strategy.yaml
paper_state_path: artifacts/paper_state.json
stages:
  promotion_evaluation: true
  multi_strategy_config_generation: true
  reporting: true
""".strip(),
        encoding="utf-8",
    )

    config = load_pipeline_run_config(path)

    assert config.run_name == "daily_governance"
    assert config.schedule_type == "daily"
    assert config.stages.promotion_evaluation is True
    assert config.stages.reporting is True
