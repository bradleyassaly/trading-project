from __future__ import annotations

import json

from trading_platform.config.loader import load_research_workflow_config


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