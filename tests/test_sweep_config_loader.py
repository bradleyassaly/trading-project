from __future__ import annotations

import json

from trading_platform.config.loader import load_parameter_sweep_config


def test_load_parameter_sweep_config_from_json(tmp_path) -> None:
    path = tmp_path / "sweep.json"
    payload = {
        "symbol": "AAPL",
        "strategy": "sma_cross",
        "fast_values": [10, 20],
        "slow_values": [50, 100],
        "cash": 10000,
        "commission": 0.001,
    }
    path.write_text(json.dumps(payload), encoding="utf-8")

    config = load_parameter_sweep_config(path)

    assert config.symbol == "AAPL"
    assert config.strategy == "sma_cross"
    assert config.fast_values == [10, 20]
    assert config.slow_values == [50, 100]