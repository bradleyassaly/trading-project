from __future__ import annotations

import json

from trading_platform.config.loader import (
    load_execution_config,
    load_monitoring_config,
    load_notification_config,
    load_pipeline_run_config,
    load_research_workflow_config,
)


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


def test_load_monitoring_config_from_yaml(tmp_path) -> None:
    path = tmp_path / "monitoring.yaml"
    path.write_text(
        """
maximum_failed_stages: 0
stale_artifact_max_age_hours: 24
minimum_approved_strategy_count: 1
minimum_generated_position_count: 2
maximum_gross_exposure: 1.0
maximum_net_exposure: 1.0
maximum_symbol_concentration: 0.3
maximum_turnover: 0.25
maximum_drawdown: 0.2
minimum_rolling_sharpe: 0.5
maximum_benchmark_underperformance: 0.05
maximum_missing_data_incidents: 1
maximum_zero_weight_runs: 0
max_drift_between_sleeve_target_and_final_combined_weight: 0.1
maximum_rejected_order_count: 2
maximum_liquidity_breaches: 3
maximum_short_availability_failures: 1
""".strip(),
        encoding="utf-8",
    )

    config = load_monitoring_config(path)

    assert config.maximum_failed_stages == 0
    assert config.minimum_generated_position_count == 2
    assert config.maximum_symbol_concentration == 0.3
    assert config.maximum_rejected_order_count == 2
    assert config.maximum_liquidity_breaches == 3
    assert config.maximum_short_availability_failures == 1


def test_load_notification_config_from_yaml(tmp_path) -> None:
    path = tmp_path / "notifications.yaml"
    path.write_text(
        """
smtp_host: smtp.example.com
smtp_port: 587
from_address: alerts@example.com
min_severity: warning
channels:
  - channel_type: email
    recipients:
      - ops@example.com
""".strip(),
        encoding="utf-8",
    )

    config = load_notification_config(path)

    assert config.smtp_host == "smtp.example.com"
    assert config.min_severity == "warning"
    assert config.channels[0].channel_type == "email"


def test_load_execution_config_from_yaml(tmp_path) -> None:
    path = tmp_path / "execution.yaml"
    path.write_text(
        """
commission_per_share: 0.005
commission_bps: 1.0
slippage_model_type: spread_plus_impact
spread_proxy_bps: 2.0
market_impact_proxy_bps: 5.0
max_participation_rate: 0.05
minimum_average_dollar_volume: 1000000
minimum_price: 5
lot_size: 10
minimum_trade_notional: 100
max_turnover_per_rebalance: 0.5
short_selling_allowed: false
short_borrow_availability: false
max_borrow_utilization: 0.1
price_source_assumption: close
partial_fill_behavior: clip
missing_liquidity_behavior: reject
""".strip(),
        encoding="utf-8",
    )

    config = load_execution_config(path)

    assert config.commission_per_share == 0.005
    assert config.commission_bps == 1.0
    assert config.lot_size == 10
    assert config.short_selling_allowed is False
