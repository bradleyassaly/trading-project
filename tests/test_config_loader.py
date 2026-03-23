from __future__ import annotations

import json
from pathlib import Path

from trading_platform.config.loader import (
    load_broker_config,
    load_dashboard_config,
    load_execution_config,
    load_monitoring_config,
    load_notification_config,
    load_pipeline_run_config,
    load_promotion_policy_config,
    load_research_workflow_config,
    load_automated_orchestration_config,
    load_adaptive_allocation_policy_config,
    load_market_regime_policy_config,
    load_strategy_governance_policy_config,
    load_strategy_portfolio_policy_config,
    load_strategy_monitoring_policy_config,
    load_strategy_validation_policy_config,
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
maximum_rejected_order_ratio: 0.5
maximum_clipped_order_ratio: 0.25
maximum_turnover_after_execution: 0.3
maximum_execution_cost: 100
maximum_zero_executable_order_runs: 0
maximum_live_risk_check_failures: 0
maximum_live_submission_failures: 1
maximum_duplicate_order_skip_events: 2
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
    assert config.maximum_rejected_order_ratio == 0.5
    assert config.maximum_clipped_order_ratio == 0.25
    assert config.maximum_turnover_after_execution == 0.3
    assert config.maximum_execution_cost == 100
    assert config.maximum_live_submission_failures == 1


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
    assert config.allow_shorts is False
    assert config.enforce_short_borrow_proxy is False
    assert config.slippage_model_type == "liquidity_scaled"
    assert config.half_spread_bps == 2.0
    assert config.liquidity_slippage_bps == 5.0


def test_load_broker_config_from_yaml(tmp_path) -> None:
    path = tmp_path / "broker.yaml"
    path.write_text(
        """
broker_name: mock
live_trading_enabled: true
require_manual_enable_flag: true
manual_enable_flag_path: flags/live.enable
global_kill_switch_path: flags/kill.switch
expected_account_id: acct-1
max_orders_per_run: 10
max_total_notional_per_run: 50000
max_symbol_notional_per_order: 10000
allowed_order_types: [market]
default_order_type: market
allow_shorts_live: false
require_clean_monitoring_status: true
allowed_monitoring_statuses: [healthy]
""".strip(),
        encoding="utf-8",
    )

    config = load_broker_config(path)

    assert config.broker_name == "mock"
    assert config.live_trading_enabled is True
    assert config.max_orders_per_run == 10
    assert config.expected_account_id == "acct-1"
    assert config.allowed_monitoring_statuses == ["healthy"]


def test_load_dashboard_config_from_yaml(tmp_path) -> None:
    path = tmp_path / "dashboard.yaml"
    path.write_text(
        """
artifacts_root: artifacts
host: 127.0.0.1
port: 8123
""".strip(),
        encoding="utf-8",
    )

    config = load_dashboard_config(path)

    assert config.artifacts_root == "artifacts"
    assert config.host == "127.0.0.1"
    assert config.port == 8123


def test_load_promotion_policy_config_from_yaml(tmp_path) -> None:
    path = tmp_path / "promotion.yaml"
    path.write_text(
        """
schema_version: 1
metric_name: portfolio_sharpe
min_metric_threshold: 0.75
min_folds_tested: 4
min_promoted_signals: 2
max_strategies_total: 3
max_strategies_per_group: 1
group_by: signal_family
require_eligible_candidates: true
default_status: inactive
pipeline_monitoring_config_path: configs/monitoring.yaml
pipeline_execution_config_path: configs/execution.yaml
""".strip(),
        encoding="utf-8",
    )

    config = load_promotion_policy_config(path)

    assert config.metric_name == "portfolio_sharpe"
    assert config.min_metric_threshold == 0.75
    assert config.max_strategies_total == 3
    assert config.default_status == "inactive"


def test_load_strategy_portfolio_policy_config_from_yaml(tmp_path) -> None:
    path = tmp_path / "strategy_portfolio.yaml"
    path.write_text(
        """
schema_version: 1
max_strategies: 4
max_strategies_per_signal_family: 1
max_strategies_per_universe: 2
max_weight_per_strategy: 0.4
min_weight_per_strategy: 0.1
selection_metric: ranking_value
weighting_mode: equal
require_active_only: false
require_promotion_eligible_only: true
deduplicate_source_runs: true
diversification_dimension: signal_family
fallback_equal_weight_mode: true
warn_on_same_family_overlap: true
""".strip(),
        encoding="utf-8",
    )

    config = load_strategy_portfolio_policy_config(path)

    assert config.max_strategies == 4
    assert config.max_strategies_per_signal_family == 1
    assert config.max_weight_per_strategy == 0.4


def test_load_strategy_validation_policy_config_from_yaml(tmp_path) -> None:
    path = tmp_path / "strategy_validation.yaml"
    path.write_text(
        """
schema_version: 1
min_folds: 4
min_out_of_sample_sharpe: 0.6
weak_out_of_sample_sharpe: 0.2
min_mean_spearman_ic: 0.02
weak_mean_spearman_ic: 0.01
min_positive_fold_ratio: 0.55
weak_positive_fold_ratio: 0.45
max_metric_std: 0.12
min_proxy_confidence_score: 0.65
""".strip(),
        encoding="utf-8",
    )

    config = load_strategy_validation_policy_config(path)

    assert config.min_folds == 4
    assert config.min_out_of_sample_sharpe == 0.6
    assert config.min_proxy_confidence_score == 0.65


def test_load_strategy_monitoring_policy_config_from_yaml(tmp_path) -> None:
    path = tmp_path / "strategy_monitoring.yaml"
    path.write_text(
        """
schema_version: 1
min_observations: 7
warning_drawdown: 0.05
deactivate_drawdown: 0.12
warning_realized_sharpe: 0.4
deactivate_realized_sharpe: -0.1
max_drift_from_expected: 0.8
max_underperformance_streak: 3
max_missing_data_days: 2
include_inactive_strategies: false
kill_switch_mode: recommendation_only
""".strip(),
        encoding="utf-8",
    )

    config = load_strategy_monitoring_policy_config(path)

    assert config.min_observations == 7
    assert config.warning_drawdown == 0.05
    assert config.include_inactive_strategies is False


def test_load_adaptive_allocation_policy_config_from_yaml(tmp_path) -> None:
    path = tmp_path / "adaptive_allocation.yaml"
    path.write_text(
        """
schema_version: 1
lookback_window_days: 21
weighting_mode: drawdown_penalized
max_upweight_per_cycle: 0.07
max_downweight_per_cycle: 0.09
max_weight_per_strategy: 0.45
min_weight_per_strategy: 0.05
neutral_weight_fallback: prior_weight
review_penalty: 0.85
reduce_penalty: 0.55
deactivate_penalty: 0.1
family_diversification_penalty: 0.95
universe_diversification_penalty: 0.98
rebalance_smoothing: 0.6
require_min_observations: 8
max_monitoring_age_days: 5
freeze_on_stale_monitoring: true
freeze_on_low_confidence: false
""".strip(),
        encoding="utf-8",
    )

    config = load_adaptive_allocation_policy_config(path)

    assert config.lookback_window_days == 21
    assert config.weighting_mode == "drawdown_penalized"
    assert config.max_upweight_per_cycle == 0.07
    assert config.require_min_observations == 8


def test_load_market_regime_policy_config_from_yaml(tmp_path) -> None:
    path = tmp_path / "market_regime.yaml"
    path.write_text(
        """
schema_version: 1
short_return_window: 15
long_return_window: 63
volatility_window: 20
dispersion_window: 10
high_volatility_threshold: 0.3
low_volatility_threshold: 0.1
trend_return_threshold: 0.05
flat_return_threshold: 0.01
confidence_floor: 0.25
allow_paper_equity_curve_proxy: true
strategy_family_regime_map:
  momentum: [trend]
  value: [mean_reversion, low_vol]
""".strip(),
        encoding="utf-8",
    )

    config = load_market_regime_policy_config(path)

    assert config.short_return_window == 15
    assert config.high_volatility_threshold == 0.3
    assert config.strategy_family_regime_map["momentum"] == ["trend"]


def test_load_strategy_governance_policy_config_from_yaml(tmp_path) -> None:
    path = tmp_path / "strategy_governance.yaml"
    path.write_text(
        """
schema_version: 1
demote_after_deactivate_events: 3
demote_after_degraded_cycles: 4
under_review_on_weak_validation: true
degrade_on_reduce_recommendation: false
under_review_on_review_recommendation: true
under_review_on_low_confidence: true
low_confidence_values:
  - low
  - weak
""".strip(),
        encoding="utf-8",
    )

    config = load_strategy_governance_policy_config(path)

    assert config.demote_after_deactivate_events == 3
    assert config.degrade_on_reduce_recommendation is False
    assert config.low_confidence_values == ["low", "weak"]


def test_load_automated_orchestration_config_from_yaml(tmp_path) -> None:
    path = tmp_path / "orchestration.yaml"
    path.write_text(
        """
run_name: auto
schedule_frequency: manual
research_artifacts_root: artifacts
experiment_name: ab_test
feature_flags:
  regime: true
  adaptive: false
output_root_dir: artifacts/orchestration_runs
promotion_policy_config_path: configs/promotion.yaml
strategy_validation_policy_config_path: configs/strategy_validation.yaml
strategy_portfolio_policy_config_path: configs/strategy_portfolio.yaml
strategy_monitoring_policy_config_path: configs/strategy_monitoring.yaml
market_regime_policy_config_path: configs/market_regime.yaml
adaptive_allocation_policy_config_path: configs/adaptive_allocation.yaml
strategy_governance_policy_config_path: configs/strategy_governance.yaml
strategy_lifecycle_path: artifacts/governance/strategy_lifecycle.json
paper_state_path: artifacts/paper/automation_state.json
max_promotions_per_run: 2
stages:
  research: true
  registry: true
  validation: true
  promotion: true
  portfolio: true
  allocation: true
  paper: true
  monitoring: true
  regime: true
  adaptive_allocation: true
  governance: true
  kill_switch: true
""".strip(),
        encoding="utf-8",
    )

    config = load_automated_orchestration_config(path)

    assert config.run_name == "auto"
    assert config.experiment_name == "ab_test"
    assert config.feature_flags["regime"] is True
    assert config.max_promotions_per_run == 2
    assert config.stages.validation is True
    assert config.stages.regime is True
    assert config.stages.adaptive_allocation is True
    assert config.stages.governance is True
    assert config.stages.kill_switch is True


def test_example_configs_load_from_repo() -> None:
    root = Path(__file__).resolve().parents[1]

    pipeline_config = load_pipeline_run_config(root / "configs" / "pipeline_daily.yaml")
    monitoring_config = load_monitoring_config(root / "configs" / "monitoring.yaml")
    notification_config = load_notification_config(root / "configs" / "notifications.yaml")
    execution_config = load_execution_config(root / "configs" / "execution.yaml")
    broker_config = load_broker_config(root / "configs" / "broker.yaml")
    dashboard_config = load_dashboard_config(root / "configs" / "dashboard.yaml")
    promotion_config = load_promotion_policy_config(root / "configs" / "promotion.yaml")
    strategy_validation_config = load_strategy_validation_policy_config(root / "configs" / "strategy_validation.yaml")
    strategy_portfolio_config = load_strategy_portfolio_policy_config(root / "configs" / "strategy_portfolio.yaml")
    strategy_monitoring_config = load_strategy_monitoring_policy_config(root / "configs" / "strategy_monitoring.yaml")
    market_regime_config = load_market_regime_policy_config(root / "configs" / "market_regime.yaml")
    adaptive_allocation_config = load_adaptive_allocation_policy_config(root / "configs" / "adaptive_allocation.yaml")
    strategy_governance_config = load_strategy_governance_policy_config(root / "configs" / "strategy_governance.yaml")
    orchestration_config = load_automated_orchestration_config(root / "configs" / "orchestration.yaml")
    minimal_demo_config = load_pipeline_run_config(root / "configs" / "minimal_local_demo.yaml")

    assert pipeline_config.schedule_type == "daily"
    assert monitoring_config.maximum_failed_stages == 0
    assert notification_config.channels[0].channel_type == "email"
    assert execution_config.enabled is True
    assert broker_config.broker_name == "mock"
    assert dashboard_config.port == 8000
    assert promotion_config.metric_name == "portfolio_sharpe"
    assert strategy_validation_config.min_folds >= 1
    assert strategy_portfolio_config.selection_metric == "ranking_value"
    assert strategy_monitoring_config.kill_switch_mode == "recommendation_only"
    assert market_regime_config.short_return_window >= 1
    assert adaptive_allocation_config.weighting_mode in {"performance_tilted", "drawdown_penalized", "score_scaled", "equal_weight"}
    assert strategy_governance_config.demote_after_deactivate_events >= 1
    assert orchestration_config.schedule_frequency == "manual"
    assert orchestration_config.experiment_name == "baseline_regime_adaptive"
    assert orchestration_config.feature_flags["regime"] is True
    assert minimal_demo_config.schedule_type == "ad_hoc"
