from __future__ import annotations

import json
from pathlib import Path

import pytest

from trading_platform.config.loader import (
    load_alpha_research_workflow_config,
    load_alpha_cycle_workflow_config,
    load_backtester_validation_workflow_config,
    load_classification_build_workflow_config,
    load_daily_trading_workflow_config,
    load_daily_replay_workflow_config,
    load_canonical_bundle_experiment_matrix_workflow_config,
    load_canonical_bundle_experiment_workflow_config,
    load_live_dry_run_workflow_config,
    load_broker_config,
    load_dashboard_config,
    load_experiment_spec_config,
    load_execution_config,
    load_fundamentals_snapshot_workflow_config,
    load_monitoring_config,
    load_notification_config,
    load_paper_run_workflow_config,
    load_pipeline_run_config,
    load_portfolio_optimizer_workflow_config,
    load_promotion_policy_config,
    load_research_input_refresh_workflow_config,
    load_research_run_workflow_config,
    load_research_workflow_config,
    load_automated_orchestration_config,
    load_adaptive_allocation_policy_config,
    load_market_regime_policy_config,
    load_strategy_governance_policy_config,
    load_strategy_portfolio_policy_config,
    load_strategy_monitoring_policy_config,
    load_strategy_validation_policy_config,
    load_walkforward_workflow_config,
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


def test_load_research_run_workflow_config_from_yaml(tmp_path) -> None:
    path = tmp_path / "research_run.yaml"
    path.write_text(
        """
preset: xsec_nasdaq100_momentum_v1_research
strategy: xsec_momentum_topn
engine: vectorized
output_dir: artifacts/research/xsec
database:
  enable: true
  database_url: postgresql+psycopg://localhost/trading
  database_schema: control_plane
lookback_bars: 84
skip_bars: 21
top_n: 2
rebalance_bars: 21
""".strip(),
        encoding="utf-8",
    )

    config = load_research_run_workflow_config(path)

    assert config.preset == "xsec_nasdaq100_momentum_v1_research"
    assert config.output_dir == "artifacts/research/xsec"
    assert config.top_n == 2


def test_load_research_run_workflow_config_with_conditional_section(tmp_path) -> None:
    path = tmp_path / "research_conditional.yaml"
    path.write_text(
        """
universe: nasdaq100
strategy: xsec_momentum_topn
output_dir: artifacts/research/nasdaq100
conditional_research:
  enabled: true
  condition_types: [regime, sub_universe, benchmark_context]
  min_sample_size: 30
  compare_to_baseline: true
  allow_variants: true
""".strip(),
        encoding="utf-8",
    )

    config = load_research_run_workflow_config(path)

    assert config.enable_conditional_evaluation is True
    assert config.conditional_condition_types == ["regime", "sub_universe", "benchmark_context"]
    assert config.conditional_min_sample_size == 30
    assert config.conditional_compare_to_baseline is True
    assert config.conditional_allow_variants is True


def test_load_research_input_refresh_workflow_config_from_yaml(tmp_path) -> None:
    path = tmp_path / "research_input_refresh.yaml"
    path.write_text(
        """
selection:
  symbols: [AAPL, MSFT]
  sub_universe_id: liquid_trend_candidates
outputs:
  feature_dir: data/features
  metadata_dir: data/metadata
  normalized_dir: data/normalized
reference_data:
  root: artifacts/reference_data/v1
  membership_history_path: artifacts/reference_data/v1/universe_membership_history.csv
  taxonomy_snapshot_path: artifacts/reference_data/v1/taxonomy_snapshots.csv
  benchmark_mapping_path: artifacts/reference_data/v1/benchmark_mapping_snapshots.csv
  market_regime_path: artifacts/regime
  benchmark: SPY
failure_handling:
  policy: fail
""".strip(),
        encoding="utf-8",
    )

    config = load_research_input_refresh_workflow_config(path)

    assert config.symbols == ["AAPL", "MSFT"]
    assert config.sub_universe_id == "liquid_trend_candidates"
    assert config.reference_data_root == "artifacts/reference_data/v1"
    assert config.benchmark == "SPY"
    assert config.failure_policy == "fail"


def test_load_research_input_refresh_workflow_config_with_fundamentals_section(tmp_path) -> None:
    path = tmp_path / "research_input_refresh_fundamentals.yaml"
    path.write_text(
        """
selection:
  symbols: [AAPL, MSFT]
outputs:
  feature_dir: data/features
  metadata_dir: data/metadata
  normalized_dir: data/normalized
fundamentals:
  enabled: true
  artifact_root: data/fundamentals
  providers: [sec, vendor]
  sec_companyfacts_root: data/sec/companyfacts
  sec_submissions_root: data/sec/submissions
  vendor_file_path: data/vendor/fundamentals.parquet
  vendor_cache_enabled: true
  vendor_cache_root: data/fundamentals/raw_fmp
  vendor_cache_ttl_hours: 48
  vendor_force_refresh: false
  vendor_request_delay_seconds: 1.5
  vendor_max_retries: 6
  vendor_max_symbols_per_run: 25
  vendor_max_requests_per_run: 100
""".strip(),
        encoding="utf-8",
    )

    config = load_research_input_refresh_workflow_config(path)

    assert config.fundamentals_enabled is True
    assert config.fundamentals_artifact_root == "data/fundamentals"
    assert config.fundamentals_providers == ["sec", "vendor"]
    assert config.fundamentals_sec_companyfacts_root == "data/sec/companyfacts"
    assert config.fundamentals_sec_submissions_root == "data/sec/submissions"
    assert config.fundamentals_vendor_file_path == "data/vendor/fundamentals.parquet"
    assert config.fundamentals_vendor_cache_enabled is True
    assert config.fundamentals_vendor_cache_root == "data/fundamentals/raw_fmp"
    assert config.fundamentals_vendor_cache_ttl_hours == 48
    assert config.fundamentals_vendor_force_refresh is False
    assert config.fundamentals_vendor_request_delay_seconds == 1.5
    assert config.fundamentals_vendor_max_retries == 6
    assert config.fundamentals_vendor_max_symbols_per_run == 25
    assert config.fundamentals_vendor_max_requests_per_run == 100


def test_load_fundamentals_snapshot_workflow_config(tmp_path) -> None:
    path = tmp_path / "fundamentals_snapshot.yaml"
    path.write_text(
        """
selection:
  universe: nasdaq100
paths:
  artifact_root: data/fundamentals
  raw_sec_cache_root: data/fundamentals/raw_sec
  symbol_cik_map_path: data/fundamentals/sec_symbol_cik_map.parquet
sec:
  sec_user_agent: trading-platform-test@example.com
  sec_request_delay_seconds: 0.25
  sec_max_retries: 5
cache:
  enabled: true
  cache_ttl_days: 14
  force_refresh: false
  offline: false
build:
  max_symbols_per_run: 50
  max_requests_per_run: 300
  build_daily_features: true
  calendar_dir: data/features
""".strip(),
        encoding="utf-8",
    )

    config = load_fundamentals_snapshot_workflow_config(path)

    assert config.universe == "nasdaq100"
    assert config.artifact_root == "data/fundamentals"
    assert config.raw_sec_cache_root == "data/fundamentals/raw_sec"
    assert config.symbol_cik_map_path == "data/fundamentals/sec_symbol_cik_map.parquet"
    assert config.sec_user_agent == "trading-platform-test@example.com"
    assert config.sec_request_delay_seconds == 0.25
    assert config.sec_max_retries == 5
    assert config.cache_enabled is True
    assert config.cache_ttl_days == 14
    assert config.max_symbols_per_run == 50
    assert config.max_requests_per_run == 300
    assert config.build_daily_features is True
    assert config.calendar_dir == "data/features"


def test_load_classification_build_workflow_config(tmp_path) -> None:
    path = tmp_path / "classification.yaml"
    path.write_text(
        """
universe: nasdaq100
output_dir: artifacts/reference/classifications
as_of_date: "2026-03-27"
""".strip(),
        encoding="utf-8",
    )

    config = load_classification_build_workflow_config(path)

    assert config.universe == "nasdaq100"
    assert config.output_dir == "artifacts/reference/classifications"
    assert config.as_of_date == "2026-03-27"


def test_load_portfolio_optimizer_workflow_config(tmp_path) -> None:
    path = tmp_path / "optimizer.yaml"
    path.write_text(
        """
returns_path: artifacts/returns.csv
output_dir: artifacts/optimizer
optimizer_name: max_sharpe
fallback_optimizer_name: metric_weighted
risk_free_rate: 0.02
min_history_rows: 30
""".strip(),
        encoding="utf-8",
    )

    config = load_portfolio_optimizer_workflow_config(path)

    assert config.returns_path == "artifacts/returns.csv"
    assert config.optimizer_name == "max_sharpe"
    assert config.fallback_optimizer_name == "metric_weighted"
    assert config.risk_free_rate == 0.02
    assert config.min_history_rows == 30


def test_load_backtester_validation_workflow_config(tmp_path) -> None:
    path = tmp_path / "validation.yaml"
    path.write_text(
        """
output_dir: artifacts/validation/vectorbt
""".strip(),
        encoding="utf-8",
    )

    config = load_backtester_validation_workflow_config(path)

    assert config.output_dir == "artifacts/validation/vectorbt"


def test_load_daily_trading_workflow_config_from_nested_yaml(tmp_path) -> None:
    path = tmp_path / "pipeline_daily.yaml"
    path.write_text(
        """
daily_trading:
  run:
    run_name: smoke_daily
  mode:
    strict_mode: true
    best_effort_mode: false
  research:
    mode: fast_refresh
  stages:
    promote: true
    build_portfolio: true
    activate_portfolio: true
    export_bundle: true
    paper_run: true
    report: true
  configs:
    research_config: configs/alpha_research.yaml
    promotion_policy_config: configs/promotion.yaml
    strategy_portfolio_policy_config: configs/strategy_portfolio.yaml
  paths:
    promoted_dir: artifacts/promoted/run_current
    portfolio_dir: artifacts/strategy_portfolio/run_current
    activated_dir: artifacts/strategy_portfolio/run_current/activated
  paper:
    auto_apply_fills: true
    execution:
      min_weight_change_to_trade: 0.015
      score_bands:
        use_percentile_thresholds: true
        entry_score_percentile: 0.85
        exit_score_percentile: 0.60
        hold_score_band: true
      ev_gate:
        enabled: true
        model_type: bucketed_mean
        horizon_days: 5
        target_type: realized_candidate_proxy
        hybrid_alpha: 0.75
        mode: soft
        training_source: candidate_decisions
        weight_multiplier: true
        weight_scale: 8.0
        extreme_negative_threshold: -0.01
        score_clip_min: -0.5
        score_clip_max: 0.5
        normalize_scores: true
        normalization_method: rank_pct
        normalize_within: by_side
        use_normalized_score_for_weighting: true
        weight_multiplier_min: 0.5
        weight_multiplier_max: 1.5
        use_confidence_weighting: true
        confidence_method: residual_std
        confidence_scale: 1.25
        confidence_clip_min: 0.6
        confidence_clip_max: 1.4
        confidence_min_samples_per_bucket: 12
        confidence_shrinkage_enabled: true
        confidence_components:
          residual_std: 0.6
          magnitude: 0.25
          model_performance: 0.15
        use_confidence_filter: true
        confidence_threshold: 0.55
        use_reliability_weighting: true
        reliability_model_type: logistic
        use_reliability_filter: true
        reliability_threshold: 0.62
        reliability_min_training_samples: 14
        reliability_recent_window: 18
        reliability_target_type: positive_net_realized_return
        reliability_top_percentile: 0.9
        reliability_hurdle: 0.002
        reliability_usage_mode: hybrid
        reliability_weight_multiplier_min: 0.85
        reliability_weight_multiplier_max: 1.1
        reliability_neutral_band: 0.03
        reliability_max_promoted_trades_per_day: 7
        min_expected_net_return: 0.001
  report:
    enable_strategy_diagnostics: true
  dashboard:
    refresh_static_data: true
    output_dir: artifacts/daily_trading/run_current/dashboard
""".strip(),
        encoding="utf-8",
    )

    config = load_daily_trading_workflow_config(path)

    assert config.run_name == "smoke_daily"
    assert config.research_mode == "fast_refresh"
    assert config.stages.promote is True
    assert config.auto_apply_fills is True
    assert config.min_weight_change_to_trade == 0.015
    assert config.use_percentile_thresholds is True
    assert config.entry_score_percentile == 0.85
    assert config.exit_score_percentile == 0.60
    assert config.ev_gate_enabled is True
    assert config.ev_gate_model_type == "bucketed_mean"
    assert config.ev_gate_horizon_days == 5
    assert config.ev_gate_target_type == "realized_candidate_proxy"
    assert config.ev_gate_hybrid_alpha == 0.75
    assert config.ev_gate_mode == "soft"
    assert config.ev_gate_training_source == "candidate_decisions"
    assert config.ev_gate_weight_multiplier is True
    assert config.ev_gate_weight_scale == 8.0
    assert config.ev_gate_extreme_negative_threshold == -0.01
    assert config.ev_gate_score_clip_min == -0.5
    assert config.ev_gate_score_clip_max == 0.5
    assert config.ev_gate_normalize_scores is True
    assert config.ev_gate_normalization_method == "rank_pct"
    assert config.ev_gate_normalize_within == "by_side"
    assert config.ev_gate_use_normalized_score_for_weighting is True
    assert config.ev_gate_weight_multiplier_min == 0.5
    assert config.ev_gate_weight_multiplier_max == 1.5
    assert config.ev_gate_use_confidence_weighting is True
    assert config.ev_gate_confidence_method == "residual_std"
    assert config.ev_gate_confidence_scale == 1.25
    assert config.ev_gate_confidence_clip_min == 0.6
    assert config.ev_gate_confidence_clip_max == 1.4
    assert config.ev_gate_confidence_min_samples_per_bucket == 12
    assert config.ev_gate_confidence_shrinkage_enabled is True
    assert config.ev_gate_confidence_component_residual_std_weight == 0.6
    assert config.ev_gate_confidence_component_magnitude_weight == 0.25
    assert config.ev_gate_confidence_component_model_performance_weight == 0.15
    assert config.ev_gate_use_confidence_filter is True
    assert config.ev_gate_confidence_threshold == 0.55
    assert config.ev_gate_use_reliability_weighting is True
    assert config.ev_gate_reliability_model_type == "logistic"
    assert config.ev_gate_use_reliability_filter is True
    assert config.ev_gate_reliability_threshold == 0.62
    assert config.ev_gate_reliability_min_training_samples == 14
    assert config.ev_gate_reliability_recent_window == 18
    assert config.ev_gate_reliability_target_type == "positive_net_realized_return"
    assert config.ev_gate_reliability_top_percentile == 0.9
    assert config.ev_gate_reliability_hurdle == pytest.approx(0.002)
    assert config.ev_gate_reliability_usage_mode == "hybrid"
    assert config.ev_gate_reliability_weight_multiplier_min == pytest.approx(0.85)
    assert config.ev_gate_reliability_weight_multiplier_max == pytest.approx(1.1)
    assert config.ev_gate_reliability_neutral_band == pytest.approx(0.03)
    assert config.ev_gate_reliability_max_promoted_trades_per_day == 7
    assert config.ev_gate_min_expected_net_return == 0.001
    assert config.activated_dir == "artifacts/strategy_portfolio/run_current/activated"
    assert config.enable_strategy_diagnostics is True
    assert config.refresh_dashboard_static_data is True
    assert config.dashboard_output_dir == "artifacts/daily_trading/run_current/dashboard"


def test_load_daily_replay_workflow_config_from_nested_yaml(tmp_path) -> None:
    path = tmp_path / "pipeline_daily.yaml"
    path.write_text(
        """
daily_trading:
  run:
    run_name: replay_base
  configs:
    promotion_policy_config: configs/promotion.yaml
    strategy_portfolio_policy_config: configs/strategy_portfolio.yaml
daily_replay:
  run:
    max_days: 5
  paths:
    output_dir: artifacts/daily_replay/run_current
    initial_state_path: artifacts/daily_replay/seed_state.json
  dates:
    start_date: 2025-01-02
    end_date: 2025-01-10
  error_handling:
    stop_on_error: false
    continue_on_error: true
  replay:
    relax_thresholds_for_testing: true
    min_expected_trade_days: 2
    override_min_signal_strength: 0.05
""".strip(),
        encoding="utf-8",
    )

    config = load_daily_replay_workflow_config(path)

    assert config.daily_trading.run_name == "replay_base"
    assert config.output_dir == "artifacts/daily_replay/run_current"
    assert config.start_date == "2025-01-02"
    assert config.end_date == "2025-01-10"
    assert config.max_days == 5
    assert config.stop_on_error is False
    assert config.continue_on_error is True
    assert config.replay.relax_thresholds_for_testing is True
    assert config.replay.min_expected_trade_days == 2
    assert config.replay.override_min_signal_strength == 0.05


def test_load_canonical_bundle_experiment_workflow_config(tmp_path) -> None:
    path = tmp_path / "canonical_bundle_experiment.yaml"
    path.write_text(
        """
baseline:
  bundle_dir: artifacts/strategy_portfolio_bundle
  promoted_dir: artifacts/promoted_strategies
  artifacts_root: artifacts/alpha_research
paths:
  output_dir: artifacts/portfolio_experiments/canonical_bundle
policy_inputs:
  preset_set: policy_sensitivity_v1
  strategy_portfolio_policy_config: configs/strategy_portfolio.yaml
baseline_variant_name: baseline
""".strip(),
        encoding="utf-8",
    )

    config = load_canonical_bundle_experiment_workflow_config(path)

    assert config.bundle_dir == "artifacts/strategy_portfolio_bundle"
    assert config.promoted_dir == "artifacts/promoted_strategies"
    assert config.artifacts_root == "artifacts/alpha_research"
    assert config.output_dir == "artifacts/portfolio_experiments/canonical_bundle"
    assert config.preset_set == "policy_sensitivity_v1"
    assert config.base_strategy_portfolio_policy_config == "configs/strategy_portfolio.yaml"
    assert config.baseline_variant_name == "baseline"
    assert config.variants == []


def test_load_canonical_bundle_experiment_matrix_workflow_config(tmp_path) -> None:
    path = tmp_path / "canonical_bundle_experiment_matrix.yaml"
    path.write_text(
        """
experiment_name: policy_sensitivity_time_stability
baseline_variant_name: baseline
paths:
  output_dir: artifacts/portfolio_experiments/policy_stability
policy_inputs:
  preset_set: policy_sensitivity_v1
cases:
  - case_id: 2026-03-20
    label: 2026-03-20 promoted bundle
    bundle_dir: artifacts/strategy_portfolio_bundle_2026-03-20
    promoted_dir: artifacts/promoted_strategies_2026-03-20
    artifacts_root: artifacts/alpha_research_2026-03-20
  - case_id: 2026-03-21
    bundle_dir: artifacts/strategy_portfolio_bundle_2026-03-21
    promoted_dir: artifacts/promoted_strategies_2026-03-21
""".strip(),
        encoding="utf-8",
    )

    config = load_canonical_bundle_experiment_matrix_workflow_config(path)

    assert config.experiment_name == "policy_sensitivity_time_stability"
    assert config.output_dir == "artifacts/portfolio_experiments/policy_stability"
    assert config.preset_set == "policy_sensitivity_v1"
    assert [case.case_id for case in config.cases] == ["2026-03-20", "2026-03-21"]


def test_load_alpha_research_workflow_config(tmp_path) -> None:
    path = tmp_path / "alpha_research.yaml"
    path.write_text(
        """
paths:
  feature_path: data/features
  output_dir: artifacts/alpha_research/run_configured
selection:
  universe: nasdaq100
signals:
  family: momentum
  candidate_grid_preset: broad_v1
  signal_composition_preset: composite_v1
  max_variants_per_family: 5
  lookbacks: [5, 10]
  horizons: [1, 5]
  min_rows: 80
  equity_context_enabled: true
  enable_flow_confirmations: true
portfolio:
  train_size: 252
  test_size: 63
  top_quantile: 0.25
  bottom_quantile: 0.25
  top_n: 8
ensemble:
  enabled: true
  mode: family_weighted
  max_members: 4
runtime:
  require_runtime_computability_for_approval: true
  min_runtime_computable_symbols_for_approval: 7
  allow_research_only_noncomputable_candidates: false
  runtime_computability_penalty_on_ranking: 0.15
  runtime_computability_check_mode: penalize
  require_composite_runtime_computability_for_approval: true
  min_composite_runtime_computable_symbols_for_approval: 9
  allow_research_only_noncomputable_composites: false
  composite_runtime_computability_check_mode: strict
  composite_runtime_computability_penalty_on_ranking: 0.2
refresh:
  fast_refresh_mode: true
  skip_heavy_diagnostics: true
  reuse_existing_fold_results: true
  restrict_to_existing_candidates: true
  max_families_for_refresh: 2
  max_candidates_for_refresh: 12
diagnostics:
  alphalens_enabled: true
  alphalens_groupby_field: sector
  classification_path: artifacts/reference/classifications/security_master.csv
  output_dir: artifacts/alpha_research/run_configured/diagnostics
reporting:
  quantstats_enabled: true
  quantstats_output_dir: artifacts/alpha_research/run_configured/reports/quantstats
tracking:
  tracker_dir: artifacts/experiment_tracking
  database_enabled: true
  database_url: postgresql+psycopg://localhost/trading
  database_schema: research
  write_candidates: false
  write_metrics: true
  write_promotions: false
""".strip(),
        encoding="utf-8",
    )

    config = load_alpha_research_workflow_config(path)

    assert config.universe == "nasdaq100"
    assert config.feature_dir == "data/features"
    assert config.output_dir == "artifacts/alpha_research/run_configured"
    assert config.signal_family == "momentum"
    assert config.candidate_grid_preset == "broad_v1"
    assert config.signal_composition_preset == "composite_v1"
    assert config.max_variants_per_family == 5
    assert config.lookbacks == [5, 10]
    assert config.horizons == [1, 5]
    assert config.min_rows == 80
    assert config.equity_context_enabled is True
    assert config.enable_flow_confirmations is True
    assert config.portfolio_top_n == 8
    assert config.enable_ensemble is True
    assert config.ensemble_mode == "family_weighted"
    assert config.require_runtime_computability_for_approval is True
    assert config.min_runtime_computable_symbols_for_approval == 7
    assert config.allow_research_only_noncomputable_candidates is False
    assert config.runtime_computability_penalty_on_ranking == 0.15
    assert config.runtime_computability_check_mode == "penalize"
    assert config.require_composite_runtime_computability_for_approval is True
    assert config.min_composite_runtime_computable_symbols_for_approval == 9
    assert config.allow_research_only_noncomputable_composites is False
    assert config.composite_runtime_computability_check_mode == "strict"
    assert config.composite_runtime_computability_penalty_on_ranking == 0.2
    assert config.fast_refresh_mode is True
    assert config.skip_heavy_diagnostics is True
    assert config.reuse_existing_fold_results is True
    assert config.restrict_to_existing_candidates is True
    assert config.max_families_for_refresh == 2
    assert config.max_candidates_for_refresh == 12
    assert config.diagnostics_alphalens_enabled is True
    assert config.diagnostics_alphalens_groupby_field == "sector"
    assert config.diagnostics_classification_path == "artifacts/reference/classifications/security_master.csv"
    assert config.diagnostics_output_dir == "artifacts/alpha_research/run_configured/diagnostics"
    assert config.reporting_quantstats_enabled is True
    assert config.reporting_quantstats_output_dir == "artifacts/alpha_research/run_configured/reports/quantstats"
    assert config.enable_database_metadata is True
    assert config.database_url == "postgresql+psycopg://localhost/trading"
    assert config.database_schema == "research"
    assert config.tracking_write_candidates is False
    assert config.tracking_write_metrics is True
    assert config.tracking_write_promotions is False


def test_load_alpha_research_workflow_config_with_fundamentals(tmp_path) -> None:
    path = tmp_path / "alpha_research_fundamentals.yaml"
    path.write_text(
        """
paths:
  feature_path: data/features
  output_dir: artifacts/alpha_research/fundamental_run
selection:
  universe: nasdaq100
signals:
  family: fundamental_quality_value
  lookbacks: [20]
  horizons: [1]
  fundamentals_enabled: true
  fundamentals_daily_features_path: data/fundamentals/daily_fundamental_features.parquet
""".strip(),
        encoding="utf-8",
    )

    config = load_alpha_research_workflow_config(path)

    assert config.signal_family == "fundamental_quality_value"
    assert config.fundamentals_enabled is True
    assert config.fundamentals_daily_features_path == "data/fundamentals/daily_fundamental_features.parquet"


def test_load_alpha_research_workflow_config_with_multiple_signal_families(tmp_path) -> None:
    path = tmp_path / "alpha_research_multi_family.yaml"
    path.write_text(
        """
paths:
  feature_path: data/features
  output_dir: artifacts/alpha_research/multi_family
selection:
  universe: nasdaq100
signals:
  families: [momentum, fundamental_value, fundamental_quality]
  lookbacks: [5]
  horizons: [1]
""".strip(),
        encoding="utf-8",
    )

    config = load_alpha_research_workflow_config(path)

    assert config.signal_families == ["momentum", "fundamental_value", "fundamental_quality"]
    assert config.signal_family == "momentum"


def test_load_alpha_cycle_workflow_config(tmp_path) -> None:
    path = tmp_path / "alpha_cycle.yaml"
    path.write_text(
        """
stages:
  refresh: true
  research: true
  promotion: true
  portfolio: true
  export_bundle: true
  report: true
configs:
  refresh_config: configs/research_input_refresh.yaml
  research_config: configs/alpha_research.yaml
  promotion_policy_config: configs/promotion.yaml
  strategy_portfolio_policy_config: configs/strategy_portfolio.yaml
paths:
  output_root: artifacts/alpha_cycle
  promoted_dir: artifacts/promoted_strategies
run:
  run_name: canonical_alpha_cycle
  run_id: run_001
mode:
  strict_mode: false
  best_effort_mode: true
promotion:
  top_n: 3
  allow_overwrite: true
portfolio:
  lifecycle: artifacts/governance/strategy_lifecycle.json
tracking:
  database_enabled: true
  database_url: postgresql+psycopg://localhost/trading
  database_schema: research
  write_candidates: true
  write_metrics: true
  write_promotions: false
""".strip(),
        encoding="utf-8",
    )

    config = load_alpha_cycle_workflow_config(path)

    assert config.refresh_config == "configs/research_input_refresh.yaml"
    assert config.research_config == "configs/alpha_research.yaml"
    assert config.promotion_policy_config == "configs/promotion.yaml"
    assert config.strategy_portfolio_policy_config == "configs/strategy_portfolio.yaml"
    assert config.output_root == "artifacts/alpha_cycle"
    assert config.promoted_dir == "artifacts/promoted_strategies"
    assert config.run_name == "canonical_alpha_cycle"
    assert config.run_id == "run_001"
    assert config.strict_mode is False
    assert config.best_effort_mode is True
    assert config.promotion_top_n == 3
    assert config.allow_overwrite is True
    assert config.lifecycle_path == "artifacts/governance/strategy_lifecycle.json"
    assert config.enable_database_metadata is True
    assert config.database_url == "postgresql+psycopg://localhost/trading"
    assert config.database_schema == "research"
    assert config.tracking_write_promotions is False


def test_load_walkforward_workflow_config_from_yaml(tmp_path) -> None:
    path = tmp_path / "walkforward.yaml"
    path.write_text(
        """
preset: xsec_nasdaq100_momentum_v1_research
strategy: xsec_momentum_topn
output: artifacts/walkforward/nasdaq100.csv
lookback_bars_values: [84]
skip_bars_values: [21]
top_n_values: [2]
rebalance_bars_values: [21]
train_bars: 756
test_bars: 126
step_bars: 126
""".strip(),
        encoding="utf-8",
    )

    config = load_walkforward_workflow_config(path)

    assert config.output == "artifacts/walkforward/nasdaq100.csv"
    assert config.lookback_bars_values == [84]
    assert config.train_bars == 756


def test_load_paper_run_workflow_config_from_yaml(tmp_path) -> None:
    path = tmp_path / "paper.yaml"
    path.write_text(
        """
preset: xsec_nasdaq100_momentum_v1_deploy
output_dir: artifacts/paper/nasdaq100
state_path: artifacts/paper/nasdaq100_state.json
execution_config: configs/execution.yaml
top_n: 2
portfolio_construction_mode: transition
use_alpaca_latest_data: true
data_sources:
  prices:
    historical: yfinance
    latest: alpaca
paper:
  execution:
    latest_data_max_age_seconds: 3600
    slippage:
      enabled: true
      model: fixed_bps
      buy_bps: 5
      sell_bps: 7
  ensemble:
    enabled: true
    mode: family_weighted
    weight_method: rank_weighted
    normalize_scores: zscore
    max_members: 4
    require_promoted_only: true
""".strip(),
        encoding="utf-8",
    )

    config = load_paper_run_workflow_config(path)

    assert config.preset == "xsec_nasdaq100_momentum_v1_deploy"
    assert config.state_path.endswith("nasdaq100_state.json")
    assert config.portfolio_construction_mode == "transition"
    assert config.use_alpaca_latest_data is True
    assert config.data_sources["prices"]["latest"] == "alpaca"
    assert config.latest_data_max_age_seconds == 3600
    assert config.slippage_model == "fixed_bps"
    assert config.slippage_buy_bps == 5
    assert config.slippage_sell_bps == 7
    assert config.ensemble_enabled is True
    assert config.ensemble_mode == "family_weighted"
    assert config.ensemble_weight_method == "rank_weighted"
    assert config.ensemble_normalize_scores == "zscore"
    assert config.ensemble_max_members == 4


def test_load_live_dry_run_workflow_config_from_yaml(tmp_path) -> None:
    path = tmp_path / "live.yaml"
    path.write_text(
        """
preset: xsec_nasdaq100_momentum_v1_deploy
output_dir: artifacts/live_dry_run/nasdaq100
execution_config: configs/execution.yaml
broker: mock
portfolio_construction_mode: transition
top_n: 2
""".strip(),
        encoding="utf-8",
    )

    config = load_live_dry_run_workflow_config(path)

    assert config.preset == "xsec_nasdaq100_momentum_v1_deploy"
    assert config.broker == "mock"
    assert config.top_n == 2


def test_load_paper_run_workflow_config_with_screening_section(tmp_path) -> None:
    path = tmp_path / "paper_screening.yaml"
    path.write_text(
        """
preset: xsec_nasdaq100_momentum_v1_deploy
state_path: artifacts/paper/nasdaq100_state.json
output_dir: artifacts/paper/nasdaq100
database:
  enable: true
  database_url: postgresql+psycopg://localhost/trading
  database_schema: control_plane
screening:
  sub_universe_id: liquid_trend_candidates
  reference_data_root: artifacts/reference_data/v1
  membership_history_path: artifacts/universe_membership/demo.csv
  taxonomy_snapshot_path: artifacts/reference_data/v1/taxonomy.csv
  benchmark_mapping_path: artifacts/reference_data/v1/benchmark.csv
  market_regime_path: artifacts/regime
  filters:
    - filter_name: min_price
      filter_type: min_price
      threshold: 5
    - filter_name: excluded_names
      filter_type: symbol_exclude_list
      symbols: [TSLA]
""".strip(),
        encoding="utf-8",
    )

    config = load_paper_run_workflow_config(path)

    assert config.sub_universe_id == "liquid_trend_candidates"
    assert config.enable_database_metadata is True
    assert config.database_url == "postgresql+psycopg://localhost/trading"
    assert config.database_schema == "control_plane"
    assert config.reference_data_root == "artifacts/reference_data/v1"
    assert config.universe_membership_path == "artifacts/universe_membership/demo.csv"
    assert config.taxonomy_snapshot_path == "artifacts/reference_data/v1/taxonomy.csv"
    assert config.benchmark_mapping_path == "artifacts/reference_data/v1/benchmark.csv"
    assert config.market_regime_path == "artifacts/regime"
    assert len(config.universe_filters) == 2
    assert config.universe_filters[0]["filter_type"] == "min_price"


def test_load_live_dry_run_workflow_config_with_screening_section(tmp_path) -> None:
    path = tmp_path / "live_screening.yaml"
    path.write_text(
        """
preset: xsec_nasdaq100_momentum_v1_deploy
output_dir: artifacts/live_dry_run/nasdaq100
database:
  enable: true
  database_url: postgresql+psycopg://localhost/trading
  database_schema: control_plane
screening:
  sub_universe_id: liquid_trend_candidates
  reference_data_root: artifacts/reference_data/v1
  membership_history_path: artifacts/universe_membership/demo.csv
  taxonomy_snapshot_path: artifacts/reference_data/v1/taxonomy.csv
  benchmark_mapping_path: artifacts/reference_data/v1/benchmark.csv
  market_regime_path: artifacts/regime
  filters:
    - filter_name: min_history
      filter_type: min_feature_history
      threshold: 252
""".strip(),
        encoding="utf-8",
    )

    config = load_live_dry_run_workflow_config(path)

    assert config.sub_universe_id == "liquid_trend_candidates"
    assert config.enable_database_metadata is True
    assert config.database_url == "postgresql+psycopg://localhost/trading"
    assert config.database_schema == "control_plane"
    assert config.reference_data_root == "artifacts/reference_data/v1"
    assert config.universe_membership_path == "artifacts/universe_membership/demo.csv"
    assert config.taxonomy_snapshot_path == "artifacts/reference_data/v1/taxonomy.csv"
    assert config.benchmark_mapping_path == "artifacts/reference_data/v1/benchmark.csv"
    assert config.market_regime_path == "artifacts/regime"
    assert config.universe_filters[0]["filter_type"] == "min_feature_history"


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
use_activated_portfolio_for_paper: true
fail_if_no_active_strategies: false
include_inactive_conditionals_in_reports: true
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
    assert config.use_activated_portfolio_for_paper is True
    assert config.fail_if_no_active_strategies is False
    assert config.include_inactive_conditionals_in_reports is True
    assert config.fail_if_no_usable_symbols is False
    assert config.fail_if_zero_targets_after_validation is False
    assert config.allow_latest_close_fallback is True
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


def test_load_promotion_policy_config_with_conditional_fields(tmp_path) -> None:
    path = tmp_path / "promotion_conditional.yaml"
    path.write_text(
        """
metric_name: portfolio_sharpe
min_metric_threshold: 0.75
bootstrap_mode: true
allow_first_promotion_without_history: true
enable_conditional_variants: true
emit_conditional_variants_alongside_baseline: true
conditional_variant_allowance: 2
conditional_variant_score_bonus: 0.05
max_strategies_per_family: 2
min_families_if_available: 3
allowed_condition_types: [regime, benchmark_context]
min_condition_sample_size: 24
min_condition_improvement: 0.01
compare_condition_to_unconditional: true
""".strip(),
        encoding="utf-8",
    )

    config = load_promotion_policy_config(path)

    assert config.bootstrap_mode is True
    assert config.allow_first_promotion_without_history is True
    assert config.enable_conditional_variants is True
    assert config.emit_conditional_variants_alongside_baseline is True
    assert config.conditional_variant_allowance == 2
    assert config.conditional_variant_score_bonus == 0.05
    assert config.max_strategies_per_family == 2
    assert config.min_families_if_available == 3
    assert config.allowed_condition_types == ["regime", "benchmark_context"]
    assert config.min_condition_sample_size == 24
    assert config.min_condition_improvement == 0.01
    assert config.compare_condition_to_unconditional is True


def test_load_strategy_portfolio_policy_config_from_yaml(tmp_path) -> None:
    path = tmp_path / "strategy_portfolio.yaml"
    path.write_text(
        """
schema_version: 1
max_strategies: 4
max_strategies_per_signal_family: 1
max_strategies_per_universe: 2
min_families_if_available: 3
max_weight_per_strategy: 0.4
min_weight_per_strategy: 0.1
selection_metric: ranking_value
weighting_mode: equal
weighting_smoothing_power: 0.8
require_active_only: false
require_promotion_eligible_only: true
deduplicate_source_runs: true
allow_conditional_variant_siblings: true
conditional_variant_score_bonus: 0.05
include_conditional_strategies: true
max_conditional_strategies_total: 2
max_conditional_strategies_per_family: 1
require_baseline_for_conditional: true
conditional_weight_multiplier: 0.8
conditional_selection_mode: separate_bucket
evaluate_conditional_activation: true
activation_context_sources: [regime, benchmark_context]
include_inactive_conditionals_in_output: false
diversification_dimension: signal_family
fallback_equal_weight_mode: true
warn_on_same_family_overlap: true
""".strip(),
        encoding="utf-8",
    )

    config = load_strategy_portfolio_policy_config(path)

    assert config.max_strategies == 4
    assert config.max_strategies_per_signal_family == 1
    assert config.min_families_if_available == 3
    assert config.max_weight_per_strategy == 0.4
    assert config.weighting_smoothing_power == 0.8
    assert config.allow_conditional_variant_siblings is True
    assert config.conditional_variant_score_bonus == 0.05
    assert config.include_conditional_strategies is True
    assert config.max_conditional_strategies_total == 2
    assert config.max_conditional_strategies_per_family == 1
    assert config.require_baseline_for_conditional is True
    assert config.conditional_weight_multiplier == 0.8
    assert config.conditional_selection_mode == "separate_bucket"
    assert config.evaluate_conditional_activation is True
    assert config.activation_context_sources == ["regime", "benchmark_context"]
    assert config.include_inactive_conditionals_in_output is False


def test_load_strategy_portfolio_policy_config_accepts_new_weighting_modes(tmp_path) -> None:
    path = tmp_path / "strategy_portfolio.yaml"
    path.write_text(
        """
schema_version: 1
weighting_mode: cost_adjusted
metric_weight_cap_multiple: 1.25
""".strip(),
        encoding="utf-8",
    )

    config = load_strategy_portfolio_policy_config(path)

    assert config.weighting_mode == "cost_adjusted"
    assert config.metric_weight_cap_multiple == 1.25


def test_load_strategy_portfolio_policy_config_accepts_strategy_weight_aliases(tmp_path) -> None:
    path = tmp_path / "strategy_portfolio.yaml"
    path.write_text(
        """
schema_version: 1
max_active_strategies: 3
min_active_strategies: 2
strategy_weighting_mode: risk_adjusted
strategy_weight_metric: portfolio_sharpe
min_strategy_weight: 0.1
max_strategy_weight: 0.6
""".strip(),
        encoding="utf-8",
    )

    config = load_strategy_portfolio_policy_config(path)

    assert config.max_strategies == 3
    assert config.min_active_strategies == 2
    assert config.weighting_mode == "risk_adjusted"
    assert config.selection_metric == "portfolio_sharpe"
    assert config.min_weight_per_strategy == 0.1
    assert config.max_weight_per_strategy == 0.6


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


def test_load_experiment_spec_config_from_yaml(tmp_path) -> None:
    path = tmp_path / "experiments.yaml"
    path.write_text(
        """
experiment_name: ab_feature_test
base_orchestration_config_path: configs/orchestration.yaml
output_root_dir: artifacts/experiments
repeat_count: 2
run_label_metadata:
  owner: qa
variants:
  - name: adaptive_on
    feature_flags:
      adaptive: true
    stage_overrides:
      adaptive_allocation: true
  - name: adaptive_off
    feature_flags:
      adaptive: false
    stage_overrides:
      adaptive_allocation: false
""".strip(),
        encoding="utf-8",
    )

    config = load_experiment_spec_config(path)

    assert config.experiment_name == "ab_feature_test"
    assert config.repeat_count == 2
    assert config.variants[0].feature_flags["adaptive"] is True
    assert config.variants[1].stage_overrides["adaptive_allocation"] is False


def test_example_configs_load_from_repo() -> None:
    root = Path(__file__).resolve().parents[1]

    pipeline_config = load_pipeline_run_config(root / "configs" / "pipeline_daily.yaml")
    daily_trading_config = load_daily_trading_workflow_config(root / "configs" / "pipeline_daily.yaml")
    classification_config = load_classification_build_workflow_config(root / "configs" / "classification_build.yaml")
    optimizer_config = load_portfolio_optimizer_workflow_config(root / "configs" / "portfolio_optimizer_research.yaml")
    validation_config = load_backtester_validation_workflow_config(root / "configs" / "backtester_validation.yaml")
    monitoring_config = load_monitoring_config(root / "configs" / "monitoring.yaml")
    notification_config = load_notification_config(root / "configs" / "notifications.yaml")
    execution_config = load_execution_config(root / "configs" / "execution.yaml")
    broker_config = load_broker_config(root / "configs" / "broker.yaml")
    dashboard_config = load_dashboard_config(root / "configs" / "dashboard.yaml")
    promotion_config = load_promotion_policy_config(root / "configs" / "promotion.yaml")
    promotion_experiment_config = load_promotion_policy_config(root / "configs" / "promotion_experiment.yaml")
    strategy_validation_config = load_strategy_validation_policy_config(root / "configs" / "strategy_validation.yaml")
    strategy_validation_experiment_config = load_strategy_validation_policy_config(
        root / "configs" / "strategy_validation_experiment.yaml"
    )
    strategy_portfolio_config = load_strategy_portfolio_policy_config(root / "configs" / "strategy_portfolio.yaml")
    strategy_monitoring_config = load_strategy_monitoring_policy_config(root / "configs" / "strategy_monitoring.yaml")
    market_regime_config = load_market_regime_policy_config(root / "configs" / "market_regime.yaml")
    adaptive_allocation_config = load_adaptive_allocation_policy_config(root / "configs" / "adaptive_allocation.yaml")
    adaptive_allocation_experiment_config = load_adaptive_allocation_policy_config(
        root / "configs" / "adaptive_allocation_experiment.yaml"
    )
    strategy_governance_config = load_strategy_governance_policy_config(root / "configs" / "strategy_governance.yaml")
    orchestration_config = load_automated_orchestration_config(root / "configs" / "orchestration.yaml")
    experiment_config = load_experiment_spec_config(root / "configs" / "experiments.yaml")
    regime_campaign_config = load_experiment_spec_config(root / "configs" / "experiment_campaign_regime.yaml")
    adaptive_campaign_config = load_experiment_spec_config(root / "configs" / "experiment_campaign_adaptive.yaml")
    governance_campaign_config = load_experiment_spec_config(root / "configs" / "experiment_campaign_governance.yaml")
    regime_campaign_fast_config = load_experiment_spec_config(root / "configs" / "experiment_campaign_regime_fast.yaml")
    adaptive_campaign_fast_config = load_experiment_spec_config(
        root / "configs" / "experiment_campaign_adaptive_fast.yaml"
    )
    governance_campaign_fast_config = load_experiment_spec_config(
        root / "configs" / "experiment_campaign_governance_fast.yaml"
    )
    regime_campaign_medium_config = load_experiment_spec_config(
        root / "configs" / "experiment_campaign_regime_medium.yaml"
    )
    adaptive_campaign_medium_config = load_experiment_spec_config(
        root / "configs" / "experiment_campaign_adaptive_medium.yaml"
    )
    governance_campaign_medium_config = load_experiment_spec_config(
        root / "configs" / "experiment_campaign_governance_medium.yaml"
    )
    research_input_refresh_config = load_research_input_refresh_workflow_config(
        root / "configs" / "research_input_refresh.yaml"
    )
    orchestration_experiment_base = load_automated_orchestration_config(
        root / "configs" / "orchestration_experiment_base.yaml"
    )
    orchestration_experiment_fast = load_automated_orchestration_config(
        root / "configs" / "orchestration_experiment_fast.yaml"
    )
    orchestration_experiment_medium = load_automated_orchestration_config(
        root / "configs" / "orchestration_experiment_medium.yaml"
    )
    governance_strict_config = load_strategy_governance_policy_config(
        root / "configs" / "strategy_governance_strict.yaml"
    )
    governance_loose_config = load_strategy_governance_policy_config(
        root / "configs" / "strategy_governance_loose.yaml"
    )
    minimal_demo_config = load_pipeline_run_config(root / "configs" / "minimal_local_demo.yaml")

    assert pipeline_config.schedule_type == "daily"
    assert daily_trading_config.run_name == "run_current"
    assert daily_trading_config.slippage_model == "fixed_bps"
    assert daily_trading_config.slippage_buy_bps == 10.0
    assert daily_trading_config.slippage_sell_bps == 10.0
    assert daily_trading_config.enable_cost_model is True
    assert daily_trading_config.commission_bps == 10.0
    assert daily_trading_config.minimum_commission == 1.0
    assert daily_trading_config.spread_bps == 20.0
    assert daily_trading_config.min_weight_change_to_trade == 0.02
    assert daily_trading_config.hold_score_band is True
    assert daily_trading_config.ev_gate_enabled is False
    assert classification_config.output_dir == "artifacts/reference/classifications"
    assert optimizer_config.optimizer_name == "min_vol"
    assert validation_config.output_dir == "artifacts/validation/vectorbt"
    assert monitoring_config.maximum_failed_stages == 0
    assert notification_config.channels[0].channel_type == "email"
    assert execution_config.enabled is True
    assert broker_config.broker_name == "mock"
    assert dashboard_config.port == 8000
    assert promotion_config.metric_name == "portfolio_sharpe"
    assert promotion_experiment_config.require_eligible_candidates is False
    assert strategy_validation_config.min_folds >= 1
    assert strategy_validation_experiment_config.min_folds <= strategy_validation_config.min_folds
    assert strategy_portfolio_config.selection_metric == "ranking_value"
    assert strategy_monitoring_config.kill_switch_mode == "recommendation_only"
    assert market_regime_config.short_return_window >= 1
    assert adaptive_allocation_config.weighting_mode in {
        "performance_tilted",
        "drawdown_penalized",
        "score_scaled",
        "equal_weight",
    }
    assert (
        adaptive_allocation_experiment_config.max_weight_per_strategy
        >= adaptive_allocation_config.max_weight_per_strategy
    )
    assert strategy_governance_config.demote_after_deactivate_events >= 1
    assert orchestration_config.schedule_frequency == "manual"
    assert orchestration_config.experiment_name == "baseline_regime_adaptive"
    assert orchestration_config.feature_flags["regime"] is True
    assert experiment_config.experiment_name == "regime_vs_static_demo"
    assert len(experiment_config.variants) >= 2
    assert regime_campaign_config.experiment_name == "campaign_regime_on_off"
    assert adaptive_campaign_config.experiment_name == "campaign_adaptive_on_off"
    assert governance_campaign_config.experiment_name == "campaign_governance_strict_vs_loose"
    assert regime_campaign_fast_config.experiment_name == "campaign_regime_on_off_fast"
    assert adaptive_campaign_fast_config.experiment_name == "campaign_adaptive_on_off_fast"
    assert governance_campaign_fast_config.experiment_name == "campaign_governance_strict_vs_loose_fast"
    assert regime_campaign_medium_config.experiment_name == "campaign_regime_on_off_medium"
    assert adaptive_campaign_medium_config.experiment_name == "campaign_adaptive_on_off_medium"
    assert governance_campaign_medium_config.experiment_name == "campaign_governance_strict_vs_loose_medium"
    assert regime_campaign_medium_config.repeat_count == 5
    assert adaptive_campaign_medium_config.repeat_count == 5
    assert governance_campaign_medium_config.repeat_count == 5
    assert research_input_refresh_config.universe == "nasdaq100"
    assert research_input_refresh_config.sub_universe_id == "liquid_trend_candidates"
    assert pipeline_config.allow_latest_close_fallback is True
    assert minimal_demo_config.fail_if_no_usable_symbols is False
    assert orchestration_experiment_base.feature_flags["adaptive"] is True
    assert orchestration_experiment_fast.research_artifacts_root == "artifacts/promotion_fixture"
    assert orchestration_experiment_fast.output_root_dir == "artifacts/orchestration_runs_fast"
    assert orchestration_experiment_fast.promotion_policy_config_path == "configs/promotion_experiment.yaml"
    assert (
        orchestration_experiment_fast.strategy_validation_policy_config_path
        == "configs/strategy_validation_experiment.yaml"
    )
    assert orchestration_experiment_medium.output_root_dir == "artifacts/orchestration_runs_medium"
    assert orchestration_experiment_medium.stages.paper is True
    assert orchestration_experiment_medium.stages.monitoring is True
    assert (
        orchestration_experiment_medium.adaptive_allocation_policy_config_path
        == "configs/adaptive_allocation_experiment.yaml"
    )
    assert (
        governance_strict_config.demote_after_deactivate_events < governance_loose_config.demote_after_deactivate_events
    )
    assert minimal_demo_config.schedule_type == "ad_hoc"
