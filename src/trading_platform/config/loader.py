from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from trading_platform.broker.models import BrokerConfig
from trading_platform.config.models import (
    MultiStrategyGroupCap,
    MultiStrategyPortfolioConfig,
    MultiStrategySleeveConfig,
    ParameterSweepConfig,
    ResearchWorkflowConfig,
    WalkForwardConfig,
)
from trading_platform.config.workflow_models import (
    AlphaResearchWorkflowConfig,
    AlphaCycleStageToggles,
    AlphaCycleWorkflowConfig,
    CanonicalBundleExperimentMatrixCaseConfig,
    CanonicalBundleExperimentMatrixWorkflowConfig,
    CanonicalBundleExperimentVariantConfig,
    CanonicalBundleExperimentWorkflowConfig,
    LiveDryRunWorkflowConfig,
    PaperRunWorkflowConfig,
    ResearchInputRefreshWorkflowConfig,
    ResearchRunWorkflowConfig,
    WalkForwardWorkflowConfig,
)
from trading_platform.dashboard.models import DashboardConfig
from trading_platform.execution.models import ExecutionConfig
from trading_platform.experiments.runner import ExperimentSpecConfig, ExperimentVariantConfig
from trading_platform.governance.strategy_lifecycle import StrategyGovernancePolicyConfig
from trading_platform.monitoring.models import MonitoringConfig, NotificationChannel, NotificationConfig
from trading_platform.monitoring.models import DailyAlertsConfig
from trading_platform.orchestration.models import (
    OrchestrationStageToggles,
    PipelineRunConfig,
)
from trading_platform.orchestration.pipeline_runner import (
    AutomatedOrchestrationConfig,
    AutomatedOrchestrationStageToggles,
)
from trading_platform.portfolio.strategy_monitoring import StrategyMonitoringPolicyConfig
from trading_platform.portfolio.adaptive_allocation import AdaptiveAllocationPolicyConfig
from trading_platform.portfolio.strategy_portfolio import StrategyPortfolioPolicyConfig
from trading_platform.regime.service import MarketRegimePolicyConfig
from trading_platform.research.promotion_pipeline import PromotionPolicyConfig
from trading_platform.research.strategy_validation import StrategyValidationPolicyConfig

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None


def _read_config_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    suffix = path.suffix.lower()

    if suffix == ".json":
        return json.loads(path.read_text(encoding="utf-8"))

    if suffix in {".yaml", ".yml"}:
        if yaml is None:
            raise ImportError(
                "PyYAML is required for YAML config files. Install with `pip install pyyaml`."
            )
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        return data or {}

    raise ValueError(f"Unsupported config file type: {suffix}")


def _apply_database_section(payload: dict[str, Any]) -> dict[str, Any]:
    database_section = payload.pop("database", {}) if isinstance(payload.get("database"), dict) else {}
    if "enable_database_metadata" not in payload and "enable" in database_section:
        payload["enable_database_metadata"] = database_section["enable"]
    if "database_url" not in payload and "database_url" in database_section:
        payload["database_url"] = database_section["database_url"]
    if "database_schema" not in payload and "database_schema" in database_section:
        payload["database_schema"] = database_section["database_schema"]
    return payload


def _pop_dict_section(payload: dict[str, Any], key: str) -> dict[str, Any]:
    value = payload.pop(key, {})
    return value if isinstance(value, dict) else {}


def _set_if_missing(payload: dict[str, Any], target_key: str, source: dict[str, Any], source_key: str | None = None) -> None:
    if target_key not in payload:
        lookup_key = source_key or target_key
        if lookup_key in source:
            payload[target_key] = source[lookup_key]


def load_research_workflow_config(path: str | Path) -> ResearchWorkflowConfig:
    data = _read_config_file(Path(path))
    return ResearchWorkflowConfig(**data)


def load_parameter_sweep_config(path: str | Path) -> ParameterSweepConfig:
    data = _read_config_file(Path(path))
    return ParameterSweepConfig(**data)


def load_walk_forward_config(path: str | Path) -> WalkForwardConfig:
    data = _read_config_file(Path(path))
    return WalkForwardConfig(**data)


def load_research_run_workflow_config(path: str | Path) -> ResearchRunWorkflowConfig:
    data = _read_config_file(Path(path))
    payload = _apply_database_section(dict(data))
    conditional_section = _pop_dict_section(payload, "conditional_research")
    _set_if_missing(payload, "enable_conditional_evaluation", conditional_section, "enabled")
    if "conditional_condition_types" not in payload and isinstance(conditional_section.get("condition_types"), list):
        payload["conditional_condition_types"] = conditional_section["condition_types"]
    _set_if_missing(payload, "conditional_min_sample_size", conditional_section, "min_sample_size")
    _set_if_missing(payload, "conditional_compare_to_baseline", conditional_section, "compare_to_baseline")
    _set_if_missing(payload, "conditional_allow_variants", conditional_section, "allow_variants")
    return ResearchRunWorkflowConfig(**payload)


def load_walkforward_workflow_config(path: str | Path) -> WalkForwardWorkflowConfig:
    data = _read_config_file(Path(path))
    return WalkForwardWorkflowConfig(**data)


def load_paper_run_workflow_config(path: str | Path) -> PaperRunWorkflowConfig:
    data = _read_config_file(Path(path))
    payload = _apply_database_section(dict(data))
    screening_section = _pop_dict_section(payload, "screening")
    paper_section = _pop_dict_section(payload, "paper")
    execution_section = (
        paper_section.get("execution", {})
        if isinstance(paper_section.get("execution"), dict)
        else {}
    )
    slippage_section = (
        execution_section.get("slippage", {})
        if isinstance(execution_section.get("slippage"), dict)
        else {}
    )
    ensemble_section = (
        paper_section.get("ensemble", {})
        if isinstance(paper_section.get("ensemble"), dict)
        else {}
    )
    _set_if_missing(payload, "latest_data_max_age_seconds", execution_section)
    if "slippage_model" not in payload and slippage_section:
        enabled = bool(slippage_section.get("enabled", False))
        payload["slippage_model"] = str(slippage_section.get("model", "fixed_bps" if enabled else "none"))
        if not enabled:
            payload["slippage_model"] = "none"
    if "slippage_buy_bps" not in payload and "buy_bps" in slippage_section:
        payload["slippage_buy_bps"] = slippage_section["buy_bps"]
    if "slippage_sell_bps" not in payload and "sell_bps" in slippage_section:
        payload["slippage_sell_bps"] = slippage_section["sell_bps"]
    if "ensemble_enabled" not in payload and ensemble_section:
        payload["ensemble_enabled"] = bool(ensemble_section.get("enabled", False))
    _set_if_missing(payload, "ensemble_mode", ensemble_section, "mode")
    _set_if_missing(payload, "ensemble_weight_method", ensemble_section, "weight_method")
    _set_if_missing(payload, "ensemble_normalize_scores", ensemble_section, "normalize_scores")
    _set_if_missing(payload, "ensemble_max_members", ensemble_section, "max_members")
    _set_if_missing(payload, "ensemble_require_promoted_only", ensemble_section, "require_promoted_only")
    _set_if_missing(payload, "ensemble_max_members_per_family", ensemble_section, "max_members_per_family")
    _set_if_missing(payload, "ensemble_minimum_member_observations", ensemble_section, "minimum_member_observations")
    _set_if_missing(payload, "ensemble_minimum_member_metric", ensemble_section, "minimum_member_metric")
    _set_if_missing(payload, "sub_universe_id", screening_section)
    if "universe_filters" not in payload and isinstance(screening_section.get("filters"), list):
        payload["universe_filters"] = screening_section["filters"]
    _set_if_missing(payload, "reference_data_root", screening_section)
    _set_if_missing(payload, "universe_membership_path", screening_section, "membership_history_path")
    _set_if_missing(payload, "taxonomy_snapshot_path", screening_section)
    _set_if_missing(payload, "benchmark_mapping_path", screening_section)
    _set_if_missing(payload, "market_regime_path", screening_section)
    return PaperRunWorkflowConfig(**payload)


def load_live_dry_run_workflow_config(path: str | Path) -> LiveDryRunWorkflowConfig:
    data = _read_config_file(Path(path))
    payload = _apply_database_section(dict(data))
    screening_section = _pop_dict_section(payload, "screening")
    _set_if_missing(payload, "sub_universe_id", screening_section)
    if "universe_filters" not in payload and isinstance(screening_section.get("filters"), list):
        payload["universe_filters"] = screening_section["filters"]
    _set_if_missing(payload, "reference_data_root", screening_section)
    _set_if_missing(payload, "universe_membership_path", screening_section, "membership_history_path")
    _set_if_missing(payload, "taxonomy_snapshot_path", screening_section)
    _set_if_missing(payload, "benchmark_mapping_path", screening_section)
    _set_if_missing(payload, "market_regime_path", screening_section)
    return LiveDryRunWorkflowConfig(**payload)


def load_research_input_refresh_workflow_config(path: str | Path) -> ResearchInputRefreshWorkflowConfig:
    data = _read_config_file(Path(path))
    payload = dict(data)
    selection_section = _pop_dict_section(payload, "selection")
    outputs_section = _pop_dict_section(payload, "outputs")
    reference_section = _pop_dict_section(payload, "reference_data")
    metadata_section = _pop_dict_section(payload, "metadata")
    failure_section = _pop_dict_section(payload, "failure_handling")
    fundamentals_section = _pop_dict_section(payload, "fundamentals")

    if "symbols" not in payload and isinstance(selection_section.get("symbols"), list):
        payload["symbols"] = selection_section["symbols"]
    _set_if_missing(payload, "universe", selection_section)
    _set_if_missing(payload, "sub_universe_id", selection_section)

    if "feature_groups" not in payload and isinstance(outputs_section.get("feature_groups"), list):
        payload["feature_groups"] = outputs_section["feature_groups"]

    _set_if_missing(payload, "feature_dir", outputs_section)
    _set_if_missing(payload, "metadata_dir", outputs_section)
    _set_if_missing(payload, "normalized_dir", outputs_section)

    _set_if_missing(payload, "reference_data_root", reference_section, "root")
    _set_if_missing(payload, "reference_data_root", reference_section)
    _set_if_missing(payload, "universe_membership_path", reference_section, "membership_history_path")
    _set_if_missing(payload, "taxonomy_snapshot_path", reference_section)
    _set_if_missing(payload, "benchmark_mapping_path", reference_section)
    _set_if_missing(payload, "market_regime_path", reference_section)
    _set_if_missing(payload, "group_map_path", reference_section)
    _set_if_missing(payload, "benchmark", reference_section)

    _set_if_missing(payload, "sub_universe_id", metadata_section)
    _set_if_missing(payload, "failure_policy", failure_section, "policy")
    _set_if_missing(payload, "fundamentals_enabled", fundamentals_section, "enabled")
    _set_if_missing(payload, "fundamentals_artifact_root", fundamentals_section, "artifact_root")
    if "fundamentals_providers" not in payload and isinstance(fundamentals_section.get("providers"), list):
        payload["fundamentals_providers"] = fundamentals_section["providers"]
    _set_if_missing(payload, "fundamentals_sec_companyfacts_root", fundamentals_section, "sec_companyfacts_root")
    _set_if_missing(payload, "fundamentals_sec_submissions_root", fundamentals_section, "sec_submissions_root")
    _set_if_missing(payload, "fundamentals_vendor_file_path", fundamentals_section, "vendor_file_path")
    _set_if_missing(payload, "fundamentals_vendor_api_key", fundamentals_section, "vendor_api_key")

    return ResearchInputRefreshWorkflowConfig(**payload)


def load_alpha_research_workflow_config(path: str | Path) -> AlphaResearchWorkflowConfig:
    data = _read_config_file(Path(path))
    payload = dict(data)
    selection_section = _pop_dict_section(payload, "selection")
    signals_section = _pop_dict_section(payload, "signals")
    paths_section = _pop_dict_section(payload, "paths")
    portfolio_section = _pop_dict_section(payload, "portfolio")
    liquidity_section = _pop_dict_section(payload, "liquidity")
    dynamic_section = _pop_dict_section(payload, "dynamic")
    regime_section = _pop_dict_section(payload, "regime")
    ensemble_section = _pop_dict_section(payload, "ensemble")
    tracking_section = _pop_dict_section(payload, "tracking")

    if "symbols" not in payload and isinstance(selection_section.get("symbols"), list):
        payload["symbols"] = selection_section["symbols"]
    _set_if_missing(payload, "universe", selection_section)

    _set_if_missing(payload, "feature_dir", paths_section, "feature_path")
    _set_if_missing(payload, "feature_dir", paths_section)
    _set_if_missing(payload, "output_dir", paths_section)

    _set_if_missing(payload, "signal_family", signals_section, "family")
    _set_if_missing(payload, "candidate_grid_preset", signals_section, "candidate_grid_preset")
    _set_if_missing(payload, "signal_composition_preset", signals_section, "signal_composition_preset")
    _set_if_missing(payload, "max_variants_per_family", signals_section, "max_variants_per_family")
    if "lookbacks" not in payload and isinstance(signals_section.get("lookbacks"), list):
        payload["lookbacks"] = signals_section["lookbacks"]
    if "horizons" not in payload and isinstance(signals_section.get("horizons"), list):
        payload["horizons"] = signals_section["horizons"]
    _set_if_missing(payload, "min_rows", signals_section)

    _set_if_missing(payload, "top_quantile", portfolio_section)
    _set_if_missing(payload, "bottom_quantile", portfolio_section)
    _set_if_missing(payload, "train_size", portfolio_section)
    _set_if_missing(payload, "test_size", portfolio_section)
    _set_if_missing(payload, "step_size", portfolio_section)
    _set_if_missing(payload, "min_train_size", portfolio_section)
    _set_if_missing(payload, "portfolio_top_n", portfolio_section, "top_n")
    _set_if_missing(payload, "portfolio_long_quantile", portfolio_section, "long_quantile")
    _set_if_missing(payload, "portfolio_short_quantile", portfolio_section, "short_quantile")
    _set_if_missing(payload, "commission", portfolio_section)
    _set_if_missing(payload, "slippage_bps_per_turnover", portfolio_section)
    _set_if_missing(payload, "slippage_bps_per_adv", portfolio_section)

    _set_if_missing(payload, "min_price", liquidity_section)
    _set_if_missing(payload, "min_volume", liquidity_section)
    _set_if_missing(payload, "min_avg_dollar_volume", liquidity_section)
    _set_if_missing(payload, "max_adv_participation", liquidity_section)
    _set_if_missing(payload, "max_position_pct_of_adv", liquidity_section)
    _set_if_missing(payload, "max_notional_per_name", liquidity_section)

    _set_if_missing(payload, "dynamic_recent_quality_window", dynamic_section, "recent_quality_window")
    _set_if_missing(payload, "dynamic_min_history", dynamic_section, "min_history")
    _set_if_missing(payload, "dynamic_downweight_mean_rank_ic", dynamic_section, "downweight_mean_rank_ic")
    _set_if_missing(payload, "dynamic_deactivate_mean_rank_ic", dynamic_section, "deactivate_mean_rank_ic")

    _set_if_missing(payload, "regime_aware_enabled", regime_section, "enabled")
    _set_if_missing(payload, "regime_min_history", regime_section, "min_history")
    _set_if_missing(payload, "regime_underweight_mean_rank_ic", regime_section, "underweight_mean_rank_ic")
    _set_if_missing(payload, "regime_exclude_mean_rank_ic", regime_section, "exclude_mean_rank_ic")

    _set_if_missing(payload, "equity_context_enabled", signals_section)
    _set_if_missing(payload, "equity_context_include_volume", signals_section)
    _set_if_missing(payload, "fundamentals_enabled", signals_section)
    _set_if_missing(payload, "fundamentals_daily_features_path", signals_section)
    _set_if_missing(payload, "enable_context_confirmations", signals_section)
    _set_if_missing(payload, "enable_relative_features", signals_section)
    _set_if_missing(payload, "enable_flow_confirmations", signals_section)
    _set_if_missing(payload, "enable_ensemble", ensemble_section, "enabled")
    _set_if_missing(payload, "ensemble_mode", ensemble_section, "mode")
    _set_if_missing(payload, "ensemble_weight_method", ensemble_section, "weight_method")
    _set_if_missing(payload, "ensemble_normalize_scores", ensemble_section, "normalize_scores")
    _set_if_missing(payload, "ensemble_max_members", ensemble_section, "max_members")
    _set_if_missing(payload, "ensemble_max_members_per_family", ensemble_section, "max_members_per_family")
    _set_if_missing(payload, "ensemble_minimum_member_observations", ensemble_section, "minimum_member_observations")
    _set_if_missing(payload, "ensemble_minimum_member_metric", ensemble_section, "minimum_member_metric")

    _set_if_missing(payload, "experiment_tracker_dir", tracking_section, "tracker_dir")

    return AlphaResearchWorkflowConfig(**payload)


def load_alpha_cycle_workflow_config(path: str | Path) -> AlphaCycleWorkflowConfig:
    data = _read_config_file(Path(path))
    payload = dict(data)
    stages_section = _pop_dict_section(payload, "stages")
    configs_section = _pop_dict_section(payload, "configs")
    paths_section = _pop_dict_section(payload, "paths")
    run_section = _pop_dict_section(payload, "run")
    mode_section = _pop_dict_section(payload, "mode")
    promotion_section = _pop_dict_section(payload, "promotion")
    portfolio_section = _pop_dict_section(payload, "portfolio")

    _set_if_missing(payload, "refresh_config", configs_section)
    _set_if_missing(payload, "research_config", configs_section)
    _set_if_missing(payload, "promotion_policy_config", configs_section)
    _set_if_missing(payload, "strategy_portfolio_policy_config", configs_section)

    _set_if_missing(payload, "output_root", paths_section)
    _set_if_missing(payload, "research_output_dir", paths_section)
    _set_if_missing(payload, "registry_dir", paths_section)
    _set_if_missing(payload, "promoted_dir", paths_section)
    _set_if_missing(payload, "portfolio_dir", paths_section)
    _set_if_missing(payload, "export_dir", paths_section)

    _set_if_missing(payload, "run_name", run_section)
    _set_if_missing(payload, "run_id", run_section)

    _set_if_missing(payload, "strict_mode", mode_section)
    _set_if_missing(payload, "best_effort_mode", mode_section)

    _set_if_missing(payload, "validation_path", promotion_section)
    _set_if_missing(payload, "promotion_top_n", promotion_section, "top_n")
    _set_if_missing(payload, "allow_overwrite", promotion_section)
    _set_if_missing(payload, "inactive", promotion_section)
    _set_if_missing(payload, "override_validation", promotion_section)

    _set_if_missing(payload, "lifecycle_path", portfolio_section, "lifecycle")
    payload["stages"] = AlphaCycleStageToggles(**stages_section)
    return AlphaCycleWorkflowConfig(**payload)


def load_canonical_bundle_experiment_workflow_config(path: str | Path) -> CanonicalBundleExperimentWorkflowConfig:
    data = _read_config_file(Path(path))
    payload = dict(data)
    baseline_section = _pop_dict_section(payload, "baseline")
    paths_section = _pop_dict_section(payload, "paths")
    policy_section = _pop_dict_section(payload, "policy_inputs")

    _set_if_missing(payload, "bundle_dir", baseline_section)
    _set_if_missing(payload, "promoted_dir", baseline_section)
    _set_if_missing(payload, "artifacts_root", baseline_section)
    _set_if_missing(payload, "lifecycle", baseline_section)

    _set_if_missing(payload, "bundle_dir", paths_section)
    _set_if_missing(payload, "promoted_dir", paths_section)
    _set_if_missing(payload, "output_dir", paths_section)
    _set_if_missing(payload, "artifacts_root", paths_section)
    _set_if_missing(payload, "lifecycle", paths_section)

    _set_if_missing(payload, "base_promotion_policy_config", policy_section, "promotion_policy_config")
    _set_if_missing(
        payload,
        "base_strategy_portfolio_policy_config",
        policy_section,
        "strategy_portfolio_policy_config",
    )
    _set_if_missing(payload, "preset_set", policy_section, "preset_set")

    raw_variants = payload.get("variants", [])
    payload["variants"] = [
        CanonicalBundleExperimentVariantConfig(**item)
        for item in raw_variants
    ]
    return CanonicalBundleExperimentWorkflowConfig(**payload)


def load_canonical_bundle_experiment_matrix_workflow_config(
    path: str | Path,
) -> CanonicalBundleExperimentMatrixWorkflowConfig:
    data = _read_config_file(Path(path))
    payload = dict(data)
    paths_section = _pop_dict_section(payload, "paths")
    policy_section = _pop_dict_section(payload, "policy_inputs")

    _set_if_missing(payload, "output_dir", paths_section)
    _set_if_missing(payload, "preset_set", policy_section, "preset_set")
    _set_if_missing(payload, "base_promotion_policy_config", policy_section, "promotion_policy_config")
    _set_if_missing(
        payload,
        "base_strategy_portfolio_policy_config",
        policy_section,
        "strategy_portfolio_policy_config",
    )

    raw_cases = payload.get("cases", [])
    payload["cases"] = [
        CanonicalBundleExperimentMatrixCaseConfig(**item)
        for item in raw_cases
    ]
    return CanonicalBundleExperimentMatrixWorkflowConfig(**payload)


def load_multi_strategy_portfolio_config(path: str | Path) -> MultiStrategyPortfolioConfig:
    data = _read_config_file(Path(path))
    sleeves = [
        MultiStrategySleeveConfig(**item)
        for item in data.get("sleeves", [])
    ]
    sector_caps = [
        MultiStrategyGroupCap(**item)
        for item in data.get("sector_caps", [])
    ]
    payload = dict(data)
    payload["sleeves"] = sleeves
    payload["sector_caps"] = sector_caps
    return MultiStrategyPortfolioConfig(**payload)


def load_pipeline_run_config(path: str | Path) -> PipelineRunConfig:
    data = _read_config_file(Path(path))
    payload = dict(data)
    payload["stages"] = OrchestrationStageToggles(**payload.get("stages", {}))
    return PipelineRunConfig(**payload)


def load_monitoring_config(path: str | Path) -> MonitoringConfig:
    data = _read_config_file(Path(path))
    return MonitoringConfig(**data)


def load_notification_config(path: str | Path) -> NotificationConfig:
    data = _read_config_file(Path(path))
    payload = dict(data)
    payload["channels"] = [NotificationChannel(**item) for item in payload.get("channels", [])]
    return NotificationConfig(**payload)


def load_daily_alerts_config(path: str | Path) -> DailyAlertsConfig:
    data = _read_config_file(Path(path))
    payload = dict(data)
    payload["email_to"] = list(payload.get("email_to", []) or [])
    payload["sms_target"] = list(payload.get("sms_target", []) or [])
    return DailyAlertsConfig(**payload)


def load_execution_config(path: str | Path) -> ExecutionConfig:
    data = _read_config_file(Path(path))
    payload = dict(data)
    compatibility_map = {
        "spread_proxy_bps": "half_spread_bps",
        "market_impact_proxy_bps": "liquidity_slippage_bps",
        "max_participation_rate": "max_participation_of_adv",
        "minimum_average_dollar_volume": "min_average_dollar_volume",
        "minimum_price": "min_price",
        "minimum_trade_notional": "min_trade_notional",
        "short_selling_allowed": "allow_shorts",
        "short_borrow_availability": "enforce_short_borrow_proxy",
        "stale_quote_behavior": "stale_market_data_behavior",
    }
    for old_key, new_key in compatibility_map.items():
        if old_key in payload and new_key not in payload:
            payload[new_key] = payload.pop(old_key)
    payload.pop("max_borrow_utilization", None)
    if payload.get("slippage_model_type") == "spread_plus_impact":
        payload["slippage_model_type"] = "liquidity_scaled"
    if "commission_per_share" in payload and "commission_model_type" not in payload:
        payload["commission_model_type"] = "per_share"
    elif "flat_commission_per_order" in payload and "commission_model_type" not in payload:
        payload["commission_model_type"] = "flat"
    return ExecutionConfig(**payload)


def load_broker_config(path: str | Path) -> BrokerConfig:
    data = _read_config_file(Path(path))
    return BrokerConfig(**data)


def load_dashboard_config(path: str | Path) -> DashboardConfig:
    data = _read_config_file(Path(path))
    return DashboardConfig(**data)


def load_promotion_policy_config(path: str | Path) -> PromotionPolicyConfig:
    data = _read_config_file(Path(path))
    return PromotionPolicyConfig(**data)


def load_strategy_validation_policy_config(path: str | Path) -> StrategyValidationPolicyConfig:
    data = _read_config_file(Path(path))
    return StrategyValidationPolicyConfig(**data)


def load_strategy_portfolio_policy_config(path: str | Path) -> StrategyPortfolioPolicyConfig:
    data = _read_config_file(Path(path))
    return StrategyPortfolioPolicyConfig(**data)


def load_strategy_monitoring_policy_config(path: str | Path) -> StrategyMonitoringPolicyConfig:
    data = _read_config_file(Path(path))
    return StrategyMonitoringPolicyConfig(**data)


def load_adaptive_allocation_policy_config(path: str | Path) -> AdaptiveAllocationPolicyConfig:
    data = _read_config_file(Path(path))
    return AdaptiveAllocationPolicyConfig(**data)


def load_market_regime_policy_config(path: str | Path) -> MarketRegimePolicyConfig:
    data = _read_config_file(Path(path))
    return MarketRegimePolicyConfig(**data)


def load_strategy_governance_policy_config(path: str | Path) -> StrategyGovernancePolicyConfig:
    data = _read_config_file(Path(path))
    return StrategyGovernancePolicyConfig(**data)


def load_automated_orchestration_config(path: str | Path) -> AutomatedOrchestrationConfig:
    data = _read_config_file(Path(path))
    payload = dict(data)
    payload["stages"] = AutomatedOrchestrationStageToggles(**payload.get("stages", {}))
    return AutomatedOrchestrationConfig(**payload)


def load_experiment_spec_config(path: str | Path) -> ExperimentSpecConfig:
    data = _read_config_file(Path(path))
    payload = dict(data)
    payload["variants"] = [ExperimentVariantConfig(**item) for item in payload.get("variants", [])]
    return ExperimentSpecConfig(**payload)
