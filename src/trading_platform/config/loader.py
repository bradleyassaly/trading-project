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
    conditional_section = payload.pop("conditional_research", {}) if isinstance(payload.get("conditional_research"), dict) else {}
    if "enable_conditional_evaluation" not in payload and "enabled" in conditional_section:
        payload["enable_conditional_evaluation"] = conditional_section["enabled"]
    if "conditional_condition_types" not in payload and isinstance(conditional_section.get("condition_types"), list):
        payload["conditional_condition_types"] = conditional_section["condition_types"]
    if "conditional_min_sample_size" not in payload and "min_sample_size" in conditional_section:
        payload["conditional_min_sample_size"] = conditional_section["min_sample_size"]
    if "conditional_compare_to_baseline" not in payload and "compare_to_baseline" in conditional_section:
        payload["conditional_compare_to_baseline"] = conditional_section["compare_to_baseline"]
    if "conditional_allow_variants" not in payload and "allow_variants" in conditional_section:
        payload["conditional_allow_variants"] = conditional_section["allow_variants"]
    return ResearchRunWorkflowConfig(**payload)


def load_walkforward_workflow_config(path: str | Path) -> WalkForwardWorkflowConfig:
    data = _read_config_file(Path(path))
    return WalkForwardWorkflowConfig(**data)


def load_paper_run_workflow_config(path: str | Path) -> PaperRunWorkflowConfig:
    data = _read_config_file(Path(path))
    payload = _apply_database_section(dict(data))
    screening_section = payload.pop("screening", {}) if isinstance(payload.get("screening"), dict) else {}
    paper_section = payload.pop("paper", {}) if isinstance(payload.get("paper"), dict) else {}
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
    if "latest_data_max_age_seconds" not in payload and "latest_data_max_age_seconds" in execution_section:
        payload["latest_data_max_age_seconds"] = execution_section["latest_data_max_age_seconds"]
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
    if "ensemble_mode" not in payload and "mode" in ensemble_section:
        payload["ensemble_mode"] = ensemble_section["mode"]
    if "ensemble_weight_method" not in payload and "weight_method" in ensemble_section:
        payload["ensemble_weight_method"] = ensemble_section["weight_method"]
    if "ensemble_normalize_scores" not in payload and "normalize_scores" in ensemble_section:
        payload["ensemble_normalize_scores"] = ensemble_section["normalize_scores"]
    if "ensemble_max_members" not in payload and "max_members" in ensemble_section:
        payload["ensemble_max_members"] = ensemble_section["max_members"]
    if "ensemble_require_promoted_only" not in payload and "require_promoted_only" in ensemble_section:
        payload["ensemble_require_promoted_only"] = ensemble_section["require_promoted_only"]
    if "ensemble_max_members_per_family" not in payload and "max_members_per_family" in ensemble_section:
        payload["ensemble_max_members_per_family"] = ensemble_section["max_members_per_family"]
    if "ensemble_minimum_member_observations" not in payload and "minimum_member_observations" in ensemble_section:
        payload["ensemble_minimum_member_observations"] = ensemble_section["minimum_member_observations"]
    if "ensemble_minimum_member_metric" not in payload and "minimum_member_metric" in ensemble_section:
        payload["ensemble_minimum_member_metric"] = ensemble_section["minimum_member_metric"]
    if "sub_universe_id" not in payload and "sub_universe_id" in screening_section:
        payload["sub_universe_id"] = screening_section["sub_universe_id"]
    if "universe_filters" not in payload and isinstance(screening_section.get("filters"), list):
        payload["universe_filters"] = screening_section["filters"]
    if "reference_data_root" not in payload and "reference_data_root" in screening_section:
        payload["reference_data_root"] = screening_section["reference_data_root"]
    if "universe_membership_path" not in payload and "membership_history_path" in screening_section:
        payload["universe_membership_path"] = screening_section["membership_history_path"]
    if "taxonomy_snapshot_path" not in payload and "taxonomy_snapshot_path" in screening_section:
        payload["taxonomy_snapshot_path"] = screening_section["taxonomy_snapshot_path"]
    if "benchmark_mapping_path" not in payload and "benchmark_mapping_path" in screening_section:
        payload["benchmark_mapping_path"] = screening_section["benchmark_mapping_path"]
    if "market_regime_path" not in payload and "market_regime_path" in screening_section:
        payload["market_regime_path"] = screening_section["market_regime_path"]
    return PaperRunWorkflowConfig(**payload)


def load_live_dry_run_workflow_config(path: str | Path) -> LiveDryRunWorkflowConfig:
    data = _read_config_file(Path(path))
    payload = _apply_database_section(dict(data))
    screening_section = payload.pop("screening", {}) if isinstance(payload.get("screening"), dict) else {}
    if "sub_universe_id" not in payload and "sub_universe_id" in screening_section:
        payload["sub_universe_id"] = screening_section["sub_universe_id"]
    if "universe_filters" not in payload and isinstance(screening_section.get("filters"), list):
        payload["universe_filters"] = screening_section["filters"]
    if "reference_data_root" not in payload and "reference_data_root" in screening_section:
        payload["reference_data_root"] = screening_section["reference_data_root"]
    if "universe_membership_path" not in payload and "membership_history_path" in screening_section:
        payload["universe_membership_path"] = screening_section["membership_history_path"]
    if "taxonomy_snapshot_path" not in payload and "taxonomy_snapshot_path" in screening_section:
        payload["taxonomy_snapshot_path"] = screening_section["taxonomy_snapshot_path"]
    if "benchmark_mapping_path" not in payload and "benchmark_mapping_path" in screening_section:
        payload["benchmark_mapping_path"] = screening_section["benchmark_mapping_path"]
    if "market_regime_path" not in payload and "market_regime_path" in screening_section:
        payload["market_regime_path"] = screening_section["market_regime_path"]
    return LiveDryRunWorkflowConfig(**payload)


def load_research_input_refresh_workflow_config(path: str | Path) -> ResearchInputRefreshWorkflowConfig:
    data = _read_config_file(Path(path))
    payload = dict(data)
    selection_section = payload.pop("selection", {}) if isinstance(payload.get("selection"), dict) else {}
    outputs_section = payload.pop("outputs", {}) if isinstance(payload.get("outputs"), dict) else {}
    reference_section = payload.pop("reference_data", {}) if isinstance(payload.get("reference_data"), dict) else {}
    metadata_section = payload.pop("metadata", {}) if isinstance(payload.get("metadata"), dict) else {}
    failure_section = payload.pop("failure_handling", {}) if isinstance(payload.get("failure_handling"), dict) else {}

    if "symbols" not in payload and isinstance(selection_section.get("symbols"), list):
        payload["symbols"] = selection_section["symbols"]
    if "universe" not in payload and "universe" in selection_section:
        payload["universe"] = selection_section["universe"]
    if "sub_universe_id" not in payload and "sub_universe_id" in selection_section:
        payload["sub_universe_id"] = selection_section["sub_universe_id"]

    if "feature_groups" not in payload and isinstance(payload.get("feature_groups"), list):
        payload["feature_groups"] = payload["feature_groups"]
    elif "feature_groups" not in payload and isinstance(outputs_section.get("feature_groups"), list):
        payload["feature_groups"] = outputs_section["feature_groups"]

    if "feature_dir" not in payload and "feature_dir" in outputs_section:
        payload["feature_dir"] = outputs_section["feature_dir"]
    if "metadata_dir" not in payload and "metadata_dir" in outputs_section:
        payload["metadata_dir"] = outputs_section["metadata_dir"]
    if "normalized_dir" not in payload and "normalized_dir" in outputs_section:
        payload["normalized_dir"] = outputs_section["normalized_dir"]

    if "reference_data_root" not in payload and "root" in reference_section:
        payload["reference_data_root"] = reference_section["root"]
    if "reference_data_root" not in payload and "reference_data_root" in reference_section:
        payload["reference_data_root"] = reference_section["reference_data_root"]
    if "universe_membership_path" not in payload and "membership_history_path" in reference_section:
        payload["universe_membership_path"] = reference_section["membership_history_path"]
    if "taxonomy_snapshot_path" not in payload and "taxonomy_snapshot_path" in reference_section:
        payload["taxonomy_snapshot_path"] = reference_section["taxonomy_snapshot_path"]
    if "benchmark_mapping_path" not in payload and "benchmark_mapping_path" in reference_section:
        payload["benchmark_mapping_path"] = reference_section["benchmark_mapping_path"]
    if "market_regime_path" not in payload and "market_regime_path" in reference_section:
        payload["market_regime_path"] = reference_section["market_regime_path"]
    if "group_map_path" not in payload and "group_map_path" in reference_section:
        payload["group_map_path"] = reference_section["group_map_path"]
    if "benchmark" not in payload and "benchmark" in reference_section:
        payload["benchmark"] = reference_section["benchmark"]

    if "sub_universe_id" not in payload and "sub_universe_id" in metadata_section:
        payload["sub_universe_id"] = metadata_section["sub_universe_id"]

    if "failure_policy" not in payload and "policy" in failure_section:
        payload["failure_policy"] = failure_section["policy"]

    return ResearchInputRefreshWorkflowConfig(**payload)


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
