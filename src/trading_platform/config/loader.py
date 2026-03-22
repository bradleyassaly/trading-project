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
from trading_platform.execution.models import ExecutionConfig
from trading_platform.monitoring.models import MonitoringConfig, NotificationChannel, NotificationConfig
from trading_platform.orchestration.models import (
    OrchestrationStageToggles,
    PipelineRunConfig,
)

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


def load_research_workflow_config(path: str | Path) -> ResearchWorkflowConfig:
    data = _read_config_file(Path(path))
    return ResearchWorkflowConfig(**data)


def load_parameter_sweep_config(path: str | Path) -> ParameterSweepConfig:
    data = _read_config_file(Path(path))
    return ParameterSweepConfig(**data)


def load_walk_forward_config(path: str | Path) -> WalkForwardConfig:
    data = _read_config_file(Path(path))
    return WalkForwardConfig(**data)


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
