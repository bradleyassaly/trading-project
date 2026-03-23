from __future__ import annotations

from pathlib import Path

from trading_platform.config.loader import load_strategy_monitoring_policy_config
from trading_platform.portfolio.strategy_monitoring import (
    StrategyMonitoringPolicyConfig,
    build_strategy_monitoring_snapshot,
)


def cmd_strategy_monitor_build(args) -> None:
    policy = (
        load_strategy_monitoring_policy_config(args.policy_config)
        if getattr(args, "policy_config", None)
        else StrategyMonitoringPolicyConfig()
    )
    result = build_strategy_monitoring_snapshot(
        strategy_portfolio_path=Path(args.portfolio),
        paper_dir=Path(args.paper_dir),
        execution_dir=Path(args.execution_dir) if getattr(args, "execution_dir", None) else None,
        allocation_dir=Path(args.allocation_dir) if getattr(args, "allocation_dir", None) else None,
        output_dir=Path(args.output_dir),
        policy=policy,
    )
    print(f"Warning strategies: {result['warning_strategy_count']}")
    print(f"Deactivation candidates: {result['deactivation_candidate_count']}")
    print(f"Strategy monitoring JSON: {result['strategy_monitoring_json_path']}")
    print(f"Strategy monitoring CSV: {result['strategy_monitoring_csv_path']}")
    print(f"Kill-switch recommendations: {result['kill_switch_recommendations_json_path']}")
