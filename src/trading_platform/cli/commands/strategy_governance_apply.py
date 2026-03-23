from __future__ import annotations

from pathlib import Path

from trading_platform.config.loader import load_strategy_governance_policy_config
from trading_platform.governance.strategy_lifecycle import (
    StrategyGovernancePolicyConfig,
    apply_strategy_governance,
)


def cmd_strategy_governance_apply(args) -> None:
    policy = (
        load_strategy_governance_policy_config(args.policy_config)
        if getattr(args, "policy_config", None)
        else StrategyGovernancePolicyConfig()
    )
    result = apply_strategy_governance(
        promoted_dir=Path(args.promoted_dir),
        strategy_validation_path=Path(args.validation) if getattr(args, "validation", None) else None,
        strategy_monitoring_path=Path(args.monitoring) if getattr(args, "monitoring", None) else None,
        adaptive_allocation_path=Path(args.adaptive_allocation) if getattr(args, "adaptive_allocation", None) else None,
        lifecycle_path=Path(args.lifecycle) if getattr(args, "lifecycle", None) else None,
        output_dir=Path(args.output_dir),
        policy=policy,
        dry_run=bool(getattr(args, "dry_run", False)),
    )
    print(f"Under review: {result['under_review_count']}")
    print(f"Degraded: {result['degraded_count']}")
    print(f"Demoted: {result['demoted_count']}")
    print(f"Strategy lifecycle JSON: {result['strategy_lifecycle_json_path']}")
    print(f"Strategy lifecycle CSV: {result['strategy_lifecycle_csv_path']}")
    print(f"Governance summary: {result['strategy_governance_summary_json_path']}")
