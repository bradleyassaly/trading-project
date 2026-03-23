from __future__ import annotations

from pathlib import Path

from trading_platform.config.loader import load_adaptive_allocation_policy_config
from trading_platform.portfolio.adaptive_allocation import (
    AdaptiveAllocationPolicyConfig,
    build_adaptive_allocation,
)


def cmd_adaptive_allocation_build(args) -> None:
    if getattr(args, "use_regime", False) and not getattr(args, "regime", None):
        raise SystemExit("--use-regime requires --regime")
    policy = (
        load_adaptive_allocation_policy_config(args.policy_config)
        if getattr(args, "policy_config", None)
        else AdaptiveAllocationPolicyConfig()
    )
    if getattr(args, "dry_run", False):
        policy = AdaptiveAllocationPolicyConfig(**{**policy.__dict__, "dry_run": True})
    result = build_adaptive_allocation(
        strategy_portfolio_path=Path(args.portfolio),
        strategy_monitoring_path=Path(args.monitoring),
        strategy_lifecycle_path=Path(args.lifecycle) if getattr(args, "lifecycle", None) else None,
        market_regime_path=Path(args.regime) if getattr(args, "use_regime", False) and getattr(args, "regime", None) else None,
        output_dir=Path(args.output_dir),
        policy=policy,
    )
    print(f"Selected strategies: {result['selected_count']}")
    print(f"Warnings: {result['warning_count']}")
    print(f"Absolute weight change: {result['absolute_weight_change']:.6f}")
    if result.get("current_regime_label"):
        print(f"Current regime: {result['current_regime_label']}")
    print(f"Adaptive allocation JSON: {result['adaptive_allocation_json_path']}")
    print(f"Adaptive allocation CSV: {result['adaptive_allocation_csv_path']}")
