from __future__ import annotations

from pathlib import Path

from trading_platform.config.loader import load_strategy_validation_policy_config
from trading_platform.research.strategy_validation import (
    StrategyValidationPolicyConfig,
    build_strategy_validation,
)


def cmd_strategy_validation_build(args) -> None:
    policy = (
        load_strategy_validation_policy_config(args.policy_config)
        if getattr(args, "policy_config", None)
        else StrategyValidationPolicyConfig()
    )
    result = build_strategy_validation(
        artifacts_root=Path(args.artifacts_root),
        output_dir=Path(args.output_dir),
        policy=policy,
    )
    print(f"Validation pass count: {result['pass_count']}")
    print(f"Validation weak count: {result['weak_count']}")
    print(f"Validation fail count: {result['fail_count']}")
    print(f"Strategy validation JSON: {result['strategy_validation_json_path']}")
    print(f"Strategy validation CSV: {result['strategy_validation_csv_path']}")
