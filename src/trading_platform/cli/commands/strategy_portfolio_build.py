from __future__ import annotations

from pathlib import Path

from trading_platform.config.loader import load_strategy_portfolio_policy_config
from trading_platform.portfolio.strategy_portfolio import (
    StrategyPortfolioPolicyConfig,
    build_strategy_portfolio,
)


def cmd_strategy_portfolio_build(args) -> None:
    policy = (
        load_strategy_portfolio_policy_config(args.policy_config)
        if getattr(args, "policy_config", None)
        else StrategyPortfolioPolicyConfig()
    )
    result = build_strategy_portfolio(
        promoted_dir=Path(args.promoted_dir),
        output_dir=Path(args.output_dir),
        policy=policy,
    )
    print(f"Selected strategies: {result['selected_count']}")
    print(f"Warnings: {result['warning_count']}")
    print(f"Strategy portfolio JSON: {result['strategy_portfolio_json_path']}")
    print(f"Strategy portfolio CSV: {result['strategy_portfolio_csv_path']}")
