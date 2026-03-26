from __future__ import annotations

from pathlib import Path

from trading_platform.portfolio.conditional_activation import (
    ConditionalActivationConfig,
    activate_strategy_portfolio,
)


def cmd_strategy_portfolio_activate(args) -> None:
    config = ConditionalActivationConfig(
        evaluate_conditional_activation=True,
        activation_context_sources=list(getattr(args, "activation_context_sources", None) or ["regime", "benchmark_context"]),
        include_inactive_conditionals_in_output=bool(getattr(args, "include_inactive_conditionals_in_output", True)),
    )
    result = activate_strategy_portfolio(
        portfolio_path=Path(args.portfolio),
        output_dir=Path(args.output_dir),
        config=config,
        market_regime_path=Path(args.market_regime) if getattr(args, "market_regime", None) else None,
        regime_labels_path=Path(args.regime_labels) if getattr(args, "regime_labels", None) else None,
        metadata_dir=Path(args.metadata_dir) if getattr(args, "metadata_dir", None) else None,
    )
    print(f"Active strategies: {result['active_count']}")
    print(f"Activated conditional strategies: {result['activated_conditional_count']}")
    print(f"Inactive conditional strategies: {result['inactive_conditional_count']}")
    print(f"Activated portfolio JSON: {result['activated_strategy_portfolio_json_path']}")
    print(f"Activated portfolio CSV: {result['activated_strategy_portfolio_csv_path']}")
