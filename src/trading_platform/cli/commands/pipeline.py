from __future__ import annotations

import argparse

from trading_platform.cli.common import print_symbol_list, resolve_symbols
from trading_platform.config.models import ResearchWorkflowConfig
from trading_platform.services.universe_research_service import (
    run_universe_research_workflow,
)


def cmd_pipeline(args: argparse.Namespace) -> None:
    symbols = resolve_symbols(args)
    print(f"Running pipeline for {len(symbols)} symbol(s): {print_symbol_list(symbols)}")

    base_config = ResearchWorkflowConfig(
        symbol="PLACEHOLDER",
        start=args.start,
        end=getattr(args, "end", None),
        interval=getattr(args, "interval", "1d"),
        feature_groups=args.feature_groups,
        strategy=args.strategy,
        fast=args.fast,
        slow=args.slow,
        lookback=args.lookback,
        cash=args.cash,
        commission=args.commission,
    )

    outputs = run_universe_research_workflow(
        symbols=symbols,
        base_config=base_config,
        continue_on_error=True,
    )

    for symbol, result in outputs["results"].items():
        stats = result["stats"]
        ret = stats.get("Return [%]", "n/a")
        sharpe = stats.get("Sharpe Ratio", "n/a")
        max_dd = stats.get("Max. Drawdown [%]", "n/a")

        print(f"[OK] {symbol}")
        print(f"  normalized: {result['normalized_path']}")
        print(f"  features: {result['features_path']}")
        print(f"  return[%]: {ret}")
        print(f"  sharpe: {sharpe}")
        print(f"  max drawdown[%]: {max_dd}")
        print(f"  experiment: {result['experiment_id']}")

    for symbol, error in outputs["errors"].items():
        print(f"[ERROR] {symbol}: {error}")