from __future__ import annotations

import argparse

from trading_platform.cli.common import print_symbol_list, resolve_symbols
from trading_platform.config.models import ResearchWorkflowConfig
from trading_platform.services.research_service import run_research_workflow


def cmd_pipeline(args: argparse.Namespace) -> None:
    symbols = resolve_symbols(args)
    print(f"Running pipeline for {len(symbols)} symbol(s): {print_symbol_list(symbols)}")

    for symbol in symbols:
        config = ResearchWorkflowConfig(
            symbol=symbol,
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

        outputs = run_research_workflow(config=config)

        normalized_path = outputs["normalized_path"]
        features_path = outputs["features_path"]
        stats = outputs["stats"]
        experiment_id = outputs["experiment_id"]

        ret = stats.get("Return [%]", "n/a")
        sharpe = stats.get("Sharpe Ratio", "n/a")
        max_dd = stats.get("Max. Drawdown [%]", "n/a")

        print(f"[OK] {symbol}")
        print(f"  normalized: {normalized_path}")
        print(f"  features: {features_path}")
        print(f"  return[%]: {ret}")
        print(f"  sharpe: {sharpe}")
        print(f"  max drawdown[%]: {max_dd}")
        print(f"  experiment: {experiment_id}")