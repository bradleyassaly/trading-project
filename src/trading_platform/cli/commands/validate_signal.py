from __future__ import annotations

import argparse
from pathlib import Path

from trading_platform.cli.common import print_symbol_list, resolve_symbols
from trading_platform.services.signal_validation_service import (
    SignalValidationConfig,
    run_signal_validation,
)


def cmd_validate_signal(args: argparse.Namespace) -> None:
    symbols = resolve_symbols(args)
    print(
        f"Validating {args.strategy} across {len(symbols)} symbol(s): "
        f"{print_symbol_list(symbols)}"
    )

    config = SignalValidationConfig(
        symbols=symbols,
        strategy=args.strategy,
        fast=args.fast,
        slow=args.slow,
        lookback=args.lookback,
        fast_values=args.fast_values,
        slow_values=args.slow_values,
        lookback_values=args.lookback_values,
        cash=args.cash,
        commission=args.commission,
        rebalance_frequency=args.rebalance_frequency,
        select_by=args.select_by,
        train_years=args.train_years,
        test_years=args.test_years,
        min_train_rows=args.min_train_rows,
        min_test_rows=args.min_test_rows,
        output_dir=Path(args.output_dir),
    )
    outputs = run_signal_validation(config)
    leaderboard = outputs["leaderboard"]

    if leaderboard.empty:
        print("No validation results generated.")
        return

    print("\nValidation leaderboard:")
    print(
        leaderboard[
            [
                "symbol",
                "strategy",
                "in_sample_return_pct",
                "walkforward_mean_return_pct",
                "walkforward_mean_sharpe",
                "worst_drawdown_pct",
                "trade_count",
                "status",
            ]
        ].to_string(index=False)
    )
    print(f"\nSaved per-symbol summaries under {outputs['output_dir'] / 'per_symbol'}")
    print(f"Saved leaderboard CSV to {outputs['leaderboard_path']}")
    print(f"Saved JSON report to {outputs['report_path']}")
