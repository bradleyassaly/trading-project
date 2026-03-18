from __future__ import annotations

import argparse
from pathlib import Path

from trading_platform.backtests.engine import run_backtest
from trading_platform.cli.common import print_symbol_list, resolve_symbols
from trading_platform.experiments.tracker import log_experiment
from trading_platform.research.service import run_vectorized_research, to_legacy_stats
from trading_platform.execution.policies import ExecutionPolicy

def cmd_research(args: argparse.Namespace) -> None:
    symbols = resolve_symbols(args)
    print(f"Running research for {len(symbols)} symbol(s): {print_symbol_list(symbols)}")

    for symbol in symbols:
        if args.engine == "legacy":
            stats = run_backtest(
                symbol=symbol,
                strategy=args.strategy,
                fast=args.fast,
                slow=args.slow,
                lookback=args.lookback,
                cash=args.cash,
                commission=args.commission,
            )

        elif args.engine == "vectorized":
            execution_policy = ExecutionPolicy(
                rebalance_frequency=args.rebalance_frequency,
            )

            result = run_vectorized_research(
                symbol=symbol,
                strategy=args.strategy,
                fast=args.fast,
                slow=args.slow,
                lookback=args.lookback,
                cost_per_turnover=args.commission,
                initial_equity=args.cash,
                execution_policy=execution_policy,
            )

            stats = to_legacy_stats(
                result,
                symbol=symbol,
                strategy=args.strategy,
                fast=args.fast,
                slow=args.slow,
                lookback=args.lookback,
                cash=args.cash,
                commission=args.commission,
            )

            if args.output_dir:
                output_dir = Path(args.output_dir)
                output_dir.mkdir(parents=True, exist_ok=True)

                timeseries_path = output_dir / f"{symbol}_{args.strategy}_timeseries.csv"
                signal_path = output_dir / f"{symbol}_{args.strategy}_signals.csv"

                result.simulation.timeseries.to_csv(timeseries_path, index=True)
                result.signal_frame.to_csv(signal_path, index=True)

                print(f"  saved timeseries: {timeseries_path}")
                print(f"  saved signals: {signal_path}")

        else:
            raise SystemExit(f"Unsupported engine: {args.engine}")

        exp_id = log_experiment(stats)

        ret = stats.get("Return [%]", "n/a")
        sharpe = stats.get("Sharpe Ratio", "n/a")
        max_dd = stats.get("Max. Drawdown [%]", "n/a")

        print(
            f"[OK] {symbol}: "
            f"engine={args.engine}, "
            f"fast={args.fast}, slow={args.slow}, cash={args.cash}, "
            f"commission={args.commission}, Return[%]={ret}, "
            f"Sharpe={sharpe}, MaxDD[%]={max_dd}, Experiment={exp_id}"
        )