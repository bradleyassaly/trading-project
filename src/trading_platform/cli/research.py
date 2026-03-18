from __future__ import annotations

import argparse

from trading_platform.backtests.engine import run_backtest
from trading_platform.cli.common import print_symbol_list, resolve_symbols
from trading_platform.experiments.tracker import log_experiment
from trading_platform.research.service import run_vectorized_research, to_legacy_stats


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
            result = run_vectorized_research(
                symbol=symbol,
                strategy=args.strategy,
                fast=args.fast,
                slow=args.slow,
                lookback=args.lookback,
                cost_per_turnover=args.commission,
                initial_equity=args.cash,
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