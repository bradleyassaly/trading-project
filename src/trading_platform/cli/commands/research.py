from __future__ import annotations

import argparse

from trading_platform.backtests.engine import run_backtest
from trading_platform.cli.common import print_symbol_list, resolve_symbols
from trading_platform.experiments.tracker import log_experiment


def cmd_research(args: argparse.Namespace) -> None:
    symbols = resolve_symbols(args)
    print(f"Running research for {len(symbols)} symbol(s): {print_symbol_list(symbols)}")

    for symbol in symbols:
        stats = run_backtest(
            symbol=symbol,
            strategy=args.strategy,
            fast=args.fast,
            slow=args.slow,
            lookback=args.lookback,
            cash=args.cash,
            commission=args.commission,
        )
        exp_id = log_experiment(stats)

        ret = stats.get("Return [%]", "n/a")
        sharpe = stats.get("Sharpe Ratio", "n/a")
        max_dd = stats.get("Max. Drawdown [%]", "n/a")

        print(
            f"[OK] {symbol}: "
            f"fast={args.fast}, slow={args.slow}, cash={args.cash}, "
            f"commission={args.commission}, Return[%]={ret}, "
            f"Sharpe={sharpe}, MaxDD[%]={max_dd}, Experiment={exp_id}"
        )