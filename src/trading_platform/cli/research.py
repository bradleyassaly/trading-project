from __future__ import annotations

import argparse

from trading_platform.backtests.engine import run_backtest
from trading_platform.cli.common import (
    add_symbol_arguments,
    print_symbol_list,
    resolve_symbols,
)
from trading_platform.experiments.tracker import log_experiment


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run backtests for one or more ticker symbols."
    )
    add_symbol_arguments(parser)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    symbols = resolve_symbols(args)
    print(f"Running research for {len(symbols)} symbol(s): {print_symbol_list(symbols)}")

    for symbol in symbols:
        stats = run_backtest(symbol)
        exp_id = log_experiment(stats)

        ret = stats.get("Return [%]", "n/a")
        sharpe = stats.get("Sharpe Ratio", "n/a")
        max_dd = stats.get("Max. Drawdown [%]", "n/a")

        print(
            f"[OK] {symbol}: "
            f"Return[%]={ret}, Sharpe={sharpe}, MaxDD[%]={max_dd}, Experiment={exp_id}"
        )


if __name__ == "__main__":
    main()

