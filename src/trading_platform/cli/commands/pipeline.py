from __future__ import annotations

import argparse

from trading_platform.backtests.engine import run_backtest
from trading_platform.cli.common import print_symbol_list, resolve_symbols
from trading_platform.data.ingest import ingest_symbol
from trading_platform.experiments.tracker import log_experiment
from trading_platform.features.build import build_features


def cmd_pipeline(args: argparse.Namespace) -> None:
    symbols = resolve_symbols(args)
    print(f"Running pipeline for {len(symbols)} symbol(s): {print_symbol_list(symbols)}")

    for symbol in symbols:
        raw_path = ingest_symbol(symbol, start=args.start)
        feat_path = build_features(symbol, feature_groups=args.feature_groups)
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

        print(f"[OK] {symbol}")
        print(f"  raw: {raw_path}")
        print(f"  features: {feat_path}")
        print(f"  return[%]: {ret}")
        print(f"  sharpe: {sharpe}")
        print(f"  max drawdown[%]: {max_dd}")
        print(f"  experiment: {exp_id}")