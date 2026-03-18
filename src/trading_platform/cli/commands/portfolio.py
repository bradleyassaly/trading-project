from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from trading_platform.cli.common import print_symbol_list, resolve_symbols
from trading_platform.portfolio.engine import run_equal_weight_portfolio_backtest
from trading_platform.portfolio.stats import summarize_portfolio_result
from trading_platform.signals.loaders import load_feature_frame
from trading_platform.signals.registry import SIGNAL_REGISTRY


def cmd_portfolio(args: argparse.Namespace) -> None:
    symbols = resolve_symbols(args)
    print(
        f"Running portfolio backtest for {len(symbols)} symbol(s): "
        f"{print_symbol_list(symbols)}"
    )

    if args.strategy not in SIGNAL_REGISTRY:
        raise SystemExit(f"Unsupported strategy: {args.strategy}")

    signal_fn = SIGNAL_REGISTRY[args.strategy]

    asset_return_frames: list[pd.Series] = []
    position_frames: list[pd.Series] = []

    for symbol in symbols:
        try:
            df = load_feature_frame(symbol)
            signal_df = signal_fn(
                df,
                fast=args.fast,
                slow=args.slow,
                lookback=args.lookback,
            )

            asset_return_frames.append(signal_df["asset_return"].rename(symbol))
            position_frames.append(signal_df["position"].rename(symbol))
            print(f"[OK] {symbol}: loaded {len(signal_df)} rows")
        except Exception as e:
            print(f"[ERROR] {symbol}: failed to build signal frame -> {e}")

    if not asset_return_frames or not position_frames:
        print("No valid symbol frames available for portfolio backtest.")
        return

    asset_returns = pd.concat(asset_return_frames, axis=1).sort_index().fillna(0.0)
    positions = pd.concat(position_frames, axis=1).sort_index().fillna(0.0)

    result, weights = run_equal_weight_portfolio_backtest(
        asset_returns=asset_returns,
        positions=positions,
    )

    summary = summarize_portfolio_result(result)
    summary["strategy"] = args.strategy
    summary["symbols"] = symbols
    summary["n_symbols"] = len(symbols)
    summary["fast"] = args.fast
    summary["slow"] = args.slow
    summary["lookback"] = args.lookback

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    result_path = output_dir / "portfolio_timeseries.csv"
    weights_path = output_dir / "portfolio_weights.csv"
    positions_path = output_dir / "portfolio_positions.csv"
    summary_path = output_dir / "portfolio_summary.json"

    result.to_csv(result_path, index=True)
    weights.to_csv(weights_path, index=True)
    positions.to_csv(positions_path, index=True)

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("\nPortfolio summary:")
    for k, v in summary.items():
        print(f"  {k}: {v}")

    print(f"\nSaved portfolio timeseries to {result_path}")
    print(f"Saved portfolio weights to {weights_path}")
    print(f"Saved portfolio positions to {positions_path}")
    print(f"Saved portfolio summary to {summary_path}")