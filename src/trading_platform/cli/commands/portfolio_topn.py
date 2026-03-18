from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from trading_platform.cli.common import print_symbol_list, resolve_symbols
from trading_platform.construction.service import build_top_n_portfolio_weights
from trading_platform.execution.policies import ExecutionPolicy
from trading_platform.signals.loaders import load_feature_frame
from trading_platform.signals.registry import SIGNAL_REGISTRY
from trading_platform.simulation.portfolio import simulate_target_weight_portfolio
from trading_platform.metadata.groups import build_group_series



def cmd_portfolio_topn(args: argparse.Namespace) -> None:
    symbols = resolve_symbols(args)
    print(
        f"Running top-N portfolio backtest for {len(symbols)} symbol(s): "
        f"{print_symbol_list(symbols)}"
    )

    if args.strategy not in SIGNAL_REGISTRY:
        raise SystemExit(f"Unsupported strategy: {args.strategy}")

    signal_fn = SIGNAL_REGISTRY[args.strategy]

    asset_return_frames: list[pd.Series] = []
    score_frames: list[pd.Series] = []

    for symbol in symbols:
        try:
            df = load_feature_frame(symbol)
            signal_df = signal_fn(
                df,
                fast=args.fast,
                slow=args.slow,
                lookback=args.lookback,
            )

            if "score" not in signal_df.columns:
                raise ValueError("Signal frame missing required column: score")

            asset_return_frames.append(signal_df["asset_return"].rename(symbol))
            score_frames.append(signal_df["score"].rename(symbol))

            print(f"[OK] {symbol}: loaded {len(signal_df)} rows")
        except Exception as e:
            print(f"[ERROR] {symbol}: failed to build signal frame -> {e}")

    if not asset_return_frames or not score_frames:
        print("No valid symbol frames available for top-N portfolio backtest.")
        return

    asset_returns = pd.concat(asset_return_frames, axis=1).sort_index().fillna(0.0)
    scores = pd.concat(score_frames, axis=1).sort_index()
    symbol_groups = build_group_series(
        symbols,
        path=args.group_map_path,
    )
    selection, target_weights = build_top_n_portfolio_weights(
        scores=scores,
        asset_returns=asset_returns,
        top_n=args.top_n,
        weighting_scheme=args.weighting_scheme,
        vol_window=args.vol_window,
        min_score=args.min_score,
        max_weight=args.max_weight,
        symbol_groups=symbol_groups,
        max_names_per_group=args.max_names_per_group,
        max_group_weight=args.max_group_weight,
    )
    execution_policy = ExecutionPolicy(
        rebalance_frequency=args.rebalance_frequency,
    )

    simulation_result = simulate_target_weight_portfolio(
        asset_returns=asset_returns,
        target_weights=target_weights,
        cost_per_turnover=args.commission,
        initial_equity=args.cash,
        execution_policy=execution_policy,
    )

    result = simulation_result.timeseries
    weights = simulation_result.weights
    positions = simulation_result.positions
    summary = simulation_result.summary

    summary["strategy"] = args.strategy
    summary["symbols"] = symbols
    summary["n_symbols"] = len(symbols)
    summary["top_n"] = args.top_n
    summary["weighting_scheme"] = args.weighting_scheme
    summary["vol_window"] = args.vol_window
    summary["min_score"] = args.min_score
    summary["max_weight"] = args.max_weight
    summary["rebalance_frequency"] = args.rebalance_frequency
    summary["fast"] = args.fast
    summary["slow"] = args.slow
    summary["lookback"] = args.lookback
    summary["max_names_per_group"] = args.max_names_per_group
    summary["max_group_weight"] = args.max_group_weight

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    result_path = output_dir / "portfolio_timeseries.csv"
    weights_path = output_dir / "portfolio_weights.csv"
    positions_path = output_dir / "portfolio_positions.csv"
    selection_path = output_dir / "portfolio_selection.csv"
    scores_path = output_dir / "portfolio_scores.csv"
    summary_path = output_dir / "portfolio_summary.json"
    groups_path = output_dir / "portfolio_groups.csv"
    symbol_groups.rename("group").to_csv(groups_path, header=True)

    result.to_csv(result_path, index=True)
    weights.to_csv(weights_path, index=True)
    positions.to_csv(positions_path, index=True)
    selection.to_csv(selection_path, index=True)
    scores.to_csv(scores_path, index=True)

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("\nTop-N portfolio summary:")
    for k, v in summary.items():
        print(f"  {k}: {v}")

    print(f"\nSaved portfolio timeseries to {result_path}")
    print(f"Saved portfolio weights to {weights_path}")
    print(f"Saved portfolio positions to {positions_path}")
    print(f"Saved portfolio selection to {selection_path}")
    print(f"Saved portfolio scores to {scores_path}")
    print(f"Saved portfolio summary to {summary_path}")
    print(f"Saved portfolio groups to {groups_path}")