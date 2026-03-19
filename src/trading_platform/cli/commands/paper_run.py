from __future__ import annotations

import argparse
from pathlib import Path

from trading_platform.cli.common import print_symbol_list, resolve_symbols
from trading_platform.paper.service import (
    JsonPaperStateStore,
    PaperTradingConfig,
    run_paper_trading_cycle,
    write_paper_trading_artifacts,
)


def cmd_paper_run(args: argparse.Namespace) -> None:
    symbols = resolve_symbols(args)
    print(
        f"Running paper trading cycle for {len(symbols)} symbol(s): "
        f"{print_symbol_list(symbols)}"
    )

    config = PaperTradingConfig(
        symbols=symbols,
        strategy=args.strategy,
        fast=args.fast,
        slow=args.slow,
        lookback=args.lookback,
        top_n=args.top_n,
        weighting_scheme=args.weighting_scheme,
        vol_window=args.vol_window,
        min_score=args.min_score,
        max_weight=args.max_weight,
        max_names_per_group=args.max_names_per_group,
        max_group_weight=args.max_group_weight,
        group_map_path=args.group_map_path,
        rebalance_frequency=args.rebalance_frequency,
        timing=args.timing,
        initial_cash=args.initial_cash,
        min_trade_dollars=args.min_trade_dollars,
        lot_size=args.lot_size,
        reserve_cash_pct=args.reserve_cash_pct,
    )

    state_store = JsonPaperStateStore(args.state_path)
    result = run_paper_trading_cycle(
        config=config,
        state_store=state_store,
        auto_apply_fills=args.auto_apply_fills,
    )

    run_output_dir = _resolve_run_output_dir(args.output_dir, result.as_of)
    artifact_paths = write_paper_trading_artifacts(
        result=result,
        output_dir=run_output_dir,
    )

    print("\nPaper trading summary:")
    print(f"  as_of: {result.as_of}")
    print(f"  cash: {result.state.cash:,.2f}")
    print(f"  gross_market_value: {result.state.gross_market_value:,.2f}")
    print(f"  equity: {result.state.equity:,.2f}")
    print(f"  order_count: {len(result.orders)}")
    print(f"  skipped_symbols: {len(result.skipped_symbols)}")
    print(f"  state_path: {Path(args.state_path)}")
    for name, path in artifact_paths.items():
        print(f"  {name}: {path}")



def _resolve_run_output_dir(base_dir: str | Path, as_of: str) -> Path:
    safe_as_of = as_of.replace(":", "-")
    return Path(base_dir) / f"run_{safe_as_of}"
