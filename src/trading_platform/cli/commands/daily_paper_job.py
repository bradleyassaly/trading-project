from __future__ import annotations

from pathlib import Path

from trading_platform.cli.common import resolve_symbols
from trading_platform.jobs.daily_paper_trading import run_daily_paper_trading_job
from trading_platform.paper.models import PaperTradingConfig


def cmd_daily_paper_job(args) -> None:
    symbols = resolve_symbols(args)

    config = PaperTradingConfig(
        symbols=symbols,
        signal_source=args.signal_source,
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
        approved_model_state_path=getattr(args, "approved_model_state", None),
        composite_artifact_dir=args.composite_artifact_dir,
        composite_horizon=args.composite_horizon,
        composite_weighting_scheme=args.composite_weighting_scheme,
        composite_portfolio_mode=args.composite_portfolio_mode,
        composite_long_quantile=args.composite_long_quantile,
        composite_short_quantile=args.composite_short_quantile,
        min_price=args.min_price,
        min_volume=args.min_volume,
        min_avg_dollar_volume=args.min_avg_dollar_volume,
        max_adv_participation=args.max_adv_participation,
        max_position_pct_of_adv=args.max_position_pct_of_adv,
        max_notional_per_name=args.max_notional_per_name,
    )

    print(
        "Running daily paper trading job for "
        f"{len(config.symbols)} symbol(s): {', '.join(config.symbols)}"
    )

    result = run_daily_paper_trading_job(
        config=config,
        state_path=Path(args.state_path),
        output_dir=Path(args.output_dir),
        auto_apply_fills=args.auto_apply_fills,
    )

    print(f"As of: {result.as_of}")
    print(f"Orders: {result.order_count}")
    print(f"Fills: {result.fill_count}")
    print(f"Cash: {result.cash:,.2f}")
    print(f"Equity: {result.equity:,.2f}")
    print("Artifacts:")
    for name, path in sorted(result.artifact_paths.items()):
        print(f"  {name}: {path}")
    print("Ledgers:")
    for name, path in sorted(result.ledger_paths.items()):
        print(f"  {name}: {path}")
