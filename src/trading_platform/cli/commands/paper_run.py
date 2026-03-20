from __future__ import annotations

from pathlib import Path

from trading_platform.paper.models import PaperTradingConfig
from trading_platform.paper.service import (
    JsonPaperStateStore,
    run_paper_trading_cycle,
    write_paper_trading_artifacts,
)
from trading_platform.research.experiment_tracking import (
    build_paper_experiment_record,
    register_experiment,
)
from trading_platform.universes.registry import get_universe_symbols


def _resolve_symbols(args) -> list[str]:
    has_symbols = bool(getattr(args, "symbols", None))
    has_universe = bool(getattr(args, "universe", None))

    if has_symbols == has_universe:
        raise ValueError("Provide exactly one of --symbols or --universe")

    if has_universe:
        return get_universe_symbols(args.universe)

    return list(args.symbols)


def cmd_paper_run(args) -> None:
    symbols = _resolve_symbols(args)

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
        "Running paper trading cycle for "
        f"{len(config.symbols)} symbol(s): {', '.join(config.symbols)}"
    )

    state_store = JsonPaperStateStore(Path(args.state_path))
    result = run_paper_trading_cycle(
        config=config,
        state_store=state_store,
        auto_apply_fills=args.auto_apply_fills,
    )
    artifact_paths = write_paper_trading_artifacts(
        result=result,
        output_dir=Path(args.output_dir),
    )
    output_dir = Path(args.output_dir)
    tracker_dir_arg = getattr(args, "experiment_tracker_dir", None)
    tracker_dir = Path(tracker_dir_arg) if tracker_dir_arg else output_dir.parent / "experiment_tracking"
    registry_paths = register_experiment(
        build_paper_experiment_record(output_dir),
        tracker_dir=tracker_dir,
    )

    print(f"As of: {result.as_of}")
    print(f"Orders: {len(result.orders)}")
    print(f"Fills: {len(result.fills)}")
    print(f"Cash: {result.state.cash:,.2f}")
    print(f"Equity: {result.state.equity:,.2f}")
    print("Artifacts:")
    for name, path in sorted(artifact_paths.items()):
        print(f"  {name}: {path}")
    print(f"  experiment_registry_path: {registry_paths['experiment_registry_path']}")


def _resolve_run_output_dir(base_dir: str | Path, as_of: str) -> Path:
    safe_as_of = as_of.replace(":", "-")
    return Path(base_dir) / f"run_{safe_as_of}"
