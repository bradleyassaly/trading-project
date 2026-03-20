from __future__ import annotations

from pathlib import Path

from trading_platform.live.control import (
    LiveExecutionControlConfig,
    run_live_execution_control,
)


def _build_config(args) -> LiveExecutionControlConfig:
    return LiveExecutionControlConfig(
        symbols=args.symbols,
        universe=args.universe,
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
        order_type=args.order_type,
        time_in_force=args.time_in_force,
        broker=args.broker,
        mock_equity=args.mock_equity,
        mock_cash=args.mock_cash,
        mock_positions_path=args.mock_positions_path,
        kill_switch=args.kill_switch,
        kill_switch_path=args.kill_switch_path,
        blocked_symbols=tuple(args.blocked_symbols or []),
        max_gross_exposure=args.max_gross_exposure,
        max_net_exposure=args.max_net_exposure,
        max_position_weight_limit=args.max_position_weight_limit,
        max_group_exposure=args.max_group_exposure,
        max_order_notional=args.max_order_notional,
        max_daily_turnover=args.max_daily_turnover,
        min_cash_reserve=args.min_cash_reserve,
        max_data_staleness_days=args.max_data_staleness_days,
        max_config_staleness_days=args.max_config_staleness_days,
        approval_artifact_path=args.approval_artifact,
        approved=args.approved,
        drift_alerts_path=args.drift_alerts_path,
        output_dir=Path(args.output_dir),
    )


def cmd_validate_live(args) -> None:
    result = run_live_execution_control(
        config=_build_config(args),
        execute=False,
    )
    print(f"Live validation decision: {result.decision}")
    print(f"Reason codes: {', '.join(result.reason_codes) if result.reason_codes else 'none'}")
    for key, value in sorted(result.artifacts.items()):
        print(f"{key}: {value}")
