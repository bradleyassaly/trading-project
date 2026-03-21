from __future__ import annotations

from pathlib import Path

from trading_platform.cli.common import resolve_symbols
from trading_platform.cli.presets import apply_cli_preset
from trading_platform.config.loader import load_execution_config
from trading_platform.live.preview import (
    LivePreviewConfig,
    run_live_dry_run_preview,
    write_live_dry_run_artifacts,
)


def _build_config(args) -> LivePreviewConfig:
    symbols = resolve_symbols(args)
    return LivePreviewConfig(
        symbols=symbols,
        preset_name=getattr(args, "_resolved_preset", getattr(args, "preset", None)),
        universe_name=getattr(args, "universe", None),
        strategy=args.strategy,
        fast=args.fast,
        slow=args.slow,
        lookback=args.lookback,
        lookback_bars=getattr(args, "lookback_bars", None),
        skip_bars=getattr(args, "skip_bars", 0),
        top_n=args.top_n,
        weighting_scheme=args.weighting_scheme,
        vol_lookback_bars=getattr(args, "vol_lookback_bars", 20),
        rebalance_bars=getattr(args, "rebalance_bars", None),
        portfolio_construction_mode=getattr(args, "portfolio_construction_mode", "pure_topn"),
        max_position_weight=getattr(args, "max_position_weight", None),
        min_avg_dollar_volume=getattr(args, "min_avg_dollar_volume", None),
        max_names_per_sector=getattr(args, "max_names_per_sector", None),
        turnover_buffer_bps=getattr(args, "turnover_buffer_bps", 0.0),
        max_turnover_per_rebalance=getattr(args, "max_turnover_per_rebalance", None),
        benchmark=getattr(args, "benchmark", None),
        initial_cash=args.initial_cash,
        min_trade_dollars=args.min_trade_dollars,
        lot_size=args.lot_size,
        reserve_cash_pct=args.reserve_cash_pct,
        order_type=args.order_type,
        time_in_force=args.time_in_force,
        broker=args.broker,
        mock_equity=args.mock_equity,
        mock_cash=args.mock_cash,
        mock_positions_path=getattr(args, "mock_positions_path", None),
        output_dir=Path(args.output_dir),
    )


def cmd_live_dry_run(args) -> None:
    apply_cli_preset(args)
    config = _build_config(args)
    print(f"Running live dry-run for {len(config.symbols)} symbol(s): {', '.join(config.symbols)}")

    execution_config = load_execution_config(args.execution_config) if getattr(args, "execution_config", None) else None
    result = run_live_dry_run_preview(config, execution_config=execution_config) if execution_config is not None else run_live_dry_run_preview(config)
    artifact_paths = write_live_dry_run_artifacts(result)

    print(f"As of: {result.as_of}")
    print(f"Broker: {config.broker}")
    if config.preset_name:
        print(f"Preset: {config.preset_name}")
    print(f"Broker equity: {result.account.equity:,.2f}")
    print(f"Broker cash: {result.account.cash:,.2f}")
    print(f"Current broker positions: {len(result.positions)}")
    print(f"Open orders: {len(result.open_orders)}")
    print(f"Raw computed orders: {len(result.reconciliation.orders)}")
    print(f"Adjusted proposed orders: {len(result.adjusted_orders)}")
    if result.execution_result is not None:
        print(f"Requested orders: {result.execution_result.summary.requested_order_count}")
        print(f"Executable orders: {result.execution_result.summary.executable_order_count}")
        print(f"Rejected orders: {result.execution_result.summary.rejected_order_count}")
        print(f"Expected total cost: {result.execution_result.summary.expected_total_cost:.6f}")

    target = result.target_diagnostics
    print(f"portfolio_construction_mode: {config.portfolio_construction_mode}")
    print(f"rebalance_timestamp: {target.get('rebalance_timestamp', result.as_of)}")
    print(f"selected_names: {target.get('selected_symbols', '')}")
    print(f"target_names: {target.get('target_selected_symbols', '')}")
    print(f"realized_holdings_count: {target.get('realized_holdings_count')}")
    print(f"realized_holdings_minus_top_n: {target.get('realized_holdings_minus_top_n')}")
    print(f"average_gross_exposure: {target.get('average_gross_exposure')}")
    print(f"liquidity_excluded_count: {target.get('liquidity_excluded_count')}")
    print(f"sector_cap_excluded_count: {target.get('sector_cap_excluded_count')}")
    print(f"turnover_cap_binding_count: {target.get('turnover_cap_binding_count')}")
    print(f"turnover_buffer_blocked_replacements: {target.get('turnover_buffer_blocked_replacements')}")
    print(f"semantic_warning: {target.get('semantic_warning') or 'none'}")

    pass_count = sum(1 for check in result.health_checks if check.status == "pass")
    warn_count = sum(1 for check in result.health_checks if check.status == "warn")
    fail_count = sum(1 for check in result.health_checks if check.status == "fail")
    print(f"Health checks: pass={pass_count} warn={warn_count} fail={fail_count}")
    for check in result.health_checks:
        if check.status != "pass":
            print(f"  {check.status}: {check.check_name} -> {check.message}")

    print("Artifacts:")
    for key, value in sorted(artifact_paths.items()):
        print(f"  {key}: {value}")
