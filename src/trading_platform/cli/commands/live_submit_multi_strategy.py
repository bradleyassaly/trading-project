from __future__ import annotations

from pathlib import Path

from trading_platform.broker.service import resolve_broker_adapter
from trading_platform.config.loader import (
    load_broker_config,
    load_execution_config,
    load_multi_strategy_portfolio_config,
)
from trading_platform.live.preview import (
    LivePreviewConfig,
    run_live_dry_run_preview_for_targets,
    write_live_dry_run_artifacts,
)
from trading_platform.live.submission import submit_live_orders
from trading_platform.portfolio.multi_strategy import (
    allocate_multi_strategy_portfolio,
    write_multi_strategy_artifacts,
)


def cmd_live_submit_multi_strategy(args) -> None:
    portfolio_config = load_multi_strategy_portfolio_config(args.config)
    execution_config = load_execution_config(args.execution_config) if getattr(args, "execution_config", None) else None
    broker_config = load_broker_config(args.broker_config)
    if getattr(args, "broker", None):
        broker_config = broker_config.__class__(**{**broker_config.to_dict(), "broker_name": args.broker})
    allocation_result = allocate_multi_strategy_portfolio(portfolio_config)
    allocation_paths = write_multi_strategy_artifacts(allocation_result, Path(args.output_dir))
    target_diagnostics = {
        "portfolio_construction_mode": "multi_strategy",
        "rebalance_timestamp": allocation_result.as_of,
        "selected_symbols": ",".join(sorted(set(row["symbol"] for row in allocation_result.sleeve_rows))),
        "target_selected_symbols": ",".join(sorted(allocation_result.combined_target_weights)),
        "target_selected_count": len(allocation_result.combined_target_weights),
        "realized_holdings_count": len(allocation_result.combined_target_weights),
        "realized_holdings_minus_top_n": 0,
        "average_gross_exposure": allocation_result.summary["gross_exposure_after_constraints"],
        "liquidity_excluded_count": 0,
        "sector_cap_excluded_count": 0,
        "turnover_cap_binding_count": int(allocation_result.summary["turnover_cap_binding"]),
        "turnover_buffer_blocked_replacements": 0,
        "semantic_warning": "",
        "multi_strategy_allocation": allocation_result.summary,
    }
    preview_config = LivePreviewConfig(
        symbols=sorted(allocation_result.combined_target_weights),
        preset_name="multi_strategy",
        universe_name=f"{allocation_result.summary['enabled_sleeve_count']}_sleeves",
        strategy="multi_strategy",
        reserve_cash_pct=portfolio_config.cash_reserve_pct,
        broker=broker_config.broker_name,
        output_dir=Path(args.output_dir),
    )
    preview_result = run_live_dry_run_preview_for_targets(
        config=preview_config,
        as_of=allocation_result.as_of,
        target_weights=allocation_result.combined_target_weights,
        latest_prices=allocation_result.latest_prices,
        target_diagnostics=target_diagnostics,
        execution_config=execution_config,
    )
    live_paths = write_live_dry_run_artifacts(preview_result)
    adapter = resolve_broker_adapter(broker_config)
    submission_result = submit_live_orders(
        preview_result=preview_result,
        broker_config=broker_config,
        broker_adapter=adapter,
        validate_only=bool(getattr(args, "validate_only", False)),
        output_dir=Path(args.output_dir),
    )
    print(f"Live submit broker: {broker_config.broker_name}")
    print(f"Validate only: {submission_result.validate_only}")
    print(f"Risk passed: {submission_result.summary.risk_passed}")
    print(f"Requested orders: {submission_result.summary.requested_order_count}")
    print(f"Submitted orders: {submission_result.summary.submitted_order_count}")
    print("Artifacts:")
    for key, value in sorted({**allocation_paths, **live_paths, **submission_result.artifacts}.items()):
        print(f"  {key}: {value}")
