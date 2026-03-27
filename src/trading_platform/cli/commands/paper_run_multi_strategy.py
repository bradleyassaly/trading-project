from __future__ import annotations

from pathlib import Path

from trading_platform.config.loader import load_execution_config
from trading_platform.paper.models import PaperTradingConfig
from trading_platform.paper.persistence import persist_paper_run_outputs
from trading_platform.paper.service import (
    JsonPaperStateStore,
    run_paper_trading_cycle_for_targets,
    write_paper_trading_artifacts,
)
from trading_platform.portfolio.multi_strategy import (
    allocate_multi_strategy_portfolio,
    write_multi_strategy_artifacts,
)
from trading_platform.portfolio.strategy_execution_handoff import (
    StrategyExecutionHandoffConfig,
    resolve_strategy_execution_handoff,
    write_strategy_execution_handoff_summary,
)
from trading_platform.research.experiment_tracking import (
    build_paper_experiment_record,
    register_experiment,
)


def _build_multi_strategy_paper_config(result, reserve_cash_pct: float) -> PaperTradingConfig:
    symbols = sorted(result.combined_target_weights)
    return PaperTradingConfig(
        symbols=symbols,
        preset_name="multi_strategy",
        universe_name=f"{result.summary['enabled_sleeve_count']}_sleeves",
        strategy="multi_strategy",
        signal_source="legacy",
        reserve_cash_pct=reserve_cash_pct,
    )


def cmd_paper_run_multi_strategy(args) -> None:
    handoff = resolve_strategy_execution_handoff(
        args.config,
        config=StrategyExecutionHandoffConfig(),
    )
    handoff_summary_path = write_strategy_execution_handoff_summary(
        handoff=handoff,
        output_dir=Path(args.output_dir),
        artifact_name="paper_active_strategy_summary.json",
    )
    if handoff.portfolio_config is None:
        if handoff.summary.get("fail_if_no_active_strategies"):
            raise ValueError(f"No active strategies available for paper trading: {args.config}")
        print("No active strategies available for paper trading.")
        print(f"Handoff summary: {handoff_summary_path}")
        return
    portfolio_config = handoff.portfolio_config
    execution_config = load_execution_config(args.execution_config) if getattr(args, "execution_config", None) else None
    allocation_result = allocate_multi_strategy_portfolio(portfolio_config)
    allocation_paths = write_multi_strategy_artifacts(allocation_result, Path(args.output_dir))

    config = _build_multi_strategy_paper_config(
        allocation_result,
        reserve_cash_pct=portfolio_config.cash_reserve_pct,
    )
    state_path = Path(args.state_path)
    state_file_preexisting = state_path.exists()
    state_store = JsonPaperStateStore(state_path)
    target_diagnostics = {
        "portfolio_construction_mode": "multi_strategy",
        "rebalance_timestamp": allocation_result.as_of,
        "selected_symbols": ",".join(sorted(set(row["symbol"] for row in allocation_result.sleeve_rows))),
        "target_selected_symbols": ",".join(sorted(allocation_result.combined_target_weights)),
        "requested_active_strategy_count": allocation_result.summary.get("requested_active_strategy_count"),
        "requested_symbol_count": allocation_result.summary.get("requested_symbol_count"),
        "pre_validation_target_symbol_count": allocation_result.summary.get("pre_validation_target_symbol_count"),
        "post_validation_target_symbol_count": len(allocation_result.combined_target_weights),
        "usable_symbol_count": allocation_result.summary.get("usable_symbol_count"),
        "skipped_symbol_count": allocation_result.summary.get("skipped_symbol_count"),
        "target_drop_stage": allocation_result.summary.get("target_drop_stage"),
        "zero_target_reason": allocation_result.summary.get("zero_target_reason"),
        "target_drop_reason": allocation_result.summary.get("target_drop_reason"),
        "latest_price_source_summary": allocation_result.summary.get("latest_price_source_summary", {}),
        "generated_preset_path": allocation_result.summary.get("generated_preset_path"),
        "signal_artifact_path": allocation_result.summary.get("signal_artifact_path"),
        "realized_holdings_count": len(allocation_result.combined_target_weights),
        "realized_holdings_minus_top_n": 0,
        "average_gross_exposure": allocation_result.summary["gross_exposure_after_constraints"],
        "liquidity_excluded_count": sum(
            int(bundle.diagnostics.get("liquidity_excluded_count") or 0)
            for bundle in allocation_result.sleeve_bundles
        ),
        "sector_cap_excluded_count": sum(
            1
            for row in allocation_result.summary["symbols_removed_or_clipped"]
            if row["constraint_name"] == "sector_cap"
        ),
        "turnover_cap_binding_count": int(allocation_result.summary["turnover_cap_binding"]),
        "turnover_buffer_blocked_replacements": sum(
            int(bundle.diagnostics.get("turnover_buffer_blocked_replacements") or 0)
            for bundle in allocation_result.sleeve_bundles
        ),
        "semantic_warning": "portfolio_constraints_applied"
        if allocation_result.summary["symbols_removed_or_clipped"]
        else "",
        "target_selected_count": len(allocation_result.combined_target_weights),
        "summary": {
            "mean_turnover": allocation_result.summary["turnover_estimate"],
        },
        "multi_strategy_allocation": allocation_result.summary,
        "strategy_execution_handoff": handoff.summary,
    }
    result = run_paper_trading_cycle_for_targets(
        config=config,
        state_store=state_store,
        as_of=allocation_result.as_of,
        latest_prices=allocation_result.latest_prices,
        latest_scores={},
        latest_scheduled_weights=allocation_result.combined_target_weights,
        latest_effective_weights=allocation_result.combined_target_weights,
        target_diagnostics=target_diagnostics,
        skipped_symbols=sorted(
            {
                str(row["symbol"])
                for row in getattr(allocation_result, "execution_symbol_coverage_rows", [])
                if str(row.get("skip_reason") or "")
            }
        ),
        extra_diagnostics={
            "multi_strategy_allocation": allocation_result.summary,
            "strategy_execution_handoff": handoff.summary,
        },
        execution_config=execution_config,
        auto_apply_fills=bool(getattr(args, "auto_apply_fills", True)),
    )
    paper_paths = write_paper_trading_artifacts(result=result, output_dir=Path(args.output_dir))
    persistence_paths, health_checks, latest_summary = persist_paper_run_outputs(
        result=result,
        config=config,
        output_dir=Path(args.output_dir),
        state_file_preexisting=state_file_preexisting,
    )
    tracker_dir = Path(args.output_dir).parent / "experiment_tracking"
    registry_paths = register_experiment(
        build_paper_experiment_record(Path(args.output_dir)),
        tracker_dir=tracker_dir,
    )

    print(f"As of: {result.as_of}")
    print(f"Enabled sleeves: {allocation_result.summary['enabled_sleeve_count']}")
    print(f"Orders: {len(result.orders)}")
    print(f"Fills: {len(result.fills)}")
    print(f"Cash: {result.state.cash:,.2f}")
    print(f"Equity: {result.state.equity:,.2f}")
    accounting_summary = result.diagnostics.get("accounting", {})
    if accounting_summary:
        print(f"Fill application status: {accounting_summary.get('fill_application_status')}")
        print(f"Total PnL: {float(accounting_summary.get('total_pnl', 0.0)):.2f}")
        print(f"Unrealized PnL: {float(accounting_summary.get('unrealized_pnl', 0.0)):.2f}")
        print(f"Realized PnL: {float(accounting_summary.get('cumulative_realized_pnl', 0.0)):.2f}")
    print(f"Gross exposure: {allocation_result.summary['gross_exposure_after_constraints']:.6f}")
    print(f"Active strategies: {handoff.summary.get('active_strategy_count', 0)}")
    if allocation_result.summary.get("zero_target_reason"):
        print(f"Zero target reason: {allocation_result.summary['zero_target_reason']}")
    print(f"Turnover estimate: {allocation_result.summary['turnover_estimate']:.6f}")
    execution_summary = result.diagnostics.get("execution", {}).get("execution_summary", {})
    if execution_summary:
        print(f"Executable orders: {execution_summary.get('executable_order_count', 0)}")
        print(f"Rejected orders: {execution_summary.get('rejected_order_count', 0)}")
        print(f"Expected total cost: {execution_summary.get('expected_total_cost', 0.0):.6f}")
    print("Artifacts:")
    combined_paths = {**allocation_paths, **paper_paths, **persistence_paths}
    for name, path in sorted(combined_paths.items()):
        print(f"  {name}: {path}")
    print(f"  paper_active_strategy_summary_path: {handoff_summary_path}")
    print(f"  experiment_registry_path: {registry_paths['experiment_registry_path']}")
