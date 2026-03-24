from __future__ import annotations

from pathlib import Path

from trading_platform.cli.config_support import apply_workflow_config, option_is_explicit
from trading_platform.cli.common import normalize_paper_weighting_scheme, resolve_symbols
from trading_platform.cli.presets import apply_cli_preset
from trading_platform.config.loader import load_execution_config, load_paper_run_workflow_config
from trading_platform.paper.models import PaperTradingConfig
from trading_platform.paper.persistence import persist_paper_run_outputs
from trading_platform.paper.service import (
    JsonPaperStateStore,
    run_paper_trading_cycle,
    write_paper_trading_artifacts,
)
from trading_platform.research.experiment_tracking import (
    build_paper_experiment_record,
    register_experiment,
)


def cmd_paper_run(args) -> None:
    loaded_config = None
    if getattr(args, "config", None):
        loaded = load_paper_run_workflow_config(args.config)
        loaded_config = loaded
        if getattr(loaded, "preset", None) and not option_is_explicit(args, "preset"):
            args.preset = loaded.preset
    apply_cli_preset(args)
    apply_workflow_config(
        args,
        config_path=getattr(args, "config", None),
        loader=load_paper_run_workflow_config,
    )
    symbols = resolve_symbols(args)

    config = PaperTradingConfig(
        symbols=symbols,
        preset_name=getattr(args, "_resolved_preset", getattr(args, "preset", None)),
        universe_name=getattr(args, "universe", None),
        signal_source=args.signal_source,
        strategy=args.strategy,
        fast=args.fast,
        slow=args.slow,
        lookback=args.lookback,
        lookback_bars=getattr(args, "lookback_bars", None),
        skip_bars=getattr(args, "skip_bars", 0),
        top_n=args.top_n,
        weighting_scheme=normalize_paper_weighting_scheme(args.weighting_scheme),
        vol_window=getattr(args, "vol_lookback_bars", args.vol_window),
        rebalance_bars=getattr(args, "rebalance_bars", None),
        portfolio_construction_mode=getattr(args, "portfolio_construction_mode", "pure_topn"),
        max_position_weight=getattr(args, "max_position_weight", None),
        min_score=args.min_score,
        max_weight=args.max_weight,
        max_names_per_group=args.max_names_per_group,
        max_group_weight=args.max_group_weight,
        group_map_path=args.group_map_path,
        max_names_per_sector=getattr(args, "max_names_per_sector", None),
        turnover_buffer_bps=getattr(args, "turnover_buffer_bps", 0.0),
        max_turnover_per_rebalance=getattr(args, "max_turnover_per_rebalance", None),
        benchmark=getattr(args, "benchmark", None),
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
        use_alpaca_latest_data=bool(getattr(args, "use_alpaca_latest_data", False)),
        latest_data_max_age_seconds=int(getattr(args, "latest_data_max_age_seconds", 86_400)),
        slippage_model=str(getattr(args, "slippage_model", "none")),
        slippage_buy_bps=float(getattr(args, "slippage_buy_bps", 0.0)),
        slippage_sell_bps=float(getattr(args, "slippage_sell_bps", 0.0)),
        ensemble_enabled=bool(getattr(args, "enable_ensemble", False) or getattr(args, "ensemble_enabled", False)),
        ensemble_mode=str(getattr(args, "ensemble_mode", "disabled")),
        ensemble_weight_method=str(getattr(args, "ensemble_weight_method", "equal")),
        ensemble_normalize_scores=str(getattr(args, "ensemble_normalize_scores", "rank_pct")),
        ensemble_max_members=int(getattr(args, "ensemble_max_members", 5)),
        ensemble_require_promoted_only=bool(getattr(args, "ensemble_require_promoted_only", True)),
        ensemble_max_members_per_family=getattr(args, "ensemble_max_members_per_family", None),
        ensemble_minimum_member_observations=int(getattr(args, "ensemble_minimum_member_observations", 0)),
        ensemble_minimum_member_metric=getattr(args, "ensemble_minimum_member_metric", None),
        sub_universe_id=getattr(args, "sub_universe_id", getattr(loaded_config, "sub_universe_id", None) if loaded_config is not None else None),
        universe_filters=list(getattr(loaded_config, "universe_filters", []) or []),
        data_sources=getattr(loaded_config, "data_sources", {}) if loaded_config is not None else {},
    )

    print(
        "Running paper trading cycle for "
        f"{len(config.symbols)} symbol(s): {', '.join(config.symbols)}"
    )
    if config.preset_name:
        print(f"Preset: {config.preset_name}")

    execution_config = load_execution_config(args.execution_config) if getattr(args, "execution_config", None) else None
    state_path = Path(args.state_path)
    state_file_preexisting = state_path.exists()
    state_store = JsonPaperStateStore(state_path)
    result = run_paper_trading_cycle(
        config=config,
        state_store=state_store,
        execution_config=execution_config,
        auto_apply_fills=args.auto_apply_fills,
    )
    artifact_paths = write_paper_trading_artifacts(
        result=result,
        output_dir=Path(args.output_dir),
    )
    persistence_paths, health_checks, latest_summary = persist_paper_run_outputs(
        result=result,
        config=config,
        output_dir=Path(args.output_dir),
        state_file_preexisting=state_file_preexisting,
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
    target_diagnostics = result.diagnostics.get("target_construction", {})
    if config.strategy == "xsec_momentum_topn" and target_diagnostics:
        print(f"portfolio_construction_mode: {target_diagnostics.get('portfolio_construction_mode')}")
        print(f"rebalance_timestamp: {target_diagnostics.get('rebalance_timestamp')}")
        print(f"selected_names: {target_diagnostics.get('selected_symbols') or 'none'}")
        print(f"target_names: {target_diagnostics.get('target_selected_symbols') or 'none'}")
        print(f"realized_holdings_count: {target_diagnostics.get('realized_holdings_count')}")
        print(f"realized_holdings_minus_top_n: {target_diagnostics.get('realized_holdings_minus_top_n')}")
        print(f"average_gross_exposure: {target_diagnostics.get('average_gross_exposure')}")
        print(f"liquidity_excluded_count: {target_diagnostics.get('liquidity_excluded_count')}")
        print(f"sector_cap_excluded_count: {target_diagnostics.get('sector_cap_excluded_count')}")
        print(f"turnover_cap_binding_count: {target_diagnostics.get('turnover_cap_binding_count')}")
        print(f"turnover_buffer_blocked_replacements: {target_diagnostics.get('turnover_buffer_blocked_replacements')}")
        print(f"semantic_warning: {target_diagnostics.get('semantic_warning') or 'none'}")
    execution_summary = result.diagnostics.get("execution", {}).get("execution_summary", {})
    paper_execution = result.diagnostics.get("paper_execution", {})
    if execution_summary:
        print(f"Requested orders: {execution_summary.get('requested_order_count', 0)}")
        print(f"Executable orders: {execution_summary.get('executable_order_count', 0)}")
        print(f"Rejected orders: {execution_summary.get('rejected_order_count', 0)}")
        print(f"Expected total cost: {execution_summary.get('expected_total_cost', 0.0):.6f}")
    if paper_execution:
        print(f"Latest data source: {paper_execution.get('latest_data_source')}")
        print(f"Latest data fallback used: {paper_execution.get('latest_data_fallback_used')}")
        print(f"Latest data stale: {paper_execution.get('latest_data_stale')}")
        print(f"Slippage model: {paper_execution.get('slippage_model')}")
        print(f"Ensemble enabled: {paper_execution.get('ensemble_enabled')}")
    health_counts = {
        "pass": sum(1 for item in health_checks if item["status"] == "pass"),
        "warn": sum(1 for item in health_checks if item["status"] == "warn"),
        "fail": sum(1 for item in health_checks if item["status"] == "fail"),
    }
    print(f"Health checks: pass={health_counts['pass']} warn={health_counts['warn']} fail={health_counts['fail']}")
    for item in health_checks:
        if item["status"] != "pass":
            print(f"  {item['status']}: {item['check_name']} -> {item['message']}")
    print("Artifacts:")
    combined_paths = dict(artifact_paths)
    combined_paths.update(persistence_paths)
    for name, path in sorted(combined_paths.items()):
        print(f"  {name}: {path}")
    print(f"  experiment_registry_path: {registry_paths['experiment_registry_path']}")


def _resolve_run_output_dir(base_dir: str | Path, as_of: str) -> Path:
    safe_as_of = as_of.replace(":", "-")
    return Path(base_dir) / f"run_{safe_as_of}"
