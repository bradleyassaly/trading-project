from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from trading_platform.artifact_schemas import WorkflowArtifactSummary
from trading_platform.backtests.engine import run_backtest_on_df
from trading_platform.cli.config_support import load_and_apply_workflow_config
from trading_platform.cli.common import (
    build_strategy_params,
    prepare_research_frame,
    print_symbol_list,
    resolve_symbols,
    resolve_turnover_cost,
)
from trading_platform.cli.presets import apply_cli_preset
from trading_platform.config.loader import load_research_run_workflow_config
from trading_platform.execution.policies import ExecutionPolicy
from trading_platform.experiments.tracker import log_experiment
from trading_platform.research.diagnostics import activity_note
from trading_platform.research.service import (
    run_vectorized_research_on_df,
    to_legacy_stats,
)
from trading_platform.research.xsec_momentum import run_xsec_momentum_topn
from trading_platform.db.services import DatabaseLineageService, register_artifact_bundle


def _save_run_artifacts(
    *,
    output_dir: Path,
    symbol: str,
    strategy: str,
    engine: str,
    feature_path: Path,
    effective_start: str,
    effective_end: str,
    rows_used: int,
    stats: dict[str, object],
    experiment_id: str,
    result=None,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    metadata_path = output_dir / f"{symbol}_{strategy}_{engine}_run_summary.json"
    summary = WorkflowArtifactSummary(
        summary_type="research_run",
        workflow_stage="research_run",
        timestamp=str(effective_end),
        status="succeeded",
        name=f"{symbol}_{strategy}",
        strategy=strategy,
        universe=symbol,
        key_counts={"rows_used": rows_used},
        key_metrics={
            "return_pct": stats.get("Return [%]"),
            "sharpe_ratio": stats.get("Sharpe Ratio"),
            "max_drawdown_pct": stats.get("Max. Drawdown [%]"),
        },
        details={
            "engine": engine,
            "effective_start_date": effective_start,
            "effective_end_date": effective_end,
            "feature_path": str(feature_path),
            "experiment_id": experiment_id,
            "stats": stats,
        },
    )
    payload = {
        **summary.to_dict(),
        "symbol": symbol,
        "engine": engine,
    }
    metadata_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    print(f"  saved summary: {metadata_path}")
    paths: dict[str, Path] = {"summary_json": metadata_path}

    if result is None:
        return paths

    timeseries_path = output_dir / f"{symbol}_{strategy}_timeseries.csv"
    signal_path = output_dir / f"{symbol}_{strategy}_signals.csv"
    result.simulation.timeseries.to_csv(timeseries_path, index=True)
    result.signal_frame.to_csv(signal_path, index=True)
    print(f"  saved timeseries: {timeseries_path}")
    print(f"  saved signals: {signal_path}")
    paths["timeseries_csv"] = timeseries_path
    paths["signals_csv"] = signal_path
    return paths


def _save_xsec_run_artifacts(
    *,
    output_dir: Path,
    strategy: str,
    engine: str,
    symbols: list[str],
    effective_start: str,
    effective_end: str,
    rows_used: int,
    stats: dict[str, object],
    experiment_id: str,
    result,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    metadata_path = output_dir / f"{strategy}_{engine}_universe_run_summary.json"
    summary = WorkflowArtifactSummary(
        summary_type="research_run",
        workflow_stage="research_run",
        timestamp=str(effective_end),
        status="succeeded",
        name=f"{strategy}_{engine}",
        strategy=strategy,
        key_counts={"rows_used": rows_used, "symbol_count": len(symbols)},
        key_metrics={
            "gross_return_pct": stats.get("gross_return_pct"),
            "net_return_pct": stats.get("net_return_pct"),
            "sharpe_ratio": stats.get("Sharpe Ratio"),
        },
        details={
            "symbols": symbols,
            "engine": engine,
            "effective_start_date": effective_start,
            "effective_end_date": effective_end,
            "experiment_id": experiment_id,
            "stats": stats,
        },
    )
    payload = {
        **summary.to_dict(),
        "symbols": symbols,
        "engine": engine,
    }
    metadata_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    timeseries_path = output_dir / f"{strategy}_portfolio_timeseries.csv"
    weights_path = output_dir / f"{strategy}_portfolio_weights.csv"
    positions_path = output_dir / f"{strategy}_portfolio_positions.csv"
    scores_path = output_dir / f"{strategy}_scores.csv"
    rebalance_path = output_dir / f"{strategy}_rebalance_diagnostics.csv"
    result.timeseries.to_csv(timeseries_path, index=True)
    result.target_weights.to_csv(weights_path, index=True)
    result.positions.to_csv(positions_path, index=True)
    result.scores.to_csv(scores_path, index=True)
    result.rebalance_diagnostics.to_csv(rebalance_path, index=True)
    print(f"  saved summary: {metadata_path}")
    return {
        "summary_json": metadata_path,
        "timeseries_csv": timeseries_path,
        "weights_csv": weights_path,
        "positions_csv": positions_path,
        "scores_csv": scores_path,
        "rebalance_csv": rebalance_path,
    }


def _research_run_key(args: argparse.Namespace, symbols: list[str]) -> str:
    selection = getattr(args, "universe", None) or getattr(args, "preset", None) or ",".join(sorted(symbols))
    return "|".join(
        [
            "research",
            str(selection),
            str(getattr(args, "strategy", "")),
            str(getattr(args, "engine", "")),
            str(getattr(args, "start", None) or "full"),
            str(getattr(args, "end", None) or "full"),
        ]
    )


def _run_xsec_research(args: argparse.Namespace, symbols: list[str]) -> None:
    prepared_frames = {
        symbol: prepare_research_frame(symbol, start=args.start, end=args.end)
        for symbol in symbols
    }
    strategy_params = build_strategy_params(args)
    result = run_xsec_momentum_topn(
        prepared_frames=prepared_frames,
        lookback_bars=int(strategy_params["lookback_bars"] or 126),
        skip_bars=int(strategy_params["skip_bars"] or 0),
        top_n=int(strategy_params["top_n"] or 3),
        rebalance_bars=int(strategy_params["rebalance_bars"] or 21),
        commission=resolve_turnover_cost(args),
        cash=args.cash,
        max_position_weight=strategy_params["max_position_weight"],
        min_avg_dollar_volume=strategy_params["min_avg_dollar_volume"],
        max_names_per_sector=strategy_params["max_names_per_sector"],
        turnover_buffer_bps=float(strategy_params["turnover_buffer_bps"] or 0.0),
        max_turnover_per_rebalance=strategy_params["max_turnover_per_rebalance"],
        weighting_scheme=strategy_params["weighting_scheme"],
        vol_lookback_bars=int(strategy_params["vol_lookback_bars"] or 20),
        portfolio_construction_mode=strategy_params["portfolio_construction_mode"],
        benchmark_type=args.benchmark,
    )
    stats = result.summary
    exp_id = log_experiment(stats)

    effective_start = str(min(frame["effective_start"] for frame in prepared_frames.values()))
    effective_end = str(max(frame["effective_end"] for frame in prepared_frames.values()))
    rows_used = int(len(result.timeseries))

    if args.output_dir:
        return _save_xsec_run_artifacts(
            output_dir=Path(args.output_dir),
            strategy=args.strategy,
            engine=args.engine,
            symbols=symbols,
            effective_start=effective_start,
            effective_end=effective_end,
            rows_used=rows_used,
            stats=stats,
            experiment_id=exp_id,
            result=result,
        )

    print(
        f"[OK] universe: engine={args.engine}, strategy={args.strategy}, "
        f"symbols={len(symbols)} ({print_symbol_list(symbols)}), "
        f"range={effective_start}->{effective_end}, rows={rows_used}, "
        f"lookback_bars={strategy_params['lookback_bars']}, skip_bars={strategy_params['skip_bars']}, "
        f"top_n={strategy_params['top_n']}, rebalance_bars={strategy_params['rebalance_bars']}, "
        f"portfolio_construction_mode={strategy_params['portfolio_construction_mode']}, "
        f"weighting_scheme={strategy_params['weighting_scheme']}, vol_lookback_bars={strategy_params['vol_lookback_bars']}, "
        f"max_position_weight={strategy_params['max_position_weight']}, min_avg_dollar_volume={strategy_params['min_avg_dollar_volume']}, "
        f"max_names_per_sector={strategy_params['max_names_per_sector']}, turnover_buffer_bps={strategy_params['turnover_buffer_bps']}, "
        f"max_turnover_per_rebalance={strategy_params['max_turnover_per_rebalance']}, benchmark={stats.get('benchmark_type', 'n/a')}, "
        f"gross_return[%]={stats.get('gross_return_pct', 'n/a')}, net_return[%]={stats.get('net_return_pct', 'n/a')}, "
        f"cost_drag[%]={stats.get('cost_drag_return_pct', 'n/a')}, Sharpe={stats.get('Sharpe Ratio', 'n/a')}, "
        f"MaxDD[%]={stats.get('Max. Drawdown [%]', 'n/a')}, trade_count={stats.get('trade_count', 'n/a')}, "
        f"time_in_market[%]={stats.get('percent_time_in_market', 'n/a')}, "
        f"avg_holdings={stats.get('average_number_of_holdings', 'n/a')}, "
        f"avg_target_selected={stats.get('average_target_selected_count', 'n/a')}, "
        f"avg_realized_holdings={stats.get('average_realized_holdings_count', 'n/a')}, "
        f"holdings_to_top_n={stats.get('average_holdings_ratio_to_top_n', 'n/a')}, "
        f"exceeded_top_n={stats.get('realized_holdings_exceeded_top_n', False)}, "
        f"rebalance_count={stats.get('rebalance_count', 'n/a')}, "
        f"avg_turnover={stats.get('mean_turnover', 'n/a')}, annualized_turnover={stats.get('annualized_turnover', 'n/a')}, "
        f"mean_transaction_cost={stats.get('mean_transaction_cost', 'n/a')}, total_transaction_cost={stats.get('total_transaction_cost', 'n/a')}, "
        f"initial_equity={stats.get('initial_equity', 'n/a')}, final_equity={stats.get('final_equity', 'n/a')}, "
        f"avg_gross_exposure={stats.get('average_gross_exposure', 'n/a')}, "
        f"percent_invested={stats.get('percent_invested', 'n/a')}, "
        f"avg_available={stats.get('average_available_symbols', 'n/a')}, "
        f"avg_eligible={stats.get('average_eligible_symbols', 'n/a')}, "
        f"avg_selected={stats.get('average_selected_symbols', 'n/a')}, "
        f"liquidity_filter_active={stats.get('liquidity_filter_active', False)}, "
        f"sector_cap_active={stats.get('sector_cap_active', False)}, "
        f"liquidity_excluded={stats.get('total_liquidity_excluded_symbols', 0)}, "
        f"sector_cap_excluded={stats.get('total_sector_cap_excluded_symbols', 0)}, "
        f"turnover_cap_bindings={stats.get('turnover_cap_binding_count', 0)}, "
        f"buffer_blocked={stats.get('turnover_buffer_blocked_replacements', 0)}, "
        f"semantic_warning={stats.get('semantic_warning', '') or 'none'}, "
        f"empty_rebalances[%]={stats.get('percent_empty_rebalances', 'n/a')}, "
        f"cost_bps={getattr(args, 'cost_bps', None) if getattr(args, 'cost_bps', None) is not None else resolve_turnover_cost(args) * 10000.0}, "
        f"activity={activity_note(stats)}, Experiment={exp_id}"
    )
    return {}


def cmd_research(args: argparse.Namespace) -> None:
    loaded_config = load_and_apply_workflow_config(
        args,
        loader=load_research_run_workflow_config,
        preset_attr="preset",
    )
    apply_cli_preset(args)
    symbols = resolve_symbols(args)
    requested_range = f"requested_range={args.start or 'full'}->{args.end or 'full'}"
    print(
        f"Running research run for {len(symbols)} symbol(s): "
        f"{print_symbol_list(symbols)} | engine={args.engine} | {requested_range}"
    )
    db_service = DatabaseLineageService.from_config(
        enable_database_metadata=getattr(loaded_config, "enable_database_metadata", None) if loaded_config is not None else None,
        database_url=getattr(loaded_config, "database_url", None) if loaded_config is not None else None,
        database_schema=getattr(loaded_config, "database_schema", None) if loaded_config is not None else None,
    )
    research_run_id = db_service.create_research_run(
        run_key=_research_run_key(args, symbols),
        run_type="research",
        config_payload=loaded_config or vars(args),
        notes=f"strategy={args.strategy}",
    )
    strategy_definition_id = db_service.upsert_strategy_definition(
        name=str(args.strategy),
        version=str(args.engine),
        config_payload=loaded_config or vars(args),
        code_hash=None,
        is_active=True,
    )

    try:
        if args.strategy == "xsec_momentum_topn":
            artifact_paths = _run_xsec_research(args, symbols)
            register_artifact_bundle(
                db_service=db_service,
                artifact_paths=artifact_paths,
                artifact_type_prefix="research",
                research_run_id=research_run_id,
            )
            db_service.complete_research_run(research_run_id, notes=f"strategy_definition_id={strategy_definition_id}")
            return

        for symbol in symbols:
            prepared = prepare_research_frame(symbol, start=args.start, end=args.end)
            df = prepared["df"]
            result = None
            strategy_params = build_strategy_params(args)

            if args.engine == "legacy":
                stats = run_backtest_on_df(
                    df=df,
                    symbol=symbol,
                    strategy=args.strategy,
                    **strategy_params,
                    cash=args.cash,
                    commission=args.commission,
                )
            elif args.engine == "vectorized":
                execution_policy = ExecutionPolicy(rebalance_frequency=args.rebalance_frequency)
                result = run_vectorized_research_on_df(
                    df=df,
                    symbol=symbol,
                    strategy=args.strategy,
                    **strategy_params,
                    cost_per_turnover=args.commission,
                    initial_equity=args.cash,
                    execution_policy=execution_policy,
                )
                stats = to_legacy_stats(
                    result,
                    symbol=symbol,
                    strategy=args.strategy,
                    **strategy_params,
                    cash=args.cash,
                    commission=args.commission,
                )
            else:
                raise SystemExit(f"Unsupported engine: {args.engine}")

            exp_id = log_experiment(stats)
            artifact_paths: dict[str, Path] = {}
            if args.output_dir:
                artifact_paths = _save_run_artifacts(
                    output_dir=Path(args.output_dir),
                    symbol=symbol,
                    strategy=args.strategy,
                    engine=args.engine,
                    feature_path=prepared["path"],
                    effective_start=prepared["effective_start"],
                    effective_end=prepared["effective_end"],
                    rows_used=prepared["rows"],
                    stats=stats,
                    experiment_id=exp_id,
                    result=result,
                )
                register_artifact_bundle(
                    db_service=db_service,
                    artifact_paths=artifact_paths,
                    artifact_type_prefix=f"research:{symbol}",
                    research_run_id=research_run_id,
                )

            print(
                f"[OK] {symbol}: engine={args.engine}, strategy={args.strategy}, "
                f"range={prepared['effective_start']}->{prepared['effective_end']}, "
                f"rows={prepared['rows']}, feature_path={prepared['path']}, "
                f"fast={strategy_params['fast']}, slow={strategy_params['slow']}, lookback={strategy_params['lookback']}, "
                f"entry_lookback={strategy_params['entry_lookback']}, exit_lookback={strategy_params['exit_lookback']}, "
                f"momentum_lookback={strategy_params['momentum_lookback']}, "
                f"Return[%]={stats.get('Return [%]', 'n/a')}, "
                f"Sharpe={stats.get('Sharpe Ratio', 'n/a')}, "
                f"MaxDD[%]={stats.get('Max. Drawdown [%]', 'n/a')}, "
                f"trade_count={stats.get('trade_count', 'n/a')}, "
                f"time_in_market[%]={stats.get('percent_time_in_market', 'n/a')}, "
                f"activity={activity_note(stats)}, "
                f"Experiment={exp_id}"
            )
        db_service.complete_research_run(research_run_id, notes=f"strategy_definition_id={strategy_definition_id}")
    except Exception as exc:
        db_service.fail_research_run(research_run_id, notes=repr(exc))
        raise
