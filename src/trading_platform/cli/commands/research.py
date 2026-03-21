from __future__ import annotations

import argparse
import json
from pathlib import Path

from trading_platform.backtests.engine import run_backtest_on_df
from trading_platform.cli.common import (
    build_strategy_params,
    prepare_research_frame,
    print_symbol_list,
    resolve_symbols,
)
from trading_platform.execution.policies import ExecutionPolicy
from trading_platform.experiments.tracker import log_experiment
from trading_platform.research.diagnostics import activity_note
from trading_platform.research.service import (
    run_vectorized_research_on_df,
    to_legacy_stats,
)
from trading_platform.research.xsec_momentum import run_xsec_momentum_topn


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
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    metadata_path = output_dir / f"{symbol}_{strategy}_{engine}_run_summary.json"
    payload = {
        "symbol": symbol,
        "strategy": strategy,
        "engine": engine,
        "effective_start_date": effective_start,
        "effective_end_date": effective_end,
        "rows_used": rows_used,
        "feature_path": str(feature_path),
        "experiment_id": experiment_id,
        "stats": stats,
    }
    metadata_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    print(f"  saved summary: {metadata_path}")

    if result is None:
        return

    timeseries_path = output_dir / f"{symbol}_{strategy}_timeseries.csv"
    signal_path = output_dir / f"{symbol}_{strategy}_signals.csv"
    result.simulation.timeseries.to_csv(timeseries_path, index=True)
    result.signal_frame.to_csv(signal_path, index=True)
    print(f"  saved timeseries: {timeseries_path}")
    print(f"  saved signals: {signal_path}")


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
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    metadata_path = output_dir / f"{strategy}_{engine}_universe_run_summary.json"
    payload = {
        "symbols": symbols,
        "strategy": strategy,
        "engine": engine,
        "effective_start_date": effective_start,
        "effective_end_date": effective_end,
        "rows_used": rows_used,
        "experiment_id": experiment_id,
        "stats": stats,
    }
    metadata_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    result.timeseries.to_csv(output_dir / f"{strategy}_portfolio_timeseries.csv", index=True)
    result.target_weights.to_csv(output_dir / f"{strategy}_portfolio_weights.csv", index=True)
    result.positions.to_csv(output_dir / f"{strategy}_portfolio_positions.csv", index=True)
    result.scores.to_csv(output_dir / f"{strategy}_scores.csv", index=True)
    print(f"  saved summary: {metadata_path}")


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
        commission=args.commission,
        cash=args.cash,
    )
    stats = result.summary
    exp_id = log_experiment(stats)

    effective_start = str(min(frame["effective_start"] for frame in prepared_frames.values()))
    effective_end = str(max(frame["effective_end"] for frame in prepared_frames.values()))
    rows_used = int(len(result.timeseries))

    if args.output_dir:
        _save_xsec_run_artifacts(
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
        f"Return[%]={stats.get('Return [%]', 'n/a')}, Sharpe={stats.get('Sharpe Ratio', 'n/a')}, "
        f"MaxDD[%]={stats.get('Max. Drawdown [%]', 'n/a')}, trade_count={stats.get('trade_count', 'n/a')}, "
        f"time_in_market[%]={stats.get('percent_time_in_market', 'n/a')}, "
        f"avg_holdings={stats.get('average_number_of_holdings', 'n/a')}, "
        f"rebalance_count={stats.get('rebalance_count', 'n/a')}, "
        f"initial_equity={stats.get('initial_equity', 'n/a')}, final_equity={stats.get('final_equity', 'n/a')}, "
        f"avg_gross_exposure={stats.get('average_gross_exposure', 'n/a')}, "
        f"percent_invested={stats.get('percent_invested', 'n/a')}, "
        f"activity={activity_note(stats)}, Experiment={exp_id}"
    )


def cmd_research(args: argparse.Namespace) -> None:
    symbols = resolve_symbols(args)
    requested_range = f"requested_range={args.start or 'full'}->{args.end or 'full'}"
    print(
        f"Running research run for {len(symbols)} symbol(s): "
        f"{print_symbol_list(symbols)} | engine={args.engine} | {requested_range}"
    )

    if args.strategy == "xsec_momentum_topn":
        _run_xsec_research(args, symbols)
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
            execution_policy = ExecutionPolicy(
                rebalance_frequency=args.rebalance_frequency,
            )
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
        if args.output_dir:
            _save_run_artifacts(
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
