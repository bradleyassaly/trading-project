from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from trading_platform.backtests.engine import run_backtest_on_df
from trading_platform.cli.common import build_strategy_params, prepare_research_frame, print_symbol_list, resolve_symbols
from trading_platform.execution.policies import ExecutionPolicy
from trading_platform.experiments.tracker import log_experiment
from trading_platform.research.diagnostics import activity_note
from trading_platform.research.service import run_vectorized_research_on_df, to_legacy_stats
from trading_platform.research.xsec_momentum import run_xsec_momentum_topn


def _build_param_sets(args: argparse.Namespace) -> tuple[list[dict[str, int | None]], list[str]]:
    warnings: list[str] = []
    if args.strategy == "sma_cross":
        if not args.fast_values or not args.slow_values:
            raise SystemExit("sma_cross sweep requires --fast-values and --slow-values")
        param_sets: list[dict[str, int | None]] = []
        for fast in args.fast_values:
            for slow in args.slow_values:
                if fast >= slow:
                    warnings.append(f"Skipping invalid combination fast={fast}, slow={slow}")
                    continue
                param_sets.append({"fast": fast, "slow": slow, "lookback": None})
        if not param_sets:
            raise SystemExit("No valid sweep parameter combinations remain after filtering fast >= slow")
        return param_sets, warnings

    if args.strategy == "momentum_hold":
        if not args.lookback_values:
            raise SystemExit("momentum_hold sweep requires --lookback-values")
        return [{"fast": None, "slow": None, "lookback": lookback} for lookback in args.lookback_values], warnings

    if args.strategy == "breakout_hold":
        if not args.entry_lookback_values or not args.exit_lookback_values:
            raise SystemExit("breakout_hold sweep requires --entry-lookback-values and --exit-lookback-values")
        momentum_values = args.momentum_lookback_values or [None]
        return [
            {
                "fast": None,
                "slow": None,
                "lookback": None,
                "entry_lookback": entry_lookback,
                "exit_lookback": exit_lookback,
                "momentum_lookback": momentum_lookback,
            }
            for entry_lookback in args.entry_lookback_values
            for exit_lookback in args.exit_lookback_values
            for momentum_lookback in momentum_values
            if entry_lookback > 0 and exit_lookback > 0
        ], warnings

    if args.strategy == "xsec_momentum_topn":
        if not args.lookback_bars_values or not args.top_n_values or not args.rebalance_bars_values:
            raise SystemExit(
                "xsec_momentum_topn sweep requires --lookback-bars-values, --top-n-values, and --rebalance-bars-values"
            )
        skip_values = args.skip_bars_values or [0]
        return [
            {
                "lookback_bars": lookback_bars,
                "skip_bars": skip_bars,
                "top_n": top_n,
                "rebalance_bars": rebalance_bars,
            }
            for lookback_bars in args.lookback_bars_values
            for skip_bars in skip_values
            for top_n in args.top_n_values
            for rebalance_bars in args.rebalance_bars_values
            if lookback_bars > 0 and skip_bars >= 0 and top_n > 0 and rebalance_bars > 0
        ], warnings

    raise SystemExit(f"Unsupported sweep strategy: {args.strategy}")


def cmd_sweep(args: argparse.Namespace) -> None:
    symbols = resolve_symbols(args)
    param_sets, grid_warnings = _build_param_sets(args)
    results: list[dict[str, object]] = []

    print(
        f"Running research sweep: symbols={len(symbols)} ({print_symbol_list(symbols)}), "
        f"param_combinations={len(param_sets)}, engine={args.engine}, "
        f"requested_range={args.start or 'full'}->{args.end or 'full'}"
    )
    for warning in grid_warnings:
        print(f"[SKIP] {warning}")

    if args.strategy == "xsec_momentum_topn":
        prepared_frames = {
            symbol: prepare_research_frame(symbol, start=args.start, end=args.end)
            for symbol in symbols
        }
        effective_start = str(min(frame["effective_start"] for frame in prepared_frames.values()))
        effective_end = str(max(frame["effective_end"] for frame in prepared_frames.values()))
        for params in param_sets:
            try:
                result = run_xsec_momentum_topn(
                    prepared_frames=prepared_frames,
                    lookback_bars=int(params["lookback_bars"] or 126),
                    skip_bars=int(params["skip_bars"] or 0),
                    top_n=int(params["top_n"] or 1),
                    rebalance_bars=int(params["rebalance_bars"] or 21),
                    commission=args.commission,
                    cash=args.cash,
                )
                stats = result.summary
                exp_id = log_experiment(stats)
                row = {
                    "symbol": "UNIVERSE",
                    "symbols": ",".join(symbols),
                    "symbol_count": len(symbols),
                    "strategy": args.strategy,
                    "engine": args.engine,
                    "start_date": effective_start,
                    "end_date": effective_end,
                    "fast": None,
                    "slow": None,
                    "lookback": None,
                    "lookback_bars": params.get("lookback_bars"),
                    "skip_bars": params.get("skip_bars"),
                    "top_n": params.get("top_n"),
                    "rebalance_bars": params.get("rebalance_bars"),
                    "entry_lookback": None,
                    "exit_lookback": None,
                    "momentum_lookback": None,
                    "return_pct": stats.get("Return [%]"),
                    "sharpe": stats.get("Sharpe Ratio"),
                    "max_drawdown_pct": stats.get("Max. Drawdown [%]"),
                    "trade_count": stats.get("trade_count"),
                    "entry_count": stats.get("entry_count"),
                    "exit_count": stats.get("exit_count"),
                    "percent_time_in_market": stats.get("percent_time_in_market"),
                    "average_holding_period_bars": stats.get("average_holding_period_bars"),
                    "final_position_size": stats.get("final_position_size"),
                    "ended_in_cash": stats.get("ended_in_cash"),
                    "average_number_of_holdings": stats.get("average_number_of_holdings"),
                    "rebalance_count": stats.get("rebalance_count"),
                    "mean_turnover": stats.get("mean_turnover"),
                    "percent_invested": stats.get("percent_invested"),
                    "initial_equity": stats.get("initial_equity"),
                    "final_equity": stats.get("final_equity"),
                    "average_gross_exposure": stats.get("average_gross_exposure"),
                    "experiment_id": exp_id,
                    "status": "ok",
                    "warning_count": 0,
                    "notes": "",
                }
                results.append(row)
                print(
                    f"[OK] universe: symbols={len(symbols)} ({print_symbol_list(symbols)}), "
                    f"range={effective_start}->{effective_end}, lookback_bars={row['lookback_bars']}, "
                    f"skip_bars={row['skip_bars']}, top_n={row['top_n']}, rebalance_bars={row['rebalance_bars']}, "
                    f"Return[%]={row['return_pct']}, Sharpe={row['sharpe']}, MaxDD[%]={row['max_drawdown_pct']}, "
                    f"trade_count={row['trade_count']}, percent_invested={row['percent_invested']}, "
                    f"avg_holdings={row['average_number_of_holdings']}, initial_equity={row['initial_equity']}, "
                    f"final_equity={row['final_equity']}, avg_gross_exposure={row['average_gross_exposure']}, "
                    f"activity={activity_note(row)}, Experiment={exp_id}"
                )
            except Exception as exc:
                results.append(
                    {
                        "symbol": "UNIVERSE",
                        "symbols": ",".join(symbols),
                        "symbol_count": len(symbols),
                        "strategy": args.strategy,
                        "engine": args.engine,
                        "start_date": effective_start,
                        "end_date": effective_end,
                        "fast": None,
                        "slow": None,
                        "lookback": None,
                        "lookback_bars": params.get("lookback_bars"),
                        "skip_bars": params.get("skip_bars"),
                        "top_n": params.get("top_n"),
                        "rebalance_bars": params.get("rebalance_bars"),
                        "entry_lookback": None,
                        "exit_lookback": None,
                        "momentum_lookback": None,
                        "return_pct": None,
                        "sharpe": None,
                        "max_drawdown_pct": None,
                        "trade_count": None,
                        "entry_count": None,
                        "exit_count": None,
                        "percent_time_in_market": None,
                        "average_holding_period_bars": None,
                        "final_position_size": None,
                        "ended_in_cash": None,
                        "average_number_of_holdings": None,
                        "rebalance_count": None,
                        "mean_turnover": None,
                        "percent_invested": None,
                        "initial_equity": None,
                        "final_equity": None,
                        "average_gross_exposure": None,
                        "experiment_id": None,
                        "status": "error",
                        "warning_count": 1,
                        "notes": f"{type(exc).__name__}: {exc}",
                    }
                )
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(results).to_csv(output_path, index=False)
        print(f"\nSaved sweep results to {output_path}")
        return

    for symbol in symbols:
        prepared = prepare_research_frame(symbol, start=args.start, end=args.end)
        df = prepared["df"]

        for params in param_sets:
            notes: list[str] = []
            warning_count = 0
            try:
                if args.engine == "legacy":
                    stats = run_backtest_on_df(
                        df=df,
                        symbol=symbol,
                        strategy=args.strategy,
                        **(build_strategy_params(args) | params),
                        cash=args.cash,
                        commission=args.commission,
                    )
                elif args.engine == "vectorized":
                    result = run_vectorized_research_on_df(
                        df=df,
                        symbol=symbol,
                        strategy=args.strategy,
                        **(build_strategy_params(args) | params),
                        cost_per_turnover=args.commission,
                        initial_equity=args.cash,
                        execution_policy=ExecutionPolicy(rebalance_frequency=args.rebalance_frequency),
                    )
                    stats = to_legacy_stats(
                        result,
                        symbol=symbol,
                        strategy=args.strategy,
                        fast=params.get("fast"),
                        slow=params.get("slow"),
                        lookback=params.get("lookback"),
                        entry_lookback=params.get("entry_lookback"),
                        exit_lookback=params.get("exit_lookback"),
                        momentum_lookback=params.get("momentum_lookback"),
                        cash=args.cash,
                        commission=args.commission,
                    )
                else:
                    raise SystemExit(f"Unsupported engine: {args.engine}")

                exp_id = log_experiment(stats)
                row = {
                    "symbol": symbol,
                    "strategy": args.strategy,
                    "engine": args.engine,
                    "start_date": prepared["effective_start"],
                    "end_date": prepared["effective_end"],
                    "fast": params.get("fast"),
                    "slow": params.get("slow"),
                    "lookback": params.get("lookback"),
                    "entry_lookback": params.get("entry_lookback"),
                    "exit_lookback": params.get("exit_lookback"),
                    "momentum_lookback": params.get("momentum_lookback"),
                    "return_pct": stats.get("Return [%]"),
                    "sharpe": stats.get("Sharpe Ratio"),
                    "max_drawdown_pct": stats.get("Max. Drawdown [%]"),
                    "trade_count": stats.get("trade_count"),
                    "entry_count": stats.get("entry_count"),
                    "exit_count": stats.get("exit_count"),
                    "percent_time_in_market": stats.get("percent_time_in_market"),
                    "average_holding_period_bars": stats.get("average_holding_period_bars"),
                    "final_position_size": stats.get("final_position_size"),
                    "ended_in_cash": stats.get("ended_in_cash"),
                    "experiment_id": exp_id,
                    "status": "ok",
                    "warning_count": warning_count,
                    "notes": "; ".join(notes),
                }
                results.append(row)
                print(
                    f"[OK] {symbol}: range={row['start_date']}->{row['end_date']}, "
                    f"fast={row['fast']}, slow={row['slow']}, lookback={row['lookback']}, "
                    f"entry_lookback={row['entry_lookback']}, exit_lookback={row['exit_lookback']}, "
                    f"momentum_lookback={row['momentum_lookback']}, "
                    f"Return[%]={row['return_pct']}, Sharpe={row['sharpe']}, "
                    f"MaxDD[%]={row['max_drawdown_pct']}, trade_count={row['trade_count']}, "
                    f"time_in_market[%]={row['percent_time_in_market']}, activity={activity_note(row)}, "
                    f"Experiment={exp_id}"
                )
            except Exception as exc:
                results.append(
                    {
                        "symbol": symbol,
                        "strategy": args.strategy,
                        "engine": args.engine,
                        "start_date": prepared["effective_start"],
                        "end_date": prepared["effective_end"],
                        "fast": params.get("fast"),
                        "slow": params.get("slow"),
                        "lookback": params.get("lookback"),
                        "entry_lookback": params.get("entry_lookback"),
                        "exit_lookback": params.get("exit_lookback"),
                        "momentum_lookback": params.get("momentum_lookback"),
                        "return_pct": None,
                        "sharpe": None,
                        "max_drawdown_pct": None,
                        "trade_count": None,
                        "entry_count": None,
                        "exit_count": None,
                        "percent_time_in_market": None,
                        "average_holding_period_bars": None,
                        "final_position_size": None,
                        "ended_in_cash": None,
                        "experiment_id": None,
                        "status": "error",
                        "warning_count": 1,
                        "notes": f"{type(exc).__name__}: {exc}",
                    }
                )
                print(f"[ERROR] {symbol}: params={params} -> {exc}")

    if not results:
        print("No successful sweep results.")
        return

    df = pd.DataFrame(results)
    successful_df = df[df["status"] == "ok"].copy()
    if successful_df.empty:
        print("No successful sweep results.")
    else:
        successful_df = successful_df.sort_values(by=["sharpe", "return_pct"], ascending=False, na_position="last")
        print("\nTop 10 results:")
        print(successful_df.head(10).to_string(index=False))

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"\nSaved sweep results to {output_path}")
