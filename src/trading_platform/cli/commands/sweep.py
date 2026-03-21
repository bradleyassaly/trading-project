from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from trading_platform.backtests.engine import run_backtest_on_df
from trading_platform.cli.common import build_strategy_params, prepare_research_frame, print_symbol_list, resolve_symbols, resolve_turnover_cost
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
        strategy_params = build_strategy_params(args)
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
                row = {
                    "symbol": "UNIVERSE",
                    "symbols": ",".join(symbols),
                    "symbol_count": len(symbols),
                    "strategy": args.strategy,
                    "engine": args.engine,
                    "benchmark_type": stats.get("benchmark_type"),
                    "start_date": effective_start,
                    "end_date": effective_end,
                    "fast": None,
                    "slow": None,
                    "lookback": None,
                    "lookback_bars": params.get("lookback_bars"),
                    "skip_bars": params.get("skip_bars"),
                    "top_n": params.get("top_n"),
                    "rebalance_bars": params.get("rebalance_bars"),
                    "portfolio_construction_mode": stats.get("portfolio_construction_mode"),
                    "weighting_scheme": stats.get("weighting_scheme"),
                    "vol_lookback_bars": stats.get("vol_lookback_bars"),
                    "max_position_weight": stats.get("max_position_weight"),
                    "min_avg_dollar_volume": stats.get("min_avg_dollar_volume"),
                    "max_names_per_sector": stats.get("max_names_per_sector"),
                    "turnover_buffer_bps": stats.get("turnover_buffer_bps"),
                    "turnover_buffer_score_gap": stats.get("turnover_buffer_score_gap"),
                    "max_turnover_per_rebalance": stats.get("max_turnover_per_rebalance"),
                    "entry_lookback": None,
                    "exit_lookback": None,
                    "momentum_lookback": None,
                    "return_pct": stats.get("Return [%]"),
                    "gross_return_pct": stats.get("gross_return_pct"),
                    "net_return_pct": stats.get("net_return_pct"),
                    "cost_drag_return_pct": stats.get("cost_drag_return_pct"),
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
                    "average_target_selected_count": stats.get("average_target_selected_count"),
                    "average_realized_holdings_count": stats.get("average_realized_holdings_count"),
                    "average_realized_holdings_minus_top_n": stats.get("average_realized_holdings_minus_top_n"),
                    "average_holdings_ratio_to_top_n": stats.get("average_holdings_ratio_to_top_n"),
                    "realized_holdings_exceeded_top_n": stats.get("realized_holdings_exceeded_top_n"),
                    "semantic_warning": stats.get("semantic_warning"),
                    "average_available_symbols": stats.get("average_available_symbols"),
                    "average_eligible_symbols": stats.get("average_eligible_symbols"),
                    "average_selected_symbols": stats.get("average_selected_symbols"),
                    "percent_empty_rebalances": stats.get("percent_empty_rebalances"),
                    "liquidity_filter_active": stats.get("liquidity_filter_active"),
                    "sector_cap_active": stats.get("sector_cap_active"),
                    "sector_warning": stats.get("sector_warning"),
                    "average_liquidity_excluded_symbols": stats.get("average_liquidity_excluded_symbols"),
                    "total_liquidity_excluded_symbols": stats.get("total_liquidity_excluded_symbols"),
                    "average_sector_cap_excluded_symbols": stats.get("average_sector_cap_excluded_symbols"),
                    "total_sector_cap_excluded_symbols": stats.get("total_sector_cap_excluded_symbols"),
                    "turnover_cap_binding_count": stats.get("turnover_cap_binding_count"),
                    "turnover_buffer_blocked_replacements": stats.get("turnover_buffer_blocked_replacements"),
                    "rebalance_count": stats.get("rebalance_count"),
                    "mean_turnover": stats.get("mean_turnover"),
                    "annualized_turnover": stats.get("annualized_turnover"),
                    "mean_transaction_cost": stats.get("mean_transaction_cost"),
                    "total_transaction_cost": stats.get("total_transaction_cost"),
                    "estimated_cost_drag_bps": stats.get("estimated_cost_drag_bps"),
                    "percent_invested": stats.get("percent_invested"),
                    "initial_equity": stats.get("initial_equity"),
                    "final_equity": stats.get("final_equity"),
                    "average_gross_exposure": stats.get("average_gross_exposure"),
                    "earliest_data_date_by_symbol": stats.get("earliest_data_date_by_symbol"),
                    "cost_bps": getattr(args, "cost_bps", None) if getattr(args, "cost_bps", None) is not None else resolve_turnover_cost(args) * 10000.0,
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
                    f"portfolio_construction_mode={row['portfolio_construction_mode']}, "
                    f"weighting_scheme={row['weighting_scheme']}, max_position_weight={row['max_position_weight']}, "
                    f"min_avg_dollar_volume={row['min_avg_dollar_volume']}, max_names_per_sector={row['max_names_per_sector']}, "
                    f"turnover_buffer_bps={row['turnover_buffer_bps']}, max_turnover_per_rebalance={row['max_turnover_per_rebalance']}, benchmark={row['benchmark_type']}, "
                    f"gross_return[%]={row['gross_return_pct']}, net_return[%]={row['net_return_pct']}, cost_drag[%]={row['cost_drag_return_pct']}, "
                    f"Sharpe={row['sharpe']}, MaxDD[%]={row['max_drawdown_pct']}, "
                    f"trade_count={row['trade_count']}, percent_invested={row['percent_invested']}, "
                    f"avg_holdings={row['average_number_of_holdings']}, avg_target_selected={row['average_target_selected_count']}, "
                    f"avg_realized_holdings={row['average_realized_holdings_count']}, holdings_to_top_n={row['average_holdings_ratio_to_top_n']}, "
                    f"exceeded_top_n={row['realized_holdings_exceeded_top_n']}, avg_available={row['average_available_symbols']}, avg_eligible={row['average_eligible_symbols']}, "
                    f"avg_selected={row['average_selected_symbols']}, empty_rebalances[%]={row['percent_empty_rebalances']}, "
                    f"liquidity_excluded={row['total_liquidity_excluded_symbols']}, sector_cap_excluded={row['total_sector_cap_excluded_symbols']}, "
                    f"turnover_cap_bindings={row['turnover_cap_binding_count']}, buffer_blocked={row['turnover_buffer_blocked_replacements']}, "
                    f"semantic_warning={row['semantic_warning'] or 'none'}, "
                    f"avg_turnover={row['mean_turnover']}, annualized_turnover={row['annualized_turnover']}, "
                    f"mean_transaction_cost={row['mean_transaction_cost']}, total_transaction_cost={row['total_transaction_cost']}, cost_bps={row['cost_bps']}, "
                    f"initial_equity={row['initial_equity']}, "
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
                        "benchmark_type": args.benchmark,
                        "start_date": effective_start,
                        "end_date": effective_end,
                        "fast": None,
                        "slow": None,
                        "lookback": None,
                        "lookback_bars": params.get("lookback_bars"),
                        "skip_bars": params.get("skip_bars"),
                        "top_n": params.get("top_n"),
                        "rebalance_bars": params.get("rebalance_bars"),
                        "portfolio_construction_mode": strategy_params["portfolio_construction_mode"],
                        "weighting_scheme": strategy_params["weighting_scheme"],
                        "vol_lookback_bars": strategy_params["vol_lookback_bars"],
                        "max_position_weight": strategy_params["max_position_weight"],
                        "min_avg_dollar_volume": strategy_params["min_avg_dollar_volume"],
                        "max_names_per_sector": strategy_params["max_names_per_sector"],
                        "turnover_buffer_bps": strategy_params["turnover_buffer_bps"],
                        "turnover_buffer_score_gap": float(strategy_params["turnover_buffer_bps"] or 0.0) / 10_000.0,
                        "max_turnover_per_rebalance": strategy_params["max_turnover_per_rebalance"],
                        "entry_lookback": None,
                        "exit_lookback": None,
                        "momentum_lookback": None,
                        "return_pct": None,
                        "gross_return_pct": None,
                        "net_return_pct": None,
                        "cost_drag_return_pct": None,
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
                        "average_target_selected_count": None,
                        "average_realized_holdings_count": None,
                        "average_realized_holdings_minus_top_n": None,
                        "average_holdings_ratio_to_top_n": None,
                        "realized_holdings_exceeded_top_n": None,
                        "semantic_warning": None,
                        "average_available_symbols": None,
                        "average_eligible_symbols": None,
                        "average_selected_symbols": None,
                        "percent_empty_rebalances": None,
                        "liquidity_filter_active": bool(strategy_params["min_avg_dollar_volume"] is not None),
                        "sector_cap_active": False,
                        "sector_warning": None,
                        "average_liquidity_excluded_symbols": None,
                        "total_liquidity_excluded_symbols": None,
                        "average_sector_cap_excluded_symbols": None,
                        "total_sector_cap_excluded_symbols": None,
                        "turnover_cap_binding_count": None,
                        "turnover_buffer_blocked_replacements": None,
                        "rebalance_count": None,
                        "mean_turnover": None,
                        "annualized_turnover": None,
                        "mean_transaction_cost": None,
                        "total_transaction_cost": None,
                        "estimated_cost_drag_bps": None,
                        "percent_invested": None,
                        "initial_equity": None,
                        "final_equity": None,
                        "average_gross_exposure": None,
                        "earliest_data_date_by_symbol": None,
                        "cost_bps": getattr(args, "cost_bps", None) if getattr(args, "cost_bps", None) is not None else resolve_turnover_cost(args) * 10000.0,
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
