from __future__ import annotations

import argparse
import json
from itertools import product
from pathlib import Path

import pandas as pd

from trading_platform.artifact_schemas import WorkflowArtifactSummary
from trading_platform.cli.config_support import apply_workflow_config, option_is_explicit
from trading_platform.backtests.engine import run_backtest_on_df
from trading_platform.cli.common import (
    build_strategy_params,
    compound_return_pct,
    compute_buy_and_hold_return_pct,
    prepare_research_frame,
    print_symbol_list,
    resolve_symbols,
    resolve_turnover_cost,
)
from trading_platform.cli.presets import apply_cli_preset
from trading_platform.config.loader import load_walkforward_workflow_config
from trading_platform.experiments.reporting import (
    save_walkforward_html_report,
    save_walkforward_param_plot,
    save_walkforward_return_plot,
)
from trading_platform.research.diagnostics import activity_note
from trading_platform.research.service import (
    run_vectorized_research_on_df,
    to_legacy_stats,
)
from trading_platform.research.xsec_momentum import build_close_panel, run_xsec_momentum_topn


def _build_param_grid(args: argparse.Namespace) -> tuple[list[dict[str, int | None]], list[str]]:
    if args.strategy == "sma_cross":
        if not args.fast_values or not args.slow_values:
            raise SystemExit(
                "walkforward with sma_cross requires --fast-values and --slow-values"
            )
        invalid: list[str] = []
        param_grid = []
        for fast, slow in product(args.fast_values, args.slow_values):
            if fast >= slow:
                invalid.append(f"Skipping invalid combination fast={fast}, slow={slow}")
                continue
            param_grid.append({"fast": fast, "slow": slow, "lookback": None})
        if not param_grid:
            raise SystemExit("No valid walk-forward parameter combinations remain after filtering fast >= slow")
        return param_grid, invalid

    if args.strategy == "momentum_hold":
        if not args.lookback_values:
            raise SystemExit(
                "walkforward with momentum_hold requires --lookback-values"
            )
        return [{"fast": None, "slow": None, "lookback": lb} for lb in args.lookback_values], []

    if args.strategy == "breakout_hold":
        if not args.entry_lookback_values or not args.exit_lookback_values:
            raise SystemExit(
                "walkforward with breakout_hold requires --entry-lookback-values and --exit-lookback-values"
            )
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
            for entry_lookback, exit_lookback, momentum_lookback in product(
                args.entry_lookback_values,
                args.exit_lookback_values,
                momentum_values,
            )
            if entry_lookback > 0 and exit_lookback > 0
        ], []

    if args.strategy == "xsec_momentum_topn":
        if not args.lookback_bars_values or not args.top_n_values or not args.rebalance_bars_values:
            raise SystemExit(
                "walkforward with xsec_momentum_topn requires --lookback-bars-values, --top-n-values, and --rebalance-bars-values"
            )
        skip_values = args.skip_bars_values or [0]
        return [
            {
                "lookback_bars": lookback_bars,
                "skip_bars": skip_bars,
                "top_n": top_n,
                "rebalance_bars": rebalance_bars,
            }
            for lookback_bars, skip_bars, top_n, rebalance_bars in product(
                args.lookback_bars_values,
                skip_values,
                args.top_n_values,
                args.rebalance_bars_values,
            )
            if lookback_bars > 0 and skip_bars >= 0 and top_n > 0 and rebalance_bars > 0
        ], []

    raise SystemExit(f"Unsupported strategy for walkforward: {args.strategy}")


TRADING_BARS_PER_YEAR = 252


def _write_workflow_summary_json(
    *,
    output_path: Path,
    args: argparse.Namespace,
    symbols: list[str],
    out_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    extra_paths: dict[str, str],
) -> Path:
    summary = WorkflowArtifactSummary(
        summary_type="walkforward",
        workflow_stage="walkforward",
        timestamp=str(pd.Timestamp.now(tz="UTC").isoformat()),
        status="succeeded",
        name=output_path.stem,
        strategy=args.strategy,
        universe=args.universe if getattr(args, "universe", None) else ",".join(symbols),
        preset_name=getattr(args, "_resolved_preset", getattr(args, "preset", None)),
        key_counts={
            "window_count": int(len(out_df)),
            "completed_window_count": int((out_df["window_status"] == "completed").sum()) if "window_status" in out_df.columns else 0,
            "summary_row_count": int(len(summary_df)),
        },
        key_metrics={
            "avg_test_return_pct": float(summary_df["avg_test_return_pct"].mean()) if "avg_test_return_pct" in summary_df.columns and not summary_df.empty else None,
            "avg_excess_return_pct": float(summary_df["avg_excess_return_pct"].mean()) if "avg_excess_return_pct" in summary_df.columns and not summary_df.empty else None,
        },
        details={
            "engine": args.engine,
            "symbols": symbols,
            "select_by": args.select_by,
            "train_bars": getattr(args, "train_bars", None),
            "test_bars": getattr(args, "test_bars", None),
            "step_bars": getattr(args, "step_bars", None),
        },
        artifact_paths=extra_paths,
    )
    summary_json_path = output_path.with_name(output_path.stem + "_workflow_summary.json")
    summary_json_path.write_text(json.dumps(summary.to_dict(), indent=2), encoding="utf-8")
    return summary_json_path


def _resolve_window_spec(args: argparse.Namespace) -> dict[str, object]:
    train_bars = getattr(args, "train_bars", None)
    test_bars = getattr(args, "test_bars", None)
    step_bars = getattr(args, "step_bars", None)

    compat_train_days = getattr(args, "train_period_days", None)
    compat_test_days = getattr(args, "test_period_days", None)
    compat_step_days = getattr(args, "step_days", None)

    aliases_used: list[str] = []
    derived_from_years: list[str] = []

    if train_bars is None:
        if compat_train_days is not None:
            train_bars = compat_train_days
            aliases_used.append("train_period_days->train_bars")
        else:
            train_bars = int(args.train_years) * TRADING_BARS_PER_YEAR
            derived_from_years.append("train_years")

    if test_bars is None:
        if compat_test_days is not None:
            test_bars = compat_test_days
            aliases_used.append("test_period_days->test_bars")
        else:
            test_bars = int(args.test_years) * TRADING_BARS_PER_YEAR
            derived_from_years.append("test_years")

    if step_bars is None:
        if compat_step_days is not None:
            step_bars = compat_step_days
            aliases_used.append("step_days->step_bars")
        else:
            step_bars = test_bars
            if compat_test_days is not None:
                aliases_used.append("test_period_days->step_bars(default)")
            elif getattr(args, "step_bars", None) is None:
                derived_from_years.append("step_bars_defaulted_to_test_bars")

    for name, value in (
        ("train_bars", train_bars),
        ("test_bars", test_bars),
        ("step_bars", step_bars),
    ):
        if value is None or int(value) <= 0:
            raise SystemExit(f"{name} must be a positive integer")

    return {
        "train_bars": int(train_bars),
        "test_bars": int(test_bars),
        "step_bars": int(step_bars),
        "window_units": "bars",
        "aliases_used": aliases_used,
        "derived_from_years": derived_from_years,
    }


def _iter_row_windows(
    df: pd.DataFrame,
    *,
    date_col: str,
    train_bars: int,
    test_bars: int,
    step_bars: int,
) -> list[dict[str, object]]:
    windows: list[dict[str, object]] = []
    total_rows = len(df)
    train_start_idx = 0
    window_index = 0

    while train_start_idx < total_rows:
        window_index += 1
        train_end_exclusive = min(train_start_idx + train_bars, total_rows)
        test_start_idx = train_end_exclusive
        test_end_exclusive = min(test_start_idx + test_bars, total_rows)

        train_df = df.iloc[train_start_idx:train_end_exclusive].copy()
        test_df = df.iloc[test_start_idx:test_end_exclusive].copy()

        train_rows = int(len(train_df))
        test_rows = int(len(test_df))

        if train_rows > 0:
            train_start = pd.Timestamp(train_df[date_col].iloc[0]).date().isoformat()
            train_end = pd.Timestamp(train_df[date_col].iloc[-1]).date().isoformat()
        else:
            train_start = None
            train_end = None

        if test_rows > 0:
            test_start = pd.Timestamp(test_df[date_col].iloc[0]).date().isoformat()
            test_end = pd.Timestamp(test_df[date_col].iloc[-1]).date().isoformat()
        else:
            test_start = None
            test_end = None

        windows.append(
            {
                "window_index": window_index,
                "train_start_idx": int(train_start_idx),
                "train_end_exclusive": int(train_end_exclusive),
                "test_start_idx": int(test_start_idx),
                "test_end_exclusive": int(test_end_exclusive),
                "train_start": train_start,
                "train_end": train_end,
                "test_start": test_start,
                "test_end": test_end,
                "train_rows": train_rows,
                "test_rows": test_rows,
                "train_df": train_df,
                "test_df": test_df,
            }
        )

        if test_rows < test_bars:
            break

        train_start_idx += step_bars

    return windows


def _run_stats(
    *,
    args: argparse.Namespace,
    df: pd.DataFrame,
    symbol: str,
    params: dict[str, int | None],
) -> dict[str, object]:
    strategy_params = build_strategy_params(args) | params
    if args.engine == "legacy":
        return run_backtest_on_df(
            df=df,
            symbol=symbol,
            strategy=args.strategy,
            **strategy_params,
            cash=args.cash,
            commission=args.commission,
        )

    if args.engine == "vectorized":
        result = run_vectorized_research_on_df(
            df=df,
            symbol=symbol,
            strategy=args.strategy,
            **strategy_params,
            cost_per_turnover=args.commission,
            initial_equity=args.cash,
        )
        return to_legacy_stats(
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

    raise SystemExit(f"Unsupported engine: {args.engine}")


def _score_candidates(
    *,
    args: argparse.Namespace,
    train_df: pd.DataFrame,
    symbol: str,
    param_grid: list[dict[str, int | None]],
) -> tuple[dict[str, int | None] | None, float | None, dict[str, object] | None, list[dict[str, object]]]:
    candidates: list[dict[str, object]] = []
    best_params = None
    best_score = None
    best_train_stats = None

    for params in param_grid:
        try:
            train_stats = _run_stats(
                args=args,
                df=train_df,
                symbol=symbol,
                params=params,
            )
        except Exception as exc:
            candidates.append(
                {
                    "params": params,
                    "score": None,
                    "status": "error",
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
            continue

        score = train_stats.get(args.select_by)
        score_value = None if score is None or pd.isna(score) else float(score)
        candidates.append(
            {
                "params": params,
                "score": score_value,
                "status": "ok",
                "return_pct": train_stats.get("Return [%]"),
                "sharpe": train_stats.get("Sharpe Ratio"),
                "max_drawdown_pct": train_stats.get("Max. Drawdown [%]"),
            }
        )
        if score_value is None:
            continue
        if best_score is None or score_value > best_score:
            best_score = score_value
            best_params = params
            best_train_stats = train_stats

    return best_params, best_score, best_train_stats, candidates


def _candidate_snapshot(candidates: list[dict[str, object]], top_n: int = 3) -> str:
    successful = [candidate for candidate in candidates if candidate.get("status") == "ok" and candidate.get("score") is not None]
    successful = sorted(successful, key=lambda item: float(item["score"]), reverse=True)[:top_n]
    payload = []
    for candidate in successful:
        payload.append(
            {
                "fast": candidate["params"].get("fast"),
                "slow": candidate["params"].get("slow"),
                "lookback": candidate["params"].get("lookback"),
                "lookback_bars": candidate["params"].get("lookback_bars"),
                "skip_bars": candidate["params"].get("skip_bars"),
                "top_n": candidate["params"].get("top_n"),
                "rebalance_bars": candidate["params"].get("rebalance_bars"),
                "weighting_scheme": candidate["params"].get("weighting_scheme"),
                "max_position_weight": candidate["params"].get("max_position_weight"),
                "min_avg_dollar_volume": candidate["params"].get("min_avg_dollar_volume"),
                "max_names_per_sector": candidate["params"].get("max_names_per_sector"),
                "turnover_buffer_bps": candidate["params"].get("turnover_buffer_bps"),
                "max_turnover_per_rebalance": candidate["params"].get("max_turnover_per_rebalance"),
                "entry_lookback": candidate["params"].get("entry_lookback"),
                "exit_lookback": candidate["params"].get("exit_lookback"),
                "momentum_lookback": candidate["params"].get("momentum_lookback"),
                "score": candidate.get("score"),
                "return_pct": candidate.get("return_pct"),
                "sharpe": candidate.get("sharpe"),
            }
        )
    return json.dumps(payload)


def _slice_prepared_frames_by_date(
    prepared_frames: dict[str, dict[str, object]],
    *,
    start: str | None = None,
    end: str | None = None,
) -> dict[str, dict[str, object]]:
    sliced: dict[str, dict[str, object]] = {}
    start_ts = pd.Timestamp(start) if start is not None else None
    end_ts = pd.Timestamp(end) if end is not None else None

    for symbol, prepared in prepared_frames.items():
        date_col = str(prepared["date_col"])
        working = prepared["df"].copy()
        working[date_col] = pd.to_datetime(working[date_col])
        if start_ts is not None:
            working = working[working[date_col] >= start_ts]
        if end_ts is not None:
            working = working[working[date_col] <= end_ts]
        sliced[symbol] = {
            **prepared,
            "df": working.reset_index(drop=True),
        }
    return sliced


def _build_summary_rows(window_df: pd.DataFrame, *, strategy: str, engine: str) -> pd.DataFrame:
    summary_rows: list[dict[str, object]] = []
    for symbol in sorted(window_df["symbol"].dropna().unique()):
        symbol_df = window_df[window_df["symbol"] == symbol].copy()
        completed_df = symbol_df[symbol_df["window_status"] == "completed"].copy()
        skipped_df = symbol_df[symbol_df["window_status"] != "completed"].copy()

        row: dict[str, object] = {
            "symbol": symbol,
            "strategy": strategy,
            "engine": engine,
            "benchmark_type": symbol_df["benchmark_type"].iloc[0] if "benchmark_type" in symbol_df.columns else None,
            "effective_start_date": symbol_df["effective_start_date"].iloc[0],
            "effective_end_date": symbol_df["effective_end_date"].iloc[0],
            "candidate_windows": int(len(symbol_df)),
            "completed_windows": int(len(completed_df)),
            "skipped_windows": int(len(skipped_df)),
            "percent_positive_windows": (
                float((completed_df["test_return_pct"] > 0).mean() * 100.0)
                if not completed_df.empty
                else float("nan")
            ),
            "avg_test_return_pct": completed_df["test_return_pct"].mean(),
            "avg_test_gross_return_pct": completed_df["test_gross_return_pct"].mean() if "test_gross_return_pct" in completed_df.columns else float("nan"),
            "avg_test_net_return_pct": completed_df["test_net_return_pct"].mean() if "test_net_return_pct" in completed_df.columns else float("nan"),
            "avg_test_cost_drag_return_pct": completed_df["test_cost_drag_return_pct"].mean() if "test_cost_drag_return_pct" in completed_df.columns else float("nan"),
            "median_test_return_pct": completed_df["test_return_pct"].median(),
            "compounded_test_return_pct": compound_return_pct(completed_df["test_return_pct"]) if not completed_df.empty else float("nan"),
            "avg_benchmark_return_pct": completed_df["benchmark_return_pct"].mean(),
            "median_benchmark_return_pct": completed_df["benchmark_return_pct"].median(),
            "compounded_benchmark_return_pct": compound_return_pct(completed_df["benchmark_return_pct"]) if not completed_df.empty else float("nan"),
            "avg_excess_return_pct": completed_df["excess_return_pct"].mean(),
            "median_excess_return_pct": completed_df["excess_return_pct"].median(),
            "compounded_excess_return_pct": (
                compound_return_pct(completed_df["test_return_pct"]) - compound_return_pct(completed_df["benchmark_return_pct"])
                if not completed_df.empty
                else float("nan")
            ),
            "worst_excess_return_pct": completed_df["excess_return_pct"].min(),
            "best_excess_return_pct": completed_df["excess_return_pct"].max(),
            "avg_test_sharpe": completed_df["test_sharpe"].mean(),
            "median_test_sharpe": completed_df["test_sharpe"].median(),
            "worst_test_max_drawdown_pct": completed_df["test_max_drawdown_pct"].min(),
            "total_trade_count": completed_df["trade_count"].fillna(0).sum() if "trade_count" in completed_df.columns else 0,
            "total_entry_count": completed_df["entry_count"].fillna(0).sum() if "entry_count" in completed_df.columns else 0,
            "total_exit_count": completed_df["exit_count"].fillna(0).sum() if "exit_count" in completed_df.columns else 0,
            "mean_percent_time_in_market": completed_df["percent_time_in_market"].mean() if "percent_time_in_market" in completed_df.columns else float("nan"),
            "mean_average_holding_period_bars": completed_df["average_holding_period_bars"].mean() if "average_holding_period_bars" in completed_df.columns else float("nan"),
            "mean_final_position_size": completed_df["final_position_size"].mean() if "final_position_size" in completed_df.columns else float("nan"),
            "mean_average_number_of_holdings": completed_df["average_number_of_holdings"].mean() if "average_number_of_holdings" in completed_df.columns else float("nan"),
            "total_rebalance_count": completed_df["rebalance_count"].fillna(0).sum() if "rebalance_count" in completed_df.columns else 0,
            "mean_turnover": completed_df["mean_turnover"].mean() if "mean_turnover" in completed_df.columns else float("nan"),
            "mean_annualized_turnover": completed_df["annualized_turnover"].mean() if "annualized_turnover" in completed_df.columns else float("nan"),
            "mean_transaction_cost": completed_df["mean_transaction_cost"].mean() if "mean_transaction_cost" in completed_df.columns else float("nan"),
            "total_transaction_cost": completed_df["total_transaction_cost"].fillna(0).sum() if "total_transaction_cost" in completed_df.columns else 0,
            "mean_percent_invested": completed_df["percent_invested"].mean() if "percent_invested" in completed_df.columns else float("nan"),
            "mean_average_gross_exposure": completed_df["average_gross_exposure"].mean() if "average_gross_exposure" in completed_df.columns else float("nan"),
            "mean_average_valid_scores": completed_df["average_valid_scores"].mean() if "average_valid_scores" in completed_df.columns else float("nan"),
            "mean_min_available_symbols": completed_df["min_available_symbols"].mean() if "min_available_symbols" in completed_df.columns else float("nan"),
            "mean_average_available_symbols": completed_df["average_available_symbols"].mean() if "average_available_symbols" in completed_df.columns else float("nan"),
            "mean_max_available_symbols": completed_df["max_available_symbols"].mean() if "max_available_symbols" in completed_df.columns else float("nan"),
            "mean_min_eligible_symbols": completed_df["min_eligible_symbols"].mean() if "min_eligible_symbols" in completed_df.columns else float("nan"),
            "mean_average_eligible_symbols": completed_df["average_eligible_symbols"].mean() if "average_eligible_symbols" in completed_df.columns else float("nan"),
            "mean_max_eligible_symbols": completed_df["max_eligible_symbols"].mean() if "max_eligible_symbols" in completed_df.columns else float("nan"),
            "mean_average_selected_symbols": completed_df["average_selected_symbols"].mean() if "average_selected_symbols" in completed_df.columns else float("nan"),
            "mean_average_target_selected_count": completed_df["target_selected_count"].mean() if "target_selected_count" in completed_df.columns else float("nan"),
            "mean_average_realized_holdings_count": completed_df["realized_holdings_count"].mean() if "realized_holdings_count" in completed_df.columns else float("nan"),
            "mean_average_realized_holdings_minus_top_n": completed_df["realized_holdings_minus_top_n"].mean() if "realized_holdings_minus_top_n" in completed_df.columns else float("nan"),
            "mean_average_holdings_ratio_to_top_n": completed_df["holdings_ratio_to_top_n"].mean() if "holdings_ratio_to_top_n" in completed_df.columns else float("nan"),
            "percent_windows_realized_holdings_exceeded_top_n": (
                float(completed_df["realized_holdings_exceeded_top_n"].fillna(False).astype(bool).mean() * 100.0)
                if "realized_holdings_exceeded_top_n" in completed_df.columns and not completed_df.empty
                else float("nan")
            ),
            "portfolio_construction_mode": completed_df["portfolio_construction_mode"].iloc[0] if "portfolio_construction_mode" in completed_df.columns and not completed_df.empty else None,
            "semantic_warning": ";".join(sorted({str(item) for item in completed_df["semantic_warning"].fillna("").tolist() if str(item).strip()})) if "semantic_warning" in completed_df.columns else "",
            "mean_percent_empty_rebalances": completed_df["percent_empty_rebalances"].mean() if "percent_empty_rebalances" in completed_df.columns else float("nan"),
            "liquidity_filter_active": bool(symbol_df["liquidity_filter_active"].fillna(False).astype(bool).any()) if "liquidity_filter_active" in symbol_df.columns else False,
            "sector_cap_active": bool(symbol_df["sector_cap_active"].fillna(False).astype(bool).any()) if "sector_cap_active" in symbol_df.columns else False,
            "sector_warning": symbol_df["sector_warning"].dropna().iloc[0] if "sector_warning" in symbol_df.columns and not symbol_df["sector_warning"].dropna().empty else None,
            "mean_average_liquidity_excluded_symbols": completed_df["average_liquidity_excluded_symbols"].mean() if "average_liquidity_excluded_symbols" in completed_df.columns else float("nan"),
            "total_liquidity_excluded_symbols": completed_df["total_liquidity_excluded_symbols"].fillna(0).sum() if "total_liquidity_excluded_symbols" in completed_df.columns else 0,
            "mean_average_sector_cap_excluded_symbols": completed_df["average_sector_cap_excluded_symbols"].mean() if "average_sector_cap_excluded_symbols" in completed_df.columns else float("nan"),
            "total_sector_cap_excluded_symbols": completed_df["total_sector_cap_excluded_symbols"].fillna(0).sum() if "total_sector_cap_excluded_symbols" in completed_df.columns else 0,
            "total_turnover_cap_binding_count": completed_df["turnover_cap_binding_count"].fillna(0).sum() if "turnover_cap_binding_count" in completed_df.columns else 0,
            "total_turnover_buffer_blocked_replacements": completed_df["turnover_buffer_blocked_replacements"].fillna(0).sum() if "turnover_buffer_blocked_replacements" in completed_df.columns else 0,
            "mean_initial_equity": completed_df["initial_equity"].mean() if "initial_equity" in completed_df.columns else float("nan"),
            "mean_final_equity": completed_df["final_equity"].mean() if "final_equity" in completed_df.columns else float("nan"),
            "earliest_data_date_by_symbol": symbol_df["earliest_data_date_by_symbol"].iloc[0] if "earliest_data_date_by_symbol" in symbol_df.columns else None,
            "percent_windows_ended_in_cash": (
                float(completed_df["ended_in_cash"].fillna(False).astype(bool).mean() * 100.0)
                if "ended_in_cash" in completed_df.columns and not completed_df.empty
                else float("nan")
            ),
        }

        if strategy == "sma_cross" and not completed_df.empty:
            param_counts = (
                completed_df.groupby(["fast", "slow"])
                .size()
                .reset_index(name="count")
                .sort_values(["count", "fast", "slow"], ascending=[False, True, True])
            )
            if not param_counts.empty:
                best_params = param_counts.iloc[0]
                row["most_selected_fast"] = best_params["fast"]
                row["most_selected_slow"] = best_params["slow"]
                row["most_selected_count"] = best_params["count"]
        elif strategy == "momentum_hold" and not completed_df.empty:
            param_counts = (
                completed_df.groupby(["lookback"])
                .size()
                .reset_index(name="count")
                .sort_values(["count", "lookback"], ascending=[False, True])
            )
            if not param_counts.empty:
                best_params = param_counts.iloc[0]
                row["most_selected_lookback"] = best_params["lookback"]
                row["most_selected_count"] = best_params["count"]
        elif strategy == "breakout_hold" and not completed_df.empty:
            param_counts = (
                completed_df.groupby(["entry_lookback", "exit_lookback", "momentum_lookback"], dropna=False)
                .size()
                .reset_index(name="count")
                .sort_values(
                    ["count", "entry_lookback", "exit_lookback", "momentum_lookback"],
                    ascending=[False, True, True, True],
                )
            )
            if not param_counts.empty:
                best_params = param_counts.iloc[0]
                row["most_selected_entry_lookback"] = best_params["entry_lookback"]
                row["most_selected_exit_lookback"] = best_params["exit_lookback"]
                row["most_selected_momentum_lookback"] = best_params["momentum_lookback"]
                row["most_selected_count"] = best_params["count"]
        elif strategy == "xsec_momentum_topn" and not completed_df.empty:
            param_counts = (
                completed_df.groupby(["lookback_bars", "skip_bars", "top_n", "rebalance_bars"], dropna=False)
                .size()
                .reset_index(name="count")
                .sort_values(["count", "lookback_bars", "skip_bars", "top_n", "rebalance_bars"], ascending=[False, True, True, True, True])
            )
            if not param_counts.empty:
                best_params = param_counts.iloc[0]
                row["most_selected_lookback_bars"] = best_params["lookback_bars"]
                row["most_selected_skip_bars"] = best_params["skip_bars"]
                row["most_selected_top_n"] = best_params["top_n"]
                row["most_selected_rebalance_bars"] = best_params["rebalance_bars"]
                row["most_selected_count"] = best_params["count"]

        summary_rows.append(row)

    return pd.DataFrame(summary_rows)


def _build_overall_universe_summary(summary_df: pd.DataFrame) -> pd.DataFrame:
    if summary_df.empty:
        return pd.DataFrame(
            [
                {
                    "symbols": 0,
                    "completed_windows": 0,
                    "skipped_windows": 0,
                    "mean_avg_test_return_pct": float("nan"),
                    "mean_avg_excess_return_pct": float("nan"),
                    "best_symbol_by_avg_excess_return": None,
                    "worst_symbol_by_avg_excess_return": None,
                }
            ]
        )

    best_symbol = None
    worst_symbol = None
    if summary_df["avg_excess_return_pct"].notna().any():
        best_symbol = summary_df.loc[summary_df["avg_excess_return_pct"].idxmax(), "symbol"]
        worst_symbol = summary_df.loc[summary_df["avg_excess_return_pct"].idxmin(), "symbol"]

    return pd.DataFrame(
        [
            {
                "symbols": int(len(summary_df)),
                "completed_windows": int(summary_df["completed_windows"].sum()),
                "skipped_windows": int(summary_df["skipped_windows"].sum()),
                "mean_avg_test_return_pct": summary_df["avg_test_return_pct"].mean(),
                "mean_avg_excess_return_pct": summary_df["avg_excess_return_pct"].mean(),
                "best_symbol_by_avg_excess_return": best_symbol,
                "worst_symbol_by_avg_excess_return": worst_symbol,
            }
        ]
    )


def run_xsec_walkforward_analysis(
    args: argparse.Namespace,
    *,
    symbols: list[str],
    param_grid: list[dict[str, int | None]],
    window_spec: dict[str, object],
    verbose: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame | None]:
    prepared_frames = {
        symbol: prepare_research_frame(symbol, start=args.start, end=args.end)
        for symbol in symbols
    }
    strategy_params = build_strategy_params(args)
    close_panel, _ = build_close_panel(prepared_frames)
    effective_start = str(pd.Timestamp(close_panel.index.min()).date())
    effective_end = str(pd.Timestamp(close_panel.index.max()).date())
    working_df = close_panel.reset_index().rename(columns={"index": "timestamp"})
    candidate_windows = _iter_row_windows(
        working_df,
        date_col="timestamp",
        train_bars=window_spec["train_bars"],
        test_bars=window_spec["test_bars"],
        step_bars=window_spec["step_bars"],
    )
    universe_rows: list[dict[str, object]] = []
    rebalance_debug_rows: list[pd.DataFrame] = []

    for window in candidate_windows:
        base_row = {
            "window_index": window["window_index"],
            "symbol": "UNIVERSE",
            "symbols": ",".join(symbols),
            "symbol_count": len(symbols),
            "strategy": args.strategy,
            "engine": args.engine,
            "benchmark_type": args.benchmark,
            "portfolio_construction_mode": strategy_params["portfolio_construction_mode"],
            "window_units": window_spec["window_units"],
            "train_bars_requested": window_spec["train_bars"],
            "test_bars_requested": window_spec["test_bars"],
            "step_bars_requested": window_spec["step_bars"],
            "train_rows": window["train_rows"],
            "test_rows": window["test_rows"],
            "effective_start_date": effective_start,
            "effective_end_date": effective_end,
            "train_start": window["train_start"],
            "train_end": window["train_end"],
            "test_start": window["test_start"],
            "test_end": window["test_end"],
            "selected_by": args.select_by,
            "selected_train_score": None,
            "selected_candidate_count": 0,
            "top_candidate_scores": "[]",
            "fast": None,
            "slow": None,
            "lookback": None,
            "lookback_bars": None,
            "skip_bars": None,
            "top_n": None,
            "rebalance_bars": None,
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
            "train_return_pct": None,
            "train_sharpe": None,
            "train_max_drawdown_pct": None,
            "test_return_pct": None,
            "test_gross_return_pct": None,
            "test_net_return_pct": None,
            "test_cost_drag_return_pct": None,
            "test_sharpe": None,
            "test_max_drawdown_pct": None,
            "trade_count": None,
            "entry_count": None,
            "exit_count": None,
            "percent_time_in_market": None,
            "average_holding_period_bars": None,
            "final_position_size": None,
            "ended_in_cash": None,
            "average_number_of_holdings": None,
            "target_selected_count": None,
            "realized_holdings_count": None,
            "realized_holdings_minus_top_n": None,
            "holdings_ratio_to_top_n": None,
            "realized_holdings_exceeded_top_n": None,
            "semantic_warning": None,
            "rebalance_count": None,
            "mean_turnover": None,
            "annualized_turnover": None,
            "mean_transaction_cost": None,
            "total_transaction_cost": None,
            "percent_invested": None,
            "initial_equity": None,
            "final_equity": None,
            "average_gross_exposure": None,
            "min_available_symbols": None,
            "average_available_symbols": None,
            "max_available_symbols": None,
            "average_valid_scores": None,
            "min_eligible_symbols": None,
            "average_eligible_symbols": None,
            "max_eligible_symbols": None,
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
            "earliest_data_date_by_symbol": None,
            "benchmark_return_pct": None,
            "excess_return_pct": None,
            "window_status": "skipped",
            "skip_reason": None,
        }
        if base_row["train_rows"] < args.min_train_rows:
            base_row["skip_reason"] = f"insufficient_train_rows:{base_row['train_rows']}<{args.min_train_rows}"
            universe_rows.append(base_row)
            continue
        if base_row["test_rows"] < args.min_test_rows:
            base_row["skip_reason"] = f"insufficient_test_rows:{base_row['test_rows']}<{args.min_test_rows}"
            universe_rows.append(base_row)
            continue

        train_prepared = _slice_prepared_frames_by_date(
            prepared_frames,
            start=base_row["train_start"],
            end=base_row["train_end"],
        )
        test_prepared = _slice_prepared_frames_by_date(
            prepared_frames,
            end=base_row["test_end"],
        )

        candidates: list[dict[str, object]] = []
        best_params = None
        best_score = None
        best_train_stats = None
        for params in param_grid:
            try:
                train_result = run_xsec_momentum_topn(
                    prepared_frames=train_prepared,
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
                train_stats = train_result.summary
            except Exception as exc:
                candidates.append({"params": params, "score": None, "status": "error", "error": f"{type(exc).__name__}: {exc}"})
                continue
            score = train_stats.get(args.select_by)
            score_value = None if score is None or pd.isna(score) else float(score)
            candidates.append(
                {
                    "params": params,
                    "score": score_value,
                    "status": "ok",
                    "return_pct": train_stats.get("Return [%]"),
                    "sharpe": train_stats.get("Sharpe Ratio"),
                    "max_drawdown_pct": train_stats.get("Max. Drawdown [%]"),
                }
            )
            if score_value is not None and (best_score is None or score_value > best_score):
                best_score = score_value
                best_params = params
                best_train_stats = train_stats
        base_row["selected_candidate_count"] = sum(1 for item in candidates if item.get("status") == "ok")
        base_row["top_candidate_scores"] = _candidate_snapshot(candidates)
        if best_params is None or best_train_stats is None:
            base_row["skip_reason"] = "no_valid_train_candidates"
            universe_rows.append(base_row)
            continue

        test_result = run_xsec_momentum_topn(
            prepared_frames=test_prepared,
            lookback_bars=int(best_params["lookback_bars"] or 126),
            skip_bars=int(best_params["skip_bars"] or 0),
            top_n=int(best_params["top_n"] or 1),
            rebalance_bars=int(best_params["rebalance_bars"] or 21),
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
            active_start=base_row["test_start"],
            active_end=base_row["test_end"],
        )
        test_stats = test_result.summary
        test_rebalance_debug = test_result.rebalance_diagnostics.reset_index()
        if not test_rebalance_debug.empty:
            context = pd.DataFrame(
                {
                    "window_index": [window["window_index"]] * len(test_rebalance_debug),
                    "symbol": ["UNIVERSE"] * len(test_rebalance_debug),
                    "test_start": [base_row["test_start"]] * len(test_rebalance_debug),
                    "test_end": [base_row["test_end"]] * len(test_rebalance_debug),
                }
            )
            rebalance_debug_rows.append(
                pd.concat([context.reset_index(drop=True), test_rebalance_debug.reset_index(drop=True)], axis=1)
            )
        base_row.update(
            {
                "selected_train_score": best_score,
                "lookback_bars": best_params.get("lookback_bars"),
                "skip_bars": best_params.get("skip_bars"),
                "top_n": best_params.get("top_n"),
                "rebalance_bars": best_params.get("rebalance_bars"),
                "weighting_scheme": test_stats.get("weighting_scheme"),
                "vol_lookback_bars": test_stats.get("vol_lookback_bars"),
                "max_position_weight": test_stats.get("max_position_weight"),
                "min_avg_dollar_volume": test_stats.get("min_avg_dollar_volume"),
                "max_names_per_sector": test_stats.get("max_names_per_sector"),
                "turnover_buffer_bps": test_stats.get("turnover_buffer_bps"),
                "turnover_buffer_score_gap": test_stats.get("turnover_buffer_score_gap"),
                "max_turnover_per_rebalance": test_stats.get("max_turnover_per_rebalance"),
                "train_return_pct": best_train_stats.get("Return [%]"),
                "train_sharpe": best_train_stats.get("Sharpe Ratio"),
                "train_max_drawdown_pct": best_train_stats.get("Max. Drawdown [%]"),
                "test_return_pct": test_stats.get("Return [%]"),
                "test_gross_return_pct": test_stats.get("gross_return_pct"),
                "test_net_return_pct": test_stats.get("net_return_pct"),
                "test_cost_drag_return_pct": test_stats.get("cost_drag_return_pct"),
                "test_sharpe": test_stats.get("Sharpe Ratio"),
                "test_max_drawdown_pct": test_stats.get("Max. Drawdown [%]"),
                "trade_count": test_stats.get("trade_count"),
                "entry_count": test_stats.get("entry_count"),
                "exit_count": test_stats.get("exit_count"),
                "percent_time_in_market": test_stats.get("percent_time_in_market"),
                "average_holding_period_bars": test_stats.get("average_holding_period_bars"),
                "final_position_size": test_stats.get("final_position_size"),
                "ended_in_cash": test_stats.get("ended_in_cash"),
                "average_number_of_holdings": test_stats.get("average_number_of_holdings"),
                "target_selected_count": test_stats.get("average_target_selected_count"),
                "realized_holdings_count": test_stats.get("average_realized_holdings_count"),
                "realized_holdings_minus_top_n": test_stats.get("average_realized_holdings_minus_top_n"),
                "holdings_ratio_to_top_n": test_stats.get("average_holdings_ratio_to_top_n"),
                "realized_holdings_exceeded_top_n": test_stats.get("realized_holdings_exceeded_top_n"),
                "semantic_warning": test_stats.get("semantic_warning"),
                "rebalance_count": test_stats.get("rebalance_count"),
                "mean_turnover": test_stats.get("mean_turnover"),
                "annualized_turnover": test_stats.get("annualized_turnover"),
                "mean_transaction_cost": test_stats.get("mean_transaction_cost"),
                "total_transaction_cost": test_stats.get("total_transaction_cost"),
                "percent_invested": test_stats.get("percent_invested"),
                "initial_equity": test_stats.get("initial_equity"),
                "final_equity": test_stats.get("final_equity"),
                "average_gross_exposure": test_stats.get("average_gross_exposure"),
                "min_available_symbols": test_stats.get("min_available_symbols"),
                "average_available_symbols": test_stats.get("average_available_symbols"),
                "max_available_symbols": test_stats.get("max_available_symbols"),
                "average_valid_scores": test_stats.get("average_valid_scores"),
                "min_eligible_symbols": test_stats.get("min_eligible_symbols"),
                "average_eligible_symbols": test_stats.get("average_eligible_symbols"),
                "max_eligible_symbols": test_stats.get("max_eligible_symbols"),
                "average_selected_symbols": test_stats.get("average_selected_symbols"),
                "percent_empty_rebalances": test_stats.get("percent_empty_rebalances"),
                "liquidity_filter_active": test_stats.get("liquidity_filter_active"),
                "sector_cap_active": test_stats.get("sector_cap_active"),
                "sector_warning": test_stats.get("sector_warning"),
                "average_liquidity_excluded_symbols": test_stats.get("average_liquidity_excluded_symbols"),
                "total_liquidity_excluded_symbols": test_stats.get("total_liquidity_excluded_symbols"),
                "average_sector_cap_excluded_symbols": test_stats.get("average_sector_cap_excluded_symbols"),
                "total_sector_cap_excluded_symbols": test_stats.get("total_sector_cap_excluded_symbols"),
                "turnover_cap_binding_count": test_stats.get("turnover_cap_binding_count"),
                "turnover_buffer_blocked_replacements": test_stats.get("turnover_buffer_blocked_replacements"),
                "earliest_data_date_by_symbol": test_stats.get("earliest_data_date_by_symbol"),
                "benchmark_return_pct": test_stats.get("benchmark_return_pct"),
                "excess_return_pct": test_stats.get("excess_return_pct"),
                "window_status": "completed",
                "skip_reason": "",
            }
        )
        universe_rows.append(base_row)
        if verbose:
            print(
                f"[OK] universe: effective {effective_start}->{effective_end} | "
                f"window {window['window_index']} train {base_row['train_start']}->{base_row['train_end']} ({base_row['train_rows']} rows) | "
                f"test {base_row['test_start']}->{base_row['test_end']} ({base_row['test_rows']} rows) | "
                f"params lookback_bars={base_row['lookback_bars']} skip_bars={base_row['skip_bars']} top_n={base_row['top_n']} "
                f"rebalance_bars={base_row['rebalance_bars']} portfolio_construction_mode={base_row['portfolio_construction_mode']} weighting_scheme={base_row['weighting_scheme']} "
                f"max_position_weight={base_row['max_position_weight']} min_avg_dollar_volume={base_row['min_avg_dollar_volume']} "
                f"max_names_per_sector={base_row['max_names_per_sector']} turnover_buffer_bps={base_row['turnover_buffer_bps']} "
                f"max_turnover_per_rebalance={base_row['max_turnover_per_rebalance']} benchmark={base_row['benchmark_type']} | selected_by={args.select_by} score={best_score} | "
                f"test gross[%]={base_row['test_gross_return_pct']} net[%]={base_row['test_net_return_pct']} cost_drag[%]={base_row['test_cost_drag_return_pct']} | avg_holdings={base_row['average_number_of_holdings']} | "
                f"target_selected={base_row['target_selected_count']} realized_holdings={base_row['realized_holdings_count']} holdings_to_top_n={base_row['holdings_ratio_to_top_n']} exceeded_top_n={base_row['realized_holdings_exceeded_top_n']} | "
                f"percent_invested={base_row['percent_invested']} | available[min/avg/max]={base_row['min_available_symbols']}/{base_row['average_available_symbols']}/{base_row['max_available_symbols']} | "
                f"eligible[min/avg/max]={base_row['min_eligible_symbols']}/{base_row['average_eligible_symbols']}/{base_row['max_eligible_symbols']} "
                f"avg_selected={base_row['average_selected_symbols']} | empty_rebalances[%]={base_row['percent_empty_rebalances']} | "
                f"liquidity_excluded={base_row['total_liquidity_excluded_symbols']} sector_cap_excluded={base_row['total_sector_cap_excluded_symbols']} "
                f"turnover_cap_bindings={base_row['turnover_cap_binding_count']} buffer_blocked={base_row['turnover_buffer_blocked_replacements']} semantic_warning={base_row['semantic_warning'] or 'none'} | "
                f"avg_turnover={base_row['mean_turnover']} annualized_turnover={base_row['annualized_turnover']} total_transaction_cost={base_row['total_transaction_cost']} | "
                f"initial_equity={base_row['initial_equity']} final_equity={base_row['final_equity']} | avg_gross_exposure={base_row['average_gross_exposure']} | "
                f"activity={activity_note(base_row)} | benchmark Return[%]={base_row['benchmark_return_pct']} | excess Return[%]={base_row['excess_return_pct']}"
            )

    out_df = pd.DataFrame(universe_rows)
    summary_df = _build_summary_rows(out_df, strategy=args.strategy, engine=args.engine)
    rebalance_debug_df = pd.concat(rebalance_debug_rows, ignore_index=True) if rebalance_debug_rows else None
    return out_df, summary_df, rebalance_debug_df


def cmd_walkforward(args: argparse.Namespace) -> None:
    if getattr(args, "config", None):
        loaded = load_walkforward_workflow_config(args.config)
        if getattr(loaded, "preset", None) and not option_is_explicit(args, "preset"):
            args.preset = loaded.preset
    apply_cli_preset(args)
    apply_workflow_config(
        args,
        config_path=getattr(args, "config", None),
        loader=load_walkforward_workflow_config,
    )
    symbols = resolve_symbols(args)
    param_grid, invalid_warnings = _build_param_grid(args)
    window_spec = _resolve_window_spec(args)
    all_rows: list[dict[str, object]] = []

    print(
        f"Running walk-forward for {len(symbols)} symbol(s): {print_symbol_list(symbols)} | "
        f"param_combinations={len(param_grid)} | requested_range={args.start or 'full'}->{args.end or 'full'} | "
        f"engine={args.engine} | window_units={window_spec['window_units']} | "
        f"train_bars={window_spec['train_bars']} test_bars={window_spec['test_bars']} step_bars={window_spec['step_bars']}"
    )
    for warning in invalid_warnings:
        print(f"[SKIP] {warning}")
    if window_spec["aliases_used"]:
        print(f"[INFO] Compatibility aliases applied: {', '.join(window_spec['aliases_used'])}")
    if window_spec["derived_from_years"]:
        print(
            "[INFO] Derived walk-forward bar counts from year defaults: "
            f"{', '.join(window_spec['derived_from_years'])}"
        )

    if args.strategy == "xsec_momentum_topn":
        out_df, summary_df, rebalance_debug_df = run_xsec_walkforward_analysis(
            args,
            symbols=symbols,
            param_grid=param_grid,
            window_spec=window_spec,
            verbose=True,
        )
        completed_out_df = out_df[out_df["window_status"] == "completed"].copy()
        print("\nWalk-forward window results:")
        print(out_df.to_string(index=False))
        print("\nWalk-forward aggregate summary:")
        print(summary_df.to_string(index=False))
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        out_df.to_csv(output_path, index=False)
        summary_path = output_path.with_name(output_path.stem + "_summary.csv")
        summary_df.to_csv(summary_path, index=False)
        rebalance_debug_path = None
        if rebalance_debug_df is not None:
            rebalance_debug_path = output_path.with_name(output_path.stem + "_rebalance_diagnostics.csv")
            rebalance_debug_df.to_csv(rebalance_debug_path, index=False)
        plot_source_df = completed_out_df if not completed_out_df.empty else out_df.iloc[0:0].copy()
        returns_plot_path = save_walkforward_return_plot(plot_source_df, output_path)
        params_plot_path = save_walkforward_param_plot(plot_source_df, output_path)
        report_path = save_walkforward_html_report(
            window_df=out_df,
            summary_df=summary_df,
            output_path=output_path,
            returns_plot_path=returns_plot_path,
            params_plot_path=params_plot_path,
        )
        workflow_summary_path = _write_workflow_summary_json(
            output_path=output_path,
            args=args,
            symbols=symbols,
            out_df=out_df,
            summary_df=summary_df,
            extra_paths={
                "windows_csv": str(output_path),
                "summary_csv": str(summary_path),
                "report_html": str(report_path),
                "returns_plot": str(returns_plot_path),
                "params_plot": str(params_plot_path) if params_plot_path is not None else "",
                "rebalance_diagnostics": str(rebalance_debug_path) if rebalance_debug_path is not None else "",
            },
        )
        print(f"Saved walk-forward returns plot to {returns_plot_path}")
        if params_plot_path is not None:
            print(f"Saved walk-forward parameter plot to {params_plot_path}")
        print(f"Saved walk-forward HTML report to {report_path}")
        print(f"\nSaved walk-forward window results to {output_path}")
        print(f"Saved walk-forward summary to {summary_path}")
        print(f"Saved walk-forward workflow summary to {workflow_summary_path}")
        if rebalance_debug_path is not None:
            print(f"Saved walk-forward rebalance diagnostics to {rebalance_debug_path}")
        return

    for symbol in symbols:
        try:
            prepared = prepare_research_frame(symbol, start=args.start, end=args.end)
            df = prepared["df"]
        except Exception as exc:
            print(f"[ERROR] {symbol}: failed to load feature frame -> {exc}")
            continue

        date_col = str(prepared["date_col"])
        effective_start = str(prepared["effective_start"])
        effective_end = str(prepared["effective_end"])
        symbol_rows: list[dict[str, object]] = []
        candidate_windows = _iter_row_windows(
            df,
            date_col=date_col,
            train_bars=window_spec["train_bars"],
            test_bars=window_spec["test_bars"],
            step_bars=window_spec["step_bars"],
        )

        for window in candidate_windows:
            train_df = window["train_df"]
            test_df = window["test_df"]
            base_row = {
                "window_index": window["window_index"],
                "symbol": symbol,
                "strategy": args.strategy,
                "engine": args.engine,
                "window_units": window_spec["window_units"],
                "train_bars_requested": window_spec["train_bars"],
                "test_bars_requested": window_spec["test_bars"],
                "step_bars_requested": window_spec["step_bars"],
                "train_rows": window["train_rows"],
                "test_rows": window["test_rows"],
                "effective_start_date": effective_start,
                "effective_end_date": effective_end,
                "train_start": window["train_start"],
                "train_end": window["train_end"],
                "test_start": window["test_start"],
                "test_end": window["test_end"],
                "selected_by": args.select_by,
                "selected_train_score": None,
                "selected_candidate_count": 0,
                "top_candidate_scores": "[]",
                "fast": None,
                "slow": None,
                "lookback": None,
                "entry_lookback": None,
                "exit_lookback": None,
                "momentum_lookback": None,
                "train_return_pct": None,
                "train_sharpe": None,
                "train_max_drawdown_pct": None,
                "test_return_pct": None,
                "test_sharpe": None,
                "test_max_drawdown_pct": None,
                "trade_count": None,
                "entry_count": None,
                "exit_count": None,
                "percent_time_in_market": None,
                "average_holding_period_bars": None,
                "final_position_size": None,
                "ended_in_cash": None,
                "benchmark_return_pct": None,
                "excess_return_pct": None,
                "window_status": "skipped",
                "skip_reason": None,
            }

            if base_row["train_rows"] < args.min_train_rows:
                base_row["skip_reason"] = f"insufficient_train_rows:{base_row['train_rows']}<{args.min_train_rows}"
                symbol_rows.append(base_row)
                continue

            if base_row["test_rows"] < args.min_test_rows:
                base_row["skip_reason"] = f"insufficient_test_rows:{base_row['test_rows']}<{args.min_test_rows}"
                symbol_rows.append(base_row)
                continue

            best_params, best_score, best_train_stats, candidates = _score_candidates(
                args=args,
                train_df=train_df,
                symbol=symbol,
                param_grid=param_grid,
            )
            base_row["selected_candidate_count"] = sum(1 for item in candidates if item.get("status") == "ok")
            base_row["top_candidate_scores"] = _candidate_snapshot(candidates)

            if best_params is None or best_train_stats is None:
                base_row["skip_reason"] = "no_valid_train_candidates"
                symbol_rows.append(base_row)
                continue

            try:
                test_stats = _run_stats(
                    args=args,
                    df=test_df,
                    symbol=symbol,
                    params=best_params,
                )
            except Exception as exc:
                base_row["skip_reason"] = f"test_eval_failed:{type(exc).__name__}"
                symbol_rows.append(base_row)
                continue

            benchmark_return_pct = compute_buy_and_hold_return_pct(test_df)
            test_return_pct = test_stats.get("Return [%]")
            if (
                test_return_pct is not None
                and not pd.isna(test_return_pct)
                and not pd.isna(benchmark_return_pct)
            ):
                excess_return_pct = test_return_pct - benchmark_return_pct
            else:
                excess_return_pct = float("nan")

            base_row.update(
                {
                    "selected_train_score": best_score,
                    "fast": best_params.get("fast"),
                    "slow": best_params.get("slow"),
                    "lookback": best_params.get("lookback"),
                    "entry_lookback": best_params.get("entry_lookback"),
                    "exit_lookback": best_params.get("exit_lookback"),
                    "momentum_lookback": best_params.get("momentum_lookback"),
                    "train_return_pct": best_train_stats.get("Return [%]"),
                    "train_sharpe": best_train_stats.get("Sharpe Ratio"),
                    "train_max_drawdown_pct": best_train_stats.get("Max. Drawdown [%]"),
                    "test_return_pct": test_return_pct,
                    "test_sharpe": test_stats.get("Sharpe Ratio"),
                    "test_max_drawdown_pct": test_stats.get("Max. Drawdown [%]"),
                    "trade_count": test_stats.get("trade_count"),
                    "entry_count": test_stats.get("entry_count"),
                    "exit_count": test_stats.get("exit_count"),
                    "percent_time_in_market": test_stats.get("percent_time_in_market"),
                    "average_holding_period_bars": test_stats.get("average_holding_period_bars"),
                    "final_position_size": test_stats.get("final_position_size"),
                    "ended_in_cash": test_stats.get("ended_in_cash"),
                    "benchmark_return_pct": benchmark_return_pct,
                    "excess_return_pct": excess_return_pct,
                    "window_status": "completed",
                    "skip_reason": "",
                }
            )
            symbol_rows.append(base_row)

            print(
                f"[OK] {symbol}: effective {effective_start}->{effective_end} | "
                f"window {window['window_index']} train {base_row['train_start']}->{base_row['train_end']} "
                f"({base_row['train_rows']} rows) | "
                f"test {base_row['test_start']}->{base_row['test_end']} "
                f"({base_row['test_rows']} rows) | "
                f"params fast={base_row['fast']} slow={base_row['slow']} lookback={base_row['lookback']} | "
                f"entry_lookback={base_row['entry_lookback']} exit_lookback={base_row['exit_lookback']} "
                f"momentum_lookback={base_row['momentum_lookback']} | "
                f"selected_by={args.select_by} score={best_score} | "
                f"test Return[%]={base_row['test_return_pct']} | "
                f"trade_count={base_row['trade_count']} | "
                f"time_in_market[%]={base_row['percent_time_in_market']} | "
                f"activity={activity_note(base_row)} | "
                f"benchmark Return[%]={base_row['benchmark_return_pct']} | "
                f"excess Return[%]={base_row['excess_return_pct']}"
            )

        symbol_window_df = pd.DataFrame(symbol_rows)
        candidate_window_count = int(len(symbol_window_df))
        completed_windows = int((symbol_window_df["window_status"] == "completed").sum()) if not symbol_window_df.empty else 0
        skipped_windows = candidate_window_count - completed_windows
        print(
            f"{symbol}: effective_range={effective_start}->{effective_end}, "
            f"param_combinations={len(param_grid)}, candidate_windows={candidate_window_count}, "
            f"completed_windows={completed_windows}, skipped_windows={skipped_windows}, "
            f"window_units={window_spec['window_units']}, "
            f"train_bars={window_spec['train_bars']}, test_bars={window_spec['test_bars']}, step_bars={window_spec['step_bars']}"
        )
        all_rows.extend(symbol_rows)

    if not all_rows:
        print("No walk-forward results generated.")
        return

    out_df = pd.DataFrame(all_rows)
    completed_out_df = out_df[out_df["window_status"] == "completed"].copy()

    print("\nWalk-forward window results:")
    print(out_df.to_string(index=False))

    summary_df = _build_summary_rows(out_df, strategy=args.strategy, engine=args.engine)
    print("\nWalk-forward aggregate summary:")
    print(summary_df.to_string(index=False))

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(output_path, index=False)

    summary_path = output_path.with_name(output_path.stem + "_summary.csv")
    summary_df.to_csv(summary_path, index=False)

    overall_summary_path = None
    if len(summary_df) > 1:
        overall_summary_df = _build_overall_universe_summary(summary_df)
        overall_summary_path = output_path.with_name(output_path.stem + "_overall_summary.csv")
        overall_summary_df.to_csv(overall_summary_path, index=False)

    plot_source_df = completed_out_df if not completed_out_df.empty else out_df.iloc[0:0].copy()
    returns_plot_path = save_walkforward_return_plot(plot_source_df, output_path)
    params_plot_path = save_walkforward_param_plot(plot_source_df, output_path)
    report_path = save_walkforward_html_report(
        window_df=out_df,
        summary_df=summary_df,
        output_path=output_path,
        returns_plot_path=returns_plot_path,
        params_plot_path=params_plot_path,
    )
    workflow_summary_path = _write_workflow_summary_json(
        output_path=output_path,
        args=args,
        symbols=symbols,
        out_df=out_df,
        summary_df=summary_df,
        extra_paths={
            "windows_csv": str(output_path),
            "summary_csv": str(summary_path),
            "overall_summary_csv": str(overall_summary_path) if overall_summary_path is not None else "",
            "report_html": str(report_path),
            "returns_plot": str(returns_plot_path),
            "params_plot": str(params_plot_path) if params_plot_path is not None else "",
        },
    )

    print(f"Saved walk-forward returns plot to {returns_plot_path}")
    if params_plot_path is not None:
        print(f"Saved walk-forward parameter plot to {params_plot_path}")
    print(f"Saved walk-forward HTML report to {report_path}")
    print(f"\nSaved walk-forward window results to {output_path}")
    print(f"Saved walk-forward summary to {summary_path}")
    print(f"Saved walk-forward workflow summary to {workflow_summary_path}")
    if overall_summary_path is not None:
        print(f"Saved walk-forward overall universe summary to {overall_summary_path}")
