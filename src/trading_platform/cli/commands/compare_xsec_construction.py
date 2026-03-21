from __future__ import annotations

import argparse
from copy import deepcopy
from pathlib import Path

import pandas as pd

from trading_platform.cli.common import print_symbol_list, resolve_symbols
from trading_platform.cli.commands.walkforward import (
    _build_param_grid,
    _resolve_window_spec,
    run_xsec_walkforward_analysis,
)
from trading_platform.cli.presets import apply_cli_preset
from trading_platform.experiments.reporting import save_xsec_construction_comparison_html_report


SUMMARY_COLUMNS = [
    "portfolio_construction_mode",
    "avg_test_return_pct",
    "avg_test_gross_return_pct",
    "avg_test_net_return_pct",
    "avg_test_cost_drag_return_pct",
    "avg_excess_return_pct",
    "percent_positive_windows",
    "worst_excess_return_pct",
    "best_excess_return_pct",
    "avg_test_sharpe",
    "worst_test_max_drawdown_pct",
    "total_trade_count",
    "mean_turnover",
    "mean_annualized_turnover",
    "mean_average_number_of_holdings",
    "mean_average_target_selected_count",
    "mean_average_realized_holdings_count",
    "percent_windows_ended_in_cash",
    "mean_average_liquidity_excluded_symbols",
    "total_turnover_cap_binding_count",
    "total_turnover_buffer_blocked_replacements",
]

WINDOW_ID_COLUMNS = [
    "window_index",
    "train_start",
    "train_end",
    "test_start",
    "test_end",
]

WINDOW_COMPARE_COLUMNS = [
    "lookback_bars",
    "skip_bars",
    "top_n",
    "rebalance_bars",
    "benchmark_return_pct",
    "excess_return_pct",
    "test_return_pct",
    "test_gross_return_pct",
    "test_net_return_pct",
    "test_cost_drag_return_pct",
    "test_sharpe",
    "test_max_drawdown_pct",
    "mean_turnover",
    "average_number_of_holdings",
    "target_selected_count",
    "realized_holdings_count",
]

WINDOW_COMPARE_ALIASES = {
    "average_number_of_holdings": [
        "average_number_of_holdings",
        "average_realized_holdings_count",
        "realized_holdings_count",
    ],
    "target_selected_count": [
        "target_selected_count",
        "average_target_selected_count",
        "top_n",
    ],
    "realized_holdings_count": [
        "realized_holdings_count",
        "average_realized_holdings_count",
        "average_number_of_holdings",
    ],
}


def _mode_args(args: argparse.Namespace, mode: str) -> argparse.Namespace:
    mode_args = deepcopy(args)
    mode_args.portfolio_construction_mode = mode
    return mode_args


def _merge_window_comparison(
    pure_df: pd.DataFrame,
    transition_df: pd.DataFrame,
) -> pd.DataFrame:
    pure = _normalize_window_schema(pure_df)
    transition = _normalize_window_schema(transition_df)
    pure = pure.rename(columns={col: f"pure_topn_{col}" for col in WINDOW_COMPARE_COLUMNS})
    transition = transition.rename(columns={col: f"transition_{col}" for col in WINDOW_COMPARE_COLUMNS})
    merged = pure.merge(transition, on=WINDOW_ID_COLUMNS, how="outer")
    merged["delta_test_return_pct"] = merged["transition_test_return_pct"] - merged["pure_topn_test_return_pct"]
    merged["delta_excess_return_pct"] = merged["transition_excess_return_pct"] - merged["pure_topn_excess_return_pct"]
    merged["delta_test_sharpe"] = merged["transition_test_sharpe"] - merged["pure_topn_test_sharpe"]
    merged["delta_test_max_drawdown_pct"] = merged["transition_test_max_drawdown_pct"] - merged["pure_topn_test_max_drawdown_pct"]
    merged["delta_mean_turnover"] = merged["transition_mean_turnover"] - merged["pure_topn_mean_turnover"]
    merged["delta_average_realized_holdings_count"] = (
        merged["transition_realized_holdings_count"] - merged["pure_topn_realized_holdings_count"]
    )
    return merged


def _resolve_window_column(df: pd.DataFrame, canonical_name: str) -> pd.Series:
    if canonical_name in df.columns:
        return df[canonical_name]
    for alias in WINDOW_COMPARE_ALIASES.get(canonical_name, []):
        if alias in df.columns:
            return df[alias]
    alias_list = WINDOW_COMPARE_ALIASES.get(canonical_name)
    if alias_list:
        raise SystemExit(
            f"compare-xsec-construction requires one of {alias_list} for '{canonical_name}' in walk-forward results"
        )
    raise SystemExit(
        f"compare-xsec-construction requires '{canonical_name}' in walk-forward results"
    )


def _normalize_window_schema(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    for canonical_name in WINDOW_COMPARE_COLUMNS:
        if canonical_name in normalized.columns:
            continue
        normalized[canonical_name] = _resolve_window_column(normalized, canonical_name)
    missing_id_cols = [column for column in WINDOW_ID_COLUMNS if column not in normalized.columns]
    if missing_id_cols:
        raise SystemExit(
            "compare-xsec-construction requires window identifier columns in walk-forward results: "
            + ", ".join(missing_id_cols)
        )
    return normalized[WINDOW_ID_COLUMNS + WINDOW_COMPARE_COLUMNS].copy()


def _build_summary_rows(
    pure_summary_df: pd.DataFrame,
    transition_summary_df: pd.DataFrame,
) -> pd.DataFrame:
    combined = pd.concat([pure_summary_df, transition_summary_df], ignore_index=True)
    for column in SUMMARY_COLUMNS:
        if column not in combined.columns:
            combined[column] = pd.NA
    return combined[SUMMARY_COLUMNS].copy()


def _print_compact_summary(summary_df: pd.DataFrame) -> None:
    display = summary_df[
        [
            "portfolio_construction_mode",
            "avg_test_return_pct",
            "avg_excess_return_pct",
            "percent_positive_windows",
            "worst_excess_return_pct",
            "avg_test_sharpe",
            "worst_test_max_drawdown_pct",
            "total_trade_count",
            "mean_turnover",
            "mean_average_number_of_holdings",
        ]
    ].copy()
    display = display.rename(
        columns={
            "portfolio_construction_mode": "Mode",
            "avg_test_return_pct": "AvgRet",
            "avg_excess_return_pct": "AvgExcess",
            "percent_positive_windows": "PosWin%",
            "worst_excess_return_pct": "WorstExcess",
            "avg_test_sharpe": "AvgSharpe",
            "worst_test_max_drawdown_pct": "WorstDD",
            "total_trade_count": "Trades",
            "mean_turnover": "AvgTurnover",
            "mean_average_number_of_holdings": "AvgHoldings",
        }
    )
    print("\nConstruction Comparison")
    print(display.to_string(index=False))

    indexed = summary_df.set_index("portfolio_construction_mode")
    pure = indexed.loc["pure_topn"]
    transition = indexed.loc["transition"]
    better_avg_excess = "pure_topn" if float(pure["avg_excess_return_pct"]) >= float(transition["avg_excess_return_pct"]) else "transition"
    lower_turnover = "pure_topn" if float(pure["mean_turnover"]) <= float(transition["mean_turnover"]) else "transition"
    lower_drawdown = "pure_topn" if float(pure["worst_test_max_drawdown_pct"]) >= float(transition["worst_test_max_drawdown_pct"]) else "transition"
    lower_drift = (
        "pure_topn"
        if float(pure["mean_average_realized_holdings_count"]) <= float(transition["mean_average_realized_holdings_count"])
        else "transition"
    )
    print("\nDelta Summary")
    print(f"- better avg excess return: {better_avg_excess}")
    print(f"- lower turnover: {lower_turnover}")
    print(f"- lower drawdown: {lower_drawdown}")
    print(f"- realized holdings drift difference: {lower_drift}")


def cmd_compare_xsec_construction(args: argparse.Namespace) -> None:
    apply_cli_preset(args)
    if args.strategy != "xsec_momentum_topn":
        raise SystemExit("compare-xsec-construction currently supports only --strategy xsec_momentum_topn")

    symbols = resolve_symbols(args)
    param_grid, invalid_warnings = _build_param_grid(args)
    window_spec = _resolve_window_spec(args)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(
        f"Comparing xsec construction modes for {len(symbols)} symbol(s): {print_symbol_list(symbols)} | "
        f"strategy={args.strategy} | requested_range={args.start or 'full'}->{args.end or 'full'} | "
        f"param_combinations={len(param_grid)}"
    )
    for warning in invalid_warnings:
        print(f"[SKIP] {warning}")

    pure_window_df, pure_summary_df, _ = run_xsec_walkforward_analysis(
        _mode_args(args, "pure_topn"),
        symbols=symbols,
        param_grid=param_grid,
        window_spec=window_spec,
        verbose=False,
    )
    transition_window_df, transition_summary_df, _ = run_xsec_walkforward_analysis(
        _mode_args(args, "transition"),
        symbols=symbols,
        param_grid=param_grid,
        window_spec=window_spec,
        verbose=False,
    )

    comparison_window_df = _merge_window_comparison(
        pure_window_df[pure_window_df["window_status"] == "completed"].copy(),
        transition_window_df[transition_window_df["window_status"] == "completed"].copy(),
    )
    comparison_summary_df = _build_summary_rows(pure_summary_df, transition_summary_df)

    window_output_path = output_dir / "compare_xsec_construction_windows.csv"
    summary_output_path = output_dir / "compare_xsec_construction_summary.csv"
    report_output_path = output_dir / "compare_xsec_construction.html"
    comparison_window_df.to_csv(window_output_path, index=False)
    comparison_summary_df.to_csv(summary_output_path, index=False)
    report_path = save_xsec_construction_comparison_html_report(
        comparison_summary_df=comparison_summary_df,
        comparison_window_df=comparison_window_df,
        output_path=report_output_path,
        config_items=[
            ("universe", getattr(args, "universe", None) or ",".join(symbols)),
            ("strategy", args.strategy),
            ("lookback-bars-values", args.lookback_bars_values),
            ("skip-bars-values", args.skip_bars_values),
            ("top-n-values", args.top_n_values),
            ("rebalance-bars-values", args.rebalance_bars_values),
            ("start", args.start),
            ("end", args.end),
            ("train-bars", args.train_bars),
            ("test-bars", args.test_bars),
            ("step-bars", args.step_bars),
            ("cost-bps", args.cost_bps),
            ("benchmark", args.benchmark),
        ],
    )

    _print_compact_summary(comparison_summary_df)
    print(f"\nSaved comparison windows to {window_output_path}")
    print(f"Saved comparison summary to {summary_output_path}")
    print(f"Saved comparison report to {report_path}")
