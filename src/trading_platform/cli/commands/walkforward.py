from __future__ import annotations

import argparse
from itertools import product
from pathlib import Path

import pandas as pd

from trading_platform.backtests.engine import run_backtest_on_df
from trading_platform.cli.common import (
    compound_return_pct,
    compute_buy_and_hold_return_pct,
    print_symbol_list,
    resolve_symbols,
)
from trading_platform.experiments.reporting import (
    save_walkforward_html_report,
    save_walkforward_param_plot,
    save_walkforward_return_plot,
)
from trading_platform.settings import FEATURES_DIR


def cmd_walkforward(args: argparse.Namespace) -> None:
    symbols = resolve_symbols(args)
    results: list[dict[str, object]] = []

    print(
        f"Running walk-forward for {len(symbols)} symbol(s): "
        f"{print_symbol_list(symbols)}"
    )

    for symbol in symbols:
        path = FEATURES_DIR / f"{symbol}.parquet"
        if not path.exists():
            print(f"[ERROR] {symbol}: feature file not found at {path}")
            continue

        df = pd.read_parquet(path)

        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"])
            date_col = "Date"
        elif "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            date_col = "timestamp"
        else:
            df = df.copy()
            df.index = pd.to_datetime(df.index)
            df = df.reset_index().rename(columns={"index": "Date"})
            date_col = "Date"

        df = df.sort_values(date_col).reset_index(drop=True)

        start_date = df[date_col].min()
        train_offset = pd.DateOffset(years=args.train_years)
        test_offset = pd.DateOffset(years=args.test_years)

        if args.strategy == "sma_cross":
            if not args.fast_values or not args.slow_values:
                raise SystemExit("walkforward with sma_cross requires --fast-values and --slow-values")
            param_grid = [
                {"fast": fast, "slow": slow}
                for fast, slow in product(args.fast_values, args.slow_values)
                if fast < slow
            ]
        elif args.strategy == "momentum_hold":
            if not args.lookback_values:
                raise SystemExit("walkforward with momentum_hold requires --lookback-values")
            param_grid = [{"lookback": lb} for lb in args.lookback_values]
        else:
            raise SystemExit(f"Unsupported strategy for walkforward: {args.strategy}")

        current_train_start = start_date

        while True:
            train_end = current_train_start + train_offset
            test_end = train_end + test_offset

            train_df = df[(df[date_col] >= current_train_start) & (df[date_col] < train_end)]
            test_df = df[(df[date_col] >= train_end) & (df[date_col] < test_end)]

            if len(train_df) < args.min_train_rows or len(test_df) < args.min_test_rows:
                break

            best_train = None
            best_train_score = None

            for params in param_grid:
                try:
                    train_stats = run_backtest_on_df(
                        df=train_df,
                        symbol=symbol,
                        strategy=args.strategy,
                        fast=params.get("fast", 20),
                        slow=params.get("slow", 100),
                        lookback=params.get("lookback", 20),
                        cash=args.cash,
                        commission=args.commission,
                    )
                    score = train_stats.get(args.select_by)

                    if score is None or pd.isna(score):
                        continue

                    if best_train_score is None or score > best_train_score:
                        best_train_score = score
                        best_train = {
                            "params": params,
                            "train_stats": train_stats,
                        }
                except Exception as e:
                    print(
                        f"[WARN] {symbol}: train window "
                        f"{current_train_start.date()} -> {train_end.date()} "
                        f"params={params} failed: {e}"
                    )

            if best_train is None:
                print(
                    f"[WARN] {symbol}: no valid params found for train window "
                    f"{current_train_start.date()} -> {train_end.date()}"
                )
                current_train_start = current_train_start + test_offset
                continue

            selected_params = best_train["params"]

            try:
                test_stats = run_backtest_on_df(
                    df=test_df,
                    symbol=symbol,
                    strategy=args.strategy,
                    fast=selected_params.get("fast", 20),
                    slow=selected_params.get("slow", 100),
                    lookback=selected_params.get("lookback", 20),
                    cash=args.cash,
                    commission=args.commission,
                )
            except Exception as e:
                print(
                    f"[WARN] {symbol}: test window "
                    f"{train_end.date()} -> {test_end.date()} failed: {e}"
                )
                current_train_start = current_train_start + test_offset
                continue

            benchmark_return_pct = compute_buy_and_hold_return_pct(test_df)
            test_return_pct = test_stats.get("Return [%]")

            if test_return_pct is not None and not pd.isna(test_return_pct) and not pd.isna(benchmark_return_pct):
                excess_return_pct = test_return_pct - benchmark_return_pct
            else:
                excess_return_pct = float("nan")

            row = {
                "symbol": symbol,
                "strategy": args.strategy,
                "train_start": current_train_start.date().isoformat(),
                "train_end": train_end.date().isoformat(),
                "test_start": train_end.date().isoformat(),
                "test_end": test_end.date().isoformat(),
                "selected_by": args.select_by,
                "selected_train_score": best_train_score,
                "fast": selected_params.get("fast"),
                "slow": selected_params.get("slow"),
                "lookback": selected_params.get("lookback"),
                "train_return_pct": best_train["train_stats"].get("Return [%]"),
                "train_sharpe": best_train["train_stats"].get("Sharpe Ratio"),
                "train_max_drawdown_pct": best_train["train_stats"].get("Max. Drawdown [%]"),
                "test_return_pct": test_return_pct,
                "test_sharpe": test_stats.get("Sharpe Ratio"),
                "test_max_drawdown_pct": test_stats.get("Max. Drawdown [%]"),
                "benchmark_return_pct": benchmark_return_pct,
                "excess_return_pct": excess_return_pct,
            }
            results.append(row)

            print(
                f"[OK] {symbol}: "
                f"train {row['train_start']}->{row['train_end']} | "
                f"test {row['test_start']}->{row['test_end']} | "
                f"params fast={row['fast']} slow={row['slow']} lookback={row['lookback']} | "
                f"test Return[%]={row['test_return_pct']} | "
                f"benchmark Return[%]={row['benchmark_return_pct']} | "
                f"excess Return[%]={row['excess_return_pct']} | "
                f"test Sharpe={row['test_sharpe']}"
            )
            current_train_start = current_train_start + test_offset

    if not results:
        print("No walk-forward results generated.")
        return

    out_df = pd.DataFrame(results)

    print("\nWalk-forward window results:")
    print(out_df.to_string(index=False))

    summary_rows: list[dict[str, object]] = []

    for symbol in sorted(out_df["symbol"].dropna().unique()):
        symbol_df = out_df[out_df["symbol"] == symbol].copy()

        row: dict[str, object] = {
            "symbol": symbol,
            "strategy": args.strategy,
            "windows": len(symbol_df),
            "avg_test_return_pct": symbol_df["test_return_pct"].mean(),
            "median_test_return_pct": symbol_df["test_return_pct"].median(),
            "compounded_test_return_pct": compound_return_pct(symbol_df["test_return_pct"]),
            "avg_benchmark_return_pct": symbol_df["benchmark_return_pct"].mean(),
            "median_benchmark_return_pct": symbol_df["benchmark_return_pct"].median(),
            "compounded_benchmark_return_pct": compound_return_pct(symbol_df["benchmark_return_pct"]),
            "avg_excess_return_pct": symbol_df["excess_return_pct"].mean(),
            "median_excess_return_pct": symbol_df["excess_return_pct"].median(),
            "compounded_excess_return_pct": (
                compound_return_pct(symbol_df["test_return_pct"])
                - compound_return_pct(symbol_df["benchmark_return_pct"])
            ),
            "avg_test_sharpe": symbol_df["test_sharpe"].mean(),
            "median_test_sharpe": symbol_df["test_sharpe"].median(),
            "worst_test_max_drawdown_pct": symbol_df["test_max_drawdown_pct"].min(),
        }

        if args.strategy == "sma_cross":
            param_counts = (
                symbol_df.groupby(["fast", "slow"])
                .size()
                .reset_index(name="count")
                .sort_values(["count", "fast", "slow"], ascending=[False, True, True])
            )
            if not param_counts.empty:
                best_params = param_counts.iloc[0]
                row["most_selected_fast"] = best_params["fast"]
                row["most_selected_slow"] = best_params["slow"]
                row["most_selected_count"] = best_params["count"]

        elif args.strategy == "momentum_hold":
            param_counts = (
                symbol_df.groupby(["lookback"])
                .size()
                .reset_index(name="count")
                .sort_values(["count", "lookback"], ascending=[False, True])
            )
            if not param_counts.empty:
                best_params = param_counts.iloc[0]
                row["most_selected_lookback"] = best_params["lookback"]
                row["most_selected_count"] = best_params["count"]

        summary_rows.append(row)

    summary_df = pd.DataFrame(summary_rows)

    print("\nWalk-forward aggregate summary:")
    print(summary_df.to_string(index=False))

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(output_path, index=False)

    summary_path = output_path.with_name(output_path.stem + "_summary.csv")
    summary_df.to_csv(summary_path, index=False)

    returns_plot_path = save_walkforward_return_plot(out_df, output_path)
    params_plot_path = save_walkforward_param_plot(out_df, output_path)
    report_path = save_walkforward_html_report(
        window_df=out_df,
        summary_df=summary_df,
        output_path=output_path,
        returns_plot_path=returns_plot_path,
        params_plot_path=params_plot_path,
    )

    print(f"Saved walk-forward returns plot to {returns_plot_path}")
    if params_plot_path is not None:
        print(f"Saved walk-forward parameter plot to {params_plot_path}")
    print(f"Saved walk-forward HTML report to {report_path}")

    print(f"\nSaved walk-forward window results to {output_path}")
    print(f"Saved walk-forward summary to {summary_path}")