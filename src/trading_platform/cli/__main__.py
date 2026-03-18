from __future__ import annotations
from pathlib import Path

import pandas as pd
import argparse

from trading_platform.backtests.engine import run_backtest, run_backtest_on_df
from trading_platform.cli.common import (
    UNIVERSES,
    add_symbol_arguments,
    print_symbol_list,
    resolve_symbols,
)
from trading_platform.data.ingest import ingest_symbol
from trading_platform.experiments.tracker import log_experiment
from trading_platform.features.build import build_features
from trading_platform.features.registry import DEFAULT_FEATURE_GROUPS, FEATURE_BUILDERS
from trading_platform.strategies.registry import STRATEGY_REGISTRY
from trading_platform.settings import FEATURES_DIR
from itertools import product
from trading_platform.experiments.reporting import (
    save_walkforward_html_report,
    save_walkforward_param_plot,
    save_walkforward_return_plot,
)


def add_shared_symbol_args(parser: argparse.ArgumentParser) -> None:
    add_symbol_arguments(parser)

def add_strategy_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--strategy",
        type=str,
        default="sma_cross",
        choices=sorted(STRATEGY_REGISTRY.keys()),
        help="Strategy to run",
    )

    parser.add_argument("--fast", type=int, default=20, help="Fast SMA window")
    parser.add_argument("--slow", type=int, default=100, help="Slow SMA window")

    parser.add_argument("--lookback", type=int, default=20, help="Momentum lookback")

    parser.add_argument("--cash", type=float, default=10_000, help="Starting cash")

    parser.add_argument(
        "--commission",
        type=float,
        default=0.001,
        help="Commission rate",
    )

def compound_return_pct(series: pd.Series) -> float:
    clean = series.dropna()
    if clean.empty:
        return float("nan")
    growth = (1 + clean / 100.0).prod()
    return (growth - 1) * 100.0

def compute_buy_and_hold_return_pct(df: pd.DataFrame) -> float:
    working = df.copy()

    if "Date" in working.columns:
        working["Date"] = pd.to_datetime(working["Date"])
        working = working.sort_values("Date")
    elif "timestamp" in working.columns:
        working["timestamp"] = pd.to_datetime(working["timestamp"])
        working = working.sort_values("timestamp")
    else:
        working = working.sort_index()

    rename_map = {}
    for col in working.columns:
        lower = str(col).lower()
        if lower == "close":
            rename_map[col] = "Close"

    working = working.rename(columns=rename_map)

    if "Close" not in working.columns:
        raise ValueError(f"Benchmark requires Close column. Available: {list(working.columns)}")

    close = working["Close"].dropna()
    if len(close) < 2:
        return float("nan")

    return (close.iloc[-1] / close.iloc[0] - 1.0) * 100.0

def cmd_ingest(args: argparse.Namespace) -> None:
    symbols = resolve_symbols(args)
    print(f"Ingesting {len(symbols)} symbol(s): {print_symbol_list(symbols)}")

    for symbol in symbols:
        path = ingest_symbol(symbol, start=args.start)
        print(f"[OK] {symbol}: saved raw data to {path}")

def cmd_list_strategies(args: argparse.Namespace) -> None:
    for name in sorted(STRATEGY_REGISTRY.keys()):
        print(name)

def cmd_features(args: argparse.Namespace) -> None:
    symbols = resolve_symbols(args)
    print(f"Building features for {len(symbols)} symbol(s): {print_symbol_list(symbols)}")

    for symbol in symbols:
        path = build_features(symbol, feature_groups=args.feature_groups)
        print(f"[OK] {symbol}: saved features to {path}")


def cmd_research(args: argparse.Namespace) -> None:
    symbols = resolve_symbols(args)
    print(f"Running research for {len(symbols)} symbol(s): {print_symbol_list(symbols)}")

    for symbol in symbols:
        stats = run_backtest(
            symbol=symbol,
            strategy=args.strategy,
            fast=args.fast,
            slow=args.slow,
            lookback=args.lookback,
            cash=args.cash,
            commission=args.commission,
        )
        exp_id = log_experiment(stats)

        ret = stats.get("Return [%]", "n/a")
        sharpe = stats.get("Sharpe Ratio", "n/a")
        max_dd = stats.get("Max. Drawdown [%]", "n/a")

        print(
            f"[OK] {symbol}: "
            f"fast={args.fast}, slow={args.slow}, cash={args.cash}, "
            f"commission={args.commission}, Return[%]={ret}, "
            f"Sharpe={sharpe}, MaxDD[%]={max_dd}, Experiment={exp_id}"
        )

def cmd_sweep(args: argparse.Namespace) -> None:
    symbols = resolve_symbols(args)
    results: list[dict[str, object]] = []

    print(
        f"Running {args.strategy} sweep for {len(symbols)} symbol(s): "
        f"{print_symbol_list(symbols)}"
    )

    if args.strategy == "sma_cross":
        if not args.fast_values or not args.slow_values:
            raise SystemExit(
                "sma_cross sweep requires --fast-values and --slow-values"
            )

        param_sets: list[dict[str, int]] = []
        for fast in args.fast_values:
            for slow in args.slow_values:
                if fast >= slow:
                    print(f"[SKIP] fast={fast} must be less than slow={slow}")
                    continue
                param_sets.append({"fast": fast, "slow": slow})

    elif args.strategy == "momentum_hold":
        if not args.lookback_values:
            raise SystemExit(
                "momentum_hold sweep requires --lookback-values"
            )

        param_sets = [{"lookback": lookback} for lookback in args.lookback_values]

    else:
        raise SystemExit(f"Unsupported sweep strategy: {args.strategy}")

    for symbol in symbols:
        for params in param_sets:
            try:
                stats = run_backtest(
                    symbol=symbol,
                    strategy=args.strategy,
                    fast=params.get("fast", 20),
                    slow=params.get("slow", 100),
                    lookback=params.get("lookback", 20),
                    cash=args.cash,
                    commission=args.commission,
                )
                exp_id = log_experiment(stats)

                row = {
                    "symbol": symbol,
                    "strategy": args.strategy,
                    "fast": params.get("fast"),
                    "slow": params.get("slow"),
                    "lookback": params.get("lookback"),
                    "cash": args.cash,
                    "commission": args.commission,
                    "return_pct": stats.get("Return [%]"),
                    "sharpe": stats.get("Sharpe Ratio"),
                    "max_drawdown_pct": stats.get("Max. Drawdown [%]"),
                    "experiment_id": exp_id,
                }
                results.append(row)

                print(
                    f"[OK] {symbol}: strategy={args.strategy}, "
                    f"fast={row['fast']}, slow={row['slow']}, "
                    f"lookback={row['lookback']}, "
                    f"Return[%]={row['return_pct']}, "
                    f"Sharpe={row['sharpe']}, "
                    f"MaxDD[%]={row['max_drawdown_pct']}, "
                    f"Experiment={exp_id}"
                )
            except Exception as e:
                print(
                    f"[ERROR] {symbol}: strategy={args.strategy}, "
                    f"params={params} -> {e}"
                )

    if not results:
        print("No successful sweep results.")
        return

    df = pd.DataFrame(results)
    sort_col = "sharpe" if "sharpe" in df.columns else "return_pct"
    df = df.sort_values(by=sort_col, ascending=False, na_position="last")

    print("\nTop 10 results:")
    print(df.head(10).to_string(index=False))

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)

    print(f"\nSaved sweep results to {output_path}")

def cmd_pipeline(args: argparse.Namespace) -> None:
    symbols = resolve_symbols(args)
    print(f"Running pipeline for {len(symbols)} symbol(s): {print_symbol_list(symbols)}")

    for symbol in symbols:
        raw_path = ingest_symbol(symbol, start=args.start)
        feat_path = build_features(symbol, feature_groups=args.feature_groups)
        stats = run_backtest(
            symbol=symbol,
            fast=args.fast,
            slow=args.slow,
            cash=args.cash,
            commission=args.commission,
        )
        exp_id = log_experiment(stats)

        ret = stats.get("Return [%]", "n/a")
        sharpe = stats.get("Sharpe Ratio", "n/a")
        max_dd = stats.get("Max. Drawdown [%]", "n/a")

        print(f"[OK] {symbol}")
        print(f"  raw: {raw_path}")
        print(f"  features: {feat_path}")
        print(f"  return[%]: {ret}")
        print(f"  sharpe: {sharpe}")
        print(f"  max drawdown[%]: {max_dd}")
        print(f"  experiment: {exp_id}")

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
        end_date = df[date_col].max()

        train_offset = pd.DateOffset(years=args.train_years)
        test_offset = pd.DateOffset(years=args.test_years)

        if args.strategy == "sma_cross":
            if not args.fast_values or not args.slow_values:
                raise SystemExit(
                    "walkforward with sma_cross requires --fast-values and --slow-values"
                )
            param_grid = [
                {"fast": fast, "slow": slow}
                for fast, slow in product(args.fast_values, args.slow_values)
                if fast < slow
            ]
        elif args.strategy == "momentum_hold":
            if not args.lookback_values:
                raise SystemExit(
                    "walkforward with momentum_hold requires --lookback-values"
                )
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

def cmd_list_universes(args: argparse.Namespace) -> None:
    for name, symbols in UNIVERSES.items():
        print(f"{name}: {', '.join(symbols)}")

def add_feature_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--feature-groups",
        nargs="+",
        default=DEFAULT_FEATURE_GROUPS,
        choices=sorted(FEATURE_BUILDERS.keys()),
        help="Feature groups to build",
    )



def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="trading_platform.cli",
        description="Trading platform command line interface",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest_parser = subparsers.add_parser("ingest", help="Download raw OHLCV data")
    add_shared_symbol_args(ingest_parser)
    ingest_parser.add_argument(
        "--start",
        type=str,
        default="2010-01-01",
        help="Start date in YYYY-MM-DD format (default: 2010-01-01)",
    )
    ingest_parser.set_defaults(func=cmd_ingest)

    features_parser = subparsers.add_parser("features", help="Build feature datasets")
    add_shared_symbol_args(features_parser)
    add_feature_arguments(features_parser)
    features_parser.set_defaults(func=cmd_features)

    research_parser = subparsers.add_parser(
        "research",
        help="Run backtests",
    )
    add_shared_symbol_args(research_parser)
    add_strategy_arguments(research_parser)
    research_parser.set_defaults(func=cmd_research)

    pipeline_parser = subparsers.add_parser(
        "pipeline",
        help="Run ingest -> features -> research in one command",
    )
    add_shared_symbol_args(pipeline_parser)
    add_feature_arguments(pipeline_parser)
    add_strategy_arguments(pipeline_parser)
    pipeline_parser.add_argument(
        "--start",
        type=str,
        default="2010-01-01",
        help="Start date in YYYY-MM-DD format (default: 2010-01-01)",
    )
    pipeline_parser.set_defaults(func=cmd_pipeline)


    universes_parser = subparsers.add_parser(
        "list-universes",
        help="Show available named universes",
    )
    universes_parser.set_defaults(func=cmd_list_universes)

    sweep_parser = subparsers.add_parser(
        "sweep",
        help="Run parameter sweep for a strategy",
    )
    add_shared_symbol_args(sweep_parser)

    sweep_parser.add_argument(
        "--strategy",
        type=str,
        default="sma_cross",
        choices=sorted(STRATEGY_REGISTRY.keys()),
        help="Strategy to sweep",
    )

    sweep_parser.add_argument(
        "--fast-values",
        type=int,
        nargs="+",
        help="One or more fast SMA windows",
    )
    sweep_parser.add_argument(
        "--slow-values",
        type=int,
        nargs="+",
        help="One or more slow SMA windows",
    )
    sweep_parser.add_argument(
        "--lookback-values",
        type=int,
        nargs="+",
        help="One or more momentum lookback windows",
    )

    sweep_parser.add_argument(
        "--cash",
        type=float,
        default=10_000,
        help="Starting cash",
    )
    sweep_parser.add_argument(
        "--commission",
        type=float,
        default=0.001,
        help="Commission rate as decimal (default: 0.001 = 10 bps)",
    )
    sweep_parser.add_argument(
        "--output",
        type=str,
        default="artifacts/experiments/sweep_results.csv",
        help="CSV output path for sweep summary",
    )
    sweep_parser.set_defaults(func=cmd_sweep)

    strategies_parser = subparsers.add_parser(
        "list-strategies",
        help="Show available strategies",
    )
    strategies_parser.set_defaults(func=cmd_list_strategies)

    walk_parser = subparsers.add_parser(
        "walkforward",
        help="Run walk-forward validation",
    )
    add_shared_symbol_args(walk_parser)

    walk_parser.add_argument(
        "--strategy",
        type=str,
        default="sma_cross",
        choices=sorted(STRATEGY_REGISTRY.keys()),
        help="Strategy to validate",
    )

    walk_parser.add_argument("--fast-values", type=int, nargs="+")
    walk_parser.add_argument("--slow-values", type=int, nargs="+")
    walk_parser.add_argument("--lookback-values", type=int, nargs="+")

    walk_parser.add_argument(
        "--train-years",
        type=int,
        default=5,
        help="Training window length in years",
    )
    walk_parser.add_argument(
        "--test-years",
        type=int,
        default=1,
        help="Test window length in years",
    )
    walk_parser.add_argument(
        "--min-train-rows",
        type=int,
        default=252,
        help="Minimum rows required in a train window",
    )
    walk_parser.add_argument(
        "--min-test-rows",
        type=int,
        default=126,
        help="Minimum rows required in a test window",
    )
    walk_parser.add_argument(
        "--select-by",
        type=str,
        default="Sharpe Ratio",
        choices=["Sharpe Ratio", "Return [%]"],
        help="Metric used to choose the best params on the train window",
    )
    walk_parser.add_argument("--cash", type=float, default=10_000)
    walk_parser.add_argument("--commission", type=float, default=0.001)
    walk_parser.add_argument(
        "--output",
        type=str,
        default="artifacts/experiments/walkforward_results.csv",
        help="CSV output path",
    )
    walk_parser.set_defaults(func=cmd_walkforward)

    return parser

def add_research_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--fast", type=int, default=20, help="Fast SMA window")
    parser.add_argument("--slow", type=int, default=100, help="Slow SMA window")
    parser.add_argument("--cash", type=float, default=10_000, help="Starting cash")
    parser.add_argument(
        "--commission",
        type=float,
        default=0.001,
        help="Commission rate as decimal (default: 0.001 = 10 bps)",
    )


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if not hasattr(args, "func"):
        parser.print_help()
        raise SystemExit(2)

    args.func(args)

if __name__ == "__main__":
    main()