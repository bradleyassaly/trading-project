from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from trading_platform.backtests.engine import run_backtest
from trading_platform.cli.common import print_symbol_list, resolve_symbols
from trading_platform.experiments.tracker import log_experiment


def cmd_sweep(args: argparse.Namespace) -> None:
    symbols = resolve_symbols(args)
    results: list[dict[str, object]] = []

    print(
        f"Running {args.strategy} sweep for {len(symbols)} symbol(s): "
        f"{print_symbol_list(symbols)}"
    )

    if args.strategy == "sma_cross":
        if not args.fast_values or not args.slow_values:
            raise SystemExit("sma_cross sweep requires --fast-values and --slow-values")

        param_sets: list[dict[str, int]] = []
        for fast in args.fast_values:
            for slow in args.slow_values:
                if fast >= slow:
                    print(f"[SKIP] fast={fast} must be less than slow={slow}")
                    continue
                param_sets.append({"fast": fast, "slow": slow})

    elif args.strategy == "momentum_hold":
        if not args.lookback_values:
            raise SystemExit("momentum_hold sweep requires --lookback-values")

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