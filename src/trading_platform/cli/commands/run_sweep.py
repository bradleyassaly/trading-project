from __future__ import annotations

import argparse

from trading_platform.config.loader import load_parameter_sweep_config
from trading_platform.services.parameter_sweep_service import run_parameter_sweep
from trading_platform.services.sweep_artifact_service import (
    make_sweep_artifact_stem,
    save_sweep_leaderboard_csv,
    save_sweep_summary_json,
)


def cmd_run_sweep(args: argparse.Namespace) -> None:
    config = load_parameter_sweep_config(args.config)

    print(f"Running sweep from config: {args.config}")
    print(f"Symbol: {config.symbol}")
    print(f"Strategy: {config.strategy}")

    outputs = run_parameter_sweep(
        config=config,
        continue_on_error=not args.fail_fast,
    )

    leaderboard = outputs["leaderboard"]
    artifact_stem = make_sweep_artifact_stem()

    leaderboard_path = save_sweep_leaderboard_csv(
        leaderboard=leaderboard,
        stem=artifact_stem,
    )

    summary_payload = {
        "config": outputs["config"],
        "result_count": len(outputs["results"]),
        "error_count": len(outputs["errors"]),
        "errors": outputs["errors"],
        "leaderboard_csv_path": str(leaderboard_path),
    }

    summary_path = save_sweep_summary_json(
        payload=summary_payload,
        stem=artifact_stem,
    )

    if leaderboard.empty:
        print("No successful sweep results.")
    else:
        print("Top sweep results:")
        top_n = min(5, len(leaderboard))
        for i in range(top_n):
            row = leaderboard.iloc[i]
            print(
                f"  [{i + 1}] "
                f"fast={row.get('fast')} "
                f"slow={row.get('slow')} "
                f"lookback={row.get('lookback')} "
                f"return[%]={row.get('return_pct')} "
                f"sharpe={row.get('sharpe_ratio')} "
                f"max_dd[%]={row.get('max_drawdown_pct')} "
                f"experiment={row.get('experiment_id')}"
            )

    if outputs["errors"]:
        print("Sweep errors:")
        for error in outputs["errors"]:
            print(f"  {error}")

    print(f"Leaderboard: {leaderboard_path}")
    print(f"Sweep summary: {summary_path}")

    if outputs["errors"] and args.fail_fast:
        raise SystemExit(1)