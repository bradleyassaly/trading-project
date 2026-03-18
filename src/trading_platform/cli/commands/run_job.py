from __future__ import annotations

import argparse

from trading_platform.config.loader import load_research_workflow_config
from trading_platform.services.job_artifact_service import (
    build_job_summary,
    make_job_artifact_stem,
    save_job_summary,
    save_leaderboard_csv,
)
from trading_platform.services.universe_research_service import (
    run_universe_research_workflow,
)
from trading_platform.services.universe_summary_service import (
    build_universe_leaderboard,
)


def cmd_run_job(args: argparse.Namespace) -> None:
    config = load_research_workflow_config(args.config)

    symbols = args.symbols or [config.symbol]

    print(f"Running job from config: {args.config}")
    print(f"Symbols: {', '.join(symbols)}")

    outputs = run_universe_research_workflow(
        symbols=symbols,
        base_config=config,
        continue_on_error=not args.fail_fast,
    )

    for symbol, result in outputs["results"].items():
        stats = result["stats"]
        ret = stats.get("Return [%]", "n/a")
        sharpe = stats.get("Sharpe Ratio", "n/a")
        max_dd = stats.get("Max. Drawdown [%]", "n/a")

        print(f"[OK] {symbol}")
        print(f"  normalized: {result['normalized_path']}")
        print(f"  features: {result['features_path']}")
        print(f"  return[%]: {ret}")
        print(f"  sharpe: {sharpe}")
        print(f"  max drawdown[%]: {max_dd}")
        print(f"  experiment: {result['experiment_id']}")

    for symbol, error in outputs["errors"].items():
        print(f"[ERROR] {symbol}: {error}")

    artifact_stem = make_job_artifact_stem()

    leaderboard = build_universe_leaderboard(outputs)
    leaderboard_path = save_leaderboard_csv(
        leaderboard=leaderboard,
        stem=artifact_stem,
    )

    summary = build_job_summary(
        config=config,
        symbols=symbols,
        outputs=outputs,
        leaderboard_csv_path=str(leaderboard_path),
    )
    summary_path = save_job_summary(
        summary=summary,
        stem=artifact_stem,
    )

    print(f"Leaderboard: {leaderboard_path}")
    print(f"Job summary: {summary_path}")

    if outputs["errors"] and args.fail_fast:
        raise SystemExit(1)