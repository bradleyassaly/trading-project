from __future__ import annotations

import argparse

from trading_platform.config.loader import load_walk_forward_config
from trading_platform.services.walk_forward_artifact_service import (
    make_walk_forward_artifact_stem,
    save_walk_forward_summary_json,
    save_walk_forward_windows_csv,
)
from trading_platform.services.walk_forward_service import run_walk_forward_evaluation


def cmd_run_walk_forward(args: argparse.Namespace) -> None:
    config = load_walk_forward_config(args.config)

    print(f"Running walk-forward evaluation from config: {args.config}")
    print(f"Symbol: {config.symbol}")
    print(f"Strategy: {config.strategy}")

    outputs = run_walk_forward_evaluation(config=config)

    artifact_stem = make_walk_forward_artifact_stem()

    windows_path = save_walk_forward_windows_csv(
        results_df=outputs["results_df"],
        stem=artifact_stem,
    )

    summary_payload = {
        "config": outputs["config"],
        "feature_path": outputs["feature_path"],
        "prep_experiment_id": outputs["prep_experiment_id"],
        "summary": outputs["summary"],
        "windows_csv_path": str(windows_path),
    }

    summary_path = save_walk_forward_summary_json(
        payload=summary_payload,
        stem=artifact_stem,
    )

    print("Walk-forward summary:")
    for key, value in outputs["summary"].items():
        print(f"  {key}: {value}")

    print(f"Windows CSV: {windows_path}")
    print(f"Summary JSON: {summary_path}")