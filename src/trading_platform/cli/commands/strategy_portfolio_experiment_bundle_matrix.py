from __future__ import annotations

from trading_platform.config.loader import load_canonical_bundle_experiment_matrix_workflow_config
from trading_platform.portfolio.canonical_bundle_experiment import run_canonical_bundle_experiment_matrix


def cmd_strategy_portfolio_experiment_bundle_matrix(args) -> None:
    config = load_canonical_bundle_experiment_matrix_workflow_config(args.config)
    result = run_canonical_bundle_experiment_matrix(config)
    print(f"Experiment output dir: {result['output_dir']}")
    print(f"Case results JSON: {result['bundle_case_results_json_path']}")
    print(f"Time stability CSV: {result['experiment_time_stability_csv_path']}")
    print(f"Time stability JSON: {result['experiment_time_stability_json_path']}")
    print(f"Stability summary JSON: {result['bundle_policy_stability_summary_json_path']}")
    for row in result["variant_stability"]:
        print(
            f"- {row['variant_name']}: cases={row['case_count']} "
            f"paper_ready={row['paper_ready_pass_count']} "
            f"live_ready={row['live_ready_pass_count']} "
            f"allocation_l1_range={row['allocation_l1_delta_vs_baseline_range']:.4f}"
        )
