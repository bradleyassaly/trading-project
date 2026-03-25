from __future__ import annotations

from trading_platform.config.loader import load_canonical_bundle_experiment_workflow_config
from trading_platform.portfolio.canonical_bundle_experiment import run_canonical_bundle_experiment


def cmd_strategy_portfolio_experiment_bundle(args) -> None:
    config = load_canonical_bundle_experiment_workflow_config(args.config)
    result = run_canonical_bundle_experiment(config)
    print(f"Baseline bundle: {result['baseline_bundle_path']}")
    print(f"Experiment output dir: {result['output_dir']}")
    print(f"Experiment summary JSON: {result['experiment_summary_json_path']}")
    print(f"Experiment results CSV: {result['experiment_variant_results_csv_path']}")
    print(f"Policy comparison CSV: {result['experiment_policy_comparison_csv_path']}")
    for row in result["variant_rows"]:
        print(
            f"- {row['variant_name']}: promoted={row['promoted_strategy_count']} "
            f"selected={row['selected_strategy_count']} "
            f"weighting={row['portfolio_weighting_mode']} "
            f"paper_ready={row['paper_ready']} live_ready={row['live_ready']}"
        )
