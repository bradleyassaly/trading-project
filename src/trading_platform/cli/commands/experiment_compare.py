from __future__ import annotations

from pathlib import Path

from trading_platform.experiments.runner import compare_experiment_variants


def cmd_experiment_compare(args) -> None:
    result = compare_experiment_variants(
        experiment_run_path=Path(args.run),
        output_dir=Path(args.output_dir),
        variant_a=getattr(args, "variant_a", None),
        variant_b=getattr(args, "variant_b", None),
    )
    print(f"Group A count: {result['group_a_count']}")
    print(f"Group B count: {result['group_b_count']}")
    print(f"Comparison JSON: {result['system_evaluation_compare_json_path']}")
    print(f"Comparison Markdown: {result['system_evaluation_compare_md_path']}")
