from __future__ import annotations

from pathlib import Path

from trading_platform.system_evaluation.service import compare_system_evaluations


def cmd_system_eval_compare(args) -> None:
    result = compare_system_evaluations(
        history_path_or_root=Path(args.history),
        output_dir=Path(args.output_dir),
        latest_count=args.latest_count,
        previous_count=args.previous_count,
        feature_flag=getattr(args, "feature_flag", None),
        value_a=getattr(args, "value_a", True),
        value_b=getattr(args, "value_b", False),
    )
    print(f"Group A count: {result['group_a_count']}")
    print(f"Group B count: {result['group_b_count']}")
    print(f"Comparison JSON: {result['system_evaluation_compare_json_path']}")
    print(f"Comparison Markdown: {result['system_evaluation_compare_md_path']}")
