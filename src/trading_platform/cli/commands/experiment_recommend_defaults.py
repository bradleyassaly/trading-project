from __future__ import annotations

from pathlib import Path

from trading_platform.experiments.decision_support import recommend_experiment_defaults


def cmd_experiment_recommend_defaults(args) -> None:
    result = recommend_experiment_defaults(
        campaign_summary_path=Path(args.summary),
        output_dir=Path(args.output_dir),
        write_config_path=Path(args.write_config) if getattr(args, "write_config", None) else None,
        base_config_path=Path(args.base_config) if getattr(args, "base_config", None) else None,
    )
    for decision in result["decisions"]:
        print(
            f"{decision['campaign_name']}: "
            f"{decision['overall_recommended_default']} "
            f"(confidence={decision['confidence_level']})"
        )
    print(f"Decision JSON: {result['experiment_decision_summary_json_path']}")
    print(f"Decision CSV: {result['experiment_decision_summary_csv_path']}")
    print(f"Decision Markdown: {result['experiment_decision_summary_md_path']}")
    if result.get("recommended_config_path"):
        print(f"Recommended config: {result['recommended_config_path']}")
