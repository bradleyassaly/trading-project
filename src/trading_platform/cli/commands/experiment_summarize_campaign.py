from __future__ import annotations

from pathlib import Path

from trading_platform.experiments.campaign import build_experiment_campaign_summary


def cmd_experiment_summarize_campaign(args) -> None:
    result = build_experiment_campaign_summary(
        experiment_runs=[Path(path) for path in args.runs],
        output_dir=Path(args.output_dir),
    )
    print(f"Variants summarized: {result['variant_count']}")
    for metric, names in sorted(result["winners"].items()):
        winner_text = ", ".join(names) if names else "n/a"
        print(f"{metric}: {winner_text}")
    print(f"Campaign JSON: {result['experiment_campaign_summary_json_path']}")
    print(f"Campaign CSV: {result['experiment_campaign_summary_csv_path']}")
    print(f"Campaign Markdown: {result['experiment_campaign_summary_md_path']}")
