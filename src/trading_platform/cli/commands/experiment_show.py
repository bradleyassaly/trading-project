from __future__ import annotations

from pathlib import Path

from trading_platform.experiments.runner import load_experiment_run


def cmd_experiment_show(args) -> None:
    payload = load_experiment_run(Path(args.run))
    summary = payload.get("summary", {})
    print(f"Experiment: {payload.get('experiment_name')}")
    print(f"Run id: {payload.get('experiment_run_id')}")
    print(f"Status: {payload.get('status')}")
    print(f"Variants: {summary.get('variant_count', 0)}")
    print(f"Variant runs: {summary.get('variant_run_count', 0)}")
    print(f"Succeeded: {summary.get('succeeded_count', 0)}")
    print(f"Failed: {summary.get('failed_count', 0)}")
    for variant in payload.get("variants", []):
        print(
            f"- {variant.get('variant_name')}[r{variant.get('repeat_index', 1)}]: "
            f"status={variant.get('status')} run_dir={variant.get('run_dir') or 'n/a'}"
        )

