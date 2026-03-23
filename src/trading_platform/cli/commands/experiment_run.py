from __future__ import annotations

from pathlib import Path

from trading_platform.config.loader import load_experiment_spec_config
from trading_platform.experiments.runner import run_experiment


def cmd_experiment_run(args) -> None:
    spec = load_experiment_spec_config(Path(args.config))
    result = run_experiment(
        spec=spec,
        selected_variants=getattr(args, "variants", None),
        dry_run=bool(getattr(args, "dry_run", False)),
    )
    print(f"Experiment: {result['experiment_name']}")
    print(f"Run id: {result['experiment_run_id']}")
    print(f"Status: {result['status']}")
    print(f"Variant runs: {result['variant_run_count']}")
    print(f"Run dir: {result['run_dir']}")
    for key, value in sorted(result["artifact_paths"].items()):
        print(f"{key}: {value}")

