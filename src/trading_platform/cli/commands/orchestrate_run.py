from __future__ import annotations

from trading_platform.config.loader import load_automated_orchestration_config
from trading_platform.orchestration.pipeline_runner import (
    orchestration_loop,
    run_automated_orchestration,
)


def _print_orchestration_summary(result, artifact_paths) -> None:
    print(f"Run id: {result.run_id}")
    print(f"Run name: {result.run_name}")
    print(f"Schedule: {result.schedule_frequency}")
    print(f"Status: {result.status}")
    print("Stages:")
    for record in result.stage_records:
        duration = f"{record.duration_seconds:.6f}" if record.duration_seconds is not None else "n/a"
        print(f"  {record.stage_name}: {record.status} ({duration}s)")
        if record.warnings:
            print(f"    warnings: {', '.join(record.warnings)}")
        if record.error_message:
            print(f"    error: {record.error_message}")
    print("Artifacts:")
    for name, path in sorted(artifact_paths.items()):
        print(f"  {name}: {path}")


def cmd_orchestrate_run(args) -> None:
    config = load_automated_orchestration_config(args.config)
    result, artifact_paths = run_automated_orchestration(config)
    _print_orchestration_summary(result, artifact_paths)
    if result.status == "failed":
        raise SystemExit(1)


def cmd_orchestrate_loop(args) -> None:
    config = load_automated_orchestration_config(args.config)
    iterations = orchestration_loop(
        config=config,
        max_iterations=getattr(args, "max_iterations", None),
    )
    for row in iterations:
        print(f"{row['run_id']}: status={row['status']} run_dir={row['run_dir']}")
    if any(row["status"] == "failed" for row in iterations):
        raise SystemExit(1)
