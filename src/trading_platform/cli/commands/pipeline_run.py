from __future__ import annotations

from trading_platform.config.loader import load_pipeline_run_config
from trading_platform.orchestration.service import run_orchestration_pipeline


def _print_stage_summary(result, artifact_paths) -> None:
    print(f"Run name: {result.run_name}")
    print(f"Schedule type: {result.schedule_type}")
    print(f"Status: {result.status}")
    print("Stages:")
    for record in result.stage_records:
        duration = f"{record.duration_seconds:.6f}" if record.duration_seconds is not None else "n/a"
        print(f"  {record.stage_name}: {record.status} ({duration}s)")
        if record.error_message:
            print(f"    error: {record.error_message}")
    if result.outputs.get("monitoring_health_status"):
        alert_counts = result.outputs.get("monitoring_alert_counts", {})
        print(f"Monitoring: status={result.outputs['monitoring_health_status']} info={alert_counts.get('info', 0)} warning={alert_counts.get('warning', 0)} critical={alert_counts.get('critical', 0)}")
    print("Artifacts:")
    for name, path in sorted(artifact_paths.items()):
        print(f"  {name}: {path}")


def cmd_pipeline_run(args) -> None:
    config = load_pipeline_run_config(args.config)
    result, artifact_paths = run_orchestration_pipeline(config)
    _print_stage_summary(result, artifact_paths)
    if result.status == "failed":
        raise SystemExit(1)


def cmd_pipeline_run_daily(args) -> None:
    config = load_pipeline_run_config(args.config)
    if config.schedule_type != "daily":
        raise SystemExit("Pipeline config schedule_type must be daily for run-daily")
    result, artifact_paths = run_orchestration_pipeline(config)
    _print_stage_summary(result, artifact_paths)
    if result.status == "failed":
        raise SystemExit(1)


def cmd_pipeline_run_weekly(args) -> None:
    config = load_pipeline_run_config(args.config)
    if config.schedule_type != "weekly":
        raise SystemExit("Pipeline config schedule_type must be weekly for run-weekly")
    result, artifact_paths = run_orchestration_pipeline(config)
    _print_stage_summary(result, artifact_paths)
    if result.status == "failed":
        raise SystemExit(1)
