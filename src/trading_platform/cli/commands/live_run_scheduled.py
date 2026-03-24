from __future__ import annotations

from trading_platform.cli.config_support import apply_workflow_config, option_is_explicit
from trading_platform.cli.commands.live_dry_run import _build_config
from trading_platform.cli.presets import apply_cli_preset
from trading_platform.config.loader import load_execution_config, load_live_dry_run_workflow_config
from trading_platform.live.persistence import persist_live_scheduled_outputs
from trading_platform.live.preview import run_live_dry_run_preview, write_live_dry_run_artifacts


def cmd_live_run_scheduled(args) -> None:
    print("Starting scheduled live dry-run")
    if getattr(args, "config", None):
        loaded = load_live_dry_run_workflow_config(args.config)
        if getattr(loaded, "preset", None) and not option_is_explicit(args, "preset"):
            args.preset = loaded.preset
    apply_cli_preset(args)
    apply_workflow_config(
        args,
        config_path=getattr(args, "config", None),
        loader=load_live_dry_run_workflow_config,
    )
    if not getattr(args, "preset", None):
        raise SystemExit("Scheduled live dry-run requires --preset")

    config = _build_config(args)
    execution_config = load_execution_config(args.execution_config) if getattr(args, "execution_config", None) else None
    result = run_live_dry_run_preview(config, execution_config=execution_config) if execution_config is not None else run_live_dry_run_preview(config)
    preview_paths = write_live_dry_run_artifacts(result)
    scheduled_paths, health_checks, summary = persist_live_scheduled_outputs(
        result=result,
        output_dir=config.output_dir,
    )

    print(f"Preset: {summary['preset_name']}")
    print(f"Broker: {summary['broker']}")
    print(f"Timestamp: {summary['timestamp']}")
    print(f"Readiness: {summary['readiness']}")
    print(f"Proposed orders: {summary['proposed_order_count']}")
    print(f"Gross exposure: {summary['gross_exposure']}")
    print(f"Target holdings count: {summary['target_holdings_count']}")
    print(f"Realized holdings count: {summary['realized_holdings_count']}")
    print(f"Turnover estimate: {summary['turnover_estimate']}")

    notable = [item for item in health_checks if item["status"] != "pass"]
    if notable:
        print("Top warnings/failures:")
        for item in notable[:5]:
            print(f"  {item['status']}: {item['check_name']} -> {item['message']}")

    print("Artifacts:")
    combined = {**preview_paths, **scheduled_paths}
    for key, value in sorted(combined.items()):
        print(f"  {key}: {value}")

    if summary["readiness"] == "blocked":
        raise SystemExit(1)
    print("Scheduled live dry-run completed")
