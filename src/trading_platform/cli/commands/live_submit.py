from __future__ import annotations

from trading_platform.broker.service import resolve_broker_adapter
from trading_platform.cli.commands.live_dry_run import _build_config
from trading_platform.cli.presets import apply_cli_preset
from trading_platform.config.loader import load_broker_config, load_execution_config
from trading_platform.live.preview import run_live_dry_run_preview, write_live_dry_run_artifacts
from trading_platform.live.submission import submit_live_orders


def cmd_live_submit(args) -> None:
    if not getattr(args, "broker_config", None):
        raise SystemExit("live submit requires --broker-config")
    apply_cli_preset(args)
    preview_config = _build_config(args)
    execution_config = load_execution_config(args.execution_config) if getattr(args, "execution_config", None) else None
    broker_config = load_broker_config(args.broker_config)
    if getattr(args, "broker", None):
        broker_config = broker_config.__class__(**{**broker_config.to_dict(), "broker_name": args.broker})
        preview_config = preview_config.__class__(**{**preview_config.__dict__, "broker": broker_config.broker_name})

    preview_result = (
        run_live_dry_run_preview(preview_config, execution_config=execution_config)
        if execution_config is not None
        else run_live_dry_run_preview(preview_config)
    )
    write_live_dry_run_artifacts(preview_result)
    adapter = resolve_broker_adapter(broker_config)
    submission_result = submit_live_orders(
        preview_result=preview_result,
        broker_config=broker_config,
        broker_adapter=adapter,
        validate_only=bool(getattr(args, "validate_only", False)),
        output_dir=preview_config.output_dir,
    )
    print(f"Live submit broker: {broker_config.broker_name}")
    print(f"Validate only: {submission_result.validate_only}")
    print(f"Risk passed: {submission_result.summary.risk_passed}")
    print(f"Requested orders: {submission_result.summary.requested_order_count}")
    print(f"Submitted orders: {submission_result.summary.submitted_order_count}")
    print(f"Skipped orders: {submission_result.summary.skipped_order_count}")
    print(f"Rejected orders: {submission_result.summary.rejected_order_count}")
    print("Artifacts:")
    combined_paths = {**preview_result.artifacts, **submission_result.artifacts}
    for key, value in sorted(combined_paths.items()):
        print(f"  {key}: {value}")
