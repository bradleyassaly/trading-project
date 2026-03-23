from __future__ import annotations

from trading_platform.orchestration.pipeline_runner import show_orchestration_run


def cmd_orchestrate_show_run(args) -> None:
    payload = show_orchestration_run(args.run)
    print(f"Run id: {payload.get('run_id')}")
    print(f"Run name: {payload.get('run_name')}")
    print(f"Status: {payload.get('status')}")
    print("Stages:")
    for record in payload.get("stage_records", []):
        print(f"  {record.get('stage_name')}: {record.get('status')}")
