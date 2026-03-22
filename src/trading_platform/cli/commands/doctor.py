from __future__ import annotations

from trading_platform.system.doctor import run_system_doctor


def cmd_doctor(args) -> None:
    report, paths = run_system_doctor(
        artifacts_root=args.artifacts_root,
        output_dir=args.output_dir,
        pipeline_config=getattr(args, "pipeline_config", None),
        monitoring_config=getattr(args, "monitoring_config", None),
        notification_config=getattr(args, "notification_config", None),
        execution_config=getattr(args, "execution_config", None),
        broker_config=getattr(args, "broker_config", None),
        dashboard_config=getattr(args, "dashboard_config", None),
    )
    print(f"Doctor status: {report['status']}")
    print(f"Checks: {report['check_count']} warnings={report['warning_count']} errors={report['error_count']}")
    for path_name, path in sorted(paths.items()):
        print(f"{path_name}: {path}")
