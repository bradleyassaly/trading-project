from __future__ import annotations

from trading_platform.dashboard.server import build_dashboard_static_data


def cmd_dashboard_build_static_data(args) -> None:
    paths = build_dashboard_static_data(
        artifacts_root=args.artifacts_root,
        output_dir=args.output_dir,
    )
    for key, value in sorted(paths.items()):
        print(f"{key}: {value}")
