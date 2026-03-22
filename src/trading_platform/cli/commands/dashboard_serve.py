from __future__ import annotations

from trading_platform.dashboard.server import serve_dashboard


def cmd_dashboard_serve(args) -> None:
    serve_dashboard(
        artifacts_root=args.artifacts_root,
        host=args.host,
        port=args.port,
    )
