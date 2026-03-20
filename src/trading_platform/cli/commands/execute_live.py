from __future__ import annotations

from trading_platform.cli.commands.validate_live import _build_config
from trading_platform.live.control import run_live_execution_control


def cmd_execute_live(args) -> None:
    result = run_live_execution_control(
        config=_build_config(args),
        execute=True,
    )
    print(f"Live execution decision: {result.decision}")
    print(f"Reason codes: {', '.join(result.reason_codes) if result.reason_codes else 'none'}")
    for key, value in sorted(result.artifacts.items()):
        print(f"{key}: {value}")
