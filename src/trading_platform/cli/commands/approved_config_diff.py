from __future__ import annotations

import json
from pathlib import Path

from trading_platform.research.refresh_monitoring import (
    show_current_vs_previous_configuration,
)


def cmd_approved_config_diff(args) -> None:
    payload = show_current_vs_previous_configuration(
        snapshot_dir=Path(args.snapshot_dir),
    )
    print(json.dumps(payload, indent=2, default=str))
