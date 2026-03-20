from __future__ import annotations

import json
from pathlib import Path

from trading_platform.research.experiment_tracking import build_latest_model_state


def cmd_experiments_latest_model(args) -> None:
    payload = build_latest_model_state(Path(args.tracker_dir))
    print(json.dumps(payload, indent=2, default=str))
