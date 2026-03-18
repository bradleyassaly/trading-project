from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.settings import JOB_ARTIFACTS_DIR


def make_sweep_artifact_stem() -> str:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"sweep_{timestamp}"


def save_sweep_leaderboard_csv(
    leaderboard: pd.DataFrame,
    stem: str | None = None,
) -> Path:
    stem = stem or make_sweep_artifact_stem()
    path = JOB_ARTIFACTS_DIR / f"{stem}.leaderboard.csv"
    leaderboard.to_csv(path, index=False)
    return path


def save_sweep_summary_json(
    payload: dict[str, Any],
    stem: str | None = None,
) -> Path:
    stem = stem or make_sweep_artifact_stem()
    path = JOB_ARTIFACTS_DIR / f"{stem}.json"
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path