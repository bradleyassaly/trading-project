from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.settings import JOB_ARTIFACTS_DIR


def make_walk_forward_artifact_stem() -> str:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"walk_forward_{timestamp}"


def save_walk_forward_windows_csv(
    results_df: pd.DataFrame,
    stem: str | None = None,
) -> Path:
    stem = stem or make_walk_forward_artifact_stem()
    path = JOB_ARTIFACTS_DIR / f"{stem}.windows.csv"
    results_df.to_csv(path, index=False)
    return path


def save_walk_forward_summary_json(
    payload: dict[str, Any],
    stem: str | None = None,
) -> Path:
    stem = stem or make_walk_forward_artifact_stem()
    path = JOB_ARTIFACTS_DIR / f"{stem}.json"
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path