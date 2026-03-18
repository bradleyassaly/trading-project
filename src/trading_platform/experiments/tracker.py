from __future__ import annotations

import csv
import json
import subprocess
from datetime import datetime, UTC
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from trading_platform.settings import EXPERIMENT_DIR


def _json_safe(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value

    if isinstance(value, (np.integer,)):
        return int(value)

    if isinstance(value, (np.floating,)):
        if pd.isna(value):
            return None
        return float(value)

    if isinstance(value, (np.bool_,)):
        return bool(value)

    if isinstance(value, (pd.Timestamp, datetime)):
        if pd.isna(value):
            return None
        return value.isoformat()

    if value is pd.NaT:
        return None

    if isinstance(value, Path):
        return str(value)

    if isinstance(value, pd.Series):
        return {str(k): _json_safe(v) for k, v in value.to_dict().items()}

    if isinstance(value, pd.DataFrame):
        return [_json_safe(record) for record in value.to_dict(orient="records")]

    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}

    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]

    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    return str(value)

def get_git_commit_hash() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except Exception:
        return "unknown"


def build_experiment_record(stats: dict[str, Any]) -> dict[str, Any]:
    timestamp = datetime.now(UTC)
    timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")

    symbol = str(stats.get("symbol", "unknown"))
    strategy = str(stats.get("strategy", "unknown"))

    experiment_id = f"{timestamp_str}_{symbol}_{strategy}"

    record = {
        "experiment_id": experiment_id,
        "timestamp": timestamp.isoformat(),
        "git_commit": get_git_commit_hash(),
        "symbol": stats.get("symbol"),
        "strategy": stats.get("strategy"),
        "fast": stats.get("fast"),
        "slow": stats.get("slow"),
        "cash": stats.get("cash"),
        "commission": stats.get("commission"),
        "return_pct": stats.get("Return [%]"),
        "sharpe": stats.get("Sharpe Ratio"),
        "max_drawdown_pct": stats.get("Max. Drawdown [%]"),
        "raw_stats": _json_safe(stats),
    }

    return record


def append_experiment_index(record: dict[str, Any]) -> Path:
    index_path = EXPERIMENT_DIR / "experiment_index.csv"

    row = {
        "experiment_id": record["experiment_id"],
        "timestamp": record["timestamp"],
        "git_commit": record["git_commit"],
        "symbol": record["symbol"],
        "strategy": record["strategy"],
        "fast": record["fast"],
        "slow": record["slow"],
        "cash": record["cash"],
        "commission": record["commission"],
        "return_pct": record["return_pct"],
        "sharpe": record["sharpe"],
        "max_drawdown_pct": record["max_drawdown_pct"],
    }

    file_exists = index_path.exists()

    with index_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()))
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)

    return index_path


def log_experiment(stats: dict[str, Any]) -> str:
    record = build_experiment_record(stats)
    experiment_id = record["experiment_id"]

    json_path = EXPERIMENT_DIR / f"{experiment_id}.json"
    with json_path.open("w", encoding="utf-8") as f:
        json.dump(record, f, indent=2)

    append_experiment_index(record)

    return experiment_id