import json
from datetime import datetime, date

import numpy as np
import pandas as pd

from trading_platform.settings import EXPERIMENT_DIR


EXCLUDE_KEYS = {"_strategy", "_equity_curve", "_trades"}


def _to_jsonable(value):
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.isoformat()
    if isinstance(value, (pd.Timedelta, np.timedelta64)):
        return str(value)
    if pd.isna(value):
        return None
    return str(value)


def log_experiment(result):
    exp_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    path = EXPERIMENT_DIR / f"{exp_id}.json"

    result_dict = dict(result)
    cleaned = {
        str(k): _to_jsonable(v)
        for k, v in result_dict.items()
        if k not in EXCLUDE_KEYS
    }

    payload = {
        "experiment_id": exp_id,
        "created_at_utc": datetime.utcnow().isoformat(),
        "metrics": cleaned,
    }

    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    return exp_id