from __future__ import annotations

from pathlib import Path

import pandas as pd

from trading_platform.settings import FEATURES_DIR


def load_feature_frame(symbol: str) -> pd.DataFrame:
    candidate_paths = [
        FEATURES_DIR / f"{symbol}.parquet",
        FEATURES_DIR / f"{symbol.lower()}.parquet",
    ]

    path = next((p for p in candidate_paths if p.exists()), None)
    if path is None:
        raise FileNotFoundError(f"Feature file not found for {symbol} in {FEATURES_DIR}")

    return pd.read_parquet(path)