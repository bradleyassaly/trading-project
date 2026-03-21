from __future__ import annotations

from pathlib import Path

import pandas as pd

from trading_platform.data.canonical import load_research_frame_from_parquet
from trading_platform.settings import FEATURES_DIR


def resolve_feature_frame_path(symbol: str) -> Path:
    candidate_paths = [
        FEATURES_DIR / f"{symbol}.parquet",
        FEATURES_DIR / f"{symbol.lower()}.parquet",
    ]

    path = next((p for p in candidate_paths if p.exists()), None)
    if path is None:
        raise FileNotFoundError(f"Feature file not found for {symbol} in {FEATURES_DIR}")
    return path


def load_feature_frame(symbol: str) -> pd.DataFrame:
    path = resolve_feature_frame_path(symbol)
    return load_research_frame_from_parquet(path, symbol=symbol)
