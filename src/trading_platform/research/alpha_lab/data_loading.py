from __future__ import annotations

from pathlib import Path

import pandas as pd

from trading_platform.data.canonical import (
    load_research_symbol_frame,
    normalize_research_frame,
)


def normalize_timestamp_column(df: pd.DataFrame) -> pd.DataFrame:
    return normalize_research_frame(df, require_close=False)


def normalize_close_column(df: pd.DataFrame) -> pd.DataFrame:
    return normalize_research_frame(df)


def load_symbol_feature_data(feature_dir: Path, symbol: str) -> pd.DataFrame:
    return load_research_symbol_frame(feature_dir, symbol)
