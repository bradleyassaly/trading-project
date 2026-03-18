from __future__ import annotations

import polars as pl

from trading_platform.features.registry import (
    DEFAULT_FEATURE_GROUPS,
    FEATURE_BUILDERS,
)
from trading_platform.schemas.bars import REQUIRED_BAR_COLUMNS
from trading_platform.settings import FEATURES_DIR, NORMALIZED_DATA_DIR


def build_features(
    symbol: str,
    feature_groups: list[str] | None = None,
) -> str:
    groups = feature_groups or DEFAULT_FEATURE_GROUPS

    normalized_path = NORMALIZED_DATA_DIR / f"{symbol}.parquet"
    if not normalized_path.exists():
        raise FileNotFoundError(
            f"Normalized data file not found for {symbol}: {normalized_path}. "
            "Run ingest first."
        )

    df = pl.read_parquet(normalized_path)

    missing = [col for col in REQUIRED_BAR_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(
            f"Missing required columns for feature build: {missing}. "
            f"Available columns: {df.columns}"
        )

    for group in groups:
        if group not in FEATURE_BUILDERS:
            raise ValueError(
                f"Unknown feature group: {group}. "
                f"Available groups: {sorted(FEATURE_BUILDERS.keys())}"
            )
        df = FEATURE_BUILDERS[group](df)

    out_path = FEATURES_DIR / f"{symbol}.parquet"
    df.write_parquet(out_path)
    return str(out_path)