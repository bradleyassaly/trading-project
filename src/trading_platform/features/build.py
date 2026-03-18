from __future__ import annotations

import polars as pl

from trading_platform.features.registry import DEFAULT_FEATURE_GROUPS, FEATURE_BUILDERS
from trading_platform.settings import FEATURES_DIR, RAW_DATA_DIR


def _normalize_ohlcv_columns(df: pl.DataFrame) -> pl.DataFrame:
    rename_map: dict[str, str] = {}

    for col in df.columns:
        lower = col.lower()
        if lower in {"date", "timestamp", "datetime"}:
            rename_map[col] = "Date"
        elif lower == "open":
            rename_map[col] = "Open"
        elif lower == "high":
            rename_map[col] = "High"
        elif lower == "low":
            rename_map[col] = "Low"
        elif lower == "close":
            rename_map[col] = "Close"
        elif lower == "volume":
            rename_map[col] = "Volume"

    if rename_map:
        df = df.rename(rename_map)

    return df

def build_features(symbol: str, feature_groups: list[str] | None = None):
    groups = feature_groups or DEFAULT_FEATURE_GROUPS

    raw_path = RAW_DATA_DIR / f"{symbol}.parquet"
    if not raw_path.exists():
        raise FileNotFoundError(
            f"Raw data file not found for {symbol}: {raw_path}. Run ingest first."
        )

    df = pl.read_parquet(raw_path)
    df = _normalize_ohlcv_columns(df)

    required_cols = {"Date", "Open", "High", "Low", "Close", "Volume"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(
            f"Missing required columns for feature build: {sorted(missing)}. "
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
    return out_path