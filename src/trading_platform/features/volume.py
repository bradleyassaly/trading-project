from __future__ import annotations

import polars as pl


def add_volume_features(df: pl.DataFrame) -> pl.DataFrame:
    volume = pl.col("volume")
    avg_20 = volume.rolling_mean(20)

    return df.with_columns(
        [
            avg_20.alias("vol_avg_20"),
            (volume / avg_20).alias("vol_ratio_20"),
        ]
    )