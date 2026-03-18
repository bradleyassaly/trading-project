from __future__ import annotations

import polars as pl


def add_volume_features(df: pl.DataFrame) -> pl.DataFrame:
    avg_20 = pl.col("Volume").rolling_mean(20)

    return df.with_columns(
        [
            avg_20.alias("vol_avg_20"),
            (pl.col("Volume") / avg_20).alias("vol_ratio_20"),
        ]
    )