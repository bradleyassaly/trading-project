from __future__ import annotations

import polars as pl


def add_trend_features(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        [
            pl.col("Close").rolling_mean(20).alias("sma_20"),
            pl.col("Close").rolling_mean(50).alias("sma_50"),
            pl.col("Close").rolling_mean(100).alias("sma_100"),
            pl.col("Close").rolling_mean(200).alias("sma_200"),
            (pl.col("Close") / pl.col("Close").rolling_mean(200) - 1).alias("dist_sma_200"),
        ]
    )