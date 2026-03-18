from __future__ import annotations

import polars as pl


def add_trend_features(df: pl.DataFrame) -> pl.DataFrame:
    close = pl.col("close")

    return df.with_columns(
        [
            close.rolling_mean(20).alias("sma_20"),
            close.rolling_mean(50).alias("sma_50"),
            close.rolling_mean(100).alias("sma_100"),
            close.rolling_mean(200).alias("sma_200"),
            (close / close.rolling_mean(200) - 1).alias("dist_sma_200"),
        ]
    )