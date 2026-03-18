from __future__ import annotations

import polars as pl


def add_momentum_features(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        [
            (pl.col("Close") / pl.col("Close").shift(5) - 1).alias("mom_5"),
            (pl.col("Close") / pl.col("Close").shift(20) - 1).alias("mom_20"),
            (pl.col("Close") / pl.col("Close").shift(60) - 1).alias("mom_60"),
        ]
    )