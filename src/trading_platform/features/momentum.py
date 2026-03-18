from __future__ import annotations

import polars as pl


def add_momentum_features(df: pl.DataFrame) -> pl.DataFrame:
    close = pl.col("close")

    return df.with_columns(
        [
            (close / close.shift(5) - 1).alias("mom_5"),
            (close / close.shift(20) - 1).alias("mom_20"),
            (close / close.shift(60) - 1).alias("mom_60"),
        ]
    )