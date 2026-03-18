from __future__ import annotations

import polars as pl


def add_volatility_features(df: pl.DataFrame) -> pl.DataFrame:
    close_ret = pl.col("close").pct_change()

    return df.with_columns(
        [
            close_ret.rolling_std(10).alias("vol_10"),
            close_ret.rolling_std(20).alias("vol_20"),
            close_ret.rolling_std(60).alias("vol_60"),
        ]
    )