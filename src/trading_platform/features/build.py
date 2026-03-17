import polars as pl
from trading_platform.settings import RAW_DATA_DIR, FEATURE_DATA_DIR


def build_features(symbol: str):
    path = RAW_DATA_DIR / f"{symbol}.parquet"

    df = pl.read_parquet(path)

    df = df.with_columns([
        pl.col("close").pct_change().alias("ret_1d"),
        pl.col("close").rolling_mean(20).alias("sma20"),
        pl.col("close").rolling_mean(100).alias("sma100"),
    ])

    out_path = FEATURE_DATA_DIR / f"{symbol}.parquet"
    df.write_parquet(out_path)

    return out_path