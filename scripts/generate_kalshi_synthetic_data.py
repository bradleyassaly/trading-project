"""
Generate synthetic Kalshi prediction market data for first research run.

Produces:
  data/kalshi/features/<TICKER>.parquet  — feature parquets (100 markets)
  data/kalshi/resolution.csv            — resolution outcomes

Synthetic market behavior:
- Each market starts at a random yes-price between 20 and 80.
- Price follows a random walk with drift toward resolution value.
- Volume is log-normal with occasional spikes.
- Markets resolve to 0 or 100 at bar 60 (no partial resolutions).
- Approximately 55% of markets resolve YES (100), 45% NO (0).
"""
from __future__ import annotations

import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from trading_platform.kalshi.features import (
    _add_momentum,
    _add_probability_calibration,
    _add_time_decay,
    _add_trend,
    _add_volume_activity,
    _add_volume_base,
    _add_volatility,
)

import polars as pl


SEED = 2026
RNG = np.random.default_rng(SEED)
N_MARKETS = 100
N_BARS = 90
RESOLUTION_BAR = N_BARS  # all bars are pre-resolution


def _generate_market(
    ticker: str,
    start_price: float,
    resolves_yes: bool,
    rng: np.random.Generator,
) -> tuple[pd.DataFrame, float]:
    resolution_price = 100.0 if resolves_yes else 0.0

    prices = [start_price]
    drift_target = resolution_price
    for i in range(1, N_BARS):
        # Drift grows linearly as we approach resolution
        drift_strength = (i / N_BARS) * 0.4
        noise = rng.normal(0, 3.0)
        drift = (drift_target - prices[-1]) * drift_strength
        new_price = prices[-1] + drift + noise
        new_price = float(np.clip(new_price, 0.5, 99.5))
        prices.append(new_price)

    prices_arr = np.array(prices)

    volume_base = rng.lognormal(4, 1, N_BARS).astype(float)
    # Add 5-10 volume spikes
    n_spikes = int(rng.integers(3, 8))
    spike_bars = rng.choice(N_BARS, n_spikes, replace=False)
    volume_base[spike_bars] *= rng.uniform(3, 8, n_spikes)

    df = pd.DataFrame({
        "close": prices_arr,
        "open": np.roll(prices_arr, 1),
        "high": prices_arr + rng.uniform(0, 2, N_BARS),
        "low": prices_arr - rng.uniform(0, 2, N_BARS),
        "volume": volume_base,
        "dollar_volume": volume_base * prices_arr,
        "symbol": ticker,
    })
    df.loc[0, "open"] = prices_arr[0]

    return df, resolution_price


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    import polars as pl

    pl_df = pl.from_pandas(df)
    pl_df = pl_df.with_columns([
        (pl.col("close") / pl.col("close").shift(5) - 1).alias("mom_5"),
        (pl.col("close") / pl.col("close").shift(20) - 1).alias("mom_20"),
        (pl.col("close") / pl.col("close").shift(60) - 1).alias("mom_60"),
        pl.col("close").rolling_mean(20).alias("sma_20"),
        pl.col("close").rolling_mean(50).alias("sma_50"),
        pl.col("close").rolling_mean(100).alias("sma_100"),
        (pl.col("close") / pl.col("close").rolling_mean(200) - 1).alias("dist_sma_200"),
        pl.col("close").pct_change().rolling_std(10).alias("vol_10"),
        pl.col("close").pct_change().rolling_std(20).alias("vol_20"),
        pl.col("volume").rolling_mean(20).alias("vol_avg_20"),
        (pl.col("volume") / pl.col("volume").rolling_mean(20)).alias("vol_ratio_20"),
    ])

    p = (pl.col("close").clip(0.5, 99.5) / 100.0)
    log_odds = (p / (1.0 - p)).log()
    log_odds_ma = log_odds.rolling_mean(20)
    log_odds_std = log_odds.rolling_std(20)
    drift = log_odds - log_odds_ma
    pl_df = pl_df.with_columns([
        log_odds.alias("log_odds"),
        log_odds_ma.alias("log_odds_ma_20"),
        drift.alias("calibration_drift"),
        (drift / log_odds_std).alias("calibration_drift_z"),
    ])

    vol = pl.col("volume")
    vol_ma = vol.rolling_mean(20)
    vol_std = vol.rolling_std(20)
    vol_z = (vol - vol_ma) / vol_std
    extreme_weight = (pl.col("close") - 50.0).abs()
    pl_df = pl_df.with_columns([
        vol_z.alias("volume_z"),
        (vol_z >= 2.0).cast(pl.Int8).alias("volume_spike"),
        (vol_z * extreme_weight).alias("extreme_volume"),
    ])

    price_var_proxy = pl.col("close") * (100.0 - pl.col("close"))
    pl_df = pl_df.with_columns(price_var_proxy.alias("price_var_proxy"))

    days_to_close = 30.0
    pl_df = pl_df.with_columns([
        pl.lit(days_to_close).alias("days_to_close"),
        (pl.col("price_var_proxy") / days_to_close).alias("tension"),
        (pl.col("vol_10") / math.sqrt(days_to_close)).alias("time_norm_vol_10"),
    ])

    return pl_df.to_pandas()


def main() -> None:
    root = Path(__file__).parent.parent
    feature_dir = root / "data" / "kalshi" / "features"
    feature_dir.mkdir(parents=True, exist_ok=True)

    resolution_rows = []
    n_yes = int(N_MARKETS * 0.55)
    outcomes = [True] * n_yes + [False] * (N_MARKETS - n_yes)
    RNG.shuffle(outcomes)

    for i in range(N_MARKETS):
        ticker = f"SYNTH-{i:03d}"
        start_price = float(RNG.uniform(20, 80))
        resolves_yes = bool(outcomes[i])

        df_raw, resolution_price = _generate_market(ticker, start_price, resolves_yes, RNG)
        df_feat = build_features(df_raw)

        path = feature_dir / f"{ticker}.parquet"
        df_feat.to_parquet(path, index=False)

        resolution_rows.append({"ticker": ticker, "resolution_price": resolution_price, "resolves_yes": resolves_yes})

    resolution_df = pd.DataFrame(resolution_rows)
    resolution_path = root / "data" / "kalshi" / "resolution.csv"
    resolution_df.to_csv(resolution_path, index=False)

    print(f"Generated {N_MARKETS} synthetic markets -> {feature_dir}")
    print(f"Resolution data -> {resolution_path}")
    print(f"YES resolves: {n_yes}, NO resolves: {N_MARKETS - n_yes}")


if __name__ == "__main__":
    main()
