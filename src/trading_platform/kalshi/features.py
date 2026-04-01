"""
Kalshi prediction market feature generator.

Converts raw Kalshi trade records into a canonical feature parquet
compatible with the existing equity research pipeline.

Price convention
----------------
``close`` = ``yes_price * 100``, expressed on a 0–100 scale so it reads
like a "stock price" in cents. This matches the ARCHITECTURE.md directive:
"treat the yes-price (0–100) as the price series."

Output schema (superset of ``data/features/<SYMBOL>.parquet``)
--------------------------------------------------------------
Canonical:
    timestamp, symbol, open, high, low, close, volume, dollar_volume

Equity-style features (same builders as equity pipeline):
    mom_5, mom_20, mom_60                    (momentum)
    sma_20, sma_50, sma_100, dist_sma_200    (trend)
    vol_10, vol_20, vol_60                   (return volatility)
    vol_avg_20, vol_ratio_20                 (volume)

Signal 1 — Probability calibration drift:
    log_odds            log(p/(1-p))  where p = close/100, clipped to avoid ±∞
    log_odds_ma_20      rolling 20-bar mean of log_odds
    calibration_drift   log_odds − log_odds_ma_20
    calibration_drift_z calibration_drift / rolling_std(log_odds, 20)

Signal 2 — Volume spike detection:
    volume_z            (volume − vol_avg_20) / rolling_std(volume, 20)
    volume_spike        1 when volume_z ≥ 2, else 0
    extreme_volume      volume_z × |close − 50|  (spike at extreme probability)

Signal 3 — Time decay curve:
    days_to_close       calendar days until market resolution (NaN if unknown)
    price_var_proxy     close × (100 − close)  (binary variance analogue)
    tension             price_var_proxy / max(days_to_close, 1)
    time_norm_vol_10    vol_10 / sqrt(max(days_to_close, 1))
"""
from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any  # noqa: F401 — used in build_kalshi_features signature

import polars as pl

logger = logging.getLogger(__name__)

_PRICE_CLIP_LOW = 0.5    # 0.5 cents — avoids log(0)
_PRICE_CLIP_HIGH = 99.5  # 99.5 cents — avoids log(inf)

KALSHI_FEATURE_GROUPS: list[str] = [
    "momentum",
    "trend",
    "volatility",
    "volume",
    "probability_calibration",
    "volume_activity",
    "time_decay",
    "base_rate",
    "metaculus",
    "informed_flow",
]


# ── Resampling ────────────────────────────────────────────────────────────────

def resample_trades_to_bars(
    trades: pl.DataFrame,
    *,
    period: str = "1h",
    timestamp_col: str = "traded_at",
    price_col: str = "yes_price",
    count_col: str = "count",
) -> pl.DataFrame:
    """
    Aggregate individual trade records into OHLCV bars.

    :param trades:         DataFrame with at least ``timestamp_col``, ``price_col``,
                           and ``count_col`` columns.
    :param period:         Polars duration string, e.g. ``"1h"``, ``"1d"``, ``"15m"``.
    :param timestamp_col:  Name of the datetime column in ``trades``.
    :param price_col:      Column holding the yes-price (0.0–1.0 float or 0–100 int).
    :param count_col:      Column holding contract count per trade.
    :returns:              DataFrame with columns:
                           ``timestamp``, ``open``, ``high``, ``low``, ``close``,
                           ``volume``, ``dollar_volume``.
    """
    if trades.is_empty():
        return pl.DataFrame(schema={
            "timestamp": pl.Datetime,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "volume": pl.Float64,
            "dollar_volume": pl.Float64,
        })

    # Normalise price to 0–100 scale
    price = trades[price_col]
    if price.max() <= 1.0:
        # dollar-string floats like 0.65 → multiply to 65
        trades = trades.with_columns((pl.col(price_col) * 100.0).alias(price_col))

    bars = (
        trades
        .sort(timestamp_col)
        .group_by_dynamic(timestamp_col, every=period)
        .agg([
            pl.col(price_col).first().alias("open"),
            pl.col(price_col).max().alias("high"),
            pl.col(price_col).min().alias("low"),
            pl.col(price_col).last().alias("close"),
            pl.col(count_col).sum().cast(pl.Float64).alias("volume"),
            (pl.col(price_col) * pl.col(count_col)).sum().alias("dollar_volume"),
        ])
        .rename({timestamp_col: "timestamp"})
        .sort("timestamp")
    )
    return bars


# ── Equity-style feature builders (operating on yes-price-as-close) ───────────

def _add_momentum(df: pl.DataFrame) -> pl.DataFrame:
    close = pl.col("close")
    return df.with_columns([
        (close / close.shift(5) - 1).alias("mom_5"),
        (close / close.shift(20) - 1).alias("mom_20"),
        (close / close.shift(60) - 1).alias("mom_60"),
    ])


def _add_trend(df: pl.DataFrame) -> pl.DataFrame:
    close = pl.col("close")
    return df.with_columns([
        close.rolling_mean(20).alias("sma_20"),
        close.rolling_mean(50).alias("sma_50"),
        close.rolling_mean(100).alias("sma_100"),
        (close / close.rolling_mean(200) - 1).alias("dist_sma_200"),
    ])


def _add_volatility(df: pl.DataFrame) -> pl.DataFrame:
    ret = pl.col("close").pct_change()
    return df.with_columns([
        ret.rolling_std(10).alias("vol_10"),
        ret.rolling_std(20).alias("vol_20"),
        ret.rolling_std(60).alias("vol_60"),
    ])


def _add_volume_base(df: pl.DataFrame) -> pl.DataFrame:
    volume = pl.col("volume")
    avg_20 = volume.rolling_mean(20)
    return df.with_columns([
        avg_20.alias("vol_avg_20"),
        (volume / avg_20).alias("vol_ratio_20"),
    ])


# ── Prediction-market-specific feature builders ───────────────────────────────

def _add_probability_calibration(df: pl.DataFrame) -> pl.DataFrame:
    """
    Signal 1 — Probability calibration drift.

    Measures whether the current implied probability has drifted away from
    the market's own rolling baseline. A large positive drift means the
    market has recently repriced sharply upward relative to its prior;
    a large negative drift means it repriced sharply downward.

    Strategy idea: fade extreme z-scores (mean-reversion on log-odds).
    """
    # Clip close to (_PRICE_CLIP_LOW, _PRICE_CLIP_HIGH) before log-odds
    p = pl.col("close").clip(_PRICE_CLIP_LOW, _PRICE_CLIP_HIGH) / 100.0
    log_odds = (p / (1.0 - p)).log()

    log_odds_ma = log_odds.rolling_mean(20)
    log_odds_std = log_odds.rolling_std(20)
    drift = log_odds - log_odds_ma

    return df.with_columns([
        log_odds.alias("log_odds"),
        log_odds_ma.alias("log_odds_ma_20"),
        drift.alias("calibration_drift"),
        (drift / log_odds_std).alias("calibration_drift_z"),
    ])


def _add_volume_activity(df: pl.DataFrame) -> pl.DataFrame:
    """
    Signal 2 — Volume spike detection.

    Identifies bars with statistically unusual contract volume. Spikes at
    extreme probabilities (close to 0 or 100) are weighted more heavily,
    as informed traders tend to push prices toward resolution.

    Strategy idea: follow volume spikes in the direction of price move.
    """
    vol = pl.col("volume")
    vol_ma = vol.rolling_mean(20)
    vol_std = vol.rolling_std(20)
    vol_z = (vol - vol_ma) / vol_std

    extreme_weight = (pl.col("close") - 50.0).abs()

    return df.with_columns([
        vol_z.alias("volume_z"),
        (vol_z >= 2.0).cast(pl.Int8).alias("volume_spike"),
        (vol_z * extreme_weight).alias("extreme_volume"),
    ])


def _add_time_decay(df: pl.DataFrame, close_time: datetime | None) -> pl.DataFrame:
    """
    Signal 3 — Time decay curve.

    Binary prediction markets converge to 0 or 100 at resolution.  As the
    deadline approaches, rational uncertainty should decrease (the Binary
    Martingale property).  These features capture how much variance
    remains relative to remaining time.

    ``price_var_proxy`` = close × (100 − close) is the Bernoulli variance
    analogue; it peaks at 50 and is 0 at the extremes.

    ``tension`` = price_var_proxy / days_to_close measures uncertainty per
    unit time. High tension → unresolved market very close to deadline.

    Strategy idea: sell markets with high tension (overpriced uncertainty
    premium) or buy markets where price_var_proxy is low but days_to_close
    is also low (converging to a sure outcome).
    """
    close = pl.col("close")
    price_var_proxy = close * (100.0 - close)

    df = df.with_columns(price_var_proxy.alias("price_var_proxy"))

    if close_time is None:
        return df.with_columns([
            pl.lit(None).cast(pl.Float64).alias("days_to_close"),
            pl.lit(None).cast(pl.Float64).alias("tension"),
            pl.lit(None).cast(pl.Float64).alias("time_norm_vol_10"),
        ])

    # days_to_close is a scalar applied to every row
    now_ts = df["timestamp"].max()
    if now_ts is None:
        days_scalar = None
    else:
        if close_time.tzinfo is not None:
            from datetime import timezone
            now_py = now_ts.replace(tzinfo=timezone.utc) if hasattr(now_ts, "replace") else datetime.now(timezone.utc)
        else:
            now_py = datetime.now()
        delta = (close_time - now_py).total_seconds()
        days_scalar = max(delta / 86400.0, 1.0)

    if days_scalar is None:
        return df.with_columns([
            pl.lit(None).cast(pl.Float64).alias("days_to_close"),
            pl.lit(None).cast(pl.Float64).alias("tension"),
            pl.lit(None).cast(pl.Float64).alias("time_norm_vol_10"),
        ])

    import math

    return df.with_columns([
        pl.lit(days_scalar).alias("days_to_close"),
        (pl.col("price_var_proxy") / days_scalar).alias("tension"),
        (pl.col("vol_10") / math.sqrt(days_scalar)).alias("time_norm_vol_10"),
    ])


# ── Top-level builder ─────────────────────────────────────────────────────────

def build_kalshi_features(
    trades: pl.DataFrame,
    *,
    ticker: str,
    period: str = "1h",
    close_time: datetime | None = None,
    feature_groups: list[str] | None = None,
    timestamp_col: str = "traded_at",
    price_col: str = "yes_price",
    count_col: str = "count",
    extra_scalar_features: dict[str, float] | None = None,
    market_context: dict[str, Any] | None = None,
) -> pl.DataFrame:
    """
    Full feature pipeline: resample trades → canonical bars → all feature groups.

    :param trades:               Raw trades DataFrame (one row per trade).
    :param ticker:               Kalshi market ticker; injected as ``symbol`` column.
    :param period:               Resampling period (default ``"1h"``).
    :param close_time:           Optional market resolution time for time-decay features.
    :param feature_groups:       Subset of :data:`KALSHI_FEATURE_GROUPS` to compute.
                                 Defaults to all groups.
    :param timestamp_col:        Datetime column name in ``trades``.
    :param price_col:            Yes-price column name in ``trades``.
    :param count_col:            Contract-count column name in ``trades``.
    :param extra_scalar_features: Optional dict mapping column name → scalar float value.
                                 Each entry is broadcast as a constant column across all
                                 bars. Used for market-level signals (base rate, Metaculus)
                                 that are scalars rather than time series.
    :param market_context:       Optional dict with keys ``title``, ``series_ticker``,
                                 ``base_rate_db_path``, and ``side_col`` forwarded to
                                 the informed flow feature builder.  Required for the
                                 ``"informed_flow"`` feature group; silently ignored
                                 when that group is not requested.
    :returns:                    Feature DataFrame with ``symbol`` column set to ``ticker``.
    :raises ValueError:          If ``trades`` is empty after resampling.
    """
    groups = set(feature_groups or KALSHI_FEATURE_GROUPS)

    bars = resample_trades_to_bars(
        trades,
        period=period,
        timestamp_col=timestamp_col,
        price_col=price_col,
        count_col=count_col,
    )

    if bars.is_empty():
        raise ValueError(f"No bars produced for {ticker} — trades DataFrame may be empty.")

    df = bars.with_columns(pl.lit(ticker).alias("symbol"))

    if "momentum" in groups:
        df = _add_momentum(df)
    if "trend" in groups:
        df = _add_trend(df)
    if "volatility" in groups:
        df = _add_volatility(df)
    if "volume" in groups:
        df = _add_volume_base(df)
    # volume_activity depends on vol_avg_20 from the volume group
    if "probability_calibration" in groups:
        df = _add_probability_calibration(df)
    if "volume_activity" in groups:
        if "vol_avg_20" not in df.columns:
            df = _add_volume_base(df)
        df = _add_volume_activity(df)
    if "time_decay" in groups:
        if "vol_10" not in df.columns:
            df = _add_volatility(df)
        df = _add_time_decay(df, close_time)

    if "informed_flow" in groups:
        try:
            from trading_platform.kalshi.signals_informed_flow import build_informed_flow_features
            ctx = market_context or {}
            df = build_informed_flow_features(
                df,
                trades,
                period=period,
                market_title=ctx.get("title"),
                series_ticker=ctx.get("series_ticker"),
                base_rate_db_path=ctx.get("base_rate_db_path"),
                timestamp_col=timestamp_col,
                price_col=price_col,
                side_col=ctx.get("side_col", "side"),
                count_col=count_col,
            )
        except Exception as exc:
            logger.warning("Informed flow feature build failed for %s: %s", ticker, exc)

    # Broadcast any extra scalar features (e.g. base_rate_prior, metaculus_probability)
    # as constant columns across all rows. These are market-level signals computed
    # externally and injected here so they land in the same parquet as time-series features.
    if extra_scalar_features:
        df = df.with_columns([
            pl.lit(v).cast(pl.Float64).alias(k)
            for k, v in extra_scalar_features.items()
        ])

    return df


# ── I/O helpers ───────────────────────────────────────────────────────────────

def write_feature_parquet(df: pl.DataFrame, output_dir: Path, ticker: str) -> Path:
    """Write feature DataFrame to ``<output_dir>/<ticker>.parquet``."""
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{ticker}.parquet"
    df.write_parquet(path)
    logger.info("Wrote %d rows to %s", len(df), path)
    return path


def load_trades_parquet(trades_dir: Path, ticker: str) -> pl.DataFrame:
    """
    Load a trades parquet file for a given ticker.

    :raises FileNotFoundError: If the file does not exist.
    """
    path = trades_dir / f"{ticker}.parquet"
    if not path.exists():
        raise FileNotFoundError(
            f"Trades parquet not found for {ticker}: {path}\n"
            "Run the Kalshi ingest command first to persist trade history."
        )
    return pl.read_parquet(path)


def export_trades_to_parquet(
    ticker: str,
    trades_dir: Path,
    *,
    session: Any = None,
) -> Path:
    """
    Export ``KalshiTradeRecord`` rows from the DB to a parquet file.

    Requires a live SQLAlchemy session.  Writes to
    ``<trades_dir>/<ticker>.parquet``.
    """
    if session is None:
        raise ValueError("A SQLAlchemy session is required to export trades from the database.")

    from trading_platform.db.models.kalshi import KalshiTradeRecord

    rows = (
        session.query(KalshiTradeRecord)
        .filter(KalshiTradeRecord.ticker == ticker)
        .order_by(KalshiTradeRecord.traded_at)
        .all()
    )
    if not rows:
        raise ValueError(f"No trade records found in DB for ticker {ticker!r}.")

    records = [
        {
            "trade_id": r.trade_id,
            "ticker": r.ticker,
            "side": r.side,
            "yes_price": r.yes_price,
            "no_price": r.no_price,
            "count": r.count,
            "traded_at": r.traded_at,
        }
        for r in rows
    ]
    df = pl.from_dicts(records)
    trades_dir.mkdir(parents=True, exist_ok=True)
    path = trades_dir / f"{ticker}.parquet"
    df.write_parquet(path)
    logger.info("Exported %d trade rows for %s to %s", len(records), ticker, path)
    return path
