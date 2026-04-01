"""
Informed flow detection signals for Kalshi prediction markets.

Informed trading leaves distinctive footprints in trade microstructure
that differ from noise trading:

1. **Taker imbalance** — informed buyers repeatedly take the ask (YES side),
   creating a sustained directional imbalance in taker-initiated volume.
   Noise traders are roughly symmetric; informed traders are not.

2. **Large order footprint** — informed traders sometimes place orders that
   are large relative to the market's typical size when they expect a move
   before others react. These "footprint" trades carry directional conviction.

3. **Unexplained price moves** — sharp probability moves that occur outside
   any known scheduled event window (FOMC, CPI release, etc.) are more likely
   to reflect someone trading on non-public or edge information than random
   noise. Moves within known event windows are priced in; moves outside them
   are anomalous.

Architecture notes
------------------
Unlike the scalar signals (base_rate, metaculus), informed flow features are
**time-series** — they vary bar by bar.  They are integrated into the feature
generator via ``build_kalshi_features(..., feature_groups=["informed_flow"])``
and the optional ``market_context`` dict.

All three builders operate on raw trade-level data (requiring ``side`` and
``count`` columns) and produce bar-aligned outputs that are joined into the
main bar DataFrame by timestamp.  If the required columns are absent (e.g.
older data without a ``side`` field), every informed-flow column is silently
set to NaN — the pipeline never raises.

Feature columns produced
------------------------
Taker imbalance:
  ``taker_buy_vol``       sum of YES-taker contract volume per bar
  ``taker_sell_vol``      sum of NO-taker contract volume per bar
  ``taker_imbalance``     (buy − sell) / (buy + sell), range −1 to +1
  ``imbalance_z``         z-score of taker_imbalance vs 30-bar rolling baseline
  ``taker_conviction``    |taker_imbalance| × log(1 + total taker volume)

Large order footprint:
  ``large_order_direction``     weighted directional mean of oversized trades (−1 to +1)
  ``large_order_conviction``    |large_order_direction|
  ``large_order_count``         number of large-order trades in bar (integer)
  ``large_order_volume_ratio``  fraction of bar volume from large orders (0 to 1)

Unexplained move:
  ``unexplained_move``          signed 1-bar price change (close[t] − close[t−1])
  ``unexplained_move_z``        z-score vs 20-bar rolling std of moves
  ``has_scheduled_catalyst``    1.0 if market is a known structured-event type, else 0.0
  ``catalyst_type``             category name from base_rate_db, or "none"

Signal direction convention (same as signals.py):
  ``KALSHI_TAKER_IMBALANCE``    direction = +1  (positive imbalance_z → buy YES)
  ``KALSHI_LARGE_ORDER``        direction = +1  (positive direction → buy YES)
  ``KALSHI_UNEXPLAINED_MOVE``   direction = +1  (positive move_z → follow price)
"""
from __future__ import annotations

import logging
import math
from pathlib import Path
from typing import Any

import polars as pl

from trading_platform.kalshi.signals import KalshiSignalFamily

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

_LARGE_ORDER_MULTIPLIER: float = 5.0   # threshold: trade > 5× rolling median size
_LARGE_ORDER_WINDOW: int = 20          # rolling window for median size (trade-count)
_IMBALANCE_Z_WINDOW: int = 30          # rolling window for imbalance z-score (bars)
_UNEXPLAINED_MOVE_WINDOW: int = 20     # rolling window for move z-score (bars)
_EPSILON: float = 1e-9                 # avoids divide-by-zero


# ── Taker imbalance ───────────────────────────────────────────────────────────

def _resolve_side_col(trades: pl.DataFrame, hint: str) -> str | None:
    """Return the name of the side column, or None if not present."""
    for candidate in (hint, "side", "taker_side"):
        if candidate in trades.columns:
            return candidate
    return None


def _compute_taker_imbalance_bars(
    trades: pl.DataFrame,
    *,
    period: str,
    timestamp_col: str,
    count_col: str,
    side_col: str,
) -> pl.DataFrame | None:
    """
    Resample trades to bars aggregating directional taker volume.

    :returns: DataFrame with columns ``timestamp``, ``taker_buy_vol``,
              ``taker_sell_vol`` aligned to ``period`` grid, or None if the
              side column is unavailable.
    """
    actual_side = _resolve_side_col(trades, side_col)
    if actual_side is None or trades.is_empty():
        return None
    if timestamp_col not in trades.columns:
        return None

    t = trades.with_columns([
        pl.when(pl.col(actual_side) == "yes")
          .then(pl.col(count_col).cast(pl.Float64))
          .otherwise(0.0)
          .alias("_buy"),
        pl.when(pl.col(actual_side) == "no")
          .then(pl.col(count_col).cast(pl.Float64))
          .otherwise(0.0)
          .alias("_sell"),
    ])

    return (
        t.sort(timestamp_col)
        .group_by_dynamic(timestamp_col, every=period)
        .agg([
            pl.col("_buy").sum().alias("taker_buy_vol"),
            pl.col("_sell").sum().alias("taker_sell_vol"),
        ])
        .rename({timestamp_col: "timestamp"})
        .sort("timestamp")
    )


def _add_taker_imbalance(
    df: pl.DataFrame,
    trades: pl.DataFrame,
    *,
    period: str,
    timestamp_col: str,
    count_col: str,
    side_col: str,
) -> pl.DataFrame:
    """
    Join taker imbalance features into the bar DataFrame.

    Adds: ``taker_buy_vol``, ``taker_sell_vol``, ``taker_imbalance``,
    ``imbalance_z``, ``taker_conviction``.
    All columns are NaN-filled when data is unavailable.
    """
    _nan = pl.lit(None).cast(pl.Float64)
    null_cols = [
        _nan.alias("taker_buy_vol"),
        _nan.alias("taker_sell_vol"),
        _nan.alias("taker_imbalance"),
        _nan.alias("imbalance_z"),
        _nan.alias("taker_conviction"),
    ]

    taker_bars = _compute_taker_imbalance_bars(
        trades, period=period, timestamp_col=timestamp_col,
        count_col=count_col, side_col=side_col,
    )
    if taker_bars is None:
        return df.with_columns(null_cols)

    df = df.join(taker_bars, on="timestamp", how="left")

    buy = pl.col("taker_buy_vol").fill_null(0.0)
    sell = pl.col("taker_sell_vol").fill_null(0.0)
    total = buy + sell

    # taker_imbalance ∈ [−1, +1]; zero when no volume in bar
    imb = ((buy - sell) / (total + _EPSILON)).alias("taker_imbalance")
    df = df.with_columns(imb)

    # Rolling z-score over _IMBALANCE_Z_WINDOW bars
    imb_col = pl.col("taker_imbalance")
    imb_ma = imb_col.rolling_mean(_IMBALANCE_Z_WINDOW)
    imb_std = imb_col.rolling_std(_IMBALANCE_Z_WINDOW)
    imbalance_z = ((imb_col - imb_ma) / (imb_std + _EPSILON)).alias("imbalance_z")

    # Conviction: directional magnitude weighted by (log-scaled) volume
    # log(1 + total_vol) normalises volume to a bounded, order-of-magnitude scale
    log_vol = (total + 1.0).log(math.e)
    conviction = (imb_col.abs() * log_vol).alias("taker_conviction")

    return df.with_columns([imbalance_z, conviction])


# ── Large order footprint ─────────────────────────────────────────────────────

def _flag_large_orders(
    trades: pl.DataFrame,
    *,
    count_col: str,
    multiplier: float = _LARGE_ORDER_MULTIPLIER,
    window: int = _LARGE_ORDER_WINDOW,
) -> pl.DataFrame:
    """
    Flag trades whose size exceeds ``multiplier × rolling median size``.

    :param trades:     Sorted trade DataFrame (must have ``count_col``).
    :param count_col:  Column holding contract count per trade.
    :param multiplier: Size threshold multiplier (default 5×).
    :param window:     Rolling window over preceding trades (default 20).
    :returns:          Input DataFrame with added ``_rolling_median`` and
                       ``_is_large`` columns.
    """
    counts = trades[count_col].cast(pl.Float64)
    # rolling_quantile(0.5) = rolling median; min_periods=3 avoids early noise
    rolling_med = counts.rolling_quantile(0.5, window_size=window, min_samples=3)
    return trades.with_columns([
        rolling_med.alias("_rolling_median"),
        (counts > (multiplier * rolling_med)).alias("_is_large"),
    ])


def _compute_large_order_bars(
    trades: pl.DataFrame,
    *,
    period: str,
    timestamp_col: str,
    count_col: str,
    side_col: str,
    multiplier: float = _LARGE_ORDER_MULTIPLIER,
    window: int = _LARGE_ORDER_WINDOW,
) -> pl.DataFrame | None:
    """
    Resample large trades to bars with directional aggregates.

    :returns: DataFrame with columns ``timestamp``, ``large_order_direction``,
              ``large_order_conviction``, ``large_order_count``,
              ``_large_order_volume``, or None if required columns absent.
    """
    actual_side = _resolve_side_col(trades, side_col)
    if actual_side is None or trades.is_empty():
        return None
    if timestamp_col not in trades.columns:
        return None

    t = trades.sort(timestamp_col)
    t = _flag_large_orders(t, count_col=count_col, multiplier=multiplier, window=window)

    # Add signed volume: +count for YES taker, −count for NO taker
    t = t.with_columns(
        pl.when(pl.col(actual_side) == "yes")
          .then(pl.col(count_col).cast(pl.Float64))
          .otherwise(-pl.col(count_col).cast(pl.Float64))
          .alias("_dir_vol")
    )

    large = t.filter(pl.col("_is_large"))
    if large.is_empty():
        return None

    large_bars = (
        large
        .group_by_dynamic(timestamp_col, every=period)
        .agg([
            # Weighted direction: sum(±count) / sum(count) → bounded [-1, +1]
            (pl.col("_dir_vol").sum() / (pl.col(count_col).cast(pl.Float64).sum() + _EPSILON))
              .alias("large_order_direction"),
            # Conviction: absolute value of the weighted direction
            (pl.col("_dir_vol").sum().abs() / (pl.col(count_col).cast(pl.Float64).sum() + _EPSILON))
              .alias("large_order_conviction"),
            pl.len().alias("large_order_count"),
            pl.col(count_col).cast(pl.Float64).sum().alias("_large_order_volume"),
        ])
        .rename({timestamp_col: "timestamp"})
        .sort("timestamp")
    )

    # Also aggregate total volume per bar for the ratio
    total_bars = (
        t
        .group_by_dynamic(timestamp_col, every=period)
        .agg(pl.col(count_col).cast(pl.Float64).sum().alias("_total_volume"))
        .rename({timestamp_col: "timestamp"})
        .sort("timestamp")
    )

    return large_bars.join(total_bars, on="timestamp", how="left")


def _add_large_order(
    df: pl.DataFrame,
    trades: pl.DataFrame,
    *,
    period: str,
    timestamp_col: str,
    count_col: str,
    side_col: str,
) -> pl.DataFrame:
    """
    Join large order footprint features into the bar DataFrame.

    Adds: ``large_order_direction``, ``large_order_conviction``,
    ``large_order_count``, ``large_order_volume_ratio``.
    """
    _nan = pl.lit(None).cast(pl.Float64)
    null_cols = [
        _nan.alias("large_order_direction"),
        _nan.alias("large_order_conviction"),
        pl.lit(None).cast(pl.Int64).alias("large_order_count"),
        _nan.alias("large_order_volume_ratio"),
    ]

    lo_bars = _compute_large_order_bars(
        trades, period=period, timestamp_col=timestamp_col,
        count_col=count_col, side_col=side_col,
    )
    if lo_bars is None:
        return df.with_columns(null_cols)

    df = df.join(lo_bars, on="timestamp", how="left")

    # Compute volume ratio from joined columns
    ratio = (
        pl.col("_large_order_volume") / (pl.col("_total_volume") + _EPSILON)
    ).alias("large_order_volume_ratio")

    df = df.with_columns(ratio)

    # Drop internal columns
    for col in ("_large_order_volume", "_total_volume"):
        if col in df.columns:
            df = df.drop(col)

    # Fill nulls for bars with no large orders
    df = df.with_columns([
        pl.col("large_order_direction").fill_null(0.0),
        pl.col("large_order_conviction").fill_null(0.0),
        pl.col("large_order_count").fill_null(0),
        pl.col("large_order_volume_ratio").fill_null(0.0),
    ])

    return df


# ── Unexplained move ──────────────────────────────────────────────────────────

def _load_base_rate_db_for_flow(db_path: str | None) -> dict[str, Any]:
    """Load the base rate DB for catalyst classification; return empty on error."""
    if db_path is None:
        return {}
    try:
        import json
        p = Path(db_path)
        if not p.exists():
            return {}
        raw = json.loads(p.read_text(encoding="utf-8"))
        return raw.get("categories", raw)
    except Exception as exc:
        logger.debug("Could not load base rate DB for flow signal: %s", exc)
        return {}


def _classify_catalyst(
    title: str | None,
    series_ticker: str | None,
    db: dict[str, Any],
) -> tuple[float, str]:
    """
    Classify a market as having a scheduled catalyst or not.

    :returns: (has_catalyst, catalyst_type_str) — has_catalyst is 0.0 or 1.0.
    """
    if not db or (not title and not series_ticker):
        return 0.0, "none"

    import re

    def _tok(s: str) -> set[str]:
        return set(re.findall(r"[a-z0-9]+", s.lower()))

    title_toks = _tok(title or "")
    series_toks = _tok(series_ticker or "")

    best_score = 0.0
    best_cat = "none"

    for cat_name, entry in db.items():
        kws: list[str] = entry.get("keywords", [])
        patterns: list[str] = entry.get("series_patterns", [])

        score = 0.0
        for kw in kws:
            kw_toks = _tok(kw)
            overlap = len(kw_toks & title_toks) / max(len(kw_toks), 1)
            score += overlap
        for p in patterns:
            if p.upper() in series_toks:
                score += 1.0

        total_possible = max(len(kws), 1) + len(patterns)
        norm = score / total_possible

        if norm > best_score:
            best_score = norm
            best_cat = cat_name

    # Threshold: 0.10 is intentionally low — we just want to identify markets
    # that are OF THE TYPE that have scheduled announcements
    has_catalyst = 1.0 if best_score >= 0.10 else 0.0
    return has_catalyst, (best_cat if has_catalyst else "none")


def _add_unexplained_move(
    df: pl.DataFrame,
    *,
    market_title: str | None = None,
    series_ticker: str | None = None,
    base_rate_db_path: str | None = None,
    window: int = _UNEXPLAINED_MOVE_WINDOW,
) -> pl.DataFrame:
    """
    Add unexplained move features to the bar DataFrame.

    Adds: ``unexplained_move``, ``unexplained_move_z``,
    ``has_scheduled_catalyst``, ``catalyst_type``.

    ``unexplained_move`` is the signed 1-bar price change (close[t] − close[t−1]).
    Its z-score measures how extreme a move is relative to this market's own
    historical volatility distribution.  When ``has_scheduled_catalyst=0``, large
    z-score moves are more suspicious as potential informed trading.
    """
    if "close" not in df.columns:
        return df.with_columns([
            pl.lit(None).cast(pl.Float64).alias("unexplained_move"),
            pl.lit(None).cast(pl.Float64).alias("unexplained_move_z"),
            pl.lit(0.0).alias("has_scheduled_catalyst"),
            pl.lit("none").alias("catalyst_type"),
        ])

    close = pl.col("close")
    move = (close - close.shift(1)).alias("unexplained_move")
    df = df.with_columns(move)

    move_col = pl.col("unexplained_move")
    move_std = move_col.rolling_std(window)
    move_z = (move_col / (move_std + _EPSILON)).alias("unexplained_move_z")
    df = df.with_columns(move_z)

    # Catalyst classification — constant per market, broadcast across all bars
    db = _load_base_rate_db_for_flow(base_rate_db_path)
    has_catalyst, cat_type = _classify_catalyst(market_title, series_ticker, db)

    df = df.with_columns([
        pl.lit(has_catalyst).cast(pl.Float64).alias("has_scheduled_catalyst"),
        pl.lit(cat_type).alias("catalyst_type"),
    ])

    return df


# ── Top-level builder ─────────────────────────────────────────────────────────

def build_informed_flow_features(
    bars_df: pl.DataFrame,
    trades: pl.DataFrame,
    *,
    period: str = "1h",
    market_title: str | None = None,
    series_ticker: str | None = None,
    base_rate_db_path: str | None = None,
    timestamp_col: str = "traded_at",
    price_col: str = "yes_price",
    side_col: str = "side",
    count_col: str = "count",
) -> pl.DataFrame:
    """
    Add all three informed flow feature groups to an existing bar DataFrame.

    This function is called from :func:`build_kalshi_features` when
    ``"informed_flow"`` is in ``feature_groups``.  It receives both the
    already-resampled ``bars_df`` and the original raw ``trades`` so it can
    resample trade-level direction information independently.

    :param bars_df:           Bar DataFrame from :func:`resample_trades_to_bars`.
                              Must have a ``timestamp`` column and a ``close`` column.
    :param trades:            Raw trade-level DataFrame (same source as used to
                              build ``bars_df``).
    :param period:            Bar resampling period, e.g. ``"1h"`` or ``"1d"``.
    :param market_title:      Optional market title for catalyst classification.
    :param series_ticker:     Optional series ticker for catalyst classification.
    :param base_rate_db_path: Optional path to ``base_rate_db.json`` for catalyst
                              type identification.
    :param timestamp_col:     Datetime column name in ``trades``.
    :param price_col:         Yes-price column name in ``trades``.
    :param side_col:          Taker side column name in ``trades`` (``"yes"`` / ``"no"``).
    :param count_col:         Contract count column name in ``trades``.
    :returns:                 ``bars_df`` extended with all informed-flow columns.
    """
    df = _add_taker_imbalance(
        bars_df, trades,
        period=period,
        timestamp_col=timestamp_col,
        count_col=count_col,
        side_col=side_col,
    )
    df = _add_large_order(
        df, trades,
        period=period,
        timestamp_col=timestamp_col,
        count_col=count_col,
        side_col=side_col,
    )
    df = _add_unexplained_move(
        df,
        market_title=market_title,
        series_ticker=series_ticker,
        base_rate_db_path=base_rate_db_path,
    )
    return df


# ── KalshiSignalFamily-compatible objects ─────────────────────────────────────
#
# These are NOT added to ALL_KALSHI_SIGNAL_FAMILIES in signals.py —
# that list is frozen at 3 to preserve the existing test assertion.
# These are imported directly by the full backtest CLI.

KALSHI_TAKER_IMBALANCE = KalshiSignalFamily(
    name="kalshi_taker_imbalance",
    feature_col="imbalance_z",
    alt_feature_cols=("taker_imbalance",),
    direction=1,
    description=(
        "Taker imbalance z-score: sustained directional imbalance in taker-initiated "
        "volume signals informed accumulation.  Positive imbalance_z (more YES takers "
        "than usual) → insiders accumulating YES contracts → BUY YES. "
        "Requires informed_flow feature group to be pre-computed during ingest."
    ),
)

KALSHI_LARGE_ORDER = KalshiSignalFamily(
    name="kalshi_large_order",
    feature_col="large_order_direction",
    alt_feature_cols=("large_order_conviction",),
    direction=1,
    description=(
        "Large order footprint: directional mean of trades exceeding 5× the rolling "
        "median trade size.  Positive direction (large YES-taker orders) → potential "
        "informed buying → BUY YES.  Requires informed_flow feature group."
    ),
)

KALSHI_UNEXPLAINED_MOVE = KalshiSignalFamily(
    name="kalshi_unexplained_move",
    feature_col="unexplained_move_z",
    alt_feature_cols=("unexplained_move",),
    direction=1,
    description=(
        "Unexplained move z-score: large price moves in markets without a known "
        "scheduled catalyst (FOMC, CPI, etc.) are more likely to reflect edge "
        "information.  Positive move_z → follow the direction of the anomalous move. "
        "Requires informed_flow feature group."
    ),
)

ALL_INFORMED_FLOW_SIGNAL_FAMILIES = [
    KALSHI_TAKER_IMBALANCE,
    KALSHI_LARGE_ORDER,
    KALSHI_UNEXPLAINED_MOVE,
]
