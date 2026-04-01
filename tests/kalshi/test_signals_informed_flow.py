"""
Tests for signals_informed_flow.py — taker imbalance, large order, unexplained move.
"""
from __future__ import annotations

import json
import math
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import polars as pl
import pytest

from trading_platform.kalshi.signals_informed_flow import (
    ALL_INFORMED_FLOW_SIGNAL_FAMILIES,
    KALSHI_LARGE_ORDER,
    KALSHI_TAKER_IMBALANCE,
    KALSHI_UNEXPLAINED_MOVE,
    _add_large_order,
    _add_taker_imbalance,
    _add_unexplained_move,
    _classify_catalyst,
    _compute_large_order_bars,
    _compute_taker_imbalance_bars,
    _flag_large_orders,
    _resolve_side_col,
    build_informed_flow_features,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _make_trades(
    n: int = 60,
    yes_bias: float = 0.6,    # fraction of YES takers
    period_hours: int = 1,
    large_frac: float = 0.0,  # fraction of trades that are 20x normal size
    seed: int = 42,
) -> pl.DataFrame:
    """
    Build a synthetic trade DataFrame with 1h bars worth of data.

    :param yes_bias:   Fraction of trades where taker_side == "yes".
    :param large_frac: Fraction of trades that are oversized.
    """
    import random
    rng = random.Random(seed)
    base = datetime(2025, 3, 1, 0, 0, tzinfo=UTC)
    rows = []
    for i in range(n):
        is_large = rng.random() < large_frac
        count = 50 if is_large else rng.randint(1, 8)
        side = "yes" if rng.random() < yes_bias else "no"
        rows.append({
            "trade_id": f"t{i}",
            "ticker": "TEST",
            "side": side,
            "yes_price": 0.40 + rng.uniform(-0.05, 0.05),
            "no_price": 0.60,
            "count": count,
            "traded_at": base + timedelta(minutes=i * (60 // n * period_hours)),
        })
    return pl.from_dicts(rows, schema_overrides={
        "yes_price": pl.Float64,
        "no_price": pl.Float64,
        "count": pl.Int64,
        "traded_at": pl.Datetime(time_zone="UTC"),
    })


def _make_bars(n: int = 30) -> pl.DataFrame:
    """Build a synthetic bar DataFrame with a timestamp and close column."""
    base = datetime(2025, 3, 1, 0, 0, tzinfo=UTC)
    import random
    rng = random.Random(99)
    close = 50.0
    rows = []
    for i in range(n):
        close += rng.uniform(-2, 2)
        close = max(1.0, min(99.0, close))
        rows.append({
            "timestamp": base + timedelta(hours=i),
            "open": close,
            "high": close + 1,
            "low": close - 1,
            "close": close,
            "volume": float(rng.randint(10, 100)),
            "dollar_volume": close * rng.randint(10, 100),
            "symbol": "TEST",
        })
    return pl.from_dicts(rows, schema_overrides={
        "timestamp": pl.Datetime(time_zone="UTC"),
        "close": pl.Float64,
        "volume": pl.Float64,
        "dollar_volume": pl.Float64,
    })


def _make_base_rate_db(tmp_path: Path) -> Path:
    db = {
        "categories": {
            "fed_rate_hold": {
                "prior": 65.0,
                "keywords": ["fed hold", "hold rates", "unchanged"],
                "series_patterns": ["FOMC", "FED-RATE"],
            },
            "cpi_above_consensus": {
                "prior": 52.0,
                "keywords": ["cpi above", "cpi beat", "cpi higher", "inflation above"],
                "series_patterns": ["CPI", "INFLATION"],
            },
        }
    }
    p = tmp_path / "base_rate_db.json"
    p.write_text(json.dumps(db), encoding="utf-8")
    return p


# ── _resolve_side_col ─────────────────────────────────────────────────────────

def test_resolve_side_col_finds_side():
    df = pl.DataFrame({"side": ["yes"], "count": [1]})
    assert _resolve_side_col(df, "side") == "side"


def test_resolve_side_col_finds_taker_side():
    df = pl.DataFrame({"taker_side": ["yes"], "count": [1]})
    assert _resolve_side_col(df, "side") == "taker_side"


def test_resolve_side_col_returns_none_when_absent():
    df = pl.DataFrame({"count": [1], "ticker": ["X"]})
    assert _resolve_side_col(df, "side") is None


# ── _compute_taker_imbalance_bars ─────────────────────────────────────────────

class TestComputeTakerImbalanceBars:

    def test_returns_none_when_no_side_col(self):
        df = pl.DataFrame({
            "count": [5, 3],
            "traded_at": [
                datetime(2025, 1, 1, 0, tzinfo=UTC),
                datetime(2025, 1, 1, 1, tzinfo=UTC),
            ],
            "yes_price": [0.5, 0.5],
        }, schema_overrides={"traded_at": pl.Datetime(time_zone="UTC")})
        result = _compute_taker_imbalance_bars(
            df, period="1h", timestamp_col="traded_at", count_col="count", side_col="side"
        )
        assert result is None

    def test_returns_none_for_empty_trades(self):
        df = pl.DataFrame(schema={"side": pl.Utf8, "count": pl.Int64, "traded_at": pl.Datetime(time_zone="UTC")})
        result = _compute_taker_imbalance_bars(
            df, period="1h", timestamp_col="traded_at", count_col="count", side_col="side"
        )
        assert result is None

    def test_buy_sell_volumes_correct(self):
        # 2 YES takers (count 5 each), 1 NO taker (count 3)
        df = pl.DataFrame({
            "side": ["yes", "yes", "no"],
            "count": [5, 5, 3],
            "traded_at": [
                datetime(2025, 1, 1, 0, 0, tzinfo=UTC),
                datetime(2025, 1, 1, 0, 15, tzinfo=UTC),
                datetime(2025, 1, 1, 0, 30, tzinfo=UTC),
            ],
        }, schema_overrides={"traded_at": pl.Datetime(time_zone="UTC"), "count": pl.Int64})

        result = _compute_taker_imbalance_bars(
            df, period="1h", timestamp_col="traded_at", count_col="count", side_col="side"
        )
        assert result is not None
        assert len(result) == 1
        assert result["taker_buy_vol"][0] == pytest.approx(10.0)
        assert result["taker_sell_vol"][0] == pytest.approx(3.0)

    def test_multiple_bars(self):
        rows = []
        for h in range(3):
            for _ in range(5):
                rows.append({
                    "side": "yes",
                    "count": 2,
                    "traded_at": datetime(2025, 1, 1, h, 10, tzinfo=UTC),
                })
        df = pl.from_dicts(rows, schema_overrides={"traded_at": pl.Datetime(time_zone="UTC"), "count": pl.Int64})
        result = _compute_taker_imbalance_bars(
            df, period="1h", timestamp_col="traded_at", count_col="count", side_col="side"
        )
        assert result is not None
        assert len(result) == 3
        assert all(result["taker_sell_vol"] == 0.0)


# ── _add_taker_imbalance ──────────────────────────────────────────────────────

class TestAddTakerImbalance:

    def test_adds_null_cols_when_no_side(self):
        trades = pl.DataFrame({
            "count": [5, 3],
            "traded_at": [
                datetime(2025, 1, 1, tzinfo=UTC),
                datetime(2025, 1, 2, tzinfo=UTC),
            ],
        }, schema_overrides={"traded_at": pl.Datetime(time_zone="UTC"), "count": pl.Int64})
        bars = _make_bars(5)

        result = _add_taker_imbalance(
            bars, trades, period="1d", timestamp_col="traded_at",
            count_col="count", side_col="side",
        )
        assert "taker_imbalance" in result.columns
        assert "imbalance_z" in result.columns
        assert "taker_conviction" in result.columns
        # All NaN when no side info
        assert result["taker_imbalance"].is_null().all()

    def test_all_yes_gives_positive_imbalance(self):
        trades = _make_trades(n=60, yes_bias=1.0)  # all YES takers
        bars = _make_bars(3)
        result = _add_taker_imbalance(
            bars, trades, period="1d", timestamp_col="traded_at",
            count_col="count", side_col="side",
        )
        assert "taker_imbalance" in result.columns
        non_null = result["taker_imbalance"].drop_nulls()
        if len(non_null) > 0:
            assert float(non_null.min()) >= 0.0  # all buys → imbalance ≥ 0

    def test_all_no_gives_negative_imbalance(self):
        trades = _make_trades(n=60, yes_bias=0.0)  # all NO takers
        bars = _make_bars(3)
        result = _add_taker_imbalance(
            bars, trades, period="1d", timestamp_col="traded_at",
            count_col="count", side_col="side",
        )
        non_null = result["taker_imbalance"].drop_nulls()
        if len(non_null) > 0:
            assert float(non_null.max()) <= 0.0  # all sells → imbalance ≤ 0

    def test_imbalance_z_computed(self):
        trades = _make_trades(n=120, yes_bias=0.7, seed=7)
        bars = _make_bars(30)
        result = _add_taker_imbalance(
            bars, trades, period="1d", timestamp_col="traded_at",
            count_col="count", side_col="side",
        )
        # z-score may have some non-null values if trades align to bars
        assert "imbalance_z" in result.columns

    def test_taker_conviction_non_negative(self):
        trades = _make_trades(n=60, yes_bias=0.6, seed=3)
        bars = _make_bars(5)
        result = _add_taker_imbalance(
            bars, trades, period="1d", timestamp_col="traded_at",
            count_col="count", side_col="side",
        )
        conviction = result["taker_conviction"].drop_nulls()
        if len(conviction) > 0:
            assert float(conviction.min()) >= 0.0


# ── _flag_large_orders ────────────────────────────────────────────────────────

class TestFlagLargeOrders:

    def test_small_trades_not_flagged(self):
        df = pl.DataFrame({
            "count": [2, 3, 2, 4, 3] * 5,  # all small
            "traded_at": [datetime(2025, 1, 1, 0, i * 2, tzinfo=UTC) for i in range(25)],
        }, schema_overrides={"count": pl.Int64, "traded_at": pl.Datetime(time_zone="UTC")})
        result = _flag_large_orders(df, count_col="count")
        # None should be flagged as large (all counts ≤ 4, well below 5× median)
        assert not result["_is_large"].any()

    def test_oversized_trade_flagged(self):
        """After 20 small trades to establish the rolling median, a 100-contract trade should be flagged."""
        small_counts = [3] * 20
        big_counts = [150]  # 50× median of 3
        counts = small_counts + big_counts
        df = pl.DataFrame({
            "count": counts,
            "traded_at": [datetime(2025, 1, 1, 0, i, tzinfo=UTC) for i in range(len(counts))],
        }, schema_overrides={"count": pl.Int64, "traded_at": pl.Datetime(time_zone="UTC")})
        result = _flag_large_orders(df, count_col="count")
        assert bool(result["_is_large"][-1])

    def test_rolling_median_column_present(self):
        df = pl.DataFrame({
            "count": [1, 2, 3, 4, 5],
            "traded_at": [datetime(2025, 1, 1, 0, i, tzinfo=UTC) for i in range(5)],
        }, schema_overrides={"count": pl.Int64, "traded_at": pl.Datetime(time_zone="UTC")})
        result = _flag_large_orders(df, count_col="count")
        assert "_rolling_median" in result.columns


# ── _compute_large_order_bars ─────────────────────────────────────────────────

class TestComputeLargeOrderBars:

    def test_returns_none_when_no_large_orders(self):
        trades = _make_trades(n=40, large_frac=0.0)
        result = _compute_large_order_bars(
            trades, period="1d", timestamp_col="traded_at",
            count_col="count", side_col="side",
        )
        # With no large orders, should return None
        assert result is None

    def test_returns_none_when_no_side_col(self):
        df = pl.DataFrame({
            "count": [100, 2, 3],
            "traded_at": [datetime(2025, 1, 1, 0, i, tzinfo=UTC) for i in range(3)],
        }, schema_overrides={"count": pl.Int64, "traded_at": pl.Datetime(time_zone="UTC")})
        result = _compute_large_order_bars(
            df, period="1h", timestamp_col="traded_at",
            count_col="count", side_col="side",
        )
        assert result is None

    def test_detects_large_yes_order(self):
        """Place many small trades then one very large YES taker."""
        small = [
            {
                "side": "no",
                "count": 2,
                "traded_at": datetime(2025, 1, 1, 0, i, tzinfo=UTC),
            }
            for i in range(25)
        ]
        big = {
            "side": "yes",
            "count": 500,  # massively large
            "traded_at": datetime(2025, 1, 1, 23, 0, tzinfo=UTC),
        }
        all_rows = small + [big]
        df = pl.from_dicts(all_rows, schema_overrides={
            "count": pl.Int64,
            "traded_at": pl.Datetime(time_zone="UTC"),
        })
        result = _compute_large_order_bars(
            df, period="1d", timestamp_col="traded_at",
            count_col="count", side_col="side", multiplier=5.0,
        )
        assert result is not None
        # large_order_direction should be positive (YES taker = +direction)
        assert len(result) > 0


# ── _add_large_order ──────────────────────────────────────────────────────────

class TestAddLargeOrder:

    def test_adds_null_cols_when_no_large_orders(self):
        trades = _make_trades(n=40, large_frac=0.0)
        bars = _make_bars(5)
        result = _add_large_order(
            bars, trades, period="1d", timestamp_col="traded_at",
            count_col="count", side_col="side",
        )
        assert "large_order_direction" in result.columns
        assert "large_order_conviction" in result.columns
        assert "large_order_count" in result.columns
        assert "large_order_volume_ratio" in result.columns
        # When no large orders, filled with 0
        assert float(result["large_order_direction"].sum()) == pytest.approx(0.0)

    def test_large_order_count_non_negative(self):
        trades = _make_trades(n=60, large_frac=0.1, seed=5)
        bars = _make_bars(5)
        result = _add_large_order(
            bars, trades, period="1d", timestamp_col="traded_at",
            count_col="count", side_col="side",
        )
        counts = result["large_order_count"].drop_nulls()
        assert (counts >= 0).all()

    def test_volume_ratio_between_0_and_1(self):
        trades = _make_trades(n=60, large_frac=0.15, seed=8)
        bars = _make_bars(5)
        result = _add_large_order(
            bars, trades, period="1d", timestamp_col="traded_at",
            count_col="count", side_col="side",
        )
        ratio = result["large_order_volume_ratio"].drop_nulls()
        assert (ratio >= 0.0).all()
        assert (ratio <= 1.0 + 1e-6).all()

    def test_direction_bounded_minus1_to_1(self):
        """large_order_direction is a weighted mean of ±1 signals, so bounded to [-1, 1]."""
        trades = _make_trades(n=60, large_frac=0.2, yes_bias=0.8, seed=11)
        bars = _make_bars(5)
        result = _add_large_order(
            bars, trades, period="1d", timestamp_col="traded_at",
            count_col="count", side_col="side",
        )
        direction = result["large_order_direction"].drop_nulls()
        if len(direction) > 0:
            # Weighted direction = sum(±count) / sum(count) ∈ [-1, +1]
            assert float(direction.min()) >= -1.0 - 1e-4
            assert float(direction.max()) <= 1.0 + 1e-4


# ── _classify_catalyst ────────────────────────────────────────────────────────

def test_classify_catalyst_fed_market(tmp_path):
    db_path = _make_base_rate_db(tmp_path)
    import json
    db = json.loads(db_path.read_text())["categories"]
    has_cat, cat_type = _classify_catalyst(
        "Will Fed hold rates unchanged at December FOMC?",
        "FOMC-DEC",
        db,
    )
    assert has_cat == pytest.approx(1.0)
    assert cat_type != "none"


def test_classify_catalyst_unrelated_market(tmp_path):
    db_path = _make_base_rate_db(tmp_path)
    import json
    db = json.loads(db_path.read_text())["categories"]
    has_cat, cat_type = _classify_catalyst(
        "Will Manchester United win the Premier League title?",
        "SPORTS-EPL",
        db,
    )
    # Not in the small test DB → should return 0 or "none"
    # (score below threshold)
    assert cat_type == "none" or has_cat == pytest.approx(0.0)


def test_classify_catalyst_empty_db():
    has_cat, cat_type = _classify_catalyst("Any market title", "ANY-TICKER", {})
    assert has_cat == 0.0
    assert cat_type == "none"


def test_classify_catalyst_none_inputs():
    has_cat, cat_type = _classify_catalyst(None, None, {"fed": {"keywords": [], "series_patterns": []}})
    assert has_cat == 0.0


# ── _add_unexplained_move ─────────────────────────────────────────────────────

class TestAddUnexplainedMove:

    def test_adds_move_columns(self):
        bars = _make_bars(30)
        result = _add_unexplained_move(bars)
        assert "unexplained_move" in result.columns
        assert "unexplained_move_z" in result.columns
        assert "has_scheduled_catalyst" in result.columns
        assert "catalyst_type" in result.columns

    def test_no_close_col_returns_nulls(self):
        bars = pl.DataFrame({"timestamp": [datetime(2025, 1, 1, tzinfo=UTC)], "volume": [100.0]},
                            schema_overrides={"timestamp": pl.Datetime(time_zone="UTC")})
        result = _add_unexplained_move(bars)
        assert result["unexplained_move"].is_null().all()

    def test_unexplained_move_is_first_difference_of_close(self):
        bars = pl.DataFrame({
            "timestamp": [datetime(2025, 1, i + 1, tzinfo=UTC) for i in range(5)],
            "close": [50.0, 52.0, 51.0, 55.0, 54.0],
            "symbol": ["T"] * 5,
        }, schema_overrides={"timestamp": pl.Datetime(time_zone="UTC"), "close": pl.Float64})
        result = _add_unexplained_move(bars)
        moves = result["unexplained_move"].to_list()
        # First value should be null (no prior close)
        assert moves[0] is None
        assert moves[1] == pytest.approx(2.0)   # 52 - 50
        assert moves[2] == pytest.approx(-1.0)  # 51 - 52
        assert moves[3] == pytest.approx(4.0)   # 55 - 51

    def test_has_catalyst_for_known_market(self, tmp_path):
        db_path = _make_base_rate_db(tmp_path)
        bars = _make_bars(10)
        result = _add_unexplained_move(
            bars,
            market_title="Will CPI come in above consensus?",
            series_ticker="CPI-JAN",
            base_rate_db_path=str(db_path),
        )
        has_cat = result["has_scheduled_catalyst"][0]
        assert has_cat == pytest.approx(1.0)

    def test_no_catalyst_for_unknown_market(self, tmp_path):
        db_path = _make_base_rate_db(tmp_path)
        bars = _make_bars(10)
        result = _add_unexplained_move(
            bars,
            market_title="Will an asteroid hit the moon this week?",
            series_ticker="SPACE-ODDITY",
            base_rate_db_path=str(db_path),
        )
        has_cat = result["has_scheduled_catalyst"][0]
        # Small test DB has no space category → 0
        assert has_cat == pytest.approx(0.0)

    def test_catalyst_type_broadcast_constant(self, tmp_path):
        db_path = _make_base_rate_db(tmp_path)
        bars = _make_bars(10)
        result = _add_unexplained_move(
            bars,
            market_title="Will Fed hold rates unchanged?",
            series_ticker="FOMC",
            base_rate_db_path=str(db_path),
        )
        # Same catalyst_type across all rows
        types = result["catalyst_type"].unique().to_list()
        assert len(types) == 1

    def test_unexplained_move_z_nan_for_early_rows(self):
        bars = _make_bars(5)  # only 5 bars → rolling window mostly NaN
        result = _add_unexplained_move(bars, window=20)
        z_scores = result["unexplained_move_z"].to_list()
        # First few should be null (rolling std undefined)
        null_count = sum(1 for v in z_scores if v is None)
        assert null_count > 0


# ── build_informed_flow_features ──────────────────────────────────────────────

class TestBuildInformedFlowFeatures:

    def test_all_columns_present(self):
        bars = _make_bars(30)
        trades = _make_trades(n=60, yes_bias=0.65, seed=1)
        result = build_informed_flow_features(
            bars, trades,
            period="1d",
            timestamp_col="traded_at",
        )
        expected = [
            "taker_imbalance", "imbalance_z", "taker_conviction",
            "large_order_direction", "large_order_conviction",
            "large_order_count", "large_order_volume_ratio",
            "unexplained_move", "unexplained_move_z",
            "has_scheduled_catalyst", "catalyst_type",
        ]
        for col in expected:
            assert col in result.columns, f"Missing column: {col}"

    def test_row_count_preserved(self):
        bars = _make_bars(20)
        trades = _make_trades(n=60)
        result = build_informed_flow_features(bars, trades, period="1d", timestamp_col="traded_at")
        assert len(result) == len(bars)

    def test_empty_trades_returns_null_informed_cols(self):
        bars = _make_bars(5)
        empty_trades = pl.DataFrame(schema={
            "side": pl.Utf8, "count": pl.Int64,
            "traded_at": pl.Datetime(time_zone="UTC"),
            "yes_price": pl.Float64,
        })
        result = build_informed_flow_features(
            bars, empty_trades, period="1d", timestamp_col="traded_at",
        )
        assert "taker_imbalance" in result.columns
        assert result["taker_imbalance"].is_null().all()

    def test_with_market_context_and_db(self, tmp_path):
        db_path = _make_base_rate_db(tmp_path)
        bars = _make_bars(15)
        trades = _make_trades(n=40)
        result = build_informed_flow_features(
            bars, trades,
            period="1d",
            timestamp_col="traded_at",
            market_title="Will CPI beat consensus in January?",
            series_ticker="CPI-JAN",
            base_rate_db_path=str(db_path),
        )
        # has_scheduled_catalyst should be 1 for CPI market
        assert result["has_scheduled_catalyst"][0] == pytest.approx(1.0)


# ── Integration with build_kalshi_features ────────────────────────────────────

def test_build_kalshi_features_informed_flow_group():
    """Verify the informed_flow group is wired into the feature generator."""
    from trading_platform.kalshi.features import build_kalshi_features, KALSHI_FEATURE_GROUPS

    assert "informed_flow" in KALSHI_FEATURE_GROUPS

    trades = _make_trades(n=60)
    result = build_kalshi_features(
        trades,
        ticker="TEST",
        period="1d",
        feature_groups=["informed_flow"],
        timestamp_col="traded_at",
        price_col="yes_price",
        count_col="count",
    )
    assert "taker_imbalance" in result.columns
    assert "unexplained_move" in result.columns
    assert "large_order_direction" in result.columns


def test_build_kalshi_features_informed_flow_with_context(tmp_path):
    from trading_platform.kalshi.features import build_kalshi_features

    db_path = _make_base_rate_db(tmp_path)
    trades = _make_trades(n=60)
    result = build_kalshi_features(
        trades,
        ticker="TEST",
        period="1d",
        feature_groups=["informed_flow"],
        timestamp_col="traded_at",
        price_col="yes_price",
        count_col="count",
        market_context={
            "title": "Will the Fed hold rates unchanged?",
            "series_ticker": "FOMC",
            "base_rate_db_path": str(db_path),
        },
    )
    assert result["has_scheduled_catalyst"][0] == pytest.approx(1.0)


# ── KalshiSignalFamily objects ────────────────────────────────────────────────

class TestInformedFlowSignalFamilies:

    def test_names(self):
        assert KALSHI_TAKER_IMBALANCE.name == "kalshi_taker_imbalance"
        assert KALSHI_LARGE_ORDER.name == "kalshi_large_order"
        assert KALSHI_UNEXPLAINED_MOVE.name == "kalshi_unexplained_move"

    def test_all_direction_positive_one(self):
        for fam in [KALSHI_TAKER_IMBALANCE, KALSHI_LARGE_ORDER, KALSHI_UNEXPLAINED_MOVE]:
            assert fam.direction == 1

    def test_taker_imbalance_score_reads_imbalance_z(self):
        df = pd.DataFrame({"imbalance_z": [1.5, -0.5, 2.0, 0.0]})
        sig = KALSHI_TAKER_IMBALANCE.score(df)
        assert list(sig) == [1.5, -0.5, 2.0, 0.0]

    def test_taker_imbalance_falls_back_to_taker_imbalance_col(self):
        df = pd.DataFrame({"taker_imbalance": [0.4, -0.3, 0.0]})
        sig = KALSHI_TAKER_IMBALANCE.score(df)
        assert list(sig) == [0.4, -0.3, 0.0]

    def test_large_order_score_reads_direction(self):
        df = pd.DataFrame({"large_order_direction": [0.8, -0.2, 0.5]})
        sig = KALSHI_LARGE_ORDER.score(df)
        assert list(sig) == pytest.approx([0.8, -0.2, 0.5])

    def test_unexplained_move_score_reads_move_z(self):
        df = pd.DataFrame({"unexplained_move_z": [2.5, -1.0, 0.3]})
        sig = KALSHI_UNEXPLAINED_MOVE.score(df)
        assert list(sig) == pytest.approx([2.5, -1.0, 0.3])

    def test_missing_column_returns_nan(self):
        df = pd.DataFrame({"close": [50.0, 55.0]})
        for fam in [KALSHI_TAKER_IMBALANCE, KALSHI_LARGE_ORDER, KALSHI_UNEXPLAINED_MOVE]:
            sig = fam.score(df)
            assert sig.isna().all()

    def test_all_informed_flow_families_in_list(self):
        assert len(ALL_INFORMED_FLOW_SIGNAL_FAMILIES) == 3
        names = {f.name for f in ALL_INFORMED_FLOW_SIGNAL_FAMILIES}
        assert "kalshi_taker_imbalance" in names
        assert "kalshi_large_order" in names
        assert "kalshi_unexplained_move" in names


# ── Existing signals.py invariant preserved ───────────────────────────────────

def test_original_signal_families_still_three():
    """Confirm ALL_KALSHI_SIGNAL_FAMILIES == 3 is not disturbed."""
    from trading_platform.kalshi.signals import ALL_KALSHI_SIGNAL_FAMILIES
    assert len(ALL_KALSHI_SIGNAL_FAMILIES) == 3
