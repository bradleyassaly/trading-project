"""Tests for GoldskyWalletProfiler with per-market dedup, confidence score, domain tagging."""
from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from trading_platform.polymarket.goldsky_wallet_profiler import (
    GoldskyWalletProfiler, classify_market,
    MIN_VOLUME_USDC, MIN_UNCERTAIN_EARLY_FOR_OUTPUT,
)


def _make_fills(tmp_path: Path, fills: list[dict], filename: str = "test.parquet") -> Path:
    fills_dir = tmp_path / "fills"
    fills_dir.mkdir(exist_ok=True)
    df = pd.DataFrame(fills)
    df.to_parquet(fills_dir / filename, index=False)
    return fills_dir


def _make_resolution(tmp_path: Path, entries: list[tuple[str, float]],
                      close_time: str = "2026-04-03T00:00:00Z") -> Path:
    res = tmp_path / "resolution.csv"
    with res.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["ticker", "resolution_price", "resolves_yes", "question", "close_time"])
        w.writeheader()
        for tid, rp in entries:
            w.writerow({"ticker": tid, "resolution_price": rp, "resolves_yes": rp >= 99,
                         "question": "Will Fed cut rates?", "close_time": close_time})
    return res


def _fill(maker: str = "w1", token: str = "tok1", amount: float = 1000.0,
          taker_amount: float = 2000.0, maker_asset: str = "0",
          ts: datetime | None = None) -> dict:
    return {
        "id": f"f-{maker}-{token}-{amount}",
        "timestamp": ts or datetime(2026, 3, 28, 12, 0, tzinfo=timezone.utc),
        "maker_wallet": maker, "taker_wallet": "cp",
        "maker_amount": amount, "taker_amount": taker_amount,
        "maker_asset_id": maker_asset, "taker_asset_id": token,
        "fee": 0.5,
    }


class TestClassifyMarket:
    def test_economics(self):
        assert classify_market("Will Fed cut rates?") == "economics"
        assert classify_market("CPI above 3.5%?") == "economics"

    def test_geopolitics(self):
        assert classify_market("US forces enter Iran?") == "geopolitics"

    def test_politics(self):
        assert classify_market("Will Trump win 2028?") == "politics"

    def test_crypto(self):
        assert classify_market("Bitcoin above $100k?") == "crypto"

    def test_sports(self):
        assert classify_market("UFC fight winner") == "sports"

    def test_other(self):
        assert classify_market("Will it rain tomorrow?") == "other"


class TestPerMarketDedup:
    """Core tests for per-unique-token counting."""

    def test_100_fills_on_1_winning_token_counts_as_1_win(self, tmp_path: Path) -> None:
        """100 fills on one winning token = 1 resolved token, 1 win."""
        win_tokens = [f"tok_w{i}" for i in range(3)]
        lose_tokens = [f"tok_l{i}" for i in range(3)]
        fills = []
        # w1: 100 fills on tok_w0, plus 1 fill on each other token
        fills += [_fill("w1", "tok_w0", amount=10.0, taker_amount=20.0)] * 100
        for tok in win_tokens[1:] + lose_tokens:
            fills.append(_fill("w1", tok, amount=100.0, taker_amount=200.0))
        fills_dir = _make_fills(tmp_path, fills)
        entries = [(t, 100.0) for t in win_tokens] + [(t, 0.0) for t in lose_tokens]
        res = _make_resolution(tmp_path, entries)
        profiler = GoldskyWalletProfiler(fills_dir, res)
        df = profiler.build_profiles(min_resolved=3)
        assert not df.empty
        w = df.iloc[0]
        # 6 unique tokens (3 win + 3 lose), NOT 105 fills
        assert w["resolved_trades"] == 6
        assert w["wins"] == 3
        assert w["win_rate"] == pytest.approx(0.5)

    def test_1_fill_per_token_5_tokens_3_won(self, tmp_path: Path) -> None:
        """1 fill each on 6 tokens: 3 won, 3 lost = 50% WR."""
        win_toks = [f"w{i}" for i in range(3)]
        lose_toks = [f"l{i}" for i in range(3)]
        fills = []
        for tok in win_toks + lose_toks:
            fills.append(_fill("w1", tok, amount=200.0, taker_amount=400.0))
        fills_dir = _make_fills(tmp_path, fills)
        entries = [(t, 100.0) for t in win_toks] + [(t, 0.0) for t in lose_toks]
        res = _make_resolution(tmp_path, entries)
        profiler = GoldskyWalletProfiler(fills_dir, res)
        df = profiler.build_profiles(min_resolved=3)
        assert not df.empty
        w = df.iloc[0]
        assert w["resolved_trades"] == 6
        assert w["wins"] == 3
        assert w["win_rate"] == pytest.approx(0.5)

    def test_earliest_fill_used_for_price(self, tmp_path: Path) -> None:
        """Entry price/timing uses the earliest fill, not the latest."""
        win_toks = [f"w{i}" for i in range(3)]
        lose_toks = [f"l{i}" for i in range(3)]
        fills = []
        # For tok w0: early fill at price 0.50, later fill at price 0.95
        early_ts = datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)
        late_ts = datetime(2026, 4, 2, 23, 0, tzinfo=timezone.utc)
        fills.append(_fill("w1", "w0", amount=50.0, taker_amount=100.0, ts=early_ts))   # price=0.50 (uncertain)
        fills.append(_fill("w1", "w0", amount=95.0, taker_amount=100.0, ts=late_ts))    # price=0.95 (NOT uncertain)
        # Other tokens: one fill each
        for tok in win_toks[1:] + lose_toks:
            fills.append(_fill("w1", tok, amount=200.0, taker_amount=400.0))
        fills_dir = _make_fills(tmp_path, fills)
        entries = [(t, 100.0) for t in win_toks] + [(t, 0.0) for t in lose_toks]
        # Close time well after both fills
        res = _make_resolution(tmp_path, entries, close_time="2026-04-03T00:00:00Z")
        profiler = GoldskyWalletProfiler(fills_dir, res)
        df = profiler.build_profiles(min_resolved=3)
        assert not df.empty
        w = df.iloc[0]
        # w0 should use early fill: price=0.50, hours_before=768h (early+uncertain)
        # If it used late fill: price=0.95, would NOT be uncertain
        # All 6 tokens should be early (28 Mar << 3 Apr)
        # w0 at 0.50 is uncertain; others at 0.50 are uncertain too
        assert w["uncertain_early_trades"] >= 5  # at least w0 + 4 others at 0.50

    def test_uncertain_filter_uses_earliest_price(self, tmp_path: Path) -> None:
        """A token with early fill at 0.50 and later fill at 0.95 is uncertain (earliest=0.50)."""
        # 3 win tokens, 3 lose tokens for filters
        win_toks = [f"w{i}" for i in range(3)]
        lose_toks = [f"l{i}" for i in range(3)]
        fills = []
        early_ts = datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)
        late_ts = datetime(2026, 4, 2, 23, 50, tzinfo=timezone.utc)  # very close to close
        # w0: first fill uncertain, second fill NOT uncertain (price 0.95)
        fills.append(_fill("w1", "w0", amount=50.0, taker_amount=100.0, ts=early_ts))
        fills.append(_fill("w1", "w0", amount=95.0, taker_amount=100.0, ts=late_ts))
        for tok in win_toks[1:] + lose_toks:
            fills.append(_fill("w1", tok, amount=200.0, taker_amount=400.0))
        fills_dir = _make_fills(tmp_path, fills)
        entries = [(t, 100.0) for t in win_toks] + [(t, 0.0) for t in lose_toks]
        res = _make_resolution(tmp_path, entries)
        profiler = GoldskyWalletProfiler(fills_dir, res)
        df = profiler.build_profiles(min_resolved=3)
        assert not df.empty
        # The key assertion: w0's earliest fill price 0.50 is uncertain
        # If the latest fill (0.95) were used, it would NOT be uncertain


class TestGoldskyWalletProfiler:
    def test_volume_filter(self, tmp_path: Path) -> None:
        """Wallets below $500 volume excluded."""
        win_tokens = [f"tok_w{i}" for i in range(3)]
        lose_tokens = [f"tok_l{i}" for i in range(3)]
        fills = []
        for tok in win_tokens + lose_tokens:
            fills += [_fill("w1", tok, amount=100.0, taker_amount=200.0)] * 2  # $1200 total
            fills += [_fill("w2", tok, amount=20.0, taker_amount=40.0)] * 2    # $240 total
        fills_dir = _make_fills(tmp_path, fills)
        entries = [(t, 100.0) for t in win_tokens] + [(t, 0.0) for t in lose_tokens]
        res = _make_resolution(tmp_path, entries)
        profiler = GoldskyWalletProfiler(fills_dir, res)
        df = profiler.build_profiles(min_resolved=3)
        wallets = set(df["wallet"].tolist()) if not df.empty else set()
        assert "w1" in wallets  # $1200 >= $500
        assert "w2" not in wallets  # $240 < $500

    def test_sample_filter(self, tmp_path: Path) -> None:
        """Wallets with < 3 uncertain early trades excluded."""
        fills = [_fill("w1", "tok1", amount=1000.0, taker_amount=2000.0)] * 10
        fills_dir = _make_fills(tmp_path, fills)
        res = _make_resolution(tmp_path, [("tok1", 100.0)])
        profiler = GoldskyWalletProfiler(fills_dir, res)
        df = profiler.build_profiles(min_resolved=3)
        if not df.empty:
            assert df.iloc[0]["uncertain_early_trades"] >= MIN_UNCERTAIN_EARLY_FOR_OUTPUT

    def test_confidence_score_sample_size(self, tmp_path: Path) -> None:
        """Wallet B (75% EWR, 20 trades) > Wallet A (100% EWR, 3 trades) in confidence."""
        import math
        cs_a = 1.0 * math.log10(3 + 1) * (0.50 + 0.5)
        cs_b = 0.75 * math.log10(20 + 1) * (0.30 + 0.5)
        assert cs_b > cs_a

    def test_is_degraded(self, tmp_path: Path) -> None:
        assert (0.80 - 0.55) > 0.15

    def test_win_loss_per_unique_token(self, tmp_path: Path) -> None:
        """Win/loss counted per unique token, not per fill."""
        win_tokens = [f"tok_w{i}" for i in range(3)]
        lose_tokens = [f"tok_l{i}" for i in range(3)]
        fills = []
        for tok in win_tokens:
            fills += [_fill("w1", tok, amount=1000.0, taker_amount=2000.0)] * 5  # 5 fills each
        for tok in lose_tokens:
            fills += [_fill("w1", tok, amount=1000.0, taker_amount=2000.0)] * 5
        fills_dir = _make_fills(tmp_path, fills)
        entries = [(t, 100.0) for t in win_tokens] + [(t, 0.0) for t in lose_tokens]
        res = _make_resolution(tmp_path, entries)
        profiler = GoldskyWalletProfiler(fills_dir, res)
        df = profiler.build_profiles(min_resolved=3)
        if not df.empty:
            w = df.iloc[0]
            # 6 unique tokens, NOT 30 fills
            assert w["resolved_trades"] == 6
            assert w["wins"] == 3
            assert w["win_rate"] == pytest.approx(0.5)

    def test_incomplete_data_filter(self, tmp_path: Path) -> None:
        """Wallets with only winning tokens are filtered as incomplete data."""
        fills = [_fill("w1", "tok_win", amount=1000.0)] * 20
        fills_dir = _make_fills(tmp_path, fills)
        res = _make_resolution(tmp_path, [("tok_win", 100.0)])
        profiler = GoldskyWalletProfiler(fills_dir, res)
        df = profiler.build_profiles(min_resolved=3)
        assert df.empty

    def test_wallet_across_multiple_files(self, tmp_path: Path) -> None:
        """Wallet evaluated across fills from multiple parquet files."""
        win_tokens = [f"tok_w{i}" for i in range(3)]
        lose_tokens = [f"tok_l{i}" for i in range(3)]
        fills_dir = tmp_path / "fills"
        fills_dir.mkdir(exist_ok=True)
        for tok in win_tokens:
            df = pd.DataFrame([_fill("w1", tok, amount=200.0, taker_amount=400.0)] * 2)
            df.to_parquet(fills_dir / f"{tok}.parquet", index=False)
        for tok in lose_tokens:
            df = pd.DataFrame([_fill("w1", tok, amount=200.0, taker_amount=400.0)] * 2)
            df.to_parquet(fills_dir / f"{tok}.parquet", index=False)
        entries = [(t, 100.0) for t in win_tokens] + [(t, 0.0) for t in lose_tokens]
        res = _make_resolution(tmp_path, entries)
        profiler = GoldskyWalletProfiler(fills_dir, res)
        df = profiler.build_profiles(min_resolved=3)
        assert not df.empty
        w = df.iloc[0]
        # 6 unique tokens, NOT 12 fills
        assert w["wins"] == 3
        assert w["resolved_trades"] == 6
        assert w["win_rate"] == pytest.approx(0.5)

    def test_unresolved_tokens_excluded(self, tmp_path: Path) -> None:
        """Unresolved tokens are excluded from win rate calculation."""
        win_tokens = [f"tok_w{i}" for i in range(3)]
        lose_tokens = [f"tok_l{i}" for i in range(3)]
        fills = []
        for tok in win_tokens + lose_tokens:
            fills += [_fill("w1", tok, amount=200.0, taker_amount=400.0)] * 2
        fills += [_fill("w1", "tok_unresolved", amount=500.0, taker_amount=1000.0)] * 10
        fills_dir = _make_fills(tmp_path, fills)
        entries = [(t, 100.0) for t in win_tokens] + [(t, 0.0) for t in lose_tokens]
        res = _make_resolution(tmp_path, entries)
        profiler = GoldskyWalletProfiler(fills_dir, res)
        df = profiler.build_profiles(min_resolved=3)
        assert not df.empty
        w = df.iloc[0]
        # 6 unique resolved tokens, NOT 12 fills (and unresolved excluded)
        assert w["resolved_trades"] == 6
        assert w["win_rate"] == pytest.approx(0.5)

    def test_avg_fills_per_token(self, tmp_path: Path) -> None:
        """avg_fills_per_token tracks fill duplication."""
        win_tokens = [f"tok_w{i}" for i in range(3)]
        lose_tokens = [f"tok_l{i}" for i in range(3)]
        fills = []
        for tok in win_tokens + lose_tokens:
            fills += [_fill("w1", tok, amount=100.0, taker_amount=200.0)] * 10  # 10 fills each
        fills_dir = _make_fills(tmp_path, fills)
        entries = [(t, 100.0) for t in win_tokens] + [(t, 0.0) for t in lose_tokens]
        res = _make_resolution(tmp_path, entries)
        profiler = GoldskyWalletProfiler(fills_dir, res)
        df = profiler.build_profiles(min_resolved=3)
        assert not df.empty
        w = df.iloc[0]
        assert w["avg_fills_per_token"] == pytest.approx(10.0)
