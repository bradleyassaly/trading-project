"""Tests for Polymarket wallet profiler."""
from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd
import pytest

from trading_platform.polymarket.wallet_profiler import WalletProfiler


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


TRADE_FIELDS = ["block_number", "timestamp", "tx_hash", "wallet", "token_id",
                "side", "tokens", "price", "total_usdc"]
RES_FIELDS = ["ticker", "resolution_price", "resolves_yes", "close_time"]


def _trades(wallet: str, token_id: str, side: str, n: int = 5,
            base_ts: str = "2026-03-30T") -> list[dict]:
    """Generate trades with timestamps starting at base_ts."""
    return [
        {
            "block_number": str(i), "timestamp": f"{base_ts}{i:02d}:00:00+00:00",
            "tx_hash": f"0x{wallet}{i}", "wallet": wallet, "token_id": token_id,
            "side": side, "tokens": "10", "price": "0.65", "total_usdc": "100.0",
        }
        for i in range(n)
    ]


def _resolution(ticker: str, price: float, close_time: str = "2026-04-03T00:00:00+00:00") -> dict:
    return {
        "ticker": ticker,
        "resolution_price": str(price),
        "resolves_yes": str(price >= 99),
        "close_time": close_time,
    }


class TestWalletProfiler:
    def test_build_profiles_happy_path(self, tmp_path: Path) -> None:
        trades_csv = tmp_path / "trades.csv"
        res_csv = tmp_path / "resolution.csv"

        # Trades placed 3-4 days before close (early = >24h before close)
        trades = _trades("walletA", "mkt1xxxxxxxxxxxx", "BUY", 10, base_ts="2026-03-30T") + \
                 _trades("walletB", "mkt1xxxxxxxxxxxx", "SELL", 10, base_ts="2026-03-30T")
        _write_csv(trades_csv, trades, TRADE_FIELDS)
        _write_csv(res_csv, [_resolution("mkt1xxxxxxxxxxxx", 100.0, "2026-04-03T00:00:00+00:00")],
                   RES_FIELDS)

        result = WalletProfiler().build_profiles(
            trades_csv, res_csv, tmp_path / "profiles.parquet",
            min_resolved_trades=5, min_early_trades=5,
        )

        assert result.wallets_with_resolved_trades == 2

        df = pd.read_parquet(tmp_path / "profiles.parquet")
        assert len(df) == 2

        wallet_a = df[df["wallet"] == "walletA"].iloc[0]
        assert wallet_a["win_rate"] == 1.0
        assert wallet_a["edge"] == 0.5
        assert wallet_a["early_trades"] == 10  # all >24h before close
        assert wallet_a["early_win_rate"] == 1.0
        assert wallet_a["is_early_informed"] == True
        assert wallet_a["smart_money"] == True

        # walletB sold YES but market resolved YES → all losses
        wallet_b = df[df["wallet"] == "walletB"].iloc[0]
        assert wallet_b["win_rate"] == 0.0
        assert wallet_b["early_win_rate"] == 0.0
        assert wallet_b["is_early_informed"] == False

    def test_late_trader_not_flagged(self, tmp_path: Path) -> None:
        """Wallet trading <24h before close should NOT be flagged as informed."""
        trades_csv = tmp_path / "trades.csv"
        res_csv = tmp_path / "resolution.csv"

        # Trades 1 hour before close — late arbitrageur
        trades = _trades("late_wallet", "mkt1xxxxxxxxxxxx", "BUY", 10,
                         base_ts="2026-04-02T23:")  # April 2 23:XX, close April 3 00:00
        # Fix timestamps to be within 1 hour of close
        for i, t in enumerate(trades):
            t["timestamp"] = f"2026-04-02T23:{i:02d}:00+00:00"

        _write_csv(trades_csv, trades, TRADE_FIELDS)
        _write_csv(res_csv, [_resolution("mkt1xxxxxxxxxxxx", 100.0, "2026-04-03T00:00:00+00:00")],
                   RES_FIELDS)

        result = WalletProfiler().build_profiles(
            trades_csv, res_csv, tmp_path / "profiles.parquet",
            min_resolved_trades=5, min_early_trades=5,
        )

        df = pd.read_parquet(tmp_path / "profiles.parquet")
        wallet = df[df["wallet"] == "late_wallet"].iloc[0]
        assert wallet["win_rate"] == 1.0  # wins overall
        assert wallet["early_trades"] == 0  # but no early trades
        assert wallet["early_win_rate"] == 0.0
        assert wallet["is_early_informed"] == False
        assert wallet["smart_money"] == False

    def test_early_columns_present(self, tmp_path: Path) -> None:
        trades_csv = tmp_path / "trades.csv"
        res_csv = tmp_path / "resolution.csv"

        trades = _trades("w1", "mkt1xxxxxxxxxxxx", "BUY", 10)
        _write_csv(trades_csv, trades, TRADE_FIELDS)
        _write_csv(res_csv, [_resolution("mkt1xxxxxxxxxxxx", 100.0)], RES_FIELDS)

        WalletProfiler().build_profiles(
            trades_csv, res_csv, tmp_path / "profiles.parquet",
            min_resolved_trades=5,
        )

        df = pd.read_parquet(tmp_path / "profiles.parquet")
        for col in ["early_trades", "early_wins", "early_win_rate",
                     "avg_hours_before_close", "is_early_informed"]:
            assert col in df.columns, f"Missing column: {col}"

    def test_smart_money_signal(self, tmp_path: Path) -> None:
        trades_csv = tmp_path / "trades.csv"
        profiles_path = tmp_path / "profiles.parquet"

        trades = _trades("smart_wallet", "mkt1xxxxxxxxxxxx", "BUY", 5)
        _write_csv(trades_csv, trades, TRADE_FIELDS)

        profiles = pd.DataFrame([{
            "wallet": "smart_wallet", "total_trades": 100, "resolved_trades": 50,
            "wins": 40, "win_rate": 0.80, "early_trades": 30, "early_wins": 22,
            "early_win_rate": 0.73, "avg_hours_before_close": 48.0,
            "total_volume_usdc": 50000, "avg_trade_size": 500,
            "markets_traded": 20, "edge": 0.30,
            "is_early_informed": True, "smart_money": True,
        }])
        profiles.to_parquet(profiles_path, index=False)

        signal = WalletProfiler.get_smart_money_signal(
            trades_csv, profiles_path, "mkt1xxxxxxxxxxxx",
        )
        assert signal == pytest.approx(1.0)

    def test_no_smart_money_returns_zero(self, tmp_path: Path) -> None:
        signal = WalletProfiler.get_smart_money_signal(
            tmp_path / "missing.csv", tmp_path / "missing.parquet", "mkt1",
        )
        assert signal == 0.0

    def test_min_resolved_trades_filter(self, tmp_path: Path) -> None:
        trades_csv = tmp_path / "trades.csv"
        res_csv = tmp_path / "resolution.csv"

        trades = _trades("walletA", "mkt1xxxxxxxxxxxx", "BUY", 3)
        _write_csv(trades_csv, trades, TRADE_FIELDS)
        _write_csv(res_csv, [_resolution("mkt1xxxxxxxxxxxx", 100.0)], RES_FIELDS)

        result = WalletProfiler().build_profiles(
            trades_csv, res_csv, tmp_path / "profiles.parquet",
            min_resolved_trades=5,
        )
        assert result.wallets_with_resolved_trades == 0

    def test_no_close_time_still_works(self, tmp_path: Path) -> None:
        """Without close_time in resolution, early detection is disabled but profiling works."""
        trades_csv = tmp_path / "trades.csv"
        res_csv = tmp_path / "resolution.csv"

        trades = _trades("w1", "mkt1xxxxxxxxxxxx", "BUY", 10)
        _write_csv(trades_csv, trades, TRADE_FIELDS)
        # Resolution without close_time column
        _write_csv(res_csv, [{"ticker": "mkt1xxxxxxxxxxxx", "resolution_price": "100.0", "resolves_yes": "True"}],
                   ["ticker", "resolution_price", "resolves_yes"])

        result = WalletProfiler().build_profiles(
            trades_csv, res_csv, tmp_path / "profiles.parquet",
            min_resolved_trades=5,
        )

        assert result.wallets_with_resolved_trades == 1
        df = pd.read_parquet(tmp_path / "profiles.parquet")
        assert df.iloc[0]["early_trades"] == 0  # no close_time → no early detection
        assert df.iloc[0]["smart_money"] == False  # can't be early informed without timing
