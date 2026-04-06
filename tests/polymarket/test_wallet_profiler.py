"""Tests for rebuilt WalletProfiler with ResolutionResolver + MarketCloseTimeMap."""
from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from trading_platform.polymarket.wallet_profiler import WalletProfiler


TRADE_FIELDS = ["timestamp", "wallet", "token_id", "side", "total_usdc", "outcome"]
RES_FIELDS = ["ticker", "resolution_price", "resolves_yes", "question"]


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _make_close_db(path: Path, entries: list[tuple[str, str, str]]) -> Path:
    conn = sqlite3.connect(str(path))
    conn.execute("""CREATE TABLE markets (
        market_id TEXT PRIMARY KEY, question TEXT, volume REAL,
        yes_token_id TEXT, end_date_iso TEXT)""")
    for mid, tid, end in entries:
        conn.execute("INSERT INTO markets VALUES (?, 'Q?', 1000, ?, ?)", (mid, tid, end))
    conn.commit()
    conn.close()
    return path


def _trades(wallet: str, token_id: str, side: str = "BUY", n: int = 5,
            base_ts: str = "2026-03-20T", outcome: str = "Yes") -> list[dict]:
    return [
        {"timestamp": f"{base_ts}{i:02d}:00:00+00:00", "wallet": wallet,
         "token_id": token_id, "side": side, "total_usdc": "100",
         "outcome": outcome}
        for i in range(n)
    ]


class TestWalletProfilerRebuilt:
    def test_late_trades_get_zero_early_trades(self, tmp_path: Path) -> None:
        """Trades < 24h before close → early_trades == 0."""
        trades_csv = tmp_path / "trades.csv"
        # Trades at 23:00 on April 2, close April 3 00:00 → <24h
        _write_csv(trades_csv,
                   _trades("w1", "12345", n=10, base_ts="2026-04-02T23:"),
                   TRADE_FIELDS)
        # Fix timestamps to be within 1 hour
        rows = []
        with trades_csv.open() as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                row["timestamp"] = f"2026-04-02T23:{i:02d}:00+00:00"
                rows.append(row)
        _write_csv(trades_csv, rows, TRADE_FIELDS)

        _write_csv(tmp_path / "res.csv",
                   [{"ticker": "12345", "resolution_price": "100.0", "resolves_yes": "True", "question": "Q"}],
                   RES_FIELDS)
        db = _make_close_db(tmp_path / "close.db", [("m1", "12345", "2026-04-03T00:00:00+00:00")])

        result = WalletProfiler().build_profiles(
            trades_csv, tmp_path / "res.csv", tmp_path / "out.parquet",
            min_resolved_trades=3, close_time_db_path=db,
        )
        df = pd.read_parquet(tmp_path / "out.parquet")
        assert df.iloc[0]["early_trades"] == 0

    def test_early_wins_flagged_informed(self, tmp_path: Path) -> None:
        """5 early wins out of 5 → is_early_informed == True."""
        trades_csv = tmp_path / "trades.csv"
        # Trades 3 days before close (>24h)
        _write_csv(trades_csv,
                   _trades("w1", "100", n=5, base_ts="2026-03-28T"),
                   TRADE_FIELDS)
        _write_csv(tmp_path / "res.csv",
                   [{"ticker": "100", "resolution_price": "100.0", "resolves_yes": "True", "question": "Q"}],
                   RES_FIELDS)
        db = _make_close_db(tmp_path / "close.db", [("m1", "100", "2026-04-03T00:00:00+00:00")])

        WalletProfiler().build_profiles(
            trades_csv, tmp_path / "res.csv", tmp_path / "out.parquet",
            min_resolved_trades=3, min_early_trades=5, close_time_db_path=db,
        )
        df = pd.read_parquet(tmp_path / "out.parquet")
        assert df.iloc[0]["early_trades"] == 5
        assert df.iloc[0]["early_win_rate"] == 1.0
        assert df.iloc[0]["is_early_informed"] == True

    def test_625_pct_not_flagged(self, tmp_path: Path) -> None:
        """5 early wins out of 8 (62.5%) → NOT flagged (threshold is 65%)."""
        trades_csv = tmp_path / "trades.csv"
        wins = _trades("w1", "100", n=5, base_ts="2026-03-28T", outcome="Yes")
        losses = _trades("w1", "200", n=3, base_ts="2026-03-28T", outcome="Yes")
        _write_csv(trades_csv, wins + losses, TRADE_FIELDS)
        _write_csv(tmp_path / "res.csv", [
            {"ticker": "100", "resolution_price": "100.0", "resolves_yes": "True", "question": "Q1"},
            {"ticker": "200", "resolution_price": "0.0", "resolves_yes": "False", "question": "Q2"},
        ], RES_FIELDS)
        db = _make_close_db(tmp_path / "close.db", [
            ("m1", "100", "2026-04-03"), ("m2", "200", "2026-04-03"),
        ])

        WalletProfiler().build_profiles(
            trades_csv, tmp_path / "res.csv", tmp_path / "out.parquet",
            min_resolved_trades=3, min_early_trades=5, close_time_db_path=db,
        )
        df = pd.read_parquet(tmp_path / "out.parquet")
        w1 = df[df["wallet"] == "w1"].iloc[0]
        assert w1["early_win_rate"] == pytest.approx(5 / 8, abs=0.01)
        assert w1["is_early_informed"] == False

    def test_714_pct_flagged(self, tmp_path: Path) -> None:
        """5 early wins out of 7 (71.4%) → IS flagged."""
        trades_csv = tmp_path / "trades.csv"
        wins = _trades("w1", "100", n=5, base_ts="2026-03-28T", outcome="Yes")
        losses = _trades("w1", "200", n=2, base_ts="2026-03-28T", outcome="Yes")
        _write_csv(trades_csv, wins + losses, TRADE_FIELDS)
        _write_csv(tmp_path / "res.csv", [
            {"ticker": "100", "resolution_price": "100.0", "resolves_yes": "True", "question": "Q1"},
            {"ticker": "200", "resolution_price": "0.0", "resolves_yes": "False", "question": "Q2"},
        ], RES_FIELDS)
        db = _make_close_db(tmp_path / "close.db", [
            ("m1", "100", "2026-04-03"), ("m2", "200", "2026-04-03"),
        ])

        WalletProfiler().build_profiles(
            trades_csv, tmp_path / "res.csv", tmp_path / "out.parquet",
            min_resolved_trades=3, min_early_trades=5, close_time_db_path=db,
        )
        df = pd.read_parquet(tmp_path / "out.parquet")
        w1 = df[df["wallet"] == "w1"].iloc[0]
        assert w1["early_win_rate"] == pytest.approx(5 / 7, abs=0.01)
        assert w1["is_early_informed"] == True

    def test_fewer_than_3_resolved_excluded(self, tmp_path: Path) -> None:
        """Wallets with < 3 resolved trades are excluded."""
        trades_csv = tmp_path / "trades.csv"
        _write_csv(trades_csv, _trades("w1", "100", n=2), TRADE_FIELDS)
        _write_csv(tmp_path / "res.csv",
                   [{"ticker": "100", "resolution_price": "100.0", "resolves_yes": "True", "question": "Q"}],
                   RES_FIELDS)

        result = WalletProfiler().build_profiles(
            trades_csv, tmp_path / "res.csv", tmp_path / "out.parquet",
            min_resolved_trades=3,
        )
        assert result.wallets_with_resolved_trades == 0

    def test_normalize_token_id_called(self, tmp_path: Path) -> None:
        """Resolution lookup normalizes token IDs (hex CSV matches decimal query)."""
        trades_csv = tmp_path / "trades.csv"
        # Trade uses decimal token_id "255"
        _write_csv(trades_csv, _trades("w1", "255", n=5), TRADE_FIELDS)
        # Resolution uses hex "0xff" (= 255)
        _write_csv(tmp_path / "res.csv",
                   [{"ticker": "0xff", "resolution_price": "100.0", "resolves_yes": "True", "question": "Q"}],
                   RES_FIELDS)

        result = WalletProfiler().build_profiles(
            trades_csv, tmp_path / "res.csv", tmp_path / "out.parquet",
            min_resolved_trades=3,
        )
        assert result.wallets_with_resolved_trades == 1

    def test_smart_money_signal(self, tmp_path: Path) -> None:
        trades_csv = tmp_path / "trades.csv"
        _write_csv(trades_csv, _trades("smart", "100", n=5), TRADE_FIELDS)
        profiles = pd.DataFrame([{
            "wallet": "smart", "total_trades": 100, "resolved_trades": 50,
            "wins": 40, "win_rate": 0.80, "early_trades": 30, "early_wins": 22,
            "early_win_rate": 0.73, "avg_hours_before_close": 48.0,
            "total_volume_usdc": 50000, "avg_trade_size": 500,
            "markets_traded": 20, "edge": 0.30, "is_early_informed": True, "smart_money": True,
        }])
        profiles.to_parquet(tmp_path / "profiles.parquet", index=False)
        signal = WalletProfiler.get_smart_money_signal(
            trades_csv, tmp_path / "profiles.parquet", "100",
        )
        assert signal == pytest.approx(1.0)

    def test_outcome_yes_with_resolver_wins(self, tmp_path: Path) -> None:
        """outcome=Yes + market resolved YES → win."""
        trades_csv = tmp_path / "trades.csv"
        _write_csv(trades_csv, [
            {"timestamp": f"2026-03-28T0{i}:00:00+00:00", "wallet": "w1",
             "token_id": "100", "side": "BUY", "total_usdc": "50", "outcome": "Yes"}
            for i in range(5)
        ], TRADE_FIELDS)
        _write_csv(tmp_path / "res.csv", [
            {"ticker": "100", "resolution_price": "100.0", "resolves_yes": "True", "question": "Q"},
        ], RES_FIELDS)

        result = WalletProfiler().build_profiles(
            trades_csv, tmp_path / "res.csv", tmp_path / "out.parquet",
            min_resolved_trades=3,
        )
        assert result.wallets_with_resolved_trades == 1
        df = pd.read_parquet(tmp_path / "out.parquet")
        assert df.iloc[0]["win_rate"] == 1.0

    def test_outcome_yes_market_no_loses(self, tmp_path: Path) -> None:
        """outcome=Yes but market resolved NO → loss."""
        trades_csv = tmp_path / "trades.csv"
        _write_csv(trades_csv, [
            {"timestamp": f"2026-03-28T0{i}:00:00+00:00", "wallet": "w1",
             "token_id": "100", "side": "BUY", "total_usdc": "50", "outcome": "Yes"}
            for i in range(5)
        ], TRADE_FIELDS)
        _write_csv(tmp_path / "res.csv", [
            {"ticker": "100", "resolution_price": "0.0", "resolves_yes": "False", "question": "Q"},
        ], RES_FIELDS)

        WalletProfiler().build_profiles(
            trades_csv, tmp_path / "res.csv", tmp_path / "out.parquet",
            min_resolved_trades=3,
        )
        df = pd.read_parquet(tmp_path / "out.parquet")
        assert df.iloc[0]["win_rate"] == 0.0

    def test_outcome_no_market_no_wins(self, tmp_path: Path) -> None:
        """outcome=No + market resolved NO → win (bought NO side, NO won)."""
        trades_csv = tmp_path / "trades.csv"
        _write_csv(trades_csv, [
            {"timestamp": f"2026-03-28T0{i}:00:00+00:00", "wallet": "w1",
             "token_id": "100", "side": "BUY", "total_usdc": "50", "outcome": "No"}
            for i in range(5)
        ], TRADE_FIELDS)
        _write_csv(tmp_path / "res.csv", [
            {"ticker": "100", "resolution_price": "0.0", "resolves_yes": "False", "question": "Q"},
        ], RES_FIELDS)

        WalletProfiler().build_profiles(
            trades_csv, tmp_path / "res.csv", tmp_path / "out.parquet",
            min_resolved_trades=3,
        )
        df = pd.read_parquet(tmp_path / "out.parquet")
        assert df.iloc[0]["win_rate"] == 1.0

    def test_falls_back_to_side_when_no_outcome(self, tmp_path: Path) -> None:
        """When outcome column is empty, uses side for win calc."""
        trades_csv = tmp_path / "trades.csv"
        _write_csv(trades_csv, [
            {"timestamp": f"2026-03-28T0{i}:00:00+00:00", "wallet": "w1",
             "token_id": "100", "side": "BUY", "total_usdc": "50", "outcome": ""}
            for i in range(5)
        ], TRADE_FIELDS)
        _write_csv(tmp_path / "res.csv", [
            {"ticker": "100", "resolution_price": "100.0", "resolves_yes": "True", "question": "Q"},
        ], RES_FIELDS)

        WalletProfiler().build_profiles(
            trades_csv, tmp_path / "res.csv", tmp_path / "out.parquet",
            min_resolved_trades=3,
        )
        df = pd.read_parquet(tmp_path / "out.parquet")
        assert df.iloc[0]["win_rate"] == 1.0  # BUY + YES resolved → win

    def test_uses_total_usdc_column(self, tmp_path: Path) -> None:
        """total_usdc column used when amount column missing."""
        trades_csv = tmp_path / "trades.csv"
        _write_csv(trades_csv, [
            {"timestamp": f"2026-03-28T0{i}:00:00+00:00", "wallet": "w1",
             "token_id": "100", "side": "BUY", "total_usdc": "75.50", "outcome": "Yes"}
            for i in range(5)
        ], TRADE_FIELDS)
        _write_csv(tmp_path / "res.csv", [
            {"ticker": "100", "resolution_price": "100.0", "resolves_yes": "True", "question": "Q"},
        ], RES_FIELDS)

        WalletProfiler().build_profiles(
            trades_csv, tmp_path / "res.csv", tmp_path / "out.parquet",
            min_resolved_trades=3,
        )
        df = pd.read_parquet(tmp_path / "out.parquet")
        assert df.iloc[0]["total_volume_usdc"] == pytest.approx(75.50 * 5)
