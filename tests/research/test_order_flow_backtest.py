"""Tests for OrderFlowBacktester."""
from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from trading_platform.research.order_flow_backtest import OrderFlowBacktester


def _make_fills(tmp_path: Path, token_id: str, n: int = 30, side: str = "BUY") -> None:
    """Write a synthetic fills parquet."""
    fills_dir = tmp_path / "fills"
    fills_dir.mkdir(exist_ok=True)
    now = datetime.now(tz=timezone.utc)
    rows = [{
        "id": f"f{i}",
        "timestamp": now,
        "maker_wallet": "w1",
        "taker_wallet": "w2",
        "maker_amount": 100.0,
        "taker_amount": 65.0,
        "maker_asset_id": "a" if side == "BUY" else "b",
        "taker_asset_id": "b" if side == "BUY" else "a",
        "side": side,
        "fee": 0.5,
    } for i in range(n)]
    pd.DataFrame(rows).to_parquet(fills_dir / f"{token_id}.parquet", index=False)


def _make_resolution(tmp_path: Path, entries: list[tuple[str, float]]) -> Path:
    res_path = tmp_path / "resolution.csv"
    with res_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["ticker", "resolution_price", "resolves_yes", "question"])
        w.writeheader()
        for tid, rp in entries:
            w.writerow({"ticker": tid, "resolution_price": rp, "resolves_yes": rp >= 99, "question": "Q"})
    return res_path


class TestOrderFlowBacktester:
    def test_markets_no_resolution_skipped(self, tmp_path: Path) -> None:
        _make_fills(tmp_path, "tok1")
        res = _make_resolution(tmp_path, [])  # no resolutions
        bt = OrderFlowBacktester(str(tmp_path / "fills"), str(res))
        result = bt.run("taker_imbalance", 12)
        assert result.n_trades == 0

    def test_few_fills_skipped(self, tmp_path: Path) -> None:
        _make_fills(tmp_path, "tok1", n=5)
        res = _make_resolution(tmp_path, [("tok1", 100.0)])
        bt = OrderFlowBacktester(str(tmp_path / "fills"), str(res), min_fills_per_market=20)
        result = bt.run("taker_imbalance", 12)
        assert result.n_trades == 0

    def test_perfect_prediction(self, tmp_path: Path) -> None:
        # All BUY flow → predicts YES. Market resolved YES → 100% win rate.
        _make_fills(tmp_path, "tok1", n=30, side="BUY")
        res = _make_resolution(tmp_path, [("tok1", 100.0)])
        bt = OrderFlowBacktester(str(tmp_path / "fills"), str(res))
        result = bt.run("taker_imbalance", 12)
        assert result.n_trades == 1
        assert result.win_rate == 1.0

    def test_random_prediction(self, tmp_path: Path) -> None:
        # Half YES flow, half NO flow across markets
        _make_fills(tmp_path, "tok_yes", n=30, side="BUY")
        _make_fills(tmp_path, "tok_no", n=30, side="SELL")
        res = _make_resolution(tmp_path, [("tok_yes", 100.0), ("tok_no", 100.0)])
        bt = OrderFlowBacktester(str(tmp_path / "fills"), str(res))
        result = bt.run("taker_imbalance", 12)
        assert result.n_trades == 2
        assert result.win_rate == 0.5  # one right, one wrong

    def test_sweep_row_count(self, tmp_path: Path) -> None:
        _make_fills(tmp_path, "tok1", n=30)
        res = _make_resolution(tmp_path, [("tok1", 100.0)])
        bt = OrderFlowBacktester(str(tmp_path / "fills"), str(res))
        df = bt.run_sweep()
        # 2 signals * 5 horizons * 3 windows = 30 rows
        assert len(df) == 30
