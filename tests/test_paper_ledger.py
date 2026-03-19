from __future__ import annotations

from pathlib import Path

import pandas as pd

from trading_platform.broker.base import BrokerFill
from trading_platform.paper.ledger import append_equity_snapshot, append_fills
from trading_platform.paper.models import PaperPortfolioState, PaperPosition


def test_append_fills_writes_expected_columns(tmp_path: Path) -> None:
    path = tmp_path / "paper_fills.csv"
    append_fills(
        path=path,
        as_of="2025-01-04",
        fills=[
            BrokerFill(
                symbol="AAPL",
                side="BUY",
                quantity=10,
                fill_price=101.0,
                notional=1010.0,
                commission=1.0,
                slippage_bps=5.0,
            )
        ],
    )

    df = pd.read_csv(path)
    assert list(df.columns) == [
        "as_of",
        "symbol",
        "side",
        "quantity",
        "fill_price",
        "notional",
        "commission",
        "slippage_bps",
    ]
    assert df.iloc[0]["as_of"] == "2025-01-04"
    assert df.iloc[0]["symbol"] == "AAPL"


def test_append_equity_snapshot_writes_expected_row(tmp_path: Path) -> None:
    path = tmp_path / "paper_equity_curve.csv"
    state = PaperPortfolioState(
        cash=9000.0,
        positions={
            "AAPL": PaperPosition(
                symbol="AAPL",
                quantity=10,
                avg_price=100.0,
                last_price=110.0,
            )
        },
    )

    append_equity_snapshot(
        path=path,
        as_of="2025-01-04",
        state=state,
    )

    df = pd.read_csv(path)
    assert df.iloc[0]["as_of"] == "2025-01-04"
    assert float(df.iloc[0]["cash"]) == 9000.0
    assert float(df.iloc[0]["gross_market_value"]) == 1100.0
    assert float(df.iloc[0]["equity"]) == 10100.0
    assert int(df.iloc[0]["position_count"]) == 1


def test_append_helpers_append_multiple_rows(tmp_path: Path) -> None:
    fills_path = tmp_path / "paper_fills.csv"
    equity_path = tmp_path / "paper_equity_curve.csv"

    append_fills(
        path=fills_path,
        as_of="2025-01-04",
        fills=[
            BrokerFill(
                symbol="AAPL",
                side="BUY",
                quantity=10,
                fill_price=101.0,
                notional=1010.0,
                commission=0.0,
                slippage_bps=0.0,
            )
        ],
    )
    append_fills(
        path=fills_path,
        as_of="2025-01-05",
        fills=[
            BrokerFill(
                symbol="MSFT",
                side="SELL",
                quantity=5,
                fill_price=202.0,
                notional=1010.0,
                commission=0.0,
                slippage_bps=0.0,
            )
        ],
    )

    append_equity_snapshot(
        path=equity_path,
        as_of="2025-01-04",
        state=PaperPortfolioState(cash=10000.0),
    )
    append_equity_snapshot(
        path=equity_path,
        as_of="2025-01-05",
        state=PaperPortfolioState(cash=9500.0),
    )

    fills_df = pd.read_csv(fills_path)
    equity_df = pd.read_csv(equity_path)

    assert len(fills_df) == 2
    assert list(fills_df["as_of"]) == ["2025-01-04", "2025-01-05"]
    assert len(equity_df) == 2
    assert list(equity_df["as_of"]) == ["2025-01-04", "2025-01-05"]
