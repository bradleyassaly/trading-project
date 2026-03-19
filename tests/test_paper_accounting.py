from __future__ import annotations

from pathlib import Path

import pandas as pd

from trading_platform.broker.base import BrokerFill
from trading_platform.paper.accounting import (
    append_equity_ledger,
    append_fill_ledger,
    append_orders_history,
    append_positions_history,
)
from trading_platform.paper.models import PaperOrder, PaperPortfolioState, PaperPosition


def test_append_fill_ledger_appends_rows(tmp_path: Path) -> None:
    path = tmp_path / "fills.csv"

    append_fill_ledger(
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
    append_fill_ledger(
        path=path,
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

    df = pd.read_csv(path)
    assert len(df) == 2
    assert list(df["as_of"]) == ["2025-01-04", "2025-01-05"]


def test_append_equity_ledger_writes_expected_row(tmp_path: Path) -> None:
    path = tmp_path / "equity_curve.csv"
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

    append_equity_ledger(path=path, as_of="2025-01-04", state=state)

    df = pd.read_csv(path)
    assert df.iloc[0]["as_of"] == "2025-01-04"
    assert float(df.iloc[0]["equity"]) == 10100.0


def test_append_positions_history_writes_positions(tmp_path: Path) -> None:
    path = tmp_path / "positions_history.csv"
    state = PaperPortfolioState(
        cash=1000.0,
        positions={
            "AAPL": PaperPosition(
                symbol="AAPL",
                quantity=10,
                avg_price=100.0,
                last_price=110.0,
            )
        },
    )

    append_positions_history(path=path, as_of="2025-01-04", state=state)

    df = pd.read_csv(path)
    assert len(df) == 1
    assert df.iloc[0]["symbol"] == "AAPL"
    assert float(df.iloc[0]["market_value"]) == 1100.0


def test_append_orders_history_writes_orders(tmp_path: Path) -> None:
    path = tmp_path / "orders_history.csv"
    orders = [
        PaperOrder(
            symbol="AAPL",
            side="BUY",
            quantity=10,
            reference_price=101.0,
            target_weight=1.0,
            current_quantity=0,
            target_quantity=10,
            notional=1010.0,
            reason="rebalance_to_target",
        )
    ]

    append_orders_history(path=path, as_of="2025-01-04", orders=orders)

    df = pd.read_csv(path)
    assert len(df) == 1
    assert df.iloc[0]["symbol"] == "AAPL"
    assert df.iloc[0]["side"] == "BUY"