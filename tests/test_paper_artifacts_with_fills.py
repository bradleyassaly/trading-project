from __future__ import annotations

from pathlib import Path

import pandas as pd

from trading_platform.broker.base import BrokerFill
from trading_platform.paper.models import (
    PaperOrder,
    PaperPortfolioState,
    PaperPosition,
    PaperTradingRunResult,
)
from trading_platform.paper.service import write_paper_trading_artifacts


def test_write_paper_trading_artifacts_writes_fills_and_equity_curve(tmp_path: Path) -> None:
    result = PaperTradingRunResult(
        as_of="2025-01-04",
        state=PaperPortfolioState(
            cash=9000.0,
            positions={
                "AAPL": PaperPosition(
                    symbol="AAPL",
                    quantity=10,
                    avg_price=100.0,
                    last_price=110.0,
                )
            },
            last_targets={"AAPL": 1.0},
        ),
        latest_prices={"AAPL": 110.0},
        latest_scores={"AAPL": 2.0},
        latest_target_weights={"AAPL": 1.0},
        scheduled_target_weights={"AAPL": 1.0},
        orders=[
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
        ],
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
        skipped_symbols=[],
        diagnostics={"ok": True},
    )

    paths = write_paper_trading_artifacts(result=result, output_dir=tmp_path)

    assert paths["fills_path"].exists()
    assert paths["equity_snapshot_path"].exists()

    fills_df = pd.read_csv(paths["fills_path"])
    equity_df = pd.read_csv(paths["equity_snapshot_path"])

    assert len(fills_df) == 1
    assert fills_df.iloc[0]["symbol"] == "AAPL"
    assert equity_df.iloc[0]["as_of"] == "2025-01-04"
    assert float(equity_df.iloc[0]["equity"]) == 10100.0
