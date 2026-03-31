from __future__ import annotations

from pathlib import Path

import pandas as pd

from trading_platform.broker.base import BrokerFill
from trading_platform.execution.costs import (
    TransactionCostReport,
    build_transaction_cost_report,
    write_transaction_cost_artifacts,
)
from trading_platform.paper.models import PaperOrder, PaperTradingConfig


def test_transaction_cost_report_round_trip_and_artifacts(tmp_path: Path) -> None:
    report = build_transaction_cost_report(
        as_of="2025-01-04",
        config=PaperTradingConfig(
            symbols=["AAPL"],
            slippage_model="fixed_bps",
            slippage_buy_bps=5.0,
            slippage_sell_bps=7.0,
            enable_cost_model=True,
            commission_bps=10.0,
            minimum_commission=1.0,
            spread_bps=20.0,
        ),
        orders=[
            PaperOrder(
                symbol="AAPL",
                side="BUY",
                quantity=10,
                reference_price=100.0,
                target_weight=0.25,
                current_quantity=0,
                target_quantity=10,
                notional=1002.0,
                reason="rebalance_to_target",
                expected_gross_notional=1_000.0,
                expected_slippage_bps=5.0,
                expected_spread_bps=10.0,
                expected_slippage_cost=0.5,
                expected_spread_cost=1.0,
                expected_commission_cost=1.0,
                expected_total_execution_cost=2.5,
                cost_model="paper_v2_cost_model",
            )
        ],
        fills=[
            BrokerFill(
                symbol="AAPL",
                side="BUY",
                quantity=10,
                fill_price=100.2,
                notional=1002.0,
                reference_price=100.0,
                gross_notional=1_000.0,
                commission=1.0,
                slippage_bps=5.0,
                spread_bps=10.0,
                slippage_cost=0.5,
                spread_cost=1.0,
                total_execution_cost=2.5,
                cost_model="paper_v2_cost_model",
            )
        ],
    )

    assert TransactionCostReport.from_dict(report.to_dict()) == report
    assert report.summary["estimated_total_cost"] == 2.5
    assert report.summary["realized_total_cost"] == 2.5

    paths = write_transaction_cost_artifacts(output_dir=tmp_path, report=report)
    frame = pd.read_csv(paths["transaction_cost_records_csv_path"])
    assert set(frame["stage"]) == {"estimate", "realized"}
    assert paths["transaction_cost_report_json_path"].exists()
