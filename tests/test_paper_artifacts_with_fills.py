from __future__ import annotations

from pathlib import Path

import pandas as pd

from trading_platform.broker.base import BrokerFill
from trading_platform.decision_journal.service import build_candidate_journal_for_snapshot
from trading_platform.paper.models import (
    PaperOrder,
    PaperPortfolioState,
    PaperPosition,
    PaperTradingRunResult,
)
from trading_platform.paper.service import write_paper_trading_artifacts
from trading_platform.universe_provenance.service import build_universe_provenance_bundle


def test_write_paper_trading_artifacts_writes_fills_and_equity_curve(tmp_path: Path) -> None:
    metadata_dir = tmp_path / "metadata"
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
            initial_cash_basis=10_000.0,
            cumulative_realized_pnl=25.0,
            cumulative_fees=1.0,
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
                realized_pnl=25.0,
            )
        ],
        skipped_symbols=[],
        diagnostics={
            "ok": True,
            "accounting": {
                "auto_apply_fills": True,
                "fill_application_status": "fills_applied",
                "starting_cash": 10_000.0,
                "ending_cash": 9_000.0,
                "starting_equity": 10_000.0,
                "ending_equity": 10_100.0,
                "fill_count": 1,
                "buy_fill_count": 1,
                "sell_fill_count": 0,
                "cumulative_realized_pnl": 25.0,
                "unrealized_pnl": 100.0,
                "total_pnl": 100.0,
            },
            "target_construction": {
                "multi_strategy_allocation": {
                    "sleeve_contribution": {"core": 1.0},
                    "normalized_capital_weights": {"core": 1.0},
                }
            },
            "strategy_execution_handoff": {
                "activation_applied": True,
                "active_strategy_count": 1,
            },
        },
        decision_bundle=build_candidate_journal_for_snapshot(
            timestamp="2025-01-04",
            run_id="manual|sma_cross|symbols|2025-01-04",
            cycle_id="2025-01-04",
            strategy_id="sma_cross",
            universe_id=None,
            score_map={"AAPL": 2.0},
            latest_prices={"AAPL": 110.0},
            selected_weights={"AAPL": 1.0},
            scheduled_weights={"AAPL": 1.0},
        ),
    )

    paths = write_paper_trading_artifacts(result=result, output_dir=tmp_path, metadata_dir=metadata_dir)

    assert paths["fills_path"].exists()
    assert paths["equity_snapshot_path"].exists()
    assert paths["candidate_snapshot_csv"].exists()
    assert paths["portfolio_performance_summary_path"].exists()
    assert paths["execution_summary_json_path"].exists()
    assert paths["strategy_contribution_summary_path"].exists()

    fills_df = pd.read_csv(paths["fills_path"])
    equity_df = pd.read_csv(paths["equity_snapshot_path"])
    positions_df = pd.read_csv(paths["positions_path"])
    candidate_df = pd.read_csv(paths["candidate_snapshot_csv"])

    assert len(fills_df) == 1
    assert fills_df.iloc[0]["symbol"] == "AAPL"
    assert float(fills_df.iloc[0]["realized_pnl"]) == 25.0
    assert equity_df.iloc[0]["as_of"] == "2025-01-04"
    assert float(equity_df.iloc[0]["equity"]) == 10100.0
    assert float(equity_df.iloc[0]["unrealized_pnl"]) == 100.0
    assert list(positions_df.columns) == [
        "symbol",
        "quantity",
        "avg_price",
        "last_price",
        "cost_basis",
        "market_value",
        "unrealized_pnl",
        "portfolio_weight",
    ]
    assert candidate_df.iloc[0]["symbol"] == "AAPL"
    assert not any(key.startswith("metadata_") for key in paths)


def test_write_paper_trading_artifacts_refreshes_metadata_sidecars_when_universe_bundle_exists(
    tmp_path: Path,
) -> None:
    metadata_dir = tmp_path / "metadata"
    universe_bundle = build_universe_provenance_bundle(
        symbols=["AAPL"],
        base_universe_id="demo",
        sub_universe_id="demo_screened",
        filter_definitions=[{"filter_name": "include", "filter_type": "symbol_include_list", "symbols": ["AAPL"]}],
        feature_loader=lambda _symbol: pd.DataFrame(
            {"timestamp": pd.date_range("2025-01-01", periods=3), "close": [10.0, 11.0, 12.0]}
        ),
    )
    result = PaperTradingRunResult(
        as_of="2025-01-04",
        state=PaperPortfolioState(cash=10_000.0, positions={}, last_targets={}),
        latest_prices={},
        latest_scores={},
        latest_target_weights={},
        scheduled_target_weights={},
        orders=[],
        universe_bundle=universe_bundle,
    )

    paths = write_paper_trading_artifacts(
        result=result,
        output_dir=tmp_path / "artifacts",
        metadata_dir=metadata_dir,
    )

    assert paths["metadata_sub_universe_snapshot_csv"].exists()
    assert paths["metadata_universe_enrichment_csv"].exists()
    sidecar_df = pd.read_csv(paths["metadata_sub_universe_snapshot_csv"])
    assert sidecar_df.iloc[0]["sub_universe_id"] == "demo_screened"
