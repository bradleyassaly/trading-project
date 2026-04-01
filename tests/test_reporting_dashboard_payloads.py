from __future__ import annotations

from pathlib import Path

import pandas as pd

from trading_platform.broker.base import BrokerFill
from trading_platform.decision_journal.models import TradeDecision
from trading_platform.execution.order_lifecycle import build_paper_order_lifecycle_records
from trading_platform.execution.reconciliation import build_order_lifecycle_reconciliation_skeleton
from trading_platform.paper.models import PaperOrder, PaperPortfolioState, PaperPosition, PaperTradingRunResult
from trading_platform.reporting.dashboard_payloads import (
    KpiPayload,
    StrategyHealthPayload,
    TradeExplorerPayload,
    build_kpi_payload,
    build_strategy_health_payload,
    build_trade_explorer_payload,
    write_kpi_payload_artifacts,
    write_strategy_health_payload_artifacts,
    write_trade_explorer_payload_artifacts,
)
from trading_platform.reporting.system_health import (
    SystemHealthPayload,
    build_system_health_payload,
    write_system_health_artifacts,
)


def _build_result() -> PaperTradingRunResult:
    orders = [
        PaperOrder(
            symbol="AAPL",
            side="BUY",
            quantity=10,
            reference_price=100.0,
            target_weight=0.5,
            current_quantity=0,
            target_quantity=10,
            notional=1_000.0,
            reason="rebalance_to_target",
        )
    ]
    fills = [
        BrokerFill(
            symbol="AAPL",
            side="BUY",
            quantity=10,
            fill_price=101.0,
            notional=1_010.0,
            realized_pnl=25.0,
        )
    ]
    state = PaperPortfolioState(
        cash=9_000.0,
        positions={"AAPL": PaperPosition(symbol="AAPL", quantity=10, avg_price=101.0, last_price=110.0)},
        initial_cash_basis=10_000.0,
        cumulative_execution_cost=5.0,
    )
    lifecycle = build_paper_order_lifecycle_records(as_of="2025-01-04", orders=orders, fills=fills)
    reconciliation = build_order_lifecycle_reconciliation_skeleton(
        as_of="2025-01-04",
        intended_target_weights={"AAPL": 0.5},
        lifecycle_records=lifecycle,
        realized_state=state,
    )
    return PaperTradingRunResult(
        as_of="2025-01-04",
        state=state,
        latest_prices={"AAPL": 110.0},
        latest_scores={"AAPL": 1.2},
        latest_target_weights={"AAPL": 0.5},
        scheduled_target_weights={"AAPL": 0.5},
        orders=orders,
        fills=fills,
        diagnostics={},
        trade_decision_contracts=[
            TradeDecision(
                decision_id="decision-1",
                timestamp="2025-01-04",
                strategy_id="sma_cross",
                strategy_family="trend",
                candidate_id="candidate-1",
                instrument="AAPL",
                side="BUY",
                horizon_days=5,
                predicted_return=0.02,
                expected_value_gross=0.03,
                expected_cost=0.01,
                expected_value_net=0.02,
                confidence_score=0.7,
                reliability_score=0.6,
                vetoed=False,
                rationale_summary="executed | passed_trade_checks",
            )
        ],
        order_lifecycle_records=lifecycle,
        reconciliation_result=reconciliation,
        attribution={
            "strategy_rows": [
                {
                    "strategy_id": "sma_cross",
                    "total_pnl": 95.0,
                    "realized_pnl": 25.0,
                    "unrealized_pnl": 70.0,
                    "total_execution_cost": 5.0,
                    "turnover": 1_010.0,
                    "trade_count": 1,
                    "win_rate": 1.0,
                }
            ],
            "trade_rows": [
                {
                    "trade_id": "trade-1",
                    "symbol": "AAPL",
                    "strategy_id": "sma_cross",
                    "side": "BUY",
                    "quantity": 10,
                    "entry_date": "2025-01-04",
                    "exit_date": None,
                    "realized_pnl": 25.0,
                    "total_execution_cost": 5.0,
                    "status": "open",
                }
            ],
        },
    )


def test_reporting_payload_contracts_round_trip() -> None:
    result = _build_result()

    kpi_payload = build_kpi_payload(result=result)
    trade_explorer_payload = build_trade_explorer_payload(result=result)
    strategy_health_payload = build_strategy_health_payload(result=result)
    system_health_payload = build_system_health_payload(result=result, artifact_paths={})

    assert KpiPayload.from_dict(kpi_payload.to_dict()) == kpi_payload
    assert TradeExplorerPayload.from_dict(trade_explorer_payload.to_dict()) == trade_explorer_payload
    assert StrategyHealthPayload.from_dict(strategy_health_payload.to_dict()) == strategy_health_payload
    assert SystemHealthPayload.from_dict(system_health_payload.to_dict()) == system_health_payload


def test_reporting_payload_writers_emit_dashboard_ready_artifacts(tmp_path: Path) -> None:
    result = _build_result()

    kpi_paths = write_kpi_payload_artifacts(output_dir=tmp_path, payload=build_kpi_payload(result=result))
    trade_paths = write_trade_explorer_payload_artifacts(
        output_dir=tmp_path,
        payload=build_trade_explorer_payload(result=result),
    )
    health_paths = write_strategy_health_payload_artifacts(
        output_dir=tmp_path,
        payload=build_strategy_health_payload(result=result),
    )
    system_health_paths = write_system_health_artifacts(
        output_dir=tmp_path,
        payload=build_system_health_payload(
            result=result,
            artifact_paths={
                "summary_path": tmp_path / "paper_summary.json",
                "portfolio_performance_summary_path": tmp_path / "portfolio_performance_summary.json",
                "execution_summary_json_path": tmp_path / "execution_summary.json",
                "kpi_payload_json_path": kpi_paths["kpi_payload_json_path"],
                "trade_explorer_payload_json_path": trade_paths["trade_explorer_payload_json_path"],
                "strategy_health_payload_json_path": health_paths["strategy_health_payload_json_path"],
                "realtime_kpi_monitoring_json_path": tmp_path / "realtime_kpi_monitoring.json",
            },
        ),
    )

    assert kpi_paths["kpi_payload_json_path"].exists()
    assert trade_paths["trade_explorer_payload_json_path"].exists()
    assert health_paths["strategy_health_payload_json_path"].exists()
    assert system_health_paths["system_health_payload_json_path"].exists()

    kpi_df = pd.read_csv(kpi_paths["kpi_records_csv_path"])
    trade_df = pd.read_csv(trade_paths["trade_explorer_rows_csv_path"])
    health_df = pd.read_csv(health_paths["strategy_health_payload_csv_path"])
    system_health_df = pd.read_csv(system_health_paths["system_health_checks_csv_path"])

    assert "equity" in set(kpi_df["metric_name"])
    assert trade_df.iloc[0]["symbol"] == "AAPL"
    assert trade_df.iloc[0]["reconciliation_status"] == "reconciled"
    assert health_df.iloc[0]["strategy_id"] == "sma_cross"
    assert "artifact_presence" in set(system_health_df["check_name"])


def test_system_health_payload_surfaces_stale_data_and_missing_artifacts() -> None:
    result = _build_result()
    result.price_snapshots = []
    result.latest_scores = {}
    result.latest_target_weights = {"AAPL": 0.5}
    result.diagnostics["paper_execution"] = {
        "latest_data_stale": True,
        "latest_bar_age_seconds": 7200.0,
        "stale_symbol_count": 1,
        "snapshot_symbol_count": 1,
        "latest_data_source": "yfinance",
        "latest_data_fallback_used": True,
    }
    result.decision_bundle = None

    payload = build_system_health_payload(
        result=result,
        artifact_paths={
            "summary_path": Path("missing_summary.json"),
        },
    )

    checks = {row.check_name: row for row in payload.checks}
    assert checks["data_freshness"].status == "warn"
    assert checks["stale_signals"].status == "fail"
    assert checks["artifact_presence"].status == "fail"
    assert checks["pipeline_integrity"].status == "fail"
    assert payload.summary["failure_count"] >= 1
