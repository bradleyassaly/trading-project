from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from trading_platform.paper.models import (
    PaperOrder,
    PaperPortfolioState,
    PaperPosition,
    PaperTradingConfig,
    PaperTradingRunResult,
)
from trading_platform.paper.persistence import _upsert_csv, persist_paper_run_outputs


def _build_result(*, equity: float = 10_100.0) -> PaperTradingRunResult:
    state = PaperPortfolioState(
        as_of="2025-01-21",
        cash=9_000.0,
        positions={
            "AAPL": PaperPosition(symbol="AAPL", quantity=10, avg_price=100.0, last_price=110.0),
        },
    )
    state.cash = equity - state.gross_market_value
    return PaperTradingRunResult(
        as_of="2025-01-21",
        state=state,
        latest_prices={"AAPL": 110.0, "MSFT": 200.0},
        latest_scores={"AAPL": 0.1, "MSFT": 0.05},
        latest_target_weights={"AAPL": 0.5, "MSFT": 0.5},
        scheduled_target_weights={"AAPL": 0.5, "MSFT": 0.5},
        orders=[
            PaperOrder(
                symbol="MSFT",
                side="BUY",
                quantity=5,
                reference_price=200.0,
                target_weight=0.5,
                current_quantity=0,
                target_quantity=5,
                notional=1_000.0,
                reason="rebalance_to_target",
            )
        ],
        fills=[],
        skipped_symbols=[],
        diagnostics={
            "target_construction": {
                "rebalance_timestamp": "2025-01-21",
                "selected_symbols": "AAPL,MSFT",
                "target_selected_symbols": "AAPL,MSFT",
                "realized_holdings_count": 2,
                "target_selected_count": 2,
                "realized_holdings_minus_top_n": 0,
                "average_gross_exposure": 1.0,
                "liquidity_excluded_count": 0,
                "sector_cap_excluded_count": 0,
                "turnover_cap_binding_count": 3,
                "turnover_buffer_blocked_replacements": 0,
                "semantic_warning": "",
                "summary": {"mean_turnover": 0.12},
                "skip_reasons": {},
            },
            "order_generation": {
                "equity": 10_000.0,
                "skipped_trades_count": 2,
                "skipped_turnover": 0.03,
                "effective_turnover_reduction": 0.2,
            },
            "paper_execution": {
                "min_weight_change_to_trade": 0.02,
                "score_band_enabled": True,
                "entry_threshold_used": 0.85,
                "exit_threshold_used": 0.60,
                "blocked_entries_count": 3,
                "held_in_hold_zone_count": 4,
                "forced_exit_count": 1,
                "skipped_due_to_entry_band_count": 3,
                "skipped_due_to_hold_zone_count": 4,
                "ev_gate_enabled": True,
                "ev_gate_model_type": "bucketed_mean",
                "ev_gate_mode": "soft",
                "ev_gate_blocked_count": 2,
                "avg_expected_net_return_traded": 0.012,
                "avg_expected_net_return_blocked": -0.004,
                "avg_ev_executed_trades": 0.012,
                "ev_weighted_exposure": 0.8,
                "avg_ev_weight_multiplier": 1.1,
                "ev_distribution": {"count": 3, "mean": 0.01},
                "ev_model_training_window": {"start": "2025-01-01", "end": "2025-01-20"},
                "ev_model_sample_count": 24,
                "skipped_trades_count": 2,
                "skipped_turnover": 0.03,
                "effective_turnover_reduction": 0.2,
            },
            "accounting": {
                "starting_cash": 10_000.0,
                "ending_cash": 9_150.0,
                "starting_gross_market_value": 0.0,
                "ending_gross_market_value": 1_100.0,
                "starting_equity": 10_000.0,
                "ending_equity": equity,
                "realized_pnl_delta": 5.0,
                "cumulative_realized_pnl": 5.0,
                "unrealized_pnl": 100.0,
                "total_pnl": equity - 10_000.0,
                "total_pnl_delta": equity - 10_000.0,
                "fees_paid_delta": 1.0,
                "cumulative_fees": 1.0,
                "fill_count": 1,
                "buy_fill_count": 1,
                "sell_fill_count": 0,
                "fill_notional": 1_000.0,
                "auto_apply_fills": True,
                "fill_application_status": "fills_applied",
            },
        },
    )


def test_persist_paper_run_outputs_writes_ledgers_and_latest_summaries(tmp_path: Path) -> None:
    config = PaperTradingConfig(
        symbols=["AAPL", "MSFT"],
        preset_name="xsec_nasdaq100_momentum_v1_deploy",
        universe_name="nasdaq100",
        strategy="xsec_momentum_topn",
        top_n=2,
        portfolio_construction_mode="transition",
        benchmark="equal_weight",
    )

    paths, health_checks, summary = persist_paper_run_outputs(
        result=_build_result(),
        config=config,
        output_dir=tmp_path,
        state_file_preexisting=False,
    )

    assert (tmp_path / "paper_equity_curve.csv").exists()
    assert (tmp_path / "portfolio_equity_curve.csv").exists()
    assert (tmp_path / "paper_positions_history.csv").exists()
    assert (tmp_path / "paper_orders_history.csv").exists()
    assert (tmp_path / "paper_run_summary.csv").exists()
    assert (tmp_path / "paper_run_summary_latest.json").exists()
    assert (tmp_path / "paper_run_summary_latest.md").exists()
    assert (tmp_path / "paper_health_checks.csv").exists()
    assert summary["preset_name"] == "xsec_nasdaq100_momentum_v1_deploy"
    assert any(item["check_name"] == "state_file" for item in health_checks)

    latest_payload = json.loads((tmp_path / "paper_run_summary_latest.json").read_text(encoding="utf-8"))
    assert latest_payload["summary"]["portfolio_construction_mode"] == "transition"
    assert latest_payload["summary"]["fill_count"] == 1
    assert latest_payload["summary"]["total_pnl"] == 100.0
    assert latest_payload["summary"]["skipped_trades_count"] == 2
    assert latest_payload["summary"]["skipped_turnover"] == 0.03
    assert latest_payload["summary"]["effective_turnover_reduction"] == 0.2
    assert latest_payload["summary"]["min_weight_change_to_trade"] == 0.02
    assert latest_payload["summary"]["score_band_enabled"] is True
    assert latest_payload["summary"]["blocked_entries_count"] == 3
    assert latest_payload["summary"]["held_in_hold_zone_count"] == 4
    assert latest_payload["summary"]["ev_gate_enabled"] is True
    assert latest_payload["summary"]["ev_gate_mode"] == "soft"
    assert latest_payload["summary"]["ev_gate_blocked_count"] == 2
    assert latest_payload["summary"]["ev_weighted_exposure"] == 0.8
    assert "health_checks" in latest_payload


def test_persist_paper_run_outputs_is_idempotent_for_same_run_key(tmp_path: Path) -> None:
    config = PaperTradingConfig(
        symbols=["AAPL", "MSFT"],
        preset_name="xsec_nasdaq100_momentum_v1_deploy",
        universe_name="nasdaq100",
        strategy="xsec_momentum_topn",
        top_n=2,
        portfolio_construction_mode="transition",
        benchmark="equal_weight",
    )

    persist_paper_run_outputs(
        result=_build_result(equity=10_100.0),
        config=config,
        output_dir=tmp_path,
        state_file_preexisting=True,
    )
    persist_paper_run_outputs(
        result=_build_result(equity=10_250.0),
        config=config,
        output_dir=tmp_path,
        state_file_preexisting=True,
    )

    summary_df = pd.read_csv(tmp_path / "paper_run_summary.csv")
    equity_df = pd.read_csv(tmp_path / "paper_equity_curve.csv")
    positions_df = pd.read_csv(tmp_path / "paper_positions_history.csv")
    orders_df = pd.read_csv(tmp_path / "paper_orders_history.csv")
    health_df = pd.read_csv(tmp_path / "paper_health_checks.csv")

    assert len(summary_df) == 1
    assert len(equity_df) == 1
    assert len(positions_df) == 1
    assert len(orders_df) == 1
    assert len(health_df["check_name"].unique()) == len(health_df)
    assert float(summary_df.iloc[0]["current_equity"]) == 10_250.0


def test_upsert_csv_handles_existing_empty_history_file(tmp_path: Path) -> None:
    history_path = tmp_path / "paper_positions_history.csv"
    pd.DataFrame(columns=["timestamp", "symbol", "qty"]).to_csv(history_path, index=False)

    _upsert_csv(
        path=history_path,
        rows=[{"timestamp": "2025-01-21", "symbol": "AAPL", "qty": 10}],
        key_columns=["timestamp", "symbol"],
        columns=["timestamp", "symbol", "qty"],
    )

    frame = pd.read_csv(history_path)
    assert len(frame) == 1
    assert frame.iloc[0]["symbol"] == "AAPL"
