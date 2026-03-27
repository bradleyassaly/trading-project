from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from trading_platform.config.models import MultiStrategyPortfolioConfig, MultiStrategySleeveConfig
from trading_platform.paper.multi_strategy_replay import build_requested_replay_dates, run_multi_strategy_paper_replay


def _allocation_result(*, as_of: str, weights: dict[str, float], prices: dict[str, float]):
    return SimpleNamespace(
        as_of=as_of,
        combined_target_weights=weights,
        latest_prices=prices,
        sleeve_rows=[{"symbol": symbol, "sleeve_name": "core"} for symbol in weights],
        execution_symbol_coverage_rows=[],
        sleeve_bundles=[],
        summary={
            "enabled_sleeve_count": 1,
            "gross_exposure_before_constraints": float(sum(abs(value) for value in weights.values())),
            "gross_exposure_after_constraints": float(sum(abs(value) for value in weights.values())),
            "net_exposure_after_constraints": float(sum(weights.values())),
            "turnover_estimate": 0.0,
            "turnover_cap_binding": False,
            "symbols_removed_or_clipped": [],
            "requested_active_strategy_count": 1,
            "requested_symbol_count": len(prices),
            "pre_validation_target_symbol_count": len(weights),
            "usable_symbol_count": len(weights),
            "skipped_symbol_count": 0,
            "zero_target_reason": "",
            "target_drop_stage": "",
            "target_drop_reason": "",
            "latest_price_source_summary": {"historical": len(weights)},
            "generated_preset_path": "artifacts/promoted/run_current/generated_multi_family.json",
            "signal_artifact_path": "artifacts/alpha_research/run_configured",
            "active_strategy_count": 1,
        },
    )


def test_build_requested_replay_dates_supports_range_and_max_steps() -> None:
    dates = build_requested_replay_dates(
        start_date="2025-01-02",
        end_date="2025-01-08",
        max_steps=3,
    )

    assert dates == ["2025-01-02", "2025-01-03", "2025-01-06"]


def test_run_multi_strategy_paper_replay_evolves_state_and_histories(monkeypatch, tmp_path: Path) -> None:
    allocation_by_date = {
        "2025-01-02": _allocation_result(
            as_of="2025-01-02",
            weights={"AAPL": 1.0},
            prices={"AAPL": 100.0},
        ),
        "2025-01-03": _allocation_result(
            as_of="2025-01-03",
            weights={"AAPL": 0.5, "MSFT": 0.5},
            prices={"AAPL": 110.0, "MSFT": 110.0},
        ),
        "2025-01-06": _allocation_result(
            as_of="2025-01-06",
            weights={"AAPL": 0.5, "MSFT": 0.5},
            prices={"AAPL": 110.0, "MSFT": 110.0},
        ),
        "2025-01-07": _allocation_result(
            as_of="2025-01-07",
            weights={"MSFT": 1.0},
            prices={"AAPL": 120.0, "MSFT": 120.0},
        ),
    }

    monkeypatch.setattr(
        "trading_platform.paper.multi_strategy_replay.allocate_multi_strategy_portfolio",
        lambda portfolio_config, as_of_date=None, previous_weights=None: allocation_by_date[str(as_of_date)],
    )
    monkeypatch.setattr(
        "trading_platform.paper.multi_strategy_replay.write_multi_strategy_artifacts",
        lambda result, output_dir: {"allocation_summary_json_path": Path(output_dir) / "allocation_summary.json"},
    )

    portfolio_config = MultiStrategyPortfolioConfig(
        sleeves=[MultiStrategySleeveConfig("core", "generated_multi_family", 1.0)],
        max_position_weight=1.0,
        max_symbol_concentration=1.0,
    )
    replay = run_multi_strategy_paper_replay(
        portfolio_config=portfolio_config,
        handoff_summary={
            "activation_applied": True,
            "source_portfolio_path": "artifacts/strategy_portfolio/run_current/strategy_portfolio.json",
            "active_strategy_count": 1,
            "active_unconditional_count": 1,
            "active_conditional_count": 0,
            "inactive_conditional_count": 0,
        },
        requested_dates=["2025-01-02", "2025-01-03", "2025-01-06", "2025-01-07"],
        state_path=tmp_path / "state.json",
        output_dir=tmp_path / "replay",
        auto_apply_fills=True,
    )

    assert len(replay.steps) == 4
    assert replay.summary["processed_date_count"] == 4
    assert replay.summary["skipped_date_count"] == 0

    state_payload = json.loads((tmp_path / "state.json").read_text(encoding="utf-8"))
    assert state_payload["as_of"] == "2025-01-07"
    assert set(state_payload["positions"]) == {"MSFT"}
    assert state_payload["positions"]["MSFT"]["quantity"] == 1000
    assert state_payload["cumulative_realized_pnl"] == 15000.0

    performance = json.loads((tmp_path / "replay" / "rolling_performance_summary.json").read_text(encoding="utf-8"))
    assert performance["final_as_of"] == "2025-01-07"
    assert performance["cumulative_realized_pnl"] == 15000.0

    equity_curve = pd.read_csv(tmp_path / "replay" / "portfolio_equity_curve.csv")
    daily_returns = pd.read_csv(tmp_path / "replay" / "portfolio_daily_returns.csv")
    execution_log = pd.read_csv(tmp_path / "replay" / "rolling_execution_log.csv")
    target_history = pd.read_csv(tmp_path / "replay" / "rolling_target_history.csv")
    fill_history = pd.read_csv(tmp_path / "replay" / "rolling_fill_history.csv")

    assert list(equity_curve["timestamp"]) == ["2025-01-02", "2025-01-03", "2025-01-06", "2025-01-07"]
    assert len(daily_returns) == 4
    assert list(execution_log["requested_order_count"]) == [1, 2, 0, 2]
    assert list(execution_log["fill_count"]) == [1, 2, 0, 2]
    assert len(target_history) == 6
    assert len(fill_history) == 5
    assert (tmp_path / "replay" / "execution_summary.json").exists()
    assert (tmp_path / "replay" / "portfolio_performance_summary.json").exists()
    assert (tmp_path / "replay" / "strategy_contribution_summary.json").exists()


def test_run_multi_strategy_paper_replay_skips_non_exact_dates(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "trading_platform.paper.multi_strategy_replay.allocate_multi_strategy_portfolio",
        lambda portfolio_config, as_of_date=None, previous_weights=None: _allocation_result(
            as_of="2025-01-03",
            weights={"AAPL": 1.0},
            prices={"AAPL": 100.0},
        ),
    )
    monkeypatch.setattr(
        "trading_platform.paper.multi_strategy_replay.write_multi_strategy_artifacts",
        lambda result, output_dir: {"allocation_summary_json_path": Path(output_dir) / "allocation_summary.json"},
    )

    replay = run_multi_strategy_paper_replay(
        portfolio_config=MultiStrategyPortfolioConfig(
            sleeves=[MultiStrategySleeveConfig("core", "generated_multi_family", 1.0)],
            max_position_weight=1.0,
            max_symbol_concentration=1.0,
        ),
        handoff_summary={"active_strategy_count": 1},
        requested_dates=["2025-01-04"],
        state_path=tmp_path / "state.json",
        output_dir=tmp_path / "replay",
        auto_apply_fills=True,
    )

    assert replay.steps == []
    assert replay.skipped_dates[0]["requested_date"] == "2025-01-04"
    assert "no_exact_data_for_requested_date" in replay.skipped_dates[0]["reason"]
