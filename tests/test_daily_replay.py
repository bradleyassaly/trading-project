from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from trading_platform.config.workflow_models import (
    DailyReplayWorkflowConfig,
    DailyReplayTuningConfig,
    DailyTradingWorkflowConfig,
)
from trading_platform.orchestration.daily_replay import _build_day_config, build_daily_replay_dates, run_daily_replay
from trading_platform.orchestration.daily_trading import DailyTradingResult


def _base_daily_config(tmp_path: Path) -> DailyTradingWorkflowConfig:
    return DailyTradingWorkflowConfig(
        run_name="base_daily",
        output_root=str(tmp_path / "unused"),
        promotion_policy_config="configs/promotion.yaml",
        strategy_portfolio_policy_config="configs/strategy_portfolio.yaml",
    )


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _install_fake_daily_runner(monkeypatch: pytest.MonkeyPatch, *, fail_dates: set[str] | None = None) -> None:
    fail_dates = fail_dates or set()

    def fake_run(config, *, replay_as_of_date=None, replay_settings=None, refresh_dashboard_static_data=None):
        assert replay_as_of_date is not None
        assert refresh_dashboard_static_data is False
        if replay_as_of_date in fail_dates:
            raise RuntimeError(f"boom:{replay_as_of_date}")
        run_dir = Path(config.output_root) / config.run_name
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "paper").mkdir(parents=True, exist_ok=True)
        (run_dir / "report").mkdir(parents=True, exist_ok=True)
        state_path = Path(config.paper_state_path)
        current_day_index = 0
        positions = {}
        cash = 100_000.0
        if state_path.exists():
            payload = json.loads(state_path.read_text(encoding="utf-8"))
            current_day_index = int(payload.get("day_index", 0))
            positions = dict(payload.get("positions", {}))
            cash = float(payload.get("cash", 100_000.0))
        next_day_index = current_day_index + 1
        if next_day_index % 2 == 1:
            positions = {"AAPL": next_day_index}
            fill_count = 1
            order_count = 1
            zero_target_reason = ""
        else:
            fill_count = 0
            order_count = 0
            zero_target_reason = "hold_within_tolerance"
        equity = 100_000.0 + (next_day_index * 100.0)
        state_payload = {
            "day_index": next_day_index,
            "cash": cash - 100.0,
            "positions": positions,
        }
        _write_json(state_path, state_payload)
        _write_json(
            run_dir / "daily_trading_summary.json",
            {
                "status": "warning" if fill_count == 0 else "succeeded",
                "active_strategy_count": 2,
                "effective_as_of_date": replay_as_of_date,
            },
        )
        _write_json(
            run_dir / "paper" / "paper_run_summary_latest.json",
            {
                "summary": {
                    "requested_symbol_count": 3,
                    "usable_symbol_count": 2,
                    "executable_order_count": order_count,
                    "fill_count": fill_count,
                    "turnover_estimate": 0.2 if fill_count else 0.0,
                    "realized_holdings_count": len(positions),
                    "current_equity": equity,
                    "cumulative_realized_pnl": float(next_day_index * 10),
                    "unrealized_pnl": float(next_day_index * 5),
                    "zero_target_reason": zero_target_reason,
                }
            },
        )
        pd.DataFrame(
            [{"symbol": "AAPL", "side": "BUY", "quantity": 1, "fill_price": 100.0}] if fill_count else [],
            columns=["symbol", "side", "quantity", "fill_price"],
        ).to_csv(run_dir / "paper" / "paper_fills.csv", index=False)
        pd.DataFrame(
            [
                {
                    "strategy_id": "alpha",
                    "is_active": True,
                    "normalized_capital_weight": 0.6,
                },
                {
                    "strategy_id": "beta",
                    "is_active": True,
                    "normalized_capital_weight": 0.4,
                },
            ]
        ).to_csv(run_dir / "report" / "strategy_comparison_summary.csv", index=False)
        pd.DataFrame(
            [
                {
                    "date": replay_as_of_date,
                    "symbol": "AAPL",
                    "strategy_id": "alpha",
                    "signal_source": "multi_strategy",
                    "signal_score": 0.9,
                    "rank": 1,
                    "current_weight": 0.0,
                    "target_weight": 0.5,
                    "weight_delta": 0.5,
                    "current_position": 0,
                    "target_position": 1,
                    "action": "buy",
                    "action_reason": "enter_new_position",
                }
            ]
        ).to_csv(run_dir / "trade_decision_log.csv", index=False)
        return DailyTradingResult(
            run_name=config.run_name,
            run_id=None,
            run_dir=str(run_dir),
            started_at="2026-03-27T00:00:00+00:00",
            ended_at="2026-03-27T00:00:01+00:00",
            duration_seconds=1.0,
            status="warning" if fill_count == 0 else "succeeded",
            stage_records=[],
            warnings=[],
            errors=[],
            summary_json_path=str(run_dir / "daily_trading_summary.json"),
            summary_md_path=str(run_dir / "daily_trading_summary.md"),
            key_artifacts={"trade_decision_log_csv_path": str(run_dir / "trade_decision_log.csv")},
        )

    monkeypatch.setattr("trading_platform.orchestration.daily_replay.run_daily_trading_pipeline", fake_run)


def test_build_daily_replay_dates_supports_range_and_dates_file(tmp_path: Path) -> None:
    dates_file = tmp_path / "dates.txt"
    dates_file.write_text("2025-01-03\n2025-01-06,2025-01-07\n", encoding="utf-8")

    assert build_daily_replay_dates(dates_file=str(dates_file)) == ["2025-01-03", "2025-01-06", "2025-01-07"]
    assert build_daily_replay_dates(start_date="2025-01-03", end_date="2025-01-07", max_days=2) == [
        "2025-01-03",
        "2025-01-06",
    ]


def test_build_day_config_preserves_upstream_inputs_when_research_is_skipped(tmp_path: Path) -> None:
    base = DailyTradingWorkflowConfig(
        run_name="base",
        output_root=str(tmp_path / "daily"),
        research_output_dir=str(tmp_path / "alpha_research" / "run_configured"),
        registry_dir=str(tmp_path / "alpha_research" / "run_configured" / "research_registry"),
        promoted_dir=str(tmp_path / "promoted" / "run_current"),
        portfolio_dir=str(tmp_path / "strategy_portfolio" / "run_current"),
        activated_dir=str(tmp_path / "strategy_portfolio" / "run_current" / "activated"),
        export_dir=str(tmp_path / "strategy_portfolio" / "run_current" / "run_bundle_activated"),
        paper_output_dir=str(tmp_path / "daily" / "run_current" / "paper"),
        paper_state_path=str(tmp_path / "daily" / "run_current" / "paper_state.json"),
        report_dir=str(tmp_path / "daily" / "run_current" / "report"),
        promotion_policy_config="configs/promotion.yaml",
        strategy_portfolio_policy_config="configs/strategy_portfolio.yaml",
        research_mode="skip",
    )

    day_config = _build_day_config(
        base,
        replay_root=tmp_path / "replay",
        requested_date="2025-01-03",
        state_path=tmp_path / "replay" / "replay_state.json",
    )

    assert day_config.research_output_dir == base.research_output_dir
    assert day_config.promoted_dir == str(tmp_path / "replay" / "2025-01-03" / "promoted")
    assert day_config.portfolio_dir == str(tmp_path / "replay" / "2025-01-03" / "strategy_portfolio")
    assert day_config.paper_output_dir == str(tmp_path / "replay" / "2025-01-03" / "paper")


def test_run_daily_replay_writes_day_folders_and_carries_state(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _install_fake_daily_runner(monkeypatch)
    config = DailyReplayWorkflowConfig(
        daily_trading=_base_daily_config(tmp_path),
        output_dir=str(tmp_path / "replay"),
        start_date="2025-01-03",
        end_date="2025-01-07",
        max_days=3,
        stop_on_error=True,
        continue_on_error=False,
        replay=DailyReplayTuningConfig(),
    )

    result = run_daily_replay(config)

    assert result.status == "warning"
    assert (tmp_path / "replay" / "2025-01-03" / "daily_trading_summary.json").exists()
    assert (tmp_path / "replay" / "2025-01-06" / "daily_trading_summary.json").exists()
    assert (tmp_path / "replay" / "2025-01-07" / "daily_trading_summary.json").exists()
    assert (
        json.loads((tmp_path / "replay" / "2025-01-06" / "paper_state_before.json").read_text(encoding="utf-8"))[
            "day_index"
        ]
        == 1
    )
    summary = json.loads(Path(result.summary_json_path).read_text(encoding="utf-8"))
    assert summary["trading_day_count"] == 3
    assert summary["trade_day_count"] == 2
    assert summary["no_op_day_count"] == 1
    assert summary["failed_day_count"] == 0
    assert summary["avg_requested_symbol_count"] > 0
    assert summary["avg_usable_symbol_count"] > 0
    assert (tmp_path / "replay" / "2025-01-03" / "replay_day_input_summary.json").exists()
    assert (tmp_path / "replay" / "replay_daily_metrics.csv").exists()
    assert (tmp_path / "replay" / "replay_trade_log.csv").exists()
    assert (tmp_path / "replay" / "replay_strategy_activity.csv").exists()
    assert (tmp_path / "replay" / "replay_strategy_pnl.csv").exists()
    assert (tmp_path / "replay" / "replay_symbol_pnl.csv").exists()
    assert (tmp_path / "replay" / "replay_trade_pnl.csv").exists()
    assert (tmp_path / "replay" / "replay_pnl_attribution_summary.json").exists()


def test_run_daily_replay_continue_on_error_records_failure(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _install_fake_daily_runner(monkeypatch, fail_dates={"2025-01-06"})
    config = DailyReplayWorkflowConfig(
        daily_trading=_base_daily_config(tmp_path),
        output_dir=str(tmp_path / "replay"),
        start_date="2025-01-03",
        end_date="2025-01-07",
        max_days=3,
        stop_on_error=False,
        continue_on_error=True,
        replay=DailyReplayTuningConfig(),
    )

    result = run_daily_replay(config)

    assert result.status == "partial_failed"
    assert result.summary["failed_day_count"] == 1
    assert len(result.day_results) == 3


def test_run_daily_replay_stop_on_error_aborts(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _install_fake_daily_runner(monkeypatch, fail_dates={"2025-01-06"})
    config = DailyReplayWorkflowConfig(
        daily_trading=_base_daily_config(tmp_path),
        output_dir=str(tmp_path / "replay"),
        start_date="2025-01-03",
        end_date="2025-01-07",
        max_days=3,
        stop_on_error=True,
        continue_on_error=False,
        replay=DailyReplayTuningConfig(),
    )

    result = run_daily_replay(config)

    assert result.status == "partial_failed"
    assert len(result.day_results) == 2
    assert result.summary["aborted"] is True


def test_run_daily_replay_surfaces_missing_research_input_warning(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fake_run(config, *, replay_as_of_date=None, replay_settings=None, refresh_dashboard_static_data=None):
        run_dir = Path(config.output_root) / config.run_name
        run_dir.mkdir(parents=True, exist_ok=True)
        _write_json(run_dir / "daily_trading_summary.json", {"status": "warning", "active_strategy_count": 0})
        return DailyTradingResult(
            run_name=config.run_name,
            run_id=None,
            run_dir=str(run_dir),
            started_at="2026-03-27T00:00:00+00:00",
            ended_at="2026-03-27T00:00:01+00:00",
            duration_seconds=1.0,
            status="warning",
            stage_records=[],
            warnings=[],
            errors=[],
            summary_json_path=str(run_dir / "daily_trading_summary.json"),
            summary_md_path=str(run_dir / "daily_trading_summary.md"),
            key_artifacts={},
        )

    monkeypatch.setattr("trading_platform.orchestration.daily_replay.run_daily_trading_pipeline", fake_run)
    config = DailyReplayWorkflowConfig(
        daily_trading=_base_daily_config(tmp_path),
        output_dir=str(tmp_path / "replay"),
        start_date="2025-01-03",
        end_date="2025-01-03",
        stop_on_error=True,
        continue_on_error=False,
        replay=DailyReplayTuningConfig(),
    )

    result = run_daily_replay(config)

    input_summary = json.loads(
        (tmp_path / "replay" / "2025-01-03" / "replay_day_input_summary.json").read_text(encoding="utf-8")
    )
    assert "missing_research_artifacts_for_promotion" in input_summary["missing_input_warnings"]
    assert "empty_research_registry_for_promotion" in input_summary["missing_input_warnings"]
    assert "missing replay upstream inputs" in result.summary["warnings"]


def test_run_daily_replay_calls_replay_aggregation_once(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _install_fake_daily_runner(monkeypatch)
    calls: list[str] = []

    def fake_aggregate(*, replay_root):
        calls.append(str(replay_root))
        return {"strategy_rows": [], "symbol_rows": [], "trade_rows": [], "summary": {}}

    monkeypatch.setattr("trading_platform.orchestration.daily_replay.aggregate_replay_attribution", fake_aggregate)
    config = DailyReplayWorkflowConfig(
        daily_trading=_base_daily_config(tmp_path),
        output_dir=str(tmp_path / "replay"),
        start_date="2025-01-03",
        end_date="2025-01-07",
        max_days=3,
        stop_on_error=True,
        continue_on_error=False,
        replay=DailyReplayTuningConfig(),
    )

    run_daily_replay(config)

    assert calls == [str(tmp_path / "replay")]


def test_run_daily_replay_refreshes_dashboard_once_at_end(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _install_fake_daily_runner(monkeypatch)
    dashboard_calls: list[tuple[str, str]] = []

    def fake_dashboard(*, artifacts_root, output_dir):
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        marker = output_path / "overview.json"
        marker.write_text("{}", encoding="utf-8")
        dashboard_calls.append((str(artifacts_root), str(output_dir)))
        return {"overview_json": marker}

    monkeypatch.setattr("trading_platform.orchestration.daily_replay.build_dashboard_static_data", fake_dashboard)
    config = DailyReplayWorkflowConfig(
        daily_trading=DailyTradingWorkflowConfig(
            run_name="base_daily",
            output_root=str(tmp_path / "unused"),
            promotion_policy_config="configs/promotion.yaml",
            strategy_portfolio_policy_config="configs/strategy_portfolio.yaml",
            refresh_dashboard_static_data=True,
        ),
        output_dir=str(tmp_path / "replay"),
        start_date="2025-01-03",
        end_date="2025-01-07",
        max_days=3,
        stop_on_error=True,
        continue_on_error=False,
        replay=DailyReplayTuningConfig(),
    )

    result = run_daily_replay(config)

    assert dashboard_calls == [(str(tmp_path / "replay"), str(tmp_path / "replay" / "dashboard"))]
    assert (tmp_path / "replay" / "dashboard" / "overview.json").exists()
    assert result.summary["readiness_flags"]["diagnostics_complete"] is True


def test_run_daily_replay_profile_timings_writes_artifacts(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _install_fake_daily_runner(monkeypatch)
    config = DailyReplayWorkflowConfig(
        daily_trading=_base_daily_config(tmp_path),
        output_dir=str(tmp_path / "replay"),
        start_date="2025-01-03",
        end_date="2025-01-07",
        max_days=2,
        stop_on_error=True,
        continue_on_error=False,
        replay=DailyReplayTuningConfig(profile_timings=True),
    )

    result = run_daily_replay(config)

    assert Path(result.artifact_paths["replay_timing_by_day_csv_path"]).exists()
    assert Path(result.artifact_paths["replay_timing_summary_json_path"]).exists()
    timing_summary = json.loads(
        Path(result.artifact_paths["replay_timing_summary_json_path"]).read_text(encoding="utf-8")
    )
    assert timing_summary["day_count"] == 2
