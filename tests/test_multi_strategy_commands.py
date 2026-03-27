from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from trading_platform.broker.live_models import BrokerAccount
from trading_platform.cli.commands.live_dry_run_multi_strategy import cmd_live_dry_run_multi_strategy
from trading_platform.cli.commands.paper_run_multi_strategy import cmd_paper_run_multi_strategy
from trading_platform.live.preview import LivePreviewConfig, LivePreviewResult
from trading_platform.paper.models import PaperPortfolioState, PaperTradingRunResult
from trading_platform.portfolio.strategy_execution_handoff import StrategyExecutionHandoff


def _allocation_result():
    return SimpleNamespace(
        as_of="2025-01-04",
        combined_target_weights={"AAPL": 0.6, "MSFT": 0.4},
        latest_prices={"AAPL": 100.0, "MSFT": 200.0},
        sleeve_rows=[
            {"symbol": "AAPL", "sleeve_name": "core"},
            {"symbol": "MSFT", "sleeve_name": "satellite"},
        ],
        execution_symbol_coverage_rows=[],
        sleeve_bundles=[],
        summary={
            "enabled_sleeve_count": 2,
            "gross_exposure_before_constraints": 1.0,
            "gross_exposure_after_constraints": 1.0,
            "net_exposure_after_constraints": 1.0,
            "turnover_estimate": 0.1,
            "turnover_cap_binding": False,
            "symbols_removed_or_clipped": [],
            "requested_active_strategy_count": 2,
            "requested_symbol_count": 2,
            "usable_symbol_count": 2,
            "skipped_symbol_count": 0,
            "zero_target_reason": None,
            "latest_price_source_summary": {"yfinance": 2},
        },
    )


def test_cmd_paper_run_multi_strategy_uses_combined_targets(monkeypatch, tmp_path: Path, capsys) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.resolve_strategy_execution_handoff",
        lambda path, config=None: StrategyExecutionHandoff(
            source_kind="multi_strategy_config",
            source_path=str(path),
            portfolio_config=SimpleNamespace(cash_reserve_pct=0.05),
            summary={"active_strategy_count": 2},
            warnings=[],
        ),
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.write_strategy_execution_handoff_summary",
        lambda **kwargs: Path(kwargs["output_dir"]) / kwargs["artifact_name"],
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.allocate_multi_strategy_portfolio",
        lambda config: _allocation_result(),
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.write_multi_strategy_artifacts",
        lambda result, output_dir: {"allocation_summary_json_path": Path(output_dir) / "allocation_summary.json"},
    )

    def fake_run(**kwargs):
        captured["effective_weights"] = kwargs["latest_effective_weights"]
        captured["reserve_cash_pct"] = kwargs["config"].reserve_cash_pct
        captured["auto_apply_fills"] = kwargs["auto_apply_fills"]
        return PaperTradingRunResult(
            as_of="2025-01-04",
            state=PaperPortfolioState(cash=100_000.0),
            latest_prices={"AAPL": 100.0, "MSFT": 200.0},
            latest_scores={},
            latest_target_weights={"AAPL": 0.6, "MSFT": 0.4},
            scheduled_target_weights={"AAPL": 0.6, "MSFT": 0.4},
            orders=[],
            fills=[],
            diagnostics={},
        )

    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.run_paper_trading_cycle_for_targets",
        fake_run,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.write_paper_trading_artifacts",
        lambda *, result, output_dir: {"summary_path": Path(output_dir) / "paper_summary.json"},
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.persist_paper_run_outputs",
        lambda **kwargs: (
            {"paper_run_summary_latest_json_path": Path(kwargs["output_dir"]) / "paper_run_summary_latest.json"},
            [],
            {},
        ),
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.register_experiment",
        lambda record, tracker_dir: {"experiment_registry_path": tracker_dir / "registry.csv"},
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.build_paper_experiment_record",
        lambda output_dir: {},
    )

    args = SimpleNamespace(
        config=str(tmp_path / "portfolio.json"),
        state_path=str(tmp_path / "paper_state.json"),
        output_dir=str(tmp_path / "paper"),
    )
    cmd_paper_run_multi_strategy(args)

    assert captured["effective_weights"] == {"AAPL": 0.6, "MSFT": 0.4}
    assert captured["reserve_cash_pct"] == 0.05
    assert captured["auto_apply_fills"] is True
    assert "Enabled sleeves: 2" in capsys.readouterr().out


def test_cmd_paper_run_multi_strategy_honors_no_auto_apply_fills(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.resolve_strategy_execution_handoff",
        lambda path, config=None: StrategyExecutionHandoff(
            source_kind="multi_strategy_config",
            source_path=str(path),
            portfolio_config=SimpleNamespace(cash_reserve_pct=0.05),
            summary={"active_strategy_count": 2},
            warnings=[],
        ),
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.write_strategy_execution_handoff_summary",
        lambda **kwargs: Path(kwargs["output_dir"]) / kwargs["artifact_name"],
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.allocate_multi_strategy_portfolio",
        lambda config: _allocation_result(),
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.write_multi_strategy_artifacts",
        lambda result, output_dir: {},
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.write_paper_trading_artifacts",
        lambda *, result, output_dir: {},
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.persist_paper_run_outputs",
        lambda **kwargs: ({}, [], {}),
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.register_experiment",
        lambda record, tracker_dir: {"experiment_registry_path": tracker_dir / "registry.csv"},
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.build_paper_experiment_record",
        lambda output_dir: {},
    )

    def fake_run(**kwargs):
        captured["auto_apply_fills"] = kwargs["auto_apply_fills"]
        return PaperTradingRunResult(
            as_of="2025-01-04",
            state=PaperPortfolioState(cash=100_000.0),
            latest_prices={"AAPL": 100.0},
            latest_scores={},
            latest_target_weights={"AAPL": 1.0},
            scheduled_target_weights={"AAPL": 1.0},
            orders=[],
            fills=[],
            diagnostics={},
        )

    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.run_paper_trading_cycle_for_targets",
        fake_run,
    )

    args = SimpleNamespace(
        config=str(tmp_path / "portfolio.json"),
        state_path=str(tmp_path / "paper_state.json"),
        output_dir=str(tmp_path / "paper"),
        auto_apply_fills=False,
    )
    cmd_paper_run_multi_strategy(args)

    assert captured["auto_apply_fills"] is False


def test_cmd_live_dry_run_multi_strategy_uses_combined_targets(monkeypatch, tmp_path: Path, capsys) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        "trading_platform.cli.commands.live_dry_run_multi_strategy.resolve_strategy_execution_handoff",
        lambda path, config=None: StrategyExecutionHandoff(
            source_kind="multi_strategy_config",
            source_path=str(path),
            portfolio_config=SimpleNamespace(cash_reserve_pct=0.1),
            summary={"active_strategy_count": 2},
            warnings=[],
        ),
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.live_dry_run_multi_strategy.write_strategy_execution_handoff_summary",
        lambda **kwargs: Path(kwargs["output_dir"]) / kwargs["artifact_name"],
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.live_dry_run_multi_strategy.allocate_multi_strategy_portfolio",
        lambda config: _allocation_result(),
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.live_dry_run_multi_strategy.write_multi_strategy_artifacts",
        lambda result, output_dir: {"allocation_summary_json_path": Path(output_dir) / "allocation_summary.json"},
    )

    def fake_preview(**kwargs):
        captured["target_weights"] = kwargs["target_weights"]
        captured["config"] = kwargs["config"]
        return LivePreviewResult(
            run_id="multi_strategy|2025-01-04",
            as_of="2025-01-04",
            config=kwargs["config"],
            account=BrokerAccount(account_id="acct-1", cash=100_000.0, equity=100_000.0, buying_power=100_000.0),
            positions={},
            open_orders=[],
            latest_prices=kwargs["latest_prices"],
            target_weights=kwargs["target_weights"],
            target_diagnostics=kwargs["target_diagnostics"],
            reconciliation=SimpleNamespace(orders=[], diagnostics={"investable_equity": 90_000.0}),
            adjusted_orders=[],
            order_adjustment_diagnostics={},
            execution_result=None,
            reconciliation_rows=[],
            health_checks=[],
        )

    monkeypatch.setattr(
        "trading_platform.cli.commands.live_dry_run_multi_strategy.run_live_dry_run_preview_for_targets",
        fake_preview,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.live_dry_run_multi_strategy.write_live_dry_run_artifacts",
        lambda result: {
            "summary_json_path": Path(result.config.output_dir) / "live_dry_run_summary.json",
        },
    )
    output_dir = tmp_path / "live"
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "live_dry_run_summary.json").write_text('{"adjusted_order_count": 0}', encoding="utf-8")

    args = SimpleNamespace(
        config=str(tmp_path / "portfolio.json"),
        broker="mock",
        output_dir=str(output_dir),
    )
    cmd_live_dry_run_multi_strategy(args)

    assert captured["target_weights"] == {"AAPL": 0.6, "MSFT": 0.4}
    assert isinstance(captured["config"], LivePreviewConfig)
    assert captured["config"].reserve_cash_pct == 0.1
    assert "Enabled sleeves: 2" in capsys.readouterr().out


def test_cmd_paper_run_multi_strategy_loads_execution_config(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}
    execution_config = object()
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.resolve_strategy_execution_handoff",
        lambda path, config=None: StrategyExecutionHandoff(
            source_kind="multi_strategy_config",
            source_path=str(path),
            portfolio_config=SimpleNamespace(cash_reserve_pct=0.05),
            summary={"active_strategy_count": 1},
            warnings=[],
        ),
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.write_strategy_execution_handoff_summary",
        lambda **kwargs: Path(kwargs["output_dir"]) / kwargs["artifact_name"],
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.load_execution_config",
        lambda path: execution_config,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.allocate_multi_strategy_portfolio",
        lambda config: _allocation_result(),
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.write_multi_strategy_artifacts",
        lambda result, output_dir: {},
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.write_paper_trading_artifacts",
        lambda *, result, output_dir: {},
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.persist_paper_run_outputs",
        lambda **kwargs: ({}, [], {}),
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.register_experiment",
        lambda record, tracker_dir: {"experiment_registry_path": tracker_dir / "registry.csv"},
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.build_paper_experiment_record",
        lambda output_dir: {},
    )

    def fake_run(**kwargs):
        captured["execution_config"] = kwargs["execution_config"]
        return PaperTradingRunResult(
            as_of="2025-01-04",
            state=PaperPortfolioState(cash=100_000.0),
            latest_prices={"AAPL": 100.0},
            latest_scores={},
            latest_target_weights={"AAPL": 1.0},
            scheduled_target_weights={"AAPL": 1.0},
            orders=[],
            fills=[],
            diagnostics={},
        )

    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.run_paper_trading_cycle_for_targets",
        fake_run,
    )

    args = SimpleNamespace(
        config=str(tmp_path / "portfolio.json"),
        execution_config=str(tmp_path / "execution.json"),
        state_path=str(tmp_path / "paper_state.json"),
        output_dir=str(tmp_path / "paper"),
    )
    cmd_paper_run_multi_strategy(args)

    assert captured["execution_config"] is execution_config


def test_cmd_live_dry_run_multi_strategy_loads_execution_config(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}
    execution_config = object()
    monkeypatch.setattr(
        "trading_platform.cli.commands.live_dry_run_multi_strategy.resolve_strategy_execution_handoff",
        lambda path, config=None: StrategyExecutionHandoff(
            source_kind="multi_strategy_config",
            source_path=str(path),
            portfolio_config=SimpleNamespace(cash_reserve_pct=0.05),
            summary={"active_strategy_count": 1},
            warnings=[],
        ),
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.live_dry_run_multi_strategy.write_strategy_execution_handoff_summary",
        lambda **kwargs: Path(kwargs["output_dir"]) / kwargs["artifact_name"],
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.live_dry_run_multi_strategy.load_execution_config",
        lambda path: execution_config,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.live_dry_run_multi_strategy.allocate_multi_strategy_portfolio",
        lambda config: _allocation_result(),
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.live_dry_run_multi_strategy.write_multi_strategy_artifacts",
        lambda result, output_dir: {},
    )

    def fake_preview(**kwargs):
        captured["execution_config"] = kwargs["execution_config"]
        return LivePreviewResult(
            run_id="multi_strategy|2025-01-04",
            as_of="2025-01-04",
            config=kwargs["config"],
            account=BrokerAccount(account_id="acct-1", cash=100_000.0, equity=100_000.0, buying_power=100_000.0),
            positions={},
            open_orders=[],
            latest_prices=kwargs["latest_prices"],
            target_weights=kwargs["target_weights"],
            target_diagnostics=kwargs["target_diagnostics"],
            reconciliation=SimpleNamespace(orders=[], diagnostics={"investable_equity": 90_000.0}),
            adjusted_orders=[],
            order_adjustment_diagnostics={},
            execution_result=None,
            reconciliation_rows=[],
            health_checks=[],
        )

    monkeypatch.setattr(
        "trading_platform.cli.commands.live_dry_run_multi_strategy.run_live_dry_run_preview_for_targets",
        fake_preview,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.live_dry_run_multi_strategy.write_live_dry_run_artifacts",
        lambda result: {"summary_json_path": Path(result.config.output_dir) / "live_dry_run_summary.json"},
    )
    output_dir = tmp_path / "live"
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "live_dry_run_summary.json").write_text('{"adjusted_order_count": 0}', encoding="utf-8")

    args = SimpleNamespace(
        config=str(tmp_path / "portfolio.json"),
        execution_config=str(tmp_path / "execution.json"),
        broker="mock",
        output_dir=str(output_dir),
    )
    cmd_live_dry_run_multi_strategy(args)

    assert captured["execution_config"] is execution_config


def test_cmd_paper_run_multi_strategy_skips_when_no_active_strategies(monkeypatch, tmp_path: Path, capsys) -> None:
    activated_path = tmp_path / "activated_strategy_portfolio.json"
    activated_path.write_text(
        '{"summary":{"active_row_count":0,"activated_unconditional_count":0,"activated_conditional_count":0,"inactive_conditional_count":1},"active_strategies":[],"strategies":[{"preset_name":"generated_conditional","promotion_variant":"conditional","is_active":false}]}',
        encoding="utf-8",
    )

    args = SimpleNamespace(
        config=str(activated_path),
        state_path=str(tmp_path / "paper_state.json"),
        output_dir=str(tmp_path / "paper"),
    )
    cmd_paper_run_multi_strategy(args)

    captured = capsys.readouterr().out
    assert "No active strategies available for paper trading." in captured
    assert (tmp_path / "paper" / "paper_active_strategy_summary.json").exists()


def test_cmd_paper_run_multi_strategy_accepts_activated_portfolio_input(monkeypatch, tmp_path: Path) -> None:
    activated_path = tmp_path / "activated_strategy_portfolio.json"
    activated_path.write_text(
        f"""{{
  "source_portfolio_path": "artifacts/strategy_portfolio/run_current/strategy_portfolio.json",
  "summary": {{
    "active_row_count": 1,
    "activated_unconditional_count": 0,
    "activated_conditional_count": 1,
    "inactive_conditional_count": 0
  }},
  "active_strategies": [
    {{
      "preset_name": "generated_conditional",
      "target_capital_fraction": 1.0,
      "generated_preset_path": "{(tmp_path / 'generated_conditional.json').as_posix()}",
      "promotion_variant": "conditional",
      "condition_id": "regime::risk_on",
      "condition_type": "regime",
      "activation_state": "active",
      "is_active": true
    }}
  ]
}}""",
        encoding="utf-8",
    )
    (tmp_path / "generated_conditional.json").write_text('{"name":"generated_conditional","params":{"symbols":["AAPL"],"strategy":"sma_cross","fast":5,"slow":20}}', encoding="utf-8")

    captured: dict[str, object] = {}
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.allocate_multi_strategy_portfolio",
        lambda config: _allocation_result(),
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.write_multi_strategy_artifacts",
        lambda result, output_dir: {},
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.write_paper_trading_artifacts",
        lambda *, result, output_dir: {"summary_path": Path(output_dir) / "paper_summary.json"},
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.persist_paper_run_outputs",
        lambda **kwargs: (
            {"paper_run_summary_latest_json_path": Path(kwargs["output_dir"]) / "paper_run_summary_latest.json"},
            [],
            {"active_strategy_count": 1, "active_conditional_count": 1},
        ),
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.register_experiment",
        lambda record, tracker_dir: {"experiment_registry_path": tracker_dir / "registry.csv"},
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.build_paper_experiment_record",
        lambda output_dir: {},
    )

    def fake_run(**kwargs):
        captured["handoff"] = kwargs["target_diagnostics"]["strategy_execution_handoff"]
        return PaperTradingRunResult(
            as_of="2025-01-04",
            state=PaperPortfolioState(cash=100_000.0),
            latest_prices={"AAPL": 100.0},
            latest_scores={},
            latest_target_weights={"AAPL": 1.0},
            scheduled_target_weights={"AAPL": 1.0},
            orders=[],
            fills=[],
            diagnostics={},
        )

    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_multi_strategy.run_paper_trading_cycle_for_targets",
        fake_run,
    )

    args = SimpleNamespace(
        config=str(activated_path),
        state_path=str(tmp_path / "paper_state.json"),
        output_dir=str(tmp_path / "paper"),
    )
    cmd_paper_run_multi_strategy(args)

    assert captured["handoff"]["active_conditional_count"] == 1


def test_cmd_live_dry_run_multi_strategy_accepts_run_bundle_input(monkeypatch, tmp_path: Path) -> None:
    multi_strategy_path = tmp_path / "multi_strategy.json"
    multi_strategy_path.write_text(
        '{"sleeves":[{"sleeve_name":"generated_base","preset_name":"generated_base","target_capital_weight":1.0}],"cash_reserve_pct":0.1,"activation_applied":true,"active_strategy_count":1,"active_unconditional_count":1,"active_conditional_count":0,"inactive_conditional_count":1}',
        encoding="utf-8",
    )
    run_bundle_path = tmp_path / "strategy_portfolio_run_bundle.json"
    run_bundle_path.write_text(
        json.dumps(
            {
                "multi_strategy_config_path": str(multi_strategy_path),
                "activation_applied": True,
                "active_strategy_count": 1,
                "active_unconditional_count": 1,
                "active_conditional_count": 0,
                "inactive_conditional_count": 1,
            }
        ),
        encoding="utf-8",
    )

    captured: dict[str, object] = {}
    monkeypatch.setattr(
        "trading_platform.cli.commands.live_dry_run_multi_strategy.allocate_multi_strategy_portfolio",
        lambda config: _allocation_result(),
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.live_dry_run_multi_strategy.write_multi_strategy_artifacts",
        lambda result, output_dir: {},
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.live_dry_run_multi_strategy.write_live_dry_run_artifacts",
        lambda result: {"summary_json_path": Path(result.config.output_dir) / "live_dry_run_summary.json"},
    )

    def fake_preview(**kwargs):
        captured["handoff"] = kwargs["target_diagnostics"]["strategy_execution_handoff"]
        return LivePreviewResult(
            run_id="multi_strategy|2025-01-04",
            as_of="2025-01-04",
            config=kwargs["config"],
            account=BrokerAccount(account_id="acct-1", cash=100_000.0, equity=100_000.0, buying_power=100_000.0),
            positions={},
            open_orders=[],
            latest_prices=kwargs["latest_prices"],
            target_weights=kwargs["target_weights"],
            target_diagnostics=kwargs["target_diagnostics"],
            reconciliation=SimpleNamespace(orders=[], diagnostics={"investable_equity": 90_000.0}),
            adjusted_orders=[],
            order_adjustment_diagnostics={},
            execution_result=None,
            reconciliation_rows=[],
            health_checks=[],
        )

    monkeypatch.setattr(
        "trading_platform.cli.commands.live_dry_run_multi_strategy.run_live_dry_run_preview_for_targets",
        fake_preview,
    )
    output_dir = tmp_path / "live"
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "live_dry_run_summary.json").write_text('{"adjusted_order_count": 0}', encoding="utf-8")

    args = SimpleNamespace(
        config=str(run_bundle_path),
        broker="mock",
        output_dir=str(output_dir),
    )
    cmd_live_dry_run_multi_strategy(args)

    assert captured["handoff"]["activation_applied"] is True
