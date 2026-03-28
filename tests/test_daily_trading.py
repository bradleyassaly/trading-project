from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from trading_platform.config.models import MultiStrategyPortfolioConfig, MultiStrategySleeveConfig
from trading_platform.config.workflow_models import DailyTradingStageToggles, DailyTradingWorkflowConfig
from trading_platform.decision_journal.models import (
    CandidateEvaluation,
    DecisionJournalBundle,
    SizingDecision,
    TradeDecisionRecord,
)
from trading_platform.orchestration.daily_trading import _summarize_paper_run, run_daily_trading_pipeline
from trading_platform.paper.models import PaperPortfolioState, PaperTradingRunResult
from trading_platform.portfolio.strategy_execution_handoff import StrategyExecutionHandoff


@dataclass
class _FakeLineageService:
    created: bool = False
    completed: bool = False
    failed: bool = False

    def create_portfolio_run(self, **kwargs):
        self.created = True
        return 101

    def complete_portfolio_run(self, run_id, *, notes: str | None = None) -> None:
        self.completed = True

    def fail_portfolio_run(self, run_id, *, notes: str | None = None) -> None:
        self.failed = True


class _FakeResearchMemory:
    def init_schema(self, schema_name=None) -> None:
        return None


def _write_json(path: Path, payload: dict) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return str(path)


def _base_config(
    tmp_path: Path, *, best_effort_mode: bool = False, strict_mode: bool = True
) -> DailyTradingWorkflowConfig:
    return DailyTradingWorkflowConfig(
        run_name="daily_smoke",
        output_root=str(tmp_path / "daily"),
        research_output_dir=str(tmp_path / "alpha_research" / "run_configured"),
        registry_dir=str(tmp_path / "alpha_research" / "run_configured" / "research_registry"),
        promoted_dir=str(tmp_path / "promoted"),
        portfolio_dir=str(tmp_path / "portfolio"),
        activated_dir=str(tmp_path / "portfolio" / "activated"),
        export_dir=str(tmp_path / "bundle"),
        paper_output_dir=str(tmp_path / "paper"),
        paper_state_path=str(tmp_path / "paper" / "state.json"),
        report_dir=str(tmp_path / "paper" / "report"),
        promotion_policy_config="configs/promotion.yaml",
        strategy_portfolio_policy_config="configs/strategy_portfolio.yaml",
        strict_mode=strict_mode,
        best_effort_mode=best_effort_mode,
    )


def _install_common_mocks(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, *, no_active: bool = False, zero_targets: bool = False
):
    fake_lineage = _FakeLineageService()
    monkeypatch.setattr(
        "trading_platform.orchestration.daily_trading.DatabaseLineageService.from_config",
        lambda **kwargs: fake_lineage,
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.daily_trading.build_research_memory_service",
        lambda **kwargs: _FakeResearchMemory(),
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.daily_trading.load_promotion_policy_config",
        lambda path: SimpleNamespace(),
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.daily_trading.load_strategy_portfolio_policy_config",
        lambda path: SimpleNamespace(
            evaluate_conditional_activation=True,
            activation_context_sources=["regime"],
            include_inactive_conditionals_in_output=True,
        ),
    )

    def fake_refresh_registry(**kwargs):
        output_dir = Path(kwargs["output_dir"])
        return {
            "registry_json_path": _write_json(output_dir / "research_registry.json", {"rows": []}),
            "promotion_candidates_json_path": _write_json(output_dir / "promotion_candidates.json", {"rows": []}),
        }

    def fake_promote(**kwargs):
        output_dir = Path(kwargs["output_dir"])
        rows = [
            {
                "preset_name": "generated_base",
                "promotion_variant": "unconditional",
                "signal_family": "momentum",
            },
            {
                "preset_name": "generated_conditional",
                "promotion_variant": "conditional",
                "signal_family": "momentum",
                "condition_id": "regime::risk_on",
                "condition_type": "regime",
            },
        ]
        return {
            "selected_count": len(rows),
            "promoted_rows": rows,
            "promoted_index_path": _write_json(output_dir / "promoted_strategies.json", {"strategies": rows}),
        }

    def fake_build_portfolio(**kwargs):
        output_dir = Path(kwargs["output_dir"])
        selected_rows = [
            {
                "preset_name": "generated_base",
                "allocation_weight": 1.0,
                "target_capital_fraction": 1.0,
                "promotion_variant": "unconditional",
                "generated_preset_path": str(tmp_path / "generated_base.json"),
            },
            {
                "preset_name": "generated_conditional",
                "allocation_weight": 0.0,
                "target_capital_fraction": 0.0,
                "promotion_variant": "conditional",
                "condition_id": "regime::risk_on",
                "condition_type": "regime",
                "activation_conditions": [{"condition_id": "regime::risk_on", "condition_type": "regime"}],
                "generated_preset_path": str(tmp_path / "generated_conditional.json"),
            },
        ]
        _write_json(
            output_dir / "strategy_portfolio.json",
            {
                "summary": {
                    "total_selected_strategies": 2,
                    "selected_conditional_variant_count": 1,
                },
                "selected_strategies": selected_rows,
                "shadow_strategies": [],
            },
        )
        (output_dir / "strategy_portfolio.csv").write_text("preset_name\ngenerated_base\n", encoding="utf-8")
        (output_dir / "strategy_portfolio_condition_summary.csv").write_text(
            "preset_name,condition_id\ngenerated_conditional,regime::risk_on\n",
            encoding="utf-8",
        )
        return {
            "strategy_portfolio_json_path": str(output_dir / "strategy_portfolio.json"),
            "strategy_portfolio_csv_path": str(output_dir / "strategy_portfolio.csv"),
            "strategy_portfolio_condition_summary_path": str(output_dir / "strategy_portfolio_condition_summary.csv"),
        }

    def fake_activate(**kwargs):
        output_dir = Path(kwargs["output_dir"])
        active_rows = (
            []
            if no_active
            else [
                {
                    "preset_name": "generated_base",
                    "allocation_weight": 1.0,
                    "target_capital_fraction": 1.0,
                    "promotion_variant": "unconditional",
                    "is_active": True,
                    "activation_state": "active",
                    "generated_preset_path": str(tmp_path / "generated_base.json"),
                }
            ]
        )
        strategies = list(active_rows) + [
            {
                "preset_name": "generated_conditional",
                "promotion_variant": "conditional",
                "condition_id": "regime::risk_on",
                "condition_type": "regime",
                "is_active": False,
                "activation_state": "inactive",
                "generated_preset_path": str(tmp_path / "generated_conditional.json"),
            }
        ]
        _write_json(
            output_dir / "activated_strategy_portfolio.json",
            {
                "source_portfolio_path": str(Path(kwargs["portfolio_path"]) / "strategy_portfolio.json"),
                "summary": {
                    "active_row_count": len(active_rows),
                    "activated_unconditional_count": len(active_rows),
                    "activated_conditional_count": 0,
                    "inactive_conditional_count": 1,
                },
                "strategies": strategies,
                "active_strategies": active_rows,
            },
        )
        (output_dir / "activated_strategy_portfolio.csv").write_text("preset_name\n", encoding="utf-8")
        return {
            "activated_strategy_portfolio_json_path": str(output_dir / "activated_strategy_portfolio.json"),
            "activated_strategy_portfolio_csv_path": str(output_dir / "activated_strategy_portfolio.csv"),
        }

    def fake_export(**kwargs):
        output_dir = Path(kwargs["output_dir"])
        return {
            "multi_strategy_config_path": _write_json(
                output_dir / "strategy_portfolio_multi_strategy.json", {"sleeves": []}
            ),
            "pipeline_config_path": _write_json(output_dir / "strategy_portfolio_pipeline.yaml", {"run_name": "paper"}),
            "run_bundle_path": _write_json(
                output_dir / "strategy_portfolio_run_bundle.json", {"activation_applied": True}
            ),
        }

    def fake_write_handoff_summary(*, handoff, output_dir, artifact_name):
        return Path(_write_json(Path(output_dir) / artifact_name, handoff.summary))

    handoff = StrategyExecutionHandoff(
        source_kind="activated_portfolio",
        source_path=str(tmp_path / "portfolio" / "activated" / "activated_strategy_portfolio.json"),
        portfolio_config=(
            None
            if no_active
            else MultiStrategyPortfolioConfig(
                sleeves=[
                    MultiStrategySleeveConfig(
                        sleeve_name="generated_base",
                        preset_name="generated_base",
                        target_capital_weight=1.0,
                        preset_path=str(tmp_path / "generated_base.json"),
                    )
                ],
                active_strategy_count=1,
                active_unconditional_count=1,
                active_conditional_count=0,
                inactive_conditional_count=1,
            )
        ),
        summary={
            "active_strategy_count": 0 if no_active else 1,
            "active_unconditional_count": 0 if no_active else 1,
            "active_conditional_count": 0,
            "inactive_conditional_count": 1,
            "source_portfolio_path": str(tmp_path / "portfolio" / "activated" / "activated_strategy_portfolio.json"),
            "activation_applied": True,
            "fail_if_no_active_strategies": False,
        },
        warnings=["no_active_strategies"] if no_active else [],
    )

    allocation_result = SimpleNamespace(
        as_of="2026-03-27T16:00:00+00:00",
        combined_target_weights={} if zero_targets else {"AAPL": 1.0},
        latest_prices={} if zero_targets else {"AAPL": 100.0},
        sleeve_rows=[] if zero_targets else [{"symbol": "AAPL"}],
        sleeve_bundles=[],
        execution_symbol_coverage_rows=[],
        summary={
            "enabled_sleeve_count": 0 if no_active else 1,
            "requested_active_strategy_count": 0 if no_active else 1,
            "requested_symbol_count": 0 if zero_targets else 1,
            "pre_validation_target_symbol_count": 0 if zero_targets else 1,
            "post_validation_target_symbol_count": 0 if zero_targets else 1,
            "usable_symbol_count": 0 if zero_targets else 1,
            "skipped_symbol_count": 0,
            "zero_target_reason": "no_targets_generated" if zero_targets else "",
            "target_drop_stage": "signal_scoring" if zero_targets else "",
            "target_drop_reason": "empty_signal_scores" if zero_targets else "",
            "gross_exposure_after_constraints": 0.0 if zero_targets else 1.0,
            "symbols_removed_or_clipped": [],
            "turnover_cap_binding": 0,
            "turnover_estimate": 0.0,
            "latest_price_source_summary": {"yfinance": 0 if zero_targets else 1},
            "generated_preset_path": str(tmp_path / "generated_base.json"),
            "signal_artifact_path": str(tmp_path / "alpha_research" / "run_configured"),
        },
    )

    def fake_write_multi_strategy_artifacts(result, output_dir):
        return {"allocation_summary_path": _write_json(Path(output_dir) / "allocation_summary.json", result.summary)}

    def fake_run_paper(**kwargs):
        state = PaperPortfolioState(
            as_of="2026-03-27T16:00:00+00:00",
            cash=0.0 if not zero_targets else 100_000.0,
            positions={},
            initial_cash_basis=100_000.0,
        )
        decision_bundle = DecisionJournalBundle(
            candidate_evaluations=[
                CandidateEvaluation(
                    decision_id="cand-1",
                    timestamp="2026-03-27T16:00:00+00:00",
                    run_id=None,
                    cycle_id=None,
                    symbol="AAPL",
                    side="long",
                    strategy_id="generated_base",
                    universe_id="test",
                    candidate_status="selected",
                    final_signal_score=0.9,
                    rank=1,
                )
            ],
            sizing_decisions=[
                SizingDecision(
                    decision_id="size-1",
                    timestamp="2026-03-27T16:00:00+00:00",
                    run_id=None,
                    cycle_id=None,
                    symbol="AAPL",
                    strategy_id="generated_base",
                    side="long",
                    target_weight_post_constraint=1.0,
                    target_quantity=1,
                    current_quantity=0,
                )
            ],
            trade_decisions=[
                TradeDecisionRecord(
                    decision_id="trade-1",
                    timestamp="2026-03-27T16:00:00+00:00",
                    run_id=None,
                    cycle_id=None,
                    symbol="AAPL",
                    side="long",
                    strategy_id="generated_base",
                    universe_id="test",
                    candidate_status="selected",
                    entry_reason_summary="enter_new_position",
                    final_signal_score=0.9,
                    target_weight_post_constraint=1.0,
                    target_quantity=1,
                    current_quantity=0,
                    metadata={"current_weight": 0.0},
                )
            ],
        )
        return PaperTradingRunResult(
            as_of="2026-03-27T16:00:00+00:00",
            state=state,
            latest_prices={} if zero_targets else {"AAPL": 100.0},
            latest_scores={},
            latest_target_weights={} if zero_targets else {"AAPL": 1.0},
            scheduled_target_weights={} if zero_targets else {"AAPL": 1.0},
            orders=[],
            fills=[],
            diagnostics={},
            decision_bundle=decision_bundle,
        )

    def fake_write_paper(result, output_dir):
        return {"paper_orders_path": str(Path(output_dir) / "paper_orders.csv")}

    def fake_persist(*, output_dir, **kwargs):
        payload = {
            "requested_symbol_count": 0 if zero_targets else 1,
            "usable_symbol_count": 0 if zero_targets else 1,
            "pre_validation_target_symbol_count": 0 if zero_targets else 1,
            "post_validation_target_symbol_count": 0 if zero_targets else 1,
            "executable_order_count": 0 if zero_targets else 1,
            "fill_count": 0 if zero_targets else 1,
            "zero_target_reason": "no_targets_generated" if zero_targets else "",
            "source_portfolio_path": str(tmp_path / "portfolio" / "activated" / "activated_strategy_portfolio.json"),
            "score_band_enabled": True,
            "entry_threshold_used": 0.85,
            "exit_threshold_used": 0.60,
            "blocked_entries_count": 1,
            "held_in_hold_zone_count": 2,
            "forced_exit_count": 0,
        }
        output_path = Path(output_dir)
        _write_json(output_path / "paper_run_summary_latest.json", payload)
        return (
            {"paper_run_summary_latest_json_path": str(output_path / "paper_run_summary_latest.json")},
            [],
            payload,
        )

    def fake_build_report(account_dir):
        return SimpleNamespace(as_of="2026-03-27", latest_equity=100_000.0)

    def fake_write_report(*, report, output_dir):
        output_path = Path(output_dir)
        return {
            "json_path": Path(_write_json(output_path / "paper_account_report.json", {"as_of": report.as_of})),
            "csv_path": Path(output_path / "paper_account_summary.csv"),
        }

    monkeypatch.setattr(
        "trading_platform.orchestration.daily_trading.refresh_research_registry_bundle", fake_refresh_registry
    )
    monkeypatch.setattr("trading_platform.orchestration.daily_trading.apply_research_promotions", fake_promote)
    monkeypatch.setattr("trading_platform.orchestration.daily_trading.build_strategy_portfolio", fake_build_portfolio)
    monkeypatch.setattr("trading_platform.orchestration.daily_trading.activate_strategy_portfolio", fake_activate)
    monkeypatch.setattr(
        "trading_platform.orchestration.daily_trading.export_strategy_portfolio_run_config", fake_export
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.daily_trading.resolve_strategy_execution_handoff",
        lambda *args, **kwargs: handoff,
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.daily_trading.write_strategy_execution_handoff_summary",
        fake_write_handoff_summary,
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.daily_trading.allocate_multi_strategy_portfolio", lambda cfg: allocation_result
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.daily_trading.write_multi_strategy_artifacts",
        fake_write_multi_strategy_artifacts,
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.daily_trading.run_paper_trading_cycle_for_targets", fake_run_paper
    )
    monkeypatch.setattr("trading_platform.orchestration.daily_trading.write_paper_trading_artifacts", fake_write_paper)
    monkeypatch.setattr("trading_platform.orchestration.daily_trading.persist_paper_run_outputs", fake_persist)
    monkeypatch.setattr("trading_platform.orchestration.daily_trading.build_paper_account_report", fake_build_report)
    monkeypatch.setattr("trading_platform.orchestration.daily_trading.write_paper_account_report", fake_write_report)
    return fake_lineage


def test_daily_trading_happy_path_writes_summary(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config = _base_config(tmp_path)
    lineage = _install_common_mocks(monkeypatch, tmp_path)

    result = run_daily_trading_pipeline(config)
    summary = json.loads(Path(result.summary_json_path).read_text(encoding="utf-8"))

    assert result.status == "succeeded"
    assert summary["promoted_strategy_count"] == 2
    assert summary["selected_portfolio_strategy_count"] == 2
    assert summary["active_strategy_count"] == 1
    assert summary["post_validation_target_symbol_count"] == 1
    assert summary["executable_order_count"] == 1
    assert summary["fill_count"] == 1
    assert summary["score_band_enabled"] is True
    assert summary["blocked_entries_count"] == 1
    assert (tmp_path / "daily" / "daily_smoke" / "trade_decision_log.csv").exists()
    assert summary["top_selected_strategies"][0]["strategy_id"] == "generated_base"
    assert summary["strategy_quality_summary"]["strategy_count"] == 2
    assert Path(result.summary_md_path).exists()
    assert (Path(config.report_dir) / "strategy_comparison_summary.csv").exists()
    assert (Path(config.report_dir) / "strategy_performance_history.csv").exists()
    assert (Path(config.report_dir) / "rolling_sharpe_by_strategy.csv").exists()
    assert (Path(config.report_dir) / "rolling_ic_by_signal.csv").exists()
    assert (Path(config.report_dir) / "drawdown_by_strategy.csv").exists()
    assert lineage.created is True
    assert lineage.completed is True
    decision_log = pd.read_csv(tmp_path / "daily" / "daily_smoke" / "trade_decision_log.csv")
    assert "band_decision" in decision_log.columns
    assert "entry_threshold" in decision_log.columns
    assert decision_log.iloc[0]["action_reason"] == "enter_new_position"


def test_daily_trading_fast_refresh_uses_refresh_runner(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config = _base_config(tmp_path)
    config = DailyTradingWorkflowConfig(
        **{
            **config.to_cli_defaults(),
            "research_mode": "fast_refresh",
            "research_config": "configs/alpha_research.yaml",
            "stages": DailyTradingStageToggles(
                refresh_inputs=False,
                research=True,
                promote=True,
                build_portfolio=True,
                activate_portfolio=True,
                export_bundle=True,
                paper_run=True,
                report=True,
            ),
        }
    )
    _install_common_mocks(monkeypatch, tmp_path)
    called = {"fast_refresh": False}

    monkeypatch.setattr(
        "trading_platform.orchestration.daily_trading.load_alpha_research_workflow_config",
        lambda path: SimpleNamespace(
            symbols=["AAPL"],
            universe=None,
            feature_dir=str(tmp_path / "features"),
            signal_family="momentum",
            signal_families=["momentum"],
            lookbacks=[5],
            horizons=[1],
            min_rows=20,
            top_quantile=0.5,
            bottom_quantile=0.5,
            candidate_grid_preset="standard",
            signal_composition_preset="standard",
            max_variants_per_family=None,
            train_size=20,
            test_size=10,
            step_size=10,
            min_train_size=20,
            portfolio_top_n=2,
            portfolio_long_quantile=0.2,
            portfolio_short_quantile=0.2,
            commission=0.0,
            min_price=None,
            min_volume=None,
            min_avg_dollar_volume=None,
            max_adv_participation=0.05,
            max_position_pct_of_adv=0.1,
            max_notional_per_name=None,
            slippage_bps_per_turnover=0.0,
            slippage_bps_per_adv=0.0,
            dynamic_recent_quality_window=20,
            dynamic_min_history=20,
            dynamic_downweight_mean_rank_ic=0.0,
            dynamic_deactivate_mean_rank_ic=0.0,
            regime_aware_enabled=False,
            regime_min_history=20,
            regime_underweight_mean_rank_ic=0.0,
            regime_exclude_mean_rank_ic=0.0,
            equity_context_enabled=False,
            equity_context_include_volume=False,
            fundamentals_enabled=False,
            fundamentals_daily_features_path=None,
            enable_context_confirmations=None,
            enable_relative_features=None,
            enable_flow_confirmations=None,
            enable_ensemble=False,
            ensemble_mode="disabled",
            ensemble_weight_method="equal",
            ensemble_normalize_scores="rank_pct",
            ensemble_max_members=5,
            ensemble_max_members_per_family=None,
            ensemble_minimum_member_observations=0,
            ensemble_minimum_member_metric=None,
            require_runtime_computability_for_approval=False,
            min_runtime_computable_symbols_for_approval=5,
            allow_research_only_noncomputable_candidates=True,
            runtime_computability_penalty_on_ranking=0.02,
            runtime_computability_check_mode="strict",
            require_composite_runtime_computability_for_approval=False,
            min_composite_runtime_computable_symbols_for_approval=5,
            allow_research_only_noncomputable_composites=True,
            composite_runtime_computability_check_mode="strict",
            composite_runtime_computability_penalty_on_ranking=0.02,
            fast_refresh_mode=True,
            skip_heavy_diagnostics=True,
            reuse_existing_fold_results=True,
            restrict_to_existing_candidates=True,
            max_families_for_refresh=None,
            max_candidates_for_refresh=None,
        ),
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.daily_trading.refresh_alpha_research_artifacts",
        lambda **kwargs: called.__setitem__("fast_refresh", True) or {},
    )

    result = run_daily_trading_pipeline(config)
    assert result.status == "succeeded"
    assert called["fast_refresh"] is True


def test_daily_trading_no_active_strategy_path_warns(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config = _base_config(tmp_path)
    _install_common_mocks(monkeypatch, tmp_path, no_active=True)

    result = run_daily_trading_pipeline(config)
    summary = json.loads(Path(result.summary_json_path).read_text(encoding="utf-8"))

    assert result.status == "warning"
    assert summary["active_strategy_count"] == 0
    assert summary["fill_count"] == 0
    assert any(record.stage_name == "paper_run" and record.status == "warning" for record in result.stage_records)


def test_daily_trading_zero_target_reason_is_reported(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config = _base_config(tmp_path)
    _install_common_mocks(monkeypatch, tmp_path, zero_targets=True)

    result = run_daily_trading_pipeline(config)
    summary = json.loads(Path(result.summary_json_path).read_text(encoding="utf-8"))

    assert result.status == "warning"
    assert summary["zero_target_reason"] == "no_targets_generated"
    assert summary["post_validation_target_symbol_count"] == 0


def test_daily_trading_paper_stage_uses_target_weight_fallback_for_target_count(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = _base_config(tmp_path)
    _install_common_mocks(monkeypatch, tmp_path)

    allocation_result = SimpleNamespace(
        as_of="2026-03-27T16:00:00+00:00",
        combined_target_weights={"AAPL": 1.0},
        latest_prices={"AAPL": 100.0},
        sleeve_rows=[{"symbol": "AAPL"}],
        sleeve_bundles=[],
        execution_symbol_coverage_rows=[],
        summary={
            "enabled_sleeve_count": 1,
            "requested_active_strategy_count": 1,
            "requested_symbol_count": 1,
            "pre_validation_target_symbol_count": 1,
            "usable_symbol_count": 1,
            "skipped_symbol_count": 0,
            "zero_target_reason": "",
            "target_drop_stage": "",
            "target_drop_reason": "",
            "gross_exposure_after_constraints": 1.0,
            "symbols_removed_or_clipped": [],
            "turnover_cap_binding": 0,
            "turnover_estimate": 0.0,
            "latest_price_source_summary": {"yfinance": 1},
            "generated_preset_path": str(tmp_path / "generated_base.json"),
            "signal_artifact_path": str(tmp_path / "alpha_research" / "run_configured"),
        },
    )
    monkeypatch.setattr(
        "trading_platform.orchestration.daily_trading.allocate_multi_strategy_portfolio", lambda cfg: allocation_result
    )

    result = run_daily_trading_pipeline(config)

    assert result.status == "succeeded"
    assert any(record.stage_name == "paper_run" and record.status == "succeeded" for record in result.stage_records)


def test_summarize_paper_run_reads_nested_summary_payload(tmp_path: Path) -> None:
    paper_dir = tmp_path / "paper"
    _write_json(
        paper_dir / "paper_run_summary_latest.json",
        {
            "summary": {
                "requested_symbol_count": 10,
                "usable_symbol_count": 9,
                "pre_validation_target_symbol_count": 8,
                "post_validation_target_symbol_count": 7,
                "executable_order_count": 6,
                "fill_count": 5,
                "zero_target_reason": "",
                "source_portfolio_path": "artifacts/strategy_portfolio/run_current",
            }
        },
    )

    summary = _summarize_paper_run(paper_dir)

    assert summary["requested_symbol_count"] == 10
    assert summary["usable_symbol_count"] == 9
    assert summary["pre_validation_target_symbol_count"] == 8
    assert summary["post_validation_target_symbol_count"] == 7
    assert summary["executable_order_count"] == 6
    assert summary["fill_count"] == 5


def test_daily_trading_best_effort_continues_after_stage_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config = _base_config(tmp_path, best_effort_mode=True, strict_mode=False)
    _install_common_mocks(monkeypatch, tmp_path)
    monkeypatch.setattr(
        "trading_platform.orchestration.daily_trading.build_strategy_portfolio",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("portfolio boom")),
    )

    result = run_daily_trading_pipeline(config)

    assert result.status == "partial_failed"
    assert any(record.stage_name == "build_portfolio" and record.status == "failed" for record in result.stage_records)
