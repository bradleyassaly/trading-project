import json
from pathlib import Path

import pandas as pd

from trading_platform.governance.lifecycle import (
    StrategyLifecycleState,
    StrategyLifecycleSummaryReport,
    apply_strategy_lifecycle_report_to_registry,
    build_strategy_lifecycle_summary_report,
    write_strategy_lifecycle_artifacts,
)
from trading_platform.governance.models import StrategyRegistry, StrategyRegistryEntry
from trading_platform.paper.models import PaperPortfolioState, PaperTradingRunResult
from trading_platform.reporting.strategy_decay import (
    StrategyDecayRecord,
    StrategyDecaySummaryReport,
    StrategyLifecycleRecommendation,
)
from trading_platform.risk.controls import PaperRiskControlReport, RiskControlAction, RiskControlTrigger


def _decay_record(
    *,
    strategy_id: str,
    severity: str = "healthy",
    recommended_action: str = "monitor",
    trade_count: int = 6,
    sufficient_samples: bool = True,
    forecast_gap: float = 0.0,
    calibration_error: float = 0.02,
    drift_warning_count: int = 0,
    risk_trigger_count: int = 0,
    risk_halted_or_restricted_count: int = 0,
    decay_score: float | None = 0.1,
) -> StrategyDecayRecord:
    return StrategyDecayRecord(
        as_of="2026-04-01T00:00:00Z",
        strategy_id=strategy_id,
        evaluation_window_start="2026-03-01",
        evaluation_window_end="2026-04-01",
        trade_count=trade_count,
        sufficient_samples=sufficient_samples,
        mean_predicted_net_return=0.03,
        mean_realized_net_return=0.02,
        mean_forecast_gap=forecast_gap,
        mean_cost_error=0.0,
        mean_execution_error=0.0,
        drift_signal_count=drift_warning_count,
        drift_warning_or_worse_count=drift_warning_count,
        calibration_confidence_error=0.01,
        calibration_expected_value_error=calibration_error,
        risk_trigger_count=risk_trigger_count,
        risk_halted_or_restricted_count=risk_halted_or_restricted_count,
        realized_drawdown_proxy=0.01,
        decay_score=decay_score,
        severity=severity,
        recommended_action=recommended_action,
        metadata={},
    )


def _recommendation(
    *,
    strategy_id: str,
    severity: str,
    recommended_action: str,
    sufficient_samples: bool = True,
    decay_score: float | None = None,
) -> StrategyLifecycleRecommendation:
    return StrategyLifecycleRecommendation(
        as_of="2026-04-01T00:00:00Z",
        strategy_id=strategy_id,
        severity=severity,
        recommended_action=recommended_action,
        decay_score=decay_score,
        sufficient_samples=sufficient_samples,
        rationale=[],
        metadata={},
    )


def _result(
    *,
    decay_report: StrategyDecaySummaryReport,
    risk_triggers: list[RiskControlTrigger] | None = None,
    risk_actions: list[RiskControlAction] | None = None,
) -> PaperTradingRunResult:
    return PaperTradingRunResult(
        as_of="2026-04-01T00:00:00Z",
        state=PaperPortfolioState(cash=10_000.0),
        latest_prices={},
        latest_scores={},
        latest_target_weights={},
        scheduled_target_weights={},
        orders=[],
        attribution={"trade_rows": [], "strategy_rows": [], "symbol_rows": [], "summary": {}},
        strategy_decay_report=decay_report,
        risk_control_report=PaperRiskControlReport(
            as_of="2026-04-01T00:00:00Z",
            enabled=True,
            operating_state="healthy",
            triggers=list(risk_triggers or []),
            actions=list(risk_actions or []),
            events=[],
            summary={},
        ),
    )


def _registry_entry(tmp_path: Path, *, strategy_id: str, status: str, stage: str) -> StrategyRegistryEntry:
    research_dir = tmp_path / strategy_id / "research"
    paper_dir = tmp_path / strategy_id / "paper"
    live_dir = tmp_path / strategy_id / "live"
    research_dir.mkdir(parents=True, exist_ok=True)
    paper_dir.mkdir(parents=True, exist_ok=True)
    live_dir.mkdir(parents=True, exist_ok=True)
    return StrategyRegistryEntry(
        strategy_id=strategy_id,
        strategy_name=f"Strategy {strategy_id}",
        family="momentum",
        version="v1",
        preset_name="xsec_nasdaq100_momentum_v1_deploy",
        research_artifact_paths=[str(research_dir)],
        created_at="2026-03-01T00:00:00Z",
        status=status,
        owner="qa",
        source="unit_test",
        current_deployment_stage=stage,
        paper_artifact_path=str(paper_dir),
        live_artifact_path=str(live_dir),
        metadata={},
    )


def test_strategy_lifecycle_builds_watch_and_constrain_actions() -> None:
    report = build_strategy_lifecycle_summary_report(
        result=_result(
            decay_report=StrategyDecaySummaryReport(
                as_of="2026-04-01T00:00:00Z",
                records=[
                    _decay_record(strategy_id="watcher", severity="watch", recommended_action="review", decay_score=0.35),
                    _decay_record(
                        strategy_id="strained",
                        severity="warning",
                        recommended_action="constrain",
                        decay_score=0.60,
                    ),
                ],
                recommendations=[
                    _recommendation(strategy_id="watcher", severity="watch", recommended_action="review", decay_score=0.35),
                    _recommendation(
                        strategy_id="strained",
                        severity="warning",
                        recommended_action="constrain",
                        decay_score=0.60,
                    ),
                ],
                summary={},
            ),
            risk_triggers=[
                RiskControlTrigger(
                    as_of="2026-04-01T00:00:00Z",
                    scope="strategy",
                    scope_id="strained",
                    trigger_type="expected_realized_divergence",
                    severity="warning",
                    threshold=0.05,
                    observed_value=0.08,
                    operating_state="restricted",
                    message="risk",
                )
            ],
        )
    )

    assert StrategyLifecycleSummaryReport.from_dict(report.to_dict()) == report
    state_by_strategy = {row.strategy_id: row.state for row in report.states}
    action_by_strategy = {row.strategy_id: row.action_type for row in report.actions}
    assert state_by_strategy["watcher"] == "watch"
    assert action_by_strategy["watcher"] == "watch"
    assert state_by_strategy["strained"] == "constrained"
    assert action_by_strategy["strained"] == "constrain"


def test_strategy_lifecycle_handles_insufficient_data_without_action() -> None:
    report = build_strategy_lifecycle_summary_report(
        result=_result(
            decay_report=StrategyDecaySummaryReport(
                as_of="2026-04-01T00:00:00Z",
                records=[
                    _decay_record(
                        strategy_id="newbie",
                        trade_count=2,
                        sufficient_samples=False,
                        severity="healthy",
                        recommended_action="monitor",
                        decay_score=None,
                    )
                ],
                recommendations=[
                    _recommendation(
                        strategy_id="newbie",
                        severity="healthy",
                        recommended_action="monitor",
                        sufficient_samples=False,
                        decay_score=None,
                    )
                ],
                summary={},
            )
        )
    )

    assert report.actions[0].status == "no_action"
    assert report.states[0].state == "active"
    assert report.summary["demotion_count"] == 0


def test_strategy_lifecycle_creates_demotion_and_retraining_trigger() -> None:
    report = build_strategy_lifecycle_summary_report(
        result=_result(
            decay_report=StrategyDecaySummaryReport(
                as_of="2026-04-01T00:00:00Z",
                records=[
                    _decay_record(
                        strategy_id="broken",
                        severity="critical",
                        recommended_action="demote_candidate",
                        forecast_gap=-0.12,
                        calibration_error=0.08,
                        drift_warning_count=3,
                        risk_trigger_count=1,
                        risk_halted_or_restricted_count=1,
                        decay_score=0.90,
                    )
                ],
                recommendations=[
                    _recommendation(
                        strategy_id="broken",
                        severity="critical",
                        recommended_action="demote_candidate",
                        decay_score=0.90,
                    )
                ],
                summary={},
            ),
            risk_triggers=[
                RiskControlTrigger(
                    as_of="2026-04-01T00:00:00Z",
                    scope="strategy",
                    scope_id="broken",
                    trigger_type="expected_realized_divergence",
                    severity="critical",
                    threshold=0.05,
                    observed_value=0.12,
                    operating_state="halted",
                    message="risk",
                )
            ],
        )
    )

    assert report.states[0].state == "demoted"
    assert report.demotion_decisions[0].approved_for_demotion is True
    assert len(report.retraining_triggers) == 1
    assert report.retraining_triggers[0].governance_required_before_reactivation is True
    assert report.summary["demotion_count"] == 1


def test_strategy_lifecycle_cooldown_suppresses_duplicate_action() -> None:
    prior_state = StrategyLifecycleState(
        as_of="2026-03-30T00:00:00Z",
        strategy_id="strained",
        state="constrained",
        active_for_selection=True,
        constrained=True,
        monitoring_level="elevated",
        retraining_requested=False,
        last_action="constrain",
        last_action_at="2026-03-30T00:00:00Z",
    )
    report = build_strategy_lifecycle_summary_report(
        result=_result(
            decay_report=StrategyDecaySummaryReport(
                as_of="2026-04-01T00:00:00Z",
                records=[_decay_record(strategy_id="strained", severity="warning", recommended_action="constrain", decay_score=0.60)],
                recommendations=[_recommendation(strategy_id="strained", severity="warning", recommended_action="constrain", decay_score=0.60)],
                summary={},
            )
        ),
        prior_states=[prior_state],
        cooldown_days=7,
    )

    assert report.actions[0].status == "suppressed_cooldown"
    assert report.states[0].state == "constrained"
    assert report.summary["suppressed_action_count"] == 1


def test_strategy_lifecycle_artifacts_are_deterministic(tmp_path: Path) -> None:
    report = build_strategy_lifecycle_summary_report(
        result=_result(
            decay_report=StrategyDecaySummaryReport(
                as_of="2026-04-01T00:00:00Z",
                records=[_decay_record(strategy_id="watcher", severity="watch", recommended_action="review", decay_score=0.35)],
                recommendations=[_recommendation(strategy_id="watcher", severity="watch", recommended_action="review", decay_score=0.35)],
                summary={},
            )
        )
    )
    paths = write_strategy_lifecycle_artifacts(output_dir=tmp_path, report=report)

    payload = json.loads(paths["strategy_lifecycle_report_json_path"].read_text(encoding="utf-8"))
    actions_df = pd.read_csv(paths["strategy_lifecycle_actions_csv_path"])
    states_df = pd.read_csv(paths["strategy_lifecycle_states_csv_path"])

    assert payload["summary"]["strategy_count"] == 1
    assert "action_type" in set(actions_df.columns)
    assert "state" in set(states_df.columns)


def test_strategy_lifecycle_registry_application_preserves_governance_for_retraining(tmp_path: Path) -> None:
    entry = _registry_entry(tmp_path, strategy_id="broken", status="approved", stage="approved")
    registry = StrategyRegistry(entries=[entry], updated_at="2026-03-31T00:00:00Z")
    report = build_strategy_lifecycle_summary_report(
        result=_result(
            decay_report=StrategyDecaySummaryReport(
                as_of="2026-04-01T00:00:00Z",
                records=[
                    _decay_record(
                        strategy_id="broken",
                        severity="critical",
                        recommended_action="demote_candidate",
                        forecast_gap=-0.12,
                        calibration_error=0.08,
                        drift_warning_count=3,
                        risk_trigger_count=1,
                        risk_halted_or_restricted_count=1,
                        decay_score=0.90,
                    )
                ],
                recommendations=[
                    _recommendation(
                        strategy_id="broken",
                        severity="critical",
                        recommended_action="demote_candidate",
                        decay_score=0.90,
                    )
                ],
                summary={},
            ),
            risk_triggers=[
                RiskControlTrigger(
                    as_of="2026-04-01T00:00:00Z",
                    scope="strategy",
                    scope_id="broken",
                    trigger_type="expected_realized_divergence",
                    severity="critical",
                    threshold=0.05,
                    observed_value=0.12,
                    operating_state="halted",
                    message="risk",
                )
            ],
        )
    )

    updated_registry = apply_strategy_lifecycle_report_to_registry(registry=registry, report=report)
    updated_entry = updated_registry.entries[0]

    assert updated_entry.status == "paper"
    assert updated_entry.metadata["retraining_requested"] is True
    assert updated_entry.metadata["retraining_governance_required_before_reactivation"] is True
    assert updated_entry.metadata["retraining_target_candidate_status"] == "candidate"
    assert updated_registry.audit_log[-1].action == "retrain_requested"
