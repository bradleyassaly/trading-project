from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from trading_platform.governance.models import (
    DegradationCriteria,
    PromotionCriteria,
    RegistrySelectionOptions,
    StrategyRegistry,
    StrategyRegistryAuditEvent,
    StrategyRegistryEntry,
)
from trading_platform.governance.persistence import (
    load_strategy_registry,
    save_strategy_registry,
    validate_status_transition,
)
from trading_platform.governance.service import (
    build_family_comparison,
    build_multi_strategy_config_from_registry,
    demote_registry_entry,
    evaluate_degradation,
    evaluate_promotion,
    promote_registry_entry,
    write_decision_artifacts,
    write_registry_backed_multi_strategy_artifacts,
)


def _write_research_artifacts(
    base_dir: Path,
    *,
    total_return: float = 0.15,
    sharpe: float = 1.2,
    max_drawdown: float = 0.10,
    hit_rate: float = 0.6,
    folds: int = 4,
    turnover: float = 0.2,
    rank_ic: float = 0.05,
    redundancy: float = 0.2,
    underperformance: float = 0.01,
    stability: float = 0.9,
) -> Path:
    base_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "portfolio_total_return": total_return,
                "portfolio_sharpe": sharpe,
                "portfolio_max_drawdown": max_drawdown,
                "hit_rate": hit_rate,
            }
        ]
    ).to_csv(base_dir / "portfolio_metrics.csv", index=False)
    pd.DataFrame(
        [
            {
                "folds_tested": folds,
                "mean_fold_return": total_return,
                "mean_fold_sharpe": sharpe,
                "mean_turnover": turnover,
            }
        ]
    ).to_csv(base_dir / "robustness_report.csv", index=False)
    pd.DataFrame([{"return_drag": 0.0, "mean_turnover": turnover}]).to_csv(
        base_dir / "implementability_report.csv", index=False
    )
    pd.DataFrame([{"mean_spearman_ic": rank_ic}]).to_csv(
        base_dir / "leaderboard.csv", index=False
    )
    pd.DataFrame([{"score_corr": redundancy}]).to_csv(
        base_dir / "redundancy_report.csv", index=False
    )
    (base_dir / "signal_diagnostics.json").write_text(
        json.dumps(
            {
                "rolling_underperformance_vs_benchmark": underperformance,
                "signal_stability": stability,
            }
        ),
        encoding="utf-8",
    )
    return base_dir


def _write_paper_artifacts(
    base_dir: Path,
    *,
    turnover: float = 0.1,
    missing_data_failures: int = 0,
    days: int = 5,
    trade_count: int = 5,
) -> Path:
    base_dir.mkdir(parents=True, exist_ok=True)
    health_checks = [
        {"status": "fail", "check_name": "data_loaded", "message": "missing"}
        for _ in range(missing_data_failures)
    ]
    (base_dir / "paper_run_summary_latest.json").write_text(
        json.dumps(
            {
                "summary": {
                    "turnover_estimate": turnover,
                    "current_equity": 100000.0,
                },
                "health_checks": health_checks,
            }
        ),
        encoding="utf-8",
    )
    timestamps = [f"2025-01-0{min(index + 1, 9)}" for index in range(trade_count)]
    pd.DataFrame(
        [
            {"timestamp": timestamps[index % len(timestamps)], "symbol": f"S{index}"}
            for index in range(trade_count)
        ]
    ).to_csv(base_dir / "paper_orders_history.csv", index=False)
    if days > 0:
        pd.DataFrame(
            [{"timestamp": f"2025-01-{index + 1:02d}", "symbol": "AAPL"} for index in range(days)]
        ).to_csv(base_dir / "paper_orders_history.csv", index=False)
    return base_dir


def _write_live_artifacts(
    base_dir: Path,
    *,
    fail_checks: int = 0,
    warn_checks: int = 0,
) -> Path:
    base_dir.mkdir(parents=True, exist_ok=True)
    health_checks = (
        [{"status": "fail", "check_name": "market_data", "message": "bad"} for _ in range(fail_checks)]
        + [{"status": "warn", "check_name": "cash_residual", "message": "warn"} for _ in range(warn_checks)]
    )
    (base_dir / "live_run_summary_latest.json").write_text(
        json.dumps(
            {
                "summary": {"readiness": "blocked" if fail_checks else "ready_for_manual_review"},
                "health_checks": health_checks,
            }
        ),
        encoding="utf-8",
    )
    return base_dir


def _entry(
    tmp_path: Path,
    *,
    strategy_id: str = "strat-1",
    family: str = "momentum",
    version: str = "v1",
    status: str = "candidate",
    stage: str | None = None,
    preset_name: str = "xsec_nasdaq100_momentum_v1_deploy",
    total_return: float = 0.15,
    sharpe: float = 1.2,
    max_drawdown: float = 0.10,
) -> StrategyRegistryEntry:
    research_dir = _write_research_artifacts(
        tmp_path / strategy_id / "research",
        total_return=total_return,
        sharpe=sharpe,
        max_drawdown=max_drawdown,
    )
    paper_dir = _write_paper_artifacts(tmp_path / strategy_id / "paper")
    live_dir = _write_live_artifacts(tmp_path / strategy_id / "live")
    return StrategyRegistryEntry(
        strategy_id=strategy_id,
        strategy_name=f"Strategy {strategy_id}",
        family=family,
        version=version,
        preset_name=preset_name,
        research_artifact_paths=[str(research_dir)],
        created_at="2025-01-01T00:00:00Z",
        status=status,
        owner="qa",
        source="unit_test",
        current_deployment_stage=stage or status if status in {"research", "paper", "approved", "live_disabled", "retired"} else "research",
        notes="note",
        tags=["core"],
        universe="nasdaq100",
        signal_type="momentum",
        rebalance_frequency="monthly",
        benchmark="equal_weight",
        risk_profile="medium",
        paper_artifact_path=str(paper_dir),
        live_artifact_path=str(live_dir),
    )


def test_strategy_registry_load_save_round_trip(tmp_path: Path) -> None:
    registry_path = tmp_path / "registry.json"
    registry = StrategyRegistry(
        updated_at="2025-01-01T00:00:00Z",
        entries=[_entry(tmp_path)],
        audit_log=[
            StrategyRegistryAuditEvent(
                timestamp="2025-01-01T00:00:00Z",
                strategy_id="strat-1",
                action="seed",
            )
        ],
    )

    save_strategy_registry(registry, registry_path)
    loaded = load_strategy_registry(registry_path)

    assert loaded.to_dict() == registry.to_dict()


def test_strategy_registry_rejects_invalid_record(tmp_path: Path) -> None:
    registry_path = tmp_path / "registry.json"
    registry_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "updated_at": "2025-01-01T00:00:00Z",
                "entries": [
                    {
                        "strategy_id": "",
                        "strategy_name": "Bad",
                        "family": "momentum",
                        "version": "v1",
                        "preset_name": "xsec_nasdaq100_momentum_v1_deploy",
                        "research_artifact_paths": ["missing"],
                        "created_at": "2025-01-01T00:00:00Z",
                        "status": "candidate",
                        "owner": "qa",
                        "source": "test",
                        "current_deployment_stage": "research",
                    }
                ],
                "audit_log": [],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="strategy_id"):
        load_strategy_registry(registry_path)


def test_evaluate_promotion_pass_case(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr("trading_platform.governance.service._now_utc", lambda: "2025-01-02T00:00:00Z")
    entry = _entry(tmp_path)

    report, snapshot = evaluate_promotion(
        entry=entry,
        criteria=PromotionCriteria(
            minimum_walk_forward_folds=2,
            minimum_mean_test_return=0.1,
            minimum_sharpe=1.0,
            maximum_drawdown=0.2,
            minimum_hit_rate=0.5,
            minimum_ic_rank_ic=0.01,
            maximum_turnover=0.5,
            maximum_redundancy_correlation=0.5,
            minimum_paper_trading_observation_window=3,
            minimum_trade_count=3,
        ),
    )

    assert report.passed is True
    assert report.failed_criteria == []
    assert snapshot.metrics["sharpe"] == pytest.approx(1.2)


def test_evaluate_promotion_fail_case_with_missing_artifacts(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr("trading_platform.governance.service._now_utc", lambda: "2025-01-02T00:00:00Z")
    entry = StrategyRegistryEntry(
        strategy_id="missing",
        strategy_name="Missing",
        family="momentum",
        version="v1",
        preset_name="xsec_nasdaq100_momentum_v1_deploy",
        research_artifact_paths=[str(tmp_path / "missing")],
        created_at="2025-01-01T00:00:00Z",
        status="candidate",
        owner="qa",
        source="test",
        current_deployment_stage="research",
    )

    report, _snapshot = evaluate_promotion(
        entry=entry,
        criteria=PromotionCriteria(minimum_walk_forward_folds=1, minimum_sharpe=0.5),
    )

    assert report.passed is False
    assert "minimum_walk_forward_folds" in report.failed_criteria
    assert "minimum_sharpe" in report.failed_criteria


def test_evaluate_degradation_pass_and_fail_cases(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr("trading_platform.governance.service._now_utc", lambda: "2025-01-02T00:00:00Z")
    passing_entry = _entry(tmp_path / "pass", status="approved", stage="approved")
    passing_report, _ = evaluate_degradation(
        entry=passing_entry,
        criteria=DegradationCriteria(
            maximum_rolling_underperformance_vs_benchmark=0.05,
            maximum_drawdown=0.2,
            maximum_turnover=0.2,
            minimum_signal_stability=0.5,
            maximum_missing_data_failures=0,
            maximum_live_fail_checks=0,
            maximum_live_warn_checks=1,
        ),
    )
    assert passing_report.passed is True

    failing_entry = _entry(
        tmp_path / "fail",
        strategy_id="strat-fail",
        status="approved",
        stage="approved",
        total_return=0.05,
        sharpe=0.2,
        max_drawdown=0.3,
    )
    _write_research_artifacts(
        Path(failing_entry.research_artifact_paths[0]),
        total_return=0.05,
        sharpe=0.2,
        max_drawdown=0.3,
        underperformance=0.2,
        stability=0.1,
    )
    _write_paper_artifacts(Path(failing_entry.paper_artifact_path), turnover=0.6, missing_data_failures=2)
    _write_live_artifacts(Path(failing_entry.live_artifact_path), fail_checks=1, warn_checks=3)

    failing_report, _ = evaluate_degradation(
        entry=failing_entry,
        criteria=DegradationCriteria(
            maximum_rolling_underperformance_vs_benchmark=0.05,
            maximum_drawdown=0.2,
            maximum_turnover=0.2,
            minimum_signal_stability=0.5,
            maximum_missing_data_failures=0,
            maximum_live_fail_checks=0,
            maximum_live_warn_checks=1,
        ),
    )
    assert failing_report.passed is False
    assert "maximum_drawdown" in failing_report.failed_criteria
    assert "maximum_live_fail_checks" in failing_report.failed_criteria


def test_build_family_comparison_identifies_champion_and_challenger(tmp_path: Path) -> None:
    champion = _entry(tmp_path, strategy_id="champion", status="approved", stage="approved", sharpe=1.0)
    challenger = _entry(tmp_path, strategy_id="challenger", status="paper", stage="paper", version="v2", sharpe=1.5)
    registry = StrategyRegistry(entries=[champion, challenger], updated_at="2025-01-01T00:00:00Z")

    rows, champions = build_family_comparison(registry)

    assert champions["momentum"].strategy_id == "champion"
    challenger_row = next(row for row in rows if row["strategy_id"] == "challenger")
    assert challenger_row["recommendation"] == "replace"


def test_build_multi_strategy_config_from_registry_approved_strategies(tmp_path: Path) -> None:
    approved = _entry(tmp_path, strategy_id="approved-a", status="approved", stage="approved")
    paper = _entry(tmp_path, strategy_id="paper-a", status="paper", stage="paper")
    registry = StrategyRegistry(entries=[approved, paper], updated_at="2025-01-01T00:00:00Z")

    config, family_rows = build_multi_strategy_config_from_registry(
        registry=registry,
        options=RegistrySelectionOptions(include_statuses=["approved"]),
    )

    assert len(config.sleeves) == 1
    assert config.sleeves[0].sleeve_name == "approved-a"
    assert family_rows


def test_build_multi_strategy_config_score_weighted_includes_paper(tmp_path: Path) -> None:
    approved = _entry(tmp_path, strategy_id="approved-a", status="approved", stage="approved", sharpe=1.0)
    paper = _entry(tmp_path, strategy_id="paper-a", status="paper", stage="paper", sharpe=2.0)
    registry = StrategyRegistry(entries=[approved, paper], updated_at="2025-01-01T00:00:00Z")

    config, _ = build_multi_strategy_config_from_registry(
        registry=registry,
        options=RegistrySelectionOptions(
            include_statuses=["approved", "paper"],
            weighting_scheme="score_weighted",
        ),
    )

    sleeves = {sleeve.sleeve_name: sleeve.target_capital_weight for sleeve in config.sleeves}
    assert sleeves["paper-a"] > sleeves["approved-a"]


def test_status_transition_validation_and_mutations(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr("trading_platform.governance.service._now_utc", lambda: "2025-01-02T00:00:00Z")
    monkeypatch.setattr("trading_platform.governance.persistence._now_utc", lambda: "2025-01-02T00:00:00Z")
    entry = _entry(tmp_path, status="candidate", stage="research")
    registry = StrategyRegistry(entries=[entry], updated_at="2025-01-01T00:00:00Z")

    promoted = promote_registry_entry(registry=registry, strategy_id="strat-1")
    assert promoted.entries[0].status == "paper"
    assert promoted.audit_log[-1].action == "promote"

    demoted = demote_registry_entry(registry=promoted, strategy_id="strat-1")
    assert demoted.entries[0].status == "candidate"
    assert demoted.audit_log[-1].action == "demote"

    with pytest.raises(ValueError, match="Invalid status transition"):
        validate_status_transition(from_status="research", to_status="approved")


def test_write_decision_and_registry_backed_artifacts_are_deterministic(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr("trading_platform.governance.service._now_utc", lambda: "2025-01-02T00:00:00Z")
    entry = _entry(tmp_path)
    report, snapshot = evaluate_promotion(
        entry=entry,
        criteria=PromotionCriteria(minimum_walk_forward_folds=1),
    )
    first_paths = write_decision_artifacts(
        report=report,
        snapshot=snapshot,
        output_dir=tmp_path / "first",
        prefix="promotion_decision",
    )
    second_paths = write_decision_artifacts(
        report=report,
        snapshot=snapshot,
        output_dir=tmp_path / "second",
        prefix="promotion_decision",
    )

    assert Path(first_paths["promotion_decision_json_path"]).read_text(encoding="utf-8") == Path(
        second_paths["promotion_decision_json_path"]
    ).read_text(encoding="utf-8")

    registry = StrategyRegistry(entries=[entry], updated_at="2025-01-01T00:00:00Z")
    config, family_rows = build_multi_strategy_config_from_registry(
        registry=registry,
        options=RegistrySelectionOptions(include_statuses=["candidate"]),
    )
    first_registry_paths = write_registry_backed_multi_strategy_artifacts(
        config=config,
        family_rows=family_rows,
        output_path=tmp_path / "first_registry" / "multi_strategy.json",
    )
    second_registry_paths = write_registry_backed_multi_strategy_artifacts(
        config=config,
        family_rows=family_rows,
        output_path=tmp_path / "second_registry" / "multi_strategy.json",
    )
    assert Path(first_registry_paths["multi_strategy_config_path"]).read_text(encoding="utf-8") == Path(
        second_registry_paths["multi_strategy_config_path"]
    ).read_text(encoding="utf-8")
