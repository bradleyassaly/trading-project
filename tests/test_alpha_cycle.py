from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import text

from trading_platform.config.loader import load_alpha_cycle_workflow_config
from trading_platform.db.session import create_session_factory
from trading_platform.db.settings import resolve_database_settings
from trading_platform.orchestration.alpha_cycle import run_alpha_cycle


def _write_normalized_frame(
    normalized_dir: Path,
    *,
    symbol: str,
    base_price: float,
    daily_return: float,
    periods: int = 90,
) -> None:
    closes = [base_price]
    for _ in range(periods - 1):
        closes.append(closes[-1] * (1.0 + daily_return))
    pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=periods, freq="D"),
            "symbol": [symbol] * periods,
            "open": [price * 0.995 for price in closes],
            "high": [price * 1.005 for price in closes],
            "low": [price * 0.99 for price in closes],
            "close": closes,
            "volume": [1_000_000 + idx * 1_000 for idx in range(periods)],
        }
    ).to_parquet(normalized_dir / f"{symbol}.parquet", index=False)


def _write_cycle_fixture(
    tmp_path: Path,
    *,
    promotion_metric_threshold: float = 0.1,
    best_effort_mode: bool = False,
    database_url: str | None = None,
) -> Path:
    normalized_dir = tmp_path / "data" / "normalized"
    feature_dir = tmp_path / "data" / "features"
    metadata_dir = tmp_path / "data" / "metadata"
    normalized_dir.mkdir(parents=True, exist_ok=True)

    for symbol, base_price, daily_return in (
        ("AAPL", 100.0, 0.010),
        ("MSFT", 200.0, 0.012),
        ("NVDA", 300.0, 0.018),
        ("AMD", 75.0, 0.016),
    ):
        _write_normalized_frame(
            normalized_dir,
            symbol=symbol,
            base_price=base_price,
            daily_return=daily_return,
        )

    refresh_config_path = tmp_path / "refresh.yaml"
    refresh_config_path.write_text(
        f"""
selection:
  symbols: [AAPL, MSFT, NVDA, AMD]
  sub_universe_id: alpha_cycle_fixture
outputs:
  feature_dir: {feature_dir.as_posix()}
  metadata_dir: {metadata_dir.as_posix()}
  normalized_dir: {normalized_dir.as_posix()}
failure_handling:
  policy: fail
""".strip(),
        encoding="utf-8",
    )

    research_config_path = tmp_path / "alpha_research.yaml"
    research_config_path.write_text(
        f"""
paths:
  feature_path: {feature_dir.as_posix()}
  output_dir: {(tmp_path / "unused_research").as_posix()}
selection:
  symbols: [AAPL, MSFT, NVDA, AMD]
signals:
  family: momentum
  lookbacks: [1, 2]
  horizons: [1]
  min_rows: 20
  equity_context_enabled: true
portfolio:
  top_quantile: 0.34
  bottom_quantile: 0.34
  train_size: 20
  test_size: 10
  step_size: 10
tracking:
  tracker_dir: {(tmp_path / "artifacts" / "tracking").as_posix()}
""".strip(),
        encoding="utf-8",
    )

    promotion_config_path = tmp_path / "promotion.yaml"
    promotion_config_path.write_text(
        f"""
schema_version: 1
metric_name: portfolio_sharpe
min_metric_threshold: {promotion_metric_threshold}
min_folds_tested: 1
min_promoted_signals: 1
require_validation_pass: false
allow_weak_validation: true
max_strategies_total: 2
max_strategies_per_group: 1
group_by: signal_family
default_status: inactive
""".strip(),
        encoding="utf-8",
    )

    strategy_portfolio_config_path = tmp_path / "strategy_portfolio.yaml"
    strategy_portfolio_config_path.write_text(
        """
schema_version: 1
max_strategies: 2
max_strategies_per_signal_family: 1
max_weight_per_strategy: 0.7
min_weight_per_strategy: 0.0
selection_metric: ranking_value
weighting_mode: equal
require_active_only: false
require_promotion_eligible_only: true
deduplicate_source_runs: true
diversification_dimension: signal_family
fallback_equal_weight_mode: true
warn_on_same_family_overlap: true
""".strip(),
        encoding="utf-8",
    )

    cycle_config_path = tmp_path / "alpha_cycle.yaml"
    cycle_config_path.write_text(
        f"""
stages:
  refresh: true
  research: true
  promotion: true
  portfolio: true
  export_bundle: true
  report: true
configs:
  refresh_config: {refresh_config_path.as_posix()}
  research_config: {research_config_path.as_posix()}
  promotion_policy_config: {promotion_config_path.as_posix()}
  strategy_portfolio_policy_config: {strategy_portfolio_config_path.as_posix()}
paths:
  output_root: {(tmp_path / "artifacts" / "alpha_cycle").as_posix()}
run:
  run_name: fixture_cycle
mode:
  strict_mode: {"false" if best_effort_mode else "true"}
  best_effort_mode: {"true" if best_effort_mode else "false"}
promotion:
  top_n: 2
  allow_overwrite: true
  inactive: false
  override_validation: false
tracking:
  database_enabled: {"true" if database_url else "false"}
  database_url: {database_url or ""}
  database_schema:
  write_candidates: true
  write_metrics: true
  write_promotions: true
""".strip(),
        encoding="utf-8",
    )
    return cycle_config_path


def test_run_alpha_cycle_executes_full_flow(tmp_path: Path) -> None:
    cycle_config_path = _write_cycle_fixture(tmp_path)

    result = run_alpha_cycle(load_alpha_cycle_workflow_config(cycle_config_path))
    summary_payload = json.loads(Path(result.summary_json_path).read_text(encoding="utf-8"))

    assert result.status == "succeeded"
    assert Path(result.summary_md_path).exists()
    assert {record.stage_name for record in result.stage_records} == {
        "refresh",
        "research",
        "promotion",
        "portfolio",
        "export_bundle",
        "report",
    }
    assert summary_payload["promoted_strategy_count"] >= 1
    assert summary_payload["selected_portfolio_strategy_count"] >= 1
    assert Path(summary_payload["key_artifacts"]["promoted_index_path"]).exists()
    assert Path(summary_payload["key_artifacts"]["strategy_portfolio_json_path"]).exists()
    assert Path(summary_payload["key_artifacts"]["multi_strategy_config_path"]).exists()


def test_run_alpha_cycle_reports_zero_promotion_without_crashing(tmp_path: Path) -> None:
    cycle_config_path = _write_cycle_fixture(tmp_path, promotion_metric_threshold=9999.0)

    result = run_alpha_cycle(load_alpha_cycle_workflow_config(cycle_config_path))
    summary_payload = json.loads(Path(result.summary_json_path).read_text(encoding="utf-8"))
    stage_statuses = {record.stage_name: record.status for record in result.stage_records}

    assert result.status == "succeeded"
    assert summary_payload["promoted_strategy_count"] == 0
    assert summary_payload["selected_portfolio_strategy_count"] == 0
    assert stage_statuses["promotion"] == "succeeded"
    assert stage_statuses["portfolio"] == "skipped"
    assert stage_statuses["export_bundle"] == "skipped"


def test_run_alpha_cycle_best_effort_emits_partial_failure_summary(monkeypatch, tmp_path: Path) -> None:
    cycle_config_path = _write_cycle_fixture(tmp_path, best_effort_mode=True)

    def boom(*args, **kwargs):
        raise RuntimeError("portfolio boom")

    monkeypatch.setattr("trading_platform.orchestration.alpha_cycle.build_strategy_portfolio", boom)

    result = run_alpha_cycle(load_alpha_cycle_workflow_config(cycle_config_path))
    summary_payload = json.loads(Path(result.summary_json_path).read_text(encoding="utf-8"))
    stage_statuses = {record.stage_name: record.status for record in result.stage_records}

    assert result.status == "partial_failed"
    assert summary_payload["status"] == "partial_failed"
    assert stage_statuses["portfolio"] == "failed"
    assert stage_statuses["export_bundle"] == "skipped"
    assert "portfolio: RuntimeError: portfolio boom" in summary_payload["errors"]


def test_alpha_cycle_summary_markdown_is_generated(tmp_path: Path) -> None:
    cycle_config_path = _write_cycle_fixture(tmp_path)

    result = run_alpha_cycle(load_alpha_cycle_workflow_config(cycle_config_path))
    summary_markdown = Path(result.summary_md_path).read_text(encoding="utf-8")

    assert "# Alpha Cycle Summary" in summary_markdown
    assert "| stage | status | duration_seconds | error |" in summary_markdown
    assert "promoted_strategy_count" in summary_markdown


def test_alpha_cycle_db_tracking_and_conditional_summary(monkeypatch, tmp_path: Path) -> None:
    db_url = f"sqlite:///{(tmp_path / 'alpha_cycle.db').as_posix()}"
    cycle_config_path = _write_cycle_fixture(tmp_path, database_url=db_url)

    def fake_apply_research_promotions(**kwargs):
        output_dir = Path(kwargs["output_dir"])
        output_dir.mkdir(parents=True, exist_ok=True)
        rows = [
            {
                "preset_name": "generated_base",
                "source_run_id": "research",
                "candidate_id": "momentum|1|1",
                "signal_family": "momentum",
                "strategy_name": "composite_alpha",
                "universe": "nasdaq100",
                "ranking_metric": "portfolio_sharpe",
                "ranking_value": 1.2,
                "promotion_timestamp": "2026-03-26T00:00:00+00:00",
                "status": "inactive",
                "promotion_variant": "unconditional",
                "activation_conditions": [],
                "rationale": "baseline promoted",
                "warnings": [],
                "generated_preset_path": str(output_dir / "generated_base.json"),
                "generated_registry_path": str(output_dir / "generated_base_registry.json"),
                "generated_pipeline_config_path": str(output_dir / "generated_base_pipeline.yaml"),
            },
            {
                "preset_name": "generated_conditional",
                "source_run_id": "research",
                "candidate_id": "momentum|1|1",
                "signal_family": "momentum",
                "strategy_name": "composite_alpha",
                "universe": "nasdaq100",
                "ranking_metric": "mean_spearman_ic",
                "ranking_value": 0.12,
                "promotion_timestamp": "2026-03-26T00:00:01+00:00",
                "status": "inactive",
                "promotion_variant": "conditional",
                "condition_id": "regime::risk_on",
                "condition_type": "regime",
                "activation_conditions": [{"condition_id": "regime::risk_on", "condition_type": "regime"}],
                "rationale": "conditional promoted",
                "warnings": [],
                "generated_preset_path": str(output_dir / "generated_conditional.json"),
                "generated_registry_path": str(output_dir / "generated_conditional_registry.json"),
                "generated_pipeline_config_path": str(output_dir / "generated_conditional_pipeline.yaml"),
            },
        ]
        (output_dir / "promoted_strategies.json").write_text(
            json.dumps({"strategies": rows, "summary": {"selected_count": 2}}, indent=2),
            encoding="utf-8",
        )
        for row in rows:
            Path(row["generated_preset_path"]).write_text("{}", encoding="utf-8")
            Path(row["generated_registry_path"]).write_text("{}", encoding="utf-8")
            Path(row["generated_pipeline_config_path"]).write_text("{}", encoding="utf-8")
        return {
            "selected_count": 2,
            "promoted_rows": rows,
            "promoted_index_path": str(output_dir / "promoted_strategies.json"),
            "promoted_condition_summary_path": str(output_dir / "promoted_condition_summary.csv"),
        }

    monkeypatch.setattr("trading_platform.orchestration.alpha_cycle.apply_research_promotions", fake_apply_research_promotions)

    result = run_alpha_cycle(load_alpha_cycle_workflow_config(cycle_config_path))
    summary_payload = json.loads(Path(result.summary_json_path).read_text(encoding="utf-8"))
    portfolio_payload = json.loads(
        Path(summary_payload["key_artifacts"]["strategy_portfolio_json_path"]).read_text(encoding="utf-8")
    )

    assert summary_payload["promoted_conditional_count"] == 1
    assert summary_payload["promoted_unconditional_count"] == 1
    assert "strategy_portfolio_condition_summary_path" in summary_payload["key_artifacts"]
    assert portfolio_payload["selected_strategies"][0]["promotion_variant"] in {"unconditional", "conditional"}

    settings = resolve_database_settings(enable_database_metadata=True, database_url=db_url)
    session_factory = create_session_factory(settings)
    assert session_factory is not None
    with session_factory() as session:
        assert session.execute(text("select count(*) from research_runs")).scalar() >= 1
        assert session.execute(text("select count(*) from promoted_strategies")).scalar() == 2
        assert session.execute(text("select count(*) from portfolio_runs")).scalar() >= 1
