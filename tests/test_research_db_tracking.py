from __future__ import annotations

from argparse import Namespace
import json
from pathlib import Path

import pandas as pd
from sqlalchemy import inspect, text

from trading_platform.cli.commands.alpha_research import cmd_alpha_research
from trading_platform.cli.commands.research_db import cmd_research_db_family_summary, cmd_research_db_init
from trading_platform.cli.commands.research_promote import cmd_research_promote
from trading_platform.db import Base
from trading_platform.db.session import create_engine_from_settings, create_session_factory
from trading_platform.db.settings import resolve_database_settings
from trading_platform.db.services import DatabaseLineageService, build_research_memory_service


def _sqlite_url(tmp_path: Path) -> str:
    return f"sqlite:///{(tmp_path / 'research_memory.db').as_posix()}"


def test_research_db_init_creates_tables_and_disabled_mode_noops(tmp_path: Path, capsys) -> None:
    disabled_args = Namespace(
        enable_database_metadata=False,
        database_url=None,
        database_schema=None,
        tracking_write_candidates=True,
        tracking_write_metrics=True,
        tracking_write_promotions=True,
    )
    cmd_research_db_init(disabled_args)
    assert "disabled" in capsys.readouterr().out.lower()

    args = Namespace(
        enable_database_metadata=True,
        database_url=_sqlite_url(tmp_path),
        database_schema=None,
        tracking_write_candidates=True,
        tracking_write_metrics=True,
        tracking_write_promotions=True,
    )
    cmd_research_db_init(args)
    settings = resolve_database_settings(enable_database_metadata=True, database_url=_sqlite_url(tmp_path))
    engine = create_engine_from_settings(settings)
    assert engine is not None
    tables = set(inspect(engine).get_table_names())
    assert "research_runs" in tables
    assert "signal_candidates" in tables
    assert "signal_metrics" in tables
    assert "promoted_strategies" in tables


def test_cmd_alpha_research_persists_research_memory_rows(monkeypatch, tmp_path: Path) -> None:
    leaderboard_path = tmp_path / "leaderboard.csv"
    pd.DataFrame(
        [
            {
                "candidate_id": "momentum_lb20_hz5_base",
                "candidate_name": "momentum_lb20_hz5",
                "signal_family": "momentum",
                "signal_variant": "base",
                "variant_parameters_json": json.dumps({"mode": "baseline"}),
                "lookback": 20,
                "horizon": 5,
                "folds_tested": 4,
                "symbols_tested": 12,
                "mean_dates_evaluated": 55,
                "mean_pearson_ic": 0.01,
                "mean_spearman_ic": 0.02,
                "mean_hit_rate": 0.53,
                "mean_long_short_spread": 0.03,
                "mean_quantile_spread": 0.04,
                "mean_turnover": 0.12,
                "worst_fold_spearman_ic": -0.01,
                "total_obs": 1000,
                "rejection_reason": "",
                "promotion_status": "promote",
            }
        ]
    ).to_csv(leaderboard_path, index=False)

    monkeypatch.setattr("trading_platform.cli.commands.alpha_research.resolve_symbols", lambda args: ["AAPL"])
    monkeypatch.setattr(
        "trading_platform.cli.commands.alpha_research.run_alpha_research",
        lambda **kwargs: {
            "leaderboard_path": str(leaderboard_path),
            "fold_results_path": str(tmp_path / "fold_results.csv"),
            "portfolio_returns_path": str(tmp_path / "portfolio_returns.csv"),
            "ensemble_member_summary_path": str(tmp_path / "ensemble_member_summary.csv"),
            "implementability_report_path": str(tmp_path / "implementability_report.csv"),
            "signal_performance_by_sub_universe_path": str(tmp_path / "signal_performance_by_sub_universe.csv"),
            "signal_performance_by_benchmark_context_path": str(tmp_path / "signal_performance_by_benchmark_context.csv"),
            "research_manifest_path": str(tmp_path / "research_manifest.json"),
        },
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.alpha_research.build_alpha_experiment_record",
        lambda output_dir: {"output_dir": str(output_dir)},
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.alpha_research.register_experiment",
        lambda record, tracker_dir: {"experiment_registry_path": tracker_dir / "registry.csv"},
    )

    args = Namespace(
        config=None,
        symbols=["AAPL"],
        universe=None,
        feature_dir=str(tmp_path / "features"),
        signal_family="momentum",
        signal_families=None,
        candidate_grid_preset="standard",
        signal_composition_preset="standard",
        max_variants_per_family=None,
        lookbacks=[20],
        horizons=[5],
        min_rows=126,
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
        top_quantile=0.2,
        bottom_quantile=0.2,
        output_dir=str(tmp_path / "alpha_run"),
        train_size=756,
        test_size=63,
        step_size=None,
        min_train_size=None,
        portfolio_top_n=10,
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
        slippage_bps_per_adv=10.0,
        dynamic_recent_quality_window=20,
        dynamic_min_history=5,
        dynamic_downweight_mean_rank_ic=0.01,
        dynamic_deactivate_mean_rank_ic=-0.02,
        regime_aware_enabled=False,
        regime_min_history=5,
        regime_underweight_mean_rank_ic=0.01,
        regime_exclude_mean_rank_ic=-0.01,
        experiment_tracker_dir=str(tmp_path / "tracking"),
        enable_database_metadata=True,
        database_url=_sqlite_url(tmp_path),
        database_schema=None,
        tracking_write_candidates=True,
        tracking_write_metrics=True,
        tracking_write_promotions=True,
        _cli_argv=["--symbols", "AAPL", "--database-enabled", "--database-url", _sqlite_url(tmp_path)],
    )

    cmd_alpha_research(args)

    settings = resolve_database_settings(enable_database_metadata=True, database_url=_sqlite_url(tmp_path))
    session_factory = create_session_factory(settings)
    assert session_factory is not None
    with session_factory() as session:
        assert session.execute(text("select count(*) from research_runs")).scalar() == 1
        assert session.execute(text("select count(*) from signal_candidates")).scalar() == 1
        assert session.execute(text("select count(*) from signal_metrics")).scalar() == 1
        run_row = session.execute(text("select output_dir, status from research_runs")).first()
        assert run_row is not None
        assert str(tmp_path / "alpha_run") in str(run_row.output_dir)
        assert run_row.status == "completed"


def test_cmd_research_promote_persists_promotions(monkeypatch, tmp_path: Path) -> None:
    settings = resolve_database_settings(enable_database_metadata=True, database_url=_sqlite_url(tmp_path))
    engine = create_engine_from_settings(settings)
    assert engine is not None
    Base.metadata.create_all(engine)
    lineage = DatabaseLineageService(create_session_factory(settings))
    run_id = lineage.create_research_run(run_key="run_current", run_type="alpha_research", config_payload={"x": 1})
    lineage.complete_research_run(run_id)

    monkeypatch.setattr(
        "trading_platform.cli.commands.research_promote._resolve_promotion_registry_scope",
        lambda args: (tmp_path, tmp_path / "research_registry", {"registry_json_path": tmp_path / "research_registry.json", "promotion_candidates_json_path": tmp_path / "promotion_candidates.json"}),
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.research_promote.apply_research_promotions",
        lambda **kwargs: {
            "selected_count": 1,
            "dry_run": False,
            "promoted_rows": [
                {
                    "preset_name": "generated_momentum_run_current_paper",
                    "source_run_id": "run_current",
                    "signal_family": "momentum",
                    "strategy_name": "composite_alpha",
                    "ranking_metric": "mean_spearman_ic",
                    "ranking_value": 0.04,
                    "validation_status": "passed",
                    "promotion_timestamp": "2026-03-26T00:00:00+00:00",
                    "status": "inactive",
                    "promotion_variant": "conditional",
                    "condition_id": "regime::risk_on",
                    "condition_type": "regime",
                    "rationale": "conditional regime edge",
                    "runtime_score_validation_pass": False,
                    "runtime_score_validation_reason": "empty_signal_scores",
                    "runtime_computable_symbol_count": 0,
                    "generated_preset_path": str(tmp_path / "preset.json"),
                    "generated_registry_path": str(tmp_path / "registry.json"),
                    "generated_pipeline_config_path": str(tmp_path / "pipeline.yaml"),
                }
            ],
            "promoted_index_path": str(tmp_path / "promoted_strategies.json"),
        },
    )

    args = Namespace(
        artifacts_root=str(tmp_path),
        run_dir=None,
        registry_scope="run_local",
        use_global_registry=False,
        registry_dir=None,
        output_dir=str(tmp_path / "promoted"),
        policy_config=None,
        validation=None,
        top_n=None,
        allow_overwrite=False,
        dry_run=False,
        inactive=False,
        override_validation=False,
        enable_database_metadata=True,
        database_url=_sqlite_url(tmp_path),
        database_schema=None,
        tracking_write_promotions=True,
    )
    cmd_research_promote(args)

    session_factory = create_session_factory(settings)
    assert session_factory is not None
    with session_factory() as session:
        assert session.execute(text("select count(*) from promoted_strategies")).scalar() >= 1
        row = session.execute(
            text(
                "select preset_name, signal_family, promotion_variant, condition_id, condition_type, rationale, "
                "runtime_score_validation_pass, runtime_score_validation_reason, runtime_computable_symbol_count "
                "from promoted_strategies where preset_name is not null"
            )
        ).first()
        assert row is not None
        assert row.preset_name == "generated_momentum_run_current_paper"
        assert row.signal_family == "momentum"
        assert row.promotion_variant == "conditional"
        assert row.condition_id == "regime::risk_on"
        assert row.condition_type == "regime"
        assert row.rationale == "conditional regime edge"
        assert row.runtime_score_validation_pass in (False, 0)
        assert row.runtime_score_validation_reason == "empty_signal_scores"
        assert row.runtime_computable_symbol_count == 0


def test_research_memory_family_summary_query_runs(tmp_path: Path) -> None:
    service = build_research_memory_service(
        enable_database_metadata=True,
        database_url=_sqlite_url(tmp_path),
    )
    assert service.init_schema(schema_name=None) is True
    lineage = DatabaseLineageService.from_config(enable_database_metadata=True, database_url=_sqlite_url(tmp_path))
    run_id = lineage.create_research_run(run_key="run_summary", run_type="alpha_research", config_payload={"x": 1})
    service.persist_alpha_research_outputs(
        run_id=run_id,
        leaderboard_df=pd.DataFrame(
            [
                {
                    "candidate_id": "fundamental_value_lb20_hz5_base",
                    "candidate_name": "fundamental_value_lb20_hz5",
                    "signal_family": "fundamental_value",
                    "signal_variant": "base",
                    "variant_parameters_json": "{}",
                    "lookback": 20,
                    "horizon": 5,
                    "mean_spearman_ic": 0.05,
                    "promotion_status": "promote",
                }
            ]
        ),
    )
    service.persist_promotions(
        run_id=run_id,
        promoted_rows=[
            {
                "strategy_definition_id": lineage.upsert_strategy_definition(
                    name="generated_value_paper",
                    version="v1",
                    config_payload={"preset_name": "generated_value_paper"},
                ),
                "preset_name": "generated_value_paper",
                "signal_family": "fundamental_value",
                "strategy_name": "composite_alpha",
                "ranking_metric": "mean_spearman_ic",
                "ranking_value": 0.05,
                "status": "inactive",
                "promotion_variant": "unconditional",
            }
        ],
    )
    rows = service.family_summary()
    assert rows
    assert rows[0]["signal_family"] == "fundamental_value"


def test_research_memory_persists_conditional_promotion_fields(tmp_path: Path) -> None:
    service = build_research_memory_service(
        enable_database_metadata=True,
        database_url=_sqlite_url(tmp_path),
    )
    assert service.init_schema(schema_name=None) is True
    lineage = DatabaseLineageService.from_config(enable_database_metadata=True, database_url=_sqlite_url(tmp_path))
    run_id = lineage.create_research_run(run_key="run_conditional", run_type="alpha_research", config_payload={"x": 1})
    strategy_definition_id = lineage.upsert_strategy_definition(
        name="generated_conditional_paper",
        version="v1",
        config_payload={"preset_name": "generated_conditional_paper"},
    )

    service.persist_promotions(
        run_id=run_id,
        promoted_rows=[
            {
                "strategy_definition_id": strategy_definition_id,
                "candidate_id": "multi_family|20|5",
                "preset_name": "generated_conditional_paper",
                "signal_family": "multi_family",
                "strategy_name": "composite_alpha",
                "ranking_metric": "mean_spearman_ic",
                "ranking_value": 0.11,
                "promotion_variant": "conditional",
                "condition_id": "benchmark_context::risk_off_outperform_broad",
                "condition_type": "benchmark_context",
                "rationale": "conditional benchmark edge",
                "runtime_score_validation_pass": True,
                "runtime_score_validation_reason": "runtime_scores_available",
                "runtime_computable_symbol_count": 12,
                "status": "inactive",
            }
        ],
    )

    settings = resolve_database_settings(enable_database_metadata=True, database_url=_sqlite_url(tmp_path))
    session_factory = create_session_factory(settings)
    assert session_factory is not None
    with session_factory() as session:
        row = session.execute(
            text(
                "select candidate_id, promotion_variant, condition_id, condition_type, rationale, "
                "runtime_score_validation_pass, runtime_score_validation_reason, runtime_computable_symbol_count "
                "from promoted_strategies where preset_name = 'generated_conditional_paper'"
            )
        ).first()
        assert row is not None
        assert row.candidate_id == "multi_family|20|5"
        assert row.promotion_variant == "conditional"
        assert row.condition_id == "benchmark_context::risk_off_outperform_broad"
        assert row.condition_type == "benchmark_context"
        assert row.rationale == "conditional benchmark edge"
        assert row.runtime_score_validation_pass in (True, 1)
        assert row.runtime_score_validation_reason == "runtime_scores_available"
        assert row.runtime_computable_symbol_count == 12
