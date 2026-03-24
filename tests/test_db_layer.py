from __future__ import annotations

from pathlib import Path

from sqlalchemy import inspect, text

from trading_platform.db import Base
from trading_platform.db.session import create_engine_from_settings, create_session_factory
from trading_platform.db.settings import resolve_database_settings
from trading_platform.db.services import (
    DatabaseLineageService,
    log_paper_orders_and_fills,
    log_portfolio_decision_bundle,
    register_artifact_bundle,
)
from trading_platform.decision_journal.models import (
    CandidateEvaluation,
    DecisionJournalBundle,
    SignalBreakdown,
    TradeDecisionRecord,
)


def _sqlite_settings(tmp_path: Path):
    return resolve_database_settings(
        enable_database_metadata=True,
        database_url=f"sqlite:///{(tmp_path / 'metadata.db').as_posix()}",
    )


def test_db_models_create_and_disabled_mode(tmp_path: Path) -> None:
    settings = _sqlite_settings(tmp_path)
    engine = create_engine_from_settings(settings)
    assert engine is not None
    Base.metadata.create_all(engine)
    tables = set(inspect(engine).get_table_names())
    assert "research_runs" in tables
    assert "portfolio_runs" in tables
    assert "orders" in tables

    disabled = DatabaseLineageService.from_config(enable_database_metadata=False, database_url=settings.database_url)
    assert disabled.enabled is False
    assert disabled.create_research_run(run_key="x", run_type="research", config_payload={}) is None


def test_db_services_persist_decisions_execution_and_artifacts(tmp_path: Path) -> None:
    settings = _sqlite_settings(tmp_path)
    engine = create_engine_from_settings(settings)
    assert engine is not None
    Base.metadata.create_all(engine)
    db_service = DatabaseLineageService(create_session_factory(settings))

    portfolio_run_id = db_service.create_portfolio_run(
        run_key="paper|demo|sma_cross",
        mode="paper",
        config_payload={"strategy": "sma_cross"},
    )
    decision_bundle = DecisionJournalBundle(
        candidate_evaluations=[
            CandidateEvaluation(
                decision_id="cand-1",
                timestamp="2025-01-04T00:00:00Z",
                run_id="paper-demo",
                cycle_id="2025-01-04",
                symbol="AAPL",
                side="BUY",
                strategy_id="sma_cross",
                universe_id="demo",
                base_universe_id="demo",
                sub_universe_id="demo_screened",
                candidate_status="selected",
                final_signal_score=1.25,
                rank=1,
                signal_breakdown=SignalBreakdown(signal_name="score", raw_components={"momentum": 1.25}),
            )
        ],
        trade_decisions=[
            TradeDecisionRecord(
                decision_id="trade-1",
                timestamp="2025-01-04T00:00:00Z",
                run_id="paper-demo",
                cycle_id="2025-01-04",
                symbol="AAPL",
                side="BUY",
                strategy_id="sma_cross",
                universe_id="demo",
                base_universe_id="demo",
                sub_universe_id="demo_screened",
                candidate_status="selected",
                final_signal_score=1.25,
                target_weight_post_constraint=0.5,
                target_quantity=10,
            )
        ],
    )
    log_portfolio_decision_bundle(
        db_service=db_service,
        portfolio_run_id=portfolio_run_id,
        decision_bundle=decision_bundle,
        universe_bundle=None,
    )
    log_paper_orders_and_fills(
        db_service=db_service,
        orders=[],
        fills=[],
        as_of="2025-01-04T00:00:00Z",
        broker="paper",
    )

    artifact_path = tmp_path / "trade_decisions.json"
    artifact_path.write_text("{}", encoding="utf-8")
    register_artifact_bundle(
        db_service=db_service,
        artifact_paths={"trade_decisions_json": artifact_path},
        artifact_type_prefix="paper",
        portfolio_run_id=portfolio_run_id,
    )
    db_service.complete_portfolio_run(portfolio_run_id, notes="done")

    session_factory = create_session_factory(settings)
    assert session_factory is not None
    with session_factory() as session:
        tables = {name: session.execute(text(f"select count(*) from {name}")).scalar() for name in ["portfolio_runs", "portfolio_decisions", "candidate_evaluations", "decision_signal_contributions", "artifacts", "run_artifact_links"]}
    assert tables["portfolio_runs"] == 1
    assert tables["portfolio_decisions"] == 1
    assert tables["candidate_evaluations"] == 1
    assert tables["decision_signal_contributions"] == 1
    assert tables["artifacts"] == 1
    assert tables["run_artifact_links"] == 1
