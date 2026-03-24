from __future__ import annotations

from datetime import UTC, datetime
import json
from io import BytesIO
from pathlib import Path

import pandas as pd

from trading_platform.dashboard.hybrid_dashboard_service import HybridDashboardDataService
from trading_platform.dashboard.server import create_dashboard_app
from trading_platform.db import Base
from trading_platform.db.repositories import (
    ArtifactRepository,
    ExecutionRepository,
    PortfolioRepository,
    ProvenanceRepository,
    RunRepository,
    StrategyRepository,
)
from trading_platform.db.session import create_engine_from_settings, create_session_factory, session_scope
from trading_platform.db.services import DecisionQueryService, OpsQueryService, RunQueryService
from trading_platform.db.settings import resolve_database_settings


def _write_minimal_dashboard_artifacts(root: Path) -> None:
    run_dir = root / "orchestration" / "daily_governance" / "2026-03-22T00-00-00+00-00"
    (run_dir / "monitoring").mkdir(parents=True, exist_ok=True)
    (run_dir / "paper_trading").mkdir(parents=True, exist_ok=True)
    (run_dir / "run_summary.json").write_text(
        json.dumps(
            {
                "run_name": "daily_governance",
                "schedule_type": "daily",
                "started_at": "2026-03-22T00:00:00+00:00",
                "ended_at": "2026-03-22T00:10:00+00:00",
                "status": "succeeded",
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    pd.DataFrame([{"stage_name": "paper_trading", "status": "succeeded"}]).to_csv(run_dir / "stage_status.csv", index=False)
    (run_dir / "monitoring" / "run_health.json").write_text(
        json.dumps({"status": "healthy", "alert_counts": {"critical": 0, "warning": 0}}, indent=2),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {
                "trade_id": "artifact-1",
                "symbol": "MSFT",
                "side": "long",
                "qty": 10,
                "entry_ts": "2026-03-22T00:00:00+00:00",
                "entry_price": 410.0,
                "status": "open",
                "realized_pnl": 0.0,
            }
        ]
    ).to_csv(run_dir / "paper_trading" / "paper_trades.csv", index=False)


def _call_app(app, path: str) -> tuple[str, dict[str, str], dict]:
    payload: dict[str, object] = {}

    def start_response(status, headers):
        payload["status"] = status
        payload["headers"] = {key: value for key, value in headers}

    result = app({"PATH_INFO": path, "QUERY_STRING": "", "wsgi.input": BytesIO(b"")}, start_response)
    body = b"".join(result)
    return str(payload["status"]), payload["headers"], json.loads(body.decode("utf-8"))


def _sqlite_settings(tmp_path: Path):
    return resolve_database_settings(
        enable_database_metadata=True,
        database_url=f"sqlite:///{(tmp_path / 'dashboard_read.db').as_posix()}",
    )


def _seed_dashboard_db(tmp_path: Path) -> tuple[object, str]:
    settings = _sqlite_settings(tmp_path)
    engine = create_engine_from_settings(settings)
    assert engine is not None
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(settings)
    assert session_factory is not None
    artifact_path = tmp_path / "db_artifacts" / "trade_decisions.json"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text("{}", encoding="utf-8")
    started_at = datetime(2026, 3, 24, 13, 30, tzinfo=UTC)
    with session_scope(session_factory) as session:
        run_repo = RunRepository(session)
        artifact_repo = ArtifactRepository(session)
        portfolio_repo = PortfolioRepository(session)
        provenance_repo = ProvenanceRepository(session)
        execution_repo = ExecutionRepository(session)
        strategy_repo = StrategyRepository(session)

        research_run = run_repo.create_research_run(
            run_key="research|momentum|2026-03-24",
            run_type="research",
            status="completed",
            started_at=started_at,
            config_json={"workflow": "research"},
            config_hash="cfg-research",
            git_commit="abc123",
            notes="research finished",
        )
        research_run.completed_at = started_at

        portfolio_run = run_repo.create_portfolio_run(
            run_key="paper|demo|momentum-core",
            mode="paper",
            status="completed",
            started_at=started_at,
            config_json={"workflow": "paper"},
            config_hash="cfg-paper",
            git_commit="abc123",
            notes="paper finished",
        )
        portfolio_run.completed_at = started_at

        artifact = artifact_repo.register_artifact(
            artifact_type="paper:trade_decisions_json",
            path=str(artifact_path),
            format="json",
            metadata_json={"role": "trade_decisions_json"},
        )
        artifact_repo.link_artifact_to_run(artifact_id=artifact.id, portfolio_run_id=portfolio_run.id, role="trade_decisions_json")

        decision = portfolio_repo.record_portfolio_decision(
            portfolio_run_id=portfolio_run.id,
            symbol="AAPL",
            side="buy",
            target_weight=0.25,
            target_shares=15,
            rank_score=1.23,
            decision_status="selected",
            explanation_json={
                "timestamp": "2026-03-24T13:30:00+00:00",
                "strategy_id": "momentum-core",
                "final_signal_score": 1.23,
                "entry_reason_summary": "ranked first in liquid trend basket",
                "base_universe_id": "nasdaq100",
                "sub_universe_id": "liquid_trend_candidates",
            },
        )
        provenance_repo.record_candidate_evaluation(
            portfolio_run_id=portfolio_run.id,
            symbol="AAPL",
            base_universe_id="nasdaq100",
            sub_universe_id="liquid_trend_candidates",
            score=1.23,
            rank=1,
            candidate_status="selected",
            rejection_reason=None,
            metadata_json={"screening_checks": [{"check_name": "min_price", "status": "pass"}]},
        )
        provenance_repo.record_universe_filter_result(
            portfolio_run_id=portfolio_run.id,
            symbol="AAPL",
            filter_name="min_price",
            pass_fail="pass",
            observed_value="185.0",
            reason="above threshold",
            metadata_json={"threshold": 10.0},
        )
        order = execution_repo.record_order(
            portfolio_decision_id=decision.id,
            symbol="AAPL",
            status="submitted",
            broker="paper",
            side="buy",
            order_type="market",
            tif="day",
            quantity=15.0,
            submitted_at=started_at,
            updated_at=started_at,
        )
        execution_repo.record_order_event(
            order_id=order.id,
            event_type="submitted",
            event_ts=started_at,
            payload_json={"client_order_id": "cid-1"},
        )
        execution_repo.record_fill(
            order_id=order.id,
            quantity=15.0,
            price=185.5,
            fill_ts=started_at,
            fees=1.25,
            liquidity_flag="maker",
            payload_json={},
        )

        strategy = strategy_repo.upsert_strategy_definition(
            name="momentum-core",
            version="v1",
            config_json={"family": "momentum"},
            code_hash="code123",
        )
        promotion = strategy_repo.record_promotion_decision(
            strategy_definition_id=strategy.id,
            source_research_run_id=research_run.id,
            decision="promote",
            reason="strong walk-forward",
            metrics_json={"portfolio_sharpe": 1.4},
        )
        strategy_repo.record_promoted_strategy(
            strategy_definition_id=strategy.id,
            promotion_decision_id=promotion.id,
            active_from=started_at,
            status="active",
        )
        return settings, str(decision.id)


def test_db_query_services_shape_recent_runs_and_decisions(tmp_path: Path) -> None:
    settings, decision_id = _seed_dashboard_db(tmp_path)
    session_factory = create_session_factory(settings)
    assert session_factory is not None

    run_queries = RunQueryService(session_factory)
    decision_queries = DecisionQueryService(session_factory)
    ops_queries = OpsQueryService(session_factory)

    portfolio_runs = run_queries.list_recent_portfolio_runs(limit=5)
    assert portfolio_runs[0].run_name == "paper|demo|momentum-core"
    assert portfolio_runs[0].artifact_count == 1

    decisions = decision_queries.list_recent_trade_decisions(limit=5)
    assert decisions[0].trade_id == decision_id
    assert decisions[0].symbol == "AAPL"
    assert decisions[0].order_status == "submitted"

    detail = decision_queries.get_trade_decision_detail(decision_id)
    assert detail is not None
    assert detail["decision"]["strategy_id"] == "momentum-core"
    assert detail["filter_results"][0]["filter_name"] == "min_price"
    assert detail["execution"][0]["fill_count"] == 1

    ops = ops_queries.get_ops_health_summary()
    assert ops["summary"]["latest_run_name"] == "paper|demo|momentum-core"
    assert ops["recent_promotions"][0]["strategy_name"] == "momentum-core"


def test_hybrid_dashboard_service_falls_back_to_artifacts_when_db_disabled(tmp_path: Path) -> None:
    _write_minimal_dashboard_artifacts(tmp_path)
    service = HybridDashboardDataService(tmp_path)

    blotter = service.trade_blotter_payload()
    ops = service.ops_payload()

    assert blotter["source"] == "artifact"
    assert ops["source"] == "artifact"


def test_hybrid_dashboard_service_falls_back_when_db_has_no_rows(tmp_path: Path, monkeypatch) -> None:
    _write_minimal_dashboard_artifacts(tmp_path)
    settings = _sqlite_settings(tmp_path)
    engine = create_engine_from_settings(settings)
    assert engine is not None
    Base.metadata.create_all(engine)
    monkeypatch.setenv("TRADING_PLATFORM_ENABLE_DATABASE_METADATA", "true")
    monkeypatch.setenv("TRADING_PLATFORM_DATABASE_URL", settings.database_url or "")

    service = HybridDashboardDataService(tmp_path)
    blotter = service.trade_blotter_payload()
    runs = service.runs_payload()

    assert blotter["source"] == "artifact"
    assert runs["source"] == "artifact"


def test_dashboard_app_prefers_db_when_records_exist(tmp_path: Path, monkeypatch) -> None:
    _write_minimal_dashboard_artifacts(tmp_path)
    settings, decision_id = _seed_dashboard_db(tmp_path)
    monkeypatch.setenv("TRADING_PLATFORM_ENABLE_DATABASE_METADATA", "true")
    monkeypatch.setenv("TRADING_PLATFORM_DATABASE_URL", settings.database_url or "")

    service = HybridDashboardDataService(tmp_path)
    blotter = service.trade_blotter_payload()
    detail = service.trade_detail_payload(decision_id)
    ops = service.ops_payload()

    assert blotter["source"] == "db"
    assert blotter["trades"][0]["trade_id"] == decision_id
    assert detail["meta"]["source"] in {"db", "hybrid"}
    assert detail["provenance"]["filter_results"][0]["filter_name"] == "min_price"
    assert ops["source"] == "hybrid"
    assert ops["runs"][0]["run_name"] == "paper|demo|momentum-core"

    app = create_dashboard_app(tmp_path)
    _status, _headers, trades_payload = _call_app(app, "/api/trades-blotter")
    assert trades_payload["source"] == "db"
    _status, _headers, trade_payload = _call_app(app, f"/api/trade/{decision_id}")
    assert trade_payload["meta"]["source"] in {"db", "hybrid"}
