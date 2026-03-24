from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pandas as pd

from trading_platform.db.repositories import PortfolioRepository, ProvenanceRepository, ReferenceRepository
from trading_platform.db.session import session_scope
from trading_platform.db.services.lineage_service import DatabaseLineageService
from trading_platform.decision_journal.models import DecisionJournalBundle
from trading_platform.universe_provenance.models import UniverseBuildBundle


def _parse_dt(value: str | None) -> datetime:
    if not value:
        return datetime.now(UTC)
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.to_pydatetime()


def log_portfolio_decision_bundle(*, db_service: DatabaseLineageService, portfolio_run_id, decision_bundle: DecisionJournalBundle | None, universe_bundle: UniverseBuildBundle | None = None) -> None:
    if not db_service.enabled or portfolio_run_id is None:
        return
    with session_scope(db_service.session_factory) as session:
        provenance_repo = ProvenanceRepository(session)
        portfolio_repo = PortfolioRepository(session)
        reference_repo = ReferenceRepository(session)

        if universe_bundle is not None and universe_bundle.summary is not None:
            reference_repo.upsert_universe(
                universe_id=str(universe_bundle.summary.base_universe_id or universe_bundle.summary.universe_id or "unknown"),
                description="Universe provenance base universe",
            )
            for record in universe_bundle.membership_records:
                reference_repo.record_universe_membership(
                    universe_id=str(record.base_universe_id or record.universe_id or "unknown"),
                    symbol=record.symbol,
                    membership_status=record.inclusion_status,
                    start_date=pd.Timestamp(record.as_of).date() if record.as_of else None,
                    metadata_json=record.to_dict(),
                )
            for row in universe_bundle.filter_results:
                provenance_repo.record_universe_filter_result(
                    portfolio_run_id=portfolio_run_id,
                    symbol=row.symbol,
                    filter_name=row.filter_name,
                    pass_fail=row.status,
                    observed_value=None if row.observed_value is None else str(row.observed_value),
                    reason=row.exclusion_reason or row.inclusion_reason,
                    metadata_json=row.to_dict(),
                )

        if decision_bundle is None:
            return

        portfolio_decision_ids: dict[str, Any] = {}
        for row in decision_bundle.trade_decisions:
            decision = portfolio_repo.record_portfolio_decision(
                portfolio_run_id=portfolio_run_id,
                symbol=row.symbol,
                side=row.side,
                target_weight=row.target_weight_post_constraint,
                target_shares=row.target_quantity,
                rank_score=row.final_signal_score,
                decision_status=row.candidate_status,
                explanation_json=row.to_dict(),
            )
            portfolio_decision_ids[row.symbol] = decision.id

        for row in decision_bundle.candidate_evaluations:
            provenance_repo.record_candidate_evaluation(
                portfolio_run_id=portfolio_run_id,
                symbol=row.symbol,
                base_universe_id=row.base_universe_id,
                sub_universe_id=row.sub_universe_id,
                score=row.final_signal_score,
                rank=row.rank,
                candidate_status=row.candidate_status,
                rejection_reason=row.rejection_reason,
                metadata_json=row.to_dict(),
            )
            if row.signal_breakdown is None:
                continue
            portfolio_decision_id = portfolio_decision_ids.get(row.symbol)
            if portfolio_decision_id is None:
                continue
            for rank, (signal_name, contribution_value) in enumerate(sorted(row.signal_breakdown.raw_components.items()), start=1):
                try:
                    numeric_value = None if contribution_value is None else float(contribution_value)
                except (TypeError, ValueError):
                    numeric_value = None
                portfolio_repo.record_signal_contribution(
                    portfolio_decision_id=portfolio_decision_id,
                    signal_name=signal_name,
                    contribution_value=numeric_value,
                    contribution_rank=rank,
                    metadata_json={"transformed_components": row.signal_breakdown.transformed_components, "reason_labels": row.signal_breakdown.reason_labels},
                )


def log_position_snapshots(*, db_service: DatabaseLineageService, positions: dict[str, Any], as_of: str, account: str | None, source: str) -> None:
    if not db_service.enabled:
        return
    with session_scope(db_service.session_factory) as session:
        repo = PortfolioRepository(session)
        as_of_ts = _parse_dt(as_of)
        for symbol, position in positions.items():
            repo.update_position_snapshot(
                symbol=symbol,
                account=account,
                quantity=float(getattr(position, "quantity", 0.0)),
                avg_price=None if getattr(position, "avg_price", None) is None else float(getattr(position, "avg_price")),
                market_value=None if getattr(position, "market_value", None) is None else float(getattr(position, "market_value")),
                as_of_ts=as_of_ts,
                source=source,
                metadata_json={},
            )
