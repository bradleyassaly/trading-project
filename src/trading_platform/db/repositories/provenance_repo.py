from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from trading_platform.db.models.provenance import CandidateEvaluation, UniverseFilterResult
from trading_platform.db.repositories.reference_repo import ReferenceRepository


class ProvenanceRepository:
    def __init__(self, session: Session) -> None:
        self.session = session
        self.reference_repo = ReferenceRepository(session)

    def record_candidate_evaluation(self, *, portfolio_run_id, symbol: str, base_universe_id: str | None, sub_universe_id: str | None, score: float | None, rank: int | None, candidate_status: str, rejection_reason: str | None = None, metadata_json: dict[str, Any] | None = None) -> CandidateEvaluation:
        symbol_row = self.reference_repo.upsert_symbol(symbol=symbol)
        row = CandidateEvaluation(portfolio_run_id=portfolio_run_id, symbol_id=symbol_row.id, base_universe_id=base_universe_id, sub_universe_id=sub_universe_id, score=score, rank=rank, candidate_status=candidate_status, rejection_reason=rejection_reason, metadata_json=dict(metadata_json or {}))
        self.session.add(row)
        self.session.flush()
        return row

    def record_universe_filter_result(self, *, symbol: str, filter_name: str, pass_fail: str, observed_value: str | None = None, reason: str | None = None, metadata_json: dict[str, Any] | None = None, portfolio_run_id=None) -> UniverseFilterResult:
        symbol_row = self.reference_repo.upsert_symbol(symbol=symbol)
        row = UniverseFilterResult(portfolio_run_id=portfolio_run_id, symbol_id=symbol_row.id, filter_name=filter_name, pass_fail=pass_fail, observed_value=observed_value, reason=reason, metadata_json=dict(metadata_json or {}))
        self.session.add(row)
        self.session.flush()
        return row
