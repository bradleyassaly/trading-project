from __future__ import annotations

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from trading_platform.db.models.runs import PortfolioRun, ResearchRun


class RunRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def create_research_run(self, *, run_key: str, run_type: str, status: str, started_at: datetime, config_json: dict, config_hash: str | None, git_commit: str | None, notes: str | None = None) -> ResearchRun:
        existing = self.session.scalar(select(ResearchRun).where(ResearchRun.run_key == run_key))
        if existing is not None:
            return existing
        row = ResearchRun(run_key=run_key, run_type=run_type, status=status, started_at=started_at, config_json=dict(config_json), config_hash=config_hash, git_commit=git_commit, notes=notes)
        self.session.add(row)
        self.session.flush()
        return row

    def create_portfolio_run(self, *, run_key: str, mode: str, status: str, started_at: datetime, config_json: dict, config_hash: str | None, git_commit: str | None, notes: str | None = None) -> PortfolioRun:
        existing = self.session.scalar(select(PortfolioRun).where(PortfolioRun.run_key == run_key))
        if existing is not None:
            return existing
        row = PortfolioRun(run_key=run_key, mode=mode, status=status, started_at=started_at, config_json=dict(config_json), config_hash=config_hash, git_commit=git_commit, notes=notes)
        self.session.add(row)
        self.session.flush()
        return row

    def get_research_run_by_key(self, run_key: str) -> ResearchRun | None:
        return self.session.scalar(select(ResearchRun).where(ResearchRun.run_key == run_key))

    def complete_research_run(self, run_id, *, completed_at: datetime, notes: str | None = None) -> ResearchRun | None:
        row = self.session.get(ResearchRun, run_id)
        if row is None:
            return None
        row.status = "completed"
        row.completed_at = completed_at
        if notes:
            row.notes = notes
        self.session.flush()
        return row

    def fail_research_run(self, run_id, *, completed_at: datetime, notes: str | None = None) -> ResearchRun | None:
        row = self.session.get(ResearchRun, run_id)
        if row is None:
            return None
        row.status = "failed"
        row.completed_at = completed_at
        if notes:
            row.notes = notes
        self.session.flush()
        return row

    def complete_portfolio_run(self, run_id, *, completed_at: datetime, notes: str | None = None) -> PortfolioRun | None:
        row = self.session.get(PortfolioRun, run_id)
        if row is None:
            return None
        row.status = "completed"
        row.completed_at = completed_at
        if notes:
            row.notes = notes
        self.session.flush()
        return row

    def fail_portfolio_run(self, run_id, *, completed_at: datetime, notes: str | None = None) -> PortfolioRun | None:
        row = self.session.get(PortfolioRun, run_id)
        if row is None:
            return None
        row.status = "failed"
        row.completed_at = completed_at
        if notes:
            row.notes = notes
        self.session.flush()
        return row
