from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import subprocess
from typing import Any

from sqlalchemy.orm import Session, sessionmaker

from trading_platform.db.repositories import RunRepository, StrategyRepository
from trading_platform.db.session import create_session_factory, session_scope
from trading_platform.db.settings import resolve_database_settings


def _normalize_payload(payload: Any) -> dict[str, Any]:
    if payload is None:
        return {}
    if is_dataclass(payload):
        return asdict(payload)
    if hasattr(payload, "to_dict"):
        return dict(payload.to_dict())
    if isinstance(payload, dict):
        return dict(payload)
    return {"value": str(payload)}


def stable_config_hash(payload: Any) -> str:
    text = json.dumps(_normalize_payload(payload), sort_keys=True, default=str)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def current_git_commit(cwd: str | Path | None = None) -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(cwd) if cwd else None,
            capture_output=True,
            text=True,
            check=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        return None
    text = result.stdout.strip()
    return text or None


class DatabaseLineageService:
    def __init__(self, session_factory: sessionmaker[Session] | None) -> None:
        self.session_factory = session_factory

    @property
    def enabled(self) -> bool:
        return self.session_factory is not None

    @classmethod
    def from_config(
        cls,
        *,
        enable_database_metadata: bool | None = None,
        database_url: str | None = None,
        database_schema: str | None = None,
    ) -> "DatabaseLineageService":
        settings = resolve_database_settings(
            enable_database_metadata=enable_database_metadata,
            database_url=database_url,
            database_schema=database_schema,
        )
        return cls(create_session_factory(settings))

    def create_research_run(self, *, run_key: str, run_type: str, config_payload: Any, notes: str | None = None):
        if not self.enabled:
            return None
        config_json = _normalize_payload(config_payload)
        with session_scope(self.session_factory) as session:
            row = RunRepository(session).create_research_run(
                run_key=run_key,
                run_type=run_type,
                status="running",
                started_at=datetime.now(UTC),
                config_json=config_json,
                config_hash=stable_config_hash(config_json),
                git_commit=current_git_commit(Path.cwd()),
                notes=notes,
            )
            return row.id

    def complete_research_run(self, run_id, *, notes: str | None = None) -> None:
        if not self.enabled or run_id is None:
            return
        with session_scope(self.session_factory) as session:
            RunRepository(session).complete_research_run(run_id, completed_at=datetime.now(UTC), notes=notes)

    def fail_research_run(self, run_id, *, notes: str | None = None) -> None:
        if not self.enabled or run_id is None:
            return
        with session_scope(self.session_factory) as session:
            RunRepository(session).fail_research_run(run_id, completed_at=datetime.now(UTC), notes=notes)

    def create_portfolio_run(self, *, run_key: str, mode: str, config_payload: Any, notes: str | None = None):
        if not self.enabled:
            return None
        config_json = _normalize_payload(config_payload)
        with session_scope(self.session_factory) as session:
            row = RunRepository(session).create_portfolio_run(
                run_key=run_key,
                mode=mode,
                status="running",
                started_at=datetime.now(UTC),
                config_json=config_json,
                config_hash=stable_config_hash(config_json),
                git_commit=current_git_commit(Path.cwd()),
                notes=notes,
            )
            return row.id

    def complete_portfolio_run(self, run_id, *, notes: str | None = None) -> None:
        if not self.enabled or run_id is None:
            return
        with session_scope(self.session_factory) as session:
            RunRepository(session).complete_portfolio_run(run_id, completed_at=datetime.now(UTC), notes=notes)

    def fail_portfolio_run(self, run_id, *, notes: str | None = None) -> None:
        if not self.enabled or run_id is None:
            return
        with session_scope(self.session_factory) as session:
            RunRepository(session).fail_portfolio_run(run_id, completed_at=datetime.now(UTC), notes=notes)

    def find_research_run_id(self, run_key: str):
        if not self.enabled:
            return None
        with session_scope(self.session_factory) as session:
            row = RunRepository(session).get_research_run_by_key(run_key)
            return None if row is None else row.id

    def upsert_strategy_definition(self, *, name: str, version: str, config_payload: Any, code_hash: str | None = None, is_active: bool = True):
        if not self.enabled:
            return None
        with session_scope(self.session_factory) as session:
            row = StrategyRepository(session).upsert_strategy_definition(
                name=name,
                version=version,
                config_json=_normalize_payload(config_payload),
                code_hash=code_hash,
                is_active=is_active,
            )
            return row.id

    def record_promotion_decision(self, *, strategy_definition_id, source_research_run_id=None, decision: str, reason: str | None = None, metrics_json: dict[str, Any] | None = None):
        if not self.enabled:
            return None
        with session_scope(self.session_factory) as session:
            row = StrategyRepository(session).record_promotion_decision(
                strategy_definition_id=strategy_definition_id,
                source_research_run_id=source_research_run_id,
                decision=decision,
                reason=reason,
                metrics_json=metrics_json,
            )
            return row.id

    def record_promoted_strategy(self, *, strategy_definition_id, promotion_decision_id=None, active_from=None, active_to=None, status: str):
        if not self.enabled:
            return None
        with session_scope(self.session_factory) as session:
            row = StrategyRepository(session).record_promoted_strategy(
                strategy_definition_id=strategy_definition_id,
                promotion_decision_id=promotion_decision_id,
                active_from=active_from,
                active_to=active_to,
                status=status,
            )
            return row.id
