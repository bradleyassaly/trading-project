from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.db.repositories import ArtifactRepository
from trading_platform.db.session import session_scope
from trading_platform.db.services.lineage_service import DatabaseLineageService


def _hash_file(path: Path) -> str | None:
    if not path.exists() or not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _row_count(path: Path) -> int | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        if path.suffix.lower() == ".csv":
            return int(pd.read_csv(path).shape[0])
        if path.suffix.lower() == ".parquet":
            return int(pd.read_parquet(path).shape[0])
    except Exception:
        return None
    return None


def register_artifact_bundle(
    *,
    db_service: DatabaseLineageService,
    artifact_paths: dict[str, Path],
    artifact_type_prefix: str,
    research_run_id=None,
    portfolio_run_id=None,
    metadata_by_role: dict[str, dict[str, Any]] | None = None,
) -> None:
    if not db_service.enabled or not artifact_paths:
        return
    with session_scope(db_service.session_factory) as session:
        repo = ArtifactRepository(session)
        for role, path in artifact_paths.items():
            file_path = Path(path)
            if not file_path.exists():
                continue
            artifact = repo.register_artifact(
                artifact_type=f"{artifact_type_prefix}:{role}",
                path=str(file_path),
                format=file_path.suffix.lstrip(".").lower() or None,
                content_hash=_hash_file(file_path),
                row_count=_row_count(file_path),
                metadata_json=dict((metadata_by_role or {}).get(role, {})),
            )
            repo.link_artifact_to_run(
                artifact_id=artifact.id,
                research_run_id=research_run_id,
                portfolio_run_id=portfolio_run_id,
                role=role,
            )
