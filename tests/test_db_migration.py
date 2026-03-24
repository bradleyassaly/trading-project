from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect


def test_alembic_upgrade_smoke(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "migration.db"
    monkeypatch.setenv("TRADING_PLATFORM_DATABASE_URL", f"sqlite:///{db_path.as_posix()}")
    config = Config(str(Path(__file__).resolve().parents[1] / "alembic.ini"))

    command.upgrade(config, "head")

    engine = create_engine(f"sqlite:///{db_path.as_posix()}")
    tables = set(inspect(engine).get_table_names())
    assert "research_runs" in tables
    assert "portfolio_runs" in tables
    assert "artifacts" in tables
