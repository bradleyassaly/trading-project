"""Initial hybrid storage schema."""
from __future__ import annotations

from alembic import op

from trading_platform.db.base import Base
import trading_platform.db.models  # noqa: F401


revision = "20260324_000001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    Base.metadata.create_all(bind=op.get_bind())


def downgrade() -> None:
    Base.metadata.drop_all(bind=op.get_bind())
