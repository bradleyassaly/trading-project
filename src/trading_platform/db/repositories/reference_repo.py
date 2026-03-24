from __future__ import annotations

from datetime import date
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from trading_platform.db.models.reference import Symbol, Universe, UniverseMembership


class ReferenceRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def upsert_symbol(
        self,
        *,
        symbol: str,
        name: str | None = None,
        asset_type: str | None = None,
        exchange: str | None = None,
        is_active: bool = True,
    ) -> Symbol:
        normalized = str(symbol).strip().upper()
        row = self.session.scalar(select(Symbol).where(Symbol.symbol == normalized))
        if row is None:
            row = Symbol(symbol=normalized, name=name, asset_type=asset_type, exchange=exchange, is_active=is_active)
            self.session.add(row)
        else:
            row.name = name or row.name
            row.asset_type = asset_type or row.asset_type
            row.exchange = exchange or row.exchange
            row.is_active = is_active
        self.session.flush()
        return row

    def upsert_universe(self, *, universe_id: str, description: str | None = None) -> Universe:
        key = str(universe_id).strip()
        row = self.session.scalar(select(Universe).where(Universe.universe_id == key))
        if row is None:
            row = Universe(universe_id=key, description=description)
            self.session.add(row)
        else:
            row.description = description or row.description
        self.session.flush()
        return row

    def record_universe_membership(
        self,
        *,
        universe_id: str,
        symbol: str,
        membership_status: str,
        start_date: date | None = None,
        end_date: date | None = None,
        metadata_json: dict[str, Any] | None = None,
    ) -> UniverseMembership:
        universe = self.upsert_universe(universe_id=universe_id)
        symbol_row = self.upsert_symbol(symbol=symbol)
        query = select(UniverseMembership).where(
            UniverseMembership.universe_id == universe.id,
            UniverseMembership.symbol_id == symbol_row.id,
            UniverseMembership.start_date == start_date,
            UniverseMembership.end_date == end_date,
        )
        row = self.session.scalar(query)
        if row is None:
            row = UniverseMembership(
                universe_id=universe.id,
                symbol_id=symbol_row.id,
                start_date=start_date,
                end_date=end_date,
                membership_status=membership_status,
                metadata_json=dict(metadata_json or {}),
            )
            self.session.add(row)
        else:
            row.membership_status = membership_status
            row.metadata_json = dict(metadata_json or row.metadata_json)
        self.session.flush()
        return row
