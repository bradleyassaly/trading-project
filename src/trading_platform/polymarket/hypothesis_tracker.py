"""Hypothesis tracker — persistent record of every alpha hypothesis we
test, its current stage in the discovery on-ramp, and the evidence
attached.

Stage definitions live in `reports/alpha_discovery_onramp_2026-04-24.md`.
This module just persists the (id, name, stage, evidence) tuple so the
team can answer 'what's in flight?' without grepping notebooks.

Schema (created lazily on first call):

    CREATE TABLE IF NOT EXISTS alpha_hypotheses (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        name            TEXT UNIQUE NOT NULL,
        slice           TEXT,         -- compact JSON: signal_type/side/category/etc
        stage           INTEGER NOT NULL DEFAULT 1, -- 1..6, see onramp doc
        status          TEXT NOT NULL DEFAULT 'active',  -- active|killed|live
        last_evidence   TEXT,         -- short summary string
        wr              REAL,
        ev              REAL,
        n_resolved      INTEGER,
        last_pnl        REAL,
        created_at      INTEGER NOT NULL,
        updated_at      INTEGER NOT NULL,
        notes           TEXT
    );
"""
from __future__ import annotations

import json
import logging
import time
from typing import Any

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS alpha_hypotheses (
    id              BIGSERIAL PRIMARY KEY,
    name            TEXT UNIQUE NOT NULL,
    slice           TEXT,
    stage           BIGINT NOT NULL DEFAULT 1,
    status          TEXT NOT NULL DEFAULT 'active',
    last_evidence   TEXT,
    wr              DOUBLE PRECISION,
    ev              DOUBLE PRECISION,
    n_resolved      BIGINT,
    last_pnl        DOUBLE PRECISION,
    created_at      BIGINT NOT NULL,
    updated_at      BIGINT NOT NULL,
    notes           TEXT
);
CREATE INDEX IF NOT EXISTS idx_alpha_hypotheses_stage ON alpha_hypotheses(stage);
CREATE INDEX IF NOT EXISTS idx_alpha_hypotheses_status ON alpha_hypotheses(status);
"""


def _ensure_schema(conn) -> None:
    for stmt in _SCHEMA.strip().split(";"):
        s = stmt.strip()
        if s:
            conn.execute(s)


def upsert(
    name: str,
    *,
    slice_: dict | None = None,
    stage: int | None = None,
    status: str | None = None,
    evidence: str | None = None,
    wr: float | None = None,
    ev: float | None = None,
    n_resolved: int | None = None,
    last_pnl: float | None = None,
    notes: str | None = None,
    db_path: str | None = None,
) -> int:
    """Create or update a hypothesis. Returns the row id."""
    conn = get_connection(db_path) if db_path else get_connection()
    try:
        _ensure_schema(conn)
        now = int(time.time())
        existing = conn.execute(
            "SELECT id, stage, status FROM alpha_hypotheses WHERE name = ?",
            (name,),
        ).fetchone()
        slice_str = json.dumps(slice_) if slice_ else None
        if existing:
            row_id = int(existing[0])
            updates: list[str] = []
            params: list[Any] = []
            if slice_str is not None: updates.append("slice = ?"); params.append(slice_str)
            if stage is not None:     updates.append("stage = ?"); params.append(stage)
            if status is not None:    updates.append("status = ?"); params.append(status)
            if evidence is not None:  updates.append("last_evidence = ?"); params.append(evidence)
            if wr is not None:        updates.append("wr = ?"); params.append(wr)
            if ev is not None:        updates.append("ev = ?"); params.append(ev)
            if n_resolved is not None: updates.append("n_resolved = ?"); params.append(n_resolved)
            if last_pnl is not None:  updates.append("last_pnl = ?"); params.append(last_pnl)
            if notes is not None:     updates.append("notes = ?"); params.append(notes)
            if updates:
                updates.append("updated_at = ?")
                params.append(now)
                params.append(row_id)
                conn.execute(
                    f"UPDATE alpha_hypotheses SET {', '.join(updates)} WHERE id = ?",
                    params,
                )
                conn.commit()
            return row_id
        conn.execute(
            """INSERT INTO alpha_hypotheses
                 (name, slice, stage, status, last_evidence, wr, ev,
                  n_resolved, last_pnl, created_at, updated_at, notes)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                name, slice_str, stage or 1, status or "active",
                evidence, wr, ev, n_resolved, last_pnl, now, now, notes,
            ),
        )
        conn.commit()
        row = conn.execute(
            "SELECT id FROM alpha_hypotheses WHERE name = ?", (name,),
        ).fetchone()
        return int(row[0]) if row else 0
    finally:
        try: conn.close()
        except Exception: pass


def list_all(
    *, status: str | None = None, db_path: str | None = None,
) -> list[dict]:
    """Return every hypothesis as a dict, optionally filtered by status."""
    conn = get_connection(db_path) if db_path else get_connection()
    try:
        _ensure_schema(conn)
        if status:
            rows = conn.execute(
                "SELECT id, name, slice, stage, status, last_evidence, wr, ev, "
                "n_resolved, last_pnl, created_at, updated_at, notes "
                "FROM alpha_hypotheses WHERE status = ? ORDER BY stage DESC, updated_at DESC",
                (status,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT id, name, slice, stage, status, last_evidence, wr, ev, "
                "n_resolved, last_pnl, created_at, updated_at, notes "
                "FROM alpha_hypotheses ORDER BY stage DESC, updated_at DESC",
            ).fetchall()
        out = []
        for r in rows:
            out.append({
                "id": r[0], "name": r[1],
                "slice": json.loads(r[2]) if r[2] else None,
                "stage": r[3], "status": r[4], "last_evidence": r[5],
                "wr": r[6], "ev": r[7], "n_resolved": r[8],
                "last_pnl": r[9], "created_at": r[10], "updated_at": r[11],
                "notes": r[12],
            })
        return out
    finally:
        try: conn.close()
        except Exception: pass
