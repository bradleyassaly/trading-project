"""
Auto-calibrating bankroll allocator.

Reads ``signal_calibration`` and the live bankroll, then assigns:

* ``live`` signals      → Kelly-weighted share of 80% of bankroll
* ``building`` signals  → fixed $50 micro-allocation for data collection
* ``weak`` signals      → half of what live would get
* ``disabled`` signals  → $0

Stake per trade is computed as ``allocation * kelly_fraction`` and
clamped to ``[MIN_STAKE, MAX_STAKE_PCT * bankroll]``.

The allocator never spends the full bankroll: 20% is reserved as a
buffer regardless of how high a Kelly fraction is.
"""
from __future__ import annotations

import logging
import sqlite3
from trading_platform.polymarket.db import connect_wallet_db
import time
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

# Allocator constants
DEFAULT_BANKROLL = 100_000
MAX_DEPLOYED_PCT = 0.80         # never allocate more than 80% of bankroll
BUILDING_ALLOCATION_USD = 50.0  # micro-allocation for new signal types
WEAK_DISCOUNT = 0.5             # halve allocations for "weak" signals
MIN_STAKE_USD = 10.0
MAX_STAKE_PCT = 0.05            # max single-trade stake = 5% of bankroll
CONFIDENCE_RAMP_TARGET = 30     # full Kelly weight at 30+ samples


@dataclass
class AllocationRow:
    signal_type: str
    status: str
    sample_size: int
    kelly_fraction: float
    confidence_factor: float
    raw_weight: float
    normalized_weight: float
    allocation_usd: float
    stake_usd: float

    def to_dict(self) -> dict[str, Any]:
        return {k: getattr(self, k) for k in self.__dataclass_fields__}


@dataclass
class AllocationPlan:
    bankroll: float
    deployed_usd: float
    reserved_usd: float
    rows: list[AllocationRow]
    timestamp: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "bankroll": self.bankroll,
            "deployed_usd": self.deployed_usd,
            "reserved_usd": self.reserved_usd,
            "rows": [r.to_dict() for r in self.rows],
            "timestamp": self.timestamp,
        }


class BankrollAllocator:
    """Compute Kelly-weighted allocations from signal_calibration."""

    def __init__(self, db_path: str, bankroll: float = DEFAULT_BANKROLL) -> None:
        self._db_path = str(db_path)
        self._bankroll = float(bankroll)

    def _read_calibration(self) -> list[dict[str, Any]]:
        conn = connect_wallet_db(self._db_path)
        try:
            rows = conn.execute(
                """SELECT signal_type, sample_size, kelly_fraction, status
                   FROM signal_calibration"""
            ).fetchall()
        finally:
            conn.close()
        return [
            {
                "signal_type": r[0],
                "sample_size": r[1] or 0,
                "kelly_fraction": float(r[2] or 0.0),
                "status": (r[3] or "building"),
            }
            for r in rows
        ]

    def compute_plan(self) -> AllocationPlan:
        """Compute the full allocation plan. Pure read — no writes."""
        cal = self._read_calibration()
        bankroll = self._bankroll
        max_stake_abs = bankroll * MAX_STAKE_PCT

        # Live signals get the lion's share. Weak get half a slot. Building
        # gets a flat micro-allocation. Disabled gets nothing.
        live_pool: list[dict[str, Any]] = []
        weak_pool: list[dict[str, Any]] = []
        building_pool: list[dict[str, Any]] = []
        for c in cal:
            if c["status"] == "live":
                live_pool.append(c)
            elif c["status"] == "weak":
                weak_pool.append(c)
            elif c["status"] == "building":
                building_pool.append(c)
            # disabled → ignored entirely

        deployable = bankroll * MAX_DEPLOYED_PCT

        # 1. Building micro-allocations come off the top
        building_total = BUILDING_ALLOCATION_USD * len(building_pool)
        building_total = min(building_total, deployable * 0.10)  # cap to 10% of deployable
        building_share = (
            building_total / len(building_pool) if building_pool else 0.0
        )

        remaining = max(0.0, deployable - building_total)

        # 2. Compute raw weights for live + weak (weak gets half)
        weighted_pool: list[dict[str, Any]] = []
        for c in live_pool:
            cf = min(1.0, (c["sample_size"] or 0) / float(CONFIDENCE_RAMP_TARGET))
            weighted_pool.append({**c, "_cf": cf, "_discount": 1.0})
        for c in weak_pool:
            cf = min(1.0, (c["sample_size"] or 0) / float(CONFIDENCE_RAMP_TARGET))
            weighted_pool.append({**c, "_cf": cf, "_discount": WEAK_DISCOUNT})

        raw_weights: list[float] = []
        for c in weighted_pool:
            rw = c["kelly_fraction"] * c["_cf"] * c["_discount"]
            raw_weights.append(max(0.0, rw))
        total_raw = sum(raw_weights)

        rows: list[AllocationRow] = []
        # Live + weak rows
        for c, raw in zip(weighted_pool, raw_weights):
            normalized = (raw / total_raw) if total_raw > 0 else 0.0
            allocation = remaining * normalized
            stake = allocation * c["kelly_fraction"] if c["kelly_fraction"] > 0 else 0.0
            stake = max(MIN_STAKE_USD, min(max_stake_abs, stake)) if allocation > 0 else 0.0
            rows.append(AllocationRow(
                signal_type=c["signal_type"],
                status=c["status"],
                sample_size=c["sample_size"],
                kelly_fraction=round(c["kelly_fraction"], 4),
                confidence_factor=round(c["_cf"], 4),
                raw_weight=round(raw, 6),
                normalized_weight=round(normalized, 4),
                allocation_usd=round(allocation, 2),
                stake_usd=round(stake, 2),
            ))

        # Building rows
        for c in building_pool:
            stake = max(MIN_STAKE_USD, min(max_stake_abs, building_share))
            rows.append(AllocationRow(
                signal_type=c["signal_type"],
                status="building",
                sample_size=c["sample_size"],
                kelly_fraction=round(c["kelly_fraction"], 4),
                confidence_factor=0.0,
                raw_weight=0.0,
                normalized_weight=0.0,
                allocation_usd=round(building_share, 2),
                stake_usd=round(stake, 2),
            ))

        deployed = sum(r.allocation_usd for r in rows)
        return AllocationPlan(
            bankroll=bankroll,
            deployed_usd=round(deployed, 2),
            reserved_usd=round(bankroll - deployed, 2),
            rows=rows,
            timestamp=int(time.time()),
        )

    def rebalance(self, dry_run: bool = False) -> AllocationPlan:
        """Compute and (unless dry_run) persist the new allocation plan."""
        plan = self.compute_plan()
        if dry_run:
            return plan
        conn = connect_wallet_db(self._db_path)
        try:
            now = int(time.time())
            for row in plan.rows:
                conn.execute(
                    """UPDATE signal_calibration
                       SET recommended_allocation_pct = ?,
                           recommended_stake_usd = ?,
                           allocated_usd = ?,
                           last_updated = ?
                       WHERE signal_type = ?""",
                    (
                        row.normalized_weight,
                        row.stake_usd,
                        row.allocation_usd,
                        now,
                        row.signal_type,
                    ),
                )
            conn.commit()
        finally:
            conn.close()
        logger.info(
            "[ALLOC] rebalanced: deployed=$%.0f reserved=$%.0f live=%d weak=%d building=%d",
            plan.deployed_usd, plan.reserved_usd,
            sum(1 for r in plan.rows if r.status == "live"),
            sum(1 for r in plan.rows if r.status == "weak"),
            sum(1 for r in plan.rows if r.status == "building"),
        )
        return plan

    def get_stake_for(self, signal_type: str) -> float:
        """Look up the current calibrated stake for a signal type.

        Returns 0 if the signal is disabled or has no allocation. Falls
        back to MIN_STAKE_USD when the calibration row is missing
        entirely (first-trade scenario for an unseen signal type).
        """
        conn = connect_wallet_db(self._db_path)
        try:
            row = conn.execute(
                "SELECT recommended_stake_usd, status FROM signal_calibration WHERE signal_type = ?",
                (signal_type,),
            ).fetchone()
        finally:
            conn.close()
        if not row:
            return MIN_STAKE_USD
        stake, status = row
        if status == "disabled":
            return 0.0
        return float(stake or MIN_STAKE_USD)
