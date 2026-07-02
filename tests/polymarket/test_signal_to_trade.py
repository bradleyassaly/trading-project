"""
Tests for the signal -> paper trade handoff in the executor.

Specifically validates the fillability floor (entry_price < 0.05 or
> 0.95) added in response to reports/data_validation.md DV-5, where
the price_velocity strategy was producing fictional +$4.97M PnL by
buying NO at 0.001-0.023 on degenerate long-tail markets.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from trading_platform.polymarket.polymarket_paper_executor import (
    PolymarketPaperExecutor,
    MIN_ENTRY_PRICE,
    MAX_ENTRY_PRICE,
)


def _signal(
    *,
    price: float = 0.5,
    confidence: float = 0.7,
    signal_type: str = "price_velocity",
    direction: str = "BUY",
    condition_id: str = "0xabc",
    wallet: str = "velocity_detector",
) -> dict:
    return {
        "signal_type": signal_type,
        "condition_id": condition_id,
        "direction": direction,
        "confidence": confidence,
        "wallet": wallet,
        "price": price,
        "size": 1000.0,
        "category": "other",
        "question": "Test question",
        "wallet_tier": "tier1",
        "directional_win_rate": 0.6,
        "fired_at": 1_700_000_000,
    }


class TestFillabilityFloor:
    def test_rejects_entry_below_floor(self, tmp_path: Path, monkeypatch) -> None:
        # Pin the wallet DB to tmp_path so the tests don't touch real data
        from trading_platform.polymarket import polymarket_paper_executor as mod
        monkeypatch.setattr(mod, "_WALLET_DB_PATH", tmp_path / "wi.db")
        executor = PolymarketPaperExecutor(tmp_path / "paper.db")
        # 0.001 — the worst case from the validation report
        result = executor.execute_signal(_signal(price=0.001))
        assert result is None

    def test_rejects_entry_above_ceiling(self, tmp_path: Path, monkeypatch) -> None:
        from trading_platform.polymarket import polymarket_paper_executor as mod
        monkeypatch.setattr(mod, "_WALLET_DB_PATH", tmp_path / "wi.db")
        executor = PolymarketPaperExecutor(tmp_path / "paper.db")
        result = executor.execute_signal(_signal(price=0.97))
        assert result is None

    def test_rejects_entry_at_extreme_boundary(self, tmp_path: Path, monkeypatch) -> None:
        from trading_platform.polymarket import polymarket_paper_executor as mod
        monkeypatch.setattr(mod, "_WALLET_DB_PATH", tmp_path / "wi.db")
        executor = PolymarketPaperExecutor(tmp_path / "paper.db")
        # Exactly below the floor
        assert executor.execute_signal(_signal(price=MIN_ENTRY_PRICE - 0.001)) is None
        # Exactly above the ceiling
        assert executor.execute_signal(_signal(price=MAX_ENTRY_PRICE + 0.001)) is None

    def test_constants_are_what_we_expect(self) -> None:
        # Validated bounds from reports/win_rate_validation.md.
        # Tightened from 0.05-0.95 to 0.10-0.80 based on profit-by-band
        # analysis, then ceiling raised to 0.85 (live fillable band is
        # [0.10, 0.85]; see polymarket_live_executor BUY_LIVE_MIN_ENTRY
        # comments).
        assert MIN_ENTRY_PRICE == 0.10
        assert MAX_ENTRY_PRICE == 0.85
