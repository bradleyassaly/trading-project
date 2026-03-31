from __future__ import annotations

import json

import pytest

from trading_platform.paper.models import (
    PERSISTENT_PAPER_STATE_SCHEMA_VERSION,
    PersistentPaperState,
    PaperPortfolioState,
    PaperPosition,
    PaperTradeLot,
)
from trading_platform.paper.service import JsonPaperStateStore


def test_persistent_paper_state_round_trip_is_deterministic() -> None:
    state = PaperPortfolioState(
        as_of="2025-01-21",
        cash=9_000.0,
        positions={
            "AAPL": PaperPosition(symbol="AAPL", quantity=10, avg_price=100.0, last_price=110.0),
        },
        last_targets={"AAPL": 0.5},
        initial_cash_basis=10_000.0,
        open_lots={
            "AAPL": [
                PaperTradeLot(
                    trade_id="trade-1",
                    symbol="AAPL",
                    strategy_id="generated_momentum_a",
                    signal_source="legacy",
                    signal_family="momentum",
                    side="BUY",
                    entry_as_of="2025-01-20",
                    entry_reference_price=100.0,
                    entry_price=101.0,
                    quantity=10,
                    remaining_quantity=10,
                )
            ]
        },
        next_trade_id=2,
    )

    persistent = PersistentPaperState.from_portfolio_state(state)
    payload = persistent.to_dict()

    assert payload["schema_version"] == PERSISTENT_PAPER_STATE_SCHEMA_VERSION
    assert payload["positions"]["AAPL"]["last_price"] == 110.0
    assert json.dumps(payload, sort_keys=True) == json.dumps(persistent.to_dict(), sort_keys=True)
    restored = PersistentPaperState.from_dict(payload).to_portfolio_state()
    assert restored.cash == state.cash
    assert restored.positions["AAPL"].last_price == state.positions["AAPL"].last_price
    assert restored.next_trade_id == 2


def test_persistent_paper_state_rejects_unknown_schema_version() -> None:
    with pytest.raises(ValueError, match="Unsupported persistent paper state schema_version"):
        PersistentPaperState.from_dict({"schema_version": 99})


def test_json_paper_state_store_loads_missing_corrupt_and_partial_state(tmp_path) -> None:
    state_path = tmp_path / "paper_state.json"
    store = JsonPaperStateStore(state_path)

    missing = store.load()
    assert missing.cash == 0.0
    assert missing.positions == {}

    state_path.write_text("{not-json", encoding="utf-8")
    corrupt = store.load()
    assert corrupt.cash == 0.0
    assert corrupt.positions == {}

    state_path.write_text(
        json.dumps(
            {
                "cash": 5_000.0,
                "positions": {
                    "AAPL": {"quantity": 3},
                    "MSFT": "bad-row",
                },
                "open_lots": {
                    "AAPL": [
                        {
                            "trade_id": "trade-1",
                            "quantity": 3,
                        },
                        "bad-row",
                    ]
                },
            }
        ),
        encoding="utf-8",
    )
    partial = store.load()
    assert partial.cash == 5_000.0
    assert partial.positions["AAPL"].symbol == "AAPL"
    assert partial.positions["AAPL"].quantity == 3
    assert "MSFT" not in partial.positions
    assert partial.open_lots["AAPL"][0].strategy_id == "unknown_strategy"
