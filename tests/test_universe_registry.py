from __future__ import annotations

import pytest

from trading_platform.universes.registry import get_universe_symbols, list_universes


def test_list_universes_contains_expected_names() -> None:
    universes = list_universes()
    assert "dow30" in universes
    assert "magnificent7" in universes
    assert "test_largecap" in universes


def test_get_universe_symbols_returns_expected_members() -> None:
    symbols = get_universe_symbols("magnificent7")
    assert "AAPL" in symbols
    assert "MSFT" in symbols
    assert "NVDA" in symbols
    assert len(symbols) == 7


def test_get_universe_symbols_raises_for_unknown_universe() -> None:
    with pytest.raises(ValueError, match="Unknown universe"):
        get_universe_symbols("not_a_real_universe")
