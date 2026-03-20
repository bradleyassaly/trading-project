from __future__ import annotations

import pytest

from trading_platform.universes.registry import get_universe_symbols, list_universes


def test_list_universes_contains_expected_names() -> None:
    universes = list_universes()
    assert "dow30" in universes
    assert "magnificent7" in universes
    assert "test_largecap" in universes
    assert "sp500" in universes
    assert "nasdaq100" in universes
    assert "liquid_top_100" in universes


def test_get_universe_symbols_returns_expected_members() -> None:
    symbols = get_universe_symbols("magnificent7")
    assert "AAPL" in symbols
    assert "MSFT" in symbols
    assert "NVDA" in symbols
    assert len(symbols) == 7


def test_get_real_universes_returns_expected_members() -> None:
    sp500 = get_universe_symbols("sp500")
    nasdaq100 = get_universe_symbols("nasdaq100")
    liquid = get_universe_symbols("liquid_top_100")

    assert "AAPL" in sp500
    assert "MSFT" in sp500
    assert "NVDA" in sp500
    assert len(sp500) >= 450

    assert "AAPL" in nasdaq100
    assert "MSFT" in nasdaq100
    assert "AMZN" in nasdaq100
    assert len(nasdaq100) >= 90

    assert "AAPL" in liquid
    assert "SPY" not in liquid
    assert len(liquid) >= 90


def test_get_universe_symbols_raises_for_unknown_universe() -> None:
    with pytest.raises(ValueError, match="Unknown universe"):
        get_universe_symbols("not_a_real_universe")
