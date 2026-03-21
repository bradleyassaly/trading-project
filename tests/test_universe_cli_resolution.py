from __future__ import annotations

from types import SimpleNamespace

import pytest

from trading_platform.cli.common import resolve_symbols


def test_resolve_single_symbol_from_symbols() -> None:
    args = SimpleNamespace(symbols=["aapl"], universe=None)
    assert resolve_symbols(args) == ["AAPL"]


def test_resolve_multiple_symbols_from_symbols() -> None:
    args = SimpleNamespace(symbols=["AAPL", "msft", "AAPL"], universe=None)
    assert resolve_symbols(args) == ["AAPL", "MSFT"]


def test_resolve_symbols_from_universe() -> None:
    args = SimpleNamespace(symbols=None, universe="test_largecap")
    assert resolve_symbols(args) == ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL"]


def test_resolve_symbols_raises_when_both_symbols_and_universe_provided() -> None:
    args = SimpleNamespace(symbols=["AAPL"], universe="test_largecap")
    with pytest.raises(SystemExit, match="exactly one"):
        resolve_symbols(args)


def test_resolve_symbols_raises_when_neither_symbols_nor_universe_provided() -> None:
    args = SimpleNamespace(symbols=None, universe=None)
    with pytest.raises(SystemExit, match="exactly one"):
        resolve_symbols(args)
