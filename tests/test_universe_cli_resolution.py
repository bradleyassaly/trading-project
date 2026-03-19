from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from trading_platform.cli.commands.daily_paper_job import _resolve_symbols as resolve_daily_symbols
from trading_platform.cli.commands.paper_run import _resolve_symbols as resolve_paper_symbols


def test_resolve_symbols_from_universe() -> None:
    args = SimpleNamespace(symbols=None, universe="test_largecap")
    assert resolve_paper_symbols(args) == ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL"]
    assert resolve_daily_symbols(args) == ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL"]


def test_resolve_symbols_raises_when_both_symbols_and_universe_provided() -> None:
    args = SimpleNamespace(symbols=["AAPL"], universe="test_largecap")
    with pytest.raises(ValueError, match="exactly one"):
        resolve_paper_symbols(args)
    with pytest.raises(ValueError, match="exactly one"):
        resolve_daily_symbols(args)


def test_resolve_symbols_raises_when_neither_symbols_nor_universe_provided() -> None:
    args = SimpleNamespace(symbols=None, universe=None)
    with pytest.raises(ValueError, match="exactly one"):
        resolve_paper_symbols(args)
    with pytest.raises(ValueError, match="exactly one"):
        resolve_daily_symbols(args)
