from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from trading_platform.cli.commands.ingest import cmd_ingest
from trading_platform.config.models import IngestConfig


def test_cmd_ingest_accepts_single_symbol_via_symbols(monkeypatch, capsys, tmp_path: Path) -> None:
    captured: list[IngestConfig] = []

    def fake_run_ingest(*, config):
        captured.append(config)
        return tmp_path / f"{config.symbol}.parquet"

    monkeypatch.setattr(
        "trading_platform.cli.commands.ingest.run_ingest",
        fake_run_ingest,
    )

    args = SimpleNamespace(symbols=["aapl"], universe=None, start="2024-01-01")
    cmd_ingest(args)

    assert len(captured) == 1
    assert captured[0] == IngestConfig(
        symbol="AAPL",
        start="2024-01-01",
        end=None,
        interval="1d",
    )
    assert "Ingesting 1 symbol(s): AAPL" in capsys.readouterr().out


def test_cmd_ingest_accepts_multiple_symbols_via_symbols(monkeypatch, tmp_path: Path) -> None:
    captured: list[IngestConfig] = []

    monkeypatch.setattr(
        "trading_platform.cli.commands.ingest.run_ingest",
        lambda *, config: captured.append(config) or (tmp_path / f"{config.symbol}.parquet"),
    )

    args = SimpleNamespace(
        symbols=["AAPL", "msft"],
        universe=None,
        start="2024-01-01",
        end="2024-12-31",
        interval="1d",
    )
    cmd_ingest(args)

    assert captured == [
        IngestConfig(symbol="AAPL", start="2024-01-01", end="2024-12-31", interval="1d"),
        IngestConfig(symbol="MSFT", start="2024-01-01", end="2024-12-31", interval="1d"),
    ]


def test_cmd_ingest_accepts_named_universe(monkeypatch, tmp_path: Path) -> None:
    captured: list[IngestConfig] = []

    monkeypatch.setattr(
        "trading_platform.cli.commands.ingest.run_ingest",
        lambda *, config: captured.append(config) or (tmp_path / f"{config.symbol}.parquet"),
    )

    args = SimpleNamespace(
        symbols=None,
        universe="test_largecap",
        start="2024-01-01",
        end=None,
        interval="1d",
    )
    cmd_ingest(args)

    assert [config.symbol for config in captured] == ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL"]
    assert all(config.start == "2024-01-01" for config in captured)
    assert all(config.end is None for config in captured)
    assert all(config.interval == "1d" for config in captured)
