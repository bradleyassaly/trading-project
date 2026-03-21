from __future__ import annotations

import csv
from pathlib import Path
from types import SimpleNamespace

from trading_platform.cli.commands.features import cmd_features


def test_cmd_features_accepts_named_universe(monkeypatch, tmp_path: Path) -> None:
    captured_symbols: list[str] = []

    def fake_run_feature_build(*, config):
        captured_symbols.append(config.symbol)
        return tmp_path / f"{config.symbol}.parquet"

    monkeypatch.setattr(
        "trading_platform.cli.commands.features.run_feature_build",
        fake_run_feature_build,
    )

    args = SimpleNamespace(
        symbols=None,
        universe="test_largecap",
        feature_groups=["trend", "momentum"],
    )

    cmd_features(args)

    assert captured_symbols == ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL"]


def test_cmd_features_accepts_explicit_symbols(monkeypatch, tmp_path: Path) -> None:
    captured_symbols: list[str] = []

    def fake_run_feature_build(*, config):
        captured_symbols.append(config.symbol)
        return tmp_path / f"{config.symbol}.parquet"

    monkeypatch.setattr(
        "trading_platform.cli.commands.features.run_feature_build",
        fake_run_feature_build,
    )

    args = SimpleNamespace(
        symbols=["aapl", "msft"],
        universe=None,
        feature_groups=["trend"],
    )

    cmd_features(args)

    assert captured_symbols == ["AAPL", "MSFT"]


def test_cmd_features_continues_after_symbol_failure_and_writes_summary(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    captured_symbols: list[str] = []
    failure_report = tmp_path / "feature_failures.csv"

    def fake_run_feature_build(*, config):
        captured_symbols.append(config.symbol)
        if config.symbol == "MSFT":
            raise FileNotFoundError("missing normalized data")
        return tmp_path / f"{config.symbol}.parquet"

    monkeypatch.setattr(
        "trading_platform.cli.commands.features.run_feature_build",
        fake_run_feature_build,
    )

    args = SimpleNamespace(
        symbols=["AAPL", "MSFT", "NVDA"],
        universe=None,
        feature_groups=["trend"],
        failure_report=failure_report,
    )

    cmd_features(args)

    output = capsys.readouterr().out

    assert captured_symbols == ["AAPL", "MSFT", "NVDA"]
    assert "[FAIL] MSFT: FileNotFoundError: missing normalized data" in output
    assert "[SUMMARY] feature build completed: 2 succeeded, 1 failed" in output
    assert "[SUMMARY] failed symbols: MSFT" in output
    assert failure_report.exists()

    rows = list(csv.DictReader(failure_report.open("r", encoding="utf-8")))
    assert rows == [
        {
            "symbol": "MSFT",
            "error_type": "FileNotFoundError",
            "error_message": "missing normalized data",
        }
    ]
