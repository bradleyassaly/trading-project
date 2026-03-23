from __future__ import annotations

import csv
from argparse import Namespace
from pathlib import Path

from trading_platform.cli.commands.regime_detect import cmd_regime_detect
from trading_platform.cli.commands.regime_show import cmd_regime_show
from trading_platform.regime.service import (
    MarketRegimePolicyConfig,
    detect_market_regime,
    infer_strategy_regime_compatibility,
    load_market_regime,
)


def _write_prices(path: Path, rows: list[tuple[str, float]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["date", "close"])
        writer.writerows(rows)
    return path


def test_detect_market_regime_classifies_trend(tmp_path: Path) -> None:
    input_path = _write_prices(
        tmp_path / "prices.csv",
        [(f"2026-01-{day:02d}", float(100 + day)) for day in range(1, 31)],
    )

    result = detect_market_regime(
        input_path=input_path,
        output_dir=tmp_path / "regime",
        policy=MarketRegimePolicyConfig(
            short_return_window=5,
            long_return_window=10,
            volatility_window=10,
            trend_return_threshold=0.03,
        ),
    )

    payload = load_market_regime(tmp_path / "regime")
    assert result["regime_label"] == "trend"
    assert payload["latest"]["regime_label"] == "trend"


def test_infer_strategy_regime_compatibility_from_family() -> None:
    assert infer_strategy_regime_compatibility(signal_family="momentum") == ["trend", "low_vol"]
    assert infer_strategy_regime_compatibility(signal_family="unknown") == ["all_weather"]


def test_regime_cli_commands_write_outputs(tmp_path: Path, capsys) -> None:
    input_path = _write_prices(
        tmp_path / "prices.csv",
        [(f"2026-02-{day:02d}", float(100 + day)) for day in range(1, 21)],
    )

    cmd_regime_detect(
        Namespace(
            input=str(input_path),
            policy_config=None,
            output_dir=str(tmp_path / "regime"),
        )
    )
    cmd_regime_show(Namespace(regime=str(tmp_path / "regime")))

    captured = capsys.readouterr().out
    assert "Regime:" in captured
    assert "Regime JSON:" in captured
