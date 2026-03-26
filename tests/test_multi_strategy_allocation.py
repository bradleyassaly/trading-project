from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from trading_platform.config.loader import load_multi_strategy_portfolio_config
from trading_platform.config.models import (
    MultiStrategyPortfolioConfig,
    MultiStrategySleeveConfig,
)
from trading_platform.paper.models import PaperExecutionPriceSnapshot, PaperSignalSnapshot
from trading_platform.portfolio.multi_strategy import (
    allocate_multi_strategy_portfolio,
    write_multi_strategy_artifacts,
)
from trading_platform.services.target_construction_service import TargetConstructionResult


def _target_result(
    *,
    as_of: str,
    weights: dict[str, float],
    prices: dict[str, float] | None = None,
    diagnostics: dict | None = None,
    scheduled_weights: dict[str, float] | None = None,
    price_snapshots: list[PaperExecutionPriceSnapshot] | None = None,
    extra_diagnostics: dict | None = None,
    signal_snapshot: PaperSignalSnapshot | None = None,
) -> TargetConstructionResult:
    return TargetConstructionResult(
        as_of=as_of,
        scheduled_target_weights=scheduled_weights or weights,
        effective_target_weights=weights,
        latest_prices=prices if prices is not None else {symbol: 100.0 for symbol in weights},
        latest_scores={symbol: 1.0 for symbol in weights},
        target_diagnostics=diagnostics or {},
        skipped_symbols=[],
        signal_snapshot=signal_snapshot or PaperSignalSnapshot(
            asset_returns=pd.DataFrame(),
            scores=pd.DataFrame(),
            closes=pd.DataFrame(),
            skipped_symbols=[],
        ),
        extra_diagnostics=extra_diagnostics or {},
        price_snapshots=price_snapshots or [],
    )


def test_load_multi_strategy_portfolio_config_from_json(tmp_path: Path) -> None:
    config_path = tmp_path / "portfolio.json"
    config_path.write_text(
        json.dumps(
            {
                "gross_leverage_cap": 1.0,
                "net_exposure_cap": 1.0,
                "max_position_weight": 0.4,
                "max_symbol_concentration": 0.5,
                "cash_reserve_pct": 0.1,
                "sleeves": [
                    {
                        "sleeve_name": "core",
                        "preset_name": "xsec_nasdaq100_momentum_v1_deploy",
                        "target_capital_weight": 0.6,
                        "enabled": True,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    config = load_multi_strategy_portfolio_config(config_path)

    assert isinstance(config, MultiStrategyPortfolioConfig)
    assert config.cash_reserve_pct == 0.1
    assert config.enabled_sleeves[0].sleeve_name == "core"


def test_allocate_multi_strategy_combines_non_overlapping_sleeves(monkeypatch) -> None:
    monkeypatch.setattr(
        "trading_platform.portfolio.multi_strategy.build_target_construction_result",
        lambda config: _target_result(
            as_of="2025-01-04",
            weights={"AAPL": 1.0} if config.preset_name == "xsec_nasdaq100_momentum_v1_deploy" else {"MSFT": 1.0},
        ),
    )
    monkeypatch.setattr(
        "trading_platform.portfolio.multi_strategy._paper_config_from_preset",
        lambda preset_name: type("DummyConfig", (), {"preset_name": preset_name})(),
    )
    config = MultiStrategyPortfolioConfig(
        sleeves=[
            MultiStrategySleeveConfig("core", "xsec_nasdaq100_momentum_v1_deploy", 0.6),
            MultiStrategySleeveConfig("satellite", "xsec_nasdaq100_momentum_v1_research", 0.4),
        ],
        max_position_weight=1.0,
        max_symbol_concentration=1.0,
    )

    result = allocate_multi_strategy_portfolio(config)

    assert result.combined_target_weights == {"AAPL": pytest.approx(0.6), "MSFT": pytest.approx(0.4)}
    assert result.summary["raw_enabled_capital_weight_sum"] == pytest.approx(1.0)


def test_allocate_multi_strategy_nets_overlapping_symbol(monkeypatch) -> None:
    monkeypatch.setattr(
        "trading_platform.portfolio.multi_strategy.build_target_construction_result",
        lambda config: _target_result(
            as_of="2025-01-04",
            weights={"AAPL": 1.0} if config.preset_name == "left" else {"AAPL": -1.0},
        ),
    )
    monkeypatch.setattr(
        "trading_platform.portfolio.multi_strategy._paper_config_from_preset",
        lambda preset_name: type("DummyConfig", (), {"preset_name": preset_name})(),
    )
    config = MultiStrategyPortfolioConfig(
        sleeves=[
            MultiStrategySleeveConfig("left", "left", 0.5),
            MultiStrategySleeveConfig("right", "right", 0.5),
        ],
        max_position_weight=1.0,
        max_symbol_concentration=1.0,
    )

    result = allocate_multi_strategy_portfolio(config)

    assert result.combined_target_weights == {}
    overlap = pd.DataFrame(result.symbol_overlap_rows)
    assert overlap.loc[0, "overlap_type"] == "conflict"


def test_allocate_multi_strategy_enforces_max_position_weight(monkeypatch) -> None:
    monkeypatch.setattr(
        "trading_platform.portfolio.multi_strategy.build_target_construction_result",
        lambda config: _target_result(as_of="2025-01-04", weights={"AAPL": 1.0}),
    )
    monkeypatch.setattr(
        "trading_platform.portfolio.multi_strategy._paper_config_from_preset",
        lambda preset_name: type("DummyConfig", (), {"preset_name": preset_name})(),
    )
    config = MultiStrategyPortfolioConfig(
        sleeves=[MultiStrategySleeveConfig("core", "preset", 1.0)],
        max_position_weight=0.25,
        max_symbol_concentration=1.0,
    )

    result = allocate_multi_strategy_portfolio(config)

    assert result.combined_target_weights["AAPL"] == pytest.approx(0.25)
    assert result.summary["symbols_removed_or_clipped"]


def test_allocate_multi_strategy_enforces_gross_and_net_caps(monkeypatch) -> None:
    weights_by_preset = {
        "left": {"AAPL": 1.0, "MSFT": 1.0},
        "right": {"NVDA": 1.0, "AMD": 1.0},
    }
    monkeypatch.setattr(
        "trading_platform.portfolio.multi_strategy.build_target_construction_result",
        lambda config: _target_result(as_of="2025-01-04", weights=weights_by_preset[config.preset_name]),
    )
    monkeypatch.setattr(
        "trading_platform.portfolio.multi_strategy._paper_config_from_preset",
        lambda preset_name: type("DummyConfig", (), {"preset_name": preset_name})(),
    )
    config = MultiStrategyPortfolioConfig(
        sleeves=[
            MultiStrategySleeveConfig("left", "left", 0.5),
            MultiStrategySleeveConfig("right", "right", 0.5),
        ],
        gross_leverage_cap=0.8,
        net_exposure_cap=0.6,
        max_position_weight=1.0,
        max_symbol_concentration=1.0,
    )

    result = allocate_multi_strategy_portfolio(config)

    gross = sum(abs(weight) for weight in result.combined_target_weights.values())
    net = abs(sum(result.combined_target_weights.values()))
    assert gross == pytest.approx(0.6)
    assert net == pytest.approx(0.6)


def test_allocate_multi_strategy_normalizes_capital_weights_when_not_one(monkeypatch) -> None:
    monkeypatch.setattr(
        "trading_platform.portfolio.multi_strategy.build_target_construction_result",
        lambda config: _target_result(as_of="2025-01-04", weights={"AAPL": 1.0}),
    )
    monkeypatch.setattr(
        "trading_platform.portfolio.multi_strategy._paper_config_from_preset",
        lambda preset_name: type("DummyConfig", (), {"preset_name": preset_name})(),
    )
    config = MultiStrategyPortfolioConfig(
        sleeves=[
            MultiStrategySleeveConfig("one", "preset_a", 2.0),
            MultiStrategySleeveConfig("two", "preset_b", 1.0),
        ],
        max_position_weight=1.0,
        max_symbol_concentration=1.0,
    )

    result = allocate_multi_strategy_portfolio(config)

    assert result.summary["normalized_capital_weights"] == {
        "one": pytest.approx(2.0 / 3.0),
        "two": pytest.approx(1.0 / 3.0),
    }


def test_allocate_multi_strategy_respects_disabled_sleeves(monkeypatch) -> None:
    monkeypatch.setattr(
        "trading_platform.portfolio.multi_strategy.build_target_construction_result",
        lambda config: _target_result(as_of="2025-01-04", weights={"AAPL": 1.0}),
    )
    monkeypatch.setattr(
        "trading_platform.portfolio.multi_strategy._paper_config_from_preset",
        lambda preset_name: type("DummyConfig", (), {"preset_name": preset_name})(),
    )
    config = MultiStrategyPortfolioConfig(
        sleeves=[
            MultiStrategySleeveConfig("enabled", "preset_a", 1.0, enabled=True),
            MultiStrategySleeveConfig("disabled", "preset_b", 1.0, enabled=False),
        ],
        max_position_weight=1.0,
        max_symbol_concentration=1.0,
    )

    result = allocate_multi_strategy_portfolio(config)

    assert result.summary["enabled_sleeve_count"] == 1


def test_allocate_multi_strategy_invalid_preset_raises() -> None:
    config = MultiStrategyPortfolioConfig(
        sleeves=[MultiStrategySleeveConfig("bad", "missing", 1.0)],
    )

    with pytest.raises(ValueError, match="Unknown preset"):
        allocate_multi_strategy_portfolio(config)


def test_write_multi_strategy_artifacts_is_deterministic(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "trading_platform.portfolio.multi_strategy.build_target_construction_result",
        lambda config: _target_result(as_of="2025-01-04", weights={"AAPL": 1.0}),
    )
    monkeypatch.setattr(
        "trading_platform.portfolio.multi_strategy._paper_config_from_preset",
        lambda preset_name: type("DummyConfig", (), {"preset_name": preset_name})(),
    )
    config = MultiStrategyPortfolioConfig(
        sleeves=[MultiStrategySleeveConfig("core", "preset", 1.0)],
        max_position_weight=1.0,
        max_symbol_concentration=1.0,
    )

    first = allocate_multi_strategy_portfolio(config)
    second = allocate_multi_strategy_portfolio(config)
    first_paths = write_multi_strategy_artifacts(first, tmp_path / "first")
    second_paths = write_multi_strategy_artifacts(second, tmp_path / "second")

    assert (tmp_path / "first" / "combined_target_weights.csv").read_text(encoding="utf-8") == (
        tmp_path / "second" / "combined_target_weights.csv"
    ).read_text(encoding="utf-8")
    assert Path(first_paths["allocation_summary_json_path"]).read_text(encoding="utf-8") == Path(
        second_paths["allocation_summary_json_path"]
    ).read_text(encoding="utf-8")


def test_allocate_multi_strategy_records_execution_symbol_coverage(monkeypatch) -> None:
    monkeypatch.setattr(
        "trading_platform.portfolio.multi_strategy.build_target_construction_result",
        lambda config: _target_result(
            as_of="2025-01-04",
            weights={"AAPL": 1.0, "MSFT": 1.0},
            prices={"AAPL": 101.0},
            price_snapshots=[
                PaperExecutionPriceSnapshot(
                    symbol="AAPL",
                    decision_timestamp="2025-01-04",
                    historical_price=101.0,
                    latest_price=101.0,
                    final_price_used=101.0,
                    price_source_used="yfinance",
                    fallback_used=False,
                    latest_bar_timestamp="2025-01-04T00:00:00Z",
                    latest_bar_age_seconds=0.0,
                    latest_data_stale=False,
                    latest_data_source="yfinance",
                )
            ],
        ),
    )
    monkeypatch.setattr(
        "trading_platform.portfolio.multi_strategy._paper_config_from_preset",
        lambda preset_name: type("DummyConfig", (), {"preset_name": preset_name, "symbols": ["AAPL", "MSFT"]})(),
    )
    config = MultiStrategyPortfolioConfig(
        sleeves=[MultiStrategySleeveConfig("core", "preset", 1.0)],
        max_position_weight=1.0,
        max_symbol_concentration=1.0,
    )

    result = allocate_multi_strategy_portfolio(config)

    assert result.combined_target_weights == {"AAPL": pytest.approx(1.0)}
    assert result.summary["usable_symbol_count"] == 1
    assert result.summary["skipped_symbol_count"] == 1
    coverage = pd.DataFrame(result.execution_symbol_coverage_rows)
    assert set(coverage["symbol"]) == {"AAPL", "MSFT"}
    assert coverage.loc[coverage["symbol"] == "MSFT", "skip_reason"].iloc[0] == "missing_market_data"


def test_allocate_multi_strategy_uses_latest_close_fallback_when_allowed(monkeypatch) -> None:
    monkeypatch.setattr(
        "trading_platform.portfolio.multi_strategy.build_target_construction_result",
        lambda config: _target_result(
            as_of="2025-01-04",
            weights={"AAPL": 1.0},
            prices={},
            price_snapshots=[
                PaperExecutionPriceSnapshot(
                    symbol="AAPL",
                    decision_timestamp="2025-01-04",
                    historical_price=99.0,
                    latest_price=None,
                    final_price_used=99.0,
                    price_source_used="yfinance",
                    fallback_used=True,
                    latest_bar_timestamp="2025-01-04T00:00:00Z",
                    latest_bar_age_seconds=0.0,
                    latest_data_stale=False,
                    latest_data_source="yfinance",
                )
            ],
        ),
    )
    monkeypatch.setattr(
        "trading_platform.portfolio.multi_strategy._paper_config_from_preset",
        lambda preset_name: type("DummyConfig", (), {"preset_name": preset_name, "symbols": ["AAPL"]})(),
    )
    config = MultiStrategyPortfolioConfig(
        sleeves=[MultiStrategySleeveConfig("core", "preset", 1.0)],
        allow_latest_close_fallback=True,
        max_position_weight=1.0,
        max_symbol_concentration=1.0,
    )

    result = allocate_multi_strategy_portfolio(config)

    assert result.latest_prices["AAPL"] == pytest.approx(99.0)
    assert result.summary["latest_price_source_summary"] == {"yfinance": 1}


def test_allocate_multi_strategy_reports_zero_target_reason_without_fallback(monkeypatch) -> None:
    monkeypatch.setattr(
        "trading_platform.portfolio.multi_strategy.build_target_construction_result",
        lambda config: _target_result(
            as_of="2025-01-04",
            weights={"AAPL": 1.0},
            prices={},
            price_snapshots=[
                PaperExecutionPriceSnapshot(
                    symbol="AAPL",
                    decision_timestamp="2025-01-04",
                    historical_price=99.0,
                    latest_price=None,
                    final_price_used=99.0,
                    price_source_used="yfinance",
                    fallback_used=True,
                    latest_bar_timestamp="2025-01-04T00:00:00Z",
                    latest_bar_age_seconds=0.0,
                    latest_data_stale=False,
                    latest_data_source="yfinance",
                )
            ],
        ),
    )
    monkeypatch.setattr(
        "trading_platform.portfolio.multi_strategy._paper_config_from_preset",
        lambda preset_name: type("DummyConfig", (), {"preset_name": preset_name, "symbols": ["AAPL"]})(),
    )
    config = MultiStrategyPortfolioConfig(
        sleeves=[MultiStrategySleeveConfig("core", "preset", 1.0)],
        allow_latest_close_fallback=False,
        max_position_weight=1.0,
        max_symbol_concentration=1.0,
    )

    result = allocate_multi_strategy_portfolio(config)

    assert result.combined_target_weights == {}
    assert result.summary["usable_symbol_count"] == 0
    assert result.summary["zero_target_reason"] == "missing_latest_price"


def test_allocate_multi_strategy_maps_empty_signal_scores_to_target_drop_reason(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "trading_platform.portfolio.multi_strategy.build_target_construction_result",
        lambda config: _target_result(
            as_of="2025-01-04",
            weights={},
            prices={"AAPL": 100.0},
            diagnostics={"target_construction": {"reason": "no_composite_scores"}},
            extra_diagnostics={
                "selected_signals": [
                    {"signal_family": "fundamental_momentum", "lookback": 5, "horizon": 20}
                ],
                "latest_component_scores": [],
                "latest_composite_scores": [],
            },
            signal_snapshot=PaperSignalSnapshot(
                asset_returns=pd.DataFrame(),
                scores=pd.DataFrame(),
                closes=pd.DataFrame({"AAPL": [100.0]}, index=pd.to_datetime(["2025-01-04"])),
                skipped_symbols=[],
            ),
        ),
    )
    monkeypatch.setattr(
        "trading_platform.portfolio.multi_strategy._paper_config_from_preset",
        lambda preset_name, preset_path=None: type(
            "DummyConfig",
            (),
            {
                "preset_name": preset_name,
                "symbols": ["AAPL"],
                "signal_source": "composite",
                "strategy": "sma_cross",
                "composite_artifact_dir": "artifacts/alpha_research/run_configured",
                "approved_model_state_path": None,
            },
        )(),
    )
    config = MultiStrategyPortfolioConfig(
        sleeves=[MultiStrategySleeveConfig("core", "preset", 1.0, preset_path=str(tmp_path / "generated.json"))],
    )

    result = allocate_multi_strategy_portfolio(config)
    paths = write_multi_strategy_artifacts(result, tmp_path / "alloc")

    assert result.summary["target_drop_stage"] == "signal_scoring"
    assert result.summary["target_drop_reason"] == "empty_signal_scores"
    summary_payload = json.loads(Path(paths["target_generation_summary_path"]).read_text(encoding="utf-8"))
    assert summary_payload["target_drop_reason"] == "empty_signal_scores"


def test_allocate_multi_strategy_maps_liquidity_filter_drop_reason(monkeypatch) -> None:
    monkeypatch.setattr(
        "trading_platform.portfolio.multi_strategy.build_target_construction_result",
        lambda config: _target_result(
            as_of="2025-01-04",
            weights={},
            diagnostics={"target_construction": {"reason": "no_eligible_names"}},
            extra_diagnostics={
                "liquidity_exclusions": [
                    {"symbol": "AAPL"},
                    {"symbol": "MSFT"},
                ]
            },
            signal_snapshot=PaperSignalSnapshot(
                asset_returns=pd.DataFrame(),
                scores=pd.DataFrame({"AAPL": [1.0], "MSFT": [0.5]}, index=pd.to_datetime(["2025-01-04"])),
                closes=pd.DataFrame({"AAPL": [100.0], "MSFT": [200.0]}, index=pd.to_datetime(["2025-01-04"])),
                skipped_symbols=[],
            ),
        ),
    )
    monkeypatch.setattr(
        "trading_platform.portfolio.multi_strategy._paper_config_from_preset",
        lambda preset_name: type("DummyConfig", (), {"preset_name": preset_name, "symbols": ["AAPL", "MSFT"]})(),
    )
    config = MultiStrategyPortfolioConfig(
        sleeves=[MultiStrategySleeveConfig("core", "preset", 1.0)],
    )

    result = allocate_multi_strategy_portfolio(config)

    assert result.summary["target_drop_stage"] == "liquidity_filter"
    assert result.summary["target_drop_reason"] == "all_symbols_failed_liquidity_filter"
