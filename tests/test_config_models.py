from __future__ import annotations

import pytest

from trading_platform.config.models import (
    BacktestConfig,
    FeatureConfig,
    IngestConfig,
    MultiStrategyPortfolioConfig,
    MultiStrategySleeveConfig,
    ParameterSweepConfig,
    ResearchWorkflowConfig,
    WalkForwardConfig,
)


def test_ingest_config_rejects_empty_symbol() -> None:
    with pytest.raises(ValueError, match="non-empty"):
        IngestConfig(symbol="")


def test_ingest_config_rejects_bad_interval() -> None:
    with pytest.raises(ValueError, match="Unsupported interval"):
        IngestConfig(symbol="AAPL", interval="2d")


def test_feature_config_rejects_unknown_feature_group() -> None:
    with pytest.raises(ValueError, match="Unknown feature groups"):
        FeatureConfig(symbol="AAPL", feature_groups=["fake_group"])


def test_backtest_config_rejects_nonpositive_cash() -> None:
    with pytest.raises(ValueError, match="cash must be > 0"):
        BacktestConfig(symbol="AAPL", strategy="buy_and_hold", cash=0)


def test_backtest_config_rejects_negative_commission() -> None:
    with pytest.raises(ValueError, match="commission must be >?= 0|commission must be >= 0"):
        BacktestConfig(symbol="AAPL", strategy="buy_and_hold", commission=-0.1)


def test_backtest_config_rejects_sma_cross_without_fast_slow() -> None:
    with pytest.raises(ValueError, match="requires both fast and slow"):
        BacktestConfig(symbol="AAPL", strategy="sma_cross")


def test_backtest_config_rejects_fast_greater_than_or_equal_to_slow() -> None:
    with pytest.raises(ValueError, match="fast must be < slow"):
        BacktestConfig(symbol="AAPL", strategy="sma_cross", fast=50, slow=20)


def test_backtest_config_accepts_valid_sma_cross() -> None:
    config = BacktestConfig(
        symbol="AAPL",
        strategy="sma_cross",
        fast=20,
        slow=50,
        cash=10000,
        commission=0.001,
    )
    assert config.fast == 20
    assert config.slow == 50


def test_research_workflow_config_rejects_unknown_strategy() -> None:
    with pytest.raises(ValueError, match="Unsupported strategy"):
        ResearchWorkflowConfig(symbol="AAPL", strategy="fake")


def test_research_workflow_config_accepts_valid_config() -> None:
    config = ResearchWorkflowConfig(
        symbol="AAPL",
        strategy="sma_cross",
        fast=20,
        slow=50,
    )
    assert config.symbol == "AAPL"

def test_parameter_sweep_config_rejects_empty_fast_slow_for_sma() -> None:
    with pytest.raises(ValueError, match="requires fast_values and slow_values"):
        ParameterSweepConfig(
            symbol="AAPL",
            strategy="sma_cross",
            fast_values=[],
            slow_values=[],
        )

def test_walk_forward_config_rejects_invalid_windows() -> None:
    with pytest.raises(ValueError, match="train_window_bars must be > 0"):
        WalkForwardConfig(
            symbol="AAPL",
            strategy="sma_cross",
            fast=10,
            slow=20,
            train_window_bars=0,
        )

def test_walk_forward_config_rejects_invalid_windows() -> None:
    with pytest.raises(ValueError, match="train_window_bars must be > 0"):
        WalkForwardConfig(
            symbol="AAPL",
            strategy="sma_cross",
            fast=10,
            slow=20,
            train_window_bars=0,
        )


def test_walk_forward_config_rejects_optimize_mode_without_sweep_values() -> None:
    with pytest.raises(ValueError, match="requires fast_values and slow_values"):
        WalkForwardConfig(
            symbol="AAPL",
            strategy="sma_cross",
            walk_forward_mode="optimize",
            fast_values=[],
            slow_values=[],
        )


def test_multi_strategy_config_rejects_empty_sleeves() -> None:
    with pytest.raises(ValueError, match="at least one sleeve"):
        MultiStrategyPortfolioConfig(sleeves=[])


def test_multi_strategy_sleeve_rejects_weight_outside_bounds() -> None:
    with pytest.raises(ValueError, match="<= max_capital_weight"):
        MultiStrategySleeveConfig(
            sleeve_name="core",
            preset_name="xsec_nasdaq100_momentum_v1_deploy",
            target_capital_weight=0.8,
            max_capital_weight=0.5,
        )
