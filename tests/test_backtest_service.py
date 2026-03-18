from __future__ import annotations

from trading_platform.config.models import BacktestConfig
from trading_platform.services.backtest_service import run_backtest_workflow


def test_run_backtest_workflow_runs_backtest_and_logs_experiment(monkeypatch) -> None:
    fake_stats = {
        "Return [%]": 12.3,
        "Sharpe Ratio": 1.5,
    }

    captured: dict[str, object] = {}

    def fake_run_backtest(**kwargs):
        captured["backtest_kwargs"] = kwargs
        return fake_stats

    def fake_log_experiment(stats):
        captured["logged_stats"] = stats
        return "exp-123"

    monkeypatch.setattr(
        "trading_platform.services.backtest_service.run_backtest",
        fake_run_backtest,
    )
    monkeypatch.setattr(
        "trading_platform.services.backtest_service.log_experiment",
        fake_log_experiment,
    )

    config = BacktestConfig(
        symbol="AAPL",
        strategy="sma_cross",
        fast=20,
        slow=50,
        cash=10000,
        commission=0.001,
    )

    out = run_backtest_workflow(config=config)

    assert out["stats"] == fake_stats
    assert out["experiment_id"] == "exp-123"

    kwargs = captured["backtest_kwargs"]
    assert kwargs["symbol"] == "AAPL"
    assert kwargs["strategy"] == "sma_cross"
    assert kwargs["fast"] == 20
    assert kwargs["slow"] == 50
    assert kwargs["cash"] == 10000
    assert kwargs["commission"] == 0.001
    assert captured["logged_stats"] == fake_stats