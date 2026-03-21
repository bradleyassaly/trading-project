from __future__ import annotations

import json
from types import SimpleNamespace

import pandas as pd

from trading_platform.services.signal_validation_service import (
    SignalValidationConfig,
    run_signal_validation,
)


def _feature_frame(periods: int = 2200) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": pd.date_range("2018-01-01", periods=periods, freq="B"),
            "Close": [100.0 + idx * 0.1 for idx in range(periods)],
        }
    )


def _fake_result(*, total_return: float, sharpe: float, max_drawdown: float):
    timeseries = pd.DataFrame({"effective_position": [0.0, 1.0, 1.0, 0.0]})
    return SimpleNamespace(
        simulation=SimpleNamespace(
            summary={
                "total_return": total_return,
                "sharpe": sharpe,
                "max_drawdown": max_drawdown,
            },
            timeseries=timeseries,
        )
    )


def test_run_signal_validation_writes_per_symbol_summary(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(
        "trading_platform.services.signal_validation_service.load_feature_frame",
        lambda symbol: _feature_frame(),
    )

    def fake_run_vectorized_research_on_df(
        *,
        df,
        symbol,
        strategy,
        fast=20,
        slow=100,
        lookback=20,
        **_,
    ):
        score = (fast or 0) * 0.8 - (slow or 0) * 0.1 + len(df) / 800.0
        return _fake_result(
            total_return=score / 100.0,
            sharpe=max(score / 10.0, 0.1),
            max_drawdown=-0.12,
        )

    monkeypatch.setattr(
        "trading_platform.services.signal_validation_service.run_vectorized_research_on_df",
        fake_run_vectorized_research_on_df,
    )

    outputs = run_signal_validation(
        SignalValidationConfig(
            symbols=["AAPL"],
            strategy="sma_cross",
            fast=20,
            slow=50,
            fast_values=[10, 20],
            slow_values=[50, 100],
            train_years=1,
            test_years=1,
            min_train_rows=120,
            min_test_rows=60,
            output_dir=tmp_path,
        )
    )

    leaderboard = outputs["leaderboard"]
    assert list(leaderboard["symbol"]) == ["AAPL"]
    assert leaderboard.iloc[0]["selected_fast"] == 20
    assert leaderboard.iloc[0]["selected_slow"] == 50
    assert leaderboard.iloc[0]["status"] == "pass"

    summary_path = tmp_path / "per_symbol" / "AAPL_summary.csv"
    assert summary_path.exists()

    report = json.loads((tmp_path / "validation_report.json").read_text(encoding="utf-8"))
    assert report["summary"]["pass_count"] == 1
    assert report["reports"][0]["symbol"] == "AAPL"


def test_run_signal_validation_aggregates_universe_results(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(
        "trading_platform.services.signal_validation_service.load_feature_frame",
        lambda symbol: _feature_frame(),
    )

    def fake_run_vectorized_research_on_df(
        *,
        df,
        symbol,
        strategy,
        fast=20,
        slow=100,
        lookback=20,
        **_,
    ):
        symbol_bonus = 6.0 if symbol == "AAPL" else 2.0
        score = symbol_bonus + (fast or lookback or 0) * 0.4 - (slow or 0) * 0.03 + len(df) / 1200.0
        return _fake_result(
            total_return=score / 100.0,
            sharpe=max(score / 8.0, 0.1),
            max_drawdown=-0.1,
        )

    monkeypatch.setattr(
        "trading_platform.services.signal_validation_service.run_vectorized_research_on_df",
        fake_run_vectorized_research_on_df,
    )

    outputs = run_signal_validation(
        SignalValidationConfig(
            symbols=["MSFT", "AAPL"],
            strategy="sma_cross",
            fast=20,
            slow=50,
            fast_values=[20],
            slow_values=[50],
            train_years=1,
            test_years=1,
            min_train_rows=120,
            min_test_rows=60,
            output_dir=tmp_path,
        )
    )

    leaderboard = outputs["leaderboard"]
    assert list(leaderboard["symbol"]) == ["AAPL", "MSFT"]

    leaderboard_path = tmp_path / "validation_leaderboard.csv"
    assert leaderboard_path.exists()

    leaderboard_df = pd.read_csv(leaderboard_path)
    assert list(leaderboard_df["symbol"]) == ["AAPL", "MSFT"]


def test_run_signal_validation_handles_missing_and_insufficient_data(monkeypatch, tmp_path) -> None:
    def fake_load_feature_frame(symbol: str) -> pd.DataFrame:
        if symbol == "MISSING":
            raise FileNotFoundError("Feature file not found")
        return _feature_frame(periods=50)

    monkeypatch.setattr(
        "trading_platform.services.signal_validation_service.load_feature_frame",
        fake_load_feature_frame,
    )
    monkeypatch.setattr(
        "trading_platform.services.signal_validation_service.run_vectorized_research_on_df",
        lambda **_: _fake_result(total_return=0.05, sharpe=0.5, max_drawdown=-0.1),
    )

    outputs = run_signal_validation(
        SignalValidationConfig(
            symbols=["MISSING", "SHORT"],
            strategy="sma_cross",
            fast=20,
            slow=50,
            fast_values=[20],
            slow_values=[50],
            train_years=1,
            test_years=1,
            min_train_rows=120,
            min_test_rows=60,
            output_dir=tmp_path,
        )
    )

    leaderboard = outputs["leaderboard"]
    assert set(leaderboard["status"]) == {"fail"}
    assert "FileNotFoundError" in leaderboard.loc[leaderboard["symbol"] == "MISSING", "error"].iloc[0]
    assert "Insufficient data" in leaderboard.loc[leaderboard["symbol"] == "SHORT", "error"].iloc[0]

    report = json.loads((tmp_path / "validation_report.json").read_text(encoding="utf-8"))
    assert report["summary"]["fail_count"] == 2
