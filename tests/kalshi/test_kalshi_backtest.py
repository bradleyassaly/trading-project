from __future__ import annotations

import json
import math
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from trading_platform.cli.commands.kalshi_full_backtest import cmd_kalshi_full_backtest
from trading_platform.kalshi.backtest import (
    KalshiBacktestResult,
    KalshiBacktester,
    _compute_max_drawdown,
    _compute_sharpe,
)
from trading_platform.kalshi.signals import KALSHI_CALIBRATION_DRIFT
from trading_platform.kalshi.signals_base_rate import KALSHI_BASE_RATE
from trading_platform.kalshi.signals_informed_flow import make_informed_flow_signal_families


def _write_feature_parquet(
    feature_dir: Path,
    ticker: str,
    *,
    close_values: list[float],
    signal_values: list[float],
    close_time: datetime,
    base_rate_edge: float | None = None,
) -> Path:
    timestamps = [close_time - timedelta(hours=48 - (i * 12)) for i in range(len(close_values))]
    payload = {
        "timestamp": timestamps,
        "close": close_values,
        "symbol": [ticker] * len(close_values),
        "calibration_drift_z": signal_values,
        "extreme_volume": [0.0] * len(close_values),
        "tension": [1.0] * len(close_values),
        "price_var_proxy": [price * (100.0 - price) for price in close_values],
    }
    if base_rate_edge is not None:
        payload["base_rate_edge"] = [base_rate_edge] * len(close_values)
    df = pd.DataFrame(payload)
    path = feature_dir / f"{ticker}.parquet"
    df.to_parquet(path, index=False)
    return path


def _write_market_metadata(raw_markets_dir: Path, ticker: str, *, close_time: datetime, category: str) -> None:
    raw_markets_dir.mkdir(parents=True, exist_ok=True)
    (raw_markets_dir / f"{ticker}.json").write_text(
        json.dumps(
            {
                "ticker": ticker,
                "title": f"{ticker} title",
                "category": category,
                "status": "settled",
                "close_time": close_time.isoformat(),
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def _make_resolution_df(tickers: list[str], outcomes: list[float]) -> pd.DataFrame:
    return pd.DataFrame({"ticker": tickers, "resolution_price": outcomes})


def test_backtester_writes_resolved_market_artifacts(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    raw_markets_dir = tmp_path / "raw" / "markets"
    output_dir = tmp_path / "out"
    feature_dir.mkdir(parents=True)

    close_time = datetime(2026, 1, 10, 0, 0, tzinfo=UTC)
    _write_feature_parquet(
        feature_dir,
        "MKT-YES",
        close_values=[40.0, 42.0, 45.0, 47.0, 50.0],
        signal_values=[0.2, 0.4, 0.6, 0.8, 1.0],
        close_time=close_time,
        base_rate_edge=-8.0,
    )
    _write_feature_parquet(
        feature_dir,
        "MKT-NO",
        close_values=[62.0, 60.0, 58.0, 56.0, 54.0],
        signal_values=[-0.2, -0.4, -0.6, -0.8, -1.0],
        close_time=close_time,
        base_rate_edge=10.0,
    )
    _write_market_metadata(raw_markets_dir, "MKT-YES", close_time=close_time, category="economics")
    _write_market_metadata(raw_markets_dir, "MKT-NO", close_time=close_time, category="politics")

    resolution_df = _make_resolution_df(["MKT-YES", "MKT-NO"], [100.0, 0.0])

    backtester = KalshiBacktester(entry_threshold=0.5, entry_offset_hours=12.0)
    results = backtester.run(
        feature_dir=feature_dir,
        resolution_data=resolution_df,
        signal_families=[KALSHI_CALIBRATION_DRIFT, KALSHI_BASE_RATE],
        output_dir=output_dir,
        raw_markets_dir=raw_markets_dir,
    )

    assert len(results) == 2
    assert all(isinstance(result, KalshiBacktestResult) for result in results)
    assert (output_dir / "backtest_results.csv").exists()
    assert (output_dir / "kalshi_backtest_summary.json").exists()
    assert (output_dir / "kalshi_signal_diagnostics.json").exists()
    assert (output_dir / "kalshi_trade_log.jsonl").exists()
    assert (output_dir / "kalshi_backtest_report.md").exists()

    summary = json.loads((output_dir / "kalshi_backtest_summary.json").read_text(encoding="utf-8"))
    assert summary["total_markets_evaluated"] == 2
    assert summary["total_candidate_signals"] == 4
    assert summary["total_executed_trades"] == 4
    assert "execution_assumptions" in summary

    diagnostics = json.loads((output_dir / "kalshi_signal_diagnostics.json").read_text(encoding="utf-8"))
    assert len(diagnostics["by_signal_family"]) == 2
    assert {row["category"] for row in diagnostics["by_category"]} == {"economics", "politics"}
    assert diagnostics["by_confidence_bucket"]

    trade_log_rows = [
        json.loads(line)
        for line in (output_dir / "kalshi_trade_log.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(trade_log_rows) == 4
    assert {"signal_family", "ticker", "predicted_edge", "confidence", "realized_return"} <= set(trade_log_rows[0])


def test_backtester_respects_holding_window_exit(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    raw_markets_dir = tmp_path / "raw" / "markets"
    output_dir = tmp_path / "out"
    feature_dir.mkdir(parents=True)

    close_time = datetime(2026, 2, 1, 0, 0, tzinfo=UTC)
    _write_feature_parquet(
        feature_dir,
        "MKT-HOLD",
        close_values=[40.0, 44.0, 47.0, 51.0, 55.0],
        signal_values=[0.1, 0.3, 0.8, 0.5, 0.2],
        close_time=close_time,
    )
    _write_market_metadata(raw_markets_dir, "MKT-HOLD", close_time=close_time, category="weather")
    resolution_df = _make_resolution_df(["MKT-HOLD"], [100.0])

    backtester = KalshiBacktester(
        entry_threshold=0.5,
        entry_offset_hours=24.0,
        holding_window_hours=6.0,
        entry_slippage_points=1.0,
        exit_slippage_points=1.0,
    )
    backtester.run(
        feature_dir=feature_dir,
        resolution_data=resolution_df,
        signal_families=[KALSHI_CALIBRATION_DRIFT],
        output_dir=output_dir,
        raw_markets_dir=raw_markets_dir,
    )

    trade_row = json.loads((output_dir / "kalshi_trade_log.jsonl").read_text(encoding="utf-8").splitlines()[0])
    assert trade_row["exit_reason"] == "holding_window"
    if trade_row["side"] == "yes":
        assert trade_row["entry_fill_yes"] > trade_row["entry_price_yes"]
    else:
        assert trade_row["entry_fill_yes"] < trade_row["entry_price_yes"]
    assert trade_row["exit_price_yes"] < 100.0


def test_backtester_emits_informed_flow_diagnostics(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    raw_markets_dir = tmp_path / "raw" / "markets"
    output_dir = tmp_path / "out"
    feature_dir.mkdir(parents=True)

    close_time = datetime(2026, 2, 20, 0, 0, tzinfo=UTC)
    df = pd.DataFrame(
        {
            "timestamp": [close_time - timedelta(hours=48), close_time - timedelta(hours=24), close_time - timedelta(hours=12)],
            "close": [42.0, 47.0, 53.0],
            "symbol": ["MKT-FLOW"] * 3,
            "taker_buy_vol": [30.0, 80.0, 90.0],
            "taker_sell_vol": [25.0, 10.0, 5.0],
            "taker_imbalance": [0.09, 0.78, 0.89],
            "imbalance_z": [0.4, 2.2, 2.6],
            "taker_conviction": [0.2, 2.4, 2.8],
            "large_order_direction": [0.0, 0.9, 0.8],
            "large_order_conviction": [0.0, 0.95, 0.85],
            "large_order_count": [0, 2, 3],
            "large_order_volume_ratio": [0.0, 0.45, 0.40],
            "unexplained_move": [1.0, 5.0, 6.0],
            "unexplained_move_z": [0.5, 2.1, 2.4],
            "has_scheduled_catalyst": [0.0, 0.0, 0.0],
            "catalyst_type": ["none", "none", "none"],
        }
    )
    df.to_parquet(feature_dir / "MKT-FLOW.parquet", index=False)
    _write_market_metadata(raw_markets_dir, "MKT-FLOW", close_time=close_time, category="politics")
    resolution_df = _make_resolution_df(["MKT-FLOW"], [100.0])

    informed_flow_families = make_informed_flow_signal_families()
    backtester = KalshiBacktester(entry_threshold=0.5, entry_offset_hours=12.0)
    backtester.run(
        feature_dir=feature_dir,
        resolution_data=resolution_df,
        signal_families=informed_flow_families[:2],
        output_dir=output_dir,
        raw_markets_dir=raw_markets_dir,
    )

    diagnostics = json.loads((output_dir / "kalshi_signal_diagnostics.json").read_text(encoding="utf-8"))
    candidate_summary = {row["signal_family"]: row for row in diagnostics["candidate_signal_summary"]}
    assert candidate_summary["kalshi_taker_imbalance"]["signal_count"] == 1
    assert candidate_summary["kalshi_taker_imbalance"]["avg_confidence"] > 0.0
    assert "supporting_feature_summaries" in diagnostics
    assert "kalshi_taker_imbalance" in diagnostics["supporting_feature_summaries"]
    assert "imbalance_z" in diagnostics["supporting_feature_summaries"]["kalshi_taker_imbalance"]

    trade_row = json.loads((output_dir / "kalshi_trade_log.jsonl").read_text(encoding="utf-8").splitlines()[0])
    assert "supporting_features" in trade_row
    assert "imbalance_z" in trade_row["supporting_features"] or "large_order_direction" in trade_row["supporting_features"]


def test_backtester_skips_unresolved_markets(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    feature_dir.mkdir(parents=True)
    close_time = datetime(2026, 3, 1, 0, 0, tzinfo=UTC)
    _write_feature_parquet(
        feature_dir,
        "MKT-UNRESOLVED",
        close_values=[40.0, 41.0, 42.0, 43.0, 44.0],
        signal_values=[0.2, 0.3, 0.4, 0.5, 0.6],
        close_time=close_time,
    )

    backtester = KalshiBacktester(entry_threshold=0.5)
    results = backtester.run(
        feature_dir=feature_dir,
        resolution_data=pd.DataFrame(columns=["ticker", "resolution_price"]),
        signal_families=[KALSHI_CALIBRATION_DRIFT],
        output_dir=tmp_path / "out",
    )

    assert results[0].markets_evaluated == 0
    assert results[0].n_trades == 0


def test_kalshi_full_backtest_command_runs_from_yaml_config(tmp_path: Path) -> None:
    feature_dir = tmp_path / "features"
    raw_markets_dir = tmp_path / "raw" / "markets"
    output_dir = tmp_path / "artifacts"
    feature_dir.mkdir(parents=True)

    close_time = datetime(2026, 2, 15, 0, 0, tzinfo=UTC)
    _write_feature_parquet(
        feature_dir,
        "MKT-CMD",
        close_values=[41.0, 43.0, 44.0, 46.0, 48.0],
        signal_values=[0.2, 0.4, 0.7, 0.9, 1.1],
        close_time=close_time,
        base_rate_edge=-6.0,
    )
    _write_market_metadata(raw_markets_dir, "MKT-CMD", close_time=close_time, category="economics")
    resolution_path = tmp_path / "resolution.csv"
    _make_resolution_df(["MKT-CMD"], [100.0]).to_csv(resolution_path, index=False)

    config_path = tmp_path / "kalshi_research.yaml"
    config_path.write_text(
        f"""
paths:
  feature_dir: {feature_dir.as_posix()}
  raw_markets_dir: {raw_markets_dir.as_posix()}
  resolution_data_path: {resolution_path.as_posix()}
  output_dir: {output_dir.as_posix()}
signals:
  families:
    - kalshi_calibration_drift
    - kalshi_base_rate
backtest:
  entry_threshold: 0.5
  entry_timing_mode: hours_before_close
  entry_offset_hours: 12
  holding_window_hours: 6
  entry_slippage_points: 1
  exit_slippage_points: 1
  signal_probability_scale: 6
""".strip(),
        encoding="utf-8",
    )

    args = type(
        "Args",
        (),
        {
            "config": str(config_path),
            "feature_dir": None,
            "resolution_data": None,
            "output_dir": None,
            "raw_markets_dir": None,
            "entry_threshold": None,
            "long_only": False,
            "entry_timing_mode": None,
            "entry_offset_hours": None,
            "holding_window_hours": None,
            "entry_slippage_points": None,
            "exit_slippage_points": None,
            "signal_probability_scale": None,
        },
    )()

    cmd_kalshi_full_backtest(args)

    assert (output_dir / "kalshi_backtest_summary.json").exists()
    assert (output_dir / "kalshi_signal_diagnostics.json").exists()
    assert (output_dir / "kalshi_trade_log.jsonl").exists()
    assert (output_dir / "kalshi_backtest_report.md").exists()
    assert (output_dir / "full_backtest_results.csv").exists()
    assert (output_dir / "full_backtest_summary.md").exists()

    summary = json.loads((output_dir / "kalshi_backtest_summary.json").read_text(encoding="utf-8"))
    assert summary["total_markets_evaluated"] == 1
    assert summary["execution_assumptions"]["entry_offset_hours"] == 12.0
    assert summary["execution_assumptions"]["holding_window_hours"] == 6.0


def test_compute_sharpe_normal() -> None:
    returns = pd.Series([1.0, 2.0, 3.0, 1.5, 2.5])
    sharpe = _compute_sharpe(returns)
    assert sharpe > 0.0
    assert not math.isnan(sharpe)


def test_compute_sharpe_zero_std_returns_zero() -> None:
    returns = pd.Series([1.0, 1.0, 1.0])
    assert _compute_sharpe(returns) == 0.0


def test_compute_max_drawdown() -> None:
    equity = pd.Series([100.0, 120.0, 90.0, 110.0, 80.0])
    dd = _compute_max_drawdown(equity)
    assert dd < 0.0
    assert dd == pytest.approx(-1.0 / 3.0, abs=0.01)


def test_compute_max_drawdown_monotonic_increase() -> None:
    equity = pd.Series([10.0, 20.0, 30.0, 40.0])
    dd = _compute_max_drawdown(equity)
    assert dd == 0.0
