from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from trading_platform.cli.commands.walkforward import cmd_walkforward


def _prepared_payload(symbol: str, periods: int) -> dict[str, object]:
    timestamps = pd.date_range("2020-01-01", periods=periods, freq="B")
    return {
        "df": pd.DataFrame(
            {
                "timestamp": timestamps,
                "Open": [100.0 + idx for idx in range(periods)],
                "High": [101.0 + idx for idx in range(periods)],
                "Low": [99.0 + idx for idx in range(periods)],
                "Close": [100.5 + idx for idx in range(periods)],
                "Volume": [1000 for _ in range(periods)],
            }
        ),
        "path": Path(f"{symbol}.parquet"),
        "date_col": "timestamp",
        "effective_start": "2020-01-01",
        "effective_end": pd.Timestamp(timestamps.max()).date().isoformat(),
        "rows": periods,
    }


def test_cmd_walkforward_generates_multiple_windows_and_summary(monkeypatch, tmp_path: Path, capsys) -> None:
    monkeypatch.setattr(
        "trading_platform.cli.commands.walkforward.prepare_research_frame",
        lambda symbol, start=None, end=None: _prepared_payload(symbol, 40),
    )

    def fake_run_backtest_on_df(*, df, symbol, strategy, fast=20, slow=100, lookback=20, **kwargs):
        score = (fast or lookback or 0) - (slow or 0) * 0.01 + len(df) * 0.01
        return {
            "Return [%]": score,
            "Sharpe Ratio": score / 10.0,
            "Max. Drawdown [%]": -5.0,
            "trade_count": 2,
            "entry_count": 1,
            "exit_count": 1,
            "percent_time_in_market": 50.0,
            "average_holding_period_bars": 2.0,
            "final_position_size": 0.0,
            "ended_in_cash": True,
        }

    monkeypatch.setattr(
        "trading_platform.cli.commands.walkforward.run_backtest_on_df",
        fake_run_backtest_on_df,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.walkforward.save_walkforward_return_plot",
        lambda df, output_path: output_path.with_suffix(".returns.png"),
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.walkforward.save_walkforward_param_plot",
        lambda df, output_path: output_path.with_suffix(".params.png"),
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.walkforward.save_walkforward_html_report",
        lambda **kwargs: Path(str(kwargs["output_path"]).replace(".csv", ".html")),
    )

    output_path = tmp_path / "walkforward.csv"
    args = argparse.Namespace(
        symbols=["AAPL"],
        universe=None,
        strategy="sma_cross",
        start="2020-01-01",
        end="2020-02-25",
        train_years=5,
        test_years=1,
        train_bars=10,
        test_bars=5,
        step_bars=5,
        train_period_days=None,
        test_period_days=None,
        step_days=None,
        min_train_rows=5,
        min_test_rows=3,
        fast_values=[10, 20],
        slow_values=[50, 100],
        lookback_values=None,
        select_by="Sharpe Ratio",
        cash=10_000.0,
        commission=0.001,
        engine="legacy",
        output=str(output_path),
    )

    cmd_walkforward(args)

    window_df = pd.read_csv(output_path)
    summary_df = pd.read_csv(output_path.with_name("walkforward_summary.csv"))

    assert len(window_df) >= 5
    assert (window_df["window_status"] == "completed").any()
    assert set(
        [
            "window_index",
            "window_status",
            "skip_reason",
            "benchmark_return_pct",
            "excess_return_pct",
            "top_candidate_scores",
            "window_units",
            "train_rows",
            "test_rows",
            "trade_count",
            "percent_time_in_market",
        ]
    ).issubset(window_df.columns)
    assert set(summary_df.columns).issuperset(
        [
            "completed_windows",
            "skipped_windows",
            "percent_positive_windows",
            "worst_excess_return_pct",
            "best_excess_return_pct",
            "total_trade_count",
            "mean_percent_time_in_market",
            "mean_average_holding_period_bars",
        ]
    )

    stdout = capsys.readouterr().out
    assert "candidate_windows=" in stdout
    assert "completed_windows=" in stdout
    assert "skipped_windows=" in stdout
    assert "window_units=bars" in stdout
    assert "trade_count=2" in stdout


def test_cmd_walkforward_records_skipped_windows_for_insufficient_data(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "trading_platform.cli.commands.walkforward.prepare_research_frame",
        lambda symbol, start=None, end=None: _prepared_payload(symbol, 16),
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.walkforward.run_backtest_on_df",
        lambda **kwargs: {
            "Return [%]": 2.0,
            "Sharpe Ratio": 0.5,
            "Max. Drawdown [%]": -1.0,
            "trade_count": 0,
            "entry_count": 0,
            "exit_count": 0,
            "percent_time_in_market": 0.0,
            "average_holding_period_bars": None,
            "final_position_size": 0.0,
            "ended_in_cash": True,
        },
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.walkforward.save_walkforward_return_plot",
        lambda df, output_path: output_path.with_suffix(".returns.png"),
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.walkforward.save_walkforward_param_plot",
        lambda df, output_path: output_path.with_suffix(".params.png"),
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.walkforward.save_walkforward_html_report",
        lambda **kwargs: Path(str(kwargs["output_path"]).replace(".csv", ".html")),
    )

    output_path = tmp_path / "walkforward.csv"
    args = argparse.Namespace(
        symbols=["AAPL"],
        universe=None,
        strategy="sma_cross",
        start="2020-01-01",
        end="2020-01-31",
        train_years=5,
        test_years=1,
        train_bars=10,
        test_bars=5,
        step_bars=3,
        train_period_days=None,
        test_period_days=None,
        step_days=None,
        min_train_rows=8,
        min_test_rows=4,
        fast_values=[10],
        slow_values=[50],
        lookback_values=None,
        select_by="Sharpe Ratio",
        cash=10_000.0,
        commission=0.001,
        engine="legacy",
        output=str(output_path),
    )

    cmd_walkforward(args)

    window_df = pd.read_csv(output_path)
    assert (window_df["window_status"] == "skipped").any()
    assert window_df.loc[window_df["window_status"] == "skipped", "skip_reason"].notna().all()
    assert (
        window_df.loc[window_df["window_status"] == "skipped", "skip_reason"]
        .astype(str)
        .str.contains("insufficient_test_rows")
        .any()
    )


def test_cmd_walkforward_writes_multi_symbol_overall_summary(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "trading_platform.cli.commands.walkforward.prepare_research_frame",
        lambda symbol, start=None, end=None: _prepared_payload(symbol, 35),
    )

    def fake_run_backtest_on_df(*, df, symbol, strategy, fast=20, slow=100, lookback=20, **kwargs):
        base = 6.0 if symbol == "AAPL" else 3.0
        score = base + (fast or lookback or 0) * 0.1
        return {
            "Return [%]": score,
            "Sharpe Ratio": score / 10.0,
            "Max. Drawdown [%]": -4.0,
        }

    monkeypatch.setattr(
        "trading_platform.cli.commands.walkforward.run_backtest_on_df",
        fake_run_backtest_on_df,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.walkforward.save_walkforward_return_plot",
        lambda df, output_path: output_path.with_suffix(".returns.png"),
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.walkforward.save_walkforward_param_plot",
        lambda df, output_path: output_path.with_suffix(".params.png"),
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.walkforward.save_walkforward_html_report",
        lambda **kwargs: Path(str(kwargs["output_path"]).replace(".csv", ".html")),
    )

    output_path = tmp_path / "walkforward.csv"
    args = argparse.Namespace(
        symbols=["AAPL", "MSFT"],
        universe=None,
        strategy="sma_cross",
        start="2020-01-01",
        end="2020-02-19",
        train_years=5,
        test_years=1,
        train_bars=10,
        test_bars=5,
        step_bars=5,
        train_period_days=None,
        test_period_days=None,
        step_days=None,
        min_train_rows=5,
        min_test_rows=3,
        fast_values=[10, 20],
        slow_values=[50],
        lookback_values=None,
        select_by="Sharpe Ratio",
        cash=10_000.0,
        commission=0.001,
        engine="legacy",
        output=str(output_path),
    )

    cmd_walkforward(args)

    summary_df = pd.read_csv(output_path.with_name("walkforward_summary.csv"))
    overall_df = pd.read_csv(output_path.with_name("walkforward_overall_summary.csv"))

    assert set(summary_df["symbol"]) == {"AAPL", "MSFT"}
    assert overall_df.iloc[0]["symbols"] == 2
    window_df = pd.read_csv(output_path)
    assert set(window_df.loc[window_df["window_status"] == "completed", "symbol"]) == {"AAPL", "MSFT"}


def test_cmd_walkforward_reports_no_trade_and_low_exposure_windows(monkeypatch, tmp_path: Path, capsys) -> None:
    monkeypatch.setattr(
        "trading_platform.cli.commands.walkforward.prepare_research_frame",
        lambda symbol, start=None, end=None: _prepared_payload(symbol, 30),
    )

    call_counter = {"count": 0}

    def fake_run_backtest_on_df(*, df, symbol, strategy, fast=20, slow=100, lookback=20, **kwargs):
        call_counter["count"] += 1
        if len(df) == 5:
            if call_counter["count"] == 2:
                return {
                    "Return [%]": 0.0,
                    "Sharpe Ratio": 0.0,
                    "Max. Drawdown [%]": 0.0,
                    "trade_count": 0,
                    "entry_count": 0,
                    "exit_count": 0,
                    "percent_time_in_market": 0.0,
                    "average_holding_period_bars": None,
                    "final_position_size": 0.0,
                    "ended_in_cash": True,
                }
            return {
                "Return [%]": 1.5,
                "Sharpe Ratio": 0.3,
                "Max. Drawdown [%]": -1.0,
                "trade_count": 2,
                "entry_count": 1,
                "exit_count": 1,
                "percent_time_in_market": 10.0,
                "average_holding_period_bars": 1.0,
                "final_position_size": 0.0,
                "ended_in_cash": True,
            }

        return {
            "Return [%]": 2.0,
            "Sharpe Ratio": 0.5,
            "Max. Drawdown [%]": -1.0,
            "trade_count": 2,
            "entry_count": 1,
            "exit_count": 1,
            "percent_time_in_market": 40.0,
            "average_holding_period_bars": 2.0,
            "final_position_size": 0.0,
            "ended_in_cash": True,
        }

    monkeypatch.setattr(
        "trading_platform.cli.commands.walkforward.run_backtest_on_df",
        fake_run_backtest_on_df,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.walkforward.save_walkforward_return_plot",
        lambda df, output_path: output_path.with_suffix(".returns.png"),
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.walkforward.save_walkforward_param_plot",
        lambda df, output_path: output_path.with_suffix(".params.png"),
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.walkforward.save_walkforward_html_report",
        lambda **kwargs: Path(str(kwargs["output_path"]).replace(".csv", ".html")),
    )

    output_path = tmp_path / "walkforward.csv"
    args = argparse.Namespace(
        symbols=["AAPL"],
        universe=None,
        strategy="sma_cross",
        start="2020-01-01",
        end="2020-02-14",
        train_years=5,
        test_years=1,
        train_bars=10,
        test_bars=5,
        step_bars=5,
        train_period_days=None,
        test_period_days=None,
        step_days=None,
        min_train_rows=5,
        min_test_rows=3,
        fast_values=[10],
        slow_values=[50],
        lookback_values=None,
        select_by="Sharpe Ratio",
        cash=10_000.0,
        commission=0.001,
        engine="legacy",
        output=str(output_path),
    )

    cmd_walkforward(args)

    window_df = pd.read_csv(output_path)
    summary_df = pd.read_csv(output_path.with_name("walkforward_summary.csv"))

    assert (window_df["trade_count"] == 0).any()
    assert (window_df["percent_time_in_market"] == 10.0).any()
    assert summary_df.iloc[0]["total_trade_count"] >= 0
    assert summary_df.iloc[0]["mean_percent_time_in_market"] >= 0.0

    stdout = capsys.readouterr().out
    assert "activity=no_trades" in stdout
    assert "activity=low_exposure" in stdout


def test_cmd_walkforward_accepts_compatibility_day_aliases_as_bar_counts(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "trading_platform.cli.commands.walkforward.prepare_research_frame",
        lambda symbol, start=None, end=None: _prepared_payload(symbol, 30),
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.walkforward.run_backtest_on_df",
        lambda **kwargs: {"Return [%]": 1.0, "Sharpe Ratio": 0.2, "Max. Drawdown [%]": -1.0},
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.walkforward.save_walkforward_return_plot",
        lambda df, output_path: output_path.with_suffix(".returns.png"),
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.walkforward.save_walkforward_param_plot",
        lambda df, output_path: output_path.with_suffix(".params.png"),
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.walkforward.save_walkforward_html_report",
        lambda **kwargs: Path(str(kwargs["output_path"]).replace(".csv", ".html")),
    )

    output_path = tmp_path / "walkforward.csv"
    args = argparse.Namespace(
        symbols=["AAPL"],
        universe=None,
        strategy="sma_cross",
        start="2020-01-01",
        end="2020-02-14",
        train_years=5,
        test_years=1,
        train_bars=None,
        test_bars=None,
        step_bars=None,
        train_period_days=10,
        test_period_days=5,
        step_days=5,
        min_train_rows=5,
        min_test_rows=3,
        fast_values=[10],
        slow_values=[50],
        lookback_values=None,
        select_by="Sharpe Ratio",
        cash=10_000.0,
        commission=0.001,
        engine="legacy",
        output=str(output_path),
    )

    cmd_walkforward(args)

    window_df = pd.read_csv(output_path)
    assert (window_df["window_status"] == "completed").any()
    assert set(window_df["train_bars_requested"]) == {10}
    assert set(window_df["test_bars_requested"]) == {5}
    assert set(window_df["step_bars_requested"]) == {5}


def test_cmd_walkforward_supports_breakout_hold(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "trading_platform.cli.commands.walkforward.prepare_research_frame",
        lambda symbol, start=None, end=None: _prepared_payload(symbol, 35),
    )

    captured_params: list[tuple[int | None, int | None, int | None]] = []

    def fake_run_backtest_on_df(*, df, symbol, strategy, **kwargs):
        captured_params.append(
            (
                kwargs.get("entry_lookback"),
                kwargs.get("exit_lookback"),
                kwargs.get("momentum_lookback"),
            )
        )
        return {
            "Return [%]": 2.0,
            "Sharpe Ratio": 0.4,
            "Max. Drawdown [%]": -1.5,
            "trade_count": 2,
            "entry_count": 1,
            "exit_count": 1,
            "percent_time_in_market": 30.0,
            "average_holding_period_bars": 3.0,
            "final_position_size": 0.0,
            "ended_in_cash": True,
        }

    monkeypatch.setattr(
        "trading_platform.cli.commands.walkforward.run_backtest_on_df",
        fake_run_backtest_on_df,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.walkforward.save_walkforward_return_plot",
        lambda df, output_path: output_path.with_suffix(".returns.png"),
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.walkforward.save_walkforward_param_plot",
        lambda df, output_path: output_path.with_suffix(".params.png"),
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.walkforward.save_walkforward_html_report",
        lambda **kwargs: Path(str(kwargs["output_path"]).replace(".csv", ".html")),
    )

    output_path = tmp_path / "walkforward_breakout.csv"
    args = argparse.Namespace(
        symbols=["AAPL"],
        universe=None,
        strategy="breakout_hold",
        start="2020-01-01",
        end="2020-02-19",
        train_years=5,
        test_years=1,
        train_bars=10,
        test_bars=5,
        step_bars=5,
        train_period_days=None,
        test_period_days=None,
        step_days=None,
        min_train_rows=5,
        min_test_rows=3,
        fast=20,
        slow=100,
        lookback=20,
        entry_lookback=55,
        exit_lookback=20,
        momentum_lookback=None,
        fast_values=None,
        slow_values=None,
        lookback_values=None,
        entry_lookback_values=[20, 55],
        exit_lookback_values=[10],
        momentum_lookback_values=[63],
        select_by="Sharpe Ratio",
        cash=10_000.0,
        commission=0.001,
        engine="legacy",
        output=str(output_path),
    )

    cmd_walkforward(args)

    window_df = pd.read_csv(output_path)
    assert (window_df["window_status"] == "completed").any()
    assert set(window_df.loc[window_df["window_status"] == "completed", "entry_lookback"]) == {20}
    assert set(window_df.loc[window_df["window_status"] == "completed", "exit_lookback"]) == {10}
    assert set(window_df.loc[window_df["window_status"] == "completed", "momentum_lookback"]) == {63}
    assert any(params == (20, 10, 63) for params in captured_params)


def test_cmd_walkforward_supports_xsec_momentum_topn(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "trading_platform.cli.commands.walkforward.prepare_research_frame",
        lambda symbol, start=None, end=None: {
            "df": pd.DataFrame(
                {
                    "timestamp": pd.date_range("2020-01-01", periods=30, freq="B"),
                    "Close": [
                        100 + idx * 1.8
                        if symbol == "AAPL"
                        else 100 + idx * (1.0 if idx % 2 == 0 else 1.4)
                        if symbol == "MSFT"
                        else 100 + idx * (0.3 if idx % 2 == 0 else 0.6)
                        for idx in range(30)
                    ],
                    "Volume": [1_000_000 for _ in range(30)],
                }
            ),
            "path": tmp_path / f"{symbol}.parquet",
            "date_col": "timestamp",
            "effective_start": "2020-01-01",
            "effective_end": "2020-02-11",
            "rows": 30,
        },
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.walkforward.save_walkforward_return_plot",
        lambda df, output_path: output_path.with_suffix(".returns.png"),
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.walkforward.save_walkforward_param_plot",
        lambda df, output_path: output_path.with_suffix(".params.png"),
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.walkforward.save_walkforward_html_report",
        lambda **kwargs: Path(str(kwargs["output_path"]).replace(".csv", ".html")),
    )

    output_path = tmp_path / "xsec_walkforward.csv"
    args = argparse.Namespace(
        symbols=["AAPL", "MSFT", "NVDA"],
        universe=None,
        strategy="xsec_momentum_topn",
        start="2020-01-01",
        end="2020-02-11",
        train_years=5,
        test_years=1,
        train_bars=10,
        test_bars=5,
        step_bars=5,
        train_period_days=None,
        test_period_days=None,
        step_days=None,
        min_train_rows=5,
        min_test_rows=3,
        fast=20,
        slow=100,
        lookback=20,
        lookback_bars=10,
        skip_bars=0,
        top_n=2,
        rebalance_bars=5,
        max_position_weight=0.5,
        min_avg_dollar_volume=50_000_000,
        max_names_per_sector=1,
        turnover_buffer_bps=25.0,
        max_turnover_per_rebalance=0.5,
        weighting_scheme="inv_vol",
        vol_lookback_bars=20,
        benchmark="equal_weight",
        entry_lookback=55,
        exit_lookback=20,
        momentum_lookback=None,
        fast_values=None,
        slow_values=None,
        lookback_values=None,
        lookback_bars_values=[10],
        skip_bars_values=[0],
        top_n_values=[2],
        rebalance_bars_values=[5],
        entry_lookback_values=None,
        exit_lookback_values=None,
        momentum_lookback_values=None,
        select_by="Return [%]",
        cash=10_000.0,
        commission=0.001,
        cost_bps=15.0,
        engine="legacy",
        output=str(output_path),
    )

    cmd_walkforward(args)

    window_df = pd.read_csv(output_path)
    summary_df = pd.read_csv(output_path.with_name("xsec_walkforward_summary.csv"))
    diagnostics_df = pd.read_csv(output_path.with_name("xsec_walkforward_rebalance_diagnostics.csv"))
    assert (window_df["window_status"] == "completed").any()
    assert set(window_df.loc[window_df["window_status"] == "completed", "symbol"]) == {"UNIVERSE"}
    completed_df = window_df.loc[window_df["window_status"] == "completed"].copy()
    assert set(completed_df["lookback_bars"]) == {10}
    assert set(window_df.loc[window_df["window_status"] == "completed", "top_n"]) == {2}
    assert set(completed_df["benchmark_type"]) == {"equal_weight"}
    assert {"test_gross_return_pct", "test_net_return_pct", "test_cost_drag_return_pct", "annualized_turnover", "total_transaction_cost", "weighting_scheme", "max_position_weight", "min_avg_dollar_volume", "turnover_buffer_bps", "max_turnover_per_rebalance", "average_available_symbols", "turnover_cap_binding_count", "turnover_buffer_blocked_replacements"}.issubset(window_df.columns)
    assert (completed_df["average_selected_symbols"] >= 0.0).all()
    assert (completed_df["percent_invested"] >= 0.0).all()
    assert (completed_df["average_selected_symbols"] > 0.0).any()
    assert (completed_df["percent_invested"] > 0.0).any()
    assert (completed_df["percent_empty_rebalances"] >= 0.0).all()
    assert (completed_df["percent_empty_rebalances"] < 100.0).any()
    assert (summary_df["mean_average_selected_symbols"] > 0.0).all()
    assert (summary_df["mean_percent_empty_rebalances"] >= 0.0).all()
    assert set(summary_df["benchmark_type"]) == {"equal_weight"}
    assert {"avg_test_gross_return_pct", "avg_test_net_return_pct", "avg_test_cost_drag_return_pct", "mean_annualized_turnover", "total_transaction_cost", "mean_average_available_symbols", "total_turnover_cap_binding_count", "total_turnover_buffer_blocked_replacements"}.issubset(summary_df.columns)
    assert {"window_index", "timestamp", "selected_symbols", "selected_weights", "eligible_symbol_count", "available_symbol_count", "liquidity_excluded_count"}.issubset(diagnostics_df.columns)
    assert diagnostics_df["empty_selection"].isin([True, False]).all()
    assert diagnostics_df["empty_selection"].eq(False).any()
    assert diagnostics_df["liquidity_filter_active"].all()


def test_cmd_walkforward_xsec_uses_dynamic_eligibility_for_late_listings(monkeypatch, tmp_path: Path) -> None:
    def fake_prepare(symbol: str, start=None, end=None):
        if symbol == "ARM":
            timestamps = pd.date_range("2020-01-15", periods=18, freq="B")
            closes = [50 + idx * 2.5 for idx in range(18)]
        else:
            timestamps = pd.date_range("2020-01-01", periods=30, freq="B")
            slope = 1.8 if symbol == "AAPL" else 1.0
            closes = [100 + idx * slope for idx in range(30)]
        return {
            "df": pd.DataFrame({"timestamp": timestamps, "Close": closes, "Volume": [1_000_000 for _ in range(len(timestamps))]}),
            "path": tmp_path / f"{symbol}.parquet",
            "date_col": "timestamp",
            "effective_start": pd.Timestamp(timestamps.min()).date().isoformat(),
            "effective_end": pd.Timestamp(timestamps.max()).date().isoformat(),
            "rows": len(timestamps),
        }

    monkeypatch.setattr(
        "trading_platform.cli.commands.walkforward.prepare_research_frame",
        fake_prepare,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.walkforward.save_walkforward_return_plot",
        lambda df, output_path: output_path.with_suffix(".returns.png"),
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.walkforward.save_walkforward_param_plot",
        lambda df, output_path: output_path.with_suffix(".params.png"),
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.walkforward.save_walkforward_html_report",
        lambda **kwargs: Path(str(kwargs["output_path"]).replace(".csv", ".html")),
    )

    output_path = tmp_path / "xsec_walkforward_late_listings.csv"
    args = argparse.Namespace(
        symbols=["AAPL", "MSFT", "ARM"],
        universe=None,
        strategy="xsec_momentum_topn",
        start="2020-01-01",
        end="2020-02-28",
        train_years=5,
        test_years=1,
        train_bars=10,
        test_bars=5,
        step_bars=5,
        train_period_days=None,
        test_period_days=None,
        step_days=None,
        min_train_rows=5,
        min_test_rows=3,
        fast=20,
        slow=100,
        lookback=20,
        lookback_bars=3,
        skip_bars=0,
        top_n=2,
        rebalance_bars=5,
        benchmark="equal_weight",
        entry_lookback=55,
        exit_lookback=20,
        momentum_lookback=None,
        fast_values=None,
        slow_values=None,
        lookback_values=None,
        lookback_bars_values=[3],
        skip_bars_values=[0],
        top_n_values=[2],
        rebalance_bars_values=[5],
        entry_lookback_values=None,
        exit_lookback_values=None,
        momentum_lookback_values=None,
        select_by="Return [%]",
        cash=10_000.0,
        commission=0.001,
        cost_bps=10.0,
        engine="legacy",
        output=str(output_path),
    )

    cmd_walkforward(args)

    window_df = pd.read_csv(output_path)
    summary_df = pd.read_csv(output_path.with_name("xsec_walkforward_late_listings_summary.csv"))
    diagnostics_df = pd.read_csv(output_path.with_name("xsec_walkforward_late_listings_rebalance_diagnostics.csv"))

    completed_df = window_df.loc[window_df["window_status"] == "completed"].copy()
    assert not completed_df.empty
    assert completed_df["effective_start_date"].iloc[0] == "2020-01-01"
    assert (completed_df["average_available_symbols"] >= 2.0).all()
    assert (completed_df["max_available_symbols"] >= 3.0).any()
    assert (completed_df["average_eligible_symbols"] >= 1.0).all()
    assert {"available_symbol_count", "eligible_symbol_count", "selected_symbol_count"}.issubset(diagnostics_df.columns)
    assert diagnostics_df["available_symbol_count"].max() == 3
    assert summary_df.iloc[0]["mean_average_available_symbols"] >= 2.0
    assert summary_df.iloc[0]["mean_max_available_symbols"] >= 3.0
