from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import pytest

from trading_platform.cli.commands.compare_xsec_construction import cmd_compare_xsec_construction


def _window_df(mode: str, *, use_legacy_holdings_columns: bool = False) -> pd.DataFrame:
    values = {
        "pure_topn": {
            "test_return_pct": 12.0,
            "test_gross_return_pct": 12.5,
            "test_net_return_pct": 12.0,
            "test_cost_drag_return_pct": 0.5,
            "test_sharpe": 1.3,
            "test_max_drawdown_pct": -8.0,
            "mean_turnover": 0.08,
            "average_realized_holdings_count": 2.0,
            "benchmark_return_pct": 5.0,
        },
        "transition": {
            "test_return_pct": 10.0,
            "test_gross_return_pct": 10.2,
            "test_net_return_pct": 10.0,
            "test_cost_drag_return_pct": 0.2,
            "test_sharpe": 1.1,
            "test_max_drawdown_pct": -6.0,
            "mean_turnover": 0.03,
            "average_realized_holdings_count": 8.0,
            "benchmark_return_pct": 5.0,
        },
    }[mode]
    frame = pd.DataFrame(
        [
            {
                "window_index": 1,
                "window_status": "completed",
                "train_start": "2020-01-01",
                "train_end": "2022-12-30",
                "test_start": "2023-01-03",
                "test_end": "2023-07-05",
                "lookback_bars": 84,
                "skip_bars": 21,
                "top_n": 2,
                "rebalance_bars": 21,
                "benchmark_return_pct": values["benchmark_return_pct"],
                "excess_return_pct": values["test_return_pct"] - values["benchmark_return_pct"],
                "test_return_pct": values["test_return_pct"],
                "test_gross_return_pct": values["test_gross_return_pct"],
                "test_net_return_pct": values["test_net_return_pct"],
                "test_cost_drag_return_pct": values["test_cost_drag_return_pct"],
                "test_sharpe": values["test_sharpe"],
                "test_max_drawdown_pct": values["test_max_drawdown_pct"],
                "mean_turnover": values["mean_turnover"],
            }
        ]
    )
    if use_legacy_holdings_columns:
        frame["average_realized_holdings_count"] = values["average_realized_holdings_count"]
    else:
        frame["average_number_of_holdings"] = values["average_realized_holdings_count"]
        frame["target_selected_count"] = 2.0
        frame["realized_holdings_count"] = values["average_realized_holdings_count"]
    return frame


def _summary_df(mode: str) -> pd.DataFrame:
    values = {
        "pure_topn": {
            "avg_test_return_pct": 12.0,
            "avg_test_gross_return_pct": 12.5,
            "avg_test_net_return_pct": 12.0,
            "avg_test_cost_drag_return_pct": 0.5,
            "avg_excess_return_pct": 7.0,
            "percent_positive_windows": 100.0,
            "worst_excess_return_pct": 7.0,
            "best_excess_return_pct": 7.0,
            "avg_test_sharpe": 1.3,
            "worst_test_max_drawdown_pct": -8.0,
            "total_trade_count": 12.0,
            "mean_turnover": 0.08,
            "mean_annualized_turnover": 20.16,
            "mean_average_number_of_holdings": 2.0,
            "mean_average_target_selected_count": 2.0,
            "mean_average_realized_holdings_count": 2.0,
            "percent_windows_ended_in_cash": 0.0,
            "mean_average_liquidity_excluded_symbols": 0.0,
            "total_turnover_cap_binding_count": 0.0,
            "total_turnover_buffer_blocked_replacements": 0.0,
        },
        "transition": {
            "avg_test_return_pct": 10.0,
            "avg_test_gross_return_pct": 10.2,
            "avg_test_net_return_pct": 10.0,
            "avg_test_cost_drag_return_pct": 0.2,
            "avg_excess_return_pct": 5.0,
            "percent_positive_windows": 100.0,
            "worst_excess_return_pct": 5.0,
            "best_excess_return_pct": 5.0,
            "avg_test_sharpe": 1.1,
            "worst_test_max_drawdown_pct": -6.0,
            "total_trade_count": 20.0,
            "mean_turnover": 0.03,
            "mean_annualized_turnover": 7.56,
            "mean_average_number_of_holdings": 8.0,
            "mean_average_target_selected_count": 2.0,
            "mean_average_realized_holdings_count": 8.0,
            "percent_windows_ended_in_cash": 0.0,
            "mean_average_liquidity_excluded_symbols": 1.0,
            "total_turnover_cap_binding_count": 6.0,
            "total_turnover_buffer_blocked_replacements": 0.0,
        },
    }[mode]
    return pd.DataFrame([{"portfolio_construction_mode": mode, **values}])


def test_compare_xsec_construction_runs_both_modes_and_writes_artifacts(monkeypatch, tmp_path: Path, capsys) -> None:
    calls: list[str] = []

    def fake_run(args, *, symbols, param_grid, window_spec, verbose=True):
        calls.append(args.portfolio_construction_mode)
        return _window_df(args.portfolio_construction_mode), _summary_df(args.portfolio_construction_mode), None

    monkeypatch.setattr(
        "trading_platform.cli.commands.compare_xsec_construction.resolve_symbols",
        lambda args: ["AAPL", "MSFT"],
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.compare_xsec_construction._build_param_grid",
        lambda args: ([{"lookback_bars": 84, "skip_bars": 21, "top_n": 2, "rebalance_bars": 21}], []),
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.compare_xsec_construction._resolve_window_spec",
        lambda args: {"train_bars": 756, "test_bars": 126, "step_bars": 126, "window_units": "bars"},
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.compare_xsec_construction.run_xsec_walkforward_analysis",
        fake_run,
    )

    args = argparse.Namespace(
        symbols=None,
        universe="nasdaq100",
        strategy="xsec_momentum_topn",
        lookback_bars_values=[84],
        skip_bars_values=[21],
        top_n_values=[2],
        rebalance_bars_values=[21],
        start="2020-01-01",
        end=None,
        train_bars=756,
        test_bars=126,
        step_bars=126,
        cost_bps=10.0,
        benchmark="equal_weight",
        output_dir=str(tmp_path),
        portfolio_construction_mode="pure_topn",
    )

    cmd_compare_xsec_construction(args)

    assert calls == ["pure_topn", "transition"]
    summary_df = pd.read_csv(tmp_path / "compare_xsec_construction_summary.csv")
    window_df = pd.read_csv(tmp_path / "compare_xsec_construction_windows.csv")
    assert set(summary_df["portfolio_construction_mode"]) == {"pure_topn", "transition"}
    assert "delta_test_return_pct" in window_df.columns
    assert "delta_excess_return_pct" in window_df.columns
    assert (tmp_path / "compare_xsec_construction_report_report.html").exists() or (tmp_path / "compare_xsec_construction_report.html").exists()

    stdout = capsys.readouterr().out
    assert "Construction Comparison" in stdout
    assert "pure_topn" in stdout
    assert "transition" in stdout


def test_compare_xsec_construction_summary_contains_expected_columns(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("trading_platform.cli.commands.compare_xsec_construction.resolve_symbols", lambda args: ["AAPL", "MSFT"])
    monkeypatch.setattr("trading_platform.cli.commands.compare_xsec_construction._build_param_grid", lambda args: ([{"lookback_bars": 84, "skip_bars": 21, "top_n": 2, "rebalance_bars": 21}], []))
    monkeypatch.setattr("trading_platform.cli.commands.compare_xsec_construction._resolve_window_spec", lambda args: {"train_bars": 756, "test_bars": 126, "step_bars": 126, "window_units": "bars"})
    monkeypatch.setattr(
        "trading_platform.cli.commands.compare_xsec_construction.run_xsec_walkforward_analysis",
        lambda args, *, symbols, param_grid, window_spec, verbose=True: (
            _window_df(args.portfolio_construction_mode),
            _summary_df(args.portfolio_construction_mode),
            None,
        ),
    )

    args = argparse.Namespace(
        symbols=None,
        universe="nasdaq100",
        strategy="xsec_momentum_topn",
        lookback_bars_values=[84],
        skip_bars_values=[21],
        top_n_values=[2],
        rebalance_bars_values=[21],
        start="2020-01-01",
        end=None,
        train_bars=756,
        test_bars=126,
        step_bars=126,
        cost_bps=10.0,
        benchmark="equal_weight",
        output_dir=str(tmp_path),
        portfolio_construction_mode="pure_topn",
    )
    cmd_compare_xsec_construction(args)
    summary_df = pd.read_csv(tmp_path / "compare_xsec_construction_summary.csv")
    expected = {
        "portfolio_construction_mode",
        "avg_test_return_pct",
        "avg_test_gross_return_pct",
        "avg_test_net_return_pct",
        "avg_test_cost_drag_return_pct",
        "avg_excess_return_pct",
        "percent_positive_windows",
        "worst_excess_return_pct",
        "best_excess_return_pct",
        "avg_test_sharpe",
        "worst_test_max_drawdown_pct",
        "total_trade_count",
        "mean_turnover",
        "mean_annualized_turnover",
        "mean_average_number_of_holdings",
        "mean_average_target_selected_count",
        "mean_average_realized_holdings_count",
        "percent_windows_ended_in_cash",
        "mean_average_liquidity_excluded_symbols",
        "total_turnover_cap_binding_count",
        "total_turnover_buffer_blocked_replacements",
    }
    assert expected.issubset(summary_df.columns)


def test_compare_xsec_construction_accepts_legacy_holdings_window_columns(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("trading_platform.cli.commands.compare_xsec_construction.resolve_symbols", lambda args: ["AAPL", "MSFT"])
    monkeypatch.setattr("trading_platform.cli.commands.compare_xsec_construction._build_param_grid", lambda args: ([{"lookback_bars": 84, "skip_bars": 21, "top_n": 2, "rebalance_bars": 21}], []))
    monkeypatch.setattr("trading_platform.cli.commands.compare_xsec_construction._resolve_window_spec", lambda args: {"train_bars": 756, "test_bars": 126, "step_bars": 126, "window_units": "bars"})
    monkeypatch.setattr(
        "trading_platform.cli.commands.compare_xsec_construction.run_xsec_walkforward_analysis",
        lambda args, *, symbols, param_grid, window_spec, verbose=True: (
            _window_df(args.portfolio_construction_mode, use_legacy_holdings_columns=True),
            _summary_df(args.portfolio_construction_mode),
            None,
        ),
    )

    args = argparse.Namespace(
        symbols=None,
        universe="nasdaq100",
        strategy="xsec_momentum_topn",
        lookback_bars_values=[84],
        skip_bars_values=[21],
        top_n_values=[2],
        rebalance_bars_values=[21],
        start="2020-01-01",
        end=None,
        train_bars=756,
        test_bars=126,
        step_bars=126,
        cost_bps=10.0,
        benchmark="equal_weight",
        output_dir=str(tmp_path),
        portfolio_construction_mode="pure_topn",
    )

    cmd_compare_xsec_construction(args)

    window_df = pd.read_csv(tmp_path / "compare_xsec_construction_windows.csv")
    assert "pure_topn_realized_holdings_count" in window_df.columns
    assert "transition_realized_holdings_count" in window_df.columns
    assert window_df.loc[0, "pure_topn_realized_holdings_count"] == 2.0
    assert window_df.loc[0, "transition_realized_holdings_count"] == 8.0


def test_compare_xsec_construction_non_xsec_strategy_fails_cleanly(tmp_path: Path) -> None:
    args = argparse.Namespace(
        symbols=["AAPL"],
        universe=None,
        strategy="sma_cross",
        output_dir=str(tmp_path),
    )
    with pytest.raises(SystemExit, match="xsec_momentum_topn"):
        cmd_compare_xsec_construction(args)
