from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from trading_platform.cli.commands.alpha_research import cmd_alpha_research
from trading_platform.cli.commands.research import cmd_research
from trading_platform.cli.commands.sweep import cmd_sweep
from trading_platform.cli.common import prepare_research_frame


def test_prepare_research_frame_applies_date_slicing(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "trading_platform.cli.common.resolve_feature_frame_path",
        lambda symbol: tmp_path / f"{symbol}.parquet",
    )
    monkeypatch.setattr(
        "trading_platform.cli.common.load_feature_frame",
        lambda symbol: pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2020-01-01", "2020-01-02", "2020-01-03", "2020-01-04"]),
                "Close": [100.0, 101.0, 102.0, 103.0],
                "Open": [99.0, 100.0, 101.0, 102.0],
                "High": [101.0, 102.0, 103.0, 104.0],
                "Low": [98.0, 99.0, 100.0, 101.0],
                "Volume": [1000, 1000, 1000, 1000],
            }
        ),
    )

    prepared = prepare_research_frame("AAPL", start="2020-01-02", end="2020-01-03")

    assert prepared["rows"] == 2
    assert prepared["effective_start"] == "2020-01-02"
    assert prepared["effective_end"] == "2020-01-03"


def test_cmd_alpha_research_supports_config_and_cli_overrides(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}
    config_path = tmp_path / "alpha_research.yaml"
    config_path.write_text(
        f"""
paths:
  feature_path: {(tmp_path / "features").as_posix()}
  output_dir: {(tmp_path / "artifacts" / "alpha_from_config").as_posix()}
selection:
  universe: nasdaq100
signals:
  family: momentum
  candidate_grid_preset: broad_v1
  signal_composition_preset: composite_v1
  max_variants_per_family: 5
  lookbacks: [5, 10]
  horizons: [1, 5]
  enable_context_confirmations: true
  enable_relative_features: true
portfolio:
  top_quantile: 0.2
  bottom_quantile: 0.2
  train_size: 252
  test_size: 63
tracking:
  tracker_dir: {(tmp_path / "tracking").as_posix()}
""".strip(),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "trading_platform.cli.commands.alpha_research.resolve_symbols",
        lambda args: args.symbols if getattr(args, "symbols", None) else ["AAPL", "MSFT"],
    )

    def fake_run_alpha_research(**kwargs):
        captured.update(kwargs)
        return {
            "leaderboard_path": str(tmp_path / "leaderboard.csv"),
            "fold_results_path": str(tmp_path / "fold_results.csv"),
            "portfolio_returns_path": str(tmp_path / "portfolio_returns.csv"),
            "ensemble_member_summary_path": str(tmp_path / "ensemble_member_summary.csv"),
            "implementability_report_path": str(tmp_path / "implementability_report.csv"),
            "signal_performance_by_sub_universe_path": str(tmp_path / "signal_performance_by_sub_universe.csv"),
            "signal_performance_by_benchmark_context_path": str(tmp_path / "signal_performance_by_benchmark_context.csv"),
            "research_manifest_path": str(tmp_path / "research_manifest.json"),
        }

    monkeypatch.setattr(
        "trading_platform.cli.commands.alpha_research.run_alpha_research",
        fake_run_alpha_research,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.alpha_research.build_alpha_experiment_record",
        lambda output_dir: {"output_dir": str(output_dir)},
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.alpha_research.register_experiment",
        lambda record, tracker_dir: {"experiment_registry_path": tracker_dir / "registry.csv"},
    )

    args = argparse.Namespace(
        config=str(config_path),
        symbols=["AAPL"],
        universe=None,
        feature_dir="data/features",
        signal_family="momentum",
        candidate_grid_preset="standard",
        signal_composition_preset="standard",
        max_variants_per_family=None,
        lookbacks=[20],
        horizons=[1],
        min_rows=126,
        equity_context_enabled=False,
        equity_context_include_volume=False,
        enable_context_confirmations=None,
        enable_relative_features=None,
        enable_flow_confirmations=None,
        enable_ensemble=False,
        ensemble_mode="disabled",
        ensemble_weight_method="equal",
        ensemble_normalize_scores="rank_pct",
        ensemble_max_members=5,
        ensemble_max_members_per_family=None,
        ensemble_minimum_member_observations=0,
        ensemble_minimum_member_metric=None,
        top_quantile=0.2,
        bottom_quantile=0.2,
        output_dir=str(tmp_path / "artifacts" / "cli_override"),
        train_size=756,
        test_size=63,
        step_size=None,
        min_train_size=None,
        portfolio_top_n=10,
        portfolio_long_quantile=0.2,
        portfolio_short_quantile=0.2,
        commission=0.0,
        min_price=None,
        min_volume=None,
        min_avg_dollar_volume=None,
        max_adv_participation=0.05,
        max_position_pct_of_adv=0.1,
        max_notional_per_name=None,
        slippage_bps_per_turnover=0.0,
        slippage_bps_per_adv=10.0,
        dynamic_recent_quality_window=20,
        dynamic_min_history=5,
        dynamic_downweight_mean_rank_ic=0.01,
        dynamic_deactivate_mean_rank_ic=-0.02,
        regime_aware_enabled=False,
        regime_min_history=5,
        regime_underweight_mean_rank_ic=0.01,
        regime_exclude_mean_rank_ic=-0.01,
        experiment_tracker_dir=None,
        _cli_argv=[
            "--config",
            str(config_path),
            "--symbols",
            "AAPL",
            "--lookbacks",
            "20",
            "--output-dir",
            str(tmp_path / "artifacts" / "cli_override"),
        ],
    )

    cmd_alpha_research(args)

    assert captured["symbols"] == ["AAPL"]
    assert captured["candidate_grid_preset"] == "broad_v1"
    assert captured["signal_composition_preset"] == "composite_v1"
    assert captured["max_variants_per_family"] == 5
    assert captured["enable_context_confirmations"] is True
    assert captured["enable_relative_features"] is True
    assert captured["enable_flow_confirmations"] is None
    assert captured["lookbacks"] == [20]
    assert captured["horizons"] == [1, 5]
    assert Path(str(captured["feature_dir"])) == tmp_path / "features"
    assert Path(str(captured["output_dir"])) == tmp_path / "artifacts" / "cli_override"


def test_cmd_alpha_research_accepts_new_signal_family_from_config(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}
    config_path = tmp_path / "alpha_research.yaml"
    config_path.write_text(
        f"""
paths:
  feature_path: {(tmp_path / "features").as_posix()}
  output_dir: {(tmp_path / "artifacts" / "alpha_from_config").as_posix()}
selection:
  symbols:
    - AAPL
signals:
  family: sector_relative_momentum
  candidate_grid_preset: broad_v1
  signal_composition_preset: composite_v1
  max_variants_per_family: 3
  lookbacks: [5]
  horizons: [1]
tracking:
  tracker_dir: {(tmp_path / "tracking").as_posix()}
""".strip(),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "trading_platform.cli.commands.alpha_research.resolve_symbols",
        lambda args: ["AAPL"],
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.alpha_research.run_alpha_research",
        lambda **kwargs: captured.update(kwargs) or {
            "leaderboard_path": str(tmp_path / "leaderboard.csv"),
            "fold_results_path": str(tmp_path / "fold_results.csv"),
            "portfolio_returns_path": str(tmp_path / "portfolio_returns.csv"),
            "ensemble_member_summary_path": str(tmp_path / "ensemble_member_summary.csv"),
            "implementability_report_path": str(tmp_path / "implementability_report.csv"),
            "signal_performance_by_sub_universe_path": str(tmp_path / "signal_performance_by_sub_universe.csv"),
            "signal_performance_by_benchmark_context_path": str(tmp_path / "signal_performance_by_benchmark_context.csv"),
            "research_manifest_path": str(tmp_path / "research_manifest.json"),
        },
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.alpha_research.build_alpha_experiment_record",
        lambda output_dir: {"output_dir": str(output_dir)},
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.alpha_research.register_experiment",
        lambda record, tracker_dir: {"experiment_registry_path": tracker_dir / "registry.csv"},
    )

    args = argparse.Namespace(
        config=str(config_path),
        symbols=None,
        universe=None,
        feature_dir="data/features",
        signal_family="momentum",
        candidate_grid_preset="standard",
        signal_composition_preset="standard",
        max_variants_per_family=None,
        lookbacks=[20],
        horizons=[1],
        min_rows=126,
        equity_context_enabled=False,
        equity_context_include_volume=False,
        enable_context_confirmations=None,
        enable_relative_features=None,
        enable_flow_confirmations=None,
        enable_ensemble=False,
        ensemble_mode="disabled",
        ensemble_weight_method="equal",
        ensemble_normalize_scores="rank_pct",
        ensemble_max_members=5,
        ensemble_max_members_per_family=None,
        ensemble_minimum_member_observations=0,
        ensemble_minimum_member_metric=None,
        top_quantile=0.2,
        bottom_quantile=0.2,
        output_dir="artifacts/alpha_research",
        train_size=756,
        test_size=63,
        step_size=None,
        min_train_size=None,
        portfolio_top_n=10,
        portfolio_long_quantile=0.2,
        portfolio_short_quantile=0.2,
        commission=0.0,
        min_price=None,
        min_volume=None,
        min_avg_dollar_volume=None,
        max_adv_participation=0.05,
        max_position_pct_of_adv=0.1,
        max_notional_per_name=None,
        slippage_bps_per_turnover=0.0,
        slippage_bps_per_adv=10.0,
        dynamic_recent_quality_window=20,
        dynamic_min_history=5,
        dynamic_downweight_mean_rank_ic=0.01,
        dynamic_deactivate_mean_rank_ic=-0.02,
        regime_aware_enabled=False,
        regime_min_history=5,
        regime_underweight_mean_rank_ic=0.01,
        regime_exclude_mean_rank_ic=-0.01,
        experiment_tracker_dir=None,
        _cli_argv=["--config", str(config_path)],
    )

    cmd_alpha_research(args)

    assert captured["signal_family"] == "sector_relative_momentum"
    assert captured["candidate_grid_preset"] == "broad_v1"
    assert captured["signal_composition_preset"] == "composite_v1"
    assert captured["max_variants_per_family"] == 3
    assert captured["lookbacks"] == [5]
    assert captured["horizons"] == [1]


def test_cmd_research_reports_effective_range_rows_and_feature_path(monkeypatch, capsys, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "trading_platform.cli.commands.research.prepare_research_frame",
        lambda symbol, start=None, end=None: {
            "df": pd.DataFrame(
                {
                    "timestamp": pd.to_datetime(["2020-01-02", "2020-01-03"]),
                    "Open": [100.0, 101.0],
                    "High": [101.0, 102.0],
                    "Low": [99.0, 100.0],
                    "Close": [100.5, 101.5],
                    "Volume": [1000, 1000],
                }
            ),
            "path": tmp_path / "AAPL.parquet",
            "date_col": "timestamp",
            "effective_start": "2020-01-02",
            "effective_end": "2020-01-03",
            "rows": 2,
        },
    )

    def fake_run_backtest_on_df(**kwargs):
        captured["df_len"] = len(kwargs["df"])
        return {
            "Return [%]": 5.0,
            "Sharpe Ratio": 1.2,
            "Max. Drawdown [%]": -3.0,
            "trade_count": 2,
            "entry_count": 1,
            "exit_count": 1,
            "percent_time_in_market": 50.0,
            "average_holding_period_bars": 1.0,
            "final_position_size": 0.0,
            "ended_in_cash": True,
        }

    monkeypatch.setattr(
        "trading_platform.cli.commands.research.run_backtest_on_df",
        fake_run_backtest_on_df,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.research.log_experiment",
        lambda stats: "exp-1",
    )

    args = argparse.Namespace(
        symbols=["AAPL"],
        universe=None,
        strategy="sma_cross",
        fast=20,
        slow=50,
        lookback=20,
        cash=10_000.0,
        commission=0.001,
        start="2020-01-02",
        end="2020-01-03",
        engine="legacy",
        rebalance_frequency="daily",
        output_dir=None,
    )

    cmd_research(args)

    assert captured["df_len"] == 2
    stdout = capsys.readouterr().out
    assert "Running research run for 1 symbol(s): AAPL" in stdout
    assert "range=2020-01-02->2020-01-03" in stdout
    assert "rows=2" in stdout
    assert f"feature_path={tmp_path / 'AAPL.parquet'}" in stdout
    assert "trade_count=2" in stdout
    assert "time_in_market[%]=50.0" in stdout
    assert "activity=active" in stdout


def test_cmd_research_breakout_hold_reports_momentum_lookback(monkeypatch, capsys, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "trading_platform.cli.commands.research.prepare_research_frame",
        lambda symbol, start=None, end=None: {
            "df": pd.DataFrame(
                {
                    "timestamp": pd.to_datetime(["2020-01-02", "2020-01-03", "2020-01-06"]),
                    "Open": [100.0, 101.0, 102.0],
                    "High": [101.0, 102.0, 103.0],
                    "Low": [99.0, 100.0, 101.0],
                    "Close": [100.5, 101.5, 102.5],
                    "Volume": [1000, 1000, 1000],
                }
            ),
            "path": tmp_path / "AAPL.parquet",
            "date_col": "timestamp",
            "effective_start": "2020-01-02",
            "effective_end": "2020-01-06",
            "rows": 3,
        },
    )

    def fake_run_backtest_on_df(**kwargs):
        captured["entry_lookback"] = kwargs["entry_lookback"]
        captured["exit_lookback"] = kwargs["exit_lookback"]
        captured["momentum_lookback"] = kwargs["momentum_lookback"]
        return {
            "Return [%]": 4.0,
            "Sharpe Ratio": 0.9,
            "Max. Drawdown [%]": -2.0,
            "trade_count": 2,
            "entry_count": 1,
            "exit_count": 1,
            "percent_time_in_market": 33.0,
            "average_holding_period_bars": 1.0,
            "final_position_size": 0.0,
            "ended_in_cash": True,
        }

    monkeypatch.setattr(
        "trading_platform.cli.commands.research.run_backtest_on_df",
        fake_run_backtest_on_df,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.research.log_experiment",
        lambda stats: "exp-breakout-run",
    )

    args = argparse.Namespace(
        symbols=["AAPL"],
        universe=None,
        strategy="breakout_hold",
        fast=20,
        slow=50,
        lookback=20,
        entry_lookback=55,
        exit_lookback=20,
        momentum_lookback=63,
        cash=10_000.0,
        commission=0.001,
        start="2020-01-02",
        end="2020-01-06",
        engine="legacy",
        rebalance_frequency="daily",
        output_dir=None,
    )

    cmd_research(args)

    assert captured["entry_lookback"] == 55
    assert captured["exit_lookback"] == 20
    assert captured["momentum_lookback"] == 63

    stdout = capsys.readouterr().out
    assert "entry_lookback=55" in stdout
    assert "exit_lookback=20" in stdout
    assert "momentum_lookback=63" in stdout


def test_cmd_research_supports_xsec_momentum_topn(monkeypatch, capsys, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "trading_platform.cli.commands.research.prepare_research_frame",
        lambda symbol, start=None, end=None: {
            "df": pd.DataFrame(
                {
                    "timestamp": pd.date_range("2020-01-02", periods=6, freq="D"),
                    "Close": [100.0, 101.0, 102.0, 103.0, 104.0, 105.0 if symbol == "AAPL" else 100.5],
                }
            ),
            "path": tmp_path / f"{symbol}.parquet",
            "date_col": "timestamp",
            "effective_start": "2020-01-02",
            "effective_end": "2020-01-07",
            "rows": 6,
        },
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.research.log_experiment",
        lambda stats: "exp-xsec-run",
    )

    args = argparse.Namespace(
        symbols=["AAPL", "MSFT"],
        universe=None,
        strategy="xsec_momentum_topn",
        fast=20,
        slow=50,
        lookback=20,
        lookback_bars=3,
        skip_bars=0,
        top_n=1,
        rebalance_bars=1,
        portfolio_construction_mode="pure_topn",
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
        cash=10_000.0,
        commission=0.001,
        cost_bps=10.0,
        start="2020-01-02",
        end="2020-01-07",
        engine="legacy",
        rebalance_frequency="daily",
        output_dir=None,
    )

    cmd_research(args)

    stdout = capsys.readouterr().out
    assert "strategy=xsec_momentum_topn" in stdout
    assert "lookback_bars=3" in stdout
    assert "top_n=1" in stdout
    assert "portfolio_construction_mode=pure_topn" in stdout
    assert "weighting_scheme=inv_vol" in stdout
    assert "max_position_weight=0.5" in stdout
    assert "min_avg_dollar_volume=50000000" in stdout
    assert "avg_holdings=" in stdout
    assert "avg_realized_holdings=" in stdout
    assert "percent_invested=" in stdout
    assert "gross_return[%]=" in stdout
    assert "net_return[%]=" in stdout
    assert "cost_bps=10.0" in stdout
    assert "benchmark=equal_weight" in stdout


def test_cmd_sweep_skips_invalid_fast_slow_combinations_and_saves_rich_artifact(
    monkeypatch,
    capsys,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        "trading_platform.cli.commands.sweep.prepare_research_frame",
        lambda symbol, start=None, end=None: {
            "df": pd.DataFrame(
                {
                    "timestamp": pd.to_datetime(["2020-01-02", "2020-01-03", "2020-01-04"]),
                    "Open": [100.0, 101.0, 102.0],
                    "High": [101.0, 102.0, 103.0],
                    "Low": [99.0, 100.0, 101.0],
                    "Close": [100.5, 101.5, 102.5],
                    "Volume": [1000, 1000, 1000],
                }
            ),
            "path": tmp_path / "AAPL.parquet",
            "date_col": "timestamp",
            "effective_start": "2020-01-02",
            "effective_end": "2020-01-04",
            "rows": 3,
        },
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.sweep.run_backtest_on_df",
        lambda **kwargs: {
            "Return [%]": 7.0,
            "Sharpe Ratio": 1.1,
            "Max. Drawdown [%]": -2.5,
            "trade_count": 2,
            "entry_count": 1,
            "exit_count": 1,
            "percent_time_in_market": 12.5,
            "average_holding_period_bars": 2.0,
            "final_position_size": 0.0,
            "ended_in_cash": True,
        },
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.sweep.log_experiment",
        lambda stats: "exp-1",
    )

    output_path = tmp_path / "sweep.csv"
    args = argparse.Namespace(
        symbols=["AAPL"],
        universe=None,
        strategy="sma_cross",
        fast_values=[10, 20],
        slow_values=[10, 100],
        lookback_values=None,
        cash=10_000.0,
        commission=0.001,
        start="2020-01-01",
        end="2024-12-31",
        engine="legacy",
        rebalance_frequency="daily",
        output=str(output_path),
    )

    cmd_sweep(args)

    stdout = capsys.readouterr().out
    assert "param_combinations=2" in stdout
    assert "requested_range=2020-01-01->2024-12-31" in stdout
    assert "Skipping invalid combination fast=10, slow=10" in stdout

    result_df = pd.read_csv(output_path)
    assert set(
        [
            "symbol",
            "strategy",
            "engine",
            "start_date",
            "end_date",
            "fast",
            "slow",
            "lookback",
            "return_pct",
            "sharpe",
            "max_drawdown_pct",
            "trade_count",
            "entry_count",
            "exit_count",
            "percent_time_in_market",
            "average_holding_period_bars",
            "final_position_size",
            "ended_in_cash",
            "experiment_id",
            "status",
            "warning_count",
            "notes",
        ]
    ).issubset(result_df.columns)
    assert len(result_df) == 2
    assert set(result_df["slow"]) == {100}
    assert set(result_df["fast"]) == {10, 20}
    assert set(result_df["trade_count"]) == {2}
    assert set(result_df["percent_time_in_market"]) == {12.5}


def test_cmd_sweep_supports_breakout_hold_parameter_grid(monkeypatch, tmp_path: Path) -> None:
    captured_params: list[tuple[int | None, int | None, int | None]] = []

    monkeypatch.setattr(
        "trading_platform.cli.commands.sweep.prepare_research_frame",
        lambda symbol, start=None, end=None: {
            "df": pd.DataFrame(
                {
                    "timestamp": pd.to_datetime(["2020-01-02", "2020-01-03", "2020-01-04", "2020-01-05"]),
                    "Open": [100.0, 101.0, 102.0, 103.0],
                    "High": [101.0, 102.0, 103.0, 104.0],
                    "Low": [99.0, 100.0, 101.0, 102.0],
                    "Close": [100.5, 101.5, 102.5, 103.5],
                    "Volume": [1000, 1000, 1000, 1000],
                }
            ),
            "path": tmp_path / "AAPL.parquet",
            "date_col": "timestamp",
            "effective_start": "2020-01-02",
            "effective_end": "2020-01-05",
            "rows": 4,
        },
    )

    def fake_run_backtest_on_df(**kwargs):
        captured_params.append(
            (
                kwargs.get("entry_lookback"),
                kwargs.get("exit_lookback"),
                kwargs.get("momentum_lookback"),
            )
        )
        return {
            "Return [%]": 3.0,
            "Sharpe Ratio": 0.8,
            "Max. Drawdown [%]": -2.0,
            "trade_count": 2,
            "entry_count": 1,
            "exit_count": 1,
            "percent_time_in_market": 25.0,
            "average_holding_period_bars": 2.0,
            "final_position_size": 0.0,
            "ended_in_cash": True,
        }

    monkeypatch.setattr(
        "trading_platform.cli.commands.sweep.run_backtest_on_df",
        fake_run_backtest_on_df,
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.sweep.log_experiment",
        lambda stats: "exp-breakout",
    )

    output_path = tmp_path / "breakout_sweep.csv"
    args = argparse.Namespace(
        symbols=["AAPL"],
        universe=None,
        strategy="breakout_hold",
        fast_values=None,
        slow_values=None,
        lookback_values=None,
        entry_lookback=55,
        exit_lookback=20,
        momentum_lookback=None,
        entry_lookback_values=[20, 55],
        exit_lookback_values=[10],
        momentum_lookback_values=[63],
        cash=10_000.0,
        commission=0.001,
        start=None,
        end=None,
        engine="legacy",
        rebalance_frequency="daily",
        output=str(output_path),
    )

    cmd_sweep(args)

    result_df = pd.read_csv(output_path)
    assert len(result_df) == 2
    assert set(result_df["entry_lookback"]) == {20, 55}
    assert set(result_df["exit_lookback"]) == {10}
    assert set(result_df["momentum_lookback"]) == {63}
    assert captured_params == [(20, 10, 63), (55, 10, 63)]


def test_cmd_sweep_supports_xsec_momentum_topn(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "trading_platform.cli.commands.sweep.prepare_research_frame",
        lambda symbol, start=None, end=None: {
            "df": pd.DataFrame(
                {
                    "timestamp": pd.date_range("2020-01-02", periods=12, freq="D"),
                    "Close": [100 + idx if symbol == "AAPL" else 90 + idx * 0.5 for idx in range(12)],
                }
            ),
            "path": tmp_path / f"{symbol}.parquet",
            "date_col": "timestamp",
            "effective_start": "2020-01-02",
            "effective_end": "2020-01-13",
            "rows": 12,
        },
    )
    monkeypatch.setattr(
        "trading_platform.cli.commands.sweep.log_experiment",
        lambda stats: "exp-xsec-sweep",
    )

    output_path = tmp_path / "xsec_sweep.csv"
    args = argparse.Namespace(
        symbols=["AAPL", "MSFT", "NVDA"],
        universe=None,
        strategy="xsec_momentum_topn",
        fast_values=None,
        slow_values=None,
        lookback_values=None,
        lookback_bars_values=[3, 5],
        skip_bars_values=[0],
        top_n_values=[2],
        rebalance_bars_values=[3],
        fast=20,
        slow=100,
        lookback=20,
        lookback_bars=5,
        skip_bars=0,
        top_n=2,
        rebalance_bars=3,
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
        entry_lookback_values=None,
        exit_lookback_values=None,
        momentum_lookback_values=None,
        cash=10_000.0,
        commission=0.001,
        cost_bps=12.0,
        start=None,
        end=None,
        engine="legacy",
        rebalance_frequency="daily",
        output=str(output_path),
    )

    cmd_sweep(args)

    result_df = pd.read_csv(output_path)
    assert len(result_df) == 2
    assert set(result_df["symbol"]) == {"UNIVERSE"}
    assert set(result_df["lookback_bars"]) == {3, 5}
    assert set(result_df["top_n"]) == {2}
    assert "average_number_of_holdings" in result_df.columns
    assert {"weighting_scheme", "max_position_weight", "min_avg_dollar_volume", "turnover_buffer_bps", "max_turnover_per_rebalance", "average_available_symbols"}.issubset(result_df.columns)
    assert {"gross_return_pct", "net_return_pct", "cost_drag_return_pct", "annualized_turnover", "cost_bps"}.issubset(result_df.columns)
    assert set(result_df["cost_bps"]) == {12.0}
    assert set(result_df["benchmark_type"]) == {"equal_weight"}
