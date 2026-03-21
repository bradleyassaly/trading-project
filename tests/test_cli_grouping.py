from __future__ import annotations

from trading_platform.cli.grouped_parser import build_parser, rewrite_legacy_cli_args


def test_grouped_data_ingest_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(["data", "ingest", "--symbols", "AAPL", "--failure-report", "artifacts/ingest_failures.csv"])

    assert args.command_family == "data"
    assert args.data_command == "ingest"
    assert args.symbols == ["AAPL"]
    assert args.failure_report == "artifacts/ingest_failures.csv"


def test_grouped_research_alpha_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(["research", "alpha", "--symbols", "AAPL", "--lookbacks", "5"])

    assert args.command_family == "research"
    assert args.research_command == "alpha"
    assert args.symbols == ["AAPL"]
    assert args.lookbacks == [5]


def test_grouped_data_features_command_parses_for_universe() -> None:
    parser = build_parser()
    args = parser.parse_args(["data", "features", "--universe", "nasdaq100"])

    assert args.command_family == "data"
    assert args.data_command == "features"
    assert args.universe == "nasdaq100"


def test_legacy_features_build_rewrites_to_data_features() -> None:
    argv, note = rewrite_legacy_cli_args(["features", "build", "--universe", "nasdaq100"])

    assert argv == ["data", "features", "--universe", "nasdaq100"]
    assert "data features" in str(note)


def test_grouped_research_run_command_parses_with_optional_dates() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "research",
            "run",
            "--symbols",
            "AAPL",
            "--strategy",
            "sma_cross",
            "--start",
            "2020-01-01",
            "--end",
            "2024-12-31",
        ]
    )

    assert args.command_family == "research"
    assert args.research_command == "run"
    assert args.symbols == ["AAPL"]
    assert args.start == "2020-01-01"
    assert args.end == "2024-12-31"


def test_grouped_research_run_command_parses_preset() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "research",
            "run",
            "--preset",
            "xsec_nasdaq100_momentum_v1_research",
        ]
    )

    assert args.research_command == "run"
    assert args.preset == "xsec_nasdaq100_momentum_v1_research"


def test_grouped_research_run_command_parses_breakout_parameters() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "research",
            "run",
            "--symbols",
            "AAPL",
            "--strategy",
            "breakout_hold",
            "--entry-lookback",
            "55",
            "--exit-lookback",
            "20",
            "--momentum-lookback",
            "63",
        ]
    )

    assert args.research_command == "run"
    assert args.strategy == "breakout_hold"
    assert args.entry_lookback == 55
    assert args.exit_lookback == 20
    assert args.momentum_lookback == 63


def test_grouped_research_run_command_parses_xsec_parameters() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "research",
            "run",
            "--symbols",
            "AAPL",
            "MSFT",
            "--strategy",
            "xsec_momentum_topn",
            "--lookback-bars",
            "126",
            "--skip-bars",
            "5",
            "--top-n",
            "2",
            "--rebalance-bars",
            "21",
            "--portfolio-construction-mode",
            "pure_topn",
            "--max-position-weight",
            "0.5",
            "--min-avg-dollar-volume",
            "50000000",
            "--max-names-per-sector",
            "1",
            "--turnover-buffer-bps",
            "25",
            "--max-turnover-per-rebalance",
            "0.5",
            "--weighting-scheme",
            "inv_vol",
            "--vol-lookback-bars",
            "20",
            "--cost-bps",
            "15",
            "--benchmark",
            "equal_weight",
        ]
    )

    assert args.strategy == "xsec_momentum_topn"
    assert args.lookback_bars == 126
    assert args.skip_bars == 5
    assert args.top_n == 2
    assert args.rebalance_bars == 21
    assert args.portfolio_construction_mode == "pure_topn"
    assert args.max_position_weight == 0.5
    assert args.min_avg_dollar_volume == 50_000_000
    assert args.max_names_per_sector == 1
    assert args.turnover_buffer_bps == 25.0
    assert args.max_turnover_per_rebalance == 0.5
    assert args.weighting_scheme == "inv_vol"
    assert args.vol_lookback_bars == 20
    assert args.cost_bps == 15.0
    assert args.benchmark == "equal_weight"


def test_grouped_research_sweep_command_parses_with_optional_dates() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "research",
            "sweep",
            "--symbols",
            "AAPL",
            "--strategy",
            "sma_cross",
            "--fast-values",
            "10",
            "20",
            "--slow-values",
            "100",
            "200",
            "--start",
            "2020-01-01",
            "--end",
            "2024-12-31",
        ]
    )

    assert args.command_family == "research"
    assert args.research_command == "sweep"
    assert args.fast_values == [10, 20]
    assert args.slow_values == [100, 200]
    assert args.start == "2020-01-01"
    assert args.end == "2024-12-31"


def test_grouped_research_sweep_command_parses_breakout_grid() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "research",
            "sweep",
            "--symbols",
            "AAPL",
            "--strategy",
            "breakout_hold",
            "--entry-lookback-values",
            "20",
            "55",
            "--exit-lookback-values",
            "10",
            "20",
            "--momentum-lookback-values",
            "63",
        ]
    )

    assert args.strategy == "breakout_hold"
    assert args.entry_lookback_values == [20, 55]
    assert args.exit_lookback_values == [10, 20]
    assert args.momentum_lookback_values == [63]


def test_grouped_research_sweep_command_parses_xsec_grid() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "research",
            "sweep",
            "--symbols",
            "AAPL",
            "MSFT",
            "--strategy",
            "xsec_momentum_topn",
            "--lookback-bars-values",
            "63",
            "126",
            "--skip-bars-values",
            "0",
            "5",
            "--top-n-values",
            "2",
            "3",
            "--rebalance-bars-values",
            "21",
            "42",
            "--portfolio-construction-mode",
            "transition",
            "--max-position-weight",
            "0.5",
            "--min-avg-dollar-volume",
            "50000000",
            "--max-names-per-sector",
            "1",
            "--turnover-buffer-bps",
            "25",
            "--max-turnover-per-rebalance",
            "0.5",
            "--weighting-scheme",
            "inv_vol",
            "--vol-lookback-bars",
            "20",
            "--benchmark",
            "equal_weight",
        ]
    )

    assert args.strategy == "xsec_momentum_topn"
    assert args.lookback_bars_values == [63, 126]
    assert args.skip_bars_values == [0, 5]
    assert args.top_n_values == [2, 3]
    assert args.rebalance_bars_values == [21, 42]
    assert args.portfolio_construction_mode == "transition"
    assert args.max_position_weight == 0.5
    assert args.min_avg_dollar_volume == 50_000_000
    assert args.max_names_per_sector == 1
    assert args.turnover_buffer_bps == 25.0
    assert args.max_turnover_per_rebalance == 0.5
    assert args.weighting_scheme == "inv_vol"
    assert args.vol_lookback_bars == 20
    assert args.benchmark == "equal_weight"


def test_grouped_research_validate_signal_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "research",
            "validate-signal",
            "--symbols",
            "AAPL",
            "--strategy",
            "sma_cross",
            "--fast-values",
            "10",
            "20",
            "--slow-values",
            "50",
            "100",
        ]
    )

    assert args.command_family == "research"
    assert args.research_command == "validate-signal"
    assert args.symbols == ["AAPL"]
    assert args.fast_values == [10, 20]
    assert args.slow_values == [50, 100]


def test_grouped_paper_run_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(["paper", "run", "--symbols", "AAPL", "--top-n", "1"])

    assert args.command_family == "paper"
    assert args.paper_command == "run"
    assert args.top_n == 1


def test_grouped_paper_run_command_parses_preset() -> None:
    parser = build_parser()
    args = parser.parse_args(["paper", "run", "--preset", "xsec_nasdaq100_momentum_v1_deploy"])

    assert args.paper_command == "run"
    assert args.preset == "xsec_nasdaq100_momentum_v1_deploy"


def test_grouped_paper_run_preset_scheduled_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(["paper", "run-preset-scheduled", "--preset", "xsec_nasdaq100_momentum_v1_deploy"])

    assert args.paper_command == "run-preset-scheduled"
    assert args.preset == "xsec_nasdaq100_momentum_v1_deploy"


def test_grouped_live_dry_run_command_parses_preset_and_output_dir() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "live",
            "dry-run",
            "--preset",
            "xsec_nasdaq100_momentum_v1_deploy",
            "--broker",
            "mock",
            "--output-dir",
            "artifacts/live_dry_run",
        ]
    )

    assert args.live_command == "dry-run"
    assert args.preset == "xsec_nasdaq100_momentum_v1_deploy"
    assert args.broker == "mock"
    assert args.output_dir == "artifacts/live_dry_run"


def test_grouped_live_run_preset_scheduled_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "live",
            "run-preset-scheduled",
            "--preset",
            "xsec_nasdaq100_momentum_v1_deploy",
            "--broker",
            "mock",
            "--output-dir",
            "artifacts/live_dry_run",
        ]
    )

    assert args.live_command == "run-preset-scheduled"
    assert args.preset == "xsec_nasdaq100_momentum_v1_deploy"
    assert args.output_dir == "artifacts/live_dry_run"


def test_legacy_alpha_command_rewrites_cleanly() -> None:
    argv, note = rewrite_legacy_cli_args(["alpha-research", "--symbols", "AAPL"])

    assert argv == ["research", "alpha", "--symbols", "AAPL"]
    assert "research alpha" in str(note)


def test_legacy_flat_research_command_rewrites_to_grouped_run() -> None:
    argv, note = rewrite_legacy_cli_args(["research", "--symbols", "AAPL"])

    assert argv == ["research", "run", "--symbols", "AAPL"]
    assert "research run" in str(note)


def test_grouped_research_walkforward_command_parses_with_optional_dates() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "research",
            "walkforward",
            "--symbols",
            "AAPL",
            "--strategy",
            "sma_cross",
            "--fast-values",
            "10",
            "20",
            "--slow-values",
            "100",
            "200",
            "--start",
            "2020-01-01",
            "--end",
            "2024-12-31",
            "--train-bars",
            "365",
            "--test-bars",
            "90",
            "--step-bars",
            "90",
        ]
    )

    assert args.research_command == "walkforward"
    assert args.start == "2020-01-01"
    assert args.end == "2024-12-31"
    assert args.train_bars == 365
    assert args.test_bars == 90
    assert args.step_bars == 90


def test_grouped_research_walkforward_command_parses_breakout_grid() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "research",
            "walkforward",
            "--symbols",
            "AAPL",
            "--strategy",
            "breakout_hold",
            "--entry-lookback-values",
            "20",
            "55",
            "--exit-lookback-values",
            "10",
            "20",
            "--momentum-lookback-values",
            "63",
            "--train-bars",
            "252",
            "--test-bars",
            "63",
        ]
    )

    assert args.strategy == "breakout_hold"
    assert args.entry_lookback_values == [20, 55]
    assert args.exit_lookback_values == [10, 20]
    assert args.momentum_lookback_values == [63]


def test_grouped_research_walkforward_command_parses_xsec_grid() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "research",
            "walkforward",
            "--symbols",
            "AAPL",
            "MSFT",
            "NVDA",
            "--strategy",
            "xsec_momentum_topn",
            "--lookback-bars-values",
            "63",
            "126",
            "--skip-bars-values",
            "0",
            "5",
            "--top-n-values",
            "2",
            "--rebalance-bars-values",
            "21",
            "--portfolio-construction-mode",
            "transition",
            "--max-position-weight",
            "0.5",
            "--min-avg-dollar-volume",
            "50000000",
            "--max-names-per-sector",
            "1",
            "--turnover-buffer-bps",
            "25",
            "--max-turnover-per-rebalance",
            "0.5",
            "--weighting-scheme",
            "inv_vol",
            "--vol-lookback-bars",
            "20",
            "--train-bars",
            "252",
            "--test-bars",
            "63",
            "--cost-bps",
            "10",
            "--benchmark",
            "equal_weight",
        ]
    )

    assert args.strategy == "xsec_momentum_topn"
    assert args.lookback_bars_values == [63, 126]
    assert args.skip_bars_values == [0, 5]
    assert args.top_n_values == [2]
    assert args.rebalance_bars_values == [21]
    assert args.portfolio_construction_mode == "transition"
    assert args.max_position_weight == 0.5
    assert args.min_avg_dollar_volume == 50_000_000
    assert args.max_names_per_sector == 1
    assert args.turnover_buffer_bps == 25.0
    assert args.max_turnover_per_rebalance == 0.5
    assert args.weighting_scheme == "inv_vol"
    assert args.vol_lookback_bars == 20
    assert args.cost_bps == 10.0
    assert args.benchmark == "equal_weight"


def test_grouped_research_compare_xsec_construction_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "research",
            "compare-xsec-construction",
            "--universe",
            "nasdaq100",
            "--strategy",
            "xsec_momentum_topn",
            "--lookback-bars-values",
            "84",
            "--skip-bars-values",
            "21",
            "--top-n-values",
            "2",
            "--rebalance-bars-values",
            "21",
            "--train-bars",
            "756",
            "--test-bars",
            "126",
            "--step-bars",
            "126",
            "--portfolio-construction-mode",
            "pure_topn",
            "--output-dir",
            "artifacts/experiments",
        ]
    )

    assert args.research_command == "compare-xsec-construction"
    assert args.universe == "nasdaq100"
    assert args.strategy == "xsec_momentum_topn"
    assert args.lookback_bars_values == [84]
    assert args.skip_bars_values == [21]
    assert args.top_n_values == [2]
    assert args.rebalance_bars_values == [21]
    assert args.portfolio_construction_mode == "pure_topn"
    assert args.output_dir == "artifacts/experiments"


def test_grouped_research_decision_memo_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "research",
            "decision-memo",
            "--preset",
            "xsec_nasdaq100_momentum_v1_research",
            "--deploy-preset",
            "xsec_nasdaq100_momentum_v1_deploy",
        ]
    )

    assert args.research_command == "decision-memo"
    assert args.preset == "xsec_nasdaq100_momentum_v1_research"
    assert args.deploy_preset == "xsec_nasdaq100_momentum_v1_deploy"


def test_grouped_research_walkforward_command_parses_compatibility_day_aliases() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "research",
            "walkforward",
            "--symbols",
            "AAPL",
            "--strategy",
            "sma_cross",
            "--fast-values",
            "10",
            "--slow-values",
            "100",
            "--train-period-days",
            "365",
            "--test-period-days",
            "90",
            "--step-days",
            "90",
        ]
    )

    assert args.train_period_days == 365
    assert args.test_period_days == 90
    assert args.step_days == 90
