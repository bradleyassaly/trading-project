from __future__ import annotations

from trading_platform.cli.grouped_parser import build_parser, rewrite_legacy_cli_args


def test_grouped_data_ingest_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(["data", "ingest", "--symbols", "AAPL"])

    assert args.command_family == "data"
    assert args.data_command == "ingest"
    assert args.symbols == ["AAPL"]


def test_grouped_research_alpha_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(["research", "alpha", "--symbols", "AAPL", "--lookbacks", "5"])

    assert args.command_family == "research"
    assert args.research_command == "alpha"
    assert args.symbols == ["AAPL"]
    assert args.lookbacks == [5]


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
        ]
    )

    assert args.strategy == "xsec_momentum_topn"
    assert args.lookback_bars == 126
    assert args.skip_bars == 5
    assert args.top_n == 2
    assert args.rebalance_bars == 21


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
        ]
    )

    assert args.strategy == "xsec_momentum_topn"
    assert args.lookback_bars_values == [63, 126]
    assert args.skip_bars_values == [0, 5]
    assert args.top_n_values == [2, 3]
    assert args.rebalance_bars_values == [21, 42]


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
            "--train-bars",
            "252",
            "--test-bars",
            "63",
        ]
    )

    assert args.strategy == "xsec_momentum_topn"
    assert args.lookback_bars_values == [63, 126]
    assert args.skip_bars_values == [0, 5]
    assert args.top_n_values == [2]
    assert args.rebalance_bars_values == [21]


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
