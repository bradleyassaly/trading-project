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


def test_grouped_research_registry_build_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "research",
            "registry",
            "build",
            "--artifacts-root",
            "artifacts",
            "--output-dir",
            "artifacts/research_registry",
        ]
    )

    assert args.command_family == "research"
    assert args.research_command == "registry"
    assert args.research_registry_command == "build"
    assert args.output_dir == "artifacts/research_registry"


def test_grouped_research_leaderboard_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "research",
            "leaderboard",
            "--artifacts-root",
            "artifacts",
            "--output-dir",
            "artifacts/research_registry",
            "--metric",
            "portfolio_sharpe",
            "--group-by",
            "signal_family",
            "--limit",
            "5",
        ]
    )

    assert args.research_command == "leaderboard"
    assert args.metric == "portfolio_sharpe"
    assert args.group_by == "signal_family"
    assert args.limit == 5


def test_grouped_research_compare_runs_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "research",
            "compare-runs",
            "--artifacts-root",
            "artifacts",
            "--run-id-a",
            "run_a",
            "--run-id-b",
            "run_b",
            "--output-dir",
            "artifacts/research_compare",
        ]
    )

    assert args.research_command == "compare-runs"
    assert args.run_id_a == "run_a"
    assert args.run_id_b == "run_b"


def test_grouped_research_promotion_candidates_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "research",
            "promotion-candidates",
            "--artifacts-root",
            "artifacts",
            "--output-dir",
            "artifacts/research_candidates",
        ]
    )

    assert args.research_command == "promotion-candidates"
    assert args.output_dir == "artifacts/research_candidates"


def test_grouped_research_promote_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "research",
            "promote",
            "--artifacts-root",
            "artifacts",
            "--registry-dir",
            "artifacts/research_registry",
            "--output-dir",
            "configs/generated_strategies",
            "--policy-config",
            "configs/promotion.yaml",
            "--top-n",
            "2",
            "--dry-run",
        ]
    )

    assert args.research_command == "promote"
    assert args.registry_dir == "artifacts/research_registry"
    assert args.output_dir == "configs/generated_strategies"
    assert args.policy_config == "configs/promotion.yaml"
    assert args.top_n == 2
    assert args.dry_run is True


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


def test_grouped_live_submit_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "live",
            "submit",
            "--symbols",
            "AAPL",
            "--broker-config",
            "configs/broker.yaml",
            "--validate-only",
        ]
    )

    assert args.command_family == "live"
    assert args.live_command == "submit"
    assert args.broker_config == "configs/broker.yaml"
    assert args.validate_only is True


def test_grouped_live_submit_multi_strategy_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "live",
            "submit-multi-strategy",
            "--config",
            "configs/multi_strategy.yaml",
            "--broker-config",
            "configs/broker.yaml",
            "--output-dir",
            "artifacts/live_submit",
        ]
    )

    assert args.command_family == "live"
    assert args.live_command == "submit-multi-strategy"
    assert args.broker_config == "configs/broker.yaml"
    assert args.output_dir == "artifacts/live_submit"


def test_grouped_broker_health_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "broker",
            "health",
            "--broker-config",
            "configs/broker.yaml",
        ]
    )

    assert args.command_family == "broker"
    assert args.broker_command == "health"
    assert args.broker_config == "configs/broker.yaml"


def test_grouped_dashboard_serve_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "dashboard",
            "serve",
            "--artifacts-root",
            "artifacts",
            "--host",
            "127.0.0.1",
            "--port",
            "8123",
        ]
    )

    assert args.command_family == "dashboard"
    assert args.dashboard_command == "serve"
    assert args.port == 8123


def test_grouped_dashboard_build_static_data_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "dashboard",
            "build-static-data",
            "--artifacts-root",
            "artifacts",
            "--output-dir",
            "artifacts/dashboard_data",
        ]
    )

    assert args.command_family == "dashboard"
    assert args.dashboard_command == "build-static-data"
    assert args.output_dir == "artifacts/dashboard_data"


def test_grouped_strategy_portfolio_build_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "strategy-portfolio",
            "build",
            "--promoted-dir",
            "configs/generated_strategies",
            "--policy-config",
            "configs/strategy_portfolio.yaml",
            "--output-dir",
            "artifacts/strategy_portfolio",
        ]
    )

    assert args.command_family == "strategy-portfolio"
    assert args.strategy_portfolio_command == "build"
    assert args.promoted_dir == "configs/generated_strategies"


def test_grouped_strategy_portfolio_show_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "strategy-portfolio",
            "show",
            "--portfolio",
            "artifacts/strategy_portfolio",
        ]
    )

    assert args.strategy_portfolio_command == "show"
    assert args.portfolio == "artifacts/strategy_portfolio"


def test_grouped_strategy_portfolio_export_run_config_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "strategy-portfolio",
            "export-run-config",
            "--portfolio",
            "artifacts/strategy_portfolio",
            "--output-dir",
            "artifacts/strategy_portfolio_run",
        ]
    )

    assert args.strategy_portfolio_command == "export-run-config"
    assert args.output_dir == "artifacts/strategy_portfolio_run"


def test_grouped_strategy_monitor_build_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "strategy-monitor",
            "build",
            "--portfolio",
            "artifacts/strategy_portfolio",
            "--paper-dir",
            "artifacts/paper/strategy_portfolio",
            "--policy-config",
            "configs/strategy_monitoring.yaml",
            "--output-dir",
            "artifacts/strategy_monitoring",
        ]
    )

    assert args.command_family == "strategy-monitor"
    assert args.strategy_monitor_command == "build"
    assert args.paper_dir == "artifacts/paper/strategy_portfolio"


def test_grouped_strategy_monitor_show_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "strategy-monitor",
            "show",
            "--monitoring",
            "artifacts/strategy_monitoring",
        ]
    )

    assert args.command_family == "strategy-monitor"
    assert args.strategy_monitor_command == "show"
    assert args.monitoring == "artifacts/strategy_monitoring"


def test_grouped_strategy_monitor_recommend_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "strategy-monitor",
            "recommend-kill-switch",
            "--monitoring",
            "artifacts/strategy_monitoring",
            "--output-dir",
            "artifacts/strategy_monitoring_recommendations",
            "--include-review",
        ]
    )

    assert args.command_family == "strategy-monitor"
    assert args.strategy_monitor_command == "recommend-kill-switch"
    assert args.include_review is True


def test_grouped_orchestrate_run_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "orchestrate",
            "run",
            "--config",
            "configs/orchestration.yaml",
        ]
    )

    assert args.command_family == "orchestrate"
    assert args.orchestrate_command == "run"
    assert args.config == "configs/orchestration.yaml"


def test_grouped_orchestrate_show_run_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "orchestrate",
            "show-run",
            "--run",
            "artifacts/orchestration_runs/latest",
        ]
    )

    assert args.orchestrate_command == "show-run"
    assert args.run == "artifacts/orchestration_runs/latest"


def test_grouped_orchestrate_loop_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "orchestrate",
            "loop",
            "--config",
            "configs/orchestration.yaml",
            "--max-iterations",
            "2",
        ]
    )

    assert args.orchestrate_command == "loop"
    assert args.max_iterations == 2


def test_grouped_doctor_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "doctor",
            "--artifacts-root",
            "artifacts",
            "--monitoring-config",
            "configs/monitoring.yaml",
            "--execution-config",
            "configs/execution.yaml",
            "--output-dir",
            "artifacts/system_check",
        ]
    )

    assert args.command_family == "doctor"
    assert args.artifacts_root == "artifacts"
    assert args.monitoring_config == "configs/monitoring.yaml"
    assert args.execution_config == "configs/execution.yaml"
    assert args.output_dir == "artifacts/system_check"


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


def test_grouped_paper_run_command_parses_execution_config() -> None:
    parser = build_parser()
    args = parser.parse_args(["paper", "run", "--symbols", "AAPL", "--execution-config", "configs/execution.yaml"])

    assert args.execution_config == "configs/execution.yaml"


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


def test_grouped_paper_run_multi_strategy_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "paper",
            "run-multi-strategy",
            "--config",
            "configs/multi_strategy.json",
            "--state-path",
            "artifacts/paper/state.json",
            "--output-dir",
            "artifacts/paper/multi",
        ]
    )

    assert args.paper_command == "run-multi-strategy"
    assert args.config == "configs/multi_strategy.json"


def test_grouped_paper_run_multi_strategy_command_parses_execution_config() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "paper",
            "run-multi-strategy",
            "--config",
            "configs/multi_strategy.json",
            "--execution-config",
            "configs/execution.json",
            "--state-path",
            "artifacts/paper/state.json",
            "--output-dir",
            "artifacts/paper/multi",
        ]
    )

    assert args.execution_config == "configs/execution.json"


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


def test_grouped_live_dry_run_command_parses_execution_config() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "live",
            "dry-run",
            "--symbols",
            "AAPL",
            "--execution-config",
            "configs/execution.yaml",
        ]
    )

    assert args.execution_config == "configs/execution.yaml"


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


def test_grouped_live_dry_run_multi_strategy_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "live",
            "dry-run-multi-strategy",
            "--config",
            "configs/multi_strategy.json",
            "--broker",
            "mock",
            "--output-dir",
            "artifacts/live_dry_run/multi",
        ]
    )

    assert args.live_command == "dry-run-multi-strategy"
    assert args.config == "configs/multi_strategy.json"
    assert args.output_dir == "artifacts/live_dry_run/multi"


def test_grouped_live_dry_run_multi_strategy_command_parses_execution_config() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "live",
            "dry-run-multi-strategy",
            "--config",
            "configs/multi_strategy.json",
            "--execution-config",
            "configs/execution.json",
            "--broker",
            "mock",
            "--output-dir",
            "artifacts/live_dry_run/multi",
        ]
    )

    assert args.execution_config == "configs/execution.json"


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


def test_grouped_portfolio_allocate_multi_strategy_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "portfolio",
            "allocate-multi-strategy",
            "--config",
            "configs/multi_strategy.json",
            "--output-dir",
            "artifacts/portfolio/multi",
        ]
    )

    assert args.portfolio_command == "allocate-multi-strategy"
    assert args.config == "configs/multi_strategy.json"


def test_grouped_portfolio_apply_execution_constraints_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "portfolio",
            "apply-execution-constraints",
            "--config",
            "configs/execution.json",
            "--allocation-dir",
            "artifacts/portfolio/multi",
            "--output-dir",
            "artifacts/execution",
        ]
    )

    assert args.portfolio_command == "apply-execution-constraints"
    assert args.config == "configs/execution.json"
    assert args.allocation_dir == "artifacts/portfolio/multi"


def test_grouped_registry_list_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(["registry", "list", "--registry", "artifacts/registry.json"])

    assert args.command_family == "registry"
    assert args.registry_command == "list"
    assert args.registry == "artifacts/registry.json"


def test_grouped_registry_evaluate_promotion_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "registry",
            "evaluate-promotion",
            "--registry",
            "artifacts/registry.json",
            "--strategy-id",
            "xsec-v1",
            "--config",
            "configs/governance.json",
            "--output-dir",
            "artifacts/registry_eval",
        ]
    )

    assert args.registry_command == "evaluate-promotion"
    assert args.strategy_id == "xsec-v1"


def test_grouped_registry_build_multi_strategy_config_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "registry",
            "build-multi-strategy-config",
            "--registry",
            "artifacts/registry.json",
            "--output-path",
            "artifacts/multi_strategy.json",
            "--include-paper",
            "--weighting-scheme",
            "score_weighted",
        ]
    )

    assert args.registry_command == "build-multi-strategy-config"
    assert args.include_paper is True
    assert args.weighting_scheme == "score_weighted"


def test_grouped_pipeline_run_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "pipeline",
            "run",
            "--config",
            "configs/pipeline.yaml",
        ]
    )

    assert args.command_family == "pipeline"
    assert args.pipeline_command == "run"
    assert args.config == "configs/pipeline.yaml"


def test_grouped_pipeline_run_daily_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "pipeline",
            "run-daily",
            "--config",
            "configs/pipeline_daily.yaml",
        ]
    )

    assert args.command_family == "pipeline"
    assert args.pipeline_command == "run-daily"
    assert args.config == "configs/pipeline_daily.yaml"


def test_grouped_pipeline_run_weekly_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "pipeline",
            "run-weekly",
            "--config",
            "configs/pipeline_weekly.yaml",
        ]
    )

    assert args.command_family == "pipeline"
    assert args.pipeline_command == "run-weekly"
    assert args.config == "configs/pipeline_weekly.yaml"


def test_grouped_monitor_run_health_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "monitor",
            "run-health",
            "--run-dir",
            "artifacts/orchestration/run_a",
            "--config",
            "configs/monitoring.yaml",
        ]
    )

    assert args.command_family == "monitor"
    assert args.monitor_command == "run-health"
    assert args.run_dir == "artifacts/orchestration/run_a"


def test_grouped_monitor_strategy_health_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "monitor",
            "strategy-health",
            "--registry",
            "artifacts/registry.json",
            "--artifacts-root",
            "artifacts",
            "--config",
            "configs/monitoring.yaml",
            "--output-dir",
            "artifacts/monitoring/strategy",
        ]
    )

    assert args.monitor_command == "strategy-health"
    assert args.registry == "artifacts/registry.json"
    assert args.artifacts_root == "artifacts"


def test_grouped_monitor_latest_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "monitor",
            "latest",
            "--pipeline-root",
            "artifacts/orchestration",
            "--config",
            "configs/monitoring.yaml",
            "--output-dir",
            "artifacts/monitoring/latest",
        ]
    )

    assert args.monitor_command == "latest"
    assert args.pipeline_root == "artifacts/orchestration"


def test_grouped_monitor_notify_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "monitor",
            "notify",
            "--alerts",
            "artifacts/monitoring/alerts.json",
            "--config",
            "configs/notifications.yaml",
        ]
    )

    assert args.monitor_command == "notify"
    assert args.alerts == "artifacts/monitoring/alerts.json"


def test_grouped_execution_simulate_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "execution",
            "simulate",
            "--config",
            "configs/execution.yaml",
            "--targets",
            "artifacts/targets.csv",
            "--output-dir",
            "artifacts/execution",
        ]
    )

    assert args.command_family == "execution"
    assert args.execution_command == "simulate"
    assert args.targets == "artifacts/targets.csv"
