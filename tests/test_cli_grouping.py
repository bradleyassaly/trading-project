from __future__ import annotations

from trading_platform.cli.grouped_parser import build_parser, rewrite_legacy_cli_args


def test_grouped_data_ingest_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        ["data", "ingest", "--symbols", "AAPL", "--failure-report", "artifacts/ingest_failures.csv"]
    )

    assert args.command_family == "data"
    assert args.data_command == "ingest"
    assert args.symbols == ["AAPL"]
    assert args.failure_report == "artifacts/ingest_failures.csv"


def test_grouped_research_alpha_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "research",
            "alpha",
            "--symbols",
            "AAPL",
            "--lookbacks",
            "5",
            "--candidate-grid-preset",
            "broad_v1",
            "--signal-composition-preset",
            "composite_v1",
            "--max-variants-per-family",
            "4",
        ]
    )

    assert args.command_family == "research"
    assert args.research_command == "alpha"
    assert args.symbols == ["AAPL"]
    assert args.lookbacks == [5]
    assert args.candidate_grid_preset == "broad_v1"
    assert args.signal_composition_preset == "composite_v1"
    assert args.max_variants_per_family == 4


def test_grouped_research_alpha_command_parses_database_tracking_args() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "research",
            "alpha",
            "--symbols",
            "AAPL",
            "--database-enabled",
            "--database-url",
            "sqlite:///research.db",
            "--database-schema",
            "research",
            "--tracking-skip-candidates",
        ]
    )

    assert args.enable_database_metadata is True
    assert args.database_url == "sqlite:///research.db"
    assert args.database_schema == "research"
    assert args.tracking_write_candidates is False


def test_grouped_research_alpha_command_parses_signal_families() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "research",
            "alpha",
            "--symbols",
            "AAPL",
            "--signal-families",
            "momentum",
            "fundamental_value",
        ]
    )

    assert args.signal_families == ["momentum", "fundamental_value"]


def test_grouped_research_alpha_command_parses_config() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "research",
            "alpha",
            "--config",
            "configs/alpha_research.yaml",
        ]
    )

    assert args.command_family == "research"
    assert args.research_command == "alpha"
    assert args.config == "configs/alpha_research.yaml"


def test_grouped_research_alpha_command_parses_external_diagnostics_args() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "research",
            "alpha",
            "--symbols",
            "AAPL",
            "--diagnostics-alphalens-enabled",
            "--diagnostics-alphalens-groupby-field",
            "sector",
            "--diagnostics-classification-path",
            "artifacts/reference/classifications/security_master.csv",
            "--reporting-quantstats-enabled",
        ]
    )

    assert args.diagnostics_alphalens_enabled is True
    assert args.diagnostics_alphalens_groupby_field == "sector"
    assert args.reporting_quantstats_enabled is True


def test_grouped_research_alpha_command_parses_ensemble_args() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "research",
            "alpha",
            "--symbols",
            "AAPL",
            "--enable-ensemble",
            "--ensemble-mode",
            "family_weighted",
            "--ensemble-weight-method",
            "rank_weighted",
            "--ensemble-normalize-scores",
            "zscore",
            "--ensemble-max-members",
            "4",
        ]
    )

    assert args.enable_ensemble is True
    assert args.ensemble_mode == "family_weighted"
    assert args.ensemble_weight_method == "rank_weighted"
    assert args.ensemble_normalize_scores == "zscore"
    assert args.ensemble_max_members == 4


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
            "--output-dir",
            "configs/generated_strategies",
            "--policy-config",
            "configs/promotion.yaml",
            "--validation",
            "artifacts/research_registry",
            "--top-n",
            "2",
            "--dry-run",
            "--override-validation",
        ]
    )

    assert args.research_command == "promote"
    assert args.registry_dir is None
    assert args.output_dir == "configs/generated_strategies"
    assert args.policy_config == "configs/promotion.yaml"
    assert args.validation == "artifacts/research_registry"


def test_grouped_ops_pipeline_daily_trading_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "ops",
            "pipeline",
            "daily-trading",
            "--config",
            "configs/pipeline_daily.yaml",
        ]
    )

    assert args.command_family == "ops"
    assert args.ops_command == "pipeline"
    assert args.ops_pipeline_command == "daily-trading"
    assert args.config == "configs/pipeline_daily.yaml"


def test_grouped_research_validate_backtester_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "research",
            "validate-backtester",
            "--output-dir",
            "artifacts/validation/vectorbt",
        ]
    )

    assert args.research_command == "validate-backtester"
    assert args.output_dir == "artifacts/validation/vectorbt"


def test_grouped_research_db_commands_parse() -> None:
    parser = build_parser()
    init_args = parser.parse_args(
        ["research", "db", "init", "--database-enabled", "--database-url", "sqlite:///research.db"]
    )
    list_args = parser.parse_args(
        ["research", "db", "list-runs", "--database-enabled", "--database-url", "sqlite:///research.db", "--limit", "5"]
    )

    assert init_args.research_command == "db"
    assert init_args.research_db_command == "init"
    assert init_args.enable_database_metadata is True
    assert list_args.research_db_command == "list-runs"
    assert list_args.limit == 5


def test_rewrite_legacy_cli_args_does_not_rewrite_research_db_command() -> None:
    rewritten, note = rewrite_legacy_cli_args(["research", "db", "init"])

    assert rewritten == ["research", "db", "init"]
    assert note is None


def test_grouped_data_features_command_parses_for_universe() -> None:
    parser = build_parser()
    args = parser.parse_args(["data", "features", "--universe", "nasdaq100"])

    assert args.command_family == "data"
    assert args.data_command == "features"
    assert args.universe == "nasdaq100"


def test_grouped_data_refresh_research_inputs_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "data",
            "refresh-research-inputs",
            "--universe",
            "nasdaq100",
            "--sub-universe-id",
            "liquid_trend_candidates",
            "--reference-data-root",
            "artifacts/reference_data/v1",
        ]
    )

    assert args.command_family == "data"
    assert args.data_command == "refresh-research-inputs"
    assert args.universe == "nasdaq100"
    assert args.sub_universe_id == "liquid_trend_candidates"
    assert args.reference_data_root == "artifacts/reference_data/v1"


def test_grouped_data_refresh_research_inputs_command_parses_fundamentals_args() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "data",
            "refresh-research-inputs",
            "--symbols",
            "AAPL",
            "--fundamentals-enabled",
            "--fundamentals-providers",
            "sec",
            "vendor",
            "--fundamentals-artifact-root",
            "data/fundamentals",
            "--fundamentals-vendor-cache-root",
            "data/fundamentals/raw_fmp",
            "--fundamentals-vendor-cache-ttl-hours",
            "48",
            "--fundamentals-vendor-force-refresh",
            "--fundamentals-vendor-request-delay-seconds",
            "1.5",
            "--fundamentals-vendor-max-retries",
            "6",
            "--fundamentals-vendor-max-symbols-per-run",
            "25",
        ]
    )

    assert args.fundamentals_enabled is True
    assert args.fundamentals_providers == ["sec", "vendor"]
    assert args.fundamentals_artifact_root == "data/fundamentals"
    assert args.fundamentals_vendor_cache_root == "data/fundamentals/raw_fmp"
    assert args.fundamentals_vendor_cache_ttl_hours == 48.0
    assert args.fundamentals_vendor_force_refresh is True
    assert args.fundamentals_vendor_request_delay_seconds == 1.5
    assert args.fundamentals_vendor_max_retries == 6
    assert args.fundamentals_vendor_max_symbols_per_run == 25


def test_grouped_data_refresh_research_inputs_command_parses_config() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "data",
            "refresh-research-inputs",
            "--config",
            "configs/research_input_refresh.yaml",
        ]
    )

    assert args.command_family == "data"
    assert args.data_command == "refresh-research-inputs"
    assert args.config == "configs/research_input_refresh.yaml"


def test_grouped_data_build_classifications_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "data",
            "build-classifications",
            "--universe",
            "nasdaq100",
            "--output-dir",
            "artifacts/reference/classifications",
            "--as-of-date",
            "2026-03-27",
        ]
    )

    assert args.command_family == "data"
    assert args.data_command == "build-classifications"
    assert args.universe == "nasdaq100"
    assert args.as_of_date == "2026-03-27"


def test_grouped_data_fundamentals_commands_parse() -> None:
    parser = build_parser()
    ingest_args = parser.parse_args(
        [
            "data",
            "fundamentals",
            "ingest",
            "--symbols",
            "AAPL",
            "--providers",
            "vendor",
            "--fundamentals-vendor-api-key",
            "test-key",
            "--vendor-cache-root",
            "data/fundamentals/raw_fmp",
            "--vendor-max-symbols-per-run",
            "10",
        ]
    )
    features_args = parser.parse_args(
        [
            "data",
            "fundamentals",
            "features",
            "--symbols",
            "AAPL",
            "--artifact-root",
            "data/fundamentals",
            "--calendar-dir",
            "data/features",
        ]
    )

    assert ingest_args.data_command == "fundamentals"
    assert ingest_args.fundamentals_command == "ingest"
    assert ingest_args.providers == ["vendor"]
    assert ingest_args.vendor_api_key == "test-key"
    assert ingest_args.vendor_cache_root == "data/fundamentals/raw_fmp"
    assert ingest_args.vendor_max_symbols_per_run == 10
    assert features_args.fundamentals_command == "features"
    assert features_args.calendar_dir == "data/features"


def test_grouped_data_fundamentals_snapshot_build_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "data",
            "fundamentals",
            "snapshot-build",
            "--config",
            "configs/fundamentals_snapshot.yaml",
            "--offline",
            "--max-symbols-per-run",
            "25",
        ]
    )

    assert args.data_command == "fundamentals"
    assert args.fundamentals_command == "snapshot-build"
    assert args.config == "configs/fundamentals_snapshot.yaml"
    assert args.offline is True
    assert args.max_symbols_per_run == 25


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
            "--lifecycle",
            "artifacts/governance",
            "--output-dir",
            "artifacts/strategy_portfolio",
        ]
    )

    assert args.command_family == "strategy-portfolio"
    assert args.strategy_portfolio_command == "build"
    assert args.promoted_dir == "configs/generated_strategies"
    assert args.lifecycle == "artifacts/governance"


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


def test_grouped_strategy_portfolio_experiment_bundle_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "strategy-portfolio",
            "experiment-bundle",
            "--config",
            "configs/canonical_bundle_experiment.yaml",
        ]
    )

    assert args.strategy_portfolio_command == "experiment-bundle"
    assert args.config == "configs/canonical_bundle_experiment.yaml"


def test_grouped_strategy_portfolio_experiment_bundle_matrix_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "strategy-portfolio",
            "experiment-bundle-matrix",
            "--config",
            "configs/canonical_bundle_experiment_matrix.yaml",
        ]
    )

    assert args.strategy_portfolio_command == "experiment-bundle-matrix"
    assert args.config == "configs/canonical_bundle_experiment_matrix.yaml"


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


def test_grouped_adaptive_allocation_build_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "adaptive-allocation",
            "build",
            "--portfolio",
            "artifacts/strategy_portfolio",
            "--monitoring",
            "artifacts/strategy_monitoring",
            "--lifecycle",
            "artifacts/governance",
            "--regime",
            "artifacts/regime",
            "--use-regime",
            "--policy-config",
            "configs/adaptive_allocation.yaml",
            "--output-dir",
            "artifacts/adaptive_allocation",
            "--dry-run",
        ]
    )

    assert args.command_family == "adaptive-allocation"
    assert args.adaptive_allocation_command == "build"
    assert args.lifecycle == "artifacts/governance"
    assert args.regime == "artifacts/regime"
    assert args.use_regime is True
    assert args.policy_config == "configs/adaptive_allocation.yaml"
    assert args.output_dir == "artifacts/adaptive_allocation"
    assert args.dry_run is True


def test_grouped_adaptive_allocation_show_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "adaptive-allocation",
            "show",
            "--allocation",
            "artifacts/adaptive_allocation",
        ]
    )

    assert args.command_family == "adaptive-allocation"
    assert args.adaptive_allocation_command == "show"
    assert args.allocation == "artifacts/adaptive_allocation"


def test_grouped_adaptive_allocation_export_run_config_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "adaptive-allocation",
            "export-run-config",
            "--allocation",
            "artifacts/adaptive_allocation",
            "--output-dir",
            "artifacts/adaptive_allocation_run",
        ]
    )

    assert args.command_family == "adaptive-allocation"
    assert args.adaptive_allocation_command == "export-run-config"
    assert args.output_dir == "artifacts/adaptive_allocation_run"


def test_grouped_regime_detect_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "regime",
            "detect",
            "--input",
            "artifacts/paper/paper_equity_curve.csv",
            "--policy-config",
            "configs/market_regime.yaml",
            "--output-dir",
            "artifacts/regime",
        ]
    )

    assert args.command_family == "regime"
    assert args.regime_command == "detect"
    assert args.input == "artifacts/paper/paper_equity_curve.csv"
    assert args.policy_config == "configs/market_regime.yaml"


def test_grouped_regime_show_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "regime",
            "show",
            "--regime",
            "artifacts/regime",
        ]
    )

    assert args.command_family == "regime"
    assert args.regime_command == "show"
    assert args.regime == "artifacts/regime"


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


def test_grouped_experiment_run_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "experiment",
            "run",
            "--config",
            "configs/experiments.yaml",
            "--variants",
            "adaptive_on",
            "adaptive_off",
            "--dry-run",
        ]
    )

    assert args.command_family == "experiment"
    assert args.experiment_command == "run"
    assert args.config == "configs/experiments.yaml"
    assert args.variants == ["adaptive_on", "adaptive_off"]
    assert args.dry_run is True


def test_grouped_experiment_show_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "experiment",
            "show",
            "--run",
            "artifacts/experiments/demo/2026-03-22T00-00-00+00-00",
        ]
    )

    assert args.command_family == "experiment"
    assert args.experiment_command == "show"
    assert args.run == "artifacts/experiments/demo/2026-03-22T00-00-00+00-00"


def test_grouped_experiment_compare_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "experiment",
            "compare",
            "--run",
            "artifacts/experiments/demo/2026-03-22T00-00-00+00-00",
            "--output-dir",
            "artifacts/experiments/demo/compare",
            "--variant-a",
            "adaptive_on",
            "--variant-b",
            "adaptive_off",
        ]
    )

    assert args.command_family == "experiment"
    assert args.experiment_command == "compare"
    assert args.variant_a == "adaptive_on"
    assert args.variant_b == "adaptive_off"


def test_grouped_experiment_summarize_campaign_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "experiment",
            "summarize-campaign",
            "--runs",
            "artifacts/experiments/campaign_regime_on_off/run_a",
            "artifacts/experiments/campaign_adaptive_on_off/run_b",
            "--output-dir",
            "artifacts/experiments/campaign_summary",
        ]
    )

    assert args.command_family == "experiment"
    assert args.experiment_command == "summarize-campaign"
    assert len(args.runs) == 2
    assert args.output_dir == "artifacts/experiments/campaign_summary"


def test_grouped_experiment_recommend_defaults_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "experiment",
            "recommend-defaults",
            "--summary",
            "artifacts/experiments/first_campaign_summary",
            "--output-dir",
            "artifacts/experiments/default_recommendations",
            "--write-config",
            "configs/orchestration_recommended_defaults.yaml",
            "--base-config",
            "configs/orchestration_experiment_base.yaml",
        ]
    )

    assert args.command_family == "experiment"
    assert args.experiment_command == "recommend-defaults"
    assert args.summary == "artifacts/experiments/first_campaign_summary"
    assert args.write_config == "configs/orchestration_recommended_defaults.yaml"


def test_grouped_system_eval_build_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "system-eval",
            "build",
            "--runs-root",
            "artifacts/orchestration_runs",
            "--output-dir",
            "artifacts/system_eval",
        ]
    )

    assert args.command_family == "system-eval"
    assert args.system_eval_command == "build"
    assert args.runs_root == "artifacts/orchestration_runs"


def test_grouped_system_eval_show_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "system-eval",
            "show",
            "--evaluation",
            "artifacts/system_eval",
        ]
    )

    assert args.command_family == "system-eval"
    assert args.system_eval_command == "show"
    assert args.evaluation == "artifacts/system_eval"


def test_grouped_system_eval_compare_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "system-eval",
            "compare",
            "--history",
            "artifacts/system_eval",
            "--output-dir",
            "artifacts/system_eval_compare",
            "--latest-count",
            "5",
            "--previous-count",
            "5",
            "--feature-flag",
            "regime",
            "--group-by-field",
            "variant_name",
            "--value-a",
            "true",
            "--value-b",
            "false",
        ]
    )

    assert args.command_family == "system-eval"
    assert args.system_eval_command == "compare"
    assert args.feature_flag == "regime"
    assert args.group_by_field == "variant_name"
    assert args.latest_count == 5


def test_grouped_strategy_validation_build_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "strategy-validation",
            "build",
            "--artifacts-root",
            "artifacts",
            "--policy-config",
            "configs/strategy_validation.yaml",
            "--output-dir",
            "artifacts/strategy_validation",
        ]
    )

    assert args.command_family == "strategy-validation"
    assert args.strategy_validation_command == "build"
    assert args.policy_config == "configs/strategy_validation.yaml"


def test_grouped_strategy_lifecycle_commands_parse() -> None:
    parser = build_parser()
    show_args = parser.parse_args(
        [
            "strategy-lifecycle",
            "show",
            "--lifecycle",
            "artifacts/governance",
        ]
    )
    update_args = parser.parse_args(
        [
            "strategy-lifecycle",
            "update",
            "--lifecycle",
            "artifacts/governance",
            "--strategy-id",
            "generated_demo",
            "--state",
            "under_review",
            "--reason",
            "manual_review",
        ]
    )

    assert show_args.command_family == "strategy-lifecycle"
    assert show_args.strategy_lifecycle_command == "show"
    assert update_args.strategy_lifecycle_command == "update"
    assert update_args.state == "under_review"


def test_grouped_strategy_governance_apply_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "strategy-governance",
            "apply",
            "--promoted-dir",
            "configs/generated_strategies",
            "--validation",
            "artifacts/strategy_validation",
            "--monitoring",
            "artifacts/strategy_monitoring",
            "--adaptive-allocation",
            "artifacts/adaptive_allocation",
            "--lifecycle",
            "artifacts/governance/strategy_lifecycle.json",
            "--policy-config",
            "configs/strategy_governance.yaml",
            "--output-dir",
            "artifacts/strategy_governance",
            "--dry-run",
        ]
    )

    assert args.command_family == "strategy-governance"
    assert args.strategy_governance_command == "apply"
    assert args.adaptive_allocation == "artifacts/adaptive_allocation"
    assert args.dry_run is True


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


def test_grouped_paper_run_command_parses_alpaca_latest_flag() -> None:
    parser = build_parser()
    args = parser.parse_args(["paper", "run", "--symbols", "AAPL", "--use-alpaca-latest-data"])

    assert args.use_alpaca_latest_data is True


def test_grouped_paper_run_command_parses_slippage_and_freshness_flags() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "paper",
            "run",
            "--symbols",
            "AAPL",
            "--slippage-model",
            "fixed_bps",
            "--slippage-buy-bps",
            "5",
            "--slippage-sell-bps",
            "7",
            "--latest-data-max-age-seconds",
            "900",
        ]
    )

    assert args.slippage_model == "fixed_bps"
    assert args.slippage_buy_bps == 5.0
    assert args.slippage_sell_bps == 7.0
    assert args.latest_data_max_age_seconds == 900


def test_grouped_paper_run_command_parses_ensemble_flags() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "paper",
            "run",
            "--symbols",
            "AAPL",
            "--signal-source",
            "ensemble",
            "--enable-ensemble",
            "--ensemble-mode",
            "candidate_weighted",
            "--ensemble-weight-method",
            "performance_weighted",
            "--ensemble-max-members",
            "3",
        ]
    )

    assert args.signal_source == "ensemble"
    assert args.enable_ensemble is True
    assert args.ensemble_mode == "candidate_weighted"
    assert args.ensemble_weight_method == "performance_weighted"
    assert args.ensemble_max_members == 3


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


def test_grouped_paper_schedule_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(["paper", "schedule", "--preset", "xsec_nasdaq100_momentum_v1_deploy"])

    assert args.paper_command == "schedule"
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


def test_grouped_paper_replay_multi_strategy_command_parses_dates() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "paper",
            "replay-multi-strategy",
            "--config",
            "configs/multi_strategy.json",
            "--state-path",
            "artifacts/paper/state.json",
            "--output-dir",
            "artifacts/paper/replay",
            "--start-date",
            "2025-01-02",
            "--end-date",
            "2025-01-10",
            "--max-steps",
            "3",
        ]
    )

    assert args.paper_command == "replay-multi-strategy"
    assert args.start_date == "2025-01-02"
    assert args.end_date == "2025-01-10"
    assert args.max_steps == 3


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


def test_grouped_live_schedule_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "live",
            "schedule",
            "--preset",
            "xsec_nasdaq100_momentum_v1_deploy",
            "--broker",
            "mock",
        ]
    )

    assert args.live_command == "schedule"
    assert args.preset == "xsec_nasdaq100_momentum_v1_deploy"


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


def test_grouped_research_alpha_accepts_new_signal_family() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "research",
            "alpha",
            "--symbols",
            "AAPL",
            "--signal-family",
            "momentum_acceleration",
        ]
    )

    assert args.signal_family == "momentum_acceleration"


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


def test_grouped_research_memo_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "research",
            "memo",
            "--preset",
            "xsec_nasdaq100_momentum_v1_research",
            "--deploy-preset",
            "xsec_nasdaq100_momentum_v1_deploy",
        ]
    )

    assert args.research_command == "memo"
    assert args.preset == "xsec_nasdaq100_momentum_v1_research"


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


def test_grouped_portfolio_optimize_research_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "portfolio",
            "optimize-research",
            "--returns-path",
            "artifacts/returns.csv",
            "--output-dir",
            "artifacts/optimizer",
            "--optimizer-name",
            "max_sharpe",
        ]
    )

    assert args.portfolio_command == "optimize-research"
    assert args.returns_path == "artifacts/returns.csv"
    assert args.optimizer_name == "max_sharpe"


def test_rewrite_legacy_cli_args_keeps_portfolio_optimize_research_command() -> None:
    argv, note = rewrite_legacy_cli_args(["portfolio", "optimize-research", "--config", "configs/portfolio.yaml"])

    assert argv == ["portfolio", "optimize-research", "--config", "configs/portfolio.yaml"]
    assert note is None


def test_rewrite_legacy_cli_args_keeps_research_validate_backtester_command() -> None:
    argv, note = rewrite_legacy_cli_args(["research", "validate-backtester", "--config", "configs/backtester.yaml"])

    assert argv == ["research", "validate-backtester", "--config", "configs/backtester.yaml"]
    assert note is None


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


def test_grouped_pipeline_alpha_cycle_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "pipeline",
            "alpha-cycle",
            "--config",
            "configs/alpha_cycle.yaml",
        ]
    )

    assert args.command_family == "pipeline"
    assert args.pipeline_command == "alpha-cycle"
    assert args.config == "configs/alpha_cycle.yaml"


def test_grouped_ops_pipeline_alpha_cycle_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "ops",
            "pipeline",
            "alpha-cycle",
            "--config",
            "configs/alpha_cycle.yaml",
        ]
    )

    assert args.command_family == "ops"
    assert args.ops_command == "pipeline"
    assert args.ops_pipeline_command == "alpha-cycle"
    assert args.config == "configs/alpha_cycle.yaml"


def test_grouped_strategy_portfolio_activate_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "strategy-portfolio",
            "activate",
            "--portfolio",
            "artifacts/strategy_portfolio/run_current/strategy_portfolio.json",
            "--output-dir",
            "artifacts/strategy_portfolio/run_current/activated",
            "--regime-labels",
            "artifacts/alpha_research/run_current",
            "--metadata-dir",
            "data/metadata",
            "--activation-context-sources",
            "regime",
            "benchmark_context",
            "--exclude-inactive-conditionals-in-output",
        ]
    )

    assert args.command_family == "strategy-portfolio"
    assert args.strategy_portfolio_command == "activate"
    assert args.portfolio.endswith("strategy_portfolio.json")
    assert args.activation_context_sources == ["regime", "benchmark_context"]
    assert args.include_inactive_conditionals_in_output is False


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


def test_grouped_research_promote_run_local_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "research",
            "promote",
            "--artifacts-root",
            "artifacts/alpha_research",
            "--run-dir",
            "artifacts/alpha_research/run_20260325",
            "--registry-scope",
            "run_local",
            "--output-dir",
            "artifacts/promoted_strategies",
        ]
    )

    assert args.command_family == "research"
    assert args.research_command == "promote"
    assert args.run_dir == "artifacts/alpha_research/run_20260325"
    assert args.registry_scope == "run_local"


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


def test_grouped_ops_pipeline_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(["ops", "pipeline", "run", "--config", "configs/pipeline.yaml"])

    assert args.command_family == "ops"
    assert args.ops_command == "pipeline"
    assert args.ops_pipeline_command == "run"
    assert args.config == "configs/pipeline.yaml"


def test_grouped_ops_registry_build_deploy_config_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "ops",
            "registry",
            "build-deploy-config",
            "--registry",
            "artifacts/registry.json",
            "--output-path",
            "artifacts/multi_strategy.json",
        ]
    )

    assert args.command_family == "ops"
    assert args.ops_command == "registry"
    assert args.ops_registry_command == "build-deploy-config"
    assert args.output_path == "artifacts/multi_strategy.json"


def test_legacy_pipeline_group_rewrites_to_ops() -> None:
    argv, note = rewrite_legacy_cli_args(["pipeline", "run", "--config", "configs/pipeline.yaml"])

    assert argv == ["ops", "pipeline", "run", "--config", "configs/pipeline.yaml"]
    assert "ops pipeline" in str(note)


def test_legacy_registry_group_rewrites_to_ops() -> None:
    argv, note = rewrite_legacy_cli_args(["registry", "list", "--registry", "artifacts/registry.json"])

    assert argv == ["ops", "registry", "list", "--registry", "artifacts/registry.json"]
    assert "ops registry" in str(note)
