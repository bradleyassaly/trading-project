from __future__ import annotations

import argparse

from trading_platform.cli.commands.features import cmd_features
from trading_platform.cli.commands.ingest import cmd_ingest
from trading_platform.cli.commands.list_strategies import cmd_list_strategies
from trading_platform.cli.commands.list_universes import cmd_list_universes
from trading_platform.cli.commands.pipeline import cmd_pipeline
from trading_platform.cli.commands.portfolio import cmd_portfolio
from trading_platform.cli.commands.research import cmd_research
from trading_platform.cli.commands.sweep import cmd_sweep
from trading_platform.cli.commands.walkforward import cmd_walkforward
from trading_platform.cli.common import (
    add_feature_arguments,
    add_shared_symbol_args,
    add_strategy_arguments,
)
from trading_platform.strategies.registry import STRATEGY_REGISTRY
from trading_platform.cli.commands.portfolio_topn import cmd_portfolio_topn
from trading_platform.cli.commands.run_job import cmd_run_job
from trading_platform.cli.commands.run_sweep import cmd_run_sweep
from trading_platform.cli.commands.run_walk_forward import cmd_run_walk_forward
from trading_platform.cli.commands.paper_run import cmd_paper_run
from trading_platform.cli.commands.daily_paper_job import cmd_daily_paper_job
from trading_platform.cli.commands.paper_report import cmd_paper_report
from trading_platform.cli.commands.live_dry_run import cmd_live_dry_run
from trading_platform.cli.commands.alpha_research import cmd_alpha_research
from trading_platform.cli.commands.alpha_research_loop import cmd_alpha_research_loop
from trading_platform.cli.commands.experiments_dashboard import cmd_experiments_dashboard
from trading_platform.cli.commands.experiments_latest_model import cmd_experiments_latest_model
from trading_platform.cli.commands.experiments_list import cmd_experiments_list
from trading_platform.cli.commands.research_refresh import cmd_research_refresh
from trading_platform.cli.commands.research_monitor import cmd_research_monitor
from trading_platform.cli.commands.approved_config_diff import cmd_approved_config_diff
from trading_platform.cli.commands.multi_universe_alpha_research import (
    cmd_multi_universe_alpha_research,
)
from trading_platform.cli.commands.multi_universe_report import cmd_multi_universe_report
from trading_platform.cli.commands.validate_live import cmd_validate_live
from trading_platform.cli.commands.execute_live import cmd_execute_live
from trading_platform.cli.commands.export_universes import cmd_export_universes

def add_execution_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--rebalance-frequency",
        type=str,
        default="daily",
        choices=["daily", "weekly", "monthly"],
        help="How often to refresh positions/weights",
    )


def add_composite_paper_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--signal-source",
        type=str,
        default="legacy",
        choices=["legacy", "composite"],
        help="Choose legacy strategy scores or the approved composite alpha signal.",
    )
    parser.add_argument(
        "--composite-artifact-dir",
        type=str,
        default=None,
        help="Alpha research artifact directory containing promoted_signals and redundancy outputs.",
    )
    parser.add_argument(
        "--composite-horizon",
        type=int,
        default=1,
        help="Approved composite horizon to trade.",
    )
    parser.add_argument(
        "--composite-weighting-scheme",
        type=str,
        default="equal",
        choices=["equal", "quality"],
        help="Composite component weighting scheme.",
    )
    parser.add_argument(
        "--composite-portfolio-mode",
        type=str,
        default="long_only_top_n",
        choices=["long_only_top_n", "long_short_quantile"],
        help="Composite portfolio construction mode.",
    )
    parser.add_argument(
        "--composite-long-quantile",
        type=float,
        default=0.2,
        help="Long quantile used when composite portfolio mode is long_short_quantile.",
    )
    parser.add_argument(
        "--composite-short-quantile",
        type=float,
        default=0.2,
        help="Short quantile used when composite portfolio mode is long_short_quantile.",
    )
    parser.add_argument(
        "--min-price",
        type=float,
        default=None,
        help="Optional minimum price required for composite eligibility.",
    )
    parser.add_argument(
        "--min-volume",
        type=float,
        default=None,
        help="Optional minimum raw share volume required for composite eligibility.",
    )
    parser.add_argument(
        "--min-avg-dollar-volume",
        type=float,
        default=None,
        help="Optional minimum rolling average dollar volume required for composite eligibility.",
    )
    parser.add_argument(
        "--max-adv-participation",
        type=float,
        default=0.05,
        help="Maximum ADV participation used in composite implementability constraints.",
    )
    parser.add_argument(
        "--max-position-pct-of-adv",
        type=float,
        default=0.1,
        help="Maximum single-name position size as a fraction of ADV.",
    )
    parser.add_argument(
        "--max-notional-per-name",
        type=float,
        default=None,
        help="Optional max notional per name used in composite implementability constraints.",
    )


def add_experiment_tracker_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--experiment-tracker-dir",
        type=str,
        default=None,
        help="Optional directory used to persist the shared experiment registry and reports.",
    )


def add_live_control_arguments(parser: argparse.ArgumentParser) -> None:
    add_composite_paper_arguments(parser)
    parser.add_argument(
        "--kill-switch",
        action="store_true",
        help="Abort before submission regardless of target portfolio state.",
    )
    parser.add_argument(
        "--kill-switch-path",
        type=str,
        default=None,
        help="Abort if this file exists.",
    )
    parser.add_argument(
        "--blocked-symbols",
        nargs="*",
        default=None,
        help="Optional list of symbols that must never be traded.",
    )
    parser.add_argument(
        "--max-gross-exposure",
        type=float,
        default=1.0,
        help="Maximum allowed gross exposure after trades.",
    )
    parser.add_argument(
        "--max-net-exposure",
        type=float,
        default=1.0,
        help="Maximum allowed absolute net exposure after trades.",
    )
    parser.add_argument(
        "--max-position-weight-limit",
        type=float,
        default=None,
        help="Maximum allowed absolute single-name target weight after trades.",
    )
    parser.add_argument(
        "--max-group-exposure",
        type=float,
        default=None,
        help="Maximum allowed aggregate exposure per symbol group where mappings exist.",
    )
    parser.add_argument(
        "--max-order-notional",
        type=float,
        default=None,
        help="Maximum allowed single-order notional.",
    )
    parser.add_argument(
        "--max-daily-turnover",
        type=float,
        default=None,
        help="Maximum allowed turnover estimate for the rebalance.",
    )
    parser.add_argument(
        "--min-cash-reserve",
        type=float,
        default=0.0,
        help="Minimum required cash reserve fraction after trades.",
    )
    parser.add_argument(
        "--max-data-staleness-days",
        type=int,
        default=3,
        help="Maximum allowed age of the latest signal/price timestamp.",
    )
    parser.add_argument(
        "--max-config-staleness-days",
        type=int,
        default=30,
        help="Maximum allowed age of the approval/config snapshot artifact.",
    )
    parser.add_argument(
        "--approval-artifact",
        type=str,
        default=None,
        help="Optional approval artifact JSON with fields such as approved and approved_at.",
    )
    parser.add_argument(
        "--approved",
        action="store_true",
        help="Explicitly mark this run as approved for live execution.",
    )
    parser.add_argument(
        "--drift-alerts-path",
        type=str,
        default=None,
        help="Optional drift alerts CSV used to block execution on high-severity alerts.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="artifacts/live_execution",
        help="Directory for live execution control artifacts.",
    )

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="trading_platform.cli",
        description="Trading platform command line interface",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest_parser = subparsers.add_parser("ingest", help="Download raw OHLCV data")
    add_shared_symbol_args(ingest_parser)
    ingest_parser.add_argument(
        "--start",
        type=str,
        default="2010-01-01",
        help="Start date in YYYY-MM-DD format (default: 2010-01-01)",
    )
    ingest_parser.set_defaults(func=cmd_ingest)

    features_parser = subparsers.add_parser("features", help="Build feature datasets")
    add_shared_symbol_args(features_parser)
    add_feature_arguments(features_parser)
    features_parser.set_defaults(func=cmd_features)

    research_parser = subparsers.add_parser("research", help="Run backtests")
    add_shared_symbol_args(research_parser)
    add_strategy_arguments(research_parser)
    add_execution_arguments(research_parser)
    research_parser.add_argument(
        "--engine",
        type=str,
        default="legacy",
        choices=["legacy", "vectorized"],
        help="Backtest engine to use",
    )
    research_parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Optional directory to save vectorized research outputs",
    )
    research_parser.set_defaults(func=cmd_research)

    pipeline_parser = subparsers.add_parser(
        "pipeline",
        help="Run ingest -> features -> research in one command",
    )
    add_shared_symbol_args(pipeline_parser)
    add_feature_arguments(pipeline_parser)
    add_strategy_arguments(pipeline_parser)
    pipeline_parser.add_argument(
        "--start",
        type=str,
        default="2010-01-01",
        help="Start date in YYYY-MM-DD format (default: 2010-01-01)",
    )
    pipeline_parser.set_defaults(func=cmd_pipeline)

    universes_parser = subparsers.add_parser(
        "list-universes",
        help="Show available named universes",
    )
    universes_parser.set_defaults(func=cmd_list_universes)

    export_universes_parser = subparsers.add_parser(
        "export-universes",
        help="Export the current static universe definitions to a JSON file",
    )
    export_universes_parser.add_argument(
        "--output",
        type=str,
        default="artifacts/universes/universes.json",
        help="Path where the universe definitions JSON should be written.",
    )
    export_universes_parser.set_defaults(func=cmd_export_universes)

    strategies_parser = subparsers.add_parser(
        "list-strategies",
        help="Show available strategies",
    )
    strategies_parser.set_defaults(func=cmd_list_strategies)

    sweep_parser = subparsers.add_parser(
        "sweep",
        help="Run parameter sweep for a strategy",
    )
    add_shared_symbol_args(sweep_parser)
    add_execution_arguments(sweep_parser)
    sweep_parser.add_argument(
        "--strategy",
        type=str,
        default="sma_cross",
        choices=sorted(STRATEGY_REGISTRY.keys()),
        help="Strategy to sweep",
    )
    sweep_parser.add_argument("--fast-values", type=int, nargs="+")
    sweep_parser.add_argument("--slow-values", type=int, nargs="+")
    sweep_parser.add_argument("--lookback-values", type=int, nargs="+")
    sweep_parser.add_argument("--cash", type=float, default=10_000)
    sweep_parser.add_argument("--commission", type=float, default=0.001)
    sweep_parser.add_argument(
        "--output",
        type=str,
        default="artifacts/experiments/sweep_results.csv",
        help="CSV output path for sweep summary",
    )
    sweep_parser.add_argument(
        "--engine",
        type=str,
        default="legacy",
        choices=["legacy", "vectorized"],
        help="Backtest engine to use",
    )

    sweep_parser.set_defaults(func=cmd_sweep)

    walk_parser = subparsers.add_parser(
        "walkforward",
        help="Run walk-forward validation",
    )
    add_shared_symbol_args(walk_parser)
    walk_parser.add_argument(
        "--strategy",
        type=str,
        default="sma_cross",
        choices=sorted(STRATEGY_REGISTRY.keys()),
        help="Strategy to validate",
    )
    add_execution_arguments(walk_parser)
    walk_parser.add_argument("--fast-values", type=int, nargs="+")
    walk_parser.add_argument("--slow-values", type=int, nargs="+")
    walk_parser.add_argument("--lookback-values", type=int, nargs="+")
    walk_parser.add_argument("--train-years", type=int, default=5)
    walk_parser.add_argument("--test-years", type=int, default=1)
    walk_parser.add_argument("--min-train-rows", type=int, default=252)
    walk_parser.add_argument("--min-test-rows", type=int, default=126)
    walk_parser.add_argument(
        "--select-by",
        type=str,
        default="Sharpe Ratio",
        choices=["Sharpe Ratio", "Return [%]"],
        help="Metric used to choose best params on the train window",
    )
    walk_parser.add_argument("--cash", type=float, default=10_000)
    walk_parser.add_argument("--commission", type=float, default=0.001)
    walk_parser.add_argument(
        "--output",
        type=str,
        default="artifacts/experiments/walkforward_results.csv",
        help="CSV output path",
    )
    walk_parser.add_argument(
        "--engine",
        type=str,
        default="legacy",
        choices=["legacy", "vectorized"],
        help="Backtest engine to use",
    )

    walk_parser.set_defaults(func=cmd_walkforward)

    portfolio_parser = subparsers.add_parser(
        "portfolio",
        help="Run equal-weight portfolio backtest across multiple symbols",
    )
    add_shared_symbol_args(portfolio_parser)
    add_strategy_arguments(portfolio_parser)
    add_execution_arguments(portfolio_parser)
    portfolio_parser.add_argument(
        "--output-dir",
        type=str,
        default="data/experiments/portfolio",
        help="Directory for portfolio outputs",
    )


    portfolio_parser.set_defaults(func=cmd_portfolio)

    portfolio_topn_parser = subparsers.add_parser(
        "portfolio-topn",
        help="Run top-N cross-sectional portfolio backtest",
    )
    add_shared_symbol_args(portfolio_topn_parser)
    add_strategy_arguments(portfolio_topn_parser)
    add_execution_arguments(portfolio_topn_parser)
    portfolio_topn_parser.add_argument(
        "--top-n",
        type=int,
        required=True,
        help="Number of top-ranked symbols to hold",
    )
    portfolio_topn_parser.add_argument(
        "--weighting-scheme",
        type=str,
        default="equal",
        choices=["equal", "inverse_vol"],
        help="How to size selected holdings",
    )
    portfolio_topn_parser.add_argument(
        "--vol-window",
        type=int,
        default=20,
        help="Rolling volatility window for inverse-vol weighting",
    )
    portfolio_topn_parser.add_argument(
        "--min-score",
        type=float,
        default=None,
        help="Optional minimum score required for a symbol to be held",
    )
    portfolio_topn_parser.add_argument(
        "--max-weight",
        type=float,
        default=None,
        help="Optional cap on any single position weight, e.g. 0.4",
    )
    portfolio_topn_parser.add_argument(
        "--max-names-per-group",
        type=int,
        default=None,
        help="Optional maximum number of holdings allowed per group",
    )
    portfolio_topn_parser.add_argument(
        "--max-group-weight",
        type=float,
        default=None,
        help="Optional cap on total portfolio weight per group, e.g. 0.4",
    )
    portfolio_topn_parser.add_argument(
        "--output-dir",
        type=str,
        default="data/experiments/portfolio_topn",
        help="Directory for portfolio outputs",
    )
    portfolio_topn_parser.add_argument(
        "--group-map-path",
        type=str,
        default=None,
        help="Optional path to CSV with columns: symbol,group",
    )
    portfolio_topn_parser.set_defaults(func=cmd_portfolio_topn)

    run_job_parser = subparsers.add_parser(
        "run-job",
        help="Run a research job from a YAML or JSON config file",
    )
    run_job_parser.add_argument(
        "--config",
        required=True,
        help="Path to a YAML or JSON research workflow config file",
    )
    run_job_parser.add_argument(
        "--symbols",
        nargs="*",
        help="Optional symbol override list",
    )
    run_job_parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop immediately on the first symbol error",
    )
    run_job_parser.set_defaults(func=cmd_run_job)

    run_sweep_parser = subparsers.add_parser(
        "run-sweep",
        help="Run a parameter sweep from a YAML or JSON config file",
    )
    run_sweep_parser.add_argument(
        "--config",
        required=True,
        help="Path to a YAML or JSON parameter sweep config file",
    )
    run_sweep_parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop immediately on the first sweep error",
    )
    run_sweep_parser.set_defaults(func=cmd_run_sweep)

    run_wf_parser = subparsers.add_parser(
        "run-walk-forward",
        help="Run a walk-forward evaluation from a YAML or JSON config file",
    )
    run_wf_parser.add_argument(
        "--config",
        required=True,
        help="Path to a YAML or JSON walk-forward config file",
    )
    run_wf_parser.set_defaults(func=cmd_run_walk_forward)

    paper_run_parser = subparsers.add_parser(
        "paper-run",
        help="Run one paper-trading cycle and write state/artifacts",
    )
    add_shared_symbol_args(paper_run_parser)
    add_strategy_arguments(paper_run_parser)
    add_execution_arguments(paper_run_parser)
    add_composite_paper_arguments(paper_run_parser)
    add_experiment_tracker_argument(paper_run_parser)
    paper_run_parser.add_argument(
        "--top-n",
        type=int,
        required=True,
        help="Number of top-ranked symbols to target",
    )
    paper_run_parser.add_argument(
        "--weighting-scheme",
        type=str,
        default="equal",
        choices=["equal", "inverse_vol"],
        help="How to size selected holdings",
    )
    paper_run_parser.add_argument(
        "--vol-window",
        type=int,
        default=20,
        help="Rolling volatility window for inverse-vol weighting",
    )
    paper_run_parser.add_argument(
        "--min-score",
        type=float,
        default=None,
        help="Optional minimum score required for a symbol to be held",
    )
    paper_run_parser.add_argument(
        "--max-weight",
        type=float,
        default=None,
        help="Optional cap on any single position weight",
    )
    paper_run_parser.add_argument(
        "--max-names-per-group",
        type=int,
        default=None,
        help="Optional maximum number of holdings allowed per group",
    )
    paper_run_parser.add_argument(
        "--max-group-weight",
        type=float,
        default=None,
        help="Optional cap on total portfolio weight per group",
    )
    paper_run_parser.add_argument(
        "--group-map-path",
        type=str,
        default=None,
        help="Optional path to CSV with columns: symbol,group",
    )
    paper_run_parser.add_argument(
        "--timing",
        type=str,
        default="next_bar",
        choices=["same_bar", "next_bar"],
        help="When scheduled target weights become effective",
    )
    paper_run_parser.add_argument(
        "--initial-cash",
        type=float,
        default=100_000.0,
        help="Starting cash used when no paper state exists yet",
    )
    paper_run_parser.add_argument(
        "--min-trade-dollars",
        type=float,
        default=25.0,
        help="Skip trades smaller than this dollar threshold",
    )
    paper_run_parser.add_argument(
        "--lot-size",
        type=int,
        default=1,
        help="Round target quantities down to this lot size",
    )
    paper_run_parser.add_argument(
        "--reserve-cash-pct",
        type=float,
        default=0.0,
        help="Fraction of equity to hold back as cash",
    )
    paper_run_parser.add_argument(
        "--state-path",
        type=str,
        default="artifacts/paper/paper_state.json",
        help="JSON file used to persist paper portfolio state",
    )
    paper_run_parser.add_argument(
        "--output-dir",
        type=str,
        default="artifacts/paper",
        help="Base directory for paper-run output artifacts",
    )
    paper_run_parser.add_argument(
        "--auto-apply-fills",
        action="store_true",
        help="Immediately apply simulated fills and update positions/cash",
    )
    paper_run_parser.set_defaults(func=cmd_paper_run)

    daily_paper_parser = subparsers.add_parser(
        "daily-paper-job",
        help="Run the daily paper trading workflow.",
    )
    add_composite_paper_arguments(daily_paper_parser)


    daily_paper_parser.add_argument(
        "--strategy",
        default="sma_cross",
        help="Signal strategy name.",
    )
    daily_paper_parser.add_argument(
        "--fast",
        type=int,
        default=None,
        help="Fast lookback parameter for the signal.",
    )
    daily_paper_parser.add_argument(
        "--slow",
        type=int,
        default=None,
        help="Slow lookback parameter for the signal.",
    )
    daily_paper_parser.add_argument(
        "--lookback",
        type=int,
        default=None,
        help="Lookback parameter for the signal.",
    )
    daily_paper_parser.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of symbols to select.",
    )
    daily_paper_parser.add_argument(
        "--weighting-scheme",
        default="equal",
        help="Weighting scheme for portfolio construction.",
    )
    daily_paper_parser.add_argument(
        "--vol-window",
        type=int,
        default=20,
        help="Volatility lookback window for inverse-vol weighting.",
    )
    daily_paper_parser.add_argument(
        "--min-score",
        type=float,
        default=None,
        help="Minimum score threshold for portfolio inclusion.",
    )
    daily_paper_parser.add_argument(
        "--max-weight",
        type=float,
        default=None,
        help="Maximum position weight.",
    )
    daily_paper_parser.add_argument(
        "--max-names-per-group",
        type=int,
        default=None,
        help="Maximum number of names per group.",
    )
    daily_paper_parser.add_argument(
        "--max-group-weight",
        type=float,
        default=None,
        help="Maximum aggregate weight per group.",
    )
    daily_paper_parser.add_argument(
        "--group-map-path",
        default=None,
        help="Optional path to symbol-to-group mapping file.",
    )
    daily_paper_parser.add_argument(
        "--rebalance-frequency",
        default="daily",
        help="Rebalance frequency.",
    )
    daily_paper_parser.add_argument(
        "--timing",
        default="next_bar",
        help="Execution timing policy.",
    )
    daily_paper_parser.add_argument(
        "--initial-cash",
        type=float,
        default=100_000.0,
        help="Initial paper trading cash balance.",
    )
    daily_paper_parser.add_argument(
        "--min-trade-dollars",
        type=float,
        default=25.0,
        help="Minimum trade notional.",
    )
    daily_paper_parser.add_argument(
        "--lot-size",
        type=int,
        default=1,
        help="Trading lot size.",
    )
    daily_paper_parser.add_argument(
        "--reserve-cash-pct",
        type=float,
        default=0.0,
        help="Fraction of equity to keep in cash reserve.",
    )
    daily_paper_parser.add_argument(
        "--state-path",
        required=True,
        help="Path to the paper trading state file.",
    )
    daily_paper_parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory for paper trading artifacts.",
    )
    daily_paper_parser.add_argument(
        "--auto-apply-fills",
        action="store_true",
        help="Apply simulated fills through the paper broker.",
    )
    daily_paper_parser.set_defaults(func=cmd_daily_paper_job)

    daily_paper_parser.add_argument(
        "--symbols",
        nargs="+",
        default=None,
        help="Symbols to include in the daily paper trading job.",
    )
    daily_paper_parser.add_argument(
        "--universe",
        default=None,
        help="Named universe to trade instead of passing --symbols.",
    )

    paper_report_parser = subparsers.add_parser(
        "paper-report",
        help="Build a summary report from paper trading ledgers.",
    )
    paper_report_parser.add_argument(
        "--account-dir",
        required=True,
        help="Base paper account directory containing ledgers/.",
    )
    paper_report_parser.add_argument(
        "--output-dir",
        default=None,
        help="Optional directory to write report artifacts.",
    )
    paper_report_parser.set_defaults(func=cmd_paper_report)

    live_dry_run_parser = subparsers.add_parser(
        "live-dry-run",
        help="Compute live broker rebalance orders without sending them.",
    )
    live_dry_run_parser.add_argument(
        "--symbols",
        nargs="+",
        default=None,
        help="Symbols to include in the live dry-run.",
    )
    live_dry_run_parser.add_argument(
        "--universe",
        default=None,
        help="Named universe to trade instead of passing --symbols.",
    )
    live_dry_run_parser.add_argument(
        "--strategy",
        default="sma_cross",
        help="Signal strategy name.",
    )
    live_dry_run_parser.add_argument(
        "--fast",
        type=int,
        default=None,
    )
    live_dry_run_parser.add_argument(
        "--slow",
        type=int,
        default=None,
    )
    live_dry_run_parser.add_argument(
        "--lookback",
        type=int,
        default=None,
    )
    live_dry_run_parser.add_argument(
        "--top-n",
        type=int,
        default=10,
    )
    live_dry_run_parser.add_argument(
        "--weighting-scheme",
        default="equal",
    )
    live_dry_run_parser.add_argument(
        "--vol-window",
        type=int,
        default=20,
    )
    live_dry_run_parser.add_argument(
        "--min-score",
        type=float,
        default=None,
    )
    live_dry_run_parser.add_argument(
        "--max-weight",
        type=float,
        default=None,
    )
    live_dry_run_parser.add_argument(
        "--max-names-per-group",
        type=int,
        default=None,
    )
    live_dry_run_parser.add_argument(
        "--max-group-weight",
        type=float,
        default=None,
    )
    live_dry_run_parser.add_argument(
        "--group-map-path",
        default=None,
    )
    live_dry_run_parser.add_argument(
        "--rebalance-frequency",
        default="daily",
    )
    live_dry_run_parser.add_argument(
        "--timing",
        default="next_bar",
    )
    live_dry_run_parser.add_argument(
        "--initial-cash",
        type=float,
        default=100_000.0,
    )
    live_dry_run_parser.add_argument(
        "--min-trade-dollars",
        type=float,
        default=25.0,
    )
    live_dry_run_parser.add_argument(
        "--lot-size",
        type=int,
        default=1,
    )
    live_dry_run_parser.add_argument(
        "--reserve-cash-pct",
        type=float,
        default=0.0,
    )
    live_dry_run_parser.add_argument(
        "--order-type",
        default="market",
        help="Order type for hypothetical live orders.",
    )
    live_dry_run_parser.add_argument(
        "--time-in-force",
        default="day",
        help="Time in force for hypothetical live orders.",
    )
    live_dry_run_parser.add_argument(
        "--broker",
        default="mock",
        choices=["mock", "alpaca"],
        help="Broker backend for dry-run reconciliation.",
    )
    live_dry_run_parser.add_argument(
        "--mock-equity",
        type=float,
        default=100_000.0,
        help="Mock broker equity used when --broker mock.",
    )
    live_dry_run_parser.add_argument(
        "--mock-cash",
        type=float,
        default=100_000.0,
        help="Mock broker cash used when --broker mock.",
    )

    live_dry_run_parser.add_argument(
        "--mock-positions-path",
        default=None,
        help="Optional CSV of mock broker positions for --broker mock.",
    )

    live_dry_run_parser.set_defaults(func=cmd_live_dry_run)

    alpha_research_parser = subparsers.add_parser(
        "alpha-research",
        help="Run cross-sectional alpha signal research across a universe",
    )
    add_experiment_tracker_argument(alpha_research_parser)
    alpha_research_parser.add_argument(
        "--symbols",
        nargs="+",
        default=None,
        help="Symbols to include in the alpha research run.",
    )
    alpha_research_parser.add_argument(
        "--universe",
        type=str,
        default=None,
        help="Named universe to evaluate instead of passing --symbols.",
    )
    alpha_research_parser.add_argument(
        "--feature-dir",
        type=str,
        default="data/features",
        help="Directory containing per-symbol feature parquet files.",
    )
    alpha_research_parser.add_argument(
        "--signal-family",
        type=str,
        default="momentum",
        choices=[
            "momentum",
            "short_term_reversal",
            "vol_adjusted_momentum",
        ],
        help="Signal family to evaluate.",
    )
    alpha_research_parser.add_argument(
        "--lookbacks",
        type=int,
        nargs="+",
        default=[5, 10, 20, 60],
        help="Lookback windows to test.",
    )
    alpha_research_parser.add_argument(
        "--horizons",
        type=int,
        nargs="+",
        default=[1, 5, 20],
        help="Forward return horizons to test.",
    )
    alpha_research_parser.add_argument(
        "--min-rows",
        type=int,
        default=126,
        help="Minimum number of usable rows required per symbol.",
    )
    alpha_research_parser.add_argument(
        "--top-quantile",
        type=float,
        default=0.2,
        help="Top quantile threshold used for spread metrics.",
    )
    alpha_research_parser.add_argument(
        "--bottom-quantile",
        type=float,
        default=0.2,
        help="Bottom quantile threshold used for spread metrics.",
    )
    alpha_research_parser.add_argument(
        "--output-dir",
        type=str,
        default="artifacts/alpha_research",
        help="Directory where alpha research artifacts will be written.",
    )

    alpha_research_parser.add_argument(
        "--train-size",
        type=int,
        default=756,
        help="Number of rows in each training window.",
    )
    alpha_research_parser.add_argument(
        "--test-size",
        type=int,
        default=63,
        help="Number of rows in each test window.",
    )
    alpha_research_parser.add_argument(
        "--step-size",
        type=int,
        default=None,
        help="Number of rows to advance after each fold. Defaults to test-size.",
    )
    alpha_research_parser.add_argument(
        "--min-train-size",
        type=int,
        default=None,
        help="Optional minimum train window size.",
    )
    alpha_research_parser.add_argument(
        "--portfolio-top-n",
        type=int,
        default=10,
        help="Top-N size used for the composite long-only portfolio.",
    )
    alpha_research_parser.add_argument(
        "--portfolio-long-quantile",
        type=float,
        default=0.2,
        help="Top quantile used for the composite long-short portfolio.",
    )
    alpha_research_parser.add_argument(
        "--portfolio-short-quantile",
        type=float,
        default=0.2,
        help="Bottom quantile used for the composite long-short portfolio.",
    )
    alpha_research_parser.add_argument(
        "--commission",
        type=float,
        default=0.0,
        help="Turnover-based transaction cost used in the composite portfolio backtest.",
    )
    alpha_research_parser.add_argument(
        "--min-price",
        type=float,
        default=None,
        help="Optional minimum price required for a name to remain investable.",
    )
    alpha_research_parser.add_argument(
        "--min-volume",
        type=float,
        default=None,
        help="Optional minimum raw share volume required for a name to remain investable.",
    )
    alpha_research_parser.add_argument(
        "--min-avg-dollar-volume",
        type=float,
        default=None,
        help="Optional minimum rolling average dollar volume required for a name to remain investable.",
    )
    alpha_research_parser.add_argument(
        "--max-adv-participation",
        type=float,
        default=0.05,
        help="Maximum participation rate used in capacity estimates.",
    )
    alpha_research_parser.add_argument(
        "--max-position-pct-of-adv",
        type=float,
        default=0.1,
        help="Maximum single-name position size as a fraction of average dollar volume.",
    )
    alpha_research_parser.add_argument(
        "--max-notional-per-name",
        type=float,
        default=None,
        help="Optional notional cap per name used in capacity estimates.",
    )
    alpha_research_parser.add_argument(
        "--slippage-bps-per-turnover",
        type=float,
        default=0.0,
        help="Linear slippage in basis points per unit of turnover.",
    )
    alpha_research_parser.add_argument(
        "--slippage-bps-per-adv",
        type=float,
        default=10.0,
        help="Additional slippage in basis points that scales with fraction of ADV traded.",
    )
    alpha_research_parser.add_argument(
        "--dynamic-recent-quality-window",
        type=int,
        default=20,
        help="Lookback window in out-of-sample dates used for dynamic signal weighting.",
    )
    alpha_research_parser.add_argument(
        "--dynamic-min-history",
        type=int,
        default=5,
        help="Minimum out-of-sample dates before lifecycle rules move beyond promote state.",
    )
    alpha_research_parser.add_argument(
        "--dynamic-downweight-mean-rank-ic",
        type=float,
        default=0.01,
        help="Recent mean rank IC threshold below which active signals are downweighted.",
    )
    alpha_research_parser.add_argument(
        "--dynamic-deactivate-mean-rank-ic",
        type=float,
        default=-0.02,
        help="Recent mean rank IC threshold below which signals are deactivated.",
    )
    alpha_research_parser.add_argument(
        "--regime-aware-enabled",
        action="store_true",
        help="Enable regime-aware signal weighting on top of the dynamic lifecycle weights.",
    )
    alpha_research_parser.add_argument(
        "--regime-min-history",
        type=int,
        default=5,
        help="Minimum same-regime out-of-sample observations before regime-aware weighting reacts.",
    )
    alpha_research_parser.add_argument(
        "--regime-underweight-mean-rank-ic",
        type=float,
        default=0.01,
        help="Same-regime mean rank IC threshold below which signals are underweighted.",
    )
    alpha_research_parser.add_argument(
        "--regime-exclude-mean-rank-ic",
        type=float,
        default=-0.01,
        help="Same-regime mean rank IC threshold below which signals are excluded.",
    )
    alpha_research_parser.set_defaults(func=cmd_alpha_research)

    alpha_research_loop_parser = subparsers.add_parser(
        "alpha-research-loop",
        help="Run the automated alpha research loop over candidate signal families and ranges",
    )
    alpha_research_loop_parser.add_argument(
        "--symbols",
        nargs="+",
        default=None,
        help="Symbols to include in the automated alpha research loop.",
    )
    alpha_research_loop_parser.add_argument(
        "--universe",
        type=str,
        default=None,
        help="Named universe to evaluate instead of passing --symbols.",
    )
    alpha_research_loop_parser.add_argument(
        "--feature-dir",
        type=str,
        default="data/features",
        help="Directory containing per-symbol feature parquet files.",
    )
    alpha_research_loop_parser.add_argument(
        "--signal-families",
        type=str,
        nargs="+",
        default=["momentum", "mean_reversion", "volatility", "feature_combo"],
        help="Signal families to generate and test.",
    )
    alpha_research_loop_parser.add_argument(
        "--lookbacks",
        type=int,
        nargs="+",
        default=[5, 10, 20, 60],
        help="Lookback windows to test for each family.",
    )
    alpha_research_loop_parser.add_argument(
        "--horizons",
        type=int,
        nargs="+",
        default=[1, 5, 20],
        help="Forward return horizons to test for each family.",
    )
    alpha_research_loop_parser.add_argument(
        "--vol-windows",
        type=int,
        nargs="+",
        default=[10, 20, 60],
        help="Volatility windows to test for volatility-based signals.",
    )
    alpha_research_loop_parser.add_argument(
        "--combo-thresholds",
        type=float,
        nargs="+",
        default=[0.5, 1.0],
        help="Threshold multipliers used for simple feature-combination signals.",
    )
    alpha_research_loop_parser.add_argument(
        "--min-rows",
        type=int,
        default=126,
        help="Minimum number of usable rows required per symbol.",
    )
    alpha_research_loop_parser.add_argument(
        "--top-quantile",
        type=float,
        default=0.2,
        help="Top quantile threshold used for spread metrics.",
    )
    alpha_research_loop_parser.add_argument(
        "--bottom-quantile",
        type=float,
        default=0.2,
        help="Bottom quantile threshold used for spread metrics.",
    )
    alpha_research_loop_parser.add_argument(
        "--output-dir",
        type=str,
        default="artifacts/alpha_research_loop",
        help="Directory where automated research artifacts will be written.",
    )
    alpha_research_loop_parser.add_argument(
        "--train-size",
        type=int,
        default=756,
        help="Number of rows in each training window.",
    )
    alpha_research_loop_parser.add_argument(
        "--test-size",
        type=int,
        default=63,
        help="Number of rows in each test window.",
    )
    alpha_research_loop_parser.add_argument(
        "--step-size",
        type=int,
        default=None,
        help="Number of rows to advance after each fold. Defaults to test-size.",
    )
    alpha_research_loop_parser.add_argument(
        "--min-train-size",
        type=int,
        default=None,
        help="Optional minimum train window size.",
    )
    alpha_research_loop_parser.add_argument(
        "--schedule-frequency",
        type=str,
        default="manual",
        choices=["manual", "daily", "weekly"],
        help="Scheduling hook used to decide when the loop is due to run.",
    )
    alpha_research_loop_parser.add_argument(
        "--force",
        action="store_true",
        help="Run immediately even if the schedule metadata says the loop is not due.",
    )
    alpha_research_loop_parser.set_defaults(func=cmd_alpha_research_loop)

    experiments_list_parser = subparsers.add_parser(
        "experiments-list",
        help="List recent tracked research and paper trading experiments",
    )
    experiments_list_parser.add_argument(
        "--tracker-dir",
        type=str,
        default="artifacts/experiment_tracking",
        help="Directory containing the shared experiment registry.",
    )
    experiments_list_parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum number of experiments to print.",
    )
    experiments_list_parser.set_defaults(func=cmd_experiments_list)

    experiments_latest_model_parser = subparsers.add_parser(
        "experiments-latest-model",
        help="Show the latest approved composite/research configuration snapshot",
    )
    experiments_latest_model_parser.add_argument(
        "--tracker-dir",
        type=str,
        default="artifacts/experiment_tracking",
        help="Directory containing the shared experiment registry.",
    )
    experiments_latest_model_parser.set_defaults(func=cmd_experiments_latest_model)

    experiments_dashboard_parser = subparsers.add_parser(
        "experiments-dashboard",
        help="Build a summary dashboard artifact from tracked experiments",
    )
    experiments_dashboard_parser.add_argument(
        "--tracker-dir",
        type=str,
        default="artifacts/experiment_tracking",
        help="Directory containing the shared experiment registry.",
    )
    experiments_dashboard_parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Optional directory where the dashboard artifacts should be written.",
    )
    experiments_dashboard_parser.add_argument(
        "--top-metric",
        type=str,
        default="portfolio_sharpe",
        help="Registry metric used to rank top experiments in the dashboard.",
    )
    experiments_dashboard_parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum number of top experiments to include.",
    )
    experiments_dashboard_parser.set_defaults(func=cmd_experiments_dashboard)

    research_refresh_parser = subparsers.add_parser(
        "research-refresh",
        help="Run the scheduled alpha discovery refresh and persist a new approved configuration snapshot",
    )
    research_refresh_parser.add_argument(
        "--symbols",
        nargs="+",
        default=None,
        help="Symbols to include in the automated alpha refresh.",
    )
    research_refresh_parser.add_argument(
        "--universe",
        type=str,
        default=None,
        help="Named universe to evaluate instead of passing --symbols.",
    )
    research_refresh_parser.add_argument(
        "--feature-dir",
        type=str,
        default="data/features",
        help="Directory containing per-symbol feature parquet files.",
    )
    research_refresh_parser.add_argument(
        "--signal-families",
        type=str,
        nargs="+",
        default=["momentum", "mean_reversion", "volatility", "feature_combo"],
        help="Signal families to generate and test.",
    )
    research_refresh_parser.add_argument(
        "--lookbacks",
        type=int,
        nargs="+",
        default=[5, 10, 20, 60],
        help="Lookback windows to test for each family.",
    )
    research_refresh_parser.add_argument(
        "--horizons",
        type=int,
        nargs="+",
        default=[1, 5, 20],
        help="Forward return horizons to test for each family.",
    )
    research_refresh_parser.add_argument(
        "--vol-windows",
        type=int,
        nargs="+",
        default=[10, 20, 60],
        help="Volatility windows to test for volatility-based signals.",
    )
    research_refresh_parser.add_argument(
        "--combo-thresholds",
        type=float,
        nargs="+",
        default=[0.5, 1.0],
        help="Threshold multipliers used for simple feature-combination signals.",
    )
    research_refresh_parser.add_argument(
        "--min-rows",
        type=int,
        default=126,
        help="Minimum number of usable rows required per symbol.",
    )
    research_refresh_parser.add_argument(
        "--top-quantile",
        type=float,
        default=0.2,
        help="Top quantile threshold used for spread metrics.",
    )
    research_refresh_parser.add_argument(
        "--bottom-quantile",
        type=float,
        default=0.2,
        help="Bottom quantile threshold used for spread metrics.",
    )
    research_refresh_parser.add_argument(
        "--output-dir",
        type=str,
        default="artifacts/research_refresh",
        help="Directory where refresh artifacts will be written.",
    )
    research_refresh_parser.add_argument(
        "--train-size",
        type=int,
        default=756,
        help="Number of rows in each training window.",
    )
    research_refresh_parser.add_argument(
        "--test-size",
        type=int,
        default=63,
        help="Number of rows in each test window.",
    )
    research_refresh_parser.add_argument(
        "--step-size",
        type=int,
        default=None,
        help="Number of rows to advance after each fold. Defaults to test-size.",
    )
    research_refresh_parser.add_argument(
        "--min-train-size",
        type=int,
        default=None,
        help="Optional minimum train window size.",
    )
    research_refresh_parser.add_argument(
        "--schedule-frequency",
        type=str,
        default="daily",
        choices=["manual", "daily", "weekly"],
        help="Scheduling hook used to decide when the refresh is due to run.",
    )
    research_refresh_parser.add_argument(
        "--stale-after-days",
        type=int,
        default=None,
        help="Optional age threshold for re-evaluating stale signal candidates.",
    )
    research_refresh_parser.add_argument(
        "--tracker-dir",
        type=str,
        default="artifacts/experiment_tracking",
        help="Optional experiment tracker directory used to enrich approved snapshots.",
    )
    research_refresh_parser.add_argument(
        "--force",
        action="store_true",
        help="Run immediately even if the refresh schedule metadata says the loop is not due.",
    )
    research_refresh_parser.set_defaults(func=cmd_research_refresh)

    research_monitor_parser = subparsers.add_parser(
        "research-monitor",
        help="Generate a file-based monitoring report and drift alerts from recent alpha and paper artifacts",
    )
    research_monitor_parser.add_argument(
        "--tracker-dir",
        type=str,
        default="artifacts/experiment_tracking",
        help="Directory containing the shared experiment registry.",
    )
    research_monitor_parser.add_argument(
        "--output-dir",
        type=str,
        default="artifacts/research_monitoring",
        help="Directory where monitoring artifacts will be written.",
    )
    research_monitor_parser.add_argument(
        "--snapshot-dir",
        type=str,
        default="artifacts/research_refresh/approved_configuration_snapshots",
        help="Directory containing approved configuration snapshots.",
    )
    research_monitor_parser.add_argument(
        "--alpha-artifact-dir",
        type=str,
        default=None,
        help="Optional alpha artifact directory override.",
    )
    research_monitor_parser.add_argument(
        "--paper-artifact-dir",
        type=str,
        default=None,
        help="Optional paper artifact directory override.",
    )
    research_monitor_parser.add_argument(
        "--recent-paper-runs",
        type=int,
        default=10,
        help="Number of recent paper runs to use for realized diagnostics.",
    )
    research_monitor_parser.add_argument(
        "--performance-degradation-buffer",
        type=float,
        default=0.002,
        help="Absolute buffer before recent paper returns trigger a degradation alert.",
    )
    research_monitor_parser.add_argument(
        "--turnover-spike-multiple",
        type=float,
        default=1.5,
        help="Multiple of expected turnover that triggers a turnover spike alert.",
    )
    research_monitor_parser.add_argument(
        "--concentration-spike-multiple",
        type=float,
        default=1.5,
        help="Multiple of expected top-position weight that triggers a concentration alert.",
    )
    research_monitor_parser.add_argument(
        "--signal-churn-threshold",
        type=int,
        default=3,
        help="Number of approved-signal additions/removals that triggers a churn alert.",
    )
    research_monitor_parser.set_defaults(func=cmd_research_monitor)

    approved_config_diff_parser = subparsers.add_parser(
        "approved-config-diff",
        help="Show the current approved configuration versus the prior approved snapshot",
    )
    approved_config_diff_parser.add_argument(
        "--snapshot-dir",
        type=str,
        default="artifacts/research_refresh/approved_configuration_snapshots",
        help="Directory containing approved configuration snapshots.",
    )
    approved_config_diff_parser.set_defaults(func=cmd_approved_config_diff)

    multi_universe_alpha_research_parser = subparsers.add_parser(
        "multi-universe-alpha-research",
        help="Run the full alpha research workflow across multiple named universes",
    )
    add_experiment_tracker_argument(multi_universe_alpha_research_parser)
    multi_universe_alpha_research_parser.add_argument(
        "--universes",
        nargs="+",
        required=True,
        help="Named universes to evaluate in one job.",
    )
    multi_universe_alpha_research_parser.add_argument(
        "--feature-dir",
        type=str,
        default="data/features",
        help="Directory containing per-symbol feature parquet files.",
    )
    multi_universe_alpha_research_parser.add_argument(
        "--signal-family",
        type=str,
        default="momentum",
        choices=["momentum", "short_term_reversal", "vol_adjusted_momentum"],
        help="Signal family to evaluate in each universe.",
    )
    multi_universe_alpha_research_parser.add_argument(
        "--lookbacks",
        type=int,
        nargs="+",
        default=[5, 10, 20, 60],
        help="Lookback windows to test.",
    )
    multi_universe_alpha_research_parser.add_argument(
        "--horizons",
        type=int,
        nargs="+",
        default=[1, 5, 20],
        help="Forward return horizons to test.",
    )
    multi_universe_alpha_research_parser.add_argument(
        "--min-rows",
        type=int,
        default=126,
        help="Minimum number of usable rows required per symbol.",
    )
    multi_universe_alpha_research_parser.add_argument(
        "--top-quantile",
        type=float,
        default=0.2,
        help="Top quantile threshold used for spread metrics.",
    )
    multi_universe_alpha_research_parser.add_argument(
        "--bottom-quantile",
        type=float,
        default=0.2,
        help="Bottom quantile threshold used for spread metrics.",
    )
    multi_universe_alpha_research_parser.add_argument(
        "--output-dir",
        type=str,
        default="artifacts/multi_universe_alpha_research",
        help="Directory where per-universe and comparison artifacts will be written.",
    )
    multi_universe_alpha_research_parser.add_argument(
        "--train-size",
        type=int,
        default=756,
        help="Number of rows in each training window.",
    )
    multi_universe_alpha_research_parser.add_argument(
        "--test-size",
        type=int,
        default=63,
        help="Number of rows in each test window.",
    )
    multi_universe_alpha_research_parser.add_argument(
        "--step-size",
        type=int,
        default=None,
        help="Number of rows to advance after each fold. Defaults to test-size.",
    )
    multi_universe_alpha_research_parser.add_argument(
        "--min-train-size",
        type=int,
        default=None,
        help="Optional minimum train window size.",
    )
    multi_universe_alpha_research_parser.add_argument(
        "--portfolio-top-n",
        type=int,
        default=10,
        help="Top-N size used for the composite long-only portfolio.",
    )
    multi_universe_alpha_research_parser.add_argument(
        "--portfolio-long-quantile",
        type=float,
        default=0.2,
        help="Top quantile used for the composite long-short portfolio.",
    )
    multi_universe_alpha_research_parser.add_argument(
        "--portfolio-short-quantile",
        type=float,
        default=0.2,
        help="Bottom quantile used for the composite long-short portfolio.",
    )
    multi_universe_alpha_research_parser.add_argument(
        "--commission",
        type=float,
        default=0.0,
        help="Turnover-based transaction cost used in the composite portfolio backtest.",
    )
    multi_universe_alpha_research_parser.add_argument(
        "--min-price",
        type=float,
        default=None,
        help="Optional minimum price required for a name to remain investable.",
    )
    multi_universe_alpha_research_parser.add_argument(
        "--min-volume",
        type=float,
        default=None,
        help="Optional minimum raw share volume required for a name to remain investable.",
    )
    multi_universe_alpha_research_parser.add_argument(
        "--min-avg-dollar-volume",
        type=float,
        default=None,
        help="Optional minimum rolling average dollar volume required for a name to remain investable.",
    )
    multi_universe_alpha_research_parser.add_argument(
        "--max-adv-participation",
        type=float,
        default=0.05,
        help="Maximum participation rate used in capacity estimates.",
    )
    multi_universe_alpha_research_parser.add_argument(
        "--max-position-pct-of-adv",
        type=float,
        default=0.1,
        help="Maximum single-name position size as a fraction of average dollar volume.",
    )
    multi_universe_alpha_research_parser.add_argument(
        "--max-notional-per-name",
        type=float,
        default=None,
        help="Optional notional cap per name used in capacity estimates.",
    )
    multi_universe_alpha_research_parser.add_argument(
        "--slippage-bps-per-turnover",
        type=float,
        default=0.0,
        help="Linear slippage in basis points per unit of turnover.",
    )
    multi_universe_alpha_research_parser.add_argument(
        "--slippage-bps-per-adv",
        type=float,
        default=10.0,
        help="Additional slippage in basis points that scales with fraction of ADV traded.",
    )
    multi_universe_alpha_research_parser.add_argument(
        "--dynamic-recent-quality-window",
        type=int,
        default=20,
        help="Lookback window in out-of-sample dates used for dynamic signal weighting.",
    )
    multi_universe_alpha_research_parser.add_argument(
        "--dynamic-min-history",
        type=int,
        default=5,
        help="Minimum out-of-sample dates before lifecycle rules move beyond promote state.",
    )
    multi_universe_alpha_research_parser.add_argument(
        "--dynamic-downweight-mean-rank-ic",
        type=float,
        default=0.01,
        help="Recent mean rank IC threshold below which active signals are downweighted.",
    )
    multi_universe_alpha_research_parser.add_argument(
        "--dynamic-deactivate-mean-rank-ic",
        type=float,
        default=-0.02,
        help="Recent mean rank IC threshold below which signals are deactivated.",
    )
    multi_universe_alpha_research_parser.add_argument(
        "--regime-aware-enabled",
        action="store_true",
        help="Enable regime-aware signal weighting on top of the dynamic lifecycle weights.",
    )
    multi_universe_alpha_research_parser.add_argument(
        "--regime-min-history",
        type=int,
        default=5,
        help="Minimum same-regime out-of-sample observations before regime-aware weighting reacts.",
    )
    multi_universe_alpha_research_parser.add_argument(
        "--regime-underweight-mean-rank-ic",
        type=float,
        default=0.01,
        help="Same-regime mean rank IC threshold below which signals are underweighted.",
    )
    multi_universe_alpha_research_parser.add_argument(
        "--regime-exclude-mean-rank-ic",
        type=float,
        default=-0.01,
        help="Same-regime mean rank IC threshold below which signals are excluded.",
    )
    multi_universe_alpha_research_parser.set_defaults(func=cmd_multi_universe_alpha_research)

    multi_universe_report_parser = subparsers.add_parser(
        "multi-universe-report",
        help="Build a cross-universe comparison report from existing per-universe alpha artifacts",
    )
    multi_universe_report_parser.add_argument(
        "--output-dir",
        type=str,
        default="artifacts/multi_universe_alpha_research",
        help="Directory containing per-universe outputs and the comparison artifacts.",
    )
    multi_universe_report_parser.set_defaults(func=cmd_multi_universe_report)

    validate_live_parser = subparsers.add_parser(
        "validate-live",
        help="Run live execution control checks and write artifacts without submitting orders",
    )
    validate_live_parser.add_argument("--symbols", nargs="+", default=None, help="Symbols to include in the validation run.")
    validate_live_parser.add_argument("--universe", default=None, help="Named universe to trade instead of passing --symbols.")
    validate_live_parser.add_argument("--strategy", default="sma_cross", help="Signal strategy name.")
    validate_live_parser.add_argument("--fast", type=int, default=None)
    validate_live_parser.add_argument("--slow", type=int, default=None)
    validate_live_parser.add_argument("--lookback", type=int, default=None)
    validate_live_parser.add_argument("--top-n", type=int, default=10)
    validate_live_parser.add_argument("--weighting-scheme", default="equal")
    validate_live_parser.add_argument("--vol-window", type=int, default=20)
    validate_live_parser.add_argument("--min-score", type=float, default=None)
    validate_live_parser.add_argument("--max-weight", type=float, default=None)
    validate_live_parser.add_argument("--max-names-per-group", type=int, default=None)
    validate_live_parser.add_argument("--max-group-weight", type=float, default=None)
    validate_live_parser.add_argument("--group-map-path", default=None)
    validate_live_parser.add_argument("--rebalance-frequency", default="daily")
    validate_live_parser.add_argument("--timing", default="next_bar")
    validate_live_parser.add_argument("--initial-cash", type=float, default=100_000.0)
    validate_live_parser.add_argument("--min-trade-dollars", type=float, default=25.0)
    validate_live_parser.add_argument("--lot-size", type=int, default=1)
    validate_live_parser.add_argument("--reserve-cash-pct", type=float, default=0.0)
    validate_live_parser.add_argument("--order-type", default="market")
    validate_live_parser.add_argument("--time-in-force", default="day")
    validate_live_parser.add_argument("--broker", default="mock", choices=["mock", "alpaca"])
    validate_live_parser.add_argument("--mock-equity", type=float, default=100_000.0)
    validate_live_parser.add_argument("--mock-cash", type=float, default=100_000.0)
    validate_live_parser.add_argument("--mock-positions-path", default=None)
    add_live_control_arguments(validate_live_parser)
    validate_live_parser.set_defaults(func=cmd_validate_live)

    execute_live_parser = subparsers.add_parser(
        "execute-live",
        help="Run live execution control checks and only submit orders if approved and safe",
    )
    execute_live_parser.add_argument("--symbols", nargs="+", default=None, help="Symbols to include in the execution run.")
    execute_live_parser.add_argument("--universe", default=None, help="Named universe to trade instead of passing --symbols.")
    execute_live_parser.add_argument("--strategy", default="sma_cross", help="Signal strategy name.")
    execute_live_parser.add_argument("--fast", type=int, default=None)
    execute_live_parser.add_argument("--slow", type=int, default=None)
    execute_live_parser.add_argument("--lookback", type=int, default=None)
    execute_live_parser.add_argument("--top-n", type=int, default=10)
    execute_live_parser.add_argument("--weighting-scheme", default="equal")
    execute_live_parser.add_argument("--vol-window", type=int, default=20)
    execute_live_parser.add_argument("--min-score", type=float, default=None)
    execute_live_parser.add_argument("--max-weight", type=float, default=None)
    execute_live_parser.add_argument("--max-names-per-group", type=int, default=None)
    execute_live_parser.add_argument("--max-group-weight", type=float, default=None)
    execute_live_parser.add_argument("--group-map-path", default=None)
    execute_live_parser.add_argument("--rebalance-frequency", default="daily")
    execute_live_parser.add_argument("--timing", default="next_bar")
    execute_live_parser.add_argument("--initial-cash", type=float, default=100_000.0)
    execute_live_parser.add_argument("--min-trade-dollars", type=float, default=25.0)
    execute_live_parser.add_argument("--lot-size", type=int, default=1)
    execute_live_parser.add_argument("--reserve-cash-pct", type=float, default=0.0)
    execute_live_parser.add_argument("--order-type", default="market")
    execute_live_parser.add_argument("--time-in-force", default="day")
    execute_live_parser.add_argument("--broker", default="mock", choices=["mock", "alpaca"])
    execute_live_parser.add_argument("--mock-equity", type=float, default=100_000.0)
    execute_live_parser.add_argument("--mock-cash", type=float, default=100_000.0)
    execute_live_parser.add_argument("--mock-positions-path", default=None)
    add_live_control_arguments(execute_live_parser)
    execute_live_parser.set_defaults(func=cmd_execute_live)

    return parser
