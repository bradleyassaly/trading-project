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

def add_execution_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--rebalance-frequency",
        type=str,
        default="daily",
        choices=["daily", "weekly", "monthly"],
        help="How often to refresh positions/weights",
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
    alpha_research_parser.set_defaults(func=cmd_alpha_research)

    return parser
