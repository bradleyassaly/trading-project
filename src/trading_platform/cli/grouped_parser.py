from __future__ import annotations

import argparse

from trading_platform.cli.commands.alpha_research import cmd_alpha_research
from trading_platform.cli.commands.alpha_research_loop import cmd_alpha_research_loop
from trading_platform.cli.commands.approved_config_diff import cmd_approved_config_diff
from trading_platform.cli.commands.daily_paper_job import cmd_daily_paper_job
from trading_platform.cli.commands.execute_live import cmd_execute_live
from trading_platform.cli.commands.experiments_dashboard import cmd_experiments_dashboard
from trading_platform.cli.commands.experiments_latest_model import cmd_experiments_latest_model
from trading_platform.cli.commands.experiments_list import cmd_experiments_list
from trading_platform.cli.commands.export_universes import cmd_export_universes
from trading_platform.cli.commands.features import cmd_features
from trading_platform.cli.commands.ingest import cmd_ingest
from trading_platform.cli.commands.list_strategies import cmd_list_strategies
from trading_platform.cli.commands.list_universes import cmd_list_universes
from trading_platform.cli.commands.live_dry_run import cmd_live_dry_run
from trading_platform.cli.commands.multi_universe_alpha_research import (
    cmd_multi_universe_alpha_research,
)
from trading_platform.cli.commands.multi_universe_report import cmd_multi_universe_report
from trading_platform.cli.commands.paper_report import cmd_paper_report
from trading_platform.cli.commands.paper_run import cmd_paper_run
from trading_platform.cli.commands.pipeline import cmd_pipeline
from trading_platform.cli.commands.portfolio import cmd_portfolio
from trading_platform.cli.commands.portfolio_topn import cmd_portfolio_topn
from trading_platform.cli.commands.research import cmd_research
from trading_platform.cli.commands.research_monitor import cmd_research_monitor
from trading_platform.cli.commands.research_refresh import cmd_research_refresh
from trading_platform.cli.commands.run_job import cmd_run_job
from trading_platform.cli.commands.run_sweep import cmd_run_sweep
from trading_platform.cli.commands.run_walk_forward import cmd_run_walk_forward
from trading_platform.cli.commands.sweep import cmd_sweep
from trading_platform.cli.commands.validate_signal import cmd_validate_signal
from trading_platform.cli.commands.validate_live import cmd_validate_live
from trading_platform.cli.commands.walkforward import cmd_walkforward
from trading_platform.cli.common import (
    add_date_range_arguments,
    add_feature_arguments,
    add_shared_symbol_args,
    add_strategy_arguments,
    add_xsec_research_arguments,
    get_strategy_choices,
)
from trading_platform.strategies.registry import STRATEGY_REGISTRY


LEGACY_REWRITE_MAP: dict[str, list[str]] = {
    "ingest": ["data", "ingest"],
    "features": ["data", "features"],
    "list-universes": ["data", "universes", "list"],
    "export-universes": ["data", "universes", "export"],
    "list-strategies": ["research", "strategies"],
    "sweep": ["research", "sweep"],
    "walkforward": ["research", "walkforward"],
    "alpha-research": ["research", "alpha"],
    "alpha-research-loop": ["research", "loop"],
    "multi-universe-alpha-research": ["research", "multi-universe"],
    "multi-universe-report": ["research", "multi-universe-report"],
    "research-refresh": ["research", "refresh"],
    "research-monitor": ["research", "monitor"],
    "pipeline": ["research", "pipeline"],
    "run-job": ["research", "run"],
    "run-sweep": ["research", "sweep"],
    "run-walk-forward": ["research", "walkforward"],
    "portfolio-topn": ["portfolio", "topn"],
    "paper-run": ["paper", "run"],
    "daily-paper-job": ["paper", "daily"],
    "paper-report": ["paper", "report"],
    "live-dry-run": ["live", "dry-run"],
    "validate-live": ["live", "validate"],
    "execute-live": ["live", "execute"],
    "experiments-list": ["experiments", "list"],
    "experiments-latest-model": ["experiments", "latest"],
    "experiments-dashboard": ["experiments", "dashboard"],
    "approved-config-diff": ["experiments", "diff"],
}
_RESEARCH_GROUP_COMMANDS = {
    "run",
    "sweep",
    "walkforward",
    "validate-signal",
    "alpha",
    "loop",
    "multi-universe",
    "multi-universe-report",
    "refresh",
    "monitor",
    "pipeline",
    "strategies",
}
_PORTFOLIO_GROUP_COMMANDS = {"backtest", "topn"}


def rewrite_legacy_cli_args(argv: list[str]) -> tuple[list[str], str | None]:
    if not argv:
        return argv, None

    first = argv[0]
    second = argv[1] if len(argv) > 1 else ""

    if first == "features" and second == "build":
        return ["data", "features", *argv[2:]], "Deprecated command `features build`; use `data features`."
    if first == "research" and second not in _RESEARCH_GROUP_COMMANDS:
        return ["research", "run", *argv[1:]], "Deprecated command `research`; use `research run`."
    if first == "portfolio" and second not in _PORTFOLIO_GROUP_COMMANDS:
        return ["portfolio", "backtest", *argv[1:]], "Deprecated command `portfolio`; use `portfolio backtest`."

    replacement = LEGACY_REWRITE_MAP.get(first)
    if replacement is None:
        return argv, None
    return [*replacement, *argv[1:]], f"Deprecated command `{first}`; use `{' '.join(replacement)}`."


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
        "--approved-model-state",
        type=str,
        default=None,
        help="Optional deployment-ready approved model-state artifact used by paper/live composite workflows.",
    )
    parser.add_argument(
        "--composite-artifact-dir",
        type=str,
        default=None,
        help="Legacy alpha artifact directory containing promoted_signals and redundancy outputs.",
    )
    parser.add_argument("--composite-horizon", type=int, default=1, help="Approved composite horizon to trade.")
    parser.add_argument("--composite-weighting-scheme", type=str, default="equal", choices=["equal", "quality"], help="Composite component weighting scheme.")
    parser.add_argument("--composite-portfolio-mode", type=str, default="long_only_top_n", choices=["long_only_top_n", "long_short_quantile"], help="Composite portfolio construction mode.")
    parser.add_argument("--composite-long-quantile", type=float, default=0.2, help="Long quantile used when composite portfolio mode is long_short_quantile.")
    parser.add_argument("--composite-short-quantile", type=float, default=0.2, help="Short quantile used when composite portfolio mode is long_short_quantile.")
    parser.add_argument("--min-price", type=float, default=None, help="Optional minimum price required for composite eligibility.")
    parser.add_argument("--min-volume", type=float, default=None, help="Optional minimum raw share volume required for composite eligibility.")
    parser.add_argument("--min-avg-dollar-volume", type=float, default=None, help="Optional minimum rolling average dollar volume required for composite eligibility.")
    parser.add_argument("--max-adv-participation", type=float, default=0.05, help="Maximum ADV participation used in composite implementability constraints.")
    parser.add_argument("--max-position-pct-of-adv", type=float, default=0.1, help="Maximum single-name position size as a fraction of ADV.")
    parser.add_argument("--max-notional-per-name", type=float, default=None, help="Optional max notional per name used in composite implementability constraints.")


def add_experiment_tracker_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--experiment-tracker-dir",
        type=str,
        default=None,
        help="Optional directory used to persist the shared experiment registry and reports.",
    )


def add_live_control_arguments(parser: argparse.ArgumentParser) -> None:
    add_composite_paper_arguments(parser)
    parser.add_argument("--kill-switch", action="store_true", help="Abort before submission regardless of target portfolio state.")
    parser.add_argument("--kill-switch-path", type=str, default=None, help="Abort if this file exists.")
    parser.add_argument("--blocked-symbols", nargs="*", default=None, help="Optional list of symbols that must never be traded.")
    parser.add_argument("--max-gross-exposure", type=float, default=1.0, help="Maximum allowed gross exposure after trades.")
    parser.add_argument("--max-net-exposure", type=float, default=1.0, help="Maximum allowed absolute net exposure after trades.")
    parser.add_argument("--max-position-weight-limit", type=float, default=None, help="Maximum allowed absolute single-name target weight after trades.")
    parser.add_argument("--max-group-exposure", type=float, default=None, help="Maximum allowed aggregate exposure per symbol group where mappings exist.")
    parser.add_argument("--max-order-notional", type=float, default=None, help="Maximum allowed single-order notional.")
    parser.add_argument("--max-daily-turnover", type=float, default=None, help="Maximum allowed turnover estimate for the rebalance.")
    parser.add_argument("--min-cash-reserve", type=float, default=0.0, help="Minimum required cash reserve fraction after trades.")
    parser.add_argument("--max-data-staleness-days", type=int, default=3, help="Maximum allowed age of the latest signal/price timestamp.")
    parser.add_argument("--max-config-staleness-days", type=int, default=30, help="Maximum allowed age of the approval/config snapshot artifact.")
    parser.add_argument("--approval-artifact", type=str, default=None, help="Optional approval artifact JSON with fields such as approved and approved_at.")
    parser.add_argument("--approved", action="store_true", help="Explicitly mark this run as approved for live execution.")
    parser.add_argument("--drift-alerts-path", type=str, default=None, help="Optional drift alerts CSV used to block execution on high-severity alerts.")
    parser.add_argument("--output-dir", type=str, default="artifacts/live_execution", help="Directory for live execution control artifacts.")


def _add_common_portfolio_selection_arguments(parser: argparse.ArgumentParser, *, required_top_n: bool) -> None:
    parser.add_argument("--top-n", type=int, required=required_top_n, default=None if required_top_n else 10, help="Number of top-ranked symbols to target")
    parser.add_argument("--weighting-scheme", type=str, default="equal", choices=["equal", "inverse_vol"], help="How to size selected holdings")
    parser.add_argument("--vol-window", type=int, default=20, help="Rolling volatility window for inverse-vol weighting")
    parser.add_argument("--min-score", type=float, default=None, help="Optional minimum score required for a symbol to be held")
    parser.add_argument("--max-weight", type=float, default=None, help="Optional cap on any single position weight")
    parser.add_argument("--max-names-per-group", type=int, default=None, help="Optional maximum number of holdings allowed per group")
    parser.add_argument("--max-group-weight", type=float, default=None, help="Optional cap on total portfolio weight per group")
    parser.add_argument("--group-map-path", type=str, default=None, help="Optional path to CSV with columns: symbol,group")


def _add_walkforward_arguments(parser: argparse.ArgumentParser) -> None:
    add_shared_symbol_args(parser)
    parser.add_argument("--config", type=str, default=None, help="Optional YAML or JSON config file for reproducible walk-forward jobs.")
    parser.add_argument("--strategy", type=str, default="sma_cross", choices=get_strategy_choices(include_xsec=True), help="Strategy to validate")
    add_date_range_arguments(parser)
    add_execution_arguments(parser)
    parser.add_argument("--fast-values", type=int, nargs="+")
    parser.add_argument("--slow-values", type=int, nargs="+")
    parser.add_argument("--lookback-values", type=int, nargs="+")
    parser.add_argument("--lookback-bars-values", type=int, nargs="+")
    parser.add_argument("--skip-bars-values", type=int, nargs="+")
    parser.add_argument("--top-n-values", type=int, nargs="+")
    parser.add_argument("--rebalance-bars-values", type=int, nargs="+")
    parser.add_argument("--max-position-weight", type=float, default=None, help="Optional cap on any single xsec position weight.")
    parser.add_argument("--min-avg-dollar-volume", type=float, default=None, help="Optional minimum 20-bar average dollar volume required for xsec eligibility.")
    parser.add_argument("--max-names-per-sector", type=int, default=None, help="Optional maximum number of selected names per sector when sector metadata is available.")
    parser.add_argument("--turnover-buffer-bps", type=float, default=0.0, help="Optional minimum momentum-score improvement, expressed in bps of score gap, required to replace an existing xsec holding.")
    parser.add_argument("--max-turnover-per-rebalance", type=float, default=None, help="Optional cap on absolute turnover per xsec rebalance.")
    parser.add_argument("--weighting-scheme", type=str, default="equal", choices=["equal", "inv_vol"], help="How to size selected xsec holdings.")
    parser.add_argument("--vol-lookback-bars", type=int, default=20, help="Lookback window for inverse-vol xsec weighting.")
    parser.add_argument("--benchmark", type=str, default="equal_weight", choices=["equal_weight"], help="Benchmark type for cross-sectional research")
    parser.add_argument("--entry-lookback-values", type=int, nargs="+")
    parser.add_argument("--exit-lookback-values", type=int, nargs="+")
    parser.add_argument("--momentum-lookback-values", type=int, nargs="+")
    parser.add_argument("--train-years", type=int, default=5)
    parser.add_argument("--test-years", type=int, default=1)
    parser.add_argument("--train-bars", type=int, default=None, help="Training window length in bars/rows. For daily data, this means trading days.")
    parser.add_argument("--test-bars", type=int, default=None, help="Test window length in bars/rows. For daily data, this means trading days.")
    parser.add_argument("--step-bars", type=int, default=None, help="Step size between windows in bars/rows. Defaults to test-bars.")
    parser.add_argument("--train-period-days", type=int, default=None, help="Compatibility alias for --train-bars in daily walk-forward research.")
    parser.add_argument("--test-period-days", type=int, default=None, help="Compatibility alias for --test-bars in daily walk-forward research.")
    parser.add_argument("--step-days", type=int, default=None, help="Compatibility alias for --step-bars in daily walk-forward research.")
    parser.add_argument("--min-train-rows", type=int, default=252)
    parser.add_argument("--min-test-rows", type=int, default=126)
    parser.add_argument("--select-by", type=str, default="Sharpe Ratio", choices=["Sharpe Ratio", "Return [%]"], help="Metric used to choose best params on the train window")
    parser.add_argument("--cash", type=float, default=10_000)
    parser.add_argument("--commission", type=float, default=0.001)
    parser.add_argument("--cost-bps", type=float, default=None, help="Optional transaction cost in basis points per unit of turnover. Overrides --commission when provided.")
    parser.add_argument("--output", type=str, default="artifacts/experiments/walkforward_results.csv", help="CSV output path")
    parser.add_argument("--engine", type=str, default="legacy", choices=["legacy", "vectorized"], help="Backtest engine to use")


def _add_validate_signal_arguments(parser: argparse.ArgumentParser) -> None:
    add_shared_symbol_args(parser)
    parser.add_argument("--strategy", type=str, default="sma_cross", choices=sorted(STRATEGY_REGISTRY.keys()), help="Strategy to validate")
    add_execution_arguments(parser)
    parser.add_argument("--fast", type=int, default=20, help="Fast SMA window used for the baseline in-sample run")
    parser.add_argument("--slow", type=int, default=100, help="Slow SMA window used for the baseline in-sample run")
    parser.add_argument("--lookback", type=int, default=20, help="Lookback used for the baseline in-sample run")
    parser.add_argument("--fast-values", type=int, nargs="+", default=None, help="Optional fast windows used in the validation sweep and walk-forward selection")
    parser.add_argument("--slow-values", type=int, nargs="+", default=None, help="Optional slow windows used in the validation sweep and walk-forward selection")
    parser.add_argument("--lookback-values", type=int, nargs="+", default=None, help="Optional lookbacks used in the validation sweep and walk-forward selection")
    parser.add_argument("--select-by", type=str, default="Sharpe Ratio", choices=["Sharpe Ratio", "Return [%]"], help="Metric used to choose walk-forward parameters on each train window")
    parser.add_argument("--train-years", type=int, default=5, help="Training window size in years")
    parser.add_argument("--test-years", type=int, default=1, help="Test window size in years")
    parser.add_argument("--min-train-rows", type=int, default=252, help="Minimum train rows required for a walk-forward window")
    parser.add_argument("--min-test-rows", type=int, default=126, help="Minimum test rows required for a walk-forward window")
    parser.add_argument("--cash", type=float, default=10_000, help="Starting equity used by the vectorized simulation")
    parser.add_argument("--commission", type=float, default=0.001, help="Linear turnover cost used by the vectorized simulation")
    parser.add_argument("--output-dir", type=str, default="artifacts/validate_signal", help="Directory where validation artifacts will be written")


def _add_alpha_research_arguments(parser: argparse.ArgumentParser) -> None:
    add_experiment_tracker_argument(parser)
    parser.add_argument("--symbols", nargs="+", default=None, help="Symbols to include in the alpha research run.")
    parser.add_argument("--universe", type=str, default=None, help="Named universe to evaluate instead of passing --symbols.")
    parser.add_argument("--feature-dir", type=str, default="data/features", help="Directory containing per-symbol feature parquet files.")
    parser.add_argument("--signal-family", type=str, default="momentum", choices=["momentum", "short_term_reversal", "vol_adjusted_momentum"], help="Signal family to evaluate.")
    parser.add_argument("--lookbacks", type=int, nargs="+", default=[5, 10, 20, 60], help="Lookback windows to test.")
    parser.add_argument("--horizons", type=int, nargs="+", default=[1, 5, 20], help="Forward return horizons to test.")
    parser.add_argument("--min-rows", type=int, default=126, help="Minimum number of usable rows required per symbol.")
    parser.add_argument("--top-quantile", type=float, default=0.2, help="Top quantile threshold used for spread metrics.")
    parser.add_argument("--bottom-quantile", type=float, default=0.2, help="Bottom quantile threshold used for spread metrics.")
    parser.add_argument("--output-dir", type=str, default="artifacts/alpha_research", help="Directory where alpha research artifacts will be written.")
    parser.add_argument("--train-size", type=int, default=756, help="Number of rows in each training window.")
    parser.add_argument("--test-size", type=int, default=63, help="Number of rows in each test window.")
    parser.add_argument("--step-size", type=int, default=None, help="Number of rows to advance after each fold. Defaults to test-size.")
    parser.add_argument("--min-train-size", type=int, default=None, help="Optional minimum train window size.")
    parser.add_argument("--portfolio-top-n", type=int, default=10, help="Top-N size used for the composite long-only portfolio.")
    parser.add_argument("--portfolio-long-quantile", type=float, default=0.2, help="Top quantile used for the composite long-short portfolio.")
    parser.add_argument("--portfolio-short-quantile", type=float, default=0.2, help="Bottom quantile used for the composite long-short portfolio.")
    parser.add_argument("--commission", type=float, default=0.0, help="Turnover-based transaction cost used in the composite portfolio backtest.")
    parser.add_argument("--min-price", type=float, default=None, help="Optional minimum price required for a name to remain investable.")
    parser.add_argument("--min-volume", type=float, default=None, help="Optional minimum raw share volume required for a name to remain investable.")
    parser.add_argument("--min-avg-dollar-volume", type=float, default=None, help="Optional minimum rolling average dollar volume required for a name to remain investable.")
    parser.add_argument("--max-adv-participation", type=float, default=0.05, help="Maximum participation rate used in capacity estimates.")
    parser.add_argument("--max-position-pct-of-adv", type=float, default=0.1, help="Maximum single-name position size as a fraction of average dollar volume.")
    parser.add_argument("--max-notional-per-name", type=float, default=None, help="Optional notional cap per name used in capacity estimates.")
    parser.add_argument("--slippage-bps-per-turnover", type=float, default=0.0, help="Linear slippage in basis points per unit of turnover.")
    parser.add_argument("--slippage-bps-per-adv", type=float, default=10.0, help="Additional slippage in basis points that scales with fraction of ADV traded.")
    parser.add_argument("--dynamic-recent-quality-window", type=int, default=20, help="Lookback window in out-of-sample dates used for dynamic signal weighting.")
    parser.add_argument("--dynamic-min-history", type=int, default=5, help="Minimum out-of-sample dates before lifecycle rules move beyond promote state.")
    parser.add_argument("--dynamic-downweight-mean-rank-ic", type=float, default=0.01, help="Recent mean rank IC threshold below which active signals are downweighted.")
    parser.add_argument("--dynamic-deactivate-mean-rank-ic", type=float, default=-0.02, help="Recent mean rank IC threshold below which signals are deactivated.")
    parser.add_argument("--regime-aware-enabled", action="store_true", help="Enable regime-aware signal weighting on top of the dynamic lifecycle weights.")
    parser.add_argument("--regime-min-history", type=int, default=5, help="Minimum same-regime out-of-sample observations before regime-aware weighting reacts.")
    parser.add_argument("--regime-underweight-mean-rank-ic", type=float, default=0.01, help="Same-regime mean rank IC threshold below which signals are underweighted.")
    parser.add_argument("--regime-exclude-mean-rank-ic", type=float, default=-0.01, help="Same-regime mean rank IC threshold below which signals are excluded.")


def _add_alpha_loop_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--symbols", nargs="+", default=None, help="Symbols to include in the automated alpha research loop.")
    parser.add_argument("--universe", type=str, default=None, help="Named universe to evaluate instead of passing --symbols.")
    parser.add_argument("--feature-dir", type=str, default="data/features", help="Directory containing per-symbol feature parquet files.")
    parser.add_argument("--signal-families", type=str, nargs="+", default=["momentum", "mean_reversion", "volatility", "feature_combo"], help="Signal families to generate and test.")
    parser.add_argument("--lookbacks", type=int, nargs="+", default=[5, 10, 20, 60], help="Lookback windows to test for each family.")
    parser.add_argument("--horizons", type=int, nargs="+", default=[1, 5, 20], help="Forward return horizons to test for each family.")
    parser.add_argument("--vol-windows", type=int, nargs="+", default=[10, 20, 60], help="Volatility windows to test for volatility-based signals.")
    parser.add_argument("--combo-thresholds", type=float, nargs="+", default=[0.5, 1.0], help="Threshold multipliers used for simple feature-combination signals.")
    parser.add_argument("--min-rows", type=int, default=126, help="Minimum number of usable rows required per symbol.")
    parser.add_argument("--top-quantile", type=float, default=0.2, help="Top quantile threshold used for spread metrics.")
    parser.add_argument("--bottom-quantile", type=float, default=0.2, help="Bottom quantile threshold used for spread metrics.")
    parser.add_argument("--output-dir", type=str, default="artifacts/alpha_research_loop", help="Directory where automated research artifacts will be written.")
    parser.add_argument("--train-size", type=int, default=756, help="Number of rows in each training window.")
    parser.add_argument("--test-size", type=int, default=63, help="Number of rows in each test window.")
    parser.add_argument("--step-size", type=int, default=None, help="Number of rows to advance after each fold. Defaults to test-size.")
    parser.add_argument("--min-train-size", type=int, default=None, help="Optional minimum train window size.")
    parser.add_argument("--schedule-frequency", type=str, default="manual", choices=["manual", "daily", "weekly"], help="Scheduling hook used to decide when the loop is due to run.")
    parser.add_argument("--force", action="store_true", help="Run immediately even if the schedule metadata says the loop is not due.")
    parser.add_argument("--max-iterations", type=int, default=1, help="Maximum number of loop iterations to run in one invocation. Use 0 for no work, default is 1 for safety.")


def _add_refresh_arguments(parser: argparse.ArgumentParser) -> None:
    _add_alpha_loop_arguments(parser)
    parser.set_defaults(output_dir="artifacts/research_refresh", schedule_frequency="daily")
    parser.add_argument("--stale-after-days", type=int, default=None, help="Optional age threshold for re-evaluating stale signal candidates.")
    parser.add_argument("--tracker-dir", type=str, default="artifacts/experiment_tracking", help="Optional experiment tracker directory used to enrich approved snapshots.")


def _add_multi_universe_arguments(parser: argparse.ArgumentParser) -> None:
    add_experiment_tracker_argument(parser)
    parser.add_argument("--universes", nargs="+", required=True, help="Named universes to evaluate in one job.")
    parser.add_argument("--feature-dir", type=str, default="data/features", help="Directory containing per-symbol feature parquet files.")
    parser.add_argument("--signal-family", type=str, default="momentum", choices=["momentum", "short_term_reversal", "vol_adjusted_momentum"], help="Signal family to evaluate in each universe.")
    parser.add_argument("--lookbacks", type=int, nargs="+", default=[5, 10, 20, 60], help="Lookback windows to test.")
    parser.add_argument("--horizons", type=int, nargs="+", default=[1, 5, 20], help="Forward return horizons to test.")
    parser.add_argument("--min-rows", type=int, default=126, help="Minimum number of usable rows required per symbol.")
    parser.add_argument("--top-quantile", type=float, default=0.2, help="Top quantile threshold used for spread metrics.")
    parser.add_argument("--bottom-quantile", type=float, default=0.2, help="Bottom quantile threshold used for spread metrics.")
    parser.add_argument("--output-dir", type=str, default="artifacts/multi_universe_alpha_research", help="Directory where per-universe and comparison artifacts will be written.")
    parser.add_argument("--train-size", type=int, default=756, help="Number of rows in each training window.")
    parser.add_argument("--test-size", type=int, default=63, help="Number of rows in each test window.")
    parser.add_argument("--step-size", type=int, default=None, help="Number of rows to advance after each fold. Defaults to test-size.")
    parser.add_argument("--min-train-size", type=int, default=None, help="Optional minimum train window size.")
    parser.add_argument("--portfolio-top-n", type=int, default=10, help="Top-N size used for the composite long-only portfolio.")
    parser.add_argument("--portfolio-long-quantile", type=float, default=0.2, help="Top quantile used for the composite long-short portfolio.")
    parser.add_argument("--portfolio-short-quantile", type=float, default=0.2, help="Bottom quantile used for the composite long-short portfolio.")
    parser.add_argument("--commission", type=float, default=0.0, help="Turnover-based transaction cost used in the composite portfolio backtest.")
    parser.add_argument("--min-price", type=float, default=None, help="Optional minimum price required for a name to remain investable.")
    parser.add_argument("--min-volume", type=float, default=None, help="Optional minimum raw share volume required for a name to remain investable.")
    parser.add_argument("--min-avg-dollar-volume", type=float, default=None, help="Optional minimum rolling average dollar volume required for a name to remain investable.")
    parser.add_argument("--max-adv-participation", type=float, default=0.05, help="Maximum participation rate used in capacity estimates.")
    parser.add_argument("--max-position-pct-of-adv", type=float, default=0.1, help="Maximum single-name position size as a fraction of average dollar volume.")
    parser.add_argument("--max-notional-per-name", type=float, default=None, help="Optional notional cap per name used in capacity estimates.")
    parser.add_argument("--slippage-bps-per-turnover", type=float, default=0.0, help="Linear slippage in basis points per unit of turnover.")
    parser.add_argument("--slippage-bps-per-adv", type=float, default=10.0, help="Additional slippage in basis points that scales with fraction of ADV traded.")
    parser.add_argument("--dynamic-recent-quality-window", type=int, default=20, help="Lookback window in out-of-sample dates used for dynamic signal weighting.")
    parser.add_argument("--dynamic-min-history", type=int, default=5, help="Minimum out-of-sample dates before lifecycle rules move beyond promote state.")
    parser.add_argument("--dynamic-downweight-mean-rank-ic", type=float, default=0.01, help="Recent mean rank IC threshold below which active signals are downweighted.")
    parser.add_argument("--dynamic-deactivate-mean-rank-ic", type=float, default=-0.02, help="Recent mean rank IC threshold below which signals are deactivated.")
    parser.add_argument("--regime-aware-enabled", action="store_true", help="Enable regime-aware signal weighting on top of the dynamic lifecycle weights.")
    parser.add_argument("--regime-min-history", type=int, default=5, help="Minimum same-regime out-of-sample observations before regime-aware weighting reacts.")
    parser.add_argument("--regime-underweight-mean-rank-ic", type=float, default=0.01, help="Same-regime mean rank IC threshold below which signals are underweighted.")
    parser.add_argument("--regime-exclude-mean-rank-ic", type=float, default=-0.01, help="Same-regime mean rank IC threshold below which signals are excluded.")


def _add_live_base_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--symbols", nargs="+", default=None, help="Symbols to include in the run.")
    parser.add_argument("--universe", default=None, help="Named universe to trade instead of passing --symbols.")
    parser.add_argument("--strategy", default="sma_cross", help="Signal strategy name.")
    parser.add_argument("--fast", type=int, default=None)
    parser.add_argument("--slow", type=int, default=None)
    parser.add_argument("--lookback", type=int, default=None)
    _add_common_portfolio_selection_arguments(parser, required_top_n=False)
    parser.add_argument("--rebalance-frequency", default="daily")
    parser.add_argument("--timing", default="next_bar")
    parser.add_argument("--initial-cash", type=float, default=100_000.0)
    parser.add_argument("--min-trade-dollars", type=float, default=25.0)
    parser.add_argument("--lot-size", type=int, default=1)
    parser.add_argument("--reserve-cash-pct", type=float, default=0.0)
    parser.add_argument("--order-type", default="market")
    parser.add_argument("--time-in-force", default="day")
    parser.add_argument("--broker", default="mock", choices=["mock", "alpaca"])
    parser.add_argument("--mock-equity", type=float, default=100_000.0)
    parser.add_argument("--mock-cash", type=float, default=100_000.0)
    parser.add_argument("--mock-positions-path", default=None)


def _cmd_research_run(args) -> None:
    if getattr(args, "config", None):
        cmd_run_job(args)
        return
    cmd_research(args)


def _cmd_research_sweep(args) -> None:
    if getattr(args, "config", None):
        cmd_run_sweep(args)
        return
    cmd_sweep(args)


def _cmd_research_walkforward(args) -> None:
    if getattr(args, "config", None):
        cmd_run_walk_forward(args)
        return
    cmd_walkforward(args)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="trading-cli",
        description=(
            "Trading platform CLI. Use grouped command families for data, research, "
            "portfolio construction, paper trading, live controls, and experiment reporting."
        ),
    )
    subparsers = parser.add_subparsers(dest="command_family", required=True)

    data_parser = subparsers.add_parser("data", help="Data ingest, feature generation, and universe registry commands")
    data_subparsers = data_parser.add_subparsers(dest="data_command", required=True)
    data_ingest = data_subparsers.add_parser("ingest", help="Download raw OHLCV data")
    add_shared_symbol_args(data_ingest)
    data_ingest.add_argument("--start", type=str, default="2010-01-01", help="Start date in YYYY-MM-DD format (default: 2010-01-01)")
    data_ingest.add_argument("--fail-fast", action="store_true", help="Stop on the first ingest failure instead of continuing through the batch.")
    data_ingest.add_argument("--failure-report", type=str, default=None, help="Optional CSV path for per-symbol ingest failures.")
    data_ingest.set_defaults(func=cmd_ingest)
    data_features = data_subparsers.add_parser("features", help="Build feature datasets. Canonical path: `trading-cli data features --symbols ...` or `--universe ...`.")
    add_shared_symbol_args(data_features)
    add_feature_arguments(data_features)
    data_features.set_defaults(func=cmd_features)
    data_universes = data_subparsers.add_parser("universes", help="Inspect or export named universes")
    data_universe_subparsers = data_universes.add_subparsers(dest="universe_command", required=True)
    data_universe_list = data_universe_subparsers.add_parser("list", help="Show available named universes")
    data_universe_list.set_defaults(func=cmd_list_universes)
    data_universe_export = data_universe_subparsers.add_parser("export", help="Export the current static universe definitions to JSON")
    data_universe_export.add_argument("--output", type=str, default="artifacts/universes/universes.json", help="Path where the universe definitions JSON should be written.")
    data_universe_export.set_defaults(func=cmd_export_universes)

    research_parser = subparsers.add_parser("research", help="Ad hoc, config-driven, and operational research workflows")
    research_subparsers = research_parser.add_subparsers(dest="research_command", required=True)
    research_run = research_subparsers.add_parser("run", help="Run backtests directly or from a config file")
    add_shared_symbol_args(research_run)
    add_strategy_arguments(research_run, include_xsec=True)
    add_xsec_research_arguments(research_run)
    add_date_range_arguments(research_run)
    add_execution_arguments(research_run)
    research_run.add_argument("--engine", type=str, default="legacy", choices=["legacy", "vectorized"], help="Backtest engine to use")
    research_run.add_argument("--output-dir", type=str, default=None, help="Optional directory to save vectorized research outputs")
    research_run.add_argument("--cost-bps", type=float, default=None, help="Optional transaction cost in basis points per unit of turnover. Overrides --commission when provided.")
    research_run.add_argument("--config", type=str, default=None, help="Optional YAML or JSON research workflow config file.")
    research_run.add_argument("--fail-fast", action="store_true", help="Stop immediately on the first symbol error when using --config.")
    research_run.set_defaults(func=_cmd_research_run)
    research_sweep = research_subparsers.add_parser("sweep", help="Run parameter sweeps directly or from a config file")
    add_shared_symbol_args(research_sweep)
    add_date_range_arguments(research_sweep)
    add_execution_arguments(research_sweep)
    research_sweep.add_argument("--strategy", type=str, default="sma_cross", choices=get_strategy_choices(include_xsec=True), help="Strategy to sweep")
    research_sweep.add_argument("--fast-values", type=int, nargs="+")
    research_sweep.add_argument("--slow-values", type=int, nargs="+")
    research_sweep.add_argument("--lookback-values", type=int, nargs="+")
    research_sweep.add_argument("--lookback-bars-values", type=int, nargs="+")
    research_sweep.add_argument("--skip-bars-values", type=int, nargs="+")
    research_sweep.add_argument("--top-n-values", type=int, nargs="+")
    research_sweep.add_argument("--rebalance-bars-values", type=int, nargs="+")
    research_sweep.add_argument("--max-position-weight", type=float, default=None, help="Optional cap on any single xsec position weight.")
    research_sweep.add_argument("--min-avg-dollar-volume", type=float, default=None, help="Optional minimum 20-bar average dollar volume required for xsec eligibility.")
    research_sweep.add_argument("--max-names-per-sector", type=int, default=None, help="Optional maximum number of selected names per sector when sector metadata is available.")
    research_sweep.add_argument("--turnover-buffer-bps", type=float, default=0.0, help="Optional minimum momentum-score improvement, expressed in bps of score gap, required to replace an existing xsec holding.")
    research_sweep.add_argument("--max-turnover-per-rebalance", type=float, default=None, help="Optional cap on absolute turnover per xsec rebalance.")
    research_sweep.add_argument("--weighting-scheme", type=str, default="equal", choices=["equal", "inv_vol"], help="How to size selected xsec holdings.")
    research_sweep.add_argument("--vol-lookback-bars", type=int, default=20, help="Lookback window for inverse-vol xsec weighting.")
    research_sweep.add_argument("--benchmark", type=str, default="equal_weight", choices=["equal_weight"], help="Benchmark type for cross-sectional research")
    research_sweep.add_argument("--entry-lookback-values", type=int, nargs="+")
    research_sweep.add_argument("--exit-lookback-values", type=int, nargs="+")
    research_sweep.add_argument("--momentum-lookback-values", type=int, nargs="+")
    research_sweep.add_argument("--cash", type=float, default=10_000)
    research_sweep.add_argument("--commission", type=float, default=0.001)
    research_sweep.add_argument("--cost-bps", type=float, default=None, help="Optional transaction cost in basis points per unit of turnover. Overrides --commission when provided.")
    research_sweep.add_argument("--output", type=str, default="artifacts/experiments/sweep_results.csv", help="CSV output path for sweep summary")
    research_sweep.add_argument("--engine", type=str, default="legacy", choices=["legacy", "vectorized"], help="Backtest engine to use")
    research_sweep.add_argument("--config", type=str, default=None, help="Optional YAML or JSON parameter sweep config file.")
    research_sweep.add_argument("--fail-fast", action="store_true", help="Stop immediately on the first sweep error when using --config.")
    research_sweep.set_defaults(func=_cmd_research_sweep)
    research_walkforward = research_subparsers.add_parser("walkforward", help="Run walk-forward validation directly or from a config file")
    _add_walkforward_arguments(research_walkforward)
    research_walkforward.set_defaults(func=_cmd_research_walkforward)
    research_validate_signal = research_subparsers.add_parser("validate-signal", help="Validate a signal on one ticker or a small universe with per-symbol reports")
    _add_validate_signal_arguments(research_validate_signal)
    research_validate_signal.set_defaults(func=cmd_validate_signal)
    research_pipeline = research_subparsers.add_parser("pipeline", help="Run ingest, features, and legacy research in one command")
    add_shared_symbol_args(research_pipeline)
    add_feature_arguments(research_pipeline)
    add_strategy_arguments(research_pipeline)
    research_pipeline.add_argument("--start", type=str, default="2010-01-01", help="Start date in YYYY-MM-DD format (default: 2010-01-01)")
    research_pipeline.set_defaults(func=cmd_pipeline)
    research_alpha = research_subparsers.add_parser("alpha", help="Run cross-sectional alpha research")
    _add_alpha_research_arguments(research_alpha)
    research_alpha.set_defaults(func=cmd_alpha_research)
    research_loop = research_subparsers.add_parser("loop", help="Run the automated alpha research loop")
    _add_alpha_loop_arguments(research_loop)
    research_loop.set_defaults(func=cmd_alpha_research_loop)
    research_multi = research_subparsers.add_parser("multi-universe", help="Run alpha research across multiple named universes")
    _add_multi_universe_arguments(research_multi)
    research_multi.set_defaults(func=cmd_multi_universe_alpha_research)
    research_multi_report = research_subparsers.add_parser("multi-universe-report", help="Build a cross-universe comparison report from existing outputs")
    research_multi_report.add_argument("--output-dir", type=str, default="artifacts/multi_universe_alpha_research", help="Directory containing per-universe outputs and the comparison artifacts.")
    research_multi_report.set_defaults(func=cmd_multi_universe_report)
    research_refresh = research_subparsers.add_parser("refresh", help="Run the scheduled alpha discovery refresh workflow")
    _add_refresh_arguments(research_refresh)
    research_refresh.set_defaults(func=cmd_research_refresh)
    research_monitor = research_subparsers.add_parser("monitor", help="Generate monitoring reports and drift alerts from recent research and paper artifacts")
    research_monitor.add_argument("--tracker-dir", type=str, default="artifacts/experiment_tracking", help="Directory containing the shared experiment registry.")
    research_monitor.add_argument("--output-dir", type=str, default="artifacts/research_monitoring", help="Directory where monitoring artifacts will be written.")
    research_monitor.add_argument("--snapshot-dir", type=str, default="artifacts/research_refresh/approved_configuration_snapshots", help="Directory containing approved configuration snapshots.")
    research_monitor.add_argument("--alpha-artifact-dir", type=str, default=None, help="Optional alpha artifact directory override.")
    research_monitor.add_argument("--paper-artifact-dir", type=str, default=None, help="Optional paper artifact directory override.")
    research_monitor.add_argument("--recent-paper-runs", type=int, default=10, help="Number of recent paper runs to use for realized diagnostics.")
    research_monitor.add_argument("--performance-degradation-buffer", type=float, default=0.002, help="Absolute buffer before recent paper returns trigger a degradation alert.")
    research_monitor.add_argument("--turnover-spike-multiple", type=float, default=1.5, help="Multiple of expected turnover that triggers a turnover spike alert.")
    research_monitor.add_argument("--concentration-spike-multiple", type=float, default=1.5, help="Multiple of expected top-position weight that triggers a concentration alert.")
    research_monitor.add_argument("--signal-churn-threshold", type=int, default=3, help="Number of approved-signal additions/removals that triggers a churn alert.")
    research_monitor.set_defaults(func=cmd_research_monitor)
    research_strategies = research_subparsers.add_parser("strategies", help="Show available legacy strategies")
    research_strategies.set_defaults(func=cmd_list_strategies)

    portfolio_parser = subparsers.add_parser("portfolio", help="Portfolio backtests and ranking-based portfolio construction")
    portfolio_subparsers = portfolio_parser.add_subparsers(dest="portfolio_command", required=True)
    portfolio_backtest = portfolio_subparsers.add_parser("backtest", help="Run an equal-weight portfolio backtest across multiple symbols")
    add_shared_symbol_args(portfolio_backtest)
    add_strategy_arguments(portfolio_backtest)
    add_execution_arguments(portfolio_backtest)
    portfolio_backtest.add_argument("--output-dir", type=str, default="data/experiments/portfolio", help="Directory for portfolio outputs")
    portfolio_backtest.set_defaults(func=cmd_portfolio)
    portfolio_topn = portfolio_subparsers.add_parser("topn", help="Run a top-N cross-sectional portfolio backtest")
    add_shared_symbol_args(portfolio_topn)
    add_strategy_arguments(portfolio_topn)
    add_execution_arguments(portfolio_topn)
    _add_common_portfolio_selection_arguments(portfolio_topn, required_top_n=True)
    portfolio_topn.add_argument("--output-dir", type=str, default="data/experiments/portfolio_topn", help="Directory for portfolio outputs")
    portfolio_topn.set_defaults(func=cmd_portfolio_topn)

    paper_parser = subparsers.add_parser("paper", help="Paper trading workflows")
    paper_subparsers = paper_parser.add_subparsers(dest="paper_command", required=True)
    paper_run = paper_subparsers.add_parser("run", help="Run one paper-trading cycle and write state/artifacts")
    add_shared_symbol_args(paper_run)
    add_strategy_arguments(paper_run)
    add_execution_arguments(paper_run)
    add_composite_paper_arguments(paper_run)
    add_experiment_tracker_argument(paper_run)
    _add_common_portfolio_selection_arguments(paper_run, required_top_n=True)
    paper_run.add_argument("--timing", type=str, default="next_bar", choices=["same_bar", "next_bar"], help="When scheduled target weights become effective")
    paper_run.add_argument("--initial-cash", type=float, default=100_000.0, help="Starting cash used when no paper state exists yet")
    paper_run.add_argument("--min-trade-dollars", type=float, default=25.0, help="Skip trades smaller than this dollar threshold")
    paper_run.add_argument("--lot-size", type=int, default=1, help="Round target quantities down to this lot size")
    paper_run.add_argument("--reserve-cash-pct", type=float, default=0.0, help="Fraction of equity to hold back as cash")
    paper_run.add_argument("--state-path", type=str, default="artifacts/paper/paper_state.json", help="JSON file used to persist paper portfolio state")
    paper_run.add_argument("--output-dir", type=str, default="artifacts/paper", help="Base directory for paper-run output artifacts")
    paper_run.add_argument("--auto-apply-fills", action="store_true", help="Immediately apply simulated fills and update positions/cash")
    paper_run.set_defaults(func=cmd_paper_run)

    paper_daily = paper_subparsers.add_parser("daily", help="Run the daily paper trading workflow")
    add_composite_paper_arguments(paper_daily)
    paper_daily.add_argument("--strategy", default="sma_cross", help="Signal strategy name.")
    paper_daily.add_argument("--fast", type=int, default=None, help="Fast lookback parameter for the signal.")
    paper_daily.add_argument("--slow", type=int, default=None, help="Slow lookback parameter for the signal.")
    paper_daily.add_argument("--lookback", type=int, default=None, help="Lookback parameter for the signal.")
    paper_daily.add_argument("--top-n", type=int, default=10, help="Number of symbols to select.")
    paper_daily.add_argument("--weighting-scheme", default="equal", help="Weighting scheme for portfolio construction.")
    paper_daily.add_argument("--vol-window", type=int, default=20, help="Volatility lookback window for inverse-vol weighting.")
    paper_daily.add_argument("--min-score", type=float, default=None, help="Minimum score threshold for portfolio inclusion.")
    paper_daily.add_argument("--max-weight", type=float, default=None, help="Maximum position weight.")
    paper_daily.add_argument("--max-names-per-group", type=int, default=None, help="Maximum number of names per group.")
    paper_daily.add_argument("--max-group-weight", type=float, default=None, help="Maximum aggregate weight per group.")
    paper_daily.add_argument("--group-map-path", default=None, help="Optional path to symbol-to-group mapping file.")
    paper_daily.add_argument("--rebalance-frequency", default="daily", help="Rebalance frequency.")
    paper_daily.add_argument("--timing", default="next_bar", help="Execution timing policy.")
    paper_daily.add_argument("--initial-cash", type=float, default=100_000.0, help="Initial paper trading cash balance.")
    paper_daily.add_argument("--min-trade-dollars", type=float, default=25.0, help="Minimum trade notional.")
    paper_daily.add_argument("--lot-size", type=int, default=1, help="Trading lot size.")
    paper_daily.add_argument("--reserve-cash-pct", type=float, default=0.0, help="Fraction of equity to keep in cash reserve.")
    paper_daily.add_argument("--state-path", required=True, help="Path to the paper trading state file.")
    paper_daily.add_argument("--output-dir", required=True, help="Directory for paper trading artifacts.")
    paper_daily.add_argument("--auto-apply-fills", action="store_true", help="Apply simulated fills through the paper broker.")
    paper_daily.add_argument("--symbols", nargs="+", default=None, help="Symbols to include in the daily paper trading job.")
    paper_daily.add_argument("--universe", default=None, help="Named universe to trade instead of passing --symbols.")
    paper_daily.set_defaults(func=cmd_daily_paper_job)

    paper_report = paper_subparsers.add_parser("report", help="Build a summary report from paper trading ledgers")
    paper_report.add_argument("--account-dir", required=True, help="Base paper account directory containing ledgers/.")
    paper_report.add_argument("--output-dir", default=None, help="Optional directory to write report artifacts.")
    paper_report.set_defaults(func=cmd_paper_report)

    live_parser = subparsers.add_parser("live", help="Broker preview, validation, and guarded execution commands")
    live_subparsers = live_parser.add_subparsers(dest="live_command", required=True)
    live_dry_run = live_subparsers.add_parser("dry-run", help="Compute live broker rebalance orders without sending them")
    _add_live_base_arguments(live_dry_run)
    live_dry_run.set_defaults(func=cmd_live_dry_run)
    live_validate = live_subparsers.add_parser("validate", help="Run live execution control checks and write artifacts without submitting orders")
    _add_live_base_arguments(live_validate)
    add_live_control_arguments(live_validate)
    live_validate.set_defaults(func=cmd_validate_live)
    live_execute = live_subparsers.add_parser("execute", help="Run live execution control checks and only submit orders if approved and safe")
    _add_live_base_arguments(live_execute)
    add_live_control_arguments(live_execute)
    live_execute.set_defaults(func=cmd_execute_live)

    experiments_parser = subparsers.add_parser("experiments", help="Experiment registry inspection and dashboard commands")
    experiments_subparsers = experiments_parser.add_subparsers(dest="experiments_command", required=True)
    experiments_list = experiments_subparsers.add_parser("list", help="List recent tracked research and paper trading experiments")
    experiments_list.add_argument("--tracker-dir", type=str, default="artifacts/experiment_tracking", help="Directory containing the shared experiment registry.")
    experiments_list.add_argument("--limit", type=int, default=10, help="Maximum number of experiments to print.")
    experiments_list.set_defaults(func=cmd_experiments_list)
    experiments_latest = experiments_subparsers.add_parser("latest", help="Show the latest approved composite or research configuration snapshot")
    experiments_latest.add_argument("--tracker-dir", type=str, default="artifacts/experiment_tracking", help="Directory containing the shared experiment registry.")
    experiments_latest.set_defaults(func=cmd_experiments_latest_model)
    experiments_dashboard = experiments_subparsers.add_parser("dashboard", help="Build a summary dashboard artifact from tracked experiments")
    experiments_dashboard.add_argument("--tracker-dir", type=str, default="artifacts/experiment_tracking", help="Directory containing the shared experiment registry.")
    experiments_dashboard.add_argument("--output-dir", type=str, default=None, help="Optional directory where the dashboard artifacts should be written.")
    experiments_dashboard.add_argument("--top-metric", type=str, default="portfolio_sharpe", help="Registry metric used to rank top experiments in the dashboard.")
    experiments_dashboard.add_argument("--limit", type=int, default=10, help="Maximum number of top experiments to include.")
    experiments_dashboard.set_defaults(func=cmd_experiments_dashboard)
    experiments_diff = experiments_subparsers.add_parser("diff", help="Show the current approved configuration versus the prior approved snapshot")
    experiments_diff.add_argument("--snapshot-dir", type=str, default="artifacts/research_refresh/approved_configuration_snapshots", help="Directory containing approved configuration snapshots.")
    experiments_diff.set_defaults(func=cmd_approved_config_diff)

    return parser
