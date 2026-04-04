from __future__ import annotations

import argparse

from trading_platform.cli.commands.alpha_research import cmd_alpha_research
from trading_platform.cli.commands.alpha_research_loop import cmd_alpha_research_loop
from trading_platform.cli.commands.adaptive_allocation_build import cmd_adaptive_allocation_build
from trading_platform.cli.commands.adaptive_allocation_export_run_config import (
    cmd_adaptive_allocation_export_run_config,
)
from trading_platform.cli.commands.adaptive_allocation_show import cmd_adaptive_allocation_show
from trading_platform.cli.commands.approved_config_diff import cmd_approved_config_diff
from trading_platform.cli.commands.binance_crypto_alerts import cmd_binance_crypto_alerts
from trading_platform.cli.commands.binance_crypto_health_check import cmd_binance_crypto_health_check
from trading_platform.cli.commands.binance_crypto_historical_ingest import cmd_binance_crypto_historical_ingest
from trading_platform.cli.commands.binance_crypto_features import cmd_binance_crypto_features
from trading_platform.cli.commands.binance_crypto_normalize import cmd_binance_crypto_normalize
from trading_platform.cli.commands.binance_crypto_notify import cmd_binance_crypto_notify
from trading_platform.cli.commands.binance_crypto_project import cmd_binance_crypto_project
from trading_platform.cli.commands.binance_crypto_status import cmd_binance_crypto_status
from trading_platform.cli.commands.binance_crypto_sync import cmd_binance_crypto_sync
from trading_platform.cli.commands.binance_crypto_websocket_ingest import cmd_binance_crypto_websocket_ingest
from trading_platform.cli.commands.broker_cancel_all import cmd_broker_cancel_all
from trading_platform.cli.commands.broker_health import cmd_broker_health
from trading_platform.cli.commands.compare_xsec_construction import cmd_compare_xsec_construction
from trading_platform.cli.commands.cross_market_monitor import cmd_cross_market_monitor
from trading_platform.cli.commands.daily_paper_job import cmd_daily_paper_job
from trading_platform.cli.commands.dashboard_build_static_data import cmd_dashboard_build_static_data
from trading_platform.cli.commands.data_build_classifications import cmd_data_build_classifications
from trading_platform.cli.commands.dashboard_serve import cmd_dashboard_serve
from trading_platform.cli.commands.decision_memo import cmd_decision_memo
from trading_platform.cli.commands.doctor import cmd_doctor
from trading_platform.cli.commands.execute_live import cmd_execute_live
from trading_platform.cli.commands.execution_simulate import cmd_execution_simulate
from trading_platform.cli.commands.experiment_compare import cmd_experiment_compare
from trading_platform.cli.commands.experiment_recommend_defaults import cmd_experiment_recommend_defaults
from trading_platform.cli.commands.experiment_run import cmd_experiment_run
from trading_platform.cli.commands.experiment_show import cmd_experiment_show
from trading_platform.cli.commands.experiment_summarize_campaign import cmd_experiment_summarize_campaign
from trading_platform.cli.commands.experiments_dashboard import cmd_experiments_dashboard
from trading_platform.cli.commands.experiments_latest_model import cmd_experiments_latest_model
from trading_platform.cli.commands.experiments_list import cmd_experiments_list
from trading_platform.cli.commands.export_universes import cmd_export_universes
from trading_platform.cli.commands.features import cmd_features
from trading_platform.cli.commands.kalshi_features import cmd_kalshi_features
from trading_platform.cli.commands.kalshi_historical_ingest import cmd_kalshi_historical_ingest
from trading_platform.cli.commands.kalshi_live_candles import cmd_kalshi_live_candles
from trading_platform.cli.commands.kalshi_recent_ingest import cmd_kalshi_recent_ingest
from trading_platform.cli.commands.kalshi_validate_dataset import cmd_kalshi_validate_dataset
from trading_platform.cli.commands.fundamentals_features import cmd_fundamentals_features
from trading_platform.cli.commands.fundamentals_ingest import cmd_fundamentals_ingest
from trading_platform.cli.commands.fundamentals_snapshot_build import cmd_fundamentals_snapshot_build
from trading_platform.cli.commands.refresh_research_inputs import cmd_refresh_research_inputs
from trading_platform.cli.commands.ingest import cmd_ingest
from trading_platform.cli.commands.list_strategies import cmd_list_strategies
from trading_platform.cli.commands.list_universes import cmd_list_universes
from trading_platform.cli.commands.live_dry_run import cmd_live_dry_run
from trading_platform.cli.commands.live_dry_run_multi_strategy import cmd_live_dry_run_multi_strategy
from trading_platform.cli.commands.live_run_scheduled import cmd_live_run_scheduled
from trading_platform.cli.commands.live_submit import cmd_live_submit
from trading_platform.cli.commands.live_submit_multi_strategy import cmd_live_submit_multi_strategy
from trading_platform.cli.commands.multi_universe_alpha_research import (
    cmd_multi_universe_alpha_research,
)
from trading_platform.cli.commands.multi_universe_report import cmd_multi_universe_report
from trading_platform.cli.commands.monitor_build_dashboard_data import cmd_monitor_build_dashboard_data
from trading_platform.cli.commands.monitor_latest import cmd_monitor_latest
from trading_platform.cli.commands.monitor_notify import cmd_monitor_notify
from trading_platform.cli.commands.monitor_portfolio_health import cmd_monitor_portfolio_health
from trading_platform.cli.commands.monitor_run_health import cmd_monitor_run_health
from trading_platform.cli.commands.monitor_strategy_health import cmd_monitor_strategy_health
from trading_platform.cli.commands.ops_monitor_providers_health import cmd_ops_monitor_providers_health
from trading_platform.cli.commands.ops_monitor_provider_timeline import cmd_ops_monitor_provider_timeline
from trading_platform.cli.commands.ops_monitor_dataset_timeline import cmd_ops_monitor_dataset_timeline
from trading_platform.cli.commands.ops_monitor_providers_summary import cmd_ops_monitor_providers_summary
from trading_platform.cli.commands.autonomous_loop import cmd_autonomous_loop_start
from trading_platform.cli.commands.orchestrate_run import cmd_orchestrate_loop, cmd_orchestrate_run
from trading_platform.cli.commands.orchestrate_show_run import cmd_orchestrate_show_run
from trading_platform.cli.commands.paper_report import cmd_paper_report
from trading_platform.cli.commands.paper_replay_multi_strategy import cmd_paper_replay_multi_strategy
from trading_platform.cli.commands.paper_run import cmd_paper_run
from trading_platform.cli.commands.paper_run_multi_strategy import cmd_paper_run_multi_strategy
from trading_platform.cli.commands.paper_run_scheduled import cmd_paper_run_scheduled
from trading_platform.cli.commands.pipeline import cmd_pipeline
from trading_platform.cli.commands.pipeline_alpha_cycle import cmd_pipeline_alpha_cycle
from trading_platform.cli.commands.pipeline_daily_trading import cmd_pipeline_daily_trading
from trading_platform.cli.commands.pipeline_replay_daily import cmd_pipeline_replay_daily
from trading_platform.cli.commands.pipeline_run import (
    cmd_pipeline_run,
    cmd_pipeline_run_daily,
    cmd_pipeline_run_weekly,
)
from trading_platform.cli.commands.portfolio import cmd_portfolio
from trading_platform.cli.commands.portfolio_apply_execution_constraints import (
    cmd_portfolio_apply_execution_constraints,
)
from trading_platform.cli.commands.portfolio_allocate_multi_strategy import cmd_portfolio_allocate_multi_strategy
from trading_platform.cli.commands.portfolio_optimize_research import cmd_portfolio_optimize_research
from trading_platform.cli.commands.portfolio_topn import cmd_portfolio_topn
from trading_platform.cli.commands.research import cmd_research
from trading_platform.cli.commands.research_compare_runs import cmd_research_compare_runs
from trading_platform.cli.commands.research_db import (
    cmd_research_db_family_summary,
    cmd_research_db_init,
    cmd_research_db_list_runs,
    cmd_research_db_promotions,
    cmd_research_db_top_candidates,
)
from trading_platform.cli.commands.kalshi_alpha_research import cmd_kalshi_alpha_research
from trading_platform.cli.commands.kalshi_full_backtest import cmd_kalshi_full_backtest
from trading_platform.cli.commands.polymarket_ingest import cmd_polymarket_ingest
from trading_platform.cli.commands.polymarket_live import cmd_polymarket_live_collect
from trading_platform.cli.commands.polymarket_blockchain_ingest import cmd_polymarket_blockchain_ingest
from trading_platform.cli.commands.polymarket_wallet_profiles import cmd_polymarket_wallet_profiles
from trading_platform.cli.commands.polymarket_clob_fetch import cmd_polymarket_clob_fetch, cmd_polymarket_orderbook_fetch
from trading_platform.cli.commands.polymarket_data_api_fetch import cmd_polymarket_data_api_fetch
from trading_platform.cli.commands.manifold_parse import cmd_manifold_parse
from trading_platform.cli.commands.predictit_parse import cmd_predictit_parse
from trading_platform.cli.commands.news_tagger import cmd_news_upcoming, cmd_news_label_moves
from trading_platform.cli.commands.metaculus_fetch import cmd_metaculus_fetch
from trading_platform.cli.commands.kalshi_paper_run import cmd_kalshi_paper_run
from trading_platform.cli.commands.research_leaderboard import cmd_research_leaderboard
from trading_platform.cli.commands.research_monitor import cmd_research_monitor
from trading_platform.cli.commands.research_promote import cmd_research_promote
from trading_platform.cli.commands.research_promotion_candidates import cmd_research_promotion_candidates
from trading_platform.cli.commands.research_dataset_registry_list import cmd_research_dataset_registry_list
from trading_platform.cli.commands.research_dataset_registry_publish import cmd_research_dataset_registry_publish
from trading_platform.cli.commands.research_replay_assemble import cmd_research_replay_assemble
from trading_platform.cli.commands.research_replay_consume import cmd_research_replay_consume
from trading_platform.cli.commands.research_replay_evaluate import cmd_research_replay_evaluate
from trading_platform.cli.commands.research_refresh import cmd_research_refresh
from trading_platform.cli.commands.research_registry_build import cmd_research_registry_build
from trading_platform.cli.commands.research_validate_backtester import cmd_research_validate_backtester
from trading_platform.cli.commands.regime_detect import cmd_regime_detect
from trading_platform.cli.commands.regime_show import cmd_regime_show
from trading_platform.cli.commands.registry_build_multi_strategy_config import cmd_registry_build_multi_strategy_config
from trading_platform.cli.commands.registry_demote import cmd_registry_demote
from trading_platform.cli.commands.registry_evaluate_degradation import cmd_registry_evaluate_degradation
from trading_platform.cli.commands.registry_evaluate_promotion import cmd_registry_evaluate_promotion
from trading_platform.cli.commands.registry_list import cmd_registry_list
from trading_platform.cli.commands.registry_promote import cmd_registry_promote
from trading_platform.cli.commands.run_job import cmd_run_job
from trading_platform.cli.commands.run_sweep import cmd_run_sweep
from trading_platform.cli.commands.run_walk_forward import cmd_run_walk_forward
from trading_platform.cli.commands.sweep import cmd_sweep
from trading_platform.cli.commands.strategy_portfolio_build import cmd_strategy_portfolio_build
from trading_platform.cli.commands.strategy_portfolio_activate import cmd_strategy_portfolio_activate
from trading_platform.cli.commands.strategy_portfolio_experiment_bundle import (
    cmd_strategy_portfolio_experiment_bundle,
)
from trading_platform.cli.commands.strategy_portfolio_experiment_bundle_matrix import (
    cmd_strategy_portfolio_experiment_bundle_matrix,
)
from trading_platform.cli.commands.strategy_portfolio_export_run_config import cmd_strategy_portfolio_export_run_config
from trading_platform.cli.commands.strategy_portfolio_show import cmd_strategy_portfolio_show
from trading_platform.cli.commands.strategy_monitor_build import cmd_strategy_monitor_build
from trading_platform.cli.commands.strategy_monitor_recommend_kill_switch import (
    cmd_strategy_monitor_recommend_kill_switch,
)
from trading_platform.cli.commands.strategy_monitor_show import cmd_strategy_monitor_show
from trading_platform.cli.commands.strategy_governance_apply import cmd_strategy_governance_apply
from trading_platform.cli.commands.strategy_lifecycle_show import cmd_strategy_lifecycle_show
from trading_platform.cli.commands.strategy_lifecycle_update import cmd_strategy_lifecycle_update
from trading_platform.cli.commands.system_eval_build import cmd_system_eval_build
from trading_platform.cli.commands.system_eval_compare import cmd_system_eval_compare
from trading_platform.cli.commands.system_eval_show import cmd_system_eval_show
from trading_platform.cli.commands.validate_signal import cmd_validate_signal
from trading_platform.cli.commands.validate_live import cmd_validate_live
from trading_platform.cli.commands.walkforward import cmd_walkforward
from trading_platform.research.alpha_lab.signals import SUPPORTED_SIGNAL_FAMILIES
from trading_platform.cli.commands.strategy_validation_build import cmd_strategy_validation_build
from trading_platform.cli.common import (
    add_date_range_arguments,
    add_feature_arguments,
    add_preset_argument,
    add_shared_symbol_args,
    add_strategy_arguments,
    add_xsec_live_arguments,
    add_xsec_paper_arguments,
    add_xsec_research_arguments,
    get_strategy_choices,
)
from trading_platform.cli.presets import get_preset_choices
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
    "pipeline": ["ops", "pipeline"],
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
    "experiments-list": ["ops", "experiments", "list"],
    "experiments-latest-model": ["ops", "experiments", "latest"],
    "experiments-dashboard": ["ops", "experiments", "dashboard"],
    "approved-config-diff": ["ops", "experiments", "diff"],
    "registry": ["ops", "registry"],
    "monitor": ["ops", "monitor"],
    "broker": ["ops", "broker"],
    "execution": ["ops", "execution"],
    "doctor": ["ops", "doctor"],
    "orchestrate": ["ops", "orchestrate"],
    "system-eval": ["ops", "system-eval"],
    "experiment": ["ops", "experiment"],
    "experiments": ["ops", "experiments"],
}
_RESEARCH_GROUP_COMMANDS = {
    "run",
    "sweep",
    "walkforward",
    "compare-xsec-construction",
    "decision-memo",
    "memo",
    "validate-signal",
    "alpha",
    "loop",
    "multi-universe",
    "multi-universe-report",
    "refresh",
    "monitor",
    "registry",
    "dataset-registry",
    "leaderboard",
    "compare-runs",
    "promotion-candidates",
    "promote",
    "db",
    "pipeline",
    "strategies",
    "validate-backtester",
    "cross-market-monitor",
    "kalshi-alpha",
    "kalshi-full-backtest",
}
_PORTFOLIO_GROUP_COMMANDS = {
    "backtest",
    "topn",
    "allocate-multi-strategy",
    "apply-execution-constraints",
    "optimize-research",
}


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
        choices=["legacy", "composite", "ensemble"],
        help="Choose legacy strategy scores, the approved composite alpha signal, or the promoted-signal ensemble.",
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
        "--min-price", type=float, default=None, help="Optional minimum price required for composite eligibility."
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
    parser.add_argument(
        "--enable-ensemble",
        action="store_true",
        help="Enable the promoted-signal ensemble overlay for paper target construction.",
    )
    parser.add_argument(
        "--ensemble-mode",
        type=str,
        default="disabled",
        choices=["disabled", "candidate_weighted", "family_weighted"],
        help="How to combine eligible ensemble members.",
    )
    parser.add_argument(
        "--ensemble-weight-method",
        type=str,
        default="equal",
        choices=["equal", "performance_weighted", "rank_weighted"],
        help="How to weight eligible ensemble members.",
    )
    parser.add_argument(
        "--ensemble-normalize-scores",
        type=str,
        default="rank_pct",
        choices=["raw", "zscore", "rank_pct"],
        help="How to normalize member scores before ensemble aggregation.",
    )
    parser.add_argument(
        "--ensemble-max-members", type=int, default=5, help="Maximum number of candidates included in the ensemble."
    )
    parser.add_argument(
        "--ensemble-max-members-per-family",
        type=int,
        default=None,
        help="Optional cap on included candidates from the same family.",
    )
    parser.add_argument(
        "--ensemble-minimum-member-observations",
        type=int,
        default=0,
        help="Minimum observations required for an ensemble member.",
    )
    parser.add_argument(
        "--ensemble-minimum-member-metric",
        type=float,
        default=None,
        help="Optional minimum metric required for ensemble inclusion.",
    )


def add_experiment_tracker_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--experiment-tracker-dir",
        type=str,
        default=None,
        help="Optional directory used to persist the shared experiment registry and reports.",
    )


def add_database_tracking_arguments(
    parser: argparse.ArgumentParser,
    *,
    include_write_toggles: bool = False,
) -> None:
    parser.add_argument(
        "--database-enabled",
        dest="enable_database_metadata",
        action="store_true",
        default=None,
        help="Enable optional database-backed research metadata persistence for this command.",
    )
    parser.add_argument(
        "--database-disabled",
        dest="enable_database_metadata",
        action="store_false",
        help="Disable database-backed research metadata persistence for this command.",
    )
    parser.add_argument(
        "--database-url",
        type=str,
        default=None,
        help="Optional SQLAlchemy database URL override for research metadata tracking.",
    )
    parser.add_argument(
        "--database-schema",
        type=str,
        default=None,
        help="Optional database schema override for research metadata tracking.",
    )
    if include_write_toggles:
        parser.add_argument(
            "--tracking-write-candidates",
            dest="tracking_write_candidates",
            action="store_true",
            default=True,
            help="Persist signal candidate definitions to the research database.",
        )
        parser.add_argument(
            "--tracking-skip-candidates",
            dest="tracking_write_candidates",
            action="store_false",
            help="Skip candidate-definition writes even when database tracking is enabled.",
        )
        parser.add_argument(
            "--tracking-write-metrics",
            dest="tracking_write_metrics",
            action="store_true",
            default=True,
            help="Persist candidate metric summaries to the research database.",
        )
        parser.add_argument(
            "--tracking-skip-metrics",
            dest="tracking_write_metrics",
            action="store_false",
            help="Skip metric writes even when database tracking is enabled.",
        )
        parser.add_argument(
            "--tracking-write-promotions",
            dest="tracking_write_promotions",
            action="store_true",
            default=True,
            help="Persist promotion outputs to the research database.",
        )
        parser.add_argument(
            "--tracking-skip-promotions",
            dest="tracking_write_promotions",
            action="store_false",
            help="Skip promotion writes even when database tracking is enabled.",
        )


def add_live_control_arguments(parser: argparse.ArgumentParser) -> None:
    add_composite_paper_arguments(parser)
    parser.add_argument(
        "--kill-switch", action="store_true", help="Abort before submission regardless of target portfolio state."
    )
    parser.add_argument("--kill-switch-path", type=str, default=None, help="Abort if this file exists.")
    parser.add_argument(
        "--blocked-symbols", nargs="*", default=None, help="Optional list of symbols that must never be traded."
    )
    parser.add_argument(
        "--max-gross-exposure", type=float, default=1.0, help="Maximum allowed gross exposure after trades."
    )
    parser.add_argument(
        "--max-net-exposure", type=float, default=1.0, help="Maximum allowed absolute net exposure after trades."
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
    parser.add_argument("--max-order-notional", type=float, default=None, help="Maximum allowed single-order notional.")
    parser.add_argument(
        "--max-daily-turnover", type=float, default=None, help="Maximum allowed turnover estimate for the rebalance."
    )
    parser.add_argument(
        "--min-cash-reserve", type=float, default=0.0, help="Minimum required cash reserve fraction after trades."
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
        "--approved", action="store_true", help="Explicitly mark this run as approved for live execution."
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


def _add_common_portfolio_selection_arguments(parser: argparse.ArgumentParser, *, required_top_n: bool) -> None:
    parser.add_argument(
        "--top-n",
        type=int,
        required=required_top_n,
        default=None if required_top_n else 10,
        help="Number of top-ranked symbols to target",
    )
    parser.add_argument(
        "--weighting-scheme",
        type=str,
        default="equal",
        choices=["equal", "inverse_vol", "inv_vol"],
        help="How to size selected holdings",
    )
    parser.add_argument(
        "--vol-window", type=int, default=20, help="Rolling volatility window for inverse-vol weighting"
    )
    parser.add_argument(
        "--min-score", type=float, default=None, help="Optional minimum score required for a symbol to be held"
    )
    parser.add_argument("--max-weight", type=float, default=None, help="Optional cap on any single position weight")
    parser.add_argument(
        "--max-names-per-group", type=int, default=None, help="Optional maximum number of holdings allowed per group"
    )
    parser.add_argument(
        "--max-group-weight", type=float, default=None, help="Optional cap on total portfolio weight per group"
    )
    parser.add_argument(
        "--group-map-path", type=str, default=None, help="Optional path to CSV with columns: symbol,group"
    )


def _add_paper_run_arguments(parser: argparse.ArgumentParser) -> None:
    add_shared_symbol_args(parser)
    add_preset_argument(
        parser,
        help_text="Optional versioned preset for validated paper-trading defaults. Explicit CLI flags still override preset values.",
    )
    add_strategy_arguments(parser, include_xsec=True)
    add_execution_arguments(parser)
    add_composite_paper_arguments(parser)
    add_experiment_tracker_argument(parser)
    _add_common_portfolio_selection_arguments(parser, required_top_n=False)
    add_xsec_paper_arguments(parser)
    parser.add_argument(
        "--timing",
        type=str,
        default="next_bar",
        choices=["same_bar", "next_bar"],
        help="When scheduled target weights become effective",
    )
    parser.add_argument(
        "--initial-cash", type=float, default=100_000.0, help="Starting cash used when no paper state exists yet"
    )
    parser.add_argument(
        "--min-trade-dollars", type=float, default=25.0, help="Skip trades smaller than this dollar threshold"
    )
    parser.add_argument("--lot-size", type=int, default=1, help="Round target quantities down to this lot size")
    parser.add_argument("--reserve-cash-pct", type=float, default=0.0, help="Fraction of equity to hold back as cash")
    parser.add_argument(
        "--execution-config", type=str, default=None, help="Optional execution realism JSON/YAML config."
    )
    parser.add_argument(
        "--use-alpaca-latest-data",
        action="store_true",
        help="Use Alpaca for the latest execution-time OHLCV bars and fall back to historical data on failure.",
    )
    parser.add_argument(
        "--latest-data-max-age-seconds",
        type=int,
        default=86_400,
        help="Mark latest execution-time market data as stale when the latest bar exceeds this age.",
    )
    parser.add_argument(
        "--slippage-model",
        type=str,
        default="none",
        choices=["none", "fixed_bps"],
        help="Optional paper-only slippage model used for expected fill pricing.",
    )
    parser.add_argument(
        "--slippage-buy-bps",
        type=float,
        default=0.0,
        help="Paper-only buy-side slippage in basis points when --slippage-model fixed_bps is enabled.",
    )
    parser.add_argument(
        "--slippage-sell-bps",
        type=float,
        default=0.0,
        help="Paper-only sell-side slippage in basis points when --slippage-model fixed_bps is enabled.",
    )
    parser.add_argument(
        "--state-path",
        type=str,
        default="artifacts/paper/paper_state.json",
        help="JSON file used to persist paper portfolio state",
    )
    parser.add_argument(
        "--output-dir", type=str, default="artifacts/paper", help="Base directory for paper-run output artifacts"
    )
    parser.add_argument(
        "--auto-apply-fills", action="store_true", help="Immediately apply simulated fills and update positions/cash"
    )


def _add_walkforward_arguments(parser: argparse.ArgumentParser) -> None:
    add_shared_symbol_args(parser)
    parser.add_argument(
        "--config", type=str, default=None, help="Optional YAML or JSON config file for reproducible walk-forward jobs."
    )
    parser.add_argument(
        "--strategy",
        type=str,
        default="sma_cross",
        choices=get_strategy_choices(include_xsec=True),
        help="Strategy to validate",
    )
    add_date_range_arguments(parser)
    add_execution_arguments(parser)
    parser.add_argument("--fast-values", type=int, nargs="+")
    parser.add_argument("--slow-values", type=int, nargs="+")
    parser.add_argument("--lookback-values", type=int, nargs="+")
    parser.add_argument("--lookback-bars-values", type=int, nargs="+")
    parser.add_argument("--skip-bars-values", type=int, nargs="+")
    parser.add_argument("--top-n-values", type=int, nargs="+")
    parser.add_argument("--rebalance-bars-values", type=int, nargs="+")
    parser.add_argument(
        "--portfolio-construction-mode",
        type=str,
        default="pure_topn",
        choices=["pure_topn", "transition"],
        help="Use pure_topn for research-clean top-N portfolios or transition for gradual deployable transitions.",
    )
    parser.add_argument(
        "--max-position-weight", type=float, default=None, help="Optional cap on any single xsec position weight."
    )
    parser.add_argument(
        "--min-avg-dollar-volume",
        type=float,
        default=None,
        help="Optional minimum 20-bar average dollar volume required for xsec eligibility.",
    )
    parser.add_argument(
        "--max-names-per-sector",
        type=int,
        default=None,
        help="Optional maximum number of selected names per sector when sector metadata is available.",
    )
    parser.add_argument(
        "--turnover-buffer-bps",
        type=float,
        default=0.0,
        help="Optional minimum momentum-score improvement, expressed in bps of score gap, required to replace an existing xsec holding.",
    )
    parser.add_argument(
        "--max-turnover-per-rebalance",
        type=float,
        default=None,
        help="Optional cap on absolute turnover per xsec rebalance.",
    )
    parser.add_argument(
        "--weighting-scheme",
        type=str,
        default="equal",
        choices=["equal", "inv_vol"],
        help="How to size selected xsec holdings.",
    )
    parser.add_argument(
        "--vol-lookback-bars", type=int, default=20, help="Lookback window for inverse-vol xsec weighting."
    )
    parser.add_argument(
        "--benchmark",
        type=str,
        default="equal_weight",
        choices=["equal_weight"],
        help="Benchmark type for cross-sectional research",
    )
    parser.add_argument("--entry-lookback-values", type=int, nargs="+")
    parser.add_argument("--exit-lookback-values", type=int, nargs="+")
    parser.add_argument("--momentum-lookback-values", type=int, nargs="+")
    parser.add_argument("--train-years", type=int, default=5)
    parser.add_argument("--test-years", type=int, default=1)
    parser.add_argument(
        "--train-bars",
        type=int,
        default=None,
        help="Training window length in bars/rows. For daily data, this means trading days.",
    )
    parser.add_argument(
        "--test-bars",
        type=int,
        default=None,
        help="Test window length in bars/rows. For daily data, this means trading days.",
    )
    parser.add_argument(
        "--step-bars", type=int, default=None, help="Step size between windows in bars/rows. Defaults to test-bars."
    )
    parser.add_argument(
        "--train-period-days",
        type=int,
        default=None,
        help="Compatibility alias for --train-bars in daily walk-forward research.",
    )
    parser.add_argument(
        "--test-period-days",
        type=int,
        default=None,
        help="Compatibility alias for --test-bars in daily walk-forward research.",
    )
    parser.add_argument(
        "--step-days",
        type=int,
        default=None,
        help="Compatibility alias for --step-bars in daily walk-forward research.",
    )
    parser.add_argument("--min-train-rows", type=int, default=252)
    parser.add_argument("--min-test-rows", type=int, default=126)
    parser.add_argument(
        "--select-by",
        type=str,
        default="Sharpe Ratio",
        choices=["Sharpe Ratio", "Return [%]"],
        help="Metric used to choose best params on the train window",
    )
    parser.add_argument("--cash", type=float, default=10_000)
    parser.add_argument("--commission", type=float, default=0.001)
    parser.add_argument(
        "--cost-bps",
        type=float,
        default=None,
        help="Optional transaction cost in basis points per unit of turnover. Overrides --commission when provided.",
    )
    parser.add_argument(
        "--output", type=str, default="artifacts/experiments/walkforward_results.csv", help="CSV output path"
    )
    parser.add_argument(
        "--engine", type=str, default="legacy", choices=["legacy", "vectorized"], help="Backtest engine to use"
    )


def _add_validate_signal_arguments(parser: argparse.ArgumentParser) -> None:
    add_shared_symbol_args(parser)
    parser.add_argument(
        "--strategy",
        type=str,
        default="sma_cross",
        choices=sorted(STRATEGY_REGISTRY.keys()),
        help="Strategy to validate",
    )
    add_execution_arguments(parser)
    parser.add_argument("--fast", type=int, default=20, help="Fast SMA window used for the baseline in-sample run")
    parser.add_argument("--slow", type=int, default=100, help="Slow SMA window used for the baseline in-sample run")
    parser.add_argument("--lookback", type=int, default=20, help="Lookback used for the baseline in-sample run")
    parser.add_argument(
        "--fast-values",
        type=int,
        nargs="+",
        default=None,
        help="Optional fast windows used in the validation sweep and walk-forward selection",
    )
    parser.add_argument(
        "--slow-values",
        type=int,
        nargs="+",
        default=None,
        help="Optional slow windows used in the validation sweep and walk-forward selection",
    )
    parser.add_argument(
        "--lookback-values",
        type=int,
        nargs="+",
        default=None,
        help="Optional lookbacks used in the validation sweep and walk-forward selection",
    )
    parser.add_argument(
        "--select-by",
        type=str,
        default="Sharpe Ratio",
        choices=["Sharpe Ratio", "Return [%]"],
        help="Metric used to choose walk-forward parameters on each train window",
    )
    parser.add_argument("--train-years", type=int, default=5, help="Training window size in years")
    parser.add_argument("--test-years", type=int, default=1, help="Test window size in years")
    parser.add_argument(
        "--min-train-rows", type=int, default=252, help="Minimum train rows required for a walk-forward window"
    )
    parser.add_argument(
        "--min-test-rows", type=int, default=126, help="Minimum test rows required for a walk-forward window"
    )
    parser.add_argument("--cash", type=float, default=10_000, help="Starting equity used by the vectorized simulation")
    parser.add_argument(
        "--commission", type=float, default=0.001, help="Linear turnover cost used by the vectorized simulation"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="artifacts/validate_signal",
        help="Directory where validation artifacts will be written",
    )


def _add_alpha_research_arguments(parser: argparse.ArgumentParser) -> None:
    add_experiment_tracker_argument(parser)
    add_database_tracking_arguments(parser, include_write_toggles=True)
    parser.add_argument(
        "--config", type=str, default=None, help="Optional YAML or JSON alpha research workflow config file."
    )
    parser.add_argument("--symbols", nargs="+", default=None, help="Symbols to include in the alpha research run.")
    parser.add_argument(
        "--universe", type=str, default=None, help="Named universe to evaluate instead of passing --symbols."
    )
    parser.add_argument(
        "--feature-dir",
        type=str,
        default="data/features",
        help="Directory containing per-symbol feature parquet files.",
    )
    parser.add_argument(
        "--signal-family",
        type=str,
        default="momentum",
        choices=list(SUPPORTED_SIGNAL_FAMILIES),
        help="Signal family to evaluate.",
    )
    parser.add_argument(
        "--signal-families",
        nargs="+",
        default=None,
        choices=list(SUPPORTED_SIGNAL_FAMILIES),
        help="Optional list of signal families to evaluate sequentially within one run.",
    )
    parser.add_argument(
        "--candidate-grid-preset",
        type=str,
        default="standard",
        choices=["standard", "broad_v1"],
        help="Structured candidate-variant preset applied within the selected signal family.",
    )
    parser.add_argument(
        "--signal-composition-preset",
        type=str,
        default="standard",
        choices=["standard", "composite_v1", "research_rich_v1"],
        help="Bounded multi-factor composition preset used to upgrade raw features into compact auditable composites.",
    )
    parser.add_argument(
        "--max-variants-per-family",
        type=int,
        default=None,
        help="Optional cap on the number of generated variants retained from the selected candidate-grid preset.",
    )
    parser.add_argument("--lookbacks", type=int, nargs="+", default=[5, 10, 20, 60], help="Lookback windows to test.")
    parser.add_argument("--horizons", type=int, nargs="+", default=[1, 5, 20], help="Forward return horizons to test.")
    parser.add_argument("--min-rows", type=int, default=126, help="Minimum number of usable rows required per symbol.")
    parser.add_argument(
        "--equity-context-enabled",
        action="store_true",
        help="Enable a small equity-only context feature expansion derived from the current universe price history.",
    )
    parser.add_argument(
        "--equity-context-include-volume",
        action="store_true",
        help="Include a simple volume-ratio context when the feature inputs contain volume.",
    )
    parser.add_argument(
        "--fundamentals-enabled",
        action="store_true",
        help="Merge daily point-in-time fundamental features into alpha research when available.",
    )
    parser.add_argument(
        "--fundamentals-daily-features-path",
        type=str,
        default=None,
        help="Optional parquet path for the daily aligned fundamental feature panel.",
    )
    parser.add_argument(
        "--enable-context-confirmations",
        action="store_true",
        default=None,
        help="Add benchmark, breadth, and regime-aware confirmation terms to composite scores.",
    )
    parser.add_argument(
        "--enable-relative-features",
        action="store_true",
        default=None,
        help="Emphasize relative-strength, cross-sectional rank, and benchmark-relative inputs in composite scores.",
    )
    parser.add_argument(
        "--enable-flow-confirmations",
        action="store_true",
        default=None,
        help="Add bounded volume and dollar-flow confirmation terms where current inputs support them.",
    )
    parser.add_argument(
        "--enable-ensemble",
        action="store_true",
        help="Build an optional ensemble score from eligible promoted members.",
    )
    parser.add_argument(
        "--ensemble-mode",
        type=str,
        default="disabled",
        choices=["disabled", "candidate_weighted", "family_weighted"],
        help="How to combine ensemble members.",
    )
    parser.add_argument(
        "--ensemble-weight-method",
        type=str,
        default="equal",
        choices=["equal", "performance_weighted", "rank_weighted"],
        help="How to weight eligible ensemble members.",
    )
    parser.add_argument(
        "--ensemble-normalize-scores",
        type=str,
        default="rank_pct",
        choices=["raw", "zscore", "rank_pct"],
        help="How to normalize per-member scores before combination.",
    )
    parser.add_argument(
        "--ensemble-max-members", type=int, default=5, help="Maximum number of members included in the ensemble."
    )
    parser.add_argument(
        "--ensemble-max-members-per-family",
        type=int,
        default=None,
        help="Optional family cap applied during ensemble member selection.",
    )
    parser.add_argument(
        "--ensemble-minimum-member-observations",
        type=int,
        default=0,
        help="Minimum observations required for an ensemble member.",
    )
    parser.add_argument(
        "--ensemble-minimum-member-metric",
        type=float,
        default=None,
        help="Optional minimum metric threshold for ensemble inclusion.",
    )
    parser.add_argument(
        "--top-quantile", type=float, default=0.2, help="Top quantile threshold used for spread metrics."
    )
    parser.add_argument(
        "--bottom-quantile", type=float, default=0.2, help="Bottom quantile threshold used for spread metrics."
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="artifacts/alpha_research",
        help="Directory where alpha research artifacts will be written.",
    )
    parser.add_argument("--train-size", type=int, default=756, help="Number of rows in each training window.")
    parser.add_argument("--test-size", type=int, default=63, help="Number of rows in each test window.")
    parser.add_argument(
        "--step-size", type=int, default=None, help="Number of rows to advance after each fold. Defaults to test-size."
    )
    parser.add_argument("--min-train-size", type=int, default=None, help="Optional minimum train window size.")
    parser.add_argument(
        "--portfolio-top-n", type=int, default=10, help="Top-N size used for the composite long-only portfolio."
    )
    parser.add_argument(
        "--portfolio-long-quantile",
        type=float,
        default=0.2,
        help="Top quantile used for the composite long-short portfolio.",
    )
    parser.add_argument(
        "--portfolio-short-quantile",
        type=float,
        default=0.2,
        help="Bottom quantile used for the composite long-short portfolio.",
    )
    parser.add_argument(
        "--commission",
        type=float,
        default=0.0,
        help="Turnover-based transaction cost used in the composite portfolio backtest.",
    )
    parser.add_argument(
        "--min-price", type=float, default=None, help="Optional minimum price required for a name to remain investable."
    )
    parser.add_argument(
        "--min-volume",
        type=float,
        default=None,
        help="Optional minimum raw share volume required for a name to remain investable.",
    )
    parser.add_argument(
        "--min-avg-dollar-volume",
        type=float,
        default=None,
        help="Optional minimum rolling average dollar volume required for a name to remain investable.",
    )
    parser.add_argument(
        "--max-adv-participation",
        type=float,
        default=0.05,
        help="Maximum participation rate used in capacity estimates.",
    )
    parser.add_argument(
        "--max-position-pct-of-adv",
        type=float,
        default=0.1,
        help="Maximum single-name position size as a fraction of average dollar volume.",
    )
    parser.add_argument(
        "--max-notional-per-name",
        type=float,
        default=None,
        help="Optional notional cap per name used in capacity estimates.",
    )
    parser.add_argument(
        "--slippage-bps-per-turnover",
        type=float,
        default=0.0,
        help="Linear slippage in basis points per unit of turnover.",
    )
    parser.add_argument(
        "--slippage-bps-per-adv",
        type=float,
        default=10.0,
        help="Additional slippage in basis points that scales with fraction of ADV traded.",
    )
    parser.add_argument(
        "--dynamic-recent-quality-window",
        type=int,
        default=20,
        help="Lookback window in out-of-sample dates used for dynamic signal weighting.",
    )
    parser.add_argument(
        "--dynamic-min-history",
        type=int,
        default=5,
        help="Minimum out-of-sample dates before lifecycle rules move beyond promote state.",
    )
    parser.add_argument(
        "--dynamic-downweight-mean-rank-ic",
        type=float,
        default=0.01,
        help="Recent mean rank IC threshold below which active signals are downweighted.",
    )
    parser.add_argument(
        "--dynamic-deactivate-mean-rank-ic",
        type=float,
        default=-0.02,
        help="Recent mean rank IC threshold below which signals are deactivated.",
    )
    parser.add_argument(
        "--regime-aware-enabled",
        action="store_true",
        help="Enable regime-aware signal weighting on top of the dynamic lifecycle weights.",
    )
    parser.add_argument(
        "--regime-min-history",
        type=int,
        default=5,
        help="Minimum same-regime out-of-sample observations before regime-aware weighting reacts.",
    )
    parser.add_argument(
        "--regime-underweight-mean-rank-ic",
        type=float,
        default=0.01,
        help="Same-regime mean rank IC threshold below which signals are underweighted.",
    )
    parser.add_argument(
        "--regime-exclude-mean-rank-ic",
        type=float,
        default=-0.01,
        help="Same-regime mean rank IC threshold below which signals are excluded.",
    )
    parser.add_argument(
        "--require-runtime-computability-for-approval",
        action="store_true",
        help="Block approval for candidates that cannot produce current computable scores.",
    )
    parser.add_argument(
        "--min-runtime-computable-symbols-for-approval",
        type=int,
        default=5,
        help="Minimum symbols with current computable scores required to remain approval-eligible.",
    )
    parser.add_argument(
        "--allow-research-only-noncomputable-candidates",
        action="store_true",
        help="Keep runtime-incomputable candidates for diagnostics while still blocking approval in strict mode.",
    )
    parser.add_argument(
        "--runtime-computability-penalty-on-ranking",
        type=float,
        default=0.02,
        help="Penalty applied to ranking metrics when runtime computability check mode is penalize.",
    )
    parser.add_argument(
        "--diagnostics-alphalens-enabled",
        action="store_true",
        help="Generate optional Alphalens factor diagnostics from the current research run.",
    )
    parser.add_argument(
        "--diagnostics-alphalens-groupby-field",
        type=str,
        default=None,
        choices=["sector", "industry_group", "industry", "country", "exchange", "category"],
        help="Optional classification column used for group-aware Alphalens diagnostics.",
    )
    parser.add_argument(
        "--diagnostics-classification-path",
        type=str,
        default=None,
        help="Optional security_master.csv path used for group-aware diagnostics.",
    )
    parser.add_argument(
        "--diagnostics-output-dir",
        type=str,
        default=None,
        help="Optional output directory for external research diagnostics.",
    )
    parser.add_argument(
        "--reporting-quantstats-enabled",
        action="store_true",
        help="Generate an optional QuantStats report from research portfolio returns.",
    )
    parser.add_argument(
        "--reporting-quantstats-output-dir",
        type=str,
        default=None,
        help="Optional output directory for QuantStats reporting artifacts.",
    )
    parser.add_argument(
        "--runtime-computability-check-mode",
        type=str,
        default="strict",
        choices=["strict", "penalize", "diagnostic_only"],
        help="How runtime computability affects candidate approval and ranking.",
    )
    parser.add_argument(
        "--require-composite-runtime-computability-for-approval",
        action="store_true",
        help="Block approval-ready composite states that cannot form current composite scores.",
    )
    parser.add_argument(
        "--min-composite-runtime-computable-symbols-for-approval",
        type=int,
        default=5,
        help="Minimum symbols with current composite scores required for composite approval readiness.",
    )
    parser.add_argument(
        "--allow-research-only-noncomputable-composites",
        action="store_true",
        help="Keep non-computable composites as research-only diagnostics while blocking approval in strict mode.",
    )
    parser.add_argument(
        "--composite-runtime-computability-check-mode",
        type=str,
        default="strict",
        choices=["strict", "penalize", "diagnostic_only"],
        help="How composite runtime computability affects approval readiness.",
    )
    parser.add_argument(
        "--composite-runtime-computability-penalty-on-ranking",
        type=float,
        default=0.02,
        help="Penalty applied to composite approval ranking when composite runtime computability check mode is penalize.",
    )
    parser.add_argument(
        "--fast-refresh-mode",
        action="store_true",
        help="Refresh approval and registry artifacts from existing research outputs instead of running a full recompute.",
    )
    parser.add_argument(
        "--skip-heavy-diagnostics",
        action="store_true",
        default=None,
        help="Skip optional heavy artifact rewrites during fast refresh.",
    )
    parser.add_argument(
        "--reuse-existing-fold-results",
        action="store_true",
        default=None,
        help="Reuse existing leaderboard and fold-derived artifacts during fast refresh.",
    )
    parser.add_argument(
        "--restrict-to-existing-candidates",
        action="store_true",
        default=None,
        help="Limit fast refresh to candidates already present in the existing leaderboard artifacts.",
    )
    parser.add_argument(
        "--max-families-for-refresh",
        type=int,
        default=None,
        help="Optional cap on signal families refreshed during fast artifact recompute.",
    )
    parser.add_argument(
        "--max-candidates-for-refresh",
        type=int,
        default=None,
        help="Optional cap on leaderboard candidates refreshed during fast artifact recompute.",
    )


def _add_alpha_loop_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--symbols", nargs="+", default=None, help="Symbols to include in the automated alpha research loop."
    )
    parser.add_argument(
        "--universe", type=str, default=None, help="Named universe to evaluate instead of passing --symbols."
    )
    parser.add_argument(
        "--feature-dir",
        type=str,
        default="data/features",
        help="Directory containing per-symbol feature parquet files.",
    )
    parser.add_argument(
        "--signal-families",
        type=str,
        nargs="+",
        default=["momentum", "mean_reversion", "volatility", "feature_combo"],
        help="Signal families to generate and test.",
    )
    parser.add_argument(
        "--lookbacks", type=int, nargs="+", default=[5, 10, 20, 60], help="Lookback windows to test for each family."
    )
    parser.add_argument(
        "--horizons", type=int, nargs="+", default=[1, 5, 20], help="Forward return horizons to test for each family."
    )
    parser.add_argument(
        "--vol-windows",
        type=int,
        nargs="+",
        default=[10, 20, 60],
        help="Volatility windows to test for volatility-based signals.",
    )
    parser.add_argument(
        "--combo-thresholds",
        type=float,
        nargs="+",
        default=[0.5, 1.0],
        help="Threshold multipliers used for simple feature-combination signals.",
    )
    parser.add_argument("--min-rows", type=int, default=126, help="Minimum number of usable rows required per symbol.")
    parser.add_argument(
        "--top-quantile", type=float, default=0.2, help="Top quantile threshold used for spread metrics."
    )
    parser.add_argument(
        "--bottom-quantile", type=float, default=0.2, help="Bottom quantile threshold used for spread metrics."
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="artifacts/alpha_research_loop",
        help="Directory where automated research artifacts will be written.",
    )
    parser.add_argument("--train-size", type=int, default=756, help="Number of rows in each training window.")
    parser.add_argument("--test-size", type=int, default=63, help="Number of rows in each test window.")
    parser.add_argument(
        "--step-size", type=int, default=None, help="Number of rows to advance after each fold. Defaults to test-size."
    )
    parser.add_argument("--min-train-size", type=int, default=None, help="Optional minimum train window size.")
    parser.add_argument(
        "--schedule-frequency",
        type=str,
        default="manual",
        choices=["manual", "daily", "weekly"],
        help="Scheduling hook used to decide when the loop is due to run.",
    )
    parser.add_argument(
        "--force", action="store_true", help="Run immediately even if the schedule metadata says the loop is not due."
    )
    parser.add_argument(
        "--max-iterations",
        type=int,
        default=1,
        help="Maximum number of loop iterations to run in one invocation. Use 0 for no work, default is 1 for safety.",
    )


def _add_refresh_arguments(parser: argparse.ArgumentParser) -> None:
    _add_alpha_loop_arguments(parser)
    parser.set_defaults(output_dir="artifacts/research_refresh", schedule_frequency="daily")
    parser.add_argument(
        "--stale-after-days",
        type=int,
        default=None,
        help="Optional age threshold for re-evaluating stale signal candidates.",
    )
    parser.add_argument(
        "--tracker-dir",
        type=str,
        default="artifacts/experiment_tracking",
        help="Optional experiment tracker directory used to enrich approved snapshots.",
    )


def _add_multi_universe_arguments(parser: argparse.ArgumentParser) -> None:
    add_experiment_tracker_argument(parser)
    parser.add_argument("--universes", nargs="+", required=True, help="Named universes to evaluate in one job.")
    parser.add_argument(
        "--feature-dir",
        type=str,
        default="data/features",
        help="Directory containing per-symbol feature parquet files.",
    )
    parser.add_argument(
        "--signal-family",
        type=str,
        default="momentum",
        choices=list(SUPPORTED_SIGNAL_FAMILIES),
        help="Signal family to evaluate in each universe.",
    )
    parser.add_argument("--lookbacks", type=int, nargs="+", default=[5, 10, 20, 60], help="Lookback windows to test.")
    parser.add_argument("--horizons", type=int, nargs="+", default=[1, 5, 20], help="Forward return horizons to test.")
    parser.add_argument("--min-rows", type=int, default=126, help="Minimum number of usable rows required per symbol.")
    parser.add_argument(
        "--top-quantile", type=float, default=0.2, help="Top quantile threshold used for spread metrics."
    )
    parser.add_argument(
        "--bottom-quantile", type=float, default=0.2, help="Bottom quantile threshold used for spread metrics."
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="artifacts/multi_universe_alpha_research",
        help="Directory where per-universe and comparison artifacts will be written.",
    )
    parser.add_argument("--train-size", type=int, default=756, help="Number of rows in each training window.")
    parser.add_argument("--test-size", type=int, default=63, help="Number of rows in each test window.")
    parser.add_argument(
        "--step-size", type=int, default=None, help="Number of rows to advance after each fold. Defaults to test-size."
    )
    parser.add_argument("--min-train-size", type=int, default=None, help="Optional minimum train window size.")
    parser.add_argument(
        "--portfolio-top-n", type=int, default=10, help="Top-N size used for the composite long-only portfolio."
    )
    parser.add_argument(
        "--portfolio-long-quantile",
        type=float,
        default=0.2,
        help="Top quantile used for the composite long-short portfolio.",
    )
    parser.add_argument(
        "--portfolio-short-quantile",
        type=float,
        default=0.2,
        help="Bottom quantile used for the composite long-short portfolio.",
    )
    parser.add_argument(
        "--commission",
        type=float,
        default=0.0,
        help="Turnover-based transaction cost used in the composite portfolio backtest.",
    )
    parser.add_argument(
        "--min-price", type=float, default=None, help="Optional minimum price required for a name to remain investable."
    )
    parser.add_argument(
        "--min-volume",
        type=float,
        default=None,
        help="Optional minimum raw share volume required for a name to remain investable.",
    )
    parser.add_argument(
        "--min-avg-dollar-volume",
        type=float,
        default=None,
        help="Optional minimum rolling average dollar volume required for a name to remain investable.",
    )
    parser.add_argument(
        "--max-adv-participation",
        type=float,
        default=0.05,
        help="Maximum participation rate used in capacity estimates.",
    )
    parser.add_argument(
        "--max-position-pct-of-adv",
        type=float,
        default=0.1,
        help="Maximum single-name position size as a fraction of average dollar volume.",
    )
    parser.add_argument(
        "--max-notional-per-name",
        type=float,
        default=None,
        help="Optional notional cap per name used in capacity estimates.",
    )
    parser.add_argument(
        "--slippage-bps-per-turnover",
        type=float,
        default=0.0,
        help="Linear slippage in basis points per unit of turnover.",
    )
    parser.add_argument(
        "--slippage-bps-per-adv",
        type=float,
        default=10.0,
        help="Additional slippage in basis points that scales with fraction of ADV traded.",
    )
    parser.add_argument(
        "--dynamic-recent-quality-window",
        type=int,
        default=20,
        help="Lookback window in out-of-sample dates used for dynamic signal weighting.",
    )
    parser.add_argument(
        "--dynamic-min-history",
        type=int,
        default=5,
        help="Minimum out-of-sample dates before lifecycle rules move beyond promote state.",
    )
    parser.add_argument(
        "--dynamic-downweight-mean-rank-ic",
        type=float,
        default=0.01,
        help="Recent mean rank IC threshold below which active signals are downweighted.",
    )
    parser.add_argument(
        "--dynamic-deactivate-mean-rank-ic",
        type=float,
        default=-0.02,
        help="Recent mean rank IC threshold below which signals are deactivated.",
    )
    parser.add_argument(
        "--regime-aware-enabled",
        action="store_true",
        help="Enable regime-aware signal weighting on top of the dynamic lifecycle weights.",
    )
    parser.add_argument(
        "--regime-min-history",
        type=int,
        default=5,
        help="Minimum same-regime out-of-sample observations before regime-aware weighting reacts.",
    )
    parser.add_argument(
        "--regime-underweight-mean-rank-ic",
        type=float,
        default=0.01,
        help="Same-regime mean rank IC threshold below which signals are underweighted.",
    )
    parser.add_argument(
        "--regime-exclude-mean-rank-ic",
        type=float,
        default=-0.01,
        help="Same-regime mean rank IC threshold below which signals are excluded.",
    )


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
    parser.add_argument(
        "--execution-config", type=str, default=None, help="Optional execution realism JSON/YAML config."
    )
    parser.add_argument("--order-type", default="market")
    parser.add_argument("--time-in-force", default="day")
    parser.add_argument("--broker", default="mock", choices=["mock", "alpaca"])
    parser.add_argument("--mock-equity", type=float, default=100_000.0)
    parser.add_argument("--mock-cash", type=float, default=100_000.0)
    parser.add_argument("--mock-positions-path", default=None)
    parser.add_argument(
        "--broker-config", type=str, default=None, help="Optional broker JSON/YAML config for submit/health flows."
    )


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


def _cmd_research_compare_xsec_construction(args) -> None:
    cmd_compare_xsec_construction(args)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="trading-cli",
        description=(
            "Trading platform CLI. Canonical production workflow: data ingest -> data features "
            "-> research run -> walkforward -> decision memo -> deploy preset -> paper scheduled -> live dry-run."
        ),
        epilog=(
            "Stable groups: data, research, portfolio, paper, live, dashboard, ops.\n"
            "Validated primary example: xsec_nasdaq100_momentum_v1_research -> "
            "xsec_nasdaq100_momentum_v1_deploy.\n"
            "Advanced and experimental workflows remain available, but the supported path is config-first and preset-driven."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command_family", required=True)

    data_parser = subparsers.add_parser("data", help="Data ingest, feature generation, and universe registry commands")
    data_subparsers = data_parser.add_subparsers(dest="data_command", required=True)
    data_ingest = data_subparsers.add_parser("ingest", help="Download raw OHLCV data")
    add_shared_symbol_args(data_ingest)
    data_ingest.add_argument(
        "--start", type=str, default="2010-01-01", help="Start date in YYYY-MM-DD format (default: 2010-01-01)"
    )
    data_ingest.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop on the first ingest failure instead of continuing through the batch.",
    )
    data_ingest.add_argument(
        "--failure-report", type=str, default=None, help="Optional CSV path for per-symbol ingest failures."
    )
    data_ingest.set_defaults(func=cmd_ingest)
    data_features = data_subparsers.add_parser(
        "features",
        help="Build feature datasets. Canonical path: `trading-cli data features --symbols ...` or `--universe ...`.",
    )
    add_shared_symbol_args(data_features)
    add_feature_arguments(data_features)
    data_features.set_defaults(func=cmd_features)
    data_refresh_inputs = data_subparsers.add_parser(
        "refresh-research-inputs",
        help="Canonical step 1: refresh research-ready features plus metadata sidecars in one deterministic step.",
    )
    add_shared_symbol_args(data_refresh_inputs)
    add_feature_arguments(data_refresh_inputs)
    data_refresh_inputs.add_argument(
        "--config",
        type=str,
        default=None,
        help="Optional JSON/YAML config describing a versioned research-input refresh spec.",
    )
    data_refresh_inputs.add_argument(
        "--feature-dir",
        type=str,
        default="data/features",
        help="Directory where research-ready feature parquet files are written.",
    )
    data_refresh_inputs.add_argument(
        "--metadata-dir",
        type=str,
        default="data/metadata",
        help="Directory where research metadata sidecars are written.",
    )
    data_refresh_inputs.add_argument(
        "--normalized-dir",
        type=str,
        default="data/normalized",
        help="Directory containing normalized OHLCV parquet inputs.",
    )
    data_refresh_inputs.add_argument(
        "--sub-universe-id",
        type=str,
        default=None,
        help="Optional sub-universe identifier to persist in refreshed metadata sidecars.",
    )
    data_refresh_inputs.add_argument(
        "--reference-data-root",
        type=str,
        default=None,
        help="Optional root directory for versioned reference-data artifacts.",
    )
    data_refresh_inputs.add_argument(
        "--universe-membership-path",
        type=str,
        default=None,
        help="Optional point-in-time universe membership history dataset.",
    )
    data_refresh_inputs.add_argument(
        "--taxonomy-snapshot-path",
        type=str,
        default=None,
        help="Optional taxonomy snapshot dataset used for enrichment.",
    )
    data_refresh_inputs.add_argument(
        "--benchmark-mapping-path",
        type=str,
        default=None,
        help="Optional benchmark mapping snapshot dataset used for enrichment.",
    )
    data_refresh_inputs.add_argument(
        "--market-regime-path",
        type=str,
        default=None,
        help="Optional market-regime artifact used to persist regime context in enrichment rows.",
    )
    data_refresh_inputs.add_argument(
        "--group-map-path",
        type=str,
        default=None,
        help="Optional legacy group-map CSV used when taxonomy snapshots are absent.",
    )
    data_refresh_inputs.add_argument(
        "--benchmark", type=str, default=None, help="Optional benchmark id stored with enriched metadata context."
    )
    data_refresh_inputs.add_argument(
        "--failure-policy",
        type=str,
        default="partial_success",
        choices=["partial_success", "fail"],
        help="Whether symbol-level feature failures produce a partial-success result or fail the refresh summary.",
    )
    data_refresh_inputs.add_argument(
        "--fundamentals-enabled",
        action="store_true",
        help="Also refresh point-in-time-safe fundamentals artifacts and daily aligned fundamental features.",
    )
    data_refresh_inputs.add_argument(
        "--fundamentals-artifact-root",
        type=str,
        default=None,
        help="Directory where canonical fundamentals artifacts are written.",
    )
    data_refresh_inputs.add_argument(
        "--fundamentals-providers",
        nargs="+",
        default=None,
        choices=["sec", "vendor"],
        help="Ordered list of fundamentals providers to use.",
    )
    data_refresh_inputs.add_argument(
        "--fundamentals-sec-companyfacts-root", type=str, default=None, help="Local SEC companyfacts JSON directory."
    )
    data_refresh_inputs.add_argument(
        "--fundamentals-sec-submissions-root", type=str, default=None, help="Local SEC submissions JSON directory."
    )
    data_refresh_inputs.add_argument(
        "--fundamentals-vendor-file-path",
        type=str,
        default=None,
        help="Optional normalized vendor fundamentals JSON/CSV/parquet file.",
    )
    data_refresh_inputs.add_argument(
        "--fundamentals-vendor-api-key",
        type=str,
        default=None,
        help="Optional FMP vendor API key. Falls back to FMP_API_KEY when omitted.",
    )
    data_refresh_inputs.add_argument(
        "--fundamentals-vendor-cache-enabled",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable or disable raw FMP response caching.",
    )
    data_refresh_inputs.add_argument(
        "--fundamentals-vendor-cache-root",
        type=str,
        default=None,
        help="Optional raw FMP cache root. Defaults under the fundamentals artifact root.",
    )
    data_refresh_inputs.add_argument(
        "--fundamentals-vendor-cache-ttl-hours",
        type=float,
        default=None,
        help="Freshness TTL for cached FMP responses.",
    )
    data_refresh_inputs.add_argument(
        "--fundamentals-vendor-force-refresh",
        action="store_true",
        help="Bypass fresh cache entries and refetch vendor data.",
    )
    data_refresh_inputs.add_argument(
        "--fundamentals-vendor-request-delay-seconds",
        type=float,
        default=None,
        help="Throttle delay between FMP requests.",
    )
    data_refresh_inputs.add_argument(
        "--fundamentals-vendor-max-retries",
        type=int,
        default=None,
        help="Maximum FMP retry attempts for 429/transient failures.",
    )
    data_refresh_inputs.add_argument(
        "--fundamentals-vendor-max-symbols-per-run",
        type=int,
        default=None,
        help="Optional cap on symbols fetched from FMP in one run.",
    )
    data_refresh_inputs.add_argument(
        "--fundamentals-vendor-max-requests-per-run",
        type=int,
        default=None,
        help="Optional cap on raw FMP requests in one run.",
    )
    data_refresh_inputs.set_defaults(func=cmd_refresh_research_inputs)
    data_crypto = data_subparsers.add_parser("crypto", help="Crypto market-data ingestion commands")
    data_crypto_subparsers = data_crypto.add_subparsers(dest="crypto_command", required=True)
    data_crypto_binance = data_crypto_subparsers.add_parser("binance", help="Binance public crypto data commands")
    data_crypto_binance_subparsers = data_crypto_binance.add_subparsers(dest="crypto_provider_command", required=True)
    data_crypto_binance_historical = data_crypto_binance_subparsers.add_parser(
        "historical-ingest",
        help="Fetch Binance public market-data history and optional normalized artifacts.",
    )
    data_crypto_binance_historical.add_argument(
        "--config",
        type=str,
        default="configs/binance.yaml",
        help="Path to Binance crypto YAML config (default: configs/binance.yaml).",
    )
    data_crypto_binance_historical.add_argument("--symbols", nargs="*", default=None, help="Optional symbol override.")
    data_crypto_binance_historical.add_argument(
        "--intervals", nargs="*", default=None, help="Optional kline interval override."
    )
    data_crypto_binance_historical.add_argument("--start", type=str, default=None, help="Override historical start.")
    data_crypto_binance_historical.add_argument("--end", type=str, default=None, help="Override historical end.")
    data_crypto_binance_historical.add_argument(
        "--kline-limit", type=int, default=None, help="Maximum klines returned per REST request."
    )
    data_crypto_binance_historical.add_argument(
        "--agg-trade-limit", type=int, default=None, help="Maximum aggregate trades returned per REST request."
    )
    data_crypto_binance_historical.add_argument(
        "--request-sleep-sec", type=float, default=None, help="Throttle delay between Binance REST requests."
    )
    data_crypto_binance_historical.add_argument(
        "--max-retries", type=int, default=None, help="Maximum retry attempts for 429/transient failures."
    )
    data_crypto_binance_historical.add_argument(
        "--backoff-base-sec", type=float, default=None, help="Base retry backoff in seconds."
    )
    data_crypto_binance_historical.add_argument(
        "--backoff-max-sec", type=float, default=None, help="Maximum retry backoff in seconds."
    )
    data_crypto_binance_historical.add_argument(
        "--capture-book-ticker",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable or disable current book ticker snapshots at the end of each symbol ingest.",
    )
    data_crypto_binance_historical.add_argument(
        "--skip-normalize",
        action="store_true",
        default=False,
        help="Fetch raw artifacts only and skip the post-ingest normalization pass.",
    )
    data_crypto_binance_historical.add_argument(
        "--raw-root", type=str, default=None, help="Optional raw output root override."
    )
    data_crypto_binance_historical.add_argument(
        "--normalized-root", type=str, default=None, help="Optional normalized output root override."
    )
    data_crypto_binance_historical.add_argument(
        "--checkpoint-path", type=str, default=None, help="Optional checkpoint JSON path override."
    )
    data_crypto_binance_historical.add_argument(
        "--summary-path", type=str, default=None, help="Optional ingest summary JSON path override."
    )
    data_crypto_binance_historical.add_argument(
        "--exchange-info-path", type=str, default=None, help="Optional exchangeInfo JSON path override."
    )
    data_crypto_binance_historical.set_defaults(func=cmd_binance_crypto_historical_ingest)
    data_crypto_binance_normalize = data_crypto_binance_subparsers.add_parser(
        "normalize",
        help="Normalize raw Binance crypto artifacts into research-ready parquet datasets.",
    )
    data_crypto_binance_normalize.add_argument(
        "--config",
        type=str,
        default="configs/binance.yaml",
        help="Path to Binance crypto YAML config (default: configs/binance.yaml).",
    )
    data_crypto_binance_normalize.add_argument("--symbols", nargs="*", default=None, help="Optional symbol filter.")
    data_crypto_binance_normalize.add_argument(
        "--intervals", nargs="*", default=None, help="Optional interval filter for kline normalization."
    )
    data_crypto_binance_normalize.add_argument(
        "--raw-root", type=str, default=None, help="Optional raw input root override."
    )
    data_crypto_binance_normalize.add_argument(
        "--normalized-root", type=str, default=None, help="Optional normalized output root override."
    )
    data_crypto_binance_normalize.add_argument(
        "--summary-path", type=str, default=None, help="Optional normalization summary JSON path override."
    )
    data_crypto_binance_normalize.set_defaults(func=cmd_binance_crypto_normalize)
    data_crypto_binance_websocket = data_crypto_binance_subparsers.add_parser(
        "websocket-ingest",
        help="Run bounded Binance public websocket ingestion and incremental normalization.",
    )
    data_crypto_binance_websocket.add_argument(
        "--config",
        type=str,
        default="configs/binance.yaml",
        help="Path to Binance crypto YAML config (default: configs/binance.yaml).",
    )
    data_crypto_binance_websocket.add_argument("--symbols", nargs="*", default=None, help="Optional symbol override.")
    data_crypto_binance_websocket.add_argument(
        "--intervals", nargs="*", default=None, help="Optional kline interval override."
    )
    data_crypto_binance_websocket.add_argument(
        "--stream-families",
        nargs="*",
        default=None,
        choices=["kline", "agg_trade", "book_ticker"],
        help="Subset of websocket stream families to ingest.",
    )
    data_crypto_binance_websocket.add_argument(
        "--combined-stream",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable or disable combined-stream websocket mode.",
    )
    data_crypto_binance_websocket.add_argument(
        "--max-runtime-seconds",
        type=int,
        default=None,
        help="Optional runtime cap for bounded local websocket test runs.",
    )
    data_crypto_binance_websocket.add_argument(
        "--max-messages",
        type=int,
        default=None,
        help="Optional message cap for bounded local websocket test runs.",
    )
    data_crypto_binance_websocket.add_argument(
        "--reconnect-backoff-base-sec", type=float, default=None, help="Base reconnect backoff in seconds."
    )
    data_crypto_binance_websocket.add_argument(
        "--reconnect-backoff-max-sec", type=float, default=None, help="Maximum reconnect backoff in seconds."
    )
    data_crypto_binance_websocket.add_argument(
        "--max-reconnect-attempts", type=int, default=None, help="Maximum reconnect attempts before exit."
    )
    data_crypto_binance_websocket.add_argument(
        "--receive-timeout-sec", type=float, default=None, help="Receive timeout used to trigger reconnects."
    )
    data_crypto_binance_websocket.add_argument(
        "--raw-incremental-root", type=str, default=None, help="Optional raw websocket JSONL root override."
    )
    data_crypto_binance_websocket.add_argument(
        "--normalized-incremental-root",
        type=str,
        default=None,
        help="Optional normalized incremental parquet root override.",
    )
    data_crypto_binance_websocket.add_argument(
        "--checkpoint-path", type=str, default=None, help="Optional websocket checkpoint JSON path override."
    )
    data_crypto_binance_websocket.add_argument(
        "--summary-path", type=str, default=None, help="Optional websocket summary JSON path override."
    )
    data_crypto_binance_websocket.add_argument(
        "--projection-output-root", type=str, default=None, help="Optional projection output root override."
    )
    data_crypto_binance_websocket.set_defaults(func=cmd_binance_crypto_websocket_ingest)
    data_crypto_binance_project = data_crypto_binance_subparsers.add_parser(
        "project",
        help="Build unified crypto market-data projections from Binance historical and incremental artifacts.",
    )
    data_crypto_binance_project.add_argument(
        "--config",
        type=str,
        default="configs/binance.yaml",
        help="Path to Binance crypto YAML config (default: configs/binance.yaml).",
    )
    data_crypto_binance_project.add_argument("--symbols", nargs="*", default=None, help="Optional symbol filter.")
    data_crypto_binance_project.add_argument(
        "--intervals", nargs="*", default=None, help="Optional interval filter for bar projections."
    )
    data_crypto_binance_project.add_argument(
        "--historical-normalized-root",
        type=str,
        default=None,
        help="Optional historical normalized Binance root override.",
    )
    data_crypto_binance_project.add_argument(
        "--incremental-normalized-root",
        type=str,
        default=None,
        help="Optional incremental normalized Binance root override.",
    )
    data_crypto_binance_project.add_argument(
        "--output-root", type=str, default=None, help="Optional projection output root override."
    )
    data_crypto_binance_project.add_argument(
        "--summary-path", type=str, default=None, help="Optional projection summary JSON path override."
    )
    data_crypto_binance_project.set_defaults(func=cmd_binance_crypto_project)
    data_crypto_binance_features = data_crypto_binance_subparsers.add_parser(
        "features",
        help="Build crypto market features from Binance projected datasets and publish feature-store artifacts.",
    )
    data_crypto_binance_features.add_argument(
        "--config",
        type=str,
        default="configs/binance.yaml",
        help="Path to Binance crypto YAML config (default: configs/binance.yaml).",
    )
    data_crypto_binance_features.add_argument("--symbols", nargs="*", default=None, help="Optional symbol filter.")
    data_crypto_binance_features.add_argument(
        "--intervals", nargs="*", default=None, help="Optional interval filter for bar-based features."
    )
    data_crypto_binance_features.add_argument(
        "--projection-root", type=str, default=None, help="Optional projection dataset root override."
    )
    data_crypto_binance_features.add_argument(
        "--features-root", type=str, default=None, help="Optional feature dataset output root override."
    )
    data_crypto_binance_features.add_argument(
        "--feature-store-root", type=str, default=None, help="Optional feature-store root override."
    )
    data_crypto_binance_features.add_argument(
        "--summary-path", type=str, default=None, help="Optional feature summary JSON path override."
    )
    data_crypto_binance_features.add_argument(
        "--incremental-refresh",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable or disable incremental feature refresh behavior.",
    )
    data_crypto_binance_features.add_argument(
        "--full-rebuild",
        action="store_true",
        default=False,
        help="Rebuild the selected feature slices from scratch instead of incremental tail refresh.",
    )
    data_crypto_binance_features.set_defaults(func=cmd_binance_crypto_features)
    data_crypto_binance_sync = data_crypto_binance_subparsers.add_parser(
        "sync",
        help="Run bounded Binance websocket ingest, projection refresh, and feature refresh as one sync cycle.",
    )
    data_crypto_binance_sync.add_argument(
        "--config",
        type=str,
        default="configs/binance.yaml",
        help="Path to Binance crypto YAML config (default: configs/binance.yaml).",
    )
    data_crypto_binance_sync.add_argument("--symbols", nargs="*", default=None, help="Optional symbol override.")
    data_crypto_binance_sync.add_argument(
        "--intervals", nargs="*", default=None, help="Optional kline interval override."
    )
    data_crypto_binance_sync.add_argument(
        "--stream-families",
        nargs="*",
        default=None,
        choices=["kline", "agg_trade", "book_ticker"],
        help="Subset of websocket stream families to ingest.",
    )
    data_crypto_binance_sync.add_argument(
        "--max-runtime-seconds",
        type=int,
        default=None,
        help="Optional runtime cap for the websocket step inside the sync cycle.",
    )
    data_crypto_binance_sync.add_argument(
        "--max-messages",
        type=int,
        default=None,
        help="Optional message cap for the websocket step inside the sync cycle.",
    )
    data_crypto_binance_sync.add_argument(
        "--skip-projection",
        action="store_true",
        default=False,
        help="Skip projection refresh after websocket ingest.",
    )
    data_crypto_binance_sync.add_argument(
        "--skip-features",
        action="store_true",
        default=False,
        help="Skip feature refresh after projection.",
    )
    data_crypto_binance_sync.add_argument(
        "--full-feature-rebuild",
        action="store_true",
        default=False,
        help="Force a full projected-feature rebuild instead of incremental tail refresh.",
    )
    data_crypto_binance_sync.add_argument(
        "--incremental-refresh",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable or disable incremental feature refresh behavior.",
    )
    data_crypto_binance_sync.add_argument(
        "--raw-incremental-root", type=str, default=None, help="Optional raw websocket JSONL root override."
    )
    data_crypto_binance_sync.add_argument(
        "--normalized-incremental-root",
        type=str,
        default=None,
        help="Optional normalized incremental parquet root override.",
    )
    data_crypto_binance_sync.add_argument(
        "--checkpoint-path", type=str, default=None, help="Optional websocket checkpoint JSON path override."
    )
    data_crypto_binance_sync.add_argument(
        "--websocket-summary-path", type=str, default=None, help="Optional websocket summary JSON path override."
    )
    data_crypto_binance_sync.add_argument(
        "--historical-normalized-root",
        type=str,
        default=None,
        help="Optional historical normalized Binance root override.",
    )
    data_crypto_binance_sync.add_argument(
        "--projection-output-root", type=str, default=None, help="Optional projection output root override."
    )
    data_crypto_binance_sync.add_argument(
        "--projection-summary-path", type=str, default=None, help="Optional projection summary JSON path override."
    )
    data_crypto_binance_sync.add_argument(
        "--features-root", type=str, default=None, help="Optional feature dataset output root override."
    )
    data_crypto_binance_sync.add_argument(
        "--feature-store-root", type=str, default=None, help="Optional feature-store root override."
    )
    data_crypto_binance_sync.add_argument(
        "--feature-summary-path", type=str, default=None, help="Optional feature summary JSON path override."
    )
    data_crypto_binance_sync.add_argument(
        "--status-summary-path", type=str, default=None, help="Optional freshness/status JSON path override."
    )
    data_crypto_binance_sync.add_argument(
        "--sync-manifest-root", type=str, default=None, help="Optional per-run sync manifest directory override."
    )
    data_crypto_binance_sync.add_argument(
        "--latest-sync-manifest-path", type=str, default=None, help="Optional latest sync manifest path override."
    )
    data_crypto_binance_sync.add_argument(
        "--sync-summary-path", type=str, default=None, help="Optional sync summary JSON path override."
    )
    data_crypto_binance_sync.set_defaults(func=cmd_binance_crypto_sync)
    data_crypto_binance_status = data_crypto_binance_subparsers.add_parser(
        "status",
        help="Inspect Binance projected-dataset and feature freshness plus the latest sync manifest reference.",
    )
    data_crypto_binance_status.add_argument(
        "--config",
        type=str,
        default="configs/binance.yaml",
        help="Path to Binance crypto YAML config (default: configs/binance.yaml).",
    )
    data_crypto_binance_status.add_argument("--symbols", nargs="*", default=None, help="Optional symbol filter.")
    data_crypto_binance_status.add_argument(
        "--intervals", nargs="*", default=None, help="Optional interval filter for bar and feature datasets."
    )
    data_crypto_binance_status.add_argument(
        "--projection-root", type=str, default=None, help="Optional projection dataset root override."
    )
    data_crypto_binance_status.add_argument(
        "--features-root", type=str, default=None, help="Optional feature dataset root override."
    )
    data_crypto_binance_status.add_argument(
        "--feature-store-root", type=str, default=None, help="Optional feature-store root override."
    )
    data_crypto_binance_status.add_argument(
        "--latest-sync-manifest-path",
        type=str,
        default=None,
        help="Optional latest sync manifest path override.",
    )
    data_crypto_binance_status.add_argument(
        "--summary-path", type=str, default=None, help="Optional status summary JSON path override."
    )
    data_crypto_binance_status.add_argument(
        "--format",
        type=str,
        default="text",
        choices=["text", "json"],
        help="Render a human-readable summary or emit JSON.",
    )
    data_crypto_binance_status.set_defaults(func=cmd_binance_crypto_status)
    data_crypto_binance_alerts = data_crypto_binance_subparsers.add_parser(
        "alerts",
        help="Evaluate Binance freshness and latest-sync alerts for scheduler and operator workflows.",
    )
    data_crypto_binance_alerts.add_argument(
        "--config",
        type=str,
        default="configs/binance.yaml",
        help="Path to Binance crypto YAML config (default: configs/binance.yaml).",
    )
    data_crypto_binance_alerts.add_argument("--symbols", nargs="*", default=None, help="Optional symbol filter.")
    data_crypto_binance_alerts.add_argument("--intervals", nargs="*", default=None, help="Optional interval filter.")
    data_crypto_binance_alerts.add_argument(
        "--latest-sync-manifest-path",
        type=str,
        default=None,
        help="Optional latest sync manifest path override.",
    )
    data_crypto_binance_alerts.add_argument(
        "--status-summary-path", type=str, default=None, help="Optional Binance status summary path override."
    )
    data_crypto_binance_alerts.add_argument(
        "--output-root", type=str, default=None, help="Optional Binance alerts output directory override."
    )
    data_crypto_binance_alerts.add_argument(
        "--summary-path", type=str, default=None, help="Optional alerts summary JSON path override."
    )
    data_crypto_binance_alerts.add_argument(
        "--format", type=str, default="text", choices=["text", "json"], help="Render text or emit JSON."
    )
    data_crypto_binance_alerts.set_defaults(func=cmd_binance_crypto_alerts)
    data_crypto_binance_health = data_crypto_binance_subparsers.add_parser(
        "health-check",
        help="Run scheduler-oriented Binance health checks for required sync, projection, and feature scopes.",
    )
    data_crypto_binance_health.add_argument(
        "--config",
        type=str,
        default="configs/binance.yaml",
        help="Path to Binance crypto YAML config (default: configs/binance.yaml).",
    )
    data_crypto_binance_health.add_argument("--symbols", nargs="*", default=None, help="Optional symbol filter.")
    data_crypto_binance_health.add_argument("--intervals", nargs="*", default=None, help="Optional interval filter.")
    data_crypto_binance_health.add_argument(
        "--latest-sync-manifest-path",
        type=str,
        default=None,
        help="Optional latest sync manifest path override.",
    )
    data_crypto_binance_health.add_argument(
        "--status-summary-path", type=str, default=None, help="Optional Binance status summary path override."
    )
    data_crypto_binance_health.add_argument(
        "--output-root", type=str, default=None, help="Optional Binance health output directory override."
    )
    data_crypto_binance_health.add_argument(
        "--summary-path", type=str, default=None, help="Optional health summary JSON path override."
    )
    data_crypto_binance_health.add_argument(
        "--format", type=str, default="text", choices=["text", "json"], help="Render text or emit JSON."
    )
    data_crypto_binance_health.set_defaults(func=cmd_binance_crypto_health_check)
    data_crypto_binance_notify = data_crypto_binance_subparsers.add_parser(
        "notify",
        help="Evaluate Binance alerts plus health results and optionally deliver transition-aware notifications.",
    )
    data_crypto_binance_notify.add_argument(
        "--config",
        type=str,
        default="configs/binance.yaml",
        help="Path to Binance crypto YAML config (default: configs/binance.yaml).",
    )
    data_crypto_binance_notify.add_argument("--symbols", nargs="*", default=None, help="Optional symbol filter.")
    data_crypto_binance_notify.add_argument("--intervals", nargs="*", default=None, help="Optional interval filter.")
    data_crypto_binance_notify.add_argument(
        "--latest-sync-manifest-path",
        type=str,
        default=None,
        help="Optional latest sync manifest path override.",
    )
    data_crypto_binance_notify.add_argument(
        "--status-summary-path", type=str, default=None, help="Optional Binance status summary path override."
    )
    data_crypto_binance_notify.add_argument(
        "--alerts-output-root", type=str, default=None, help="Optional Binance alerts output directory override."
    )
    data_crypto_binance_notify.add_argument(
        "--alerts-summary-path", type=str, default=None, help="Optional Binance alerts summary JSON path override."
    )
    data_crypto_binance_notify.add_argument(
        "--health-output-root", type=str, default=None, help="Optional Binance health output directory override."
    )
    data_crypto_binance_notify.add_argument(
        "--health-summary-path", type=str, default=None, help="Optional Binance health summary JSON path override."
    )
    data_crypto_binance_notify.add_argument(
        "--output-root", type=str, default=None, help="Optional Binance notification output directory override."
    )
    data_crypto_binance_notify.add_argument(
        "--summary-path", type=str, default=None, help="Optional notification summary JSON path override."
    )
    data_crypto_binance_notify.add_argument(
        "--state-path", type=str, default=None, help="Optional notification state JSON path override."
    )
    data_crypto_binance_notify.add_argument(
        "--notification-config-path",
        type=str,
        default=None,
        help="Optional shared notification JSON/YAML config path override.",
    )
    data_crypto_binance_notify.add_argument(
        "--enabled",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable or disable notification delivery while still writing evaluation artifacts.",
    )
    data_crypto_binance_notify.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Evaluate and format notifications without delivering them.",
    )
    data_crypto_binance_notify.add_argument(
        "--subject-prefix", type=str, default=None, help="Optional notification subject prefix override."
    )
    data_crypto_binance_notify.add_argument(
        "--format", type=str, default="text", choices=["text", "json"], help="Render text or emit JSON."
    )
    data_crypto_binance_notify.set_defaults(func=cmd_binance_crypto_notify)
    data_kalshi = data_subparsers.add_parser(
        "kalshi", help="Kalshi prediction-market data and feature generation commands"
    )
    data_kalshi_subparsers = data_kalshi.add_subparsers(dest="kalshi_command", required=True)
    data_kalshi_features = data_kalshi_subparsers.add_parser(
        "features",
        help="Build Kalshi feature parquet files from ingested trade history.",
    )
    data_kalshi_features.add_argument(
        "--config",
        type=str,
        default="configs/kalshi.yaml",
        help="Path to kalshi.yaml config (default: configs/kalshi.yaml).",
    )
    data_kalshi_features.add_argument(
        "--tickers",
        nargs="*",
        default=None,
        help="One or more Kalshi market tickers to process. Overrides config tracked_tickers.",
    )
    data_kalshi_features.add_argument(
        "--trades-dir",
        type=str,
        default="data/kalshi/normalized/trades",
        help="Directory containing <TICKER>.parquet trade files (default: data/kalshi/normalized/trades).",
    )
    data_kalshi_features.add_argument(
        "--output-dir",
        type=str,
        default="data/kalshi/features/real",
        help="Directory to write <TICKER>.parquet feature files (default: data/kalshi/features/real).",
    )
    data_kalshi_features.add_argument(
        "--period",
        type=str,
        default="1h",
        help="Bar resampling period, e.g. '1h', '15m', '1d' (default: 1h).",
    )
    data_kalshi_features.add_argument(
        "--feature-groups",
        nargs="*",
        default=None,
        metavar="GROUP",
        help=(
            "Subset of feature groups to compute. "
            "Choices: momentum trend volatility volume probability_calibration "
            "volume_activity time_decay. Defaults to all groups."
        ),
    )
    data_kalshi_features.set_defaults(func=cmd_kalshi_features)

    data_kalshi_historical_ingest = data_kalshi_subparsers.add_parser(
        "historical-ingest",
        help="Download resolved Kalshi markets from the past year and build feature parquets.",
    )
    data_kalshi_historical_ingest.add_argument(
        "--config",
        type=str,
        default="configs/kalshi.yaml",
        help="Path to kalshi.yaml config (default: configs/kalshi.yaml).",
    )
    data_kalshi_historical_ingest.add_argument(
        "--lookback-days",
        type=int,
        default=None,
        help="Number of days to look back for resolved markets (default: 365).",
    )
    data_kalshi_historical_ingest.add_argument(
        "--period",
        type=str,
        default=None,
        help="Bar resampling period, e.g. '1h', '1d' (default: 1h).",
    )
    data_kalshi_historical_ingest.add_argument(
        "--sleep",
        type=float,
        default=None,
        help="Sleep seconds between API requests (default: 0.05 = 20 req/sec).",
    )
    data_kalshi_historical_ingest.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Directory to write feature parquets (default: data/kalshi/features/real).",
    )
    data_kalshi_historical_ingest.add_argument(
        "--tickers",
        nargs="*",
        default=None,
        help="Optional list of specific tickers to ingest. Defaults to all resolved markets.",
    )
    data_kalshi_historical_ingest.add_argument(
        "--no-base-rate",
        action="store_true",
        default=False,
        help="Disable base rate feature enrichment.",
    )
    data_kalshi_historical_ingest.add_argument(
        "--metaculus",
        action="store_true",
        default=False,
        help="Enable Metaculus divergence feature enrichment (requires pre-built matches).",
    )
    data_kalshi_historical_ingest.add_argument(
        "--skip-validation",
        action="store_true",
        default=False,
        help="Skip the post-ingest Kalshi dataset validation pass.",
    )
    data_kalshi_historical_ingest.add_argument(
        "--resume-from-checkpoint",
        type=str,
        default=None,
        help="Resume from an explicit Kalshi historical-ingest checkpoint JSON path.",
    )
    data_kalshi_historical_ingest.add_argument(
        "--fresh-run",
        action="store_true",
        default=False,
        help="Ignore prior checkpoint state and start a fresh ingest run.",
    )
    data_kalshi_historical_ingest.add_argument(
        "--reprocess",
        action="store_true",
        default=False,
        help="Re-fetch candles and regenerate features for all existing raw market JSON files.",
    )
    data_kalshi_historical_ingest.add_argument(
        "--resume-recovery-mode",
        type=str,
        default=None,
        choices=["automatic", "backup_only", "cursor_reset_only", "fail_fast"],
        help="How resume should recover from a poisoned saved live pagination cursor.",
    )
    data_kalshi_historical_ingest.set_defaults(func=cmd_kalshi_historical_ingest)
    data_kalshi_recent_ingest = data_kalshi_subparsers.add_parser(
        "recent-ingest",
        help="Fetch recent filtered Kalshi markets from the live /markets endpoint and optional direct historical tickers.",
    )
    data_kalshi_recent_ingest.add_argument(
        "--config",
        type=str,
        default="configs/kalshi.yaml",
        help="Path to kalshi.yaml config (default: configs/kalshi.yaml).",
    )
    data_kalshi_recent_ingest.add_argument(
        "--lookback-days",
        type=int,
        default=None,
        help="Optional lookback window for trade/candle feature generation.",
    )
    data_kalshi_recent_ingest.add_argument(
        "--status",
        nargs="*",
        default=None,
        help="One or more live /markets status filters, e.g. settled open.",
    )
    data_kalshi_recent_ingest.add_argument(
        "--category",
        nargs="*",
        default=None,
        help="One or more Kalshi categories for live recent ingestion.",
    )
    data_kalshi_recent_ingest.add_argument(
        "--series",
        nargs="*",
        default=None,
        help="Optional series_ticker filters for live recent ingestion.",
    )
    data_kalshi_recent_ingest.add_argument(
        "--event",
        nargs="*",
        default=None,
        help="Optional event_ticker filters for live recent ingestion.",
    )
    data_kalshi_recent_ingest.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of live /markets records to fetch across pagination before stopping.",
    )
    data_kalshi_recent_ingest.add_argument(
        "--min-volume",
        type=float,
        default=None,
        help="Optional minimum market volume required for recent-ingest retention.",
    )
    data_kalshi_recent_ingest.add_argument(
        "--disable-market-type-filter",
        action="store_true",
        default=False,
        help="Disable recent-ingest ticker-pattern exclusions such as CROSSCATEGORY and EXTENDED.",
    )
    data_kalshi_recent_ingest.add_argument(
        "--direct-historical-tickers",
        nargs="*",
        default=None,
        help="Optional older Kalshi tickers to fetch directly from /historical/markets/{ticker}.",
    )
    data_kalshi_recent_ingest.add_argument(
        "--period",
        type=str,
        default=None,
        help="Bar resampling period, e.g. '1h', '1d' (default: 1h).",
    )
    data_kalshi_recent_ingest.add_argument(
        "--sleep",
        type=float,
        default=None,
        help="Sleep seconds between historical requests such as direct ticker lookups.",
    )
    data_kalshi_recent_ingest.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Directory to write feature parquets (default: data/kalshi/features/real).",
    )
    data_kalshi_recent_ingest.add_argument(
        "--no-base-rate",
        action="store_true",
        default=False,
        help="Disable base rate feature enrichment.",
    )
    data_kalshi_recent_ingest.add_argument(
        "--metaculus",
        action="store_true",
        default=False,
        help="Enable Metaculus divergence feature enrichment (requires pre-built matches).",
    )
    data_kalshi_recent_ingest.add_argument(
        "--skip-validation",
        action="store_true",
        default=False,
        help="Skip the post-ingest Kalshi dataset validation pass.",
    )
    data_kalshi_recent_ingest.add_argument(
        "--resume",
        action="store_true",
        default=False,
        help="Reuse the recent-ingest checkpoint to skip already-processed retained tickers.",
    )
    data_kalshi_recent_ingest.set_defaults(func=cmd_kalshi_recent_ingest)
    data_kalshi_validate = data_kalshi_subparsers.add_parser(
        "validate-dataset",
        help="Audit normalized Kalshi ingest artifacts and emit structured data-quality reports.",
    )
    data_kalshi_validate.add_argument(
        "--config",
        type=str,
        default="configs/kalshi.yaml",
        help="Path to kalshi.yaml config (default: configs/kalshi.yaml).",
    )
    data_kalshi_validate.add_argument(
        "--markets-path",
        type=str,
        default=None,
        help="Optional override for data/kalshi/normalized/markets.parquet.",
    )
    data_kalshi_validate.add_argument(
        "--trades-path",
        type=str,
        default=None,
        help="Optional override for the normalized Kalshi trades parquet file or directory.",
    )
    data_kalshi_validate.add_argument(
        "--candles-path",
        type=str,
        default=None,
        help="Optional override for the normalized Kalshi candles parquet file or directory.",
    )
    data_kalshi_validate.add_argument(
        "--resolution-path",
        type=str,
        default=None,
        help="Optional override for data/kalshi/normalized/resolution.csv.",
    )
    data_kalshi_validate.add_argument(
        "--ingest-summary-path",
        type=str,
        default=None,
        help="Optional override for data/kalshi/raw/ingest_summary.json.",
    )
    data_kalshi_validate.add_argument(
        "--ingest-manifest-path",
        type=str,
        default=None,
        help="Optional override for data/kalshi/raw/ingest_manifest.json.",
    )
    data_kalshi_validate.add_argument(
        "--ingest-checkpoint-path",
        type=str,
        default=None,
        help="Optional override for data/kalshi/raw/ingest_checkpoint.json.",
    )
    data_kalshi_validate.add_argument(
        "--features-dir",
        type=str,
        default=None,
        help="Optional override for the real-data Kalshi feature directory.",
    )
    data_kalshi_validate.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Directory where validation artifacts will be written (default: data/kalshi/validation).",
    )
    data_kalshi_validate.set_defaults(func=cmd_kalshi_validate_dataset)

    data_kalshi_live_candles = data_kalshi_subparsers.add_parser(
        "live-candles",
        help="Fetch hourly candles for open Kalshi markets via authenticated series endpoint.",
    )
    data_kalshi_live_candles.add_argument(
        "--config", type=str, default="configs/kalshi.yaml",
        help="Path to Kalshi YAML config.",
    )
    data_kalshi_live_candles.add_argument(
        "--lookback-days", type=int, default=30,
        help="Days of candle history to fetch (default: 30).",
    )
    data_kalshi_live_candles.add_argument(
        "--output-dir", type=str, default=None,
        help="Output directory (default: data/kalshi/live).",
    )
    data_kalshi_live_candles.add_argument(
        "--loop", action="store_true", default=False,
        help="Run continuously, re-fetching every --interval minutes.",
    )
    data_kalshi_live_candles.add_argument(
        "--interval", type=int, default=60,
        help="Minutes between collection runs when --loop is set (default: 60).",
    )
    data_kalshi_live_candles.set_defaults(func=cmd_kalshi_live_candles)

    # ── Polymarket data commands ──────────────────────────────────────────────
    data_polymarket = data_subparsers.add_parser(
        "polymarket", help="Polymarket prediction market data ingestion commands"
    )
    data_polymarket_subparsers = data_polymarket.add_subparsers(dest="polymarket_command", required=True)
    data_polymarket_ingest = data_polymarket_subparsers.add_parser(
        "ingest",
        help="Download closed/resolved Polymarket markets and generate feature parquets.",
    )
    data_polymarket_ingest.add_argument(
        "--config",
        type=str,
        default="configs/polymarket.yaml",
        help="Path to polymarket YAML config (default: configs/polymarket.yaml).",
    )
    data_polymarket_ingest.add_argument(
        "--lookback-days",
        type=int,
        default=None,
        help="Number of days back to ingest (overrides config).",
    )
    data_polymarket_ingest.add_argument(
        "--min-volume",
        type=float,
        default=None,
        help="Minimum lifetime volume to include a market (overrides config).",
    )
    data_polymarket_ingest.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Override features output directory.",
    )
    data_polymarket_ingest.add_argument(
        "--sleep",
        type=float,
        default=None,
        help="Sleep between API requests in seconds (overrides config).",
    )
    data_polymarket_ingest.add_argument(
        "--sort-newest-first",
        type=lambda v: v.lower() in ("true", "1", "yes"),
        default=None,
        metavar="BOOL",
        help="Sort markets newest-first to avoid paging through old data (default: true).",
    )
    data_polymarket_ingest.set_defaults(func=cmd_polymarket_ingest)

    data_polymarket_live = data_polymarket_subparsers.add_parser(
        "live-collect",
        help="Start live WebSocket price collector for open Polymarket markets.",
    )
    data_polymarket_live.add_argument(
        "--config",
        type=str,
        default="configs/polymarket.yaml",
        help="Path to polymarket YAML config (default: configs/polymarket.yaml).",
    )
    data_polymarket_live.add_argument(
        "--max-markets",
        type=int,
        default=None,
        help="Maximum number of markets to collect (default: 75).",
    )
    data_polymarket_live.set_defaults(func=cmd_polymarket_live_collect)

    data_polymarket_blockchain = data_polymarket_subparsers.add_parser(
        "blockchain-ingest",
        help="Convert poly-trade-scan on-chain trade CSV into feature parquets.",
    )
    data_polymarket_blockchain.add_argument(
        "--trades-csv", type=str, required=True,
        help="Path to poly-trade-scan output CSV.",
    )
    data_polymarket_blockchain.add_argument(
        "--output-dir", type=str, default=None,
        help="Output directory (default: data/polymarket/blockchain).",
    )
    data_polymarket_blockchain.add_argument(
        "--min-trades", type=int, default=10,
        help="Minimum trades per market (default: 10).",
    )
    data_polymarket_blockchain.add_argument(
        "--limit", type=int, default=None,
        help="Max markets to process (for testing).",
    )
    data_polymarket_blockchain.set_defaults(func=cmd_polymarket_blockchain_ingest)

    data_polymarket_wallets = data_polymarket_subparsers.add_parser(
        "wallet-profiles",
        help="Build wallet performance profiles from blockchain trade history.",
    )
    data_polymarket_wallets.add_argument(
        "--trades-csv", type=str, required=True,
        help="Path to poly-trade-scan output CSV.",
    )
    data_polymarket_wallets.add_argument(
        "--resolution-csv", type=str, required=True,
        help="Path to resolution CSV (from blockchain-ingest).",
    )
    data_polymarket_wallets.add_argument(
        "--output", type=str, default=None,
        help="Output parquet path (default: data/polymarket/wallet_profiles.parquet).",
    )
    data_polymarket_wallets.set_defaults(func=cmd_polymarket_wallet_profiles)

    data_polymarket_clob = data_polymarket_subparsers.add_parser(
        "clob-fetch", help="Fetch trade history from CLOB API for active markets.",
    )
    data_polymarket_clob.add_argument(
        "--output-dir", type=str, default=None,
        help="Output directory for trade CSVs (default: data/polymarket/clob_trades).",
    )
    data_polymarket_clob.add_argument(
        "--hours-back", type=int, default=168,
        help="Hours of trade history to fetch (default: 168 = 7 days).",
    )
    data_polymarket_clob.set_defaults(func=cmd_polymarket_clob_fetch)

    data_polymarket_orderbook = data_polymarket_subparsers.add_parser(
        "orderbook-fetch", help="Fetch orderbook depth from Goldsky subgraph.",
    )
    data_polymarket_orderbook.set_defaults(func=cmd_polymarket_orderbook_fetch)

    data_polymarket_data_api = data_polymarket_subparsers.add_parser(
        "data-api-fetch", help="Fetch trades from Polymarket Data API (no auth, all markets).",
    )
    data_polymarket_data_api.add_argument(
        "--output-dir", type=str, default=None,
        help="Output directory (default: data/polymarket/data_api_trades).",
    )
    data_polymarket_data_api.add_argument(
        "--hours-back", type=int, default=168,
        help="Hours of trade history (default: 168 = 7 days).",
    )
    data_polymarket_data_api.add_argument(
        "--condition-id", type=str, default=None,
        help="Fetch trades for a specific market conditionId.",
    )
    data_polymarket_data_api.set_defaults(func=cmd_polymarket_data_api_fetch)

    # ── data manifold ────────────────────────────────────────────────────────
    data_manifold = data_subparsers.add_parser(
        "manifold", help="Manifold Markets data dump parsing commands"
    )
    data_manifold_subparsers = data_manifold.add_subparsers(dest="manifold_command", required=True)
    data_manifold_parse = data_manifold_subparsers.add_parser(
        "parse",
        help="Parse a Manifold data dump into feature parquets and resolution CSV.",
    )
    data_manifold_parse.add_argument(
        "--dump-dir",
        type=str,
        required=True,
        help="Path to the Manifold data dump directory containing markets.json and bets.json.",
    )
    data_manifold_parse.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Output directory for features and resolution.csv (default: data/manifold).",
    )
    data_manifold_parse.add_argument(
        "--min-bets",
        type=int,
        default=10,
        help="Minimum bets per market to include (default: 10).",
    )
    data_manifold_parse.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of markets to process (for testing).",
    )
    data_manifold_parse.set_defaults(func=cmd_manifold_parse)

    # ── data predictit ───────────────────────────────────────────────────────
    data_predictit = data_subparsers.add_parser(
        "predictit", help="PredictIt historical data parsing commands"
    )
    data_predictit_subparsers = data_predictit.add_subparsers(dest="predictit_command", required=True)
    data_predictit_parse = data_predictit_subparsers.add_parser(
        "parse",
        help="Parse a PredictIt historical CSV into feature parquets and resolution CSV.",
    )
    data_predictit_parse.add_argument(
        "--csv-path",
        type=str,
        required=True,
        help="Path to the PredictIt daily market data CSV.",
    )
    data_predictit_parse.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Output directory for features and resolution.csv (default: data/predictit).",
    )
    data_predictit_parse.add_argument(
        "--min-bars",
        type=int,
        default=10,
        help="Minimum daily bars per contract to include (default: 10).",
    )
    data_predictit_parse.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of contracts to process (for testing).",
    )
    data_predictit_parse.set_defaults(func=cmd_predictit_parse)

    # ── data news ────────────────────────────────────────────────────────────
    data_news = data_subparsers.add_parser(
        "news", help="Economic news calendar and market move labeling"
    )
    data_news_subparsers = data_news.add_subparsers(dest="news_command", required=True)
    data_news_upcoming = data_news_subparsers.add_parser(
        "upcoming", help="Show upcoming economic events from Kalshi tickers.",
    )
    data_news_upcoming.add_argument(
        "--days", type=int, default=7,
        help="Days ahead to scan (default: 7).",
    )
    data_news_upcoming.set_defaults(func=cmd_news_upcoming)

    data_news_label = data_news_subparsers.add_parser(
        "label-moves", help="Label a market move as scheduled/pre-event/unscheduled.",
    )
    data_news_label.add_argument(
        "--ticker", type=str, required=True,
        help="Kalshi market ticker (e.g. KXCPI-26MAY-T0.3).",
    )
    data_news_label.set_defaults(func=cmd_news_label_moves)

    # ── data metaculus ───────────────────────────────────────────────────────
    data_metaculus = data_subparsers.add_parser(
        "metaculus", help="Metaculus prediction data fetching commands"
    )
    data_metaculus_subparsers = data_metaculus.add_subparsers(dest="metaculus_command", required=True)
    data_metaculus_fetch = data_metaculus_subparsers.add_parser(
        "fetch", help="Fetch resolved binary questions from Metaculus public API.",
    )
    data_metaculus_fetch.add_argument(
        "--output-dir", type=str, default=None,
        help="Output directory (default: data/metaculus).",
    )
    data_metaculus_fetch.add_argument(
        "--limit", type=int, default=2000,
        help="Maximum questions to fetch (default: 2000).",
    )
    data_metaculus_fetch.add_argument(
        "--min-forecasts", type=int, default=5,
        help="Minimum forecast points per question (default: 5).",
    )
    data_metaculus_fetch.set_defaults(func=cmd_metaculus_fetch)

    data_fundamentals = data_subparsers.add_parser(
        "fundamentals", help="Canonical fundamentals ingest and daily feature generation commands"
    )
    data_fundamentals_subparsers = data_fundamentals.add_subparsers(dest="fundamentals_command", required=True)
    data_fundamentals_ingest = data_fundamentals_subparsers.add_parser(
        "ingest", help="Ingest canonical company master, filing, and normalized fundamental value artifacts"
    )
    add_shared_symbol_args(data_fundamentals_ingest)
    data_fundamentals_ingest.add_argument(
        "--artifact-root",
        type=str,
        default="data/fundamentals",
        help="Directory where canonical fundamentals artifacts are written.",
    )
    data_fundamentals_ingest.add_argument(
        "--providers",
        nargs="+",
        default=["sec", "vendor"],
        choices=["sec", "vendor"],
        help="Ordered list of fundamentals providers to use.",
    )
    data_fundamentals_ingest.add_argument(
        "--sec-companyfacts-root", type=str, default=None, help="Local SEC companyfacts JSON directory."
    )
    data_fundamentals_ingest.add_argument(
        "--sec-submissions-root", type=str, default=None, help="Local SEC submissions JSON directory."
    )
    data_fundamentals_ingest.add_argument(
        "--vendor-file-path",
        type=str,
        default=None,
        help="Optional normalized vendor fundamentals JSON/CSV/parquet file.",
    )
    data_fundamentals_ingest.add_argument(
        "--vendor-api-key",
        "--fundamentals-vendor-api-key",
        dest="vendor_api_key",
        type=str,
        default=None,
        help="Optional FMP vendor API key. Falls back to FMP_API_KEY when omitted.",
    )
    data_fundamentals_ingest.add_argument(
        "--vendor-cache-enabled",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable or disable raw FMP response caching.",
    )
    data_fundamentals_ingest.add_argument(
        "--vendor-cache-root",
        type=str,
        default=None,
        help="Optional raw FMP cache root. Defaults under the fundamentals artifact root.",
    )
    data_fundamentals_ingest.add_argument(
        "--vendor-cache-ttl-hours", type=float, default=24.0, help="Freshness TTL for cached FMP responses."
    )
    data_fundamentals_ingest.add_argument(
        "--vendor-force-refresh", action="store_true", help="Bypass fresh cache entries and refetch vendor data."
    )
    data_fundamentals_ingest.add_argument(
        "--vendor-request-delay-seconds", type=float, default=0.5, help="Throttle delay between FMP requests."
    )
    data_fundamentals_ingest.add_argument(
        "--vendor-max-retries", type=int, default=4, help="Maximum FMP retry attempts for 429/transient failures."
    )
    data_fundamentals_ingest.add_argument(
        "--vendor-max-symbols-per-run",
        type=int,
        default=None,
        help="Optional cap on symbols fetched from FMP in one run.",
    )
    data_fundamentals_ingest.add_argument(
        "--vendor-max-requests-per-run", type=int, default=None, help="Optional cap on raw FMP requests in one run."
    )
    data_fundamentals_ingest.set_defaults(func=cmd_fundamentals_ingest)
    data_fundamentals_snapshot = data_fundamentals_subparsers.add_parser(
        "snapshot-build", help="Fetch/cache SEC raw fundamentals locally and rebuild canonical artifacts"
    )
    add_shared_symbol_args(data_fundamentals_snapshot)
    data_fundamentals_snapshot.add_argument(
        "--config", type=str, default=None, help="Optional SEC fundamentals snapshot workflow config."
    )
    data_fundamentals_snapshot.add_argument(
        "--artifact-root",
        type=str,
        default="data/fundamentals",
        help="Directory where canonical fundamentals artifacts are written.",
    )
    data_fundamentals_snapshot.add_argument(
        "--raw-sec-cache-root",
        type=str,
        default=None,
        help="Raw SEC cache root containing submissions/ and companyfacts/. Defaults under the artifact root.",
    )
    data_fundamentals_snapshot.add_argument(
        "--symbol-cik-map-path", type=str, default=None, help="Optional symbol-to-CIK lookup artifact path."
    )
    data_fundamentals_snapshot.add_argument(
        "--sec-user-agent", type=str, default=None, help="SEC User-Agent header for network fetches."
    )
    data_fundamentals_snapshot.add_argument(
        "--sec-request-delay-seconds", type=float, default=0.2, help="Throttle delay between SEC requests."
    )
    data_fundamentals_snapshot.add_argument(
        "--sec-max-retries", type=int, default=4, help="Maximum SEC retry attempts for transient failures."
    )
    data_fundamentals_snapshot.add_argument(
        "--cache-enabled",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable or disable SEC raw cache reuse.",
    )
    data_fundamentals_snapshot.add_argument(
        "--cache-ttl-days", type=float, default=30.0, help="Freshness TTL for cached SEC raw responses."
    )
    data_fundamentals_snapshot.add_argument(
        "--force-refresh", action="store_true", help="Refetch stale/fresh SEC raw files instead of reusing cache."
    )
    data_fundamentals_snapshot.add_argument(
        "--max-symbols-per-run", type=int, default=None, help="Optional cap on symbols fetched in one SEC snapshot run."
    )
    data_fundamentals_snapshot.add_argument(
        "--max-requests-per-run",
        type=int,
        default=None,
        help="Optional cap on raw SEC requests in one SEC snapshot run.",
    )
    data_fundamentals_snapshot.add_argument(
        "--build-daily-features",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Build daily aligned fundamental features after canonical normalization.",
    )
    data_fundamentals_snapshot.add_argument(
        "--calendar-dir",
        type=str,
        default="data/features",
        help="Directory containing per-symbol research calendar parquet files used for daily alignment.",
    )
    data_fundamentals_snapshot.add_argument(
        "--offline",
        action="store_true",
        help="Rebuild canonical artifacts strictly from cached SEC raw files with no network calls.",
    )
    data_fundamentals_snapshot.set_defaults(func=cmd_fundamentals_snapshot_build)
    data_fundamentals_features = data_fundamentals_subparsers.add_parser(
        "features", help="Build point-in-time-safe daily fundamental features aligned to a research calendar"
    )
    add_shared_symbol_args(data_fundamentals_features)
    data_fundamentals_features.add_argument(
        "--artifact-root",
        type=str,
        default="data/fundamentals",
        help="Directory containing canonical fundamentals artifacts.",
    )
    data_fundamentals_features.add_argument(
        "--calendar-dir",
        type=str,
        default="data/features",
        help="Directory containing per-symbol research calendar parquet files used for daily alignment.",
    )
    data_fundamentals_features.add_argument(
        "--daily-features-path",
        type=str,
        default=None,
        help="Optional output parquet path for the daily fundamental feature panel.",
    )
    data_fundamentals_features.set_defaults(func=cmd_fundamentals_features)
    data_universes = data_subparsers.add_parser("universes", help="Inspect or export named universes")
    data_universe_subparsers = data_universes.add_subparsers(dest="universe_command", required=True)
    data_universe_list = data_universe_subparsers.add_parser("list", help="Show available named universes")
    data_universe_list.set_defaults(func=cmd_list_universes)
    data_universe_export = data_universe_subparsers.add_parser(
        "export", help="Export the current static universe definitions to JSON"
    )
    data_universe_export.add_argument(
        "--output",
        type=str,
        default="artifacts/universes/universes.json",
        help="Path where the universe definitions JSON should be written.",
    )
    data_universe_export.set_defaults(func=cmd_export_universes)
    data_build_classifications = data_subparsers.add_parser(
        "build-classifications",
        help="Build a normalized security master classification artifact using FinanceDatabase when installed",
    )
    data_build_classifications.add_argument(
        "--config", type=str, default=None, help="Optional YAML or JSON config file for classification builds."
    )
    add_shared_symbol_args(data_build_classifications)
    data_build_classifications.add_argument(
        "--output-dir",
        type=str,
        default="artifacts/reference/classifications",
        help="Directory where normalized classification artifacts will be written.",
    )
    data_build_classifications.add_argument(
        "--as-of-date",
        type=str,
        default=None,
        help="Optional as-of date label stored with the classification snapshot.",
    )
    data_build_classifications.set_defaults(func=cmd_data_build_classifications)

    research_parser = subparsers.add_parser(
        "research", help="Canonical research and promotion workflow plus clearly labeled secondary workflows"
    )
    research_subparsers = research_parser.add_subparsers(dest="research_command", required=True)
    research_run = research_subparsers.add_parser("run", help="Run backtests directly or from a config file")
    add_shared_symbol_args(research_run)
    add_preset_argument(
        research_run,
        help_text="Optional versioned preset for validated research configurations. Explicit CLI flags still override preset values.",
    )
    add_strategy_arguments(research_run, include_xsec=True)
    add_xsec_research_arguments(research_run)
    add_date_range_arguments(research_run)
    add_execution_arguments(research_run)
    research_run.add_argument(
        "--engine", type=str, default="legacy", choices=["legacy", "vectorized"], help="Backtest engine to use"
    )
    research_run.add_argument(
        "--output-dir", type=str, default=None, help="Optional directory to save vectorized research outputs"
    )
    research_run.add_argument(
        "--cost-bps",
        type=float,
        default=None,
        help="Optional transaction cost in basis points per unit of turnover. Overrides --commission when provided.",
    )
    research_run.add_argument(
        "--config", type=str, default=None, help="Optional YAML or JSON research workflow config file."
    )
    research_run.add_argument(
        "--fail-fast", action="store_true", help="Stop immediately on the first symbol error when using --config."
    )
    research_run.set_defaults(func=_cmd_research_run)
    research_sweep = research_subparsers.add_parser("sweep", help="Run parameter sweeps directly or from a config file")
    add_shared_symbol_args(research_sweep)
    add_preset_argument(
        research_sweep,
        help_text="Optional versioned preset for validated research configurations. Explicit CLI flags still override preset values.",
    )
    add_date_range_arguments(research_sweep)
    add_execution_arguments(research_sweep)
    research_sweep.add_argument(
        "--strategy",
        type=str,
        default="sma_cross",
        choices=get_strategy_choices(include_xsec=True),
        help="Strategy to sweep",
    )
    research_sweep.add_argument("--fast-values", type=int, nargs="+")
    research_sweep.add_argument("--slow-values", type=int, nargs="+")
    research_sweep.add_argument("--lookback-values", type=int, nargs="+")
    research_sweep.add_argument("--lookback-bars-values", type=int, nargs="+")
    research_sweep.add_argument("--skip-bars-values", type=int, nargs="+")
    research_sweep.add_argument("--top-n-values", type=int, nargs="+")
    research_sweep.add_argument("--rebalance-bars-values", type=int, nargs="+")
    research_sweep.add_argument(
        "--portfolio-construction-mode",
        type=str,
        default="pure_topn",
        choices=["pure_topn", "transition"],
        help="Use pure_topn for research-clean top-N portfolios or transition for gradual deployable transitions.",
    )
    research_sweep.add_argument(
        "--max-position-weight", type=float, default=None, help="Optional cap on any single xsec position weight."
    )
    research_sweep.add_argument(
        "--min-avg-dollar-volume",
        type=float,
        default=None,
        help="Optional minimum 20-bar average dollar volume required for xsec eligibility.",
    )
    research_sweep.add_argument(
        "--max-names-per-sector",
        type=int,
        default=None,
        help="Optional maximum number of selected names per sector when sector metadata is available.",
    )
    research_sweep.add_argument(
        "--turnover-buffer-bps",
        type=float,
        default=0.0,
        help="Optional minimum momentum-score improvement, expressed in bps of score gap, required to replace an existing xsec holding.",
    )
    research_sweep.add_argument(
        "--max-turnover-per-rebalance",
        type=float,
        default=None,
        help="Optional cap on absolute turnover per xsec rebalance.",
    )
    research_sweep.add_argument(
        "--weighting-scheme",
        type=str,
        default="equal",
        choices=["equal", "inv_vol"],
        help="How to size selected xsec holdings.",
    )
    research_sweep.add_argument(
        "--vol-lookback-bars", type=int, default=20, help="Lookback window for inverse-vol xsec weighting."
    )
    research_sweep.add_argument(
        "--benchmark",
        type=str,
        default="equal_weight",
        choices=["equal_weight"],
        help="Benchmark type for cross-sectional research",
    )
    research_sweep.add_argument("--entry-lookback-values", type=int, nargs="+")
    research_sweep.add_argument("--exit-lookback-values", type=int, nargs="+")
    research_sweep.add_argument("--momentum-lookback-values", type=int, nargs="+")
    research_sweep.add_argument("--cash", type=float, default=10_000)
    research_sweep.add_argument("--commission", type=float, default=0.001)
    research_sweep.add_argument(
        "--cost-bps",
        type=float,
        default=None,
        help="Optional transaction cost in basis points per unit of turnover. Overrides --commission when provided.",
    )
    research_sweep.add_argument(
        "--output",
        type=str,
        default="artifacts/experiments/sweep_results.csv",
        help="CSV output path for sweep summary",
    )
    research_sweep.add_argument(
        "--engine", type=str, default="legacy", choices=["legacy", "vectorized"], help="Backtest engine to use"
    )
    research_sweep.add_argument(
        "--config", type=str, default=None, help="Optional YAML or JSON parameter sweep config file."
    )
    research_sweep.add_argument(
        "--fail-fast", action="store_true", help="Stop immediately on the first sweep error when using --config."
    )
    research_sweep.set_defaults(func=_cmd_research_sweep)
    research_walkforward = research_subparsers.add_parser(
        "walkforward", help="Canonical step 4: run walk-forward validation directly or from a config file"
    )
    add_preset_argument(
        research_walkforward,
        help_text="Optional versioned preset for validated research configurations. Explicit CLI flags still override preset values.",
    )
    _add_walkforward_arguments(research_walkforward)
    research_walkforward.set_defaults(func=_cmd_research_walkforward)
    research_compare_xsec = research_subparsers.add_parser(
        "compare-xsec-construction", help="Compare pure_topn versus transition xsec walk-forward behavior"
    )
    add_preset_argument(
        research_compare_xsec,
        help_text="Optional versioned preset for the validated xsec family. Explicit CLI flags still override preset values.",
    )
    _add_walkforward_arguments(research_compare_xsec)
    research_compare_xsec.add_argument(
        "--output-dir",
        type=str,
        default="artifacts/experiments",
        help="Directory where xsec construction comparison artifacts will be written.",
    )
    research_compare_xsec.set_defaults(func=_cmd_research_compare_xsec_construction)
    research_decision_memo = research_subparsers.add_parser(
        "decision-memo", help="Generate a durable strategy decision memo for selected presets"
    )
    add_preset_argument(research_decision_memo, help_text="Research preset to document in the decision memo.")
    research_decision_memo.add_argument(
        "--deploy-preset",
        type=str,
        required=True,
        choices=get_preset_choices(),
        help="Deployable overlay preset to document alongside the research preset.",
    )
    research_decision_memo.add_argument(
        "--output-dir",
        type=str,
        default="artifacts/experiments",
        help="Directory where the decision memo artifacts will be written.",
    )
    research_decision_memo.add_argument(
        "--output-stem", type=str, default=None, help="Optional filename stem for the decision memo artifacts."
    )
    research_decision_memo.set_defaults(func=cmd_decision_memo)
    research_memo = research_subparsers.add_parser(
        "memo", help="Canonical step 5: generate a durable decision memo for the selected research and deploy presets"
    )
    add_preset_argument(research_memo, help_text="Research preset to document in the decision memo.")
    research_memo.add_argument(
        "--deploy-preset",
        type=str,
        required=True,
        choices=get_preset_choices(),
        help="Deployable overlay preset to document alongside the research preset.",
    )
    research_memo.add_argument(
        "--output-dir",
        type=str,
        default="artifacts/experiments",
        help="Directory where the decision memo artifacts will be written.",
    )
    research_memo.add_argument(
        "--output-stem", type=str, default=None, help="Optional filename stem for the decision memo artifacts."
    )
    research_memo.set_defaults(func=cmd_decision_memo)
    research_validate_signal = research_subparsers.add_parser(
        "validate-signal", help="Validate a signal on one ticker or a small universe with per-symbol reports"
    )
    _add_validate_signal_arguments(research_validate_signal)
    research_validate_signal.set_defaults(func=cmd_validate_signal)
    research_pipeline = research_subparsers.add_parser(
        "pipeline", help="Run ingest, features, and legacy research in one command"
    )
    add_shared_symbol_args(research_pipeline)
    add_feature_arguments(research_pipeline)
    add_strategy_arguments(research_pipeline)
    research_pipeline.add_argument(
        "--start", type=str, default="2010-01-01", help="Start date in YYYY-MM-DD format (default: 2010-01-01)"
    )
    research_pipeline.set_defaults(func=cmd_pipeline)
    research_alpha = research_subparsers.add_parser(
        "alpha", help="Canonical step 2: run cross-sectional alpha research on refreshed inputs"
    )
    _add_alpha_research_arguments(research_alpha)
    research_alpha.set_defaults(func=cmd_alpha_research)
    research_validate_backtester = research_subparsers.add_parser(
        "validate-backtester",
        help="Run a narrow vectorbt-vs-platform validation harness on canonical benchmark scenarios",
    )
    research_validate_backtester.add_argument(
        "--config", type=str, default=None, help="Optional YAML or JSON config file for backtester validation."
    )
    research_validate_backtester.add_argument(
        "--output-dir",
        type=str,
        default="artifacts/validation/vectorbt",
        help="Directory where vectorbt comparison artifacts will be written.",
    )
    research_validate_backtester.set_defaults(func=cmd_research_validate_backtester)
    research_loop = research_subparsers.add_parser("loop", help="Experimental: run the automated alpha research loop")
    _add_alpha_loop_arguments(research_loop)
    research_loop.set_defaults(func=cmd_alpha_research_loop)
    research_multi = research_subparsers.add_parser(
        "multi-universe", help="Experimental: run alpha research across multiple named universes"
    )
    _add_multi_universe_arguments(research_multi)
    research_multi.set_defaults(func=cmd_multi_universe_alpha_research)
    research_multi_report = research_subparsers.add_parser(
        "multi-universe-report", help="Experimental: build a cross-universe comparison report from existing outputs"
    )
    research_multi_report.add_argument(
        "--output-dir",
        type=str,
        default="artifacts/multi_universe_alpha_research",
        help="Directory containing per-universe outputs and the comparison artifacts.",
    )
    research_multi_report.set_defaults(func=cmd_multi_universe_report)
    research_refresh = research_subparsers.add_parser(
        "refresh", help="Experimental: run the scheduled alpha discovery refresh workflow"
    )
    _add_refresh_arguments(research_refresh)
    research_refresh.set_defaults(func=cmd_research_refresh)
    research_monitor = research_subparsers.add_parser(
        "monitor",
        help="Experimental: generate monitoring reports and drift alerts from recent research and paper artifacts",
    )
    research_monitor.add_argument(
        "--tracker-dir",
        type=str,
        default="artifacts/experiment_tracking",
        help="Directory containing the shared experiment registry.",
    )
    research_monitor.add_argument(
        "--output-dir",
        type=str,
        default="artifacts/research_monitoring",
        help="Directory where monitoring artifacts will be written.",
    )
    research_monitor.add_argument(
        "--snapshot-dir",
        type=str,
        default="artifacts/research_refresh/approved_configuration_snapshots",
        help="Directory containing approved configuration snapshots.",
    )
    research_monitor.add_argument(
        "--alpha-artifact-dir", type=str, default=None, help="Optional alpha artifact directory override."
    )
    research_monitor.add_argument(
        "--paper-artifact-dir", type=str, default=None, help="Optional paper artifact directory override."
    )
    research_monitor.add_argument(
        "--recent-paper-runs", type=int, default=10, help="Number of recent paper runs to use for realized diagnostics."
    )
    research_monitor.add_argument(
        "--performance-degradation-buffer",
        type=float,
        default=0.002,
        help="Absolute buffer before recent paper returns trigger a degradation alert.",
    )
    research_monitor.add_argument(
        "--turnover-spike-multiple",
        type=float,
        default=1.5,
        help="Multiple of expected turnover that triggers a turnover spike alert.",
    )
    research_monitor.add_argument(
        "--concentration-spike-multiple",
        type=float,
        default=1.5,
        help="Multiple of expected top-position weight that triggers a concentration alert.",
    )
    research_monitor.add_argument(
        "--signal-churn-threshold",
        type=int,
        default=3,
        help="Number of approved-signal additions/removals that triggers a churn alert.",
    )
    research_monitor.set_defaults(func=cmd_research_monitor)
    research_kalshi_alpha = research_subparsers.add_parser(
        "kalshi-alpha",
        help="Run alpha research on Kalshi prediction market feature data",
    )
    research_kalshi_alpha.add_argument(
        "--config",
        type=str,
        default="configs/kalshi_research.yaml",
        help="Path to YAML research config (default: configs/kalshi_research.yaml).",
    )
    research_kalshi_alpha.add_argument(
        "--feature-dir",
        type=str,
        default=None,
        help="Directory containing Kalshi feature parquets. Overrides config.",
    )
    research_kalshi_alpha.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Output artifact directory. Overrides config.",
    )
    research_kalshi_alpha.add_argument(
        "--resolution-data",
        type=str,
        default=None,
        help="CSV with ticker,resolution_price columns for backtesting.",
    )
    research_kalshi_alpha.add_argument(
        "--backtest",
        action="store_true",
        help="Run the binary market backtester after alpha research.",
    )
    research_kalshi_alpha.add_argument(
        "--forward-horizon",
        type=int,
        default=None,
        help="Bars ahead for forward IC calculation (default: 10).",
    )
    research_kalshi_alpha.add_argument(
        "--min-rows",
        type=int,
        default=None,
        help="Minimum rows required per market parquet (default: 30).",
    )
    research_kalshi_alpha.set_defaults(func=cmd_kalshi_alpha_research)

    research_kalshi_full_backtest = research_subparsers.add_parser(
        "kalshi-full-backtest",
        help="Run the resolved-market Kalshi backtest framework on local historical artifacts.",
    )
    research_kalshi_full_backtest.add_argument(
        "--config",
        type=str,
        default=None,
        help="Optional YAML config path.",
    )
    research_kalshi_full_backtest.add_argument(
        "--feature-dir",
        type=str,
        default=None,
        help="Directory containing Kalshi feature parquets (default: data/kalshi/features/real).",
    )
    research_kalshi_full_backtest.add_argument(
        "--resolution-data",
        type=str,
        default=None,
        help="Path to resolution CSV (default: data/kalshi/normalized/resolution.csv).",
    )
    research_kalshi_full_backtest.add_argument(
        "--raw-markets-dir",
        type=str,
        default=None,
        help="Directory containing raw historical market JSON files (default: data/kalshi/raw/markets).",
    )
    research_kalshi_full_backtest.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Output directory for backtest artifacts (default: artifacts/kalshi_research).",
    )
    research_kalshi_full_backtest.add_argument(
        "--entry-threshold",
        type=float,
        default=None,
        help="Minimum |signal| to enter a trade (default: 0.5).",
    )
    research_kalshi_full_backtest.add_argument(
        "--long-only",
        action="store_true",
        default=False,
        help="Only take YES positions (skip NO trades).",
    )
    research_kalshi_full_backtest.add_argument(
        "--entry-timing-mode",
        type=str,
        default=None,
        choices=["hours_before_close", "last_bar"],
        help="How to choose the entry bar: relative to close time or last available bar.",
    )
    research_kalshi_full_backtest.add_argument(
        "--entry-offset-hours",
        type=float,
        default=None,
        help="When entry-timing-mode=hours_before_close, enter this many hours before close.",
    )
    research_kalshi_full_backtest.add_argument(
        "--holding-window-hours",
        type=float,
        default=None,
        help="Optional holding window in hours; omit to hold to final resolution.",
    )
    research_kalshi_full_backtest.add_argument(
        "--entry-slippage-points",
        type=float,
        default=None,
        help="Entry slippage on the yes-price scale (0-100 points).",
    )
    research_kalshi_full_backtest.add_argument(
        "--exit-slippage-points",
        type=float,
        default=None,
        help="Exit slippage on the yes-price scale (0-100 points).",
    )
    research_kalshi_full_backtest.add_argument(
        "--signal-probability-scale",
        type=float,
        default=None,
        help="Scale used to map raw signal scores into predicted probabilities for calibration metrics.",
    )
    research_kalshi_full_backtest.add_argument(
        "--validation-summary",
        type=str,
        default=None,
        help="Optional Kalshi validation summary JSON path used when validation gating is enabled.",
    )
    research_kalshi_full_backtest.add_argument(
        "--require-validation-pass",
        action="store_true",
        default=False,
        help="Require a passing Kalshi dataset validation summary before running the backtest.",
    )
    research_kalshi_full_backtest.add_argument(
        "--include-polymarket",
        action="store_true",
        default=False,
        help=(
            "Also run the backtest on Polymarket feature parquets and write "
            "artifacts/kalshi_research/polymarket_backtest_results.csv."
        ),
    )
    research_kalshi_full_backtest.add_argument(
        "--polymarket-feature-dir",
        type=str,
        default=None,
        help="Directory containing Polymarket feature parquets (default: data/polymarket/features).",
    )
    research_kalshi_full_backtest.add_argument(
        "--polymarket-resolution-data",
        type=str,
        default=None,
        help="Path to Polymarket resolution CSV (default: data/polymarket/resolution.csv).",
    )
    research_kalshi_full_backtest.add_argument(
        "--include-manifold",
        action="store_true",
        default=False,
        help=(
            "Also run the backtest on Manifold Markets feature parquets "
            "(requires prior 'trading-cli data manifold parse')."
        ),
    )
    research_kalshi_full_backtest.add_argument(
        "--manifold-feature-dir",
        type=str,
        default=None,
        help="Directory containing Manifold feature parquets (default: data/manifold/features).",
    )
    research_kalshi_full_backtest.add_argument(
        "--manifold-resolution-data",
        type=str,
        default=None,
        help="Path to Manifold resolution CSV (default: data/manifold/resolution.csv).",
    )
    research_kalshi_full_backtest.add_argument(
        "--include-metaculus",
        action="store_true",
        default=False,
        help="Also run the backtest on Metaculus feature parquets.",
    )
    research_kalshi_full_backtest.add_argument(
        "--metaculus-feature-dir",
        type=str,
        default=None,
        help="Directory containing Metaculus feature parquets (default: data/metaculus/features).",
    )
    research_kalshi_full_backtest.add_argument(
        "--metaculus-resolution-data",
        type=str,
        default=None,
        help="Path to Metaculus resolution CSV (default: data/metaculus/resolution.csv).",
    )
    research_kalshi_full_backtest.set_defaults(func=cmd_kalshi_full_backtest)

    research_cross_market_monitor = research_subparsers.add_parser(
        "cross-market-monitor",
        help="Monitor Kalshi and Polymarket for conservative cross-market dislocations.",
    )
    research_cross_market_monitor.add_argument(
        "--config",
        type=str,
        default="configs/kalshi_research.yaml",
        help="Path to YAML research config (default: configs/kalshi_research.yaml).",
    )
    research_cross_market_monitor.add_argument(
        "--kalshi-config",
        type=str,
        default="configs/kalshi.yaml",
        help="Path to Kalshi trading config used for environment/auth selection.",
    )
    research_cross_market_monitor.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Optional override for the cross-market artifact directory.",
    )
    research_cross_market_monitor.add_argument(
        "--kalshi-max-markets",
        type=int,
        default=None,
        help="Optional cap on open Kalshi markets scanned.",
    )
    research_cross_market_monitor.add_argument(
        "--polymarket-max-markets",
        type=int,
        default=None,
        help="Optional cap on active Polymarket markets scanned.",
    )
    research_cross_market_monitor.add_argument(
        "--min-probability-spread",
        type=float,
        default=None,
        help="Minimum absolute YES probability spread required to log an opportunity.",
    )
    research_cross_market_monitor.add_argument(
        "--match-threshold",
        type=float,
        default=None,
        help="Minimum conservative match score required to accept a cross-venue pair.",
    )
    research_cross_market_monitor.add_argument(
        "--ambiguity-margin",
        type=float,
        default=None,
        help="Score margin under which a near-tied second candidate marks the match as ambiguous.",
    )
    research_cross_market_monitor.add_argument(
        "--max-expiration-diff-hours",
        type=float,
        default=None,
        help="Maximum expiry mismatch allowed between venues before rejecting a match.",
    )
    research_cross_market_monitor.add_argument(
        "--min-title-similarity",
        type=float,
        default=None,
        help="Minimum normalized title similarity required before accepting a match.",
    )
    research_cross_market_monitor.add_argument(
        "--min-token-overlap",
        type=float,
        default=None,
        help="Minimum token-overlap score required before accepting a match.",
    )
    research_cross_market_monitor.add_argument(
        "--snapshot-tag",
        type=str,
        default=None,
        help="Optional human-readable tag attached to this monitor snapshot.",
    )
    research_cross_market_monitor.add_argument(
        "--append-history",
        dest="append_history",
        action="store_true",
        default=None,
        help="Append matches and opportunities to existing JSONL logs.",
    )
    research_cross_market_monitor.add_argument(
        "--overwrite-history",
        dest="append_history",
        action="store_false",
        help="Overwrite JSONL logs for this run instead of appending.",
    )
    research_cross_market_monitor.set_defaults(func=cmd_cross_market_monitor)

    research_registry = research_subparsers.add_parser("registry", help="Research manifest registry commands")
    research_registry_subparsers = research_registry.add_subparsers(dest="research_registry_command", required=True)
    research_registry_build = research_registry_subparsers.add_parser(
        "build", help="Scan research run manifests and write normalized registry artifacts"
    )
    research_registry_build.add_argument(
        "--artifacts-root",
        type=str,
        default="artifacts",
        help="Root artifact directory to scan for research manifests.",
    )
    research_registry_build.add_argument(
        "--output-dir", type=str, required=True, help="Directory where registry artifacts will be written."
    )
    research_registry_build.set_defaults(func=cmd_research_registry_build)
    research_dataset_registry = research_subparsers.add_parser(
        "dataset-registry", help="Cross-provider research dataset registry commands"
    )
    research_dataset_registry_subparsers = research_dataset_registry.add_subparsers(
        dest="research_dataset_registry_command",
        required=True,
    )
    research_dataset_registry_publish = research_dataset_registry_subparsers.add_parser(
        "publish", help="Publish provider research datasets into the shared dataset registry"
    )
    research_dataset_registry_publish.add_argument(
        "--registry-path",
        type=str,
        default="data/research/dataset_registry.json",
        help="Path to the shared dataset registry JSON artifact.",
    )
    research_dataset_registry_publish.add_argument(
        "--kalshi-config",
        type=str,
        default="configs/kalshi.yaml",
        help="Kalshi config used to derive research dataset metadata.",
    )
    research_dataset_registry_publish.add_argument(
        "--polymarket-config",
        type=str,
        default="configs/polymarket.yaml",
        help="Polymarket config used to derive research dataset metadata.",
    )
    research_dataset_registry_publish.add_argument(
        "--providers",
        nargs="+",
        default=["kalshi", "polymarket"],
        choices=["kalshi", "polymarket"],
        help="Subset of provider publishers to run.",
    )
    research_dataset_registry_publish.add_argument(
        "--summary-path",
        type=str,
        default="artifacts/provider_monitoring/latest_registry_summary.json",
        help="Path for the registry publication summary artifact.",
    )
    research_dataset_registry_publish.set_defaults(func=cmd_research_dataset_registry_publish)
    research_dataset_registry_list = research_dataset_registry_subparsers.add_parser(
        "list", help="List shared research dataset registry entries"
    )
    research_dataset_registry_list.add_argument(
        "--registry-path",
        type=str,
        default="data/research/dataset_registry.json",
        help="Path to the shared dataset registry JSON artifact.",
    )
    research_dataset_registry_list.add_argument(
        "--provider", type=str, default=None, help="Optional provider filter."
    )
    research_dataset_registry_list.add_argument(
        "--asset-class", type=str, default=None, help="Optional asset-class filter."
    )
    research_dataset_registry_list.add_argument(
        "--dataset-name", type=str, default=None, help="Optional dataset-name filter."
    )
    research_dataset_registry_list.add_argument(
        "--format",
        type=str,
        default="text",
        choices=["text", "json"],
        help="Render output as human-readable text or JSON.",
    )
    research_dataset_registry_list.set_defaults(func=cmd_research_dataset_registry_list)
    research_replay = research_subparsers.add_parser(
        "replay", help="Shared registry-backed replay dataset assembly commands"
    )
    research_replay_subparsers = research_replay.add_subparsers(
        dest="research_replay_command",
        required=True,
    )
    research_replay_assemble = research_replay_subparsers.add_parser(
        "assemble", help="Assemble replay-ready datasets from shared registry readers"
    )
    research_replay_assemble.add_argument(
        "--registry-path",
        type=str,
        default="data/research/dataset_registry.json",
        help="Path to the shared dataset registry JSON artifact.",
    )
    research_replay_assemble.add_argument("--dataset-keys", nargs="+", default=None, help="Explicit dataset keys to assemble.")
    research_replay_assemble.add_argument("--providers", nargs="+", default=None, help="Optional provider filters.")
    research_replay_assemble.add_argument("--asset-class", type=str, default=None, help="Optional asset-class filter.")
    research_replay_assemble.add_argument("--dataset-names", nargs="+", default=None, help="Optional dataset-name filters.")
    research_replay_assemble.add_argument("--symbols", nargs="+", default=None, help="Optional symbol or market filters.")
    research_replay_assemble.add_argument("--intervals", nargs="+", default=None, help="Optional interval filters.")
    research_replay_assemble.add_argument("--start", type=str, default=None, help="Inclusive start timestamp filter.")
    research_replay_assemble.add_argument("--end", type=str, default=None, help="Inclusive end timestamp filter.")
    research_replay_assemble.add_argument(
        "--alignment-mode",
        type=str,
        choices=["outer_union", "anchor"],
        default="outer_union",
        help="Replay alignment mode.",
    )
    research_replay_assemble.add_argument(
        "--anchor-dataset-key",
        type=str,
        default=None,
        help="Anchor dataset key when using anchor alignment.",
    )
    research_replay_assemble.add_argument(
        "--tolerance",
        type=str,
        default=None,
        help="Optional backward-asof tolerance for anchor alignment, e.g. 5m.",
    )
    research_replay_assemble.add_argument(
        "--output-path",
        type=str,
        default=None,
        help="Optional parquet output path for the assembled replay dataset.",
    )
    research_replay_assemble.add_argument(
        "--summary-path",
        type=str,
        default="artifacts/research_replay/latest_replay_assembly_summary.json",
        help="Summary artifact path when output is written.",
    )
    research_replay_assemble.add_argument(
        "--format",
        type=str,
        default="text",
        choices=["text", "json"],
        help="Render output as human-readable text or JSON.",
    )
    research_replay_assemble.set_defaults(func=cmd_research_replay_assemble)
    research_replay_consume = research_replay_subparsers.add_parser(
        "consume", help="Load replay assemblies as evaluation-ready research inputs"
    )
    research_replay_consume.add_argument(
        "--registry-path",
        type=str,
        default="data/research/dataset_registry.json",
        help="Path to the shared dataset registry JSON artifact.",
    )
    research_replay_consume.add_argument("--dataset-keys", nargs="+", default=None, help="Explicit dataset keys to assemble.")
    research_replay_consume.add_argument("--providers", nargs="+", default=None, help="Optional provider filters.")
    research_replay_consume.add_argument("--asset-class", type=str, default=None, help="Optional asset-class filter.")
    research_replay_consume.add_argument("--dataset-names", nargs="+", default=None, help="Optional dataset-name filters.")
    research_replay_consume.add_argument("--symbols", nargs="+", default=None, help="Optional symbol or market filters.")
    research_replay_consume.add_argument("--intervals", nargs="+", default=None, help="Optional interval filters.")
    research_replay_consume.add_argument("--start", type=str, default=None, help="Inclusive start timestamp filter.")
    research_replay_consume.add_argument("--end", type=str, default=None, help="Inclusive end timestamp filter.")
    research_replay_consume.add_argument(
        "--alignment-mode",
        type=str,
        choices=["outer_union", "anchor"],
        default="outer_union",
        help="Replay alignment mode.",
    )
    research_replay_consume.add_argument(
        "--anchor-dataset-key",
        type=str,
        default=None,
        help="Anchor dataset key when using anchor alignment.",
    )
    research_replay_consume.add_argument(
        "--tolerance",
        type=str,
        default=None,
        help="Optional backward-asof tolerance for anchor alignment, e.g. 5m.",
    )
    research_replay_consume.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional row limit applied after replay consumption.",
    )
    research_replay_consume.add_argument(
        "--summary-path",
        type=str,
        default="artifacts/research_replay/latest_replay_consumer_summary.json",
        help="Summary artifact path for the replay consumer payload.",
    )
    research_replay_consume.add_argument(
        "--format",
        type=str,
        default="text",
        choices=["text", "json"],
        help="Render output as human-readable text or JSON.",
    )
    research_replay_consume.set_defaults(func=cmd_research_replay_consume)
    research_replay_evaluate = research_replay_subparsers.add_parser(
        "evaluate", help="Evaluate replay-consumer inputs with simple registry-backed metrics"
    )
    research_replay_evaluate.add_argument(
        "--registry-path",
        type=str,
        default="data/research/dataset_registry.json",
        help="Path to the shared dataset registry JSON artifact.",
    )
    research_replay_evaluate.add_argument("--dataset-keys", nargs="+", default=None, help="Explicit dataset keys to assemble.")
    research_replay_evaluate.add_argument("--providers", nargs="+", default=None, help="Optional provider filters.")
    research_replay_evaluate.add_argument("--dataset-names", nargs="+", default=None, help="Optional dataset-name filters.")
    research_replay_evaluate.add_argument("--symbols", nargs="+", default=None, help="Optional symbol or market filters.")
    research_replay_evaluate.add_argument("--intervals", nargs="+", default=None, help="Optional interval filters.")
    research_replay_evaluate.add_argument("--start", type=str, default=None, help="Inclusive start timestamp filter.")
    research_replay_evaluate.add_argument("--end", type=str, default=None, help="Inclusive end timestamp filter.")
    research_replay_evaluate.add_argument(
        "--alignment-mode",
        type=str,
        choices=["outer_union", "anchor"],
        default="outer_union",
        help="Replay alignment mode.",
    )
    research_replay_evaluate.add_argument(
        "--anchor-dataset-key",
        type=str,
        default=None,
        help="Anchor dataset key when using anchor alignment.",
    )
    research_replay_evaluate.add_argument(
        "--tolerance",
        type=str,
        default=None,
        help="Optional backward-asof tolerance for anchor alignment, e.g. 5m.",
    )
    research_replay_evaluate.add_argument("--feature-columns", nargs="+", default=None, help="Optional explicit feature columns.")
    research_replay_evaluate.add_argument("--target-columns", nargs="+", default=None, help="Optional explicit target columns.")
    research_replay_evaluate.add_argument("--limit", type=int, default=None, help="Optional row limit.")
    research_replay_evaluate.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Optional directory where evaluation artifacts will be written.",
    )
    research_replay_evaluate.add_argument(
        "--format",
        type=str,
        default="text",
        choices=["text", "json"],
        help="Render output as human-readable text or JSON.",
    )
    research_replay_evaluate.set_defaults(func=cmd_research_replay_evaluate)
    research_leaderboard = research_subparsers.add_parser(
        "leaderboard", help="Build a cross-run research leaderboard from manifest summaries"
    )
    research_leaderboard.add_argument(
        "--artifacts-root",
        type=str,
        default="artifacts",
        help="Root artifact directory to scan for research manifests.",
    )
    research_leaderboard.add_argument(
        "--output-dir", type=str, required=True, help="Directory where leaderboard artifacts will be written."
    )
    research_leaderboard.add_argument(
        "--metric", type=str, default="portfolio_sharpe", help="Top-metric field used for ranking."
    )
    research_leaderboard.add_argument(
        "--group-by",
        type=str,
        default="none",
        choices=["none", "signal_family", "universe", "workflow_type"],
        help="Optional grouping applied before ranking.",
    )
    research_leaderboard.add_argument(
        "--limit", type=int, default=20, help="Maximum number of leaderboard rows to write."
    )
    research_leaderboard.set_defaults(func=cmd_research_leaderboard)
    research_compare_runs = research_subparsers.add_parser("compare-runs", help="Compare two research runs by run_id")
    research_compare_runs.add_argument(
        "--artifacts-root",
        type=str,
        default="artifacts",
        help="Root artifact directory to scan for research manifests.",
    )
    research_compare_runs.add_argument("--run-id-a", type=str, required=True, help="Baseline run_id.")
    research_compare_runs.add_argument("--run-id-b", type=str, required=True, help="Candidate run_id.")
    research_compare_runs.add_argument(
        "--output-dir", type=str, required=True, help="Directory where comparison artifacts will be written."
    )
    research_compare_runs.set_defaults(func=cmd_research_compare_runs)
    research_promotion_candidates = research_subparsers.add_parser(
        "promotion-candidates", help="Inspect promotion readiness from research manifests without generating strategies"
    )
    research_promotion_candidates.add_argument(
        "--artifacts-root",
        type=str,
        default="artifacts",
        help="Root artifact directory to scan for research manifests.",
    )
    research_promotion_candidates.add_argument(
        "--output-dir", type=str, required=True, help="Directory where promotion-candidate artifacts will be written."
    )
    research_promotion_candidates.set_defaults(func=cmd_research_promotion_candidates)
    research_promote = research_subparsers.add_parser(
        "promote", help="Canonical step 3: refresh promotion inputs and generate promoted strategy presets/configs"
    )
    research_promote.add_argument(
        "--artifacts-root",
        type=str,
        default="artifacts",
        help="Root artifact directory to scan for research manifests.",
    )
    research_promote.add_argument(
        "--run-dir",
        type=str,
        default=None,
        help="Optional research run directory to promote from. Defaults to the latest run under --artifacts-root when using run-local promotion.",
    )
    research_promote.add_argument(
        "--registry-scope",
        type=str,
        default="run_local",
        choices=["run_local", "global"],
        help="Promotion source scope. Defaults to run-local registry resolution to avoid mixing stale historical runs.",
    )
    research_promote.add_argument(
        "--use-global-registry", action="store_true", help="Legacy convenience flag that sets --registry-scope global."
    )
    research_promote.add_argument(
        "--registry-dir",
        type=str,
        default=None,
        help="Optional directory where refreshed research registry and promotion-candidate artifacts will be written. Defaults to <run-dir>/research_registry for run-local scope or <artifacts-root>/research_registry for global scope.",
    )
    research_promote.add_argument(
        "--output-dir",
        type=str,
        required=True,
        help="Directory where generated strategy presets and config bundles will be written.",
    )
    research_promote.add_argument(
        "--policy-config", type=str, default=None, help="Optional promotion policy JSON/YAML file."
    )
    research_promote.add_argument(
        "--validation",
        type=str,
        default=None,
        help="Optional strategy_validation.json path or directory used to gate promotion.",
    )
    research_promote.add_argument(
        "--top-n", type=int, default=None, help="Optional maximum number of strategies to promote this run."
    )
    research_promote.add_argument(
        "--allow-overwrite", action="store_true", help="Allow replacing existing generated preset/config artifacts."
    )
    research_promote.add_argument(
        "--dry-run", action="store_true", help="Evaluate and print promotions without writing artifacts."
    )
    research_promote.add_argument(
        "--inactive",
        action="store_true",
        help="Write promoted strategies with inactive status regardless of policy default.",
    )
    research_promote.add_argument(
        "--override-validation", action="store_true", help="Allow promotion even when validation does not pass."
    )
    add_database_tracking_arguments(research_promote, include_write_toggles=True)
    research_promote.set_defaults(func=cmd_research_promote)
    research_db = research_subparsers.add_parser(
        "db", help="Initialize and query the optional research metadata database."
    )
    add_database_tracking_arguments(research_db, include_write_toggles=True)
    research_db_subparsers = research_db.add_subparsers(dest="research_db_command", required=True)
    research_db_init = research_db_subparsers.add_parser(
        "init", help="Create the research metadata schema and tables if they do not exist."
    )
    add_database_tracking_arguments(research_db_init, include_write_toggles=True)
    research_db_init.set_defaults(func=cmd_research_db_init)
    research_db_list_runs = research_db_subparsers.add_parser(
        "list-runs", help="List recent research runs stored in the metadata database."
    )
    add_database_tracking_arguments(research_db_list_runs, include_write_toggles=True)
    research_db_list_runs.add_argument("--limit", type=int, default=20, help="Maximum number of runs to list.")
    research_db_list_runs.set_defaults(func=cmd_research_db_list_runs)
    research_db_top_candidates = research_db_subparsers.add_parser(
        "top-candidates", help="Show the highest-ranked candidates across stored research runs."
    )
    add_database_tracking_arguments(research_db_top_candidates, include_write_toggles=True)
    research_db_top_candidates.add_argument(
        "--metric",
        type=str,
        default="mean_spearman_ic",
        choices=[
            "mean_pearson_ic",
            "mean_spearman_ic",
            "mean_hit_rate",
            "mean_long_short_spread",
            "mean_quantile_spread",
        ],
        help="Metric column used for cross-run candidate ranking.",
    )
    research_db_top_candidates.add_argument(
        "--limit", type=int, default=20, help="Maximum number of candidates to list."
    )
    research_db_top_candidates.set_defaults(func=cmd_research_db_top_candidates)
    research_db_family_summary = research_db_subparsers.add_parser(
        "family-summary", help="Summarize cross-run metrics and promotions by signal family."
    )
    add_database_tracking_arguments(research_db_family_summary, include_write_toggles=True)
    research_db_family_summary.set_defaults(func=cmd_research_db_family_summary)
    research_db_promotions = research_db_subparsers.add_parser(
        "promotions", help="List recently persisted promoted strategies."
    )
    add_database_tracking_arguments(research_db_promotions, include_write_toggles=True)
    research_db_promotions.add_argument(
        "--limit", type=int, default=20, help="Maximum number of promotion rows to list."
    )
    research_db_promotions.set_defaults(func=cmd_research_db_promotions)
    research_strategies = research_subparsers.add_parser("strategies", help="Show available legacy strategies")
    research_strategies.set_defaults(func=cmd_list_strategies)

    portfolio_parser = subparsers.add_parser(
        "portfolio", help="Portfolio backtests and ranking-based portfolio construction"
    )
    portfolio_subparsers = portfolio_parser.add_subparsers(dest="portfolio_command", required=True)
    portfolio_backtest = portfolio_subparsers.add_parser(
        "backtest", help="Run an equal-weight portfolio backtest across multiple symbols"
    )
    add_shared_symbol_args(portfolio_backtest)
    add_strategy_arguments(portfolio_backtest)
    add_execution_arguments(portfolio_backtest)
    portfolio_backtest.add_argument(
        "--output-dir", type=str, default="data/experiments/portfolio", help="Directory for portfolio outputs"
    )
    portfolio_backtest.set_defaults(func=cmd_portfolio)
    portfolio_topn = portfolio_subparsers.add_parser("topn", help="Run a top-N cross-sectional portfolio backtest")
    add_shared_symbol_args(portfolio_topn)
    add_strategy_arguments(portfolio_topn)
    add_execution_arguments(portfolio_topn)
    _add_common_portfolio_selection_arguments(portfolio_topn, required_top_n=True)
    portfolio_topn.add_argument(
        "--output-dir", type=str, default="data/experiments/portfolio_topn", help="Directory for portfolio outputs"
    )
    portfolio_topn.set_defaults(func=cmd_portfolio_topn)
    portfolio_multi = portfolio_subparsers.add_parser(
        "allocate-multi-strategy", help="Allocate multiple approved deploy sleeves into one combined target portfolio"
    )
    portfolio_multi.add_argument(
        "--config", type=str, required=True, help="Path to the multi-strategy portfolio YAML/JSON config."
    )
    portfolio_multi.add_argument(
        "--output-dir", type=str, required=True, help="Directory where allocation artifacts will be written."
    )
    portfolio_multi.set_defaults(func=cmd_portfolio_allocate_multi_strategy)
    portfolio_exec = portfolio_subparsers.add_parser(
        "apply-execution-constraints", help="Apply execution realism constraints to an allocation artifact directory"
    )
    portfolio_exec.add_argument(
        "--config", type=str, required=True, help="Path to the execution-config JSON/YAML file."
    )
    portfolio_exec.add_argument(
        "--allocation-dir", type=str, required=True, help="Directory containing allocation artifacts."
    )
    portfolio_exec.add_argument(
        "--output-dir", type=str, required=True, help="Directory where execution realism artifacts will be written."
    )
    portfolio_exec.set_defaults(func=cmd_portfolio_apply_execution_constraints)
    portfolio_optimize = portfolio_subparsers.add_parser(
        "optimize-research", help="Run optional allocator research experiments behind a thin PyPortfolioOpt adapter"
    )
    portfolio_optimize.add_argument(
        "--config", type=str, default=None, help="Optional YAML or JSON config file for optimizer experiments."
    )
    portfolio_optimize.add_argument(
        "--returns-path", type=str, default=None, help="CSV file containing asset return history with symbol columns."
    )
    portfolio_optimize.add_argument(
        "--output-dir", type=str, default=None, help="Directory where optimizer artifacts will be written."
    )
    portfolio_optimize.add_argument(
        "--optimizer-name",
        type=str,
        default="equal_weight",
        choices=["equal_weight", "metric_weighted", "min_vol", "max_sharpe", "hrp"],
        help="Allocator experiment to run.",
    )
    portfolio_optimize.add_argument(
        "--fallback-optimizer-name",
        type=str,
        default="equal_weight",
        choices=["equal_weight", "metric_weighted"],
        help="Fallback allocator when optimization fails or is unavailable.",
    )
    portfolio_optimize.add_argument(
        "--risk-free-rate", type=float, default=0.0, help="Risk-free rate used by max_sharpe optimization."
    )
    portfolio_optimize.add_argument(
        "--min-history-rows", type=int, default=20, help="Minimum rows required before attempting optimization."
    )
    portfolio_optimize.set_defaults(func=cmd_portfolio_optimize_research)

    paper_parser = subparsers.add_parser("paper", help="Paper trading workflows")
    paper_subparsers = paper_parser.add_subparsers(dest="paper_command", required=True)
    paper_run = paper_subparsers.add_parser(
        "run", help="Canonical step 5: run one paper-trading cycle and write state/artifacts"
    )
    _add_paper_run_arguments(paper_run)
    paper_run.add_argument(
        "--config", type=str, default=None, help="Optional YAML or JSON config file for canonical paper runs."
    )
    paper_run.set_defaults(func=cmd_paper_run)
    paper_multi = paper_subparsers.add_parser(
        "run-multi-strategy", help="Run one paper-trading cycle from a combined multi-strategy allocation config"
    )
    paper_multi.add_argument(
        "--config", type=str, required=True, help="Path to the multi-strategy portfolio YAML/JSON config."
    )
    paper_multi.add_argument(
        "--execution-config", type=str, default=None, help="Optional execution realism JSON/YAML config."
    )
    paper_multi.add_argument(
        "--state-path", type=str, required=True, help="JSON file used to persist paper portfolio state"
    )
    paper_multi.add_argument(
        "--output-dir", type=str, required=True, help="Directory for paper-run and allocation artifacts"
    )
    paper_multi.add_argument(
        "--auto-apply-fills",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Apply simulated fills and persist updated multi-strategy paper positions/cash. Use --no-auto-apply-fills to keep the older order-only behavior.",
    )
    paper_multi.set_defaults(func=cmd_paper_run_multi_strategy)
    paper_replay_multi = paper_subparsers.add_parser(
        "replay-multi-strategy",
        help="Replay multi-strategy paper trading across multiple dates using a persistent paper state",
    )
    paper_replay_multi.add_argument(
        "--config", type=str, required=True, help="Path to the multi-strategy portfolio YAML/JSON config."
    )
    paper_replay_multi.add_argument(
        "--execution-config", type=str, default=None, help="Optional execution realism JSON/YAML config."
    )
    paper_replay_multi.add_argument(
        "--state-path", type=str, required=True, help="JSON file used to persist rolling paper portfolio state"
    )
    paper_replay_multi.add_argument(
        "--output-dir", type=str, required=True, help="Directory for replay artifacts and rolling ledgers"
    )
    paper_replay_multi.add_argument(
        "--start-date", type=str, default=None, help="Replay start date in YYYY-MM-DD format."
    )
    paper_replay_multi.add_argument("--end-date", type=str, default=None, help="Replay end date in YYYY-MM-DD format.")
    paper_replay_multi.add_argument(
        "--dates", nargs="+", default=None, help="Optional explicit replay date list in YYYY-MM-DD format."
    )
    paper_replay_multi.add_argument(
        "--max-steps", type=int, default=None, help="Optional maximum number of replay steps to process."
    )
    paper_replay_multi.add_argument(
        "--reset-state",
        action="store_true",
        help="Delete the existing replay state file and replay output directory before running.",
    )
    paper_replay_multi.add_argument(
        "--auto-apply-fills",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Apply simulated fills and persist updated multi-strategy paper positions/cash during replay. Use --no-auto-apply-fills to keep the older order-only behavior.",
    )
    paper_replay_multi.set_defaults(func=cmd_paper_replay_multi_strategy)
    paper_run_scheduled = paper_subparsers.add_parser(
        "run-preset-scheduled", help="Task-Scheduler-friendly wrapper around paper run for versioned presets"
    )
    _add_paper_run_arguments(paper_run_scheduled)
    paper_run_scheduled.add_argument(
        "--config", type=str, default=None, help="Optional YAML or JSON config file for scheduled paper runs."
    )
    paper_run_scheduled.set_defaults(func=cmd_paper_run_scheduled)
    paper_schedule = paper_subparsers.add_parser(
        "schedule", help="Canonical step 7: run the preset-driven scheduled paper workflow"
    )
    _add_paper_run_arguments(paper_schedule)
    paper_schedule.add_argument(
        "--config", type=str, default=None, help="Optional YAML or JSON config file for scheduled paper runs."
    )
    paper_schedule.set_defaults(func=cmd_paper_run_scheduled)

    paper_daily = paper_subparsers.add_parser("daily", help="Run the daily paper trading workflow")
    add_composite_paper_arguments(paper_daily)
    paper_daily.add_argument("--strategy", default="sma_cross", help="Signal strategy name.")
    paper_daily.add_argument("--fast", type=int, default=None, help="Fast lookback parameter for the signal.")
    paper_daily.add_argument("--slow", type=int, default=None, help="Slow lookback parameter for the signal.")
    paper_daily.add_argument("--lookback", type=int, default=None, help="Lookback parameter for the signal.")
    paper_daily.add_argument("--top-n", type=int, default=10, help="Number of symbols to select.")
    paper_daily.add_argument("--weighting-scheme", default="equal", help="Weighting scheme for portfolio construction.")
    paper_daily.add_argument(
        "--vol-window", type=int, default=20, help="Volatility lookback window for inverse-vol weighting."
    )
    paper_daily.add_argument(
        "--min-score", type=float, default=None, help="Minimum score threshold for portfolio inclusion."
    )
    paper_daily.add_argument("--max-weight", type=float, default=None, help="Maximum position weight.")
    paper_daily.add_argument("--max-names-per-group", type=int, default=None, help="Maximum number of names per group.")
    paper_daily.add_argument("--max-group-weight", type=float, default=None, help="Maximum aggregate weight per group.")
    paper_daily.add_argument("--group-map-path", default=None, help="Optional path to symbol-to-group mapping file.")
    paper_daily.add_argument("--rebalance-frequency", default="daily", help="Rebalance frequency.")
    paper_daily.add_argument("--timing", default="next_bar", help="Execution timing policy.")
    paper_daily.add_argument(
        "--initial-cash", type=float, default=100_000.0, help="Initial paper trading cash balance."
    )
    paper_daily.add_argument("--min-trade-dollars", type=float, default=25.0, help="Minimum trade notional.")
    paper_daily.add_argument("--lot-size", type=int, default=1, help="Trading lot size.")
    paper_daily.add_argument(
        "--reserve-cash-pct", type=float, default=0.0, help="Fraction of equity to keep in cash reserve."
    )
    paper_daily.add_argument("--state-path", required=True, help="Path to the paper trading state file.")
    paper_daily.add_argument("--output-dir", required=True, help="Directory for paper trading artifacts.")
    paper_daily.add_argument(
        "--auto-apply-fills", action="store_true", help="Apply simulated fills through the paper broker."
    )
    paper_daily.add_argument(
        "--symbols", nargs="+", default=None, help="Symbols to include in the daily paper trading job."
    )
    paper_daily.add_argument("--universe", default=None, help="Named universe to trade instead of passing --symbols.")
    paper_daily.set_defaults(func=cmd_daily_paper_job)

    paper_report = paper_subparsers.add_parser("report", help="Build a summary report from paper trading ledgers")
    paper_report.add_argument("--account-dir", required=True, help="Base paper account directory containing ledgers/.")
    paper_report.add_argument("--output-dir", default=None, help="Optional directory to write report artifacts.")
    paper_report.add_argument(
        "--quantstats-output-dir",
        default=None,
        help="Optional directory where QuantStats report artifacts should be written.",
    )
    paper_report.set_defaults(func=cmd_paper_report)
    paper_kalshi_run = paper_subparsers.add_parser(
        "kalshi-run",
        help="Run the Kalshi paper trading loop using live market snapshots and paper-only execution.",
    )
    paper_kalshi_run.add_argument(
        "--config",
        type=str,
        default="configs/kalshi.yaml",
        help="Path to Kalshi trading config (default: configs/kalshi.yaml).",
    )
    paper_kalshi_run.add_argument(
        "--research-config",
        type=str,
        default="configs/kalshi_research.yaml",
        help="Path to Kalshi research config (default: configs/kalshi_research.yaml).",
    )
    paper_kalshi_run.add_argument(
        "--state-path",
        type=str,
        default=None,
        help="Optional override for the persistent Kalshi paper state JSON path.",
    )
    paper_kalshi_run.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Optional override for the Kalshi paper artifact directory.",
    )
    paper_kalshi_run.add_argument(
        "--tracked-series",
        nargs="+",
        default=None,
        help="Optional tracked series override. Defaults to configs/kalshi.yaml ingestion.tracked_series.",
    )
    paper_kalshi_run.add_argument(
        "--tracked-tickers",
        nargs="+",
        default=None,
        help="Optional tracked ticker override. Defaults to configs/kalshi.yaml ingestion.tracked_tickers.",
    )
    paper_kalshi_run.add_argument(
        "--entry-threshold",
        type=float,
        default=None,
        help="Optional override for the minimum absolute signal value required to enter a paper trade.",
    )
    paper_kalshi_run.add_argument(
        "--orderbook-depth",
        type=int,
        default=None,
        help="Optional override for Kalshi orderbook depth used by the liquidity model.",
    )
    paper_kalshi_run.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=None,
        help="Optional override for the loop sleep interval between Kalshi paper sessions.",
    )
    paper_kalshi_run.add_argument(
        "--max-iterations",
        type=int,
        default=None,
        help="Optional override for the number of consecutive Kalshi paper sessions to run.",
    )
    paper_kalshi_run.add_argument(
        "--validation-summary",
        type=str,
        default=None,
        help="Optional Kalshi validation summary JSON path used when validation gating is enabled.",
    )
    paper_kalshi_run.add_argument(
        "--require-validation-pass",
        action="store_true",
        default=False,
        help="Require a passing Kalshi dataset validation summary before running paper trading.",
    )
    paper_kalshi_run.set_defaults(func=cmd_kalshi_paper_run)

    live_parser = subparsers.add_parser("live", help="Broker preview, validation, and guarded execution commands")
    live_subparsers = live_parser.add_subparsers(dest="live_command", required=True)
    live_dry_run = live_subparsers.add_parser(
        "dry-run", help="Canonical step 6: compute live broker rebalance orders without sending them"
    )
    _add_live_base_arguments(live_dry_run)
    add_preset_argument(
        live_dry_run,
        help_text="Optional versioned preset for validated live dry-run defaults. Explicit CLI flags still override preset values.",
    )
    add_xsec_live_arguments(live_dry_run)
    live_dry_run.add_argument(
        "--config", type=str, default=None, help="Optional YAML or JSON config file for canonical live dry-runs."
    )
    live_dry_run.add_argument(
        "--output-dir", default="artifacts/live_dry_run", help="Directory for live dry-run preview artifacts."
    )
    live_dry_run.set_defaults(func=cmd_live_dry_run)
    live_multi = live_subparsers.add_parser(
        "dry-run-multi-strategy",
        help="Compute live broker rebalance preview orders from a combined multi-strategy allocation config",
    )
    live_multi.add_argument(
        "--config", type=str, required=True, help="Path to the multi-strategy portfolio YAML/JSON config."
    )
    live_multi.add_argument(
        "--execution-config", type=str, default=None, help="Optional execution realism JSON/YAML config."
    )
    live_multi.add_argument(
        "--broker", type=str, default="mock", choices=["mock", "alpaca"], help="Broker backend to preview against."
    )
    live_multi.add_argument(
        "--output-dir", type=str, required=True, help="Directory for live dry-run and allocation artifacts."
    )
    live_multi.set_defaults(func=cmd_live_dry_run_multi_strategy)
    live_run_scheduled = live_subparsers.add_parser(
        "run-preset-scheduled", help="Task-Scheduler-friendly wrapper around live dry-run for versioned presets"
    )
    _add_live_base_arguments(live_run_scheduled)
    add_preset_argument(live_run_scheduled, help_text="Versioned preset used for the scheduled live dry-run workflow.")
    add_xsec_live_arguments(live_run_scheduled)
    live_run_scheduled.add_argument(
        "--config", type=str, default=None, help="Optional YAML or JSON config file for scheduled live dry-runs."
    )
    live_run_scheduled.add_argument(
        "--output-dir", default="artifacts/live_dry_run", help="Directory for scheduled live dry-run artifacts."
    )
    live_run_scheduled.set_defaults(func=cmd_live_run_scheduled)
    live_schedule = live_subparsers.add_parser(
        "schedule", help="Canonical step 8: run the preset-driven live dry-run on a schedule"
    )
    _add_live_base_arguments(live_schedule)
    add_preset_argument(live_schedule, help_text="Versioned preset used for the scheduled live dry-run workflow.")
    add_xsec_live_arguments(live_schedule)
    live_schedule.add_argument(
        "--config", type=str, default=None, help="Optional YAML or JSON config file for scheduled live dry-runs."
    )
    live_schedule.add_argument(
        "--output-dir", default="artifacts/live_dry_run", help="Directory for scheduled live dry-run artifacts."
    )
    live_schedule.set_defaults(func=cmd_live_run_scheduled)
    live_submit = live_subparsers.add_parser(
        "submit", help="Run live preview plus strict pre-trade checks and optionally submit orders"
    )
    _add_live_base_arguments(live_submit)
    add_preset_argument(
        live_submit,
        help_text="Optional versioned preset for validated live defaults. Explicit CLI flags still override preset values.",
    )
    add_xsec_live_arguments(live_submit)
    live_submit.add_argument(
        "--validate-only", action="store_true", help="Write the exact submission package without sending orders."
    )
    live_submit.add_argument(
        "--output-dir", default="artifacts/live_execution", help="Directory for live submit artifacts."
    )
    live_submit.set_defaults(func=cmd_live_submit)
    live_submit_multi = live_subparsers.add_parser(
        "submit-multi-strategy",
        help="Run multi-strategy live preview plus strict pre-trade checks and optionally submit orders",
    )
    live_submit_multi.add_argument(
        "--config", type=str, required=True, help="Path to the multi-strategy portfolio YAML/JSON config."
    )
    live_submit_multi.add_argument(
        "--execution-config", type=str, default=None, help="Optional execution realism JSON/YAML config."
    )
    live_submit_multi.add_argument(
        "--broker-config", type=str, required=True, help="Path to the broker-config JSON/YAML file."
    )
    live_submit_multi.add_argument(
        "--broker", type=str, default=None, choices=["mock", "alpaca"], help="Optional broker override."
    )
    live_submit_multi.add_argument(
        "--validate-only", action="store_true", help="Write the exact submission package without sending orders."
    )
    live_submit_multi.add_argument(
        "--output-dir", type=str, required=True, help="Directory for live submit and allocation artifacts."
    )
    live_submit_multi.set_defaults(func=cmd_live_submit_multi_strategy)
    live_validate = live_subparsers.add_parser(
        "validate", help="Run live execution control checks and write artifacts without submitting orders"
    )
    _add_live_base_arguments(live_validate)
    add_live_control_arguments(live_validate)
    live_validate.set_defaults(func=cmd_validate_live)
    live_execute = live_subparsers.add_parser(
        "execute", help="Run live execution control checks and only submit orders if approved and safe"
    )
    _add_live_base_arguments(live_execute)
    add_live_control_arguments(live_execute)
    live_execute.set_defaults(func=cmd_execute_live)

    ops_parser = subparsers.add_parser(
        "ops", help="Operational controls: doctor, deploy pipeline, monitoring, registry, broker, and execution tooling"
    )
    ops_subparsers = ops_parser.add_subparsers(dest="ops_command", required=True)

    ops_doctor = ops_subparsers.add_parser("doctor", help="Run local environment, config, and artifact sanity checks")
    ops_doctor.add_argument("--artifacts-root", type=str, default="artifacts", help="Artifact root to inspect.")
    ops_doctor.add_argument("--pipeline-config", type=str, default=None, help="Optional pipeline config to validate.")
    ops_doctor.add_argument(
        "--monitoring-config", type=str, default=None, help="Optional monitoring config to validate."
    )
    ops_doctor.add_argument(
        "--notification-config", type=str, default=None, help="Optional notification config to validate."
    )
    ops_doctor.add_argument("--execution-config", type=str, default=None, help="Optional execution config to validate.")
    ops_doctor.add_argument("--broker-config", type=str, default=None, help="Optional broker config to validate.")
    ops_doctor.add_argument("--dashboard-config", type=str, default=None, help="Optional dashboard config to validate.")
    ops_doctor.add_argument(
        "--check-integrations",
        action="store_true",
        help="Check optional external integration packages, example configs, and validation artifacts.",
    )
    ops_doctor.add_argument(
        "--output-dir",
        type=str,
        default="artifacts/system_check",
        help="Directory where doctor artifacts will be written.",
    )
    ops_doctor.set_defaults(func=cmd_doctor)

    ops_pipeline = ops_subparsers.add_parser("pipeline", help="Run the canonical deploy pipeline and schedule wrappers")
    ops_pipeline_subparsers = ops_pipeline.add_subparsers(dest="ops_pipeline_command", required=True)
    ops_pipeline_run = ops_pipeline_subparsers.add_parser(
        "run", help="Run the orchestration pipeline exactly as specified in the config"
    )
    ops_pipeline_run.add_argument("--config", type=str, required=True, help="Path to the pipeline JSON/YAML config.")
    ops_pipeline_run.set_defaults(func=cmd_pipeline_run)
    ops_pipeline_daily = ops_pipeline_subparsers.add_parser(
        "run-daily", help="Run a daily pipeline config and validate schedule_type=daily"
    )
    ops_pipeline_daily.add_argument("--config", type=str, required=True, help="Path to the pipeline JSON/YAML config.")
    ops_pipeline_daily.set_defaults(func=cmd_pipeline_run_daily)
    ops_pipeline_weekly = ops_pipeline_subparsers.add_parser(
        "run-weekly", help="Run a weekly pipeline config and validate schedule_type=weekly"
    )
    ops_pipeline_weekly.add_argument("--config", type=str, required=True, help="Path to the pipeline JSON/YAML config.")
    ops_pipeline_weekly.set_defaults(func=cmd_pipeline_run_weekly)
    ops_pipeline_alpha_cycle = ops_pipeline_subparsers.add_parser(
        "alpha-cycle", help="Run the one-command research-to-portfolio alpha cycle"
    )
    ops_pipeline_alpha_cycle.add_argument(
        "--config", type=str, required=True, help="Path to the alpha cycle JSON/YAML config."
    )
    ops_pipeline_alpha_cycle.set_defaults(func=cmd_pipeline_alpha_cycle)
    ops_pipeline_daily_trading = ops_pipeline_subparsers.add_parser(
        "daily-trading", help="Run the automated daily paper-trading workflow"
    )
    ops_pipeline_daily_trading.add_argument(
        "--config", type=str, required=True, help="Path to the daily-trading JSON/YAML config."
    )
    ops_pipeline_daily_trading.set_defaults(func=cmd_pipeline_daily_trading)
    ops_pipeline_replay_daily = ops_pipeline_subparsers.add_parser(
        "replay-daily",
        help="Run a sequential historical paper-trading replay across trading dates; not a live scheduler",
    )
    ops_pipeline_replay_daily.add_argument(
        "--config", type=str, required=True, help="Path to the daily replay JSON/YAML config."
    )
    ops_pipeline_replay_daily.add_argument("--start-date", type=str, default=None, help="Replay start date YYYY-MM-DD.")
    ops_pipeline_replay_daily.add_argument("--end-date", type=str, default=None, help="Replay end date YYYY-MM-DD.")
    ops_pipeline_replay_daily.add_argument(
        "--dates-file",
        type=str,
        default=None,
        help="Optional file containing explicit replay dates, one per line or comma separated.",
    )
    ops_pipeline_replay_daily.add_argument(
        "--initial-state-path",
        type=str,
        default=None,
        help="Optional initial paper state snapshot used to seed the replay state.",
    )
    ops_pipeline_replay_daily.add_argument(
        "--output-dir",
        type=str,
        required=True,
        help="Directory where per-day replay artifacts and replay-level summaries will be written.",
    )
    ops_pipeline_replay_daily.add_argument(
        "--stop-on-error",
        action="store_true",
        help="Abort the replay on the first failed replay day.",
    )
    ops_pipeline_replay_daily.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Record failed replay days and continue processing later dates.",
    )
    ops_pipeline_replay_daily.add_argument(
        "--max-days",
        type=int,
        default=None,
        help="Optional maximum number of replay dates to process after date resolution.",
    )
    ops_pipeline_replay_daily.add_argument(
        "--profile-replay",
        action="store_true",
        help="Write replay_timing_summary.json and replay_timing_by_day.csv profiling artifacts.",
    )
    ops_pipeline_replay_daily.set_defaults(func=cmd_pipeline_replay_daily)

    ops_monitor = ops_subparsers.add_parser("monitor", help="Monitoring, alerting, and dashboard data builders")
    ops_monitor_subparsers = ops_monitor.add_subparsers(dest="ops_monitor_command", required=True)
    ops_monitor_run = ops_monitor_subparsers.add_parser(
        "run-health", help="Evaluate run health for a completed pipeline run"
    )
    ops_monitor_run.add_argument(
        "--run-dir", type=str, required=True, help="Path to a completed pipeline run directory."
    )
    ops_monitor_run.add_argument("--config", type=str, required=True, help="Path to the monitoring JSON/YAML config.")
    ops_monitor_run.set_defaults(func=cmd_monitor_run_health)
    ops_monitor_latest = ops_monitor_subparsers.add_parser(
        "latest", help="Locate the newest pipeline run and evaluate run health"
    )
    ops_monitor_latest.add_argument(
        "--pipeline-root", type=str, required=True, help="Root directory containing timestamped pipeline runs."
    )
    ops_monitor_latest.add_argument(
        "--config", type=str, required=True, help="Path to the monitoring JSON/YAML config."
    )
    ops_monitor_latest.add_argument(
        "--output-dir", type=str, required=True, help="Directory where latest-run monitoring artifacts will be written."
    )
    ops_monitor_latest.set_defaults(func=cmd_monitor_latest)
    ops_monitor_strategy = ops_monitor_subparsers.add_parser(
        "strategy-health", help="Evaluate per-strategy health from registry and paper/live artifacts"
    )
    ops_monitor_strategy.add_argument(
        "--registry", type=str, required=True, help="Path to the strategy registry JSON/YAML file."
    )
    ops_monitor_strategy.add_argument(
        "--artifacts-root", type=str, required=True, help="Root directory used to resolve relative artifact paths."
    )
    ops_monitor_strategy.add_argument(
        "--config", type=str, required=True, help="Path to the monitoring JSON/YAML config."
    )
    ops_monitor_strategy.add_argument(
        "--output-dir", type=str, required=True, help="Directory where strategy health artifacts will be written."
    )
    ops_monitor_strategy.set_defaults(func=cmd_monitor_strategy_health)
    ops_monitor_portfolio = ops_monitor_subparsers.add_parser(
        "portfolio-health", help="Evaluate health for combined portfolio allocation artifacts"
    )
    ops_monitor_portfolio.add_argument(
        "--allocation-dir", type=str, required=True, help="Directory containing multi-strategy allocation artifacts."
    )
    ops_monitor_portfolio.add_argument(
        "--config", type=str, required=True, help="Path to the monitoring JSON/YAML config."
    )
    ops_monitor_portfolio.add_argument(
        "--output-dir", type=str, required=True, help="Directory where portfolio health artifacts will be written."
    )
    ops_monitor_portfolio.set_defaults(func=cmd_monitor_portfolio_health)
    ops_monitor_dashboard = ops_monitor_subparsers.add_parser(
        "build-dashboard-data", help="Build compact dashboard-ready monitoring data from pipeline runs"
    )
    ops_monitor_dashboard.add_argument(
        "--pipeline-root", type=str, required=True, help="Root directory containing timestamped pipeline runs."
    )
    ops_monitor_dashboard.add_argument(
        "--output-dir",
        type=str,
        required=True,
        help="Directory where dashboard-ready JSON/CSV artifacts will be written.",
    )
    ops_monitor_dashboard.set_defaults(func=cmd_monitor_build_dashboard_data)
    ops_monitor_notify = ops_monitor_subparsers.add_parser(
        "notify", help="Send aggregated notifications from an alerts JSON artifact"
    )
    ops_monitor_notify.add_argument("--alerts", type=str, required=True, help="Path to alerts.json.")
    ops_monitor_notify.add_argument(
        "--config", type=str, required=True, help="Path to the notification JSON/YAML config."
    )
    ops_monitor_notify.set_defaults(func=cmd_monitor_notify)
    ops_monitor_providers_summary = ops_monitor_subparsers.add_parser(
        "providers-summary", help="Build a shared cross-provider monitoring summary from dataset registry entries"
    )
    ops_monitor_providers_summary.add_argument(
        "--registry-path",
        type=str,
        default="data/research/dataset_registry.json",
        help="Path to the shared dataset registry JSON artifact.",
    )
    ops_monitor_providers_summary.add_argument(
        "--output-root",
        type=str,
        default="artifacts/provider_monitoring",
        help="Directory where aggregate provider monitoring artifacts will be written.",
    )
    ops_monitor_providers_summary.add_argument(
        "--providers",
        nargs="+",
        default=None,
        help="Optional provider filter.",
    )
    ops_monitor_providers_summary.add_argument(
        "--asset-class",
        type=str,
        default=None,
        help="Optional asset-class filter.",
    )
    ops_monitor_providers_summary.add_argument(
        "--staleness-threshold-hours",
        type=int,
        default=48,
        help="Age threshold used when classifying registry entries as stale.",
    )
    ops_monitor_providers_summary.add_argument(
        "--format",
        type=str,
        default="text",
        choices=["text", "json"],
        help="Render output as human-readable text or JSON.",
    )
    ops_monitor_providers_summary.set_defaults(func=cmd_ops_monitor_providers_summary)
    ops_monitor_providers_health = ops_monitor_subparsers.add_parser(
        "providers-health", help="Build a concise provider-level health summary from dataset registry entries"
    )
    ops_monitor_providers_health.add_argument(
        "--registry-path",
        type=str,
        default="data/research/dataset_registry.json",
        help="Path to the shared dataset registry JSON artifact.",
    )
    ops_monitor_providers_health.add_argument(
        "--output-root",
        type=str,
        default="artifacts/provider_monitoring",
        help="Directory where aggregate provider monitoring artifacts will be written.",
    )
    ops_monitor_providers_health.add_argument(
        "--providers",
        nargs="+",
        default=None,
        help="Optional provider filter.",
    )
    ops_monitor_providers_health.add_argument(
        "--asset-class",
        type=str,
        default=None,
        help="Optional asset-class filter.",
    )
    ops_monitor_providers_health.add_argument(
        "--staleness-threshold-hours",
        type=int,
        default=48,
        help="Age threshold used when classifying registry entries as stale.",
    )
    ops_monitor_providers_health.add_argument(
        "--format",
        type=str,
        default="text",
        choices=["text", "json"],
        help="Render output as human-readable text or JSON.",
    )
    ops_monitor_providers_health.set_defaults(func=cmd_ops_monitor_providers_health)
    ops_monitor_provider_timeline = ops_monitor_subparsers.add_parser(
        "provider-timeline", help="Show timeline-oriented status history for one provider"
    )
    ops_monitor_provider_timeline.add_argument("--provider", type=str, required=True, help="Provider name.")
    ops_monitor_provider_timeline.add_argument(
        "--output-root",
        type=str,
        default="artifacts/provider_monitoring",
        help="Directory containing shared provider monitoring artifacts.",
    )
    ops_monitor_provider_timeline.add_argument(
        "--format",
        type=str,
        default="text",
        choices=["text", "json"],
        help="Render output as human-readable text or JSON.",
    )
    ops_monitor_provider_timeline.set_defaults(func=cmd_ops_monitor_provider_timeline)
    ops_monitor_dataset_timeline = ops_monitor_subparsers.add_parser(
        "dataset-timeline", help="Show timeline-oriented status history for one registry dataset"
    )
    ops_monitor_dataset_timeline.add_argument("--dataset-key", type=str, required=True, help="Registry dataset key.")
    ops_monitor_dataset_timeline.add_argument(
        "--output-root",
        type=str,
        default="artifacts/provider_monitoring",
        help="Directory containing shared provider monitoring artifacts.",
    )
    ops_monitor_dataset_timeline.add_argument(
        "--format",
        type=str,
        default="text",
        choices=["text", "json"],
        help="Render output as human-readable text or JSON.",
    )
    ops_monitor_dataset_timeline.set_defaults(func=cmd_ops_monitor_dataset_timeline)

    ops_registry = ops_subparsers.add_parser(
        "registry", help="Registry-backed deployment controls and governance decisions"
    )
    ops_registry_subparsers = ops_registry.add_subparsers(dest="ops_registry_command", required=True)
    ops_registry_list = ops_registry_subparsers.add_parser("list", help="List strategies in the governance registry")
    ops_registry_list.add_argument(
        "--registry", type=str, required=True, help="Path to the strategy registry JSON/YAML file."
    )
    ops_registry_list.set_defaults(func=cmd_registry_list)
    ops_registry_eval = ops_registry_subparsers.add_parser(
        "evaluate-promotion",
        help="Evaluate a registry strategy against promotion criteria without mutating the registry",
    )
    ops_registry_eval.add_argument(
        "--registry", type=str, required=True, help="Path to the strategy registry JSON/YAML file."
    )
    ops_registry_eval.add_argument(
        "--strategy-id", type=str, required=True, help="Strategy identifier in the registry."
    )
    ops_registry_eval.add_argument(
        "--config", type=str, required=True, help="Path to the governance criteria JSON/YAML file."
    )
    ops_registry_eval.add_argument(
        "--output-dir", type=str, required=True, help="Directory where promotion evaluation artifacts will be written."
    )
    ops_registry_eval.set_defaults(func=cmd_registry_evaluate_promotion)
    ops_registry_promote = ops_registry_subparsers.add_parser(
        "promote", help="Promote a registry strategy by one validated lifecycle stage and append an audit event"
    )
    ops_registry_promote.add_argument(
        "--registry", type=str, required=True, help="Path to the strategy registry JSON/YAML file."
    )
    ops_registry_promote.add_argument(
        "--strategy-id", type=str, required=True, help="Strategy identifier in the registry."
    )
    ops_registry_promote.add_argument(
        "--note", type=str, default=None, help="Optional audit note for this registry mutation."
    )
    ops_registry_promote.set_defaults(func=cmd_registry_promote)
    ops_registry_degrade = ops_registry_subparsers.add_parser(
        "evaluate-degradation",
        help="Evaluate an active registry strategy for degradation without mutating the registry",
    )
    ops_registry_degrade.add_argument(
        "--registry", type=str, required=True, help="Path to the strategy registry JSON/YAML file."
    )
    ops_registry_degrade.add_argument(
        "--strategy-id", type=str, required=True, help="Strategy identifier in the registry."
    )
    ops_registry_degrade.add_argument(
        "--config",
        type=str,
        default=None,
        help="Optional governance criteria JSON/YAML file. Defaults to built-in degradation thresholds.",
    )
    ops_registry_degrade.add_argument(
        "--output-dir",
        type=str,
        required=True,
        help="Directory where degradation evaluation artifacts will be written.",
    )
    ops_registry_degrade.set_defaults(func=cmd_registry_evaluate_degradation)
    ops_registry_demote = ops_registry_subparsers.add_parser(
        "demote", help="Demote a registry strategy by one lifecycle stage and append an audit event"
    )
    ops_registry_demote.add_argument(
        "--registry", type=str, required=True, help="Path to the strategy registry JSON/YAML file."
    )
    ops_registry_demote.add_argument(
        "--strategy-id", type=str, required=True, help="Strategy identifier in the registry."
    )
    ops_registry_demote.add_argument(
        "--note", type=str, default=None, help="Optional audit note for this registry mutation."
    )
    ops_registry_demote.set_defaults(func=cmd_registry_demote)
    ops_registry_build = ops_registry_subparsers.add_parser(
        "build-deploy-config", help="Build a multi-strategy portfolio config from the strategy registry"
    )
    ops_registry_build.add_argument(
        "--registry", type=str, required=True, help="Path to the strategy registry JSON/YAML file."
    )
    ops_registry_build.add_argument(
        "--output-path", type=str, required=True, help="Path to the generated multi-strategy config JSON file."
    )
    ops_registry_build.add_argument(
        "--include-paper", action="store_true", help="Include paper-stage strategies alongside approved strategies."
    )
    ops_registry_build.add_argument("--universe", type=str, default=None, help="Optional universe filter.")
    ops_registry_build.add_argument("--family", type=str, default=None, help="Optional family filter.")
    ops_registry_build.add_argument("--tag", type=str, default=None, help="Optional tag filter.")
    ops_registry_build.add_argument(
        "--deployment-stage", type=str, default=None, help="Optional deployment-stage filter."
    )
    ops_registry_build.add_argument(
        "--max-strategies", type=int, default=None, help="Optional maximum number of strategies to include."
    )
    ops_registry_build.add_argument(
        "--weighting-scheme",
        type=str,
        default="equal",
        choices=["equal", "score_weighted"],
        help="How to assign sleeve capital weights.",
    )
    ops_registry_build.set_defaults(func=cmd_registry_build_multi_strategy_config)

    ops_broker = ops_subparsers.add_parser("broker", help="Direct broker health and emergency commands")
    ops_broker_subparsers = ops_broker.add_subparsers(dest="ops_broker_command", required=True)
    ops_broker_health = ops_broker_subparsers.add_parser(
        "health", help="Run broker connectivity and account health checks"
    )
    ops_broker_health.add_argument(
        "--broker", type=str, default=None, choices=["mock", "alpaca"], help="Optional broker override."
    )
    ops_broker_health.add_argument(
        "--broker-config", type=str, required=True, help="Path to the broker-config JSON/YAML file."
    )
    ops_broker_health.set_defaults(func=cmd_broker_health)
    ops_broker_cancel = ops_broker_subparsers.add_parser("cancel-all", help="Cancel all currently open broker orders")
    ops_broker_cancel.add_argument(
        "--broker", type=str, default=None, choices=["mock", "alpaca"], help="Optional broker override."
    )
    ops_broker_cancel.add_argument(
        "--broker-config", type=str, required=True, help="Path to the broker-config JSON/YAML file."
    )
    ops_broker_cancel.set_defaults(func=cmd_broker_cancel_all)

    ops_execution = ops_subparsers.add_parser("execution", help="Execution realism simulation commands")
    ops_execution_subparsers = ops_execution.add_subparsers(dest="ops_execution_command", required=True)
    ops_execution_simulate = ops_execution_subparsers.add_parser(
        "simulate", help="Simulate executable orders from target/order CSV inputs"
    )
    ops_execution_simulate.add_argument(
        "--config", type=str, required=True, help="Path to the execution-config JSON/YAML file."
    )
    ops_execution_simulate.add_argument(
        "--targets", type=str, required=True, help="CSV containing requested execution orders."
    )
    ops_execution_simulate.add_argument(
        "--output-dir", type=str, required=True, help="Directory where execution artifacts will be written."
    )
    ops_execution_simulate.set_defaults(func=cmd_execution_simulate)

    ops_orchestrate = ops_subparsers.add_parser("orchestrate", help="Automate the full research-to-monitoring pipeline")
    ops_orchestrate_subparsers = ops_orchestrate.add_subparsers(dest="ops_orchestrate_command", required=True)
    ops_orchestrate_run = ops_orchestrate_subparsers.add_parser(
        "run", help="Run the automated promotion, portfolio, paper, and monitoring pipeline"
    )
    ops_orchestrate_run.add_argument(
        "--config", type=str, required=True, help="Path to the automated orchestration JSON/YAML config."
    )
    ops_orchestrate_run.set_defaults(func=cmd_orchestrate_run)
    ops_orchestrate_show = ops_orchestrate_subparsers.add_parser(
        "show-run", help="Show a concise summary of an orchestration run artifact"
    )
    ops_orchestrate_show.add_argument(
        "--run", type=str, required=True, help="Path to orchestration_run.json or its parent directory."
    )
    ops_orchestrate_show.set_defaults(func=cmd_orchestrate_show_run)
    ops_orchestrate_loop = ops_orchestrate_subparsers.add_parser(
        "loop", help="Run the automated orchestration pipeline repeatedly with sleep intervals"
    )
    ops_orchestrate_loop.add_argument(
        "--config", type=str, required=True, help="Path to the automated orchestration JSON/YAML config."
    )
    ops_orchestrate_loop.add_argument(
        "--max-iterations", type=int, default=None, help="Optional maximum iterations before the loop exits."
    )
    ops_orchestrate_loop.set_defaults(func=cmd_orchestrate_loop)

    ops_autonomous_loop = ops_subparsers.add_parser(
        "autonomous-loop",
        help="Cron-scheduled autonomous orchestration loop with staleness checks, degradation detection, and circuit breaker",
    )
    ops_autonomous_loop_subparsers = ops_autonomous_loop.add_subparsers(
        dest="ops_autonomous_loop_command", required=True
    )
    ops_autonomous_loop_start = ops_autonomous_loop_subparsers.add_parser(
        "start",
        help="Start the autonomous loop (runs until KILL_SWITCH file, circuit breaker, or --max-iterations)",
    )
    ops_autonomous_loop_start.add_argument(
        "--config",
        type=str,
        required=True,
        help="Path to autonomous_loop.yaml config file.",
    )
    ops_autonomous_loop_start.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Evaluate decisions and log them without executing orchestration runs.",
    )
    ops_autonomous_loop_start.add_argument(
        "--max-iterations",
        type=int,
        default=None,
        help="Optional maximum iterations before the loop exits.",
    )
    ops_autonomous_loop_start.set_defaults(func=cmd_autonomous_loop_start)

    ops_system_eval = ops_subparsers.add_parser(
        "system-eval", help="Build and compare full-system evaluation artifacts across orchestration runs"
    )
    ops_system_eval_subparsers = ops_system_eval.add_subparsers(dest="ops_system_eval_command", required=True)
    ops_system_eval_build = ops_system_eval_subparsers.add_parser(
        "build", help="Scan orchestration runs and build system evaluation history artifacts"
    )
    ops_system_eval_build.add_argument(
        "--runs-root", type=str, required=True, help="Root directory containing timestamped orchestration runs."
    )
    ops_system_eval_build.add_argument(
        "--output-dir",
        type=str,
        required=True,
        help="Directory where system evaluation history artifacts will be written.",
    )
    ops_system_eval_build.set_defaults(func=cmd_system_eval_build)
    ops_system_eval_show = ops_system_eval_subparsers.add_parser(
        "show", help="Print a concise summary of a system evaluation artifact"
    )
    ops_system_eval_show.add_argument(
        "--evaluation", type=str, required=True, help="Path to system_evaluation.json or its parent directory."
    )
    ops_system_eval_show.set_defaults(func=cmd_system_eval_show)
    ops_system_eval_compare = ops_system_eval_subparsers.add_parser(
        "compare", help="Compare system evaluation groups across run history"
    )
    ops_system_eval_compare.add_argument(
        "--history",
        type=str,
        required=True,
        help="Path to system_evaluation_history.json, its parent directory, or an orchestration runs root.",
    )
    ops_system_eval_compare.add_argument(
        "--output-dir", type=str, required=True, help="Directory where comparison artifacts will be written."
    )
    ops_system_eval_compare.add_argument(
        "--latest-count",
        type=int,
        default=10,
        help="Number of most recent runs to include in group A when feature-flag grouping is not used.",
    )
    ops_system_eval_compare.add_argument(
        "--previous-count",
        type=int,
        default=None,
        help="Optional number of older runs to include in group B when feature-flag grouping is not used.",
    )
    ops_system_eval_compare.add_argument(
        "--feature-flag", type=str, default=None, help="Optional feature flag name used to split runs into A/B groups."
    )
    ops_system_eval_compare.add_argument(
        "--group-by-field",
        type=str,
        default=None,
        choices=["variant_name", "experiment_name", "experiment_run_id"],
        help="Optional non-feature field used to split runs into A/B groups.",
    )
    ops_system_eval_compare.add_argument(
        "--value-a", type=str, default="true", help="Feature-flag value for group A when --feature-flag is used."
    )
    ops_system_eval_compare.add_argument(
        "--value-b", type=str, default="false", help="Feature-flag value for group B when --feature-flag is used."
    )
    ops_system_eval_compare.set_defaults(func=cmd_system_eval_compare)

    ops_experiment = ops_subparsers.add_parser(
        "experiment", help="Run and compare controlled orchestration experiment variants"
    )
    ops_experiment_subparsers = ops_experiment.add_subparsers(dest="ops_experiment_command", required=True)
    ops_experiment_run = ops_experiment_subparsers.add_parser(
        "run", help="Materialize orchestration variants and run a controlled experiment cohort"
    )
    ops_experiment_run.add_argument(
        "--config", type=str, required=True, help="Path to the experiment spec JSON/YAML file."
    )
    ops_experiment_run.add_argument(
        "--variants", nargs="*", default=None, help="Optional subset of variant names to run."
    )
    ops_experiment_run.add_argument(
        "--dry-run",
        action="store_true",
        help="Only materialize per-variant orchestration configs without running them.",
    )
    ops_experiment_run.set_defaults(func=cmd_experiment_run)
    ops_experiment_show = ops_experiment_subparsers.add_parser(
        "show", help="Show a concise summary of an experiment run artifact"
    )
    ops_experiment_show.add_argument(
        "--run", type=str, required=True, help="Path to experiment_run.json or its parent directory."
    )
    ops_experiment_show.set_defaults(func=cmd_experiment_show)
    ops_experiment_compare = ops_experiment_subparsers.add_parser(
        "compare", help="Compare experiment variants using system evaluation history"
    )
    ops_experiment_compare.add_argument(
        "--run", type=str, required=True, help="Path to experiment_run.json or its parent directory."
    )
    ops_experiment_compare.add_argument(
        "--output-dir", type=str, required=True, help="Directory where comparison artifacts will be written."
    )
    ops_experiment_compare.add_argument(
        "--variant-a", type=str, default=None, help="Optional variant name for group A."
    )
    ops_experiment_compare.add_argument(
        "--variant-b", type=str, default=None, help="Optional variant name for group B."
    )
    ops_experiment_compare.set_defaults(func=cmd_experiment_compare)
    ops_experiment_summary = ops_experiment_subparsers.add_parser(
        "summarize-campaign", help="Summarize one or more experiment runs into a concise metric winner report"
    )
    ops_experiment_summary.add_argument(
        "--runs", nargs="+", required=True, help="One or more experiment run directories or experiment_run.json paths."
    )
    ops_experiment_summary.add_argument(
        "--output-dir", type=str, required=True, help="Directory where campaign summary artifacts will be written."
    )
    ops_experiment_summary.set_defaults(func=cmd_experiment_summarize_campaign)
    ops_experiment_recommend = ops_experiment_subparsers.add_parser(
        "recommend-defaults", help="Turn campaign summary artifacts into recommended default settings"
    )
    ops_experiment_recommend.add_argument(
        "--summary", type=str, required=True, help="Path to experiment_campaign_summary.json or its parent directory."
    )
    ops_experiment_recommend.add_argument(
        "--output-dir", type=str, required=True, help="Directory where decision summary artifacts will be written."
    )
    ops_experiment_recommend.add_argument(
        "--write-config",
        type=str,
        default=None,
        help="Optional output path for a recommended orchestration config snapshot.",
    )
    ops_experiment_recommend.add_argument(
        "--base-config", type=str, default=None, help="Base orchestration config used when --write-config is provided."
    )
    ops_experiment_recommend.set_defaults(func=cmd_experiment_recommend_defaults)

    ops_experiments = ops_subparsers.add_parser(
        "experiments", help="Experiment registry inspection and dashboard commands"
    )
    ops_experiments_subparsers = ops_experiments.add_subparsers(dest="ops_experiments_command", required=True)
    ops_experiments_list = ops_experiments_subparsers.add_parser(
        "list", help="List recent tracked research and paper trading experiments"
    )
    ops_experiments_list.add_argument(
        "--tracker-dir",
        type=str,
        default="artifacts/experiment_tracking",
        help="Directory containing the shared experiment registry.",
    )
    ops_experiments_list.add_argument("--limit", type=int, default=10, help="Maximum number of experiments to print.")
    ops_experiments_list.set_defaults(func=cmd_experiments_list)
    ops_experiments_latest = ops_experiments_subparsers.add_parser(
        "latest", help="Show the latest approved composite or research configuration snapshot"
    )
    ops_experiments_latest.add_argument(
        "--tracker-dir",
        type=str,
        default="artifacts/experiment_tracking",
        help="Directory containing the shared experiment registry.",
    )
    ops_experiments_latest.set_defaults(func=cmd_experiments_latest_model)
    ops_experiments_dashboard = ops_experiments_subparsers.add_parser(
        "dashboard", help="Build a summary dashboard artifact from tracked experiments"
    )
    ops_experiments_dashboard.add_argument(
        "--tracker-dir",
        type=str,
        default="artifacts/experiment_tracking",
        help="Directory containing the shared experiment registry.",
    )
    ops_experiments_dashboard.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Optional directory where the dashboard artifacts should be written.",
    )
    ops_experiments_dashboard.add_argument(
        "--top-metric",
        type=str,
        default="portfolio_sharpe",
        help="Registry metric used to rank top experiments in the dashboard.",
    )
    ops_experiments_dashboard.add_argument(
        "--limit", type=int, default=10, help="Maximum number of top experiments to include."
    )
    ops_experiments_dashboard.set_defaults(func=cmd_experiments_dashboard)
    ops_experiments_diff = ops_experiments_subparsers.add_parser(
        "diff", help="Show the current approved configuration versus the prior approved snapshot"
    )
    ops_experiments_diff.add_argument(
        "--snapshot-dir",
        type=str,
        default="artifacts/research_refresh/approved_configuration_snapshots",
        help="Directory containing approved configuration snapshots.",
    )
    ops_experiments_diff.set_defaults(func=cmd_approved_config_diff)

    experiments_parser = subparsers.add_parser(
        "experiments", help="Experiment registry inspection and dashboard commands"
    )
    experiments_subparsers = experiments_parser.add_subparsers(dest="experiments_command", required=True)
    experiments_list = experiments_subparsers.add_parser(
        "list", help="List recent tracked research and paper trading experiments"
    )
    experiments_list.add_argument(
        "--tracker-dir",
        type=str,
        default="artifacts/experiment_tracking",
        help="Directory containing the shared experiment registry.",
    )
    experiments_list.add_argument("--limit", type=int, default=10, help="Maximum number of experiments to print.")
    experiments_list.set_defaults(func=cmd_experiments_list)
    experiments_latest = experiments_subparsers.add_parser(
        "latest", help="Show the latest approved composite or research configuration snapshot"
    )
    experiments_latest.add_argument(
        "--tracker-dir",
        type=str,
        default="artifacts/experiment_tracking",
        help="Directory containing the shared experiment registry.",
    )
    experiments_latest.set_defaults(func=cmd_experiments_latest_model)
    experiments_dashboard = experiments_subparsers.add_parser(
        "dashboard", help="Build a summary dashboard artifact from tracked experiments"
    )
    experiments_dashboard.add_argument(
        "--tracker-dir",
        type=str,
        default="artifacts/experiment_tracking",
        help="Directory containing the shared experiment registry.",
    )
    experiments_dashboard.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Optional directory where the dashboard artifacts should be written.",
    )
    experiments_dashboard.add_argument(
        "--top-metric",
        type=str,
        default="portfolio_sharpe",
        help="Registry metric used to rank top experiments in the dashboard.",
    )
    experiments_dashboard.add_argument(
        "--limit", type=int, default=10, help="Maximum number of top experiments to include."
    )
    experiments_dashboard.set_defaults(func=cmd_experiments_dashboard)
    experiments_diff = experiments_subparsers.add_parser(
        "diff", help="Show the current approved configuration versus the prior approved snapshot"
    )
    experiments_diff.add_argument(
        "--snapshot-dir",
        type=str,
        default="artifacts/research_refresh/approved_configuration_snapshots",
        help="Directory containing approved configuration snapshots.",
    )
    experiments_diff.set_defaults(func=cmd_approved_config_diff)

    registry_parser = subparsers.add_parser("registry", help="Strategy registry governance commands")
    registry_subparsers = registry_parser.add_subparsers(dest="registry_command", required=True)
    registry_list = registry_subparsers.add_parser("list", help="List strategies in the governance registry")
    registry_list.add_argument(
        "--registry", type=str, required=True, help="Path to the strategy registry JSON/YAML file."
    )
    registry_list.set_defaults(func=cmd_registry_list)

    registry_eval_promotion = registry_subparsers.add_parser(
        "evaluate-promotion",
        help="Evaluate a registry strategy against promotion criteria without mutating the registry",
    )
    registry_eval_promotion.add_argument(
        "--registry", type=str, required=True, help="Path to the strategy registry JSON/YAML file."
    )
    registry_eval_promotion.add_argument(
        "--strategy-id", type=str, required=True, help="Strategy identifier in the registry."
    )
    registry_eval_promotion.add_argument(
        "--config", type=str, required=True, help="Path to the governance criteria JSON/YAML file."
    )
    registry_eval_promotion.add_argument(
        "--output-dir", type=str, required=True, help="Directory where promotion evaluation artifacts will be written."
    )
    registry_eval_promotion.set_defaults(func=cmd_registry_evaluate_promotion)

    registry_promote = registry_subparsers.add_parser(
        "promote", help="Promote a registry strategy by one validated lifecycle stage and append an audit event"
    )
    registry_promote.add_argument(
        "--registry", type=str, required=True, help="Path to the strategy registry JSON/YAML file."
    )
    registry_promote.add_argument("--strategy-id", type=str, required=True, help="Strategy identifier in the registry.")
    registry_promote.add_argument(
        "--note", type=str, default=None, help="Optional audit note for this registry mutation."
    )
    registry_promote.set_defaults(func=cmd_registry_promote)

    registry_eval_degradation = registry_subparsers.add_parser(
        "evaluate-degradation",
        help="Evaluate an active registry strategy for degradation without mutating the registry",
    )
    registry_eval_degradation.add_argument(
        "--registry", type=str, required=True, help="Path to the strategy registry JSON/YAML file."
    )
    registry_eval_degradation.add_argument(
        "--strategy-id", type=str, required=True, help="Strategy identifier in the registry."
    )
    registry_eval_degradation.add_argument(
        "--config",
        type=str,
        default=None,
        help="Optional governance criteria JSON/YAML file. Defaults to built-in degradation thresholds.",
    )
    registry_eval_degradation.add_argument(
        "--output-dir",
        type=str,
        required=True,
        help="Directory where degradation evaluation artifacts will be written.",
    )
    registry_eval_degradation.set_defaults(func=cmd_registry_evaluate_degradation)

    registry_demote = registry_subparsers.add_parser(
        "demote", help="Demote a registry strategy by one lifecycle stage and append an audit event"
    )
    registry_demote.add_argument(
        "--registry", type=str, required=True, help="Path to the strategy registry JSON/YAML file."
    )
    registry_demote.add_argument("--strategy-id", type=str, required=True, help="Strategy identifier in the registry.")
    registry_demote.add_argument(
        "--note", type=str, default=None, help="Optional audit note for this registry mutation."
    )
    registry_demote.set_defaults(func=cmd_registry_demote)

    registry_build = registry_subparsers.add_parser(
        "build-multi-strategy-config", help="Build a multi-strategy portfolio config from the strategy registry"
    )
    registry_build.add_argument(
        "--registry", type=str, required=True, help="Path to the strategy registry JSON/YAML file."
    )
    registry_build.add_argument(
        "--output-path", type=str, required=True, help="Path to the generated multi-strategy config JSON file."
    )
    registry_build.add_argument(
        "--include-paper", action="store_true", help="Include paper-stage strategies alongside approved strategies."
    )
    registry_build.add_argument("--universe", type=str, default=None, help="Optional universe filter.")
    registry_build.add_argument("--family", type=str, default=None, help="Optional family filter.")
    registry_build.add_argument("--tag", type=str, default=None, help="Optional tag filter.")
    registry_build.add_argument("--deployment-stage", type=str, default=None, help="Optional deployment-stage filter.")
    registry_build.add_argument(
        "--max-strategies", type=int, default=None, help="Optional maximum number of strategies to include."
    )
    registry_build.add_argument(
        "--weighting-scheme",
        type=str,
        default="equal",
        choices=["equal", "score_weighted"],
        help="How to assign sleeve capital weights.",
    )
    registry_build.set_defaults(func=cmd_registry_build_multi_strategy_config)

    pipeline_parser = subparsers.add_parser("pipeline", help="Scheduled orchestration and governance runner")
    pipeline_subparsers = pipeline_parser.add_subparsers(dest="pipeline_command", required=True)
    pipeline_run = pipeline_subparsers.add_parser(
        "run", help="Run the orchestration pipeline exactly as specified in the config"
    )
    pipeline_run.add_argument("--config", type=str, required=True, help="Path to the pipeline JSON/YAML config.")
    pipeline_run.set_defaults(func=cmd_pipeline_run)
    pipeline_run_daily = pipeline_subparsers.add_parser(
        "run-daily", help="Run a daily pipeline config and validate schedule_type=daily"
    )
    pipeline_run_daily.add_argument("--config", type=str, required=True, help="Path to the pipeline JSON/YAML config.")
    pipeline_run_daily.set_defaults(func=cmd_pipeline_run_daily)
    pipeline_run_weekly = pipeline_subparsers.add_parser(
        "run-weekly", help="Run a weekly pipeline config and validate schedule_type=weekly"
    )
    pipeline_run_weekly.add_argument("--config", type=str, required=True, help="Path to the pipeline JSON/YAML config.")
    pipeline_run_weekly.set_defaults(func=cmd_pipeline_run_weekly)
    pipeline_alpha_cycle = pipeline_subparsers.add_parser(
        "alpha-cycle", help="Run the one-command research-to-portfolio alpha cycle"
    )
    pipeline_alpha_cycle.add_argument(
        "--config", type=str, required=True, help="Path to the alpha cycle JSON/YAML config."
    )
    pipeline_alpha_cycle.set_defaults(func=cmd_pipeline_alpha_cycle)
    pipeline_daily_trading = pipeline_subparsers.add_parser(
        "daily-trading", help="Run the automated daily paper-trading workflow"
    )
    pipeline_daily_trading.add_argument(
        "--config", type=str, required=True, help="Path to the daily-trading JSON/YAML config."
    )
    pipeline_daily_trading.set_defaults(func=cmd_pipeline_daily_trading)
    pipeline_replay_daily = pipeline_subparsers.add_parser(
        "replay-daily",
        help="Run a sequential historical paper-trading replay across trading dates; not a live scheduler",
    )
    pipeline_replay_daily.add_argument(
        "--config", type=str, required=True, help="Path to the daily replay JSON/YAML config."
    )
    pipeline_replay_daily.add_argument("--start-date", type=str, default=None, help="Replay start date YYYY-MM-DD.")
    pipeline_replay_daily.add_argument("--end-date", type=str, default=None, help="Replay end date YYYY-MM-DD.")
    pipeline_replay_daily.add_argument(
        "--dates-file",
        type=str,
        default=None,
        help="Optional file containing explicit replay dates, one per line or comma separated.",
    )
    pipeline_replay_daily.add_argument(
        "--initial-state-path",
        type=str,
        default=None,
        help="Optional initial paper state snapshot used to seed the replay state.",
    )
    pipeline_replay_daily.add_argument(
        "--output-dir",
        type=str,
        required=True,
        help="Directory where per-day replay artifacts and replay-level summaries will be written.",
    )
    pipeline_replay_daily.add_argument(
        "--stop-on-error", action="store_true", help="Abort the replay on the first failed replay day."
    )
    pipeline_replay_daily.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Record failed replay days and continue processing later dates.",
    )
    pipeline_replay_daily.add_argument(
        "--max-days",
        type=int,
        default=None,
        help="Optional maximum number of replay dates to process after date resolution.",
    )
    pipeline_replay_daily.add_argument(
        "--profile-replay",
        action="store_true",
        help="Write replay_timing_summary.json and replay_timing_by_day.csv profiling artifacts.",
    )
    pipeline_replay_daily.set_defaults(func=cmd_pipeline_replay_daily)

    execution_parser = subparsers.add_parser("execution", help="Execution realism simulation commands")
    execution_subparsers = execution_parser.add_subparsers(dest="execution_command", required=True)
    execution_simulate = execution_subparsers.add_parser(
        "simulate", help="Simulate executable orders from target/order CSV inputs"
    )
    execution_simulate.add_argument(
        "--config", type=str, required=True, help="Path to the execution-config JSON/YAML file."
    )
    execution_simulate.add_argument(
        "--targets", type=str, required=True, help="CSV containing requested execution orders."
    )
    execution_simulate.add_argument(
        "--output-dir", type=str, required=True, help="Directory where execution artifacts will be written."
    )
    execution_simulate.set_defaults(func=cmd_execution_simulate)

    monitor_parser = subparsers.add_parser("monitor", help="Operational monitoring and alerting commands")
    monitor_subparsers = monitor_parser.add_subparsers(dest="monitor_command", required=True)
    monitor_run = monitor_subparsers.add_parser("run-health", help="Evaluate run health for a completed pipeline run")
    monitor_run.add_argument("--run-dir", type=str, required=True, help="Path to a completed pipeline run directory.")
    monitor_run.add_argument("--config", type=str, required=True, help="Path to the monitoring JSON/YAML config.")
    monitor_run.set_defaults(func=cmd_monitor_run_health)
    monitor_strategy = monitor_subparsers.add_parser(
        "strategy-health", help="Evaluate per-strategy health from registry and paper/live artifacts"
    )
    monitor_strategy.add_argument(
        "--registry", type=str, required=True, help="Path to the strategy registry JSON/YAML file."
    )
    monitor_strategy.add_argument(
        "--artifacts-root", type=str, required=True, help="Root directory used to resolve relative artifact paths."
    )
    monitor_strategy.add_argument("--config", type=str, required=True, help="Path to the monitoring JSON/YAML config.")
    monitor_strategy.add_argument(
        "--output-dir", type=str, required=True, help="Directory where strategy health artifacts will be written."
    )
    monitor_strategy.set_defaults(func=cmd_monitor_strategy_health)
    monitor_portfolio = monitor_subparsers.add_parser(
        "portfolio-health", help="Evaluate health for combined portfolio allocation artifacts"
    )
    monitor_portfolio.add_argument(
        "--allocation-dir", type=str, required=True, help="Directory containing multi-strategy allocation artifacts."
    )
    monitor_portfolio.add_argument("--config", type=str, required=True, help="Path to the monitoring JSON/YAML config.")
    monitor_portfolio.add_argument(
        "--output-dir", type=str, required=True, help="Directory where portfolio health artifacts will be written."
    )
    monitor_portfolio.set_defaults(func=cmd_monitor_portfolio_health)
    monitor_latest = monitor_subparsers.add_parser(
        "latest", help="Locate the newest pipeline run and evaluate run health"
    )
    monitor_latest.add_argument(
        "--pipeline-root", type=str, required=True, help="Root directory containing timestamped pipeline runs."
    )
    monitor_latest.add_argument("--config", type=str, required=True, help="Path to the monitoring JSON/YAML config.")
    monitor_latest.add_argument(
        "--output-dir", type=str, required=True, help="Directory where latest-run monitoring artifacts will be written."
    )
    monitor_latest.set_defaults(func=cmd_monitor_latest)
    monitor_dashboard = monitor_subparsers.add_parser(
        "build-dashboard-data", help="Build compact dashboard-ready monitoring data from pipeline runs"
    )
    monitor_dashboard.add_argument(
        "--pipeline-root", type=str, required=True, help="Root directory containing timestamped pipeline runs."
    )
    monitor_dashboard.add_argument(
        "--output-dir",
        type=str,
        required=True,
        help="Directory where dashboard-ready JSON/CSV artifacts will be written.",
    )
    monitor_dashboard.set_defaults(func=cmd_monitor_build_dashboard_data)
    monitor_notify = monitor_subparsers.add_parser(
        "notify", help="Send aggregated notifications from an alerts JSON artifact"
    )
    monitor_notify.add_argument("--alerts", type=str, required=True, help="Path to alerts.json.")
    monitor_notify.add_argument("--config", type=str, required=True, help="Path to the notification JSON/YAML config.")
    monitor_notify.set_defaults(func=cmd_monitor_notify)

    broker_parser = subparsers.add_parser("broker", help="Direct broker health and emergency commands")
    broker_subparsers = broker_parser.add_subparsers(dest="broker_command", required=True)
    broker_health = broker_subparsers.add_parser("health", help="Run broker connectivity and account health checks")
    broker_health.add_argument(
        "--broker", type=str, default=None, choices=["mock", "alpaca"], help="Optional broker override."
    )
    broker_health.add_argument(
        "--broker-config", type=str, required=True, help="Path to the broker-config JSON/YAML file."
    )
    broker_health.set_defaults(func=cmd_broker_health)
    broker_cancel = broker_subparsers.add_parser("cancel-all", help="Cancel all currently open broker orders")
    broker_cancel.add_argument(
        "--broker", type=str, default=None, choices=["mock", "alpaca"], help="Optional broker override."
    )
    broker_cancel.add_argument(
        "--broker-config", type=str, required=True, help="Path to the broker-config JSON/YAML file."
    )
    broker_cancel.set_defaults(func=cmd_broker_cancel_all)

    dashboard_parser = subparsers.add_parser("dashboard", help="Local read-only dashboard for artifact inspection")
    dashboard_subparsers = dashboard_parser.add_subparsers(dest="dashboard_command", required=True)
    dashboard_serve = dashboard_subparsers.add_parser("serve", help="Start the local dashboard server")
    dashboard_serve.add_argument(
        "--artifacts-root", type=str, default="artifacts", help="Root artifact directory to scan."
    )
    dashboard_serve.add_argument(
        "--host", type=str, default="127.0.0.1", help="Host to bind the local dashboard server."
    )
    dashboard_serve.add_argument("--port", type=int, default=8000, help="Port to bind the local dashboard server.")
    dashboard_serve.set_defaults(func=cmd_dashboard_serve)
    dashboard_build = dashboard_subparsers.add_parser(
        "build-static-data", help="Write normalized dashboard JSON payloads for external inspection"
    )
    dashboard_build.add_argument(
        "--artifacts-root", type=str, default="artifacts", help="Root artifact directory to scan."
    )
    dashboard_build.add_argument(
        "--output-dir",
        type=str,
        required=True,
        help="Directory where normalized dashboard JSON payloads will be written.",
    )
    dashboard_build.set_defaults(func=cmd_dashboard_build_static_data)

    strategy_portfolio_parser = subparsers.add_parser(
        "strategy-portfolio", help="Select and weight promoted strategies into a portfolio bundle"
    )
    strategy_portfolio_subparsers = strategy_portfolio_parser.add_subparsers(
        dest="strategy_portfolio_command", required=True
    )
    strategy_portfolio_build = strategy_portfolio_subparsers.add_parser(
        "build", help="Canonical step 4: build a strategy portfolio from promoted strategies and a selection policy"
    )
    strategy_portfolio_build.add_argument(
        "--promoted-dir", type=str, required=True, help="Directory containing promoted_strategies.json."
    )
    strategy_portfolio_build.add_argument(
        "--policy-config", type=str, default=None, help="Optional strategy portfolio policy JSON/YAML file."
    )
    strategy_portfolio_build.add_argument(
        "--lifecycle",
        type=str,
        default=None,
        help="Optional strategy_lifecycle.json path or directory used to exclude demoted strategies.",
    )
    strategy_portfolio_build.add_argument(
        "--output-dir", type=str, required=True, help="Directory where strategy portfolio artifacts will be written."
    )
    strategy_portfolio_build.set_defaults(func=cmd_strategy_portfolio_build)
    strategy_portfolio_show = strategy_portfolio_subparsers.add_parser(
        "show", help="Print a concise summary of a strategy portfolio artifact"
    )
    strategy_portfolio_show.add_argument(
        "--portfolio", type=str, required=True, help="Path to strategy_portfolio.json or its parent directory."
    )
    strategy_portfolio_show.set_defaults(func=cmd_strategy_portfolio_show)
    strategy_portfolio_activate = strategy_portfolio_subparsers.add_parser(
        "activate", help="Evaluate runtime activation conditions for a strategy portfolio"
    )
    strategy_portfolio_activate.add_argument(
        "--portfolio", type=str, required=True, help="Path to strategy_portfolio.json or its parent directory."
    )
    strategy_portfolio_activate.add_argument(
        "--output-dir", type=str, required=True, help="Directory where activated portfolio artifacts will be written."
    )
    strategy_portfolio_activate.add_argument(
        "--market-regime", type=str, default=None, help="Optional market_regime.json path or directory."
    )
    strategy_portfolio_activate.add_argument(
        "--regime-labels", type=str, default=None, help="Optional regime_labels_by_date.csv path or directory."
    )
    strategy_portfolio_activate.add_argument(
        "--metadata-dir",
        type=str,
        default=None,
        help="Optional metadata directory containing universe_enrichment.csv and sub_universe_snapshot.csv.",
    )
    strategy_portfolio_activate.add_argument(
        "--activation-context-sources",
        nargs="+",
        default=None,
        help="Optional activation context sources to evaluate. Supported: regime benchmark_context sub_universe.",
    )
    strategy_portfolio_activate.add_argument(
        "--include-inactive-conditionals-in-output",
        action="store_true",
        default=True,
        help="Include inactive conditional rows in the activated output artifact.",
    )
    strategy_portfolio_activate.add_argument(
        "--exclude-inactive-conditionals-in-output",
        dest="include_inactive_conditionals_in_output",
        action="store_false",
        help="Exclude inactive conditional rows from the activated output artifact.",
    )
    strategy_portfolio_activate.set_defaults(func=cmd_strategy_portfolio_activate)
    strategy_portfolio_export = strategy_portfolio_subparsers.add_parser(
        "export-run-config",
        help="Export a runnable multi-strategy and pipeline config bundle from a strategy portfolio",
    )
    strategy_portfolio_export.add_argument(
        "--portfolio", type=str, required=True, help="Path to strategy_portfolio.json or its parent directory."
    )
    strategy_portfolio_export.add_argument(
        "--output-dir", type=str, required=True, help="Directory where runnable config artifacts will be written."
    )
    strategy_portfolio_export.set_defaults(func=cmd_strategy_portfolio_export_run_config)
    strategy_portfolio_experiment = strategy_portfolio_subparsers.add_parser(
        "experiment-bundle",
        help="Run a small canonical-bundle experiment matrix by varying promotion and portfolio policy inputs",
    )
    strategy_portfolio_experiment.add_argument(
        "--config",
        type=str,
        required=True,
        help="Path to the canonical bundle experiment JSON/YAML config.",
    )
    strategy_portfolio_experiment.set_defaults(func=cmd_strategy_portfolio_experiment_bundle)
    strategy_portfolio_experiment_matrix = strategy_portfolio_subparsers.add_parser(
        "experiment-bundle-matrix",
        help="Run the same canonical policy-sensitivity preset set across multiple bundle/date cases",
    )
    strategy_portfolio_experiment_matrix.add_argument(
        "--config",
        type=str,
        required=True,
        help="Path to the canonical bundle matrix experiment JSON/YAML config.",
    )
    strategy_portfolio_experiment_matrix.set_defaults(func=cmd_strategy_portfolio_experiment_bundle_matrix)

    strategy_monitor_parser = subparsers.add_parser(
        "strategy-monitor", help="Build monitoring snapshots and kill-switch recommendations for strategy portfolios"
    )
    strategy_monitor_subparsers = strategy_monitor_parser.add_subparsers(dest="strategy_monitor_command", required=True)
    strategy_monitor_build = strategy_monitor_subparsers.add_parser(
        "build", help="Build a strategy monitoring snapshot from a strategy portfolio and paper artifacts"
    )
    strategy_monitor_build.add_argument(
        "--portfolio", type=str, required=True, help="Path to strategy_portfolio.json or its parent directory."
    )
    strategy_monitor_build.add_argument(
        "--paper-dir",
        type=str,
        required=True,
        help="Directory containing paper_run_summary and paper_equity_curve artifacts.",
    )
    strategy_monitor_build.add_argument(
        "--execution-dir",
        type=str,
        default=None,
        help="Optional execution artifact directory for cost and rejection diagnostics.",
    )
    strategy_monitor_build.add_argument(
        "--allocation-dir",
        type=str,
        default=None,
        help="Optional multi-strategy allocation directory for sleeve attribution diagnostics.",
    )
    strategy_monitor_build.add_argument(
        "--policy-config", type=str, default=None, help="Optional strategy monitoring policy JSON/YAML file."
    )
    strategy_monitor_build.add_argument(
        "--output-dir", type=str, required=True, help="Directory where strategy monitoring artifacts will be written."
    )
    strategy_monitor_build.set_defaults(func=cmd_strategy_monitor_build)
    strategy_monitor_show = strategy_monitor_subparsers.add_parser(
        "show", help="Print a concise summary of a strategy monitoring artifact"
    )
    strategy_monitor_show.add_argument(
        "--monitoring", type=str, required=True, help="Path to strategy_monitoring.json or its parent directory."
    )
    strategy_monitor_show.set_defaults(func=cmd_strategy_monitor_show)
    strategy_monitor_recommend = strategy_monitor_subparsers.add_parser(
        "recommend-kill-switch", help="Filter and rewrite kill-switch recommendations from a monitoring snapshot"
    )
    strategy_monitor_recommend.add_argument(
        "--monitoring", type=str, required=True, help="Path to strategy_monitoring.json or its parent directory."
    )
    strategy_monitor_recommend.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Optional directory where recommendation artifacts should be written.",
    )
    strategy_monitor_recommend.add_argument(
        "--include-review",
        action="store_true",
        help="Include review-only recommendations alongside reduce/deactivate recommendations.",
    )
    strategy_monitor_recommend.set_defaults(func=cmd_strategy_monitor_recommend_kill_switch)

    adaptive_allocation_parser = subparsers.add_parser(
        "adaptive-allocation", help="Adapt multi-strategy capital weights using strategy monitoring outcomes"
    )
    adaptive_allocation_subparsers = adaptive_allocation_parser.add_subparsers(
        dest="adaptive_allocation_command", required=True
    )
    adaptive_allocation_build = adaptive_allocation_subparsers.add_parser(
        "build",
        help="Build an adaptive allocation snapshot from a strategy portfolio and strategy monitoring artifacts",
    )
    adaptive_allocation_build.add_argument(
        "--portfolio", type=str, required=True, help="Path to strategy_portfolio.json or its parent directory."
    )
    adaptive_allocation_build.add_argument(
        "--monitoring", type=str, required=True, help="Path to strategy_monitoring.json or its parent directory."
    )
    adaptive_allocation_build.add_argument(
        "--lifecycle",
        type=str,
        default=None,
        help="Optional strategy_lifecycle.json path or directory used to cap weights by governance state.",
    )
    adaptive_allocation_build.add_argument(
        "--regime",
        type=str,
        default=None,
        help="Optional market_regime.json path or directory used for regime-aware adjustments.",
    )
    adaptive_allocation_build.add_argument(
        "--use-regime", action="store_true", help="Apply regime-aware modifiers using --regime."
    )
    adaptive_allocation_build.add_argument(
        "--policy-config", type=str, default=None, help="Optional adaptive allocation policy JSON/YAML file."
    )
    adaptive_allocation_build.add_argument(
        "--output-dir", type=str, required=True, help="Directory where adaptive allocation artifacts will be written."
    )
    adaptive_allocation_build.add_argument(
        "--dry-run", action="store_true", help="Mark the adaptive allocation snapshot as dry-run output."
    )
    adaptive_allocation_build.set_defaults(func=cmd_adaptive_allocation_build)

    regime_parser = subparsers.add_parser("regime", help="Detect and inspect simple market regime artifacts")
    regime_subparsers = regime_parser.add_subparsers(dest="regime_command", required=True)
    regime_detect = regime_subparsers.add_parser(
        "detect", help="Detect a simple market regime from price or equity history"
    )
    regime_detect.add_argument(
        "--input", type=str, required=True, help="CSV file or directory containing price/equity history."
    )
    regime_detect.add_argument(
        "--policy-config", type=str, default=None, help="Optional market regime policy JSON/YAML file."
    )
    regime_detect.add_argument(
        "--output-dir", type=str, required=True, help="Directory where market regime artifacts will be written."
    )
    regime_detect.set_defaults(func=cmd_regime_detect)
    regime_show = regime_subparsers.add_parser("show", help="Print a concise summary of a market regime artifact")
    regime_show.add_argument(
        "--regime", type=str, required=True, help="Path to market_regime.json or its parent directory."
    )
    regime_show.set_defaults(func=cmd_regime_show)

    strategy_validation_parser = subparsers.add_parser(
        "strategy-validation", help="Build walk-forward validation snapshots from research manifests"
    )
    strategy_validation_subparsers = strategy_validation_parser.add_subparsers(
        dest="strategy_validation_command", required=True
    )
    strategy_validation_build = strategy_validation_subparsers.add_parser(
        "build", help="Build strategy validation artifacts from research manifests"
    )
    strategy_validation_build.add_argument(
        "--artifacts-root",
        type=str,
        default="artifacts",
        help="Root artifact directory to scan for research manifests.",
    )
    strategy_validation_build.add_argument(
        "--policy-config", type=str, default=None, help="Optional strategy validation policy JSON/YAML file."
    )
    strategy_validation_build.add_argument(
        "--output-dir", type=str, required=True, help="Directory where strategy validation artifacts will be written."
    )
    strategy_validation_build.set_defaults(func=cmd_strategy_validation_build)

    strategy_lifecycle_parser = subparsers.add_parser(
        "strategy-lifecycle", help="Inspect and update file-based strategy lifecycle state"
    )
    strategy_lifecycle_subparsers = strategy_lifecycle_parser.add_subparsers(
        dest="strategy_lifecycle_command", required=True
    )
    strategy_lifecycle_show = strategy_lifecycle_subparsers.add_parser(
        "show", help="Show a concise summary of a strategy lifecycle artifact"
    )
    strategy_lifecycle_show.add_argument(
        "--lifecycle", type=str, required=True, help="Path to strategy_lifecycle.json or its parent directory."
    )
    strategy_lifecycle_show.set_defaults(func=cmd_strategy_lifecycle_show)
    strategy_lifecycle_update = strategy_lifecycle_subparsers.add_parser(
        "update", help="Append or apply an explicit lifecycle state transition"
    )
    strategy_lifecycle_update.add_argument(
        "--lifecycle", type=str, required=True, help="Path to strategy_lifecycle.json or its parent directory."
    )
    strategy_lifecycle_update.add_argument(
        "--strategy-id", type=str, required=True, help="Strategy identifier or preset name to update."
    )
    strategy_lifecycle_update.add_argument(
        "--state",
        type=str,
        required=True,
        choices=["candidate", "validated", "promoted", "active", "under_review", "degraded", "demoted"],
        help="New lifecycle state.",
    )
    strategy_lifecycle_update.add_argument(
        "--reason", type=str, required=True, help="Human-readable reason for the transition."
    )
    strategy_lifecycle_update.add_argument(
        "--output-path",
        type=str,
        default=None,
        help="Optional alternate output path for the updated lifecycle artifact.",
    )
    strategy_lifecycle_update.set_defaults(func=cmd_strategy_lifecycle_update)

    strategy_governance_parser = subparsers.add_parser(
        "strategy-governance", help="Apply validation, monitoring, and allocation governance to promoted strategies"
    )
    strategy_governance_subparsers = strategy_governance_parser.add_subparsers(
        dest="strategy_governance_command", required=True
    )
    strategy_governance_apply = strategy_governance_subparsers.add_parser(
        "apply", help="Build strategy lifecycle and governance summary artifacts"
    )
    strategy_governance_apply.add_argument(
        "--promoted-dir", type=str, required=True, help="Directory containing promoted_strategies.json."
    )
    strategy_governance_apply.add_argument(
        "--validation", type=str, default=None, help="Optional strategy_validation.json path or directory."
    )
    strategy_governance_apply.add_argument(
        "--monitoring", type=str, default=None, help="Optional strategy_monitoring.json path or directory."
    )
    strategy_governance_apply.add_argument(
        "--adaptive-allocation", type=str, default=None, help="Optional adaptive_allocation.json path or directory."
    )
    strategy_governance_apply.add_argument(
        "--lifecycle", type=str, default=None, help="Optional persistent strategy_lifecycle.json path or directory."
    )
    strategy_governance_apply.add_argument(
        "--policy-config", type=str, default=None, help="Optional strategy governance policy JSON/YAML file."
    )
    strategy_governance_apply.add_argument(
        "--output-dir", type=str, required=True, help="Directory where governance artifacts will be written."
    )
    strategy_governance_apply.add_argument(
        "--dry-run", action="store_true", help="Evaluate governance without updating the persistent lifecycle artifact."
    )
    strategy_governance_apply.set_defaults(func=cmd_strategy_governance_apply)
    adaptive_allocation_show = adaptive_allocation_subparsers.add_parser(
        "show", help="Print a concise summary of an adaptive allocation artifact"
    )
    adaptive_allocation_show.add_argument(
        "--allocation", type=str, required=True, help="Path to adaptive_allocation.json or its parent directory."
    )
    adaptive_allocation_show.set_defaults(func=cmd_adaptive_allocation_show)
    adaptive_allocation_export = adaptive_allocation_subparsers.add_parser(
        "export-run-config",
        help="Export a runnable multi-strategy and pipeline config bundle from an adaptive allocation snapshot",
    )
    adaptive_allocation_export.add_argument(
        "--allocation", type=str, required=True, help="Path to adaptive_allocation.json or its parent directory."
    )
    adaptive_allocation_export.add_argument(
        "--output-dir", type=str, required=True, help="Directory where runnable config artifacts will be written."
    )
    adaptive_allocation_export.set_defaults(func=cmd_adaptive_allocation_export_run_config)

    doctor_parser = subparsers.add_parser("doctor", help="Run local environment, config, and artifact sanity checks")
    doctor_parser.add_argument("--artifacts-root", type=str, default="artifacts", help="Artifact root to inspect.")
    doctor_parser.add_argument(
        "--pipeline-config", type=str, default=None, help="Optional pipeline config to validate."
    )
    doctor_parser.add_argument(
        "--monitoring-config", type=str, default=None, help="Optional monitoring config to validate."
    )
    doctor_parser.add_argument(
        "--notification-config", type=str, default=None, help="Optional notification config to validate."
    )
    doctor_parser.add_argument(
        "--execution-config", type=str, default=None, help="Optional execution config to validate."
    )
    doctor_parser.add_argument("--broker-config", type=str, default=None, help="Optional broker config to validate.")
    doctor_parser.add_argument(
        "--dashboard-config", type=str, default=None, help="Optional dashboard config to validate."
    )
    doctor_parser.add_argument(
        "--check-integrations",
        action="store_true",
        help="Check optional external integration packages, example configs, and validation artifacts.",
    )
    doctor_parser.add_argument(
        "--output-dir",
        type=str,
        default="artifacts/system_check",
        help="Directory where doctor artifacts will be written.",
    )
    doctor_parser.set_defaults(func=cmd_doctor)

    orchestrate_parser = subparsers.add_parser("orchestrate", help="Automate the full research-to-monitoring pipeline")
    orchestrate_subparsers = orchestrate_parser.add_subparsers(dest="orchestrate_command", required=True)
    orchestrate_run = orchestrate_subparsers.add_parser(
        "run", help="Run the automated promotion, portfolio, paper, and monitoring pipeline"
    )
    orchestrate_run.add_argument(
        "--config", type=str, required=True, help="Path to the automated orchestration JSON/YAML config."
    )
    orchestrate_run.set_defaults(func=cmd_orchestrate_run)
    orchestrate_show = orchestrate_subparsers.add_parser(
        "show-run", help="Show a concise summary of an orchestration run artifact"
    )
    orchestrate_show.add_argument(
        "--run", type=str, required=True, help="Path to orchestration_run.json or its parent directory."
    )
    orchestrate_show.set_defaults(func=cmd_orchestrate_show_run)
    orchestrate_loop = orchestrate_subparsers.add_parser(
        "loop", help="Run the automated orchestration pipeline repeatedly with sleep intervals"
    )
    orchestrate_loop.add_argument(
        "--config", type=str, required=True, help="Path to the automated orchestration JSON/YAML config."
    )
    orchestrate_loop.add_argument(
        "--max-iterations", type=int, default=None, help="Optional maximum iterations before the loop exits."
    )
    orchestrate_loop.set_defaults(func=cmd_orchestrate_loop)

    system_eval_parser = subparsers.add_parser(
        "system-eval", help="Build and compare full-system evaluation artifacts across orchestration runs"
    )
    system_eval_subparsers = system_eval_parser.add_subparsers(dest="system_eval_command", required=True)
    system_eval_build = system_eval_subparsers.add_parser(
        "build", help="Scan orchestration runs and build system evaluation history artifacts"
    )
    system_eval_build.add_argument(
        "--runs-root", type=str, required=True, help="Root directory containing timestamped orchestration runs."
    )
    system_eval_build.add_argument(
        "--output-dir",
        type=str,
        required=True,
        help="Directory where system evaluation history artifacts will be written.",
    )
    system_eval_build.set_defaults(func=cmd_system_eval_build)
    system_eval_show = system_eval_subparsers.add_parser(
        "show", help="Print a concise summary of a system evaluation artifact"
    )
    system_eval_show.add_argument(
        "--evaluation", type=str, required=True, help="Path to system_evaluation.json or its parent directory."
    )
    system_eval_show.set_defaults(func=cmd_system_eval_show)
    system_eval_compare = system_eval_subparsers.add_parser(
        "compare", help="Compare system evaluation groups across run history"
    )
    system_eval_compare.add_argument(
        "--history",
        type=str,
        required=True,
        help="Path to system_evaluation_history.json, its parent directory, or an orchestration runs root.",
    )
    system_eval_compare.add_argument(
        "--output-dir", type=str, required=True, help="Directory where comparison artifacts will be written."
    )
    system_eval_compare.add_argument(
        "--latest-count",
        type=int,
        default=10,
        help="Number of most recent runs to include in group A when feature-flag grouping is not used.",
    )
    system_eval_compare.add_argument(
        "--previous-count",
        type=int,
        default=None,
        help="Optional number of older runs to include in group B when feature-flag grouping is not used.",
    )
    system_eval_compare.add_argument(
        "--feature-flag", type=str, default=None, help="Optional feature flag name used to split runs into A/B groups."
    )
    system_eval_compare.add_argument(
        "--group-by-field",
        type=str,
        default=None,
        choices=["variant_name", "experiment_name", "experiment_run_id"],
        help="Optional non-feature field used to split runs into A/B groups.",
    )
    system_eval_compare.add_argument(
        "--value-a", type=str, default="true", help="Feature-flag value for group A when --feature-flag is used."
    )
    system_eval_compare.add_argument(
        "--value-b", type=str, default="false", help="Feature-flag value for group B when --feature-flag is used."
    )
    system_eval_compare.set_defaults(func=cmd_system_eval_compare)

    experiment_parser = subparsers.add_parser(
        "experiment", help="Run and compare controlled orchestration experiment variants"
    )
    experiment_subparsers = experiment_parser.add_subparsers(dest="experiment_command", required=True)
    experiment_run = experiment_subparsers.add_parser(
        "run", help="Materialize orchestration variants and run a controlled experiment cohort"
    )
    experiment_run.add_argument("--config", type=str, required=True, help="Path to the experiment spec JSON/YAML file.")
    experiment_run.add_argument("--variants", nargs="*", default=None, help="Optional subset of variant names to run.")
    experiment_run.add_argument(
        "--dry-run",
        action="store_true",
        help="Only materialize per-variant orchestration configs without running them.",
    )
    experiment_run.set_defaults(func=cmd_experiment_run)
    experiment_show = experiment_subparsers.add_parser(
        "show", help="Show a concise summary of an experiment run artifact"
    )
    experiment_show.add_argument(
        "--run", type=str, required=True, help="Path to experiment_run.json or its parent directory."
    )
    experiment_show.set_defaults(func=cmd_experiment_show)
    experiment_compare = experiment_subparsers.add_parser(
        "compare", help="Compare experiment variants using system evaluation history"
    )
    experiment_compare.add_argument(
        "--run", type=str, required=True, help="Path to experiment_run.json or its parent directory."
    )
    experiment_compare.add_argument(
        "--output-dir", type=str, required=True, help="Directory where comparison artifacts will be written."
    )
    experiment_compare.add_argument("--variant-a", type=str, default=None, help="Optional variant name for group A.")
    experiment_compare.add_argument("--variant-b", type=str, default=None, help="Optional variant name for group B.")
    experiment_compare.set_defaults(func=cmd_experiment_compare)
    experiment_summary = experiment_subparsers.add_parser(
        "summarize-campaign", help="Summarize one or more experiment runs into a concise metric winner report"
    )
    experiment_summary.add_argument(
        "--runs", nargs="+", required=True, help="One or more experiment run directories or experiment_run.json paths."
    )
    experiment_summary.add_argument(
        "--output-dir", type=str, required=True, help="Directory where campaign summary artifacts will be written."
    )
    experiment_summary.set_defaults(func=cmd_experiment_summarize_campaign)
    experiment_recommend = experiment_subparsers.add_parser(
        "recommend-defaults", help="Turn campaign summary artifacts into recommended default settings"
    )
    experiment_recommend.add_argument(
        "--summary", type=str, required=True, help="Path to experiment_campaign_summary.json or its parent directory."
    )
    experiment_recommend.add_argument(
        "--output-dir", type=str, required=True, help="Directory where decision summary artifacts will be written."
    )
    experiment_recommend.add_argument(
        "--write-config",
        type=str,
        default=None,
        help="Optional output path for a recommended orchestration config snapshot.",
    )
    experiment_recommend.add_argument(
        "--base-config", type=str, default=None, help="Base orchestration config used when --write-config is provided."
    )
    experiment_recommend.set_defaults(func=cmd_experiment_recommend_defaults)

    return parser
