from __future__ import annotations

from collections.abc import Callable
from typing import Any


DEFAULT_OPTION_ALIASES: dict[str, list[str]] = {
    "output": ["--output"],
    "output_dir": ["--output-dir"],
    "state_path": ["--state-path"],
    "execution_config": ["--execution-config"],
    "preset": ["--preset"],
    "symbols": ["--symbols"],
    "universe": ["--universe"],
    "strategy": ["--strategy"],
    "engine": ["--engine"],
    "start": ["--start"],
    "end": ["--end"],
    "top_n": ["--top-n"],
    "weighting_scheme": ["--weighting-scheme"],
    "lookback_bars": ["--lookback-bars"],
    "skip_bars": ["--skip-bars"],
    "rebalance_bars": ["--rebalance-bars"],
    "portfolio_construction_mode": ["--portfolio-construction-mode"],
    "max_position_weight": ["--max-position-weight"],
    "min_avg_dollar_volume": ["--min-avg-dollar-volume"],
    "max_names_per_sector": ["--max-names-per-sector"],
    "turnover_buffer_bps": ["--turnover-buffer-bps"],
    "max_turnover_per_rebalance": ["--max-turnover-per-rebalance"],
    "vol_lookback_bars": ["--vol-lookback-bars"],
    "benchmark": ["--benchmark"],
    "cost_bps": ["--cost-bps"],
    "train_bars": ["--train-bars"],
    "test_bars": ["--test-bars"],
    "step_bars": ["--step-bars"],
    "select_by": ["--select-by"],
    "min_train_rows": ["--min-train-rows"],
    "min_test_rows": ["--min-test-rows"],
    "broker": ["--broker"],
    "order_type": ["--order-type"],
    "time_in_force": ["--time-in-force"],
    "use_alpaca_latest_data": ["--use-alpaca-latest-data"],
    "latest_data_max_age_seconds": ["--latest-data-max-age-seconds"],
    "slippage_model": ["--slippage-model"],
    "slippage_buy_bps": ["--slippage-buy-bps"],
    "slippage_sell_bps": ["--slippage-sell-bps"],
    "ensemble_enabled": ["--enable-ensemble"],
    "ensemble_mode": ["--ensemble-mode"],
    "ensemble_weight_method": ["--ensemble-weight-method"],
    "ensemble_normalize_scores": ["--ensemble-normalize-scores"],
    "ensemble_max_members": ["--ensemble-max-members"],
    "ensemble_max_members_per_family": ["--ensemble-max-members-per-family"],
    "ensemble_minimum_member_observations": ["--ensemble-minimum-member-observations"],
    "ensemble_minimum_member_metric": ["--ensemble-minimum-member-metric"],
}


def explicit_options(args: Any) -> set[str]:
    argv = list(getattr(args, "_cli_argv", []) or [])
    return {token.split("=", 1)[0] for token in argv if token.startswith("--")}


def option_is_explicit(args: Any, attr_name: str, option_aliases: dict[str, list[str]] | None = None) -> bool:
    aliases = (option_aliases or DEFAULT_OPTION_ALIASES).get(
        attr_name,
        [f"--{attr_name.replace('_', '-')}"],
    )
    present = explicit_options(args)
    return any(alias in present for alias in aliases)


def apply_workflow_config(
    args: Any,
    *,
    config_path: str | None,
    loader: Callable[[str], Any],
    option_aliases: dict[str, list[str]] | None = None,
) -> Any | None:
    if not config_path:
        return None

    loaded = loader(config_path)
    for attr_name, value in loaded.to_cli_defaults().items():
        if not hasattr(args, attr_name):
            continue
        if option_is_explicit(args, attr_name, option_aliases=option_aliases):
            continue
        setattr(args, attr_name, value)
    return loaded
