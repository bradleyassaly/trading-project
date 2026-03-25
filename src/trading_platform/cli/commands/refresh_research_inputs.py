from __future__ import annotations

from pathlib import Path

from trading_platform.cli.config_support import apply_workflow_config
from trading_platform.cli.common import print_symbol_list, resolve_symbols
from trading_platform.config.loader import load_research_input_refresh_workflow_config
from trading_platform.services.research_input_refresh_service import (
    ResearchInputRefreshRequest,
    refresh_research_inputs,
)


def cmd_refresh_research_inputs(args) -> None:
    if getattr(args, "config", None):
        apply_workflow_config(
            args,
            config_path=args.config,
            loader=load_research_input_refresh_workflow_config,
        )
    symbols = resolve_symbols(args)
    request = ResearchInputRefreshRequest(
        symbols=symbols,
        feature_groups=getattr(args, "feature_groups", None),
        universe_name=getattr(args, "universe", None),
        sub_universe_id=getattr(args, "sub_universe_id", None),
        reference_data_root=getattr(args, "reference_data_root", None),
        universe_membership_path=getattr(args, "universe_membership_path", None),
        taxonomy_snapshot_path=getattr(args, "taxonomy_snapshot_path", None),
        benchmark_mapping_path=getattr(args, "benchmark_mapping_path", None),
        market_regime_path=getattr(args, "market_regime_path", None),
        group_map_path=getattr(args, "group_map_path", None),
        benchmark_id=getattr(args, "benchmark", None),
        feature_dir=Path(getattr(args, "feature_dir", "data/features")),
        metadata_dir=Path(getattr(args, "metadata_dir", "data/metadata")),
        normalized_dir=Path(getattr(args, "normalized_dir", "data/normalized")),
        failure_policy=getattr(args, "failure_policy", "partial_success"),
    )
    result = refresh_research_inputs(
        request=request,
    )
    print(
        "Refreshed research inputs for "
        f"{len(result.feature_symbols_built)}/{len(result.feature_symbols_requested)} symbol(s): "
        f"{print_symbol_list(result.feature_symbols_built) if result.feature_symbols_built else 'none'}"
    )
    print(f"Status: {result.status}")
    print(f"Features dir: {result.feature_dir}")
    print(f"Metadata dir: {result.metadata_dir}")
    if result.feature_failures:
        failed_symbols = [row["symbol"] for row in result.feature_failures]
        print(f"Failed symbols: {', '.join(failed_symbols)}")
    for key, path in sorted(result.paths.items()):
        print(f"{key}: {path}")
