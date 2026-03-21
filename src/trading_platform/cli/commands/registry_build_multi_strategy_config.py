from __future__ import annotations

from trading_platform.governance.models import RegistrySelectionOptions
from trading_platform.governance.persistence import load_strategy_registry
from trading_platform.governance.service import (
    build_multi_strategy_config_from_registry,
    write_registry_backed_multi_strategy_artifacts,
)


def cmd_registry_build_multi_strategy_config(args) -> None:
    registry = load_strategy_registry(args.registry)
    include_statuses = ["approved", "paper"] if args.include_paper else ["approved"]
    options = RegistrySelectionOptions(
        include_statuses=include_statuses,
        universe=getattr(args, "universe", None),
        family=getattr(args, "family", None),
        tag=getattr(args, "tag", None),
        deployment_stage=getattr(args, "deployment_stage", None),
        max_strategies=getattr(args, "max_strategies", None),
        weighting_scheme=getattr(args, "weighting_scheme", "equal"),
    )
    config, comparison_rows = build_multi_strategy_config_from_registry(
        registry=registry,
        options=options,
    )
    paths = write_registry_backed_multi_strategy_artifacts(
        config=config,
        family_rows=comparison_rows,
        output_path=args.output_path,
    )

    print(f"Generated sleeves: {len(config.sleeves)}")
    print("Artifacts:")
    for name, path in sorted(paths.items()):
        print(f"  {name}: {path}")
