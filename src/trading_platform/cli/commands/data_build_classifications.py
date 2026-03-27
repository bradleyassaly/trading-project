from __future__ import annotations

from pathlib import Path

from trading_platform.cli.config_support import load_and_apply_workflow_config
from trading_platform.reference.classification_service import (
    build_classification_artifacts,
    resolve_classification_symbols,
)
from trading_platform.config.loader import load_classification_build_workflow_config


def cmd_data_build_classifications(args) -> None:
    load_and_apply_workflow_config(
        args,
        loader=load_classification_build_workflow_config,
    )
    symbols = resolve_classification_symbols(
        symbols=getattr(args, "symbols", None),
        universe=getattr(args, "universe", None),
    )
    paths = build_classification_artifacts(
        symbols=symbols,
        output_dir=Path(args.output_dir),
        as_of_date=getattr(args, "as_of_date", None),
    )
    print("Classification build complete.")
    print(f"Security master: {paths['security_master_path']}")
    print(f"Summary: {paths['classification_summary_path']}")
