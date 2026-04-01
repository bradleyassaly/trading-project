from __future__ import annotations

import dataclasses

from trading_platform.orchestration.autonomous_loop import (
    AutonomousLoopConfig,
    load_autonomous_loop_config,
    run_autonomous_loop,
)


def cmd_autonomous_loop_start(args) -> None:
    config = load_autonomous_loop_config(args.config)

    # Apply CLI overrides on top of YAML config
    overrides: dict[str, object] = {}
    if getattr(args, "dry_run", False):
        overrides["dry_run"] = True
    if getattr(args, "max_iterations", None) is not None:
        overrides["max_iterations"] = args.max_iterations

    if overrides:
        config = dataclasses.replace(config, **overrides)

    run_autonomous_loop(config)
