from __future__ import annotations

from pathlib import Path

import pandas as pd

from trading_platform.cli.config_support import load_and_apply_workflow_config
from trading_platform.config.loader import load_portfolio_optimizer_workflow_config
from trading_platform.portfolio.optimizer_adapters import (
    PortfolioOptimizerPolicyConfig,
    run_optimizer_experiment,
    write_optimizer_artifacts,
)


def cmd_portfolio_optimize_research(args) -> None:
    load_and_apply_workflow_config(
        args,
        loader=load_portfolio_optimizer_workflow_config,
    )
    if not getattr(args, "returns_path", None):
        raise ValueError("--returns-path is required")
    if not getattr(args, "output_dir", None):
        raise ValueError("--output-dir is required")
    returns_frame = pd.read_csv(args.returns_path)
    if "timestamp" in returns_frame.columns:
        returns_frame["timestamp"] = pd.to_datetime(returns_frame["timestamp"], errors="coerce")
        returns_frame = returns_frame.dropna(subset=["timestamp"]).set_index("timestamp")
    elif "date" in returns_frame.columns:
        returns_frame["date"] = pd.to_datetime(returns_frame["date"], errors="coerce")
        returns_frame = returns_frame.dropna(subset=["date"]).set_index("date")
    policy = PortfolioOptimizerPolicyConfig(
        optimizer_name=args.optimizer_name,
        fallback_optimizer_name=args.fallback_optimizer_name,
        risk_free_rate=args.risk_free_rate,
        min_history_rows=args.min_history_rows,
    )
    result = run_optimizer_experiment(
        returns_frame=returns_frame,
        policy=policy,
    )
    paths = write_optimizer_artifacts(
        result=result,
        output_dir=Path(args.output_dir),
    )
    print("Portfolio optimizer experiment complete.")
    print(f"Weights: {paths['optimizer_weights_path']}")
    print(f"Diagnostics: {paths['optimizer_diagnostics_path']}")
    print(f"Comparison: {paths['optimizer_weight_comparison_path']}")
