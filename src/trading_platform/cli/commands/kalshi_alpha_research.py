"""
CLI handler for: trading-cli research kalshi-alpha

Runs alpha research on Kalshi prediction market feature data.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _load_kalshi_research_config(config_path: str | None) -> dict:
    if config_path is None:
        return {}
    path = Path(config_path)
    if not path.exists():
        print(f"Config file not found: {config_path}", file=sys.stderr)
        return {}
    try:
        import yaml  # type: ignore[import-untyped]
        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except ImportError:
        pass
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        print(f"Failed to parse config: {exc}", file=sys.stderr)
        return {}


def cmd_kalshi_alpha_research(args: argparse.Namespace) -> None:
    from trading_platform.kalshi.research import KalshiResearchConfig, run_kalshi_alpha_research
    from trading_platform.kalshi.signals import KALSHI_SIGNAL_FAMILY_NAMES

    raw = _load_kalshi_research_config(getattr(args, "config", None))
    paths_cfg = raw.get("paths", {})
    signals_cfg = raw.get("signals", {})
    backtest_cfg = raw.get("backtest", {})

    feature_dir = getattr(args, "feature_dir", None) or paths_cfg.get("feature_dir", "data/kalshi/features")
    output_dir = getattr(args, "output_dir", None) or paths_cfg.get("output_dir", "artifacts/kalshi_research")
    resolution_data_path = getattr(args, "resolution_data", None) or paths_cfg.get("resolution_data_path")
    run_backtest = getattr(args, "backtest", False) or backtest_cfg.get("enabled", False)
    forward_horizon = int(getattr(args, "forward_horizon", None) or signals_cfg.get("forward_horizon_bars", 10))
    min_rows = int(getattr(args, "min_rows", None) or signals_cfg.get("min_rows", 30))

    families_cfg = signals_cfg.get("families", list(KALSHI_SIGNAL_FAMILY_NAMES))
    signal_families = tuple(families_cfg)

    config = KalshiResearchConfig(
        feature_dir=feature_dir,
        output_dir=output_dir,
        signal_families=signal_families,
        resolution_data_path=resolution_data_path,
        run_backtest=run_backtest,
        forward_horizon_bars=forward_horizon,
        min_rows=min_rows,
    )

    print(f"Running Kalshi alpha research on: {feature_dir}")
    print(f"Signal families: {', '.join(signal_families)}")

    result = run_kalshi_alpha_research(config)

    print(f"\nRun ID: {result.run_id}")
    print(f"Output: {result.output_dir}")
    print(f"Markets analyzed: {result.signal_summary[0]['n_markets'] if result.signal_summary else 0}")
    print(f"\nSignal leaderboard:")
    for row in result.leaderboard:
        ic_str = f"{row['ic']:.4f}" if row['ic'] == row['ic'] else "n/a"
        wr_str = f"{row['win_rate']:.1%}" if row['win_rate'] == row['win_rate'] else "n/a"
        me_str = f"{row['mean_edge']:.3f}" if row['mean_edge'] == row['mean_edge'] else "n/a"
        print(f"  {row['signal_family']:<30}  IC={ic_str}  win_rate={wr_str}  mean_edge={me_str}")

    if result.best_family:
        print(f"\nBest family: {result.best_family}")

    for name, path in result.artifact_paths.items():
        print(f"  {name}: {path}")
