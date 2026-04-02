"""
CLI command: trading-cli research kalshi-full-backtest

Runs the resolved-market Kalshi backtest framework on locally ingested
historical artifacts and writes structured research outputs.
"""
from __future__ import annotations

import argparse
import json
import math
import shutil
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from trading_platform.kalshi.signal_registry import known_kalshi_signal_families
from trading_platform.kalshi.validation import load_kalshi_validation_summary


def _load_yaml(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    cfg_path = Path(path)
    if not cfg_path.exists():
        return {}
    with cfg_path.open(encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def _validation_summary_path(raw_cfg: dict[str, Any], override: str | None) -> Path:
    if override:
        return Path(override)
    validation_cfg = raw_cfg.get("data_validation", {})
    output_dir = Path(validation_cfg.get("output_dir", "data/kalshi/validation"))
    output_dir = output_dir if output_dir.is_absolute() else Path.cwd() / output_dir
    return output_dir / "kalshi_data_validation_summary.json"

def cmd_kalshi_full_backtest(args: argparse.Namespace) -> None:
    from trading_platform.kalshi.backtest import KalshiBacktester

    raw_cfg = _load_yaml(getattr(args, "config", None))
    paths_cfg = raw_cfg.get("paths", {})
    signals_cfg = raw_cfg.get("signals", {})
    backtest_cfg = raw_cfg.get("backtest", {})
    informed_flow_cfg = signals_cfg.get("informed_flow", {})

    feature_dir = Path(getattr(args, "feature_dir", None) or paths_cfg.get("feature_dir", "data/kalshi/features/real"))
    resolution_path = Path(
        getattr(args, "resolution_data", None)
        or paths_cfg.get("resolution_data_path")
        or "data/kalshi/resolution.csv"
    )
    output_dir = Path(getattr(args, "output_dir", None) or paths_cfg.get("output_dir", "artifacts/kalshi_research"))
    raw_markets_dir = Path(
        getattr(args, "raw_markets_dir", None) or paths_cfg.get("raw_markets_dir", "data/kalshi/raw/markets")
    )
    validation_summary_path = _validation_summary_path(raw_cfg, getattr(args, "validation_summary", None))
    output_dir.mkdir(parents=True, exist_ok=True)

    all_families = known_kalshi_signal_families(informed_flow_config=informed_flow_cfg)
    configured_family_names = tuple(signals_cfg.get("families", ())) or tuple(all_families)
    signal_families = [all_families[name] for name in configured_family_names if name in all_families]
    if not signal_families:
        signal_families = list(all_families.values())

    entry_threshold = float(getattr(args, "entry_threshold", None) or backtest_cfg.get("entry_threshold", 0.5))
    long_only = bool(getattr(args, "long_only", False) or backtest_cfg.get("long_only", False))
    entry_timing_mode = str(
        getattr(args, "entry_timing_mode", None) or backtest_cfg.get("entry_timing_mode", "hours_before_close")
    )
    entry_offset_hours = float(
        getattr(args, "entry_offset_hours", None) or backtest_cfg.get("entry_offset_hours", 24.0)
    )
    holding_window_raw = getattr(args, "holding_window_hours", None)
    if holding_window_raw is None:
        holding_window_raw = backtest_cfg.get("holding_window_hours")
    holding_window_hours = float(holding_window_raw) if holding_window_raw is not None else None
    entry_slippage_points = float(
        getattr(args, "entry_slippage_points", None) or backtest_cfg.get("entry_slippage_points", 0.0)
    )
    exit_slippage_points = float(
        getattr(args, "exit_slippage_points", None) or backtest_cfg.get("exit_slippage_points", 0.0)
    )
    signal_probability_scale = float(
        getattr(args, "signal_probability_scale", None) or backtest_cfg.get("signal_probability_scale", 8.0)
    )

    print("Kalshi Resolved-Market Backtest")
    print(f"  feature dir             : {feature_dir}")
    print(f"  resolution data         : {resolution_path}")
    print(f"  raw markets dir         : {raw_markets_dir}")
    print(f"  output dir              : {output_dir}")
    print(f"  signal families         : {', '.join(family.name for family in signal_families)}")
    print(f"  entry threshold         : {entry_threshold}")
    print(f"  long only               : {long_only}")
    print(f"  entry timing mode       : {entry_timing_mode}")
    print(f"  entry offset hours      : {entry_offset_hours}")
    print(f"  holding window hours    : {holding_window_hours if holding_window_hours is not None else 'resolution'}")
    print(f"  entry slippage points   : {entry_slippage_points}")
    print(f"  exit slippage points    : {exit_slippage_points}")
    print(f"  signal prob scale       : {signal_probability_scale}")
    if getattr(args, "require_validation_pass", False):
        print(f"  validation summary      : {validation_summary_path}")

    if not feature_dir.exists():
        print(f"\n[ERROR] Feature directory not found: {feature_dir}")
        print("Run 'trading-cli data kalshi historical-ingest' first.")
        return

    resolution_data = pd.DataFrame()
    if resolution_path.exists():
        resolution_data = pd.read_csv(resolution_path)
    else:
        print(f"\n[ERROR] Resolution data not found: {resolution_path}")
        print("Run 'trading-cli data kalshi historical-ingest' first.")
        return
    if getattr(args, "require_validation_pass", False):
        try:
            validation_summary = load_kalshi_validation_summary(validation_summary_path)
        except FileNotFoundError:
            print(f"\n[ERROR] Validation summary not found: {validation_summary_path}")
            print("Run 'trading-cli data kalshi validate-dataset' first.")
            return
        if not bool(validation_summary.get("passed")):
            print(
                f"\n[ERROR] Kalshi dataset validation is not passing "
                f"(status={validation_summary.get('status', 'unknown')})."
            )
            print(f"Review: {validation_summary_path}")
            return

    backtester = KalshiBacktester(
        entry_threshold=entry_threshold,
        long_only=long_only,
        entry_timing_mode=entry_timing_mode,
        entry_offset_hours=entry_offset_hours,
        holding_window_hours=holding_window_hours,
        entry_slippage_points=entry_slippage_points,
        exit_slippage_points=exit_slippage_points,
        signal_probability_scale=signal_probability_scale,
    )

    family_results = backtester.run(
        feature_dir=feature_dir,
        resolution_data=resolution_data,
        signal_families=signal_families,
        output_dir=output_dir,
        raw_markets_dir=raw_markets_dir,
    )

    summary_path = output_dir / "kalshi_backtest_summary.json"
    diagnostics_path = output_dir / "kalshi_signal_diagnostics.json"
    trade_log_path = output_dir / "kalshi_trade_log.jsonl"
    report_path = output_dir / "kalshi_backtest_report.md"

    compatibility_csv = output_dir / "full_backtest_results.csv"
    compatibility_md = output_dir / "full_backtest_summary.md"
    if (output_dir / "backtest_results.csv").exists():
        shutil.copy(output_dir / "backtest_results.csv", compatibility_csv)
    if report_path.exists():
        shutil.copy(report_path, compatibility_md)

    summary_payload = json.loads(summary_path.read_text(encoding="utf-8")) if summary_path.exists() else {}

    print("\nBacktest complete.")
    print(f"  Markets evaluated        : {summary_payload.get('total_markets_evaluated', 0)}")
    print(f"  Candidate signals        : {summary_payload.get('total_candidate_signals', 0)}")
    print(f"  Executed trades          : {summary_payload.get('total_executed_trades', 0)}")
    print(f"  Win rate                 : {_format_metric(summary_payload.get('win_rate'), '.1%')}")
    print(f"  Average predicted edge   : {_format_metric(summary_payload.get('average_predicted_edge'), '.4f')}")
    print(f"  Average confidence       : {_format_metric(summary_payload.get('average_confidence'), '.4f')}")
    print(f"  Realized average return  : {_format_metric(summary_payload.get('realized_average_return'), '.4f')}")
    print(f"  Brier score              : {_format_metric(summary_payload.get('brier_score'), '.4f')}")
    print("\nPer-family results:")
    for result in family_results:
        print(
            f"  - {result.signal_family}: trades={result.n_trades}, "
            f"win_rate={_format_metric(result.win_rate, '.1%')}, "
            f"avg_return={_format_metric(result.realized_avg_return, '.4f')}, "
            f"brier={_format_metric(result.brier_score, '.4f')}"
        )

    print("\nArtifacts written:")
    print(f"  Summary     : {summary_path}")
    print(f"  Diagnostics : {diagnostics_path}")
    print(f"  Trade Log   : {trade_log_path}")
    print(f"  Report      : {report_path}")
    if compatibility_csv.exists():
        print(f"  Compat CSV  : {compatibility_csv}")
    if compatibility_md.exists():
        print(f"  Compat MD   : {compatibility_md}")


def _format_metric(value: Any, spec: str) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "n/a"
    if math.isnan(number):
        return "n/a"
    return format(number, spec)
