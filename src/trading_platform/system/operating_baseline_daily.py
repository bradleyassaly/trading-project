from __future__ import annotations

import argparse
import json
import traceback
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from trading_platform.config.loader import load_automated_orchestration_config
from trading_platform.dashboard.server import build_dashboard_static_data
from trading_platform.orchestration.pipeline_runner import run_automated_orchestration
from trading_platform.system_evaluation.service import (
    build_system_evaluation_history,
    load_system_evaluation,
)


DEFAULT_BASELINE_CONFIG = Path("configs/orchestration_operating_baseline.yaml")
DEFAULT_SUMMARY_DIR = Path("artifacts/operating_baseline_daily")
DEFAULT_DASHBOARD_OUTPUT_DIR = Path("artifacts/dashboard_data")


def _now_utc() -> str:
    return datetime.now(UTC).isoformat()


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def _write_markdown(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _stage_outputs(result: Any, stage_name: str) -> dict[str, Any]:
    for record in getattr(result, "stage_records", []):
        if getattr(record, "stage_name", None) == stage_name:
            outputs = getattr(record, "outputs", {})
            return outputs if isinstance(outputs, dict) else {}
    return {}


def _render_summary_markdown(summary: dict[str, Any]) -> str:
    eval_payload = summary.get("system_evaluation", {})
    latest_metrics = eval_payload.get("latest_metrics", {})
    history_metrics = eval_payload.get("history_metrics", {})
    paths = summary.get("paths", {})
    lines = [
        "# Daily Operating Baseline Summary",
        "",
        f"- Generated at: `{summary.get('generated_at')}`",
        f"- Run id: `{summary.get('run_id') or 'n/a'}`",
        f"- Run status: `{summary.get('run_status') or 'unknown'}`",
        f"- Promoted strategies: `{summary.get('promoted_strategy_count')}`",
        f"- Selected strategies: `{summary.get('selected_strategy_count')}`",
        f"- Paper orders: `{summary.get('paper_order_count')}`",
        f"- Monitoring warnings: `{summary.get('monitoring_warning_count')}`",
        "",
        "## System Evaluation",
        f"- Latest total return: `{latest_metrics.get('total_return')}`",
        f"- Latest sharpe: `{latest_metrics.get('sharpe')}`",
        f"- Latest max drawdown: `{latest_metrics.get('max_drawdown')}`",
        f"- Latest turnover: `{latest_metrics.get('turnover')}`",
        f"- History total return: `{history_metrics.get('total_return')}`",
        f"- History sharpe: `{history_metrics.get('sharpe')}`",
        f"- History max drawdown: `{history_metrics.get('max_drawdown')}`",
    ]
    metric_warnings = eval_payload.get("metric_warnings") or []
    history_metric_warnings = eval_payload.get("history_metric_warnings") or []
    if metric_warnings or history_metric_warnings:
        lines.extend(["", "## Warnings"])
        lines.extend([f"- latest: `{warning}`" for warning in metric_warnings])
        lines.extend([f"- history: `{warning}`" for warning in history_metric_warnings])
    if summary.get("error"):
        lines.extend(["", "## Error", f"- {summary['error']}"])
    lines.extend(
        [
            "",
            "## Paths",
            f"- Config: `{summary.get('config_path')}`",
            f"- Run dir: `{summary.get('run_dir')}`",
            f"- Orchestration run: `{paths.get('orchestration_run_json_path')}`",
            f"- System evaluation: `{paths.get('system_evaluation_json_path')}`",
            f"- System evaluation history: `{paths.get('system_evaluation_history_json_path')}`",
            f"- Log: `{summary.get('log_path') or 'n/a'}`",
        ]
    )
    if paths.get("dashboard_output_dir"):
        lines.append(f"- Dashboard output dir: `{paths['dashboard_output_dir']}`")
    return "\n".join(lines) + "\n"


def _build_summary(
    *,
    config_path: Path,
    result: Any,
    artifact_paths: dict[str, Path],
    system_eval_history: dict[str, Any],
    latest_eval_payload: dict[str, Any],
    dashboard_paths: dict[str, Path] | None,
    log_path: Path | None,
    summary_dir: Path,
) -> dict[str, Any]:
    paper_outputs = _stage_outputs(result, "paper")
    monitoring_outputs = _stage_outputs(result, "monitoring")
    latest_metrics = latest_eval_payload.get("metrics", {}) or {}
    history_metrics = latest_eval_payload.get("history_metrics", {}) or {}
    diagnostic = latest_eval_payload.get("diagnostic", {}) or {}
    return {
        "generated_at": _now_utc(),
        "config_path": str(config_path),
        "summary_dir": str(summary_dir),
        "run_id": getattr(result, "run_id", None),
        "run_name": getattr(result, "run_name", None),
        "run_status": getattr(result, "status", None),
        "run_dir": getattr(result, "run_dir", None),
        "log_path": str(log_path) if log_path is not None else None,
        "promoted_strategy_count": _safe_int(getattr(result, "outputs", {}).get("promoted_strategy_count")),
        "selected_strategy_count": _safe_int(getattr(result, "outputs", {}).get("selected_strategy_count")),
        "paper_order_count": _safe_int(paper_outputs.get("paper_order_count")),
        "monitoring_warning_count": _safe_int(
            monitoring_outputs.get("warning_strategy_count") or getattr(result, "outputs", {}).get("warning_strategy_count")
        ),
        "warning_count": len(getattr(result, "warnings", []) or []),
        "stage_outcomes": [
            {
                "stage_name": getattr(record, "stage_name", None),
                "status": getattr(record, "status", None),
                "warning_count": len(getattr(record, "warnings", []) or []),
                "error_message": getattr(record, "error_message", None),
            }
            for record in getattr(result, "stage_records", [])
        ],
        "system_evaluation": {
            "latest_metrics": {
                "total_return": latest_metrics.get("total_return"),
                "volatility": latest_metrics.get("volatility"),
                "sharpe": latest_metrics.get("sharpe"),
                "max_drawdown": latest_metrics.get("max_drawdown"),
                "turnover": latest_eval_payload.get("row", {}).get("turnover"),
                "observation_count": latest_metrics.get("observation_count"),
                "return_observation_count": latest_metrics.get("return_observation_count"),
            },
            "history_metrics": {
                "total_return": history_metrics.get("total_return"),
                "volatility": history_metrics.get("volatility"),
                "sharpe": history_metrics.get("sharpe"),
                "max_drawdown": history_metrics.get("max_drawdown"),
                "observation_count": history_metrics.get("observation_count"),
                "return_observation_count": history_metrics.get("return_observation_count"),
            },
            "metric_warnings": diagnostic.get("metric_warnings", []),
            "history_metric_warnings": diagnostic.get("history_metric_warnings", []),
            "run_count": system_eval_history.get("run_count"),
        },
        "paths": {
            "orchestration_run_json_path": str(artifact_paths.get("orchestration_run_json_path")) if artifact_paths.get("orchestration_run_json_path") else None,
            "orchestration_run_md_path": str(artifact_paths.get("orchestration_run_md_path")) if artifact_paths.get("orchestration_run_md_path") else None,
            "system_evaluation_json_path": system_eval_history.get("system_evaluation_json_path"),
            "system_evaluation_history_json_path": system_eval_history.get("system_evaluation_history_json_path"),
            "system_evaluation_history_csv_path": system_eval_history.get("system_evaluation_history_csv_path"),
            "dashboard_output_dir": str(dashboard_paths.get("dashboard_output_dir")) if dashboard_paths and dashboard_paths.get("dashboard_output_dir") else None,
        },
    }


def _write_summary(summary_dir: Path, summary: dict[str, Any]) -> dict[str, Path]:
    summary_dir.mkdir(parents=True, exist_ok=True)
    json_path = summary_dir / "daily_baseline_summary.json"
    md_path = summary_dir / "daily_baseline_summary.md"
    _write_json(json_path, summary)
    _write_markdown(md_path, _render_summary_markdown(summary))
    return {
        "daily_baseline_summary_json_path": json_path,
        "daily_baseline_summary_md_path": md_path,
    }


def run_operating_baseline_daily(
    *,
    config_path: str | Path = DEFAULT_BASELINE_CONFIG,
    summary_dir: str | Path = DEFAULT_SUMMARY_DIR,
    refresh_dashboard_static_data: bool = False,
    dashboard_artifacts_root: str | Path = "artifacts",
    dashboard_output_dir: str | Path = DEFAULT_DASHBOARD_OUTPUT_DIR,
    log_path: str | Path | None = None,
) -> dict[str, Any]:
    config_file = Path(config_path)
    summary_root = Path(summary_dir)
    log_file = Path(log_path) if log_path is not None else None
    dashboard_paths: dict[str, Path] | None = None
    try:
        config = load_automated_orchestration_config(config_file)
        result, artifact_paths = run_automated_orchestration(config)
        runs_root = Path(config.output_root_dir) / config.run_name
        system_eval_output_dir = Path(config.output_root_dir) / "system_eval_history"
        system_eval_history = build_system_evaluation_history(
            runs_root=runs_root,
            output_dir=system_eval_output_dir,
        )
        latest_eval_payload = load_system_evaluation(system_eval_output_dir)
        if refresh_dashboard_static_data:
            dashboard_paths = build_dashboard_static_data(
                artifacts_root=dashboard_artifacts_root,
                output_dir=dashboard_output_dir,
            )
            dashboard_paths["dashboard_output_dir"] = Path(dashboard_output_dir)
        summary = _build_summary(
            config_path=config_file,
            result=result,
            artifact_paths=artifact_paths,
            system_eval_history=system_eval_history,
            latest_eval_payload=latest_eval_payload,
            dashboard_paths=dashboard_paths,
            log_path=log_file,
            summary_dir=summary_root,
        )
        summary_paths = _write_summary(summary_root, summary)
        summary["paths"].update({key: str(value) for key, value in summary_paths.items()})
        _write_summary(summary_root, summary)
        return summary
    except Exception as exc:
        failure_summary = {
            "generated_at": _now_utc(),
            "config_path": str(config_file),
            "summary_dir": str(summary_root),
            "run_status": "failed",
            "log_path": str(log_file) if log_file is not None else None,
            "error": f"{type(exc).__name__}: {exc}",
            "traceback": traceback.format_exc(),
        }
        _write_summary(summary_root, failure_summary)
        raise


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the operating baseline once and refresh daily monitoring artifacts.")
    parser.add_argument("--config", default=str(DEFAULT_BASELINE_CONFIG), help="Path to the operating baseline orchestration config.")
    parser.add_argument("--summary-dir", default=str(DEFAULT_SUMMARY_DIR), help="Directory for daily summary artifacts.")
    parser.add_argument("--log-path", default=None, help="Optional log path to record in the summary artifact.")
    parser.add_argument(
        "--refresh-dashboard-static-data",
        action="store_true",
        help="Refresh dashboard static data after orchestration and system-eval complete.",
    )
    parser.add_argument("--dashboard-artifacts-root", default="artifacts", help="Artifacts root to scan for dashboard static data.")
    parser.add_argument("--dashboard-output-dir", default=str(DEFAULT_DASHBOARD_OUTPUT_DIR), help="Output directory for dashboard static data.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)
    summary = run_operating_baseline_daily(
        config_path=args.config,
        summary_dir=args.summary_dir,
        refresh_dashboard_static_data=args.refresh_dashboard_static_data,
        dashboard_artifacts_root=args.dashboard_artifacts_root,
        dashboard_output_dir=args.dashboard_output_dir,
        log_path=args.log_path,
    )
    print(f"Run id: {summary.get('run_id')}")
    print(f"Run status: {summary.get('run_status')}")
    print(f"Promoted strategies: {summary.get('promoted_strategy_count')}")
    print(f"Selected strategies: {summary.get('selected_strategy_count')}")
    print(f"Paper orders: {summary.get('paper_order_count')}")
    print(f"Monitoring warnings: {summary.get('monitoring_warning_count')}")
    print(f"Daily summary JSON: {summary.get('paths', {}).get('daily_baseline_summary_json_path')}")
    if summary.get("run_status") == "failed":
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
