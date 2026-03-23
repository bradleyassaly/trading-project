from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.experiments.runner import load_experiment_run


def _now_utc() -> str:
    return datetime.now(UTC).isoformat()


def _safe_read_json(path: str | Path) -> dict[str, Any]:
    file_path = Path(path)
    if not file_path.exists():
        return {}
    try:
        return json.loads(file_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _experiment_dir(path_or_dir: str | Path) -> Path:
    path = Path(path_or_dir)
    return path if path.is_dir() else path.parent


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _metric_mean(rows: list[dict[str, Any]], key: str) -> float | None:
    values = [_coerce_float(row.get(key)) for row in rows]
    clean = [value for value in values if value is not None]
    if not clean:
        return None
    return round(sum(clean) / len(clean), 10)


def _metric_sum(rows: list[dict[str, Any]], key: str) -> int:
    total = 0
    for row in rows:
        value = row.get(key)
        try:
            total += int(value or 0)
        except (TypeError, ValueError):
            continue
    return total


@dataclass(frozen=True)
class CampaignVariantSummary:
    experiment_name: str | None
    variant_name: str
    run_count: int
    total_return: float | None
    sharpe: float | None
    max_drawdown: float | None
    turnover: float | None
    promoted_strategy_count: int
    demoted_count: int
    active_strategy_count: int
    warning_count: int
    kill_switch_count: int
    regime_enabled: bool | None
    adaptive_enabled: bool | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _winner_map(rows: list[CampaignVariantSummary]) -> dict[str, list[str]]:
    if not rows:
        return {}
    metrics: dict[str, str] = {
        "total_return": "max",
        "sharpe": "max",
        "max_drawdown": "min",
        "turnover": "min",
        "promoted_strategy_count": "max",
        "demoted_count": "min",
        "active_strategy_count": "max",
        "warning_count": "min",
        "kill_switch_count": "min",
    }
    winners: dict[str, list[str]] = {}
    for metric, direction in metrics.items():
        values = [(row.variant_name, getattr(row, metric)) for row in rows if getattr(row, metric) is not None]
        if not values:
            winners[metric] = []
            continue
        best = max(value for _, value in values) if direction == "max" else min(value for _, value in values)
        winners[metric] = sorted(name for name, value in values if value == best)
    return winners


def build_experiment_campaign_summary(
    *,
    experiment_runs: list[str | Path],
    output_dir: str | Path,
) -> dict[str, Any]:
    if not experiment_runs:
        raise ValueError("At least one experiment run is required")

    grouped: dict[tuple[str | None, str], list[dict[str, Any]]] = {}
    included_runs: list[dict[str, Any]] = []
    for run in experiment_runs:
        experiment_payload = load_experiment_run(run)
        experiment_dir = _experiment_dir(run)
        history_path = experiment_payload.get("system_evaluation", {}).get("system_evaluation_history_json_path")
        if history_path is None:
            history_path = experiment_dir / "system_evaluation" / "system_evaluation_history.json"
        history_payload = _safe_read_json(history_path)
        rows = history_payload.get("rows", [])
        if not rows:
            raise ValueError(f"No system evaluation history rows found for experiment run: {experiment_dir}")
        included_runs.append(
            {
                "experiment_name": experiment_payload.get("experiment_name"),
                "experiment_run_id": experiment_payload.get("experiment_run_id"),
                "run_dir": str(experiment_dir),
                "variant_count": experiment_payload.get("summary", {}).get("variant_count", 0),
            }
        )
        for row in rows:
            variant_name = row.get("variant_name")
            if not variant_name:
                continue
            grouped.setdefault((row.get("experiment_name"), str(variant_name)), []).append(row)

    summaries = [
        CampaignVariantSummary(
            experiment_name=experiment_name,
            variant_name=variant_name,
            run_count=len(rows),
            total_return=_metric_mean(rows, "total_return"),
            sharpe=_metric_mean(rows, "sharpe"),
            max_drawdown=_metric_mean(rows, "max_drawdown"),
            turnover=_metric_mean(rows, "turnover"),
            promoted_strategy_count=_metric_sum(rows, "promoted_strategy_count"),
            demoted_count=_metric_sum(rows, "demoted_count"),
            active_strategy_count=_metric_sum(rows, "active_strategy_count"),
            warning_count=_metric_sum(rows, "warning_count"),
            kill_switch_count=_metric_sum(rows, "kill_switch_count"),
            regime_enabled=rows[0].get("regime_enabled"),
            adaptive_enabled=rows[0].get("adaptive_enabled"),
        )
        for (experiment_name, variant_name), rows in sorted(grouped.items(), key=lambda item: (str(item[0][0]), item[0][1]))
    ]
    winners = _winner_map(summaries)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": _now_utc(),
        "summary": {
            "experiment_run_count": len(included_runs),
            "variant_count": len(summaries),
        },
        "included_runs": included_runs,
        "variants": [summary.to_dict() for summary in summaries],
        "metric_winners": winners,
    }
    json_path = output_path / "experiment_campaign_summary.json"
    csv_path = output_path / "experiment_campaign_summary.csv"
    md_path = output_path / "experiment_campaign_summary.md"
    json_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    pd.DataFrame([summary.to_dict() for summary in summaries]).to_csv(csv_path, index=False)
    lines = [
        "# Experiment Campaign Summary",
        "",
        f"- Experiment runs: `{len(included_runs)}`",
        f"- Variants: `{len(summaries)}`",
        "",
        "## Variant Means",
    ]
    for summary in summaries:
        lines.append(
            f"- `{summary.variant_name}`: return=`{summary.total_return}` sharpe=`{summary.sharpe}` "
            f"drawdown=`{summary.max_drawdown}` turnover=`{summary.turnover}` warnings=`{summary.warning_count}`"
        )
    lines.extend(["", "## Metric Winners"])
    for metric, names in winners.items():
        lines.append(f"- `{metric}`: {', '.join(f'`{name}`' for name in names) if names else 'n/a'}")
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {
        "experiment_campaign_summary_json_path": str(json_path),
        "experiment_campaign_summary_csv_path": str(csv_path),
        "experiment_campaign_summary_md_path": str(md_path),
        "variant_count": len(summaries),
        "winners": winners,
    }
