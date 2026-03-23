from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd


SYSTEM_EVALUATION_SCHEMA_VERSION = 1


def _now_utc() -> str:
    return datetime.now(UTC).isoformat()


def _safe_read_json(path: str | Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    file_path = Path(path)
    if not file_path.exists():
        return {}
    try:
        return json.loads(file_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _safe_read_csv(path: str | Path | None) -> pd.DataFrame:
    if path is None:
        return pd.DataFrame()
    file_path = Path(path)
    if not file_path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(file_path)
    except (pd.errors.EmptyDataError, pd.errors.ParserError):
        return pd.DataFrame()


def _parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(number):
        return None
    return number


def _candidate_files(root: Path, names: list[str]) -> list[Path]:
    matches: list[Path] = []
    for name in names:
        direct = root / name
        if direct.exists():
            matches.append(direct)
        matches.extend(root.rglob(name))
    seen: set[str] = set()
    unique: list[Path] = []
    for path in matches:
        key = str(path.resolve())
        if key not in seen:
            seen.add(key)
            unique.append(path)
    return unique


def _newest_path(paths: list[Path]) -> Path | None:
    existing = [path for path in paths if path.exists()]
    if not existing:
        return None
    return max(existing, key=lambda item: item.stat().st_mtime)


def _coerce_feature_flag(value: Any) -> Any:
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "false"}:
            return lowered == "true"
    return value


def _config_hash(config_payload: dict[str, Any]) -> str | None:
    if not config_payload:
        return None
    encoded = json.dumps(config_payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:12]


def _paper_equity_curve_path(run_dir: Path) -> Path | None:
    return _newest_path(_candidate_files(run_dir, ["paper_equity_curve.csv"]))


def _paper_summary_payload(run_dir: Path) -> dict[str, Any]:
    latest = _newest_path(_candidate_files(run_dir, ["paper_run_summary_latest.json"]))
    payload = _safe_read_json(latest)
    return payload.get("summary", payload)


def _execution_summary_payload(run_dir: Path) -> dict[str, Any]:
    latest = _newest_path(_candidate_files(run_dir, ["execution_summary.json"]))
    return _safe_read_json(latest)


def _regime_payload(run_dir: Path) -> dict[str, Any]:
    latest = _newest_path(_candidate_files(run_dir, ["market_regime.json"]))
    return _safe_read_json(latest)


def _governance_payload(run_dir: Path) -> dict[str, Any]:
    latest = _newest_path(_candidate_files(run_dir, ["strategy_governance_summary.json"]))
    return _safe_read_json(latest)


def _lifecycle_payload(run_dir: Path) -> dict[str, Any]:
    latest = _newest_path(_candidate_files(run_dir, ["strategy_lifecycle.json"]))
    return _safe_read_json(latest)


def _kill_switch_payload(run_dir: Path) -> dict[str, Any]:
    latest = _newest_path(_candidate_files(run_dir, ["kill_switch_recommendations.json"]))
    return _safe_read_json(latest)


def _equity_curve_metrics(equity_curve: pd.DataFrame) -> dict[str, float | None]:
    if equity_curve.empty:
        return {
            "total_return": None,
            "volatility": None,
            "sharpe": None,
            "max_drawdown": None,
            "observation_count": 0,
        }
    frame = equity_curve.copy()
    timestamp_col = "timestamp" if "timestamp" in frame.columns else "date" if "date" in frame.columns else None
    equity_col = "equity" if "equity" in frame.columns else "value" if "value" in frame.columns else None
    if equity_col is None:
        return {
            "total_return": None,
            "volatility": None,
            "sharpe": None,
            "max_drawdown": None,
            "observation_count": int(len(frame.index)),
        }
    if timestamp_col is not None:
        frame[timestamp_col] = pd.to_datetime(frame[timestamp_col], utc=True, errors="coerce")
        frame = frame.dropna(subset=[timestamp_col]).sort_values(timestamp_col)
    frame[equity_col] = pd.to_numeric(frame[equity_col], errors="coerce")
    frame = frame.dropna(subset=[equity_col])
    if frame.empty:
        return {
            "total_return": None,
            "volatility": None,
            "sharpe": None,
            "max_drawdown": None,
            "observation_count": 0,
        }
    series = frame[equity_col].astype(float)
    returns = series.pct_change().dropna()
    total_return = ((series.iloc[-1] / series.iloc[0]) - 1.0) if len(series) > 1 and series.iloc[0] != 0 else 0.0
    volatility = returns.std(ddof=0) * (252 ** 0.5) if not returns.empty else 0.0
    sharpe = ((returns.mean() / returns.std(ddof=0)) * (252 ** 0.5)) if not returns.empty and returns.std(ddof=0) > 0 else 0.0
    drawdowns = (series / series.cummax()) - 1.0
    max_drawdown = abs(float(drawdowns.min())) if not drawdowns.empty else 0.0
    return {
        "total_return": round(float(total_return), 10),
        "volatility": round(float(volatility), 10),
        "sharpe": round(float(sharpe), 10),
        "max_drawdown": round(float(max_drawdown), 10),
        "observation_count": int(len(series.index)),
    }


@dataclass(frozen=True)
class SystemEvaluationRow:
    run_id: str
    timestamp: str | None
    run_dir: str
    status: str
    run_name: str | None
    schedule_frequency: str | None
    experiment_name: str | None
    variant_name: str | None
    experiment_run_id: str | None
    config_hash: str | None
    strategy_count: int | None
    active_strategy_count: int | None
    promoted_strategy_count: int | None
    demoted_count: int | None
    warning_count: int | None
    kill_switch_count: int | None
    total_return: float | None
    volatility: float | None
    sharpe: float | None
    max_drawdown: float | None
    turnover: float | None
    regime: str | None
    adaptive_enabled: bool | None
    regime_enabled: bool | None
    no_op: bool
    insufficient_output_reason: str | None
    feature_flags: dict[str, Any]
    warnings: list[str]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["feature_flags"] = json.dumps(payload["feature_flags"], sort_keys=True)
        payload["warnings"] = "|".join(payload["warnings"])
        return payload


def evaluate_orchestration_run(*, run_dir: str | Path, output_dir: str | Path | None = None) -> dict[str, Any]:
    run_path = Path(run_dir)
    orchestration = _safe_read_json(run_path / "orchestration_run.json")
    if not orchestration:
        raise FileNotFoundError(f"orchestration_run.json not found under {run_path}")
    config_snapshot = _safe_read_json(run_path / "orchestration_config_snapshot.json")
    paper_summary = _paper_summary_payload(run_path)
    equity_curve = _safe_read_csv(_paper_equity_curve_path(run_path))
    execution_summary = _execution_summary_payload(run_path)
    governance = _governance_payload(run_path)
    lifecycle = _lifecycle_payload(run_path)
    kill_switch = _kill_switch_payload(run_path)
    regime = _regime_payload(run_path)
    metrics = _equity_curve_metrics(equity_curve)

    outputs = orchestration.get("outputs", {})
    lifecycle_summary = lifecycle.get("summary", {})
    governance_summary = governance.get("summary", governance)
    feature_flags = dict(sorted((orchestration.get("feature_flags") or config_snapshot.get("feature_flags") or {}).items()))
    stages = orchestration.get("stage_records", [])
    warning_count = len(orchestration.get("warnings", []))
    warning_count += sum(len(row.get("warnings", [])) for row in stages)
    adaptive_enabled = feature_flags.get("adaptive")
    if adaptive_enabled is None:
        adaptive_enabled = bool(config_snapshot.get("stages", {}).get("adaptive_allocation"))
    regime_enabled = feature_flags.get("regime")
    if regime_enabled is None:
        regime_enabled = bool(config_snapshot.get("stages", {}).get("regime"))
    turnover = _safe_float(
        paper_summary.get("turnover_after_execution_constraints")
        or execution_summary.get("turnover_after_constraints")
        or paper_summary.get("turnover_estimate")
    )
    row = SystemEvaluationRow(
        run_id=str(orchestration.get("run_id") or run_path.name),
        timestamp=orchestration.get("started_at") or orchestration.get("ended_at"),
        run_dir=str(run_path),
        status=str(orchestration.get("status") or "unknown"),
        run_name=orchestration.get("run_name"),
        schedule_frequency=orchestration.get("schedule_frequency"),
        experiment_name=orchestration.get("experiment_name") or config_snapshot.get("experiment_name"),
        variant_name=orchestration.get("variant_name") or config_snapshot.get("variant_name"),
        experiment_run_id=orchestration.get("experiment_run_id") or config_snapshot.get("experiment_run_id"),
        config_hash=_config_hash(config_snapshot),
        strategy_count=int(outputs.get("selected_strategy_count", 0) or lifecycle_summary.get("strategy_count", 0) or 0),
        active_strategy_count=int(lifecycle_summary.get("active_count", 0) or governance_summary.get("active_count", 0) or 0),
        promoted_strategy_count=int(outputs.get("promoted_strategy_count", 0) or 0),
        demoted_count=int(outputs.get("demoted_count", 0) or governance_summary.get("demoted_count", 0) or lifecycle_summary.get("demoted_count", 0) or 0),
        warning_count=warning_count,
        kill_switch_count=int(outputs.get("kill_switch_recommendation_count", 0) or kill_switch.get("summary", {}).get("recommendation_count", 0) or 0),
        total_return=metrics["total_return"],
        volatility=metrics["volatility"],
        sharpe=metrics["sharpe"],
        max_drawdown=metrics["max_drawdown"],
        turnover=turnover,
        regime=outputs.get("current_regime_label") or regime.get("latest", {}).get("regime_label"),
        adaptive_enabled=bool(adaptive_enabled) if adaptive_enabled is not None else None,
        regime_enabled=bool(regime_enabled) if regime_enabled is not None else None,
        no_op=bool(outputs.get("no_op", False)),
        insufficient_output_reason=outputs.get("no_op_reason") or outputs.get("skip_reason"),
        feature_flags=feature_flags,
        warnings=sorted(set(str(item) for item in orchestration.get("warnings", []))),
    )
    payload = {
        "schema_version": SYSTEM_EVALUATION_SCHEMA_VERSION,
        "generated_at": _now_utc(),
        "row": asdict(row),
        "metrics": metrics,
        "paper_equity_curve_path": str(_paper_equity_curve_path(run_path)) if _paper_equity_curve_path(run_path) is not None else None,
        "orchestration_run_path": str(run_path / "orchestration_run.json"),
    }
    if output_dir is not None:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        json_path = output_path / "system_evaluation.json"
        csv_path = output_path / "system_evaluation.csv"
        json_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        pd.DataFrame([row.to_dict()]).to_csv(csv_path, index=False)
        payload["system_evaluation_json_path"] = str(json_path)
        payload["system_evaluation_csv_path"] = str(csv_path)
    return payload


def load_system_evaluation(path_or_dir: str | Path) -> dict[str, Any]:
    path = Path(path_or_dir)
    if path.is_dir():
        path = path / "system_evaluation.json"
    payload = _safe_read_json(path)
    if not payload:
        raise FileNotFoundError(f"System evaluation artifact not found or invalid: {path}")
    return payload


def build_system_evaluation_history(*, runs_root: str | Path, output_dir: str | Path) -> dict[str, Any]:
    rows: list[SystemEvaluationRow] = []
    for orchestration_path in sorted(Path(runs_root).rglob("orchestration_run.json")):
        run_payload = evaluate_orchestration_run(run_dir=orchestration_path.parent)
        row_payload = run_payload["row"]
        rows.append(
            SystemEvaluationRow(
                run_id=str(row_payload["run_id"]),
                timestamp=row_payload.get("timestamp"),
                run_dir=str(row_payload["run_dir"]),
                status=str(row_payload["status"]),
                run_name=row_payload.get("run_name"),
                schedule_frequency=row_payload.get("schedule_frequency"),
                experiment_name=row_payload.get("experiment_name"),
                variant_name=row_payload.get("variant_name"),
                experiment_run_id=row_payload.get("experiment_run_id"),
                config_hash=row_payload.get("config_hash"),
                strategy_count=row_payload.get("strategy_count"),
                active_strategy_count=row_payload.get("active_strategy_count"),
                promoted_strategy_count=row_payload.get("promoted_strategy_count"),
                demoted_count=row_payload.get("demoted_count"),
                warning_count=row_payload.get("warning_count"),
                kill_switch_count=row_payload.get("kill_switch_count"),
                total_return=row_payload.get("total_return"),
                volatility=row_payload.get("volatility"),
                sharpe=row_payload.get("sharpe"),
                max_drawdown=row_payload.get("max_drawdown"),
                turnover=row_payload.get("turnover"),
                regime=row_payload.get("regime"),
                adaptive_enabled=row_payload.get("adaptive_enabled"),
                regime_enabled=row_payload.get("regime_enabled"),
                no_op=bool(row_payload.get("no_op", False)),
                insufficient_output_reason=row_payload.get("insufficient_output_reason"),
                feature_flags=row_payload.get("feature_flags", {}),
                warnings=row_payload.get("warnings", []),
            )
        )
    rows.sort(key=lambda row: str(row.timestamp or row.run_id), reverse=True)
    payload = {
        "schema_version": SYSTEM_EVALUATION_SCHEMA_VERSION,
        "generated_at": _now_utc(),
        "summary": {
            "run_count": len(rows),
            "best_run_id": next((row.run_id for row in sorted(rows, key=lambda item: item.total_return or float("-inf"), reverse=True)), None),
            "worst_run_id": next((row.run_id for row in sorted(rows, key=lambda item: item.total_return if item.total_return is not None else float("inf"))), None),
            "experiment_names": sorted({row.experiment_name for row in rows if row.experiment_name}),
            "variant_names": sorted({row.variant_name for row in rows if row.variant_name}),
        },
        "rows": [asdict(row) for row in rows],
    }
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    json_path = output_path / "system_evaluation_history.json"
    csv_path = output_path / "system_evaluation_history.csv"
    latest_json_path = output_path / "system_evaluation.json"
    latest_csv_path = output_path / "system_evaluation.csv"
    json_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    pd.DataFrame([row.to_dict() for row in rows]).to_csv(csv_path, index=False)
    if rows:
        latest_payload = {
            "schema_version": SYSTEM_EVALUATION_SCHEMA_VERSION,
            "generated_at": _now_utc(),
            "row": asdict(rows[0]),
            "metrics": {
                "total_return": rows[0].total_return,
                "volatility": rows[0].volatility,
                "sharpe": rows[0].sharpe,
                "max_drawdown": rows[0].max_drawdown,
                "observation_count": None,
            },
        }
        latest_json_path.write_text(json.dumps(latest_payload, indent=2, default=str), encoding="utf-8")
        pd.DataFrame([rows[0].to_dict()]).to_csv(latest_csv_path, index=False)
    return {
        "system_evaluation_json_path": str(latest_json_path),
        "system_evaluation_csv_path": str(latest_csv_path),
        "system_evaluation_history_json_path": str(json_path),
        "system_evaluation_history_csv_path": str(csv_path),
        "run_count": len(rows),
    }


def _history_rows(path_or_dir: str | Path) -> list[dict[str, Any]]:
    path = Path(path_or_dir)
    if path.is_dir() and (path / "system_evaluation_history.json").exists():
        payload = _safe_read_json(path / "system_evaluation_history.json")
        return payload.get("rows", [])
    if path.is_file():
        payload = _safe_read_json(path)
        if payload:
            return payload.get("rows", [])
    temp_rows: list[dict[str, Any]] = []
    for orchestration_path in sorted(path.rglob("orchestration_run.json")):
        temp_rows.append(evaluate_orchestration_run(run_dir=orchestration_path.parent)["row"])
    temp_rows.sort(key=lambda row: str(row.get("timestamp") or row.get("run_id")), reverse=True)
    return temp_rows


def _mean(values: list[float | None]) -> float | None:
    clean = [float(value) for value in values if value is not None]
    if not clean:
        return None
    return sum(clean) / len(clean)


def compare_system_evaluations(
    *,
    history_path_or_root: str | Path,
    output_dir: str | Path,
    latest_count: int = 10,
    previous_count: int | None = None,
    feature_flag: str | None = None,
    group_by_field: str | None = None,
    value_a: Any = True,
    value_b: Any = False,
) -> dict[str, Any]:
    rows = _history_rows(history_path_or_root)
    if not rows:
        raise ValueError("No system evaluation rows available for comparison")
    previous_count = previous_count or latest_count
    value_a = _coerce_feature_flag(value_a)
    value_b = _coerce_feature_flag(value_b)
    if feature_flag:
        group_a = [row for row in rows if _coerce_feature_flag((row.get("feature_flags") or {}).get(feature_flag)) == value_a]
        group_b = [row for row in rows if _coerce_feature_flag((row.get("feature_flags") or {}).get(feature_flag)) == value_b]
        label_a = f"{feature_flag}={value_a}"
        label_b = f"{feature_flag}={value_b}"
    elif group_by_field:
        group_a = [row for row in rows if _coerce_feature_flag(row.get(group_by_field)) == value_a]
        group_b = [row for row in rows if _coerce_feature_flag(row.get(group_by_field)) == value_b]
        label_a = f"{group_by_field}={value_a}"
        label_b = f"{group_by_field}={value_b}"
    else:
        sorted_rows = sorted(rows, key=lambda row: str(row.get("timestamp") or row.get("run_id")), reverse=True)
        group_a = sorted_rows[:latest_count]
        group_b = sorted_rows[latest_count : latest_count + previous_count]
        label_a = f"latest_{latest_count}"
        label_b = f"previous_{previous_count}"
    if not group_a or not group_b:
        raise ValueError("Comparison groups are empty")
    metrics = ["total_return", "volatility", "sharpe", "max_drawdown", "turnover", "warning_count", "kill_switch_count"]
    comparison_rows = []
    for metric in metrics:
        mean_a = _mean([row.get(metric) for row in group_a])
        mean_b = _mean([row.get(metric) for row in group_b])
        delta = (mean_a - mean_b) if mean_a is not None and mean_b is not None else None
        comparison_rows.append({"metric": metric, "group_a_mean": mean_a, "group_b_mean": mean_b, "delta": delta})
    payload = {
        "schema_version": SYSTEM_EVALUATION_SCHEMA_VERSION,
        "generated_at": _now_utc(),
        "comparison": {
            "group_a_label": label_a,
            "group_b_label": label_b,
            "group_a_count": len(group_a),
            "group_b_count": len(group_b),
            "feature_flag": feature_flag,
            "group_by_field": group_by_field,
        },
        "rows": comparison_rows,
        "group_a_run_ids": [row.get("run_id") for row in group_a],
        "group_b_run_ids": [row.get("run_id") for row in group_b],
    }
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    json_path = output_path / "system_evaluation_compare.json"
    md_path = output_path / "system_evaluation_compare.md"
    json_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    md_lines = [
        "# System Evaluation Comparison",
        "",
        f"- Group A: `{label_a}` ({len(group_a)} run(s))",
        f"- Group B: `{label_b}` ({len(group_b)} run(s))",
        "",
        "## Metric Means",
    ]
    md_lines.extend(
        [f"- `{row['metric']}`: group_a=`{row['group_a_mean']}` group_b=`{row['group_b_mean']}` delta=`{row['delta']}`" for row in comparison_rows]
    )
    md_path.write_text("\n".join(md_lines) + "\n", encoding="utf-8")
    return {
        "system_evaluation_compare_json_path": str(json_path),
        "system_evaluation_compare_md_path": str(md_path),
        "group_a_count": len(group_a),
        "group_b_count": len(group_b),
    }
