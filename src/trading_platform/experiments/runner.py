from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.orchestration.pipeline_runner import (
    AutomatedOrchestrationConfig,
    AutomatedOrchestrationStageToggles,
    run_automated_orchestration,
)
from trading_platform.system_evaluation.service import (
    build_system_evaluation_history,
    compare_system_evaluations,
)

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None


EXPERIMENT_RUN_SCHEMA_VERSION = 1


def _now_utc() -> str:
    return datetime.now(UTC).isoformat()


def _sanitize_name(value: str) -> str:
    safe = "".join(character if character.isalnum() or character in {"-", "_"} else "_" for character in value.strip())
    return safe.strip("_") or "variant"


def _sanitize_timestamp(timestamp: str) -> str:
    return timestamp.replace(":", "-")


def _read_config_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    suffix = path.suffix.lower()
    if suffix == ".json":
        return json.loads(path.read_text(encoding="utf-8"))
    if suffix in {".yaml", ".yml"}:
        if yaml is None:
            raise ImportError("PyYAML is required for YAML config files. Install with `pip install pyyaml`.")
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
        return payload or {}
    raise ValueError(f"Unsupported experiment config file type: {suffix}")


def _write_payload(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    suffix = path.suffix.lower()
    if suffix == ".json":
        path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        return
    if suffix in {".yaml", ".yml"}:
        if yaml is None:
            raise ImportError("PyYAML is required for YAML config files. Install with `pip install pyyaml`.")
        path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
        return
    raise ValueError(f"Unsupported experiment config file type: {suffix}")


@dataclass(frozen=True)
class ExperimentVariantConfig:
    name: str
    feature_flags: dict[str, Any] = field(default_factory=dict)
    config_overrides: dict[str, Any] = field(default_factory=dict)
    stage_overrides: dict[str, bool] = field(default_factory=dict)
    repeat_count: int = 1
    tags: list[str] = field(default_factory=list)
    notes: str | None = None

    def __post_init__(self) -> None:
        if not self.name or not self.name.strip():
            raise ValueError("Experiment variant name must be non-empty")
        if self.repeat_count <= 0:
            raise ValueError("Experiment variant repeat_count must be > 0")


@dataclass(frozen=True)
class ExperimentSpecConfig:
    experiment_name: str
    base_orchestration_config_path: str
    output_root_dir: str = "artifacts/experiments"
    repeat_count: int = 1
    variants: list[ExperimentVariantConfig] = field(default_factory=list)
    run_label_metadata: dict[str, Any] = field(default_factory=dict)
    notes: str | None = None

    def __post_init__(self) -> None:
        if not self.experiment_name or not self.experiment_name.strip():
            raise ValueError("experiment_name must be non-empty")
        if not self.base_orchestration_config_path or not self.base_orchestration_config_path.strip():
            raise ValueError("base_orchestration_config_path must be non-empty")
        if self.repeat_count <= 0:
            raise ValueError("repeat_count must be > 0")
        if not self.variants:
            raise ValueError("At least one experiment variant is required")
        variant_names = [variant.name for variant in self.variants]
        if len(variant_names) != len(set(variant_names)):
            raise ValueError("Experiment variant names must be unique")


@dataclass(frozen=True)
class ExperimentVariantRunRecord:
    variant_name: str
    repeat_index: int
    status: str
    materialized_config_path: str
    experiment_run_id: str
    feature_flags: dict[str, Any]
    stage_overrides: dict[str, bool]
    run_dir: str | None = None
    orchestration_run_path: str | None = None
    system_evaluation_path: str | None = None
    warning_count: int = 0
    error_message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["feature_flags"] = json.dumps(self.feature_flags, sort_keys=True)
        payload["stage_overrides"] = json.dumps(self.stage_overrides, sort_keys=True)
        return payload


def load_experiment_spec_config(path: str | Path) -> ExperimentSpecConfig:
    data = _read_config_file(Path(path))
    payload = dict(data)
    payload["variants"] = [ExperimentVariantConfig(**item) for item in payload.get("variants", [])]
    return ExperimentSpecConfig(**payload)


def materialize_variant_orchestration_config(
    *,
    spec: ExperimentSpecConfig,
    variant: ExperimentVariantConfig,
    experiment_run_id: str,
    experiment_run_dir: str | Path,
    repeat_index: int = 1,
) -> tuple[AutomatedOrchestrationConfig, Path]:
    base_path = Path(spec.base_orchestration_config_path)
    base_payload = _read_config_file(base_path)
    merged = dict(base_payload)
    merged["experiment_name"] = spec.experiment_name
    merged["variant_name"] = variant.name
    merged["experiment_run_id"] = experiment_run_id
    merged["run_label_metadata"] = {
        **dict(merged.get("run_label_metadata") or {}),
        **dict(spec.run_label_metadata or {}),
        "experiment_name": spec.experiment_name,
        "variant_name": variant.name,
        "repeat_index": repeat_index,
    }
    merged["feature_flags"] = {
        **dict(merged.get("feature_flags") or {}),
        **dict(variant.feature_flags or {}),
    }
    merged["stages"] = {
        **dict(merged.get("stages") or {}),
        **dict(variant.stage_overrides or {}),
    }
    merged.update(dict(variant.config_overrides or {}))

    base_run_name = str(merged.get("run_name") or "automation")
    variant_slug = _sanitize_name(variant.name)
    repeat_suffix = f"__r{repeat_index:02d}" if repeat_index > 1 else ""
    merged["run_name"] = f"{base_run_name}__{variant_slug}{repeat_suffix}"
    merged["output_root_dir"] = str(Path(experiment_run_dir) / "variants" / variant_slug / f"repeat_{repeat_index:02d}" / "orchestration_runs")

    config = AutomatedOrchestrationConfig(
        **{
            **merged,
            "stages": AutomatedOrchestrationStageToggles(**dict(merged.get("stages") or {})),
        }
    )
    config_path = Path(experiment_run_dir) / "materialized_configs" / f"{variant_slug}__r{repeat_index:02d}{base_path.suffix or '.json'}"
    _write_payload(config_path, config.to_dict())
    return config, config_path


def _variant_rows_for_execution(
    spec: ExperimentSpecConfig,
    selected_variants: list[str] | None,
) -> list[tuple[ExperimentVariantConfig, int]]:
    selected_set = set(selected_variants or [])
    rows: list[tuple[ExperimentVariantConfig, int]] = []
    for variant in spec.variants:
        if selected_set and variant.name not in selected_set:
            continue
        for repeat_index in range(1, (spec.repeat_count * variant.repeat_count) + 1):
            rows.append((variant, repeat_index))
    if not rows:
        raise ValueError("No experiment variants selected")
    return rows


def _write_experiment_artifacts(
    *,
    spec: ExperimentSpecConfig,
    experiment_run_id: str,
    experiment_run_dir: Path,
    started_at: str,
    ended_at: str,
    status: str,
    dry_run: bool,
    variant_records: list[ExperimentVariantRunRecord],
    warnings: list[str],
    errors: list[dict[str, Any]],
    history_paths: dict[str, Any] | None,
) -> dict[str, str]:
    summary = {
        "variant_count": len({record.variant_name for record in variant_records}),
        "variant_run_count": len(variant_records),
        "succeeded_count": sum(1 for record in variant_records if record.status == "succeeded"),
        "failed_count": sum(1 for record in variant_records if record.status == "failed"),
        "dry_run_count": sum(1 for record in variant_records if record.status == "dry_run"),
        "warning_count": len(warnings),
    }
    payload = {
        "schema_version": EXPERIMENT_RUN_SCHEMA_VERSION,
        "experiment_name": spec.experiment_name,
        "experiment_run_id": experiment_run_id,
        "base_orchestration_config_path": spec.base_orchestration_config_path,
        "output_root_dir": spec.output_root_dir,
        "run_label_metadata": spec.run_label_metadata,
        "notes": spec.notes,
        "started_at": started_at,
        "ended_at": ended_at,
        "status": status,
        "dry_run": dry_run,
        "summary": summary,
        "variants": [asdict(record) for record in variant_records],
        "warnings": warnings,
        "errors": errors,
        "system_evaluation": history_paths or {},
    }
    json_path = experiment_run_dir / "experiment_run.json"
    csv_path = experiment_run_dir / "experiment_run.csv"
    json_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    pd.DataFrame([record.to_dict() for record in variant_records]).to_csv(csv_path, index=False)
    return {
        "experiment_run_json_path": str(json_path),
        "experiment_run_csv_path": str(csv_path),
        **(history_paths or {}),
    }


def run_experiment(
    *,
    spec: ExperimentSpecConfig,
    selected_variants: list[str] | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    started_at = _now_utc()
    experiment_run_id = _sanitize_timestamp(started_at)
    experiment_run_dir = Path(spec.output_root_dir) / _sanitize_name(spec.experiment_name) / experiment_run_id
    experiment_run_dir.mkdir(parents=True, exist_ok=True)

    variant_records: list[ExperimentVariantRunRecord] = []
    warnings: list[str] = []
    errors: list[dict[str, Any]] = []

    for variant, repeat_index in _variant_rows_for_execution(spec, selected_variants):
        materialized_config, config_path = materialize_variant_orchestration_config(
            spec=spec,
            variant=variant,
            experiment_run_id=experiment_run_id,
            experiment_run_dir=experiment_run_dir,
            repeat_index=repeat_index,
        )
        if dry_run:
            variant_records.append(
                ExperimentVariantRunRecord(
                    variant_name=variant.name,
                    repeat_index=repeat_index,
                    status="dry_run",
                    materialized_config_path=str(config_path),
                    experiment_run_id=experiment_run_id,
                    feature_flags=dict(sorted(materialized_config.feature_flags.items())),
                    stage_overrides=dict(sorted(variant.stage_overrides.items())),
                )
            )
            continue
        try:
            result, artifact_paths = run_automated_orchestration(materialized_config)
            variant_records.append(
                ExperimentVariantRunRecord(
                    variant_name=variant.name,
                    repeat_index=repeat_index,
                    status=result.status,
                    materialized_config_path=str(config_path),
                    experiment_run_id=experiment_run_id,
                    feature_flags=dict(sorted(materialized_config.feature_flags.items())),
                    stage_overrides=dict(sorted(variant.stage_overrides.items())),
                    run_dir=result.run_dir,
                    orchestration_run_path=str(artifact_paths["orchestration_run_json_path"]),
                    system_evaluation_path=str(artifact_paths.get("system_evaluation_json_path")) if artifact_paths.get("system_evaluation_json_path") else None,
                    warning_count=len(result.warnings),
                    error_message="; ".join(item["error_message"] for item in result.errors) if result.errors else None,
                )
            )
            if result.warnings:
                warnings.extend(f"{variant.name}: {warning}" for warning in result.warnings)
            if result.errors:
                errors.extend(
                    {
                        "variant_name": variant.name,
                        "repeat_index": repeat_index,
                        "stage_name": item["stage_name"],
                        "error_message": item["error_message"],
                    }
                    for item in result.errors
                )
        except Exception as exc:
            variant_records.append(
                ExperimentVariantRunRecord(
                    variant_name=variant.name,
                    repeat_index=repeat_index,
                    status="failed",
                    materialized_config_path=str(config_path),
                    experiment_run_id=experiment_run_id,
                    feature_flags=dict(sorted(materialized_config.feature_flags.items())),
                    stage_overrides=dict(sorted(variant.stage_overrides.items())),
                    warning_count=0,
                    error_message=f"{type(exc).__name__}: {exc}",
                )
            )
            errors.append(
                {
                    "variant_name": variant.name,
                    "repeat_index": repeat_index,
                    "stage_name": "experiment_runner",
                    "error_message": f"{type(exc).__name__}: {exc}",
                }
            )

    history_paths: dict[str, Any] | None = None
    if not dry_run:
        history_dir = experiment_run_dir / "system_evaluation"
        history_paths = build_system_evaluation_history(
            runs_root=experiment_run_dir / "variants",
            output_dir=history_dir,
        )
    ended_at = _now_utc()
    status = "failed" if errors else "succeeded"
    artifact_paths = _write_experiment_artifacts(
        spec=spec,
        experiment_run_id=experiment_run_id,
        experiment_run_dir=experiment_run_dir,
        started_at=started_at,
        ended_at=ended_at,
        status="dry_run" if dry_run and not errors else status,
        dry_run=dry_run,
        variant_records=variant_records,
        warnings=warnings,
        errors=errors,
        history_paths=history_paths,
    )
    return {
        "experiment_name": spec.experiment_name,
        "experiment_run_id": experiment_run_id,
        "run_dir": str(experiment_run_dir),
        "status": "dry_run" if dry_run and not errors else status,
        "variant_count": len({record.variant_name for record in variant_records}),
        "variant_run_count": len(variant_records),
        "artifact_paths": artifact_paths,
    }


def load_experiment_run(path_or_dir: str | Path) -> dict[str, Any]:
    path = Path(path_or_dir)
    if path.is_dir():
        path = path / "experiment_run.json"
    if not path.exists():
        raise FileNotFoundError(f"Experiment run artifact not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def compare_experiment_variants(
    *,
    experiment_run_path: str | Path,
    output_dir: str | Path,
    variant_a: str | None = None,
    variant_b: str | None = None,
) -> dict[str, Any]:
    payload = load_experiment_run(experiment_run_path)
    experiment_path = Path(experiment_run_path)
    experiment_dir = experiment_path if experiment_path.is_dir() else experiment_path.parent
    variants = sorted({str(item.get("variant_name")) for item in payload.get("variants", []) if item.get("variant_name")})
    if variant_a is None or variant_b is None:
        if len(variants) < 2:
            raise ValueError("At least two experiment variants are required for comparison")
        variant_a = variant_a or variants[0]
        variant_b = variant_b or variants[1]
    history_dir = Path(payload.get("system_evaluation", {}).get("system_evaluation_history_json_path") or experiment_dir / "system_evaluation")
    return compare_system_evaluations(
        history_path_or_root=history_dir,
        output_dir=output_dir,
        group_by_field="variant_name",
        value_a=variant_a,
        value_b=variant_b,
    )
