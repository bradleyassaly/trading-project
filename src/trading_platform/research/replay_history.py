from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


REPLAY_HISTORY_SCHEMA_VERSION = "shared_replay_history_v1"


def _now_utc() -> str:
    return datetime.now(UTC).isoformat()


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _list_str(values: list[Any] | None) -> list[str]:
    return [str(value) for value in list(values or []) if str(value)]


def _candidate_id(
    *,
    providers: list[str],
    dataset_keys: list[str],
    alignment_mode: str | None,
    feature_column: str | None,
    target_column: str | None,
) -> str:
    provider_part = "+".join(sorted(set(providers))) or "unknown_provider"
    dataset_part = "+".join(sorted(set(dataset_keys))) or "unknown_dataset"
    alignment_part = alignment_mode or "unknown_alignment"
    feature_part = feature_column or "unknown_feature"
    target_part = target_column or "unknown_target"
    return "|".join([provider_part, dataset_part, alignment_part, feature_part, target_part])


def _record_id(
    *,
    source_type: str,
    source_generated_at: str | None,
    source_summary_path: str | None,
    candidate_id: str,
    comparison_rank: int | None,
) -> str:
    payload = "|".join(
        [
            source_type,
            source_generated_at or "",
            source_summary_path or "",
            candidate_id,
            str(comparison_rank) if comparison_rank is not None else "",
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _summary_paths(root: str | Path | None, pattern: str) -> list[Path]:
    if root is None:
        return []
    path = Path(root)
    if not path.exists():
        return []
    if path.is_file():
        return [path]
    return sorted(path.rglob(pattern))


@dataclass(frozen=True)
class ReplayHistoryRecord:
    record_id: str
    history_run_id: str
    recorded_at: str
    source_type: str
    source_name: str
    source_summary_path: str | None
    source_generated_at: str | None
    candidate_id: str
    providers: list[str] = field(default_factory=list)
    dataset_keys: list[str] = field(default_factory=list)
    alignment_mode: str | None = None
    comparison_mode: str | None = None
    feature_column: str | None = None
    target_column: str | None = None
    row_count: int | None = None
    pearson_correlation: float | None = None
    spearman_correlation: float | None = None
    directional_accuracy: float | None = None
    top_bottom_spread: float | None = None
    composite_score: float | None = None
    comparison_rank: int | None = None
    comparison_rank_percentile: float | None = None
    eligible: bool | None = None
    warnings: list[str] = field(default_factory=list)
    provenance: dict[str, Any] = field(default_factory=dict)
    schema_version: str = REPLAY_HISTORY_SCHEMA_VERSION

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "ReplayHistoryRecord":
        data = dict(payload or {})
        return cls(
            record_id=str(data["record_id"]),
            history_run_id=str(data["history_run_id"]),
            recorded_at=str(data["recorded_at"]),
            source_type=str(data["source_type"]),
            source_name=str(data["source_name"]),
            source_summary_path=str(data["source_summary_path"]) if data.get("source_summary_path") else None,
            source_generated_at=str(data["source_generated_at"]) if data.get("source_generated_at") else None,
            candidate_id=str(data["candidate_id"]),
            providers=_list_str(data.get("providers")),
            dataset_keys=_list_str(data.get("dataset_keys")),
            alignment_mode=str(data["alignment_mode"]) if data.get("alignment_mode") else None,
            comparison_mode=str(data["comparison_mode"]) if data.get("comparison_mode") else None,
            feature_column=str(data["feature_column"]) if data.get("feature_column") else None,
            target_column=str(data["target_column"]) if data.get("target_column") else None,
            row_count=_safe_int(data.get("row_count")),
            pearson_correlation=_safe_float(data.get("pearson_correlation")),
            spearman_correlation=_safe_float(data.get("spearman_correlation")),
            directional_accuracy=_safe_float(data.get("directional_accuracy")),
            top_bottom_spread=_safe_float(data.get("top_bottom_spread")),
            composite_score=_safe_float(data.get("composite_score")),
            comparison_rank=_safe_int(data.get("comparison_rank")),
            comparison_rank_percentile=_safe_float(data.get("comparison_rank_percentile")),
            eligible=bool(data["eligible"]) if data.get("eligible") is not None else None,
            warnings=_list_str(data.get("warnings")),
            provenance=dict(data.get("provenance") or {}),
            schema_version=str(data.get("schema_version") or REPLAY_HISTORY_SCHEMA_VERSION),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ReplayHistoryBuildResult:
    history_run_id: str
    recorded_at: str
    history_path: str
    latest_summary_path: str
    existing_record_count: int
    appended_record_count: int
    total_record_count: int
    discovered_evaluation_summaries: list[str] = field(default_factory=list)
    discovered_comparison_summaries: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def to_summary(self) -> dict[str, Any]:
        return {
            "schema_version": REPLAY_HISTORY_SCHEMA_VERSION,
            "history_run_id": self.history_run_id,
            "recorded_at": self.recorded_at,
            "history_path": self.history_path,
            "latest_summary_path": self.latest_summary_path,
            "existing_record_count": self.existing_record_count,
            "appended_record_count": self.appended_record_count,
            "total_record_count": self.total_record_count,
            "discovered_evaluation_summaries": list(self.discovered_evaluation_summaries),
            "discovered_comparison_summaries": list(self.discovered_comparison_summaries),
            "warnings": list(self.warnings),
        }


def load_replay_history(history_path: str | Path) -> list[ReplayHistoryRecord]:
    path = Path(history_path)
    if not path.exists():
        return []
    records: list[ReplayHistoryRecord] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            records.append(ReplayHistoryRecord.from_dict(json.loads(stripped)))
    return records


def filter_replay_history(
    records: list[ReplayHistoryRecord],
    *,
    candidate_id: str | None = None,
    provider: str | None = None,
    limit: int | None = None,
) -> list[ReplayHistoryRecord]:
    filtered = records
    if candidate_id:
        filtered = [record for record in filtered if record.candidate_id == candidate_id]
    if provider:
        filtered = [record for record in filtered if provider in record.providers]
    filtered = sorted(filtered, key=lambda record: (record.recorded_at, record.record_id), reverse=True)
    if limit is not None:
        return filtered[: max(int(limit), 0)]
    return filtered


def _providers_from_summary(summary: dict[str, Any]) -> list[str]:
    metadata = summary.get("consumer_summary", {}).get("metadata", {}) or {}
    request = summary.get("request", {}).get("consumer_request", {}) or {}
    providers = _list_str(metadata.get("providers") or request.get("providers") or [])
    if providers:
        return sorted(set(providers))
    datasets = metadata.get("datasets") or []
    return sorted({str(item.get("provider")) for item in datasets if item.get("provider")})


def _dataset_keys_from_summary(summary: dict[str, Any]) -> list[str]:
    metadata = summary.get("consumer_summary", {}).get("metadata", {}) or {}
    datasets = metadata.get("datasets") or []
    keys = [str(item.get("dataset_key")) for item in datasets if item.get("dataset_key")]
    if keys:
        return keys
    request = summary.get("request", {}).get("consumer_request", {}) or {}
    return _list_str(request.get("dataset_keys"))


def _load_summary(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _records_from_evaluation_summary(
    summary: dict[str, Any],
    *,
    history_run_id: str,
    recorded_at: str,
    summary_path: str | None,
) -> list[ReplayHistoryRecord]:
    providers = _providers_from_summary(summary)
    dataset_keys = _dataset_keys_from_summary(summary)
    request = summary.get("request", {}).get("consumer_request", {}) or {}
    alignment_mode = str(
        request.get("alignment_mode")
        or summary.get("consumer_summary", {}).get("metadata", {}).get("alignment_mode")
        or "unknown"
    )
    source_name = str(summary.get("evaluation_name") or "shared_replay_evaluation")
    source_generated_at = str(summary.get("generated_at")) if summary.get("generated_at") else None
    records: list[ReplayHistoryRecord] = []
    for metric in list(summary.get("metrics") or []):
        feature_column = str(metric.get("feature_column") or "")
        target_column = str(metric.get("target_column") or "")
        candidate_id = _candidate_id(
            providers=providers,
            dataset_keys=dataset_keys,
            alignment_mode=alignment_mode,
            feature_column=feature_column,
            target_column=target_column,
        )
        record_id = _record_id(
            source_type="evaluation",
            source_generated_at=source_generated_at,
            source_summary_path=summary_path,
            candidate_id=candidate_id,
            comparison_rank=None,
        )
        records.append(
            ReplayHistoryRecord(
                record_id=record_id,
                history_run_id=history_run_id,
                recorded_at=recorded_at,
                source_type="evaluation",
                source_name=source_name,
                source_summary_path=summary_path,
                source_generated_at=source_generated_at,
                candidate_id=candidate_id,
                providers=providers,
                dataset_keys=dataset_keys,
                alignment_mode=alignment_mode,
                feature_column=feature_column,
                target_column=target_column,
                row_count=_safe_int(metric.get("row_count")),
                pearson_correlation=_safe_float(metric.get("pearson_correlation")),
                spearman_correlation=_safe_float(metric.get("spearman_correlation")),
                directional_accuracy=_safe_float(metric.get("directional_accuracy")),
                top_bottom_spread=_safe_float(metric.get("top_bottom_spread")),
                warnings=_list_str(summary.get("warnings")),
                provenance={
                    "summary_path": summary_path,
                    "evaluation_name": source_name,
                },
            )
        )
    return records


def _records_from_comparison_summary(
    summary: dict[str, Any],
    *,
    history_run_id: str,
    recorded_at: str,
    summary_path: str | None,
) -> list[ReplayHistoryRecord]:
    rankings = list(summary.get("rankings") or [])
    total = len(rankings)
    source_generated_at = str(summary.get("generated_at")) if summary.get("generated_at") else None
    source_name = str(summary.get("comparison_name") or "shared_replay_comparison")
    comparison_mode = str(summary.get("request", {}).get("comparison_mode") or "provider")
    records: list[ReplayHistoryRecord] = []
    for index, ranking in enumerate(rankings, start=1):
        providers = _list_str(ranking.get("providers"))
        dataset_keys = _list_str(ranking.get("dataset_keys"))
        alignment_mode = str(ranking.get("alignment_mode") or "unknown")
        feature_column = str(ranking.get("feature_column") or "")
        target_column = str(ranking.get("target_column") or "")
        candidate_id = _candidate_id(
            providers=providers,
            dataset_keys=dataset_keys,
            alignment_mode=alignment_mode,
            feature_column=feature_column,
            target_column=target_column,
        )
        percentile = float(index) / float(total) if total else None
        record_id = _record_id(
            source_type="comparison",
            source_generated_at=source_generated_at,
            source_summary_path=summary_path,
            candidate_id=candidate_id,
            comparison_rank=index,
        )
        records.append(
            ReplayHistoryRecord(
                record_id=record_id,
                history_run_id=history_run_id,
                recorded_at=recorded_at,
                source_type="comparison",
                source_name=source_name,
                source_summary_path=summary_path,
                source_generated_at=source_generated_at,
                candidate_id=candidate_id,
                providers=providers,
                dataset_keys=dataset_keys,
                alignment_mode=alignment_mode,
                comparison_mode=comparison_mode,
                feature_column=feature_column,
                target_column=target_column,
                row_count=_safe_int(ranking.get("row_count")),
                pearson_correlation=_safe_float(ranking.get("pearson_correlation")),
                spearman_correlation=_safe_float(ranking.get("spearman_correlation")),
                directional_accuracy=_safe_float(ranking.get("directional_accuracy")),
                top_bottom_spread=_safe_float(ranking.get("top_bottom_spread")),
                composite_score=_safe_float(ranking.get("composite_score")),
                comparison_rank=index,
                comparison_rank_percentile=percentile,
                eligible=bool(ranking.get("eligible")) if ranking.get("eligible") is not None else None,
                warnings=_list_str(ranking.get("warnings")),
                provenance={
                    "summary_path": summary_path,
                    "comparison_name": source_name,
                    "evaluation_name": ranking.get("evaluation_name"),
                    "slice_summary_path": ranking.get("summary_path"),
                },
            )
        )
    return records


def update_shared_replay_history(
    *,
    output_dir: str | Path,
    evaluation_summary_paths: list[str] | None = None,
    comparison_summary_paths: list[str] | None = None,
    evaluation_root: str | Path | None = None,
    comparison_root: str | Path | None = None,
) -> ReplayHistoryBuildResult:
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    history_path = root / "shared_replay_history.jsonl"
    latest_summary_path = root / "latest_replay_history_summary.json"

    discovered_evaluation_paths = [Path(path) for path in list(evaluation_summary_paths or [])]
    if not discovered_evaluation_paths:
        discovered_evaluation_paths = _summary_paths(evaluation_root, "latest_replay_evaluation_summary.json")
    discovered_comparison_paths = [Path(path) for path in list(comparison_summary_paths or [])]
    if not discovered_comparison_paths:
        discovered_comparison_paths = _summary_paths(comparison_root, "latest_replay_comparison_summary.json")

    history_run_id = f"replay_history_{datetime.now(UTC).strftime('%Y%m%dT%H%M%S')}"
    recorded_at = _now_utc()
    existing_records = load_replay_history(history_path)
    existing_record_ids = {record.record_id for record in existing_records}
    warnings: list[str] = []
    appended_records: list[ReplayHistoryRecord] = []

    for path in discovered_evaluation_paths:
        try:
            summary = _load_summary(path)
        except (FileNotFoundError, json.JSONDecodeError) as exc:
            warnings.append(f"skipped_evaluation_summary:{path}:{exc}")
            continue
        for record in _records_from_evaluation_summary(
            summary,
            history_run_id=history_run_id,
            recorded_at=recorded_at,
            summary_path=str(path),
        ):
            if record.record_id in existing_record_ids:
                continue
            existing_record_ids.add(record.record_id)
            appended_records.append(record)

    for path in discovered_comparison_paths:
        try:
            summary = _load_summary(path)
        except (FileNotFoundError, json.JSONDecodeError) as exc:
            warnings.append(f"skipped_comparison_summary:{path}:{exc}")
            continue
        for record in _records_from_comparison_summary(
            summary,
            history_run_id=history_run_id,
            recorded_at=recorded_at,
            summary_path=str(path),
        ):
            if record.record_id in existing_record_ids:
                continue
            existing_record_ids.add(record.record_id)
            appended_records.append(record)

    if appended_records:
        with history_path.open("a", encoding="utf-8") as handle:
            for record in appended_records:
                handle.write(json.dumps(record.to_dict()) + "\n")

    result = ReplayHistoryBuildResult(
        history_run_id=history_run_id,
        recorded_at=recorded_at,
        history_path=str(history_path),
        latest_summary_path=str(latest_summary_path),
        existing_record_count=len(existing_records),
        appended_record_count=len(appended_records),
        total_record_count=len(existing_records) + len(appended_records),
        discovered_evaluation_summaries=[str(path) for path in discovered_evaluation_paths],
        discovered_comparison_summaries=[str(path) for path in discovered_comparison_paths],
        warnings=warnings,
    )
    latest_summary_path.write_text(json.dumps(result.to_summary(), indent=2), encoding="utf-8")
    return result
