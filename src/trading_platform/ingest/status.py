from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from typing import Any

logger = logging.getLogger(__name__)

RUN_STATUSES = {"pending", "running", "completed", "failed"}
STAGE_STATUSES = {"pending", "running", "completed", "failed", "skipped"}


def _now_utc() -> str:
    return datetime.now(UTC).isoformat()


@dataclass
class IngestStageStatus:
    stage_name: str
    status: str = "pending"
    started_at: str | None = None
    updated_at: str | None = None
    ended_at: str | None = None
    elapsed_seconds: float | None = None
    item_count_total: int = 0
    item_count_completed: int = 0
    item_count_failed: int = 0
    items_per_second: float | None = None
    last_progress_message: str | None = None
    counters: dict[str, Any] = field(default_factory=dict)
    error_summary: str | None = None
    _started_clock: float | None = field(default=None, repr=False, compare=False)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload.pop("_started_clock", None)
        return payload


@dataclass
class IngestRunStatus:
    run_id: str
    pipeline_name: str
    mode: str
    lookback_days: int
    overall_status: str = "pending"
    current_stage: str | None = None
    started_at: str | None = None
    updated_at: str | None = None
    ended_at: str | None = None
    elapsed_seconds: float | None = None
    pages_seen: int = 0
    pages_with_retained_markets: int = 0
    pages_without_retained_markets: int = 0
    retained_markets_seen: int = 0
    retained_markets_started: int = 0
    markets_completed: int = 0
    markets_failed: int = 0
    raw_market_files_written: int = 0
    normalized_outputs_written: int = 0
    processed_ticker_count: int = 0
    fail_fast_triggered: bool = False
    fail_fast_reason: str | None = None
    stop_reason: str | None = None
    first_retained_processing_ticker: str | None = None
    resumed_from_run_id: str | None = None
    resumed_from_checkpoint: str | None = None
    resumed_stage: str | None = None
    resumed_processed_ticker_count: int = 0
    replayed_work_skipped: int = 0
    replayed_work_replayed: int = 0
    configured_resume_recovery_mode: str | None = None
    resume_cursor: str | None = None
    resume_cursor_retry_count: int = 0
    resume_cursor_last_http_status: int | None = None
    resume_recovery_action: str | None = None
    resumed_from_backup_checkpoint: bool = False
    resumed_with_cursor_reset: bool = False
    backup_checkpoint_recovery_attempted: bool = False
    cursor_reset_recovery_attempted: bool = False
    stages: list[IngestStageStatus] = field(default_factory=list)
    _started_clock: float | None = field(default=None, repr=False, compare=False)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["stages"] = [stage.to_dict() for stage in self.stages]
        payload.pop("_started_clock", None)
        return payload


class IngestStatusTracker:
    def __init__(
        self,
        *,
        run_id: str,
        pipeline_name: str,
        mode: str,
        lookback_days: int,
        stage_names: list[str],
        output_root: Path,
        log_prefix: str | None = None,
    ) -> None:
        self.output_root = output_root
        self.output_root.mkdir(parents=True, exist_ok=True)
        self.status_path = self.output_root / "ingest_status.json"
        self.summary_path = self.output_root / "ingest_run_summary.json"
        self.log_prefix = log_prefix or pipeline_name
        self.run = IngestRunStatus(
            run_id=run_id,
            pipeline_name=pipeline_name,
            mode=mode,
            lookback_days=lookback_days,
            stages=[IngestStageStatus(stage_name=name) for name in stage_names],
        )
        self._stage_index = {stage.stage_name: stage for stage in self.run.stages}

    def _write_json(self, path: Path, payload: dict[str, Any]) -> None:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        except Exception as exc:  # pragma: no cover - resilience path
            logger.warning("[%s] failed to write status artifact %s: %s", self.log_prefix, path, exc)

    def _refresh_run_timing(self) -> None:
        now = _now_utc()
        self.run.updated_at = now
        if self.run._started_clock is not None:
            self.run.elapsed_seconds = round(perf_counter() - self.run._started_clock, 6)

    def _refresh_stage_timing(self, stage: IngestStageStatus) -> None:
        stage.updated_at = _now_utc()
        if stage._started_clock is not None:
            stage.elapsed_seconds = round(perf_counter() - stage._started_clock, 6)
            processed = stage.item_count_completed + stage.item_count_failed
            if stage.elapsed_seconds > 0:
                stage.items_per_second = round(processed / stage.elapsed_seconds, 6)

    def snapshot(self) -> dict[str, Any]:
        self._refresh_run_timing()
        for stage in self.run.stages:
            if stage.status == "running":
                self._refresh_stage_timing(stage)
        payload = self.run.to_dict()
        self._write_json(self.status_path, payload)
        return payload

    def start_run(self, *, current_stage: str | None = None) -> None:
        self.run.overall_status = "running"
        self.run.started_at = _now_utc()
        self.run.updated_at = self.run.started_at
        self.run.current_stage = current_stage
        self.run._started_clock = perf_counter()
        self.snapshot()

    def set_run_counters(self, **updates: Any) -> None:
        for key, value in updates.items():
            if hasattr(self.run, key):
                setattr(self.run, key, value)
        self.snapshot()

    def increment_run_counters(self, **increments: int) -> None:
        for key, value in increments.items():
            if hasattr(self.run, key):
                current = getattr(self.run, key)
                if isinstance(current, (int, float)):
                    setattr(self.run, key, current + value)
        self.snapshot()

    def start_stage(
        self,
        stage_name: str,
        *,
        current_stage: bool = True,
        item_count_total: int | None = None,
        message: str | None = None,
        counters: dict[str, Any] | None = None,
    ) -> None:
        stage = self._stage_index[stage_name]
        if stage.status == "pending":
            stage.started_at = _now_utc()
            stage._started_clock = perf_counter()
        stage.status = "running"
        if item_count_total is not None:
            stage.item_count_total = item_count_total
        if message:
            stage.last_progress_message = message
        if counters:
            stage.counters.update(counters)
        self._refresh_stage_timing(stage)
        if current_stage:
            self.run.current_stage = stage_name
        self.snapshot()

    def update_stage(
        self,
        stage_name: str,
        *,
        current_stage: bool = True,
        item_count_total: int | None = None,
        item_count_completed: int | None = None,
        item_count_failed: int | None = None,
        increment_completed: int = 0,
        increment_failed: int = 0,
        message: str | None = None,
        counters: dict[str, Any] | None = None,
        run_counters: dict[str, Any] | None = None,
        log_line: str | None = None,
    ) -> None:
        stage = self._stage_index[stage_name]
        if stage.status == "pending":
            self.start_stage(stage_name, message=message)
            stage = self._stage_index[stage_name]
        if item_count_total is not None:
            stage.item_count_total = item_count_total
        if item_count_completed is not None:
            stage.item_count_completed = item_count_completed
        else:
            stage.item_count_completed += increment_completed
        if item_count_failed is not None:
            stage.item_count_failed = item_count_failed
        else:
            stage.item_count_failed += increment_failed
        if message:
            stage.last_progress_message = message
        if counters:
            stage.counters.update(counters)
        if run_counters:
            for key, value in run_counters.items():
                if hasattr(self.run, key):
                    setattr(self.run, key, value)
        self._refresh_stage_timing(stage)
        if current_stage:
            self.run.current_stage = stage_name
        payload = self.snapshot()
        if log_line:
            logger.info("%s", log_line)
        return payload

    def complete_stage(
        self,
        stage_name: str,
        *,
        current_stage: bool = True,
        message: str | None = None,
        counters: dict[str, Any] | None = None,
        item_count_total: int | None = None,
    ) -> None:
        stage = self._stage_index[stage_name]
        if stage.status == "pending":
            self.start_stage(stage_name, current_stage=False)
            stage = self._stage_index[stage_name]
        if item_count_total is not None:
            stage.item_count_total = item_count_total
        if message:
            stage.last_progress_message = message
        if counters:
            stage.counters.update(counters)
        stage.status = "completed"
        stage.ended_at = _now_utc()
        self._refresh_stage_timing(stage)
        if current_stage:
            self.run.current_stage = stage_name
        self.snapshot()

    def skip_stage(self, stage_name: str, *, current_stage: bool = True, message: str | None = None) -> None:
        stage = self._stage_index[stage_name]
        now = _now_utc()
        if stage.started_at is None:
            stage.started_at = now
        stage.updated_at = now
        stage.ended_at = now
        stage.status = "skipped"
        stage.last_progress_message = message
        stage.elapsed_seconds = 0.0
        if current_stage:
            self.run.current_stage = stage_name
        self.snapshot()

    def fail_stage(
        self,
        stage_name: str,
        *,
        current_stage: bool = True,
        error_summary: str,
        message: str | None = None,
    ) -> None:
        stage = self._stage_index[stage_name]
        if stage.status == "pending":
            self.start_stage(stage_name, current_stage=False)
            stage = self._stage_index[stage_name]
        stage.status = "failed"
        stage.error_summary = error_summary
        stage.last_progress_message = message or error_summary
        stage.ended_at = _now_utc()
        self._refresh_stage_timing(stage)
        if current_stage:
            self.run.current_stage = stage_name
        self.snapshot()

    def complete_run(
        self,
        *,
        stop_reason: str | None = None,
        extra_summary: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.run.overall_status = "completed"
        self.run.stop_reason = stop_reason
        self.run.ended_at = _now_utc()
        payload = self.snapshot()
        if extra_summary is not None:
            payload["run_summary"] = extra_summary
        self._write_json(self.summary_path, payload)
        return payload

    def fail_run(
        self,
        *,
        current_stage: str | None = None,
        error_summary: str | None = None,
        stop_reason: str | None = None,
        fail_fast_reason: str | None = None,
        extra_summary: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.run.overall_status = "failed"
        if current_stage is not None:
            self.run.current_stage = current_stage
        self.run.ended_at = _now_utc()
        self.run.stop_reason = stop_reason
        if fail_fast_reason:
            self.run.fail_fast_triggered = True
            self.run.fail_fast_reason = fail_fast_reason
        payload = self.snapshot()
        if error_summary:
            payload["error_summary"] = error_summary
        if extra_summary is not None:
            payload["run_summary"] = extra_summary
        self._write_json(self.summary_path, payload)
        return payload
