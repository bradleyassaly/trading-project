from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


PIPELINE_STAGE_NAMES = [
    "data_refresh",
    "feature_generation",
    "research",
    "promotion_evaluation",
    "registry_mutation",
    "multi_strategy_config_generation",
    "portfolio_allocation",
    "paper_trading",
    "live_dry_run",
    "reporting",
]
PIPELINE_STAGE_DEPENDENCIES: dict[str, list[str]] = {
    "feature_generation": ["data_refresh"],
    "research": ["feature_generation"],
    "registry_mutation": ["promotion_evaluation"],
    "portfolio_allocation": ["multi_strategy_config_generation"],
    "paper_trading": ["multi_strategy_config_generation"],
    "live_dry_run": ["multi_strategy_config_generation"],
    "reporting": [],
}
PIPELINE_STAGE_STATUSES = {"pending", "skipped", "running", "succeeded", "failed"}


@dataclass(frozen=True)
class OrchestrationStageToggles:
    data_refresh: bool = False
    feature_generation: bool = False
    research: bool = False
    promotion_evaluation: bool = False
    registry_mutation: bool = False
    multi_strategy_config_generation: bool = False
    portfolio_allocation: bool = False
    paper_trading: bool = False
    live_dry_run: bool = False
    reporting: bool = True

    def enabled_stage_names(self) -> list[str]:
        return [
            stage_name
            for stage_name in PIPELINE_STAGE_NAMES
            if bool(getattr(self, stage_name))
        ]


@dataclass(frozen=True)
class PipelineRunConfig:
    run_name: str
    schedule_type: str
    universes: list[str]
    preset_filters: list[str] = field(default_factory=list)
    registry_path: str | None = None
    governance_config_path: str | None = None
    multi_strategy_output_path: str | None = None
    paper_state_path: str | None = None
    live_broker: str = "mock"
    output_root_dir: str = "artifacts/orchestration"
    fail_fast: bool = False
    continue_on_stage_error: bool = False
    max_parallel_jobs: int = 1
    tags: list[str] = field(default_factory=list)
    notes: str | None = None
    stage_order: list[str] = field(default_factory=lambda: list(PIPELINE_STAGE_NAMES))
    stages: OrchestrationStageToggles = field(default_factory=OrchestrationStageToggles)
    data_start: str = "2010-01-01"
    data_end: str | None = None
    data_interval: str = "1d"
    feature_groups: list[str] | None = None
    research_strategy: str = "sma_cross"
    research_fast: int | None = None
    research_slow: int | None = None
    research_lookback: int | None = None
    research_cash: float = 10_000.0
    research_commission: float = 0.001
    auto_promote_qualifying_candidates: bool = False
    registry_include_paper_strategies: bool = False
    registry_selection_weighting_scheme: str = "equal"
    registry_max_strategies: int | None = None

    def __post_init__(self) -> None:
        if not self.run_name or not self.run_name.strip():
            raise ValueError("run_name must be a non-empty string")
        if self.schedule_type not in {"daily", "weekly", "ad_hoc"}:
            raise ValueError("schedule_type must be one of: daily, weekly, ad_hoc")
        if not self.universes:
            raise ValueError("universes must contain at least one universe")
        if self.max_parallel_jobs <= 0:
            raise ValueError("max_parallel_jobs must be > 0")
        if self.registry_selection_weighting_scheme not in {"equal", "score_weighted"}:
            raise ValueError(
                "registry_selection_weighting_scheme must be one of: equal, score_weighted"
            )
        self._validate_stage_order()
        self._validate_stage_requirements()

    def _validate_stage_order(self) -> None:
        if len(self.stage_order) != len(set(self.stage_order)):
            raise ValueError("stage_order must not contain duplicate stage names")
        unknown = [stage_name for stage_name in self.stage_order if stage_name not in PIPELINE_STAGE_NAMES]
        if unknown:
            raise ValueError(f"stage_order contains unknown stage names: {unknown}")
        index_map = {stage_name: index for index, stage_name in enumerate(self.stage_order)}
        for stage_name, dependencies in PIPELINE_STAGE_DEPENDENCIES.items():
            for dependency in dependencies:
                if dependency in index_map and stage_name in index_map:
                    if index_map[dependency] > index_map[stage_name]:
                        raise ValueError(
                            f"Invalid stage ordering: {dependency} must come before {stage_name}"
                        )

    def _validate_stage_requirements(self) -> None:
        if self.stages.promotion_evaluation and not self.registry_path:
            raise ValueError("registry_path is required when promotion_evaluation is enabled")
        if self.stages.promotion_evaluation and not self.governance_config_path:
            raise ValueError(
                "governance_config_path is required when promotion_evaluation is enabled"
            )
        if self.stages.registry_mutation and not self.registry_path:
            raise ValueError("registry_path is required when registry_mutation is enabled")
        if self.stages.multi_strategy_config_generation and not self.registry_path:
            raise ValueError(
                "registry_path is required when multi_strategy_config_generation is enabled"
            )
        if (
            self.stages.multi_strategy_config_generation
            and not self.multi_strategy_output_path
        ):
            raise ValueError(
                "multi_strategy_output_path is required when multi_strategy_config_generation is enabled"
            )
        if self.stages.paper_trading and not self.paper_state_path:
            raise ValueError("paper_state_path is required when paper_trading is enabled")

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["stages"] = asdict(self.stages)
        return payload


@dataclass
class PipelineStageRecord:
    stage_name: str
    status: str = "pending"
    started_at: str | None = None
    ended_at: str | None = None
    duration_seconds: float | None = None
    inputs: dict[str, Any] = field(default_factory=dict)
    outputs: dict[str, Any] = field(default_factory=dict)
    error_message: str | None = None

    def __post_init__(self) -> None:
        if self.stage_name not in PIPELINE_STAGE_NAMES:
            raise ValueError(f"Unknown stage_name: {self.stage_name}")
        if self.status not in PIPELINE_STAGE_STATUSES:
            raise ValueError(f"Unknown pipeline stage status: {self.status}")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PipelineRunResult:
    run_name: str
    schedule_type: str
    started_at: str
    ended_at: str
    status: str
    run_dir: str
    stage_records: list[PipelineStageRecord]
    errors: list[dict[str, Any]]
    outputs: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_name": self.run_name,
            "schedule_type": self.schedule_type,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "status": self.status,
            "run_dir": self.run_dir,
            "stage_records": [record.to_dict() for record in self.stage_records],
            "errors": self.errors,
            "outputs": self.outputs,
        }
