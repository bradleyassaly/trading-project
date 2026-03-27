from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from uuid import UUID

import pandas as pd
from sqlalchemy import func, select, text
from sqlalchemy.orm import Session, sessionmaker

from trading_platform.db import Base
from trading_platform.db.models import PromotedStrategy, ResearchRun, SignalCandidate, SignalMetric
from trading_platform.db.services.lineage_service import DatabaseLineageService, _normalize_payload


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_text(value: Any) -> str | None:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value or None


def _parse_variant_parameters(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if value in (None, ""):
        return {}
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {"raw": value}
        return dict(parsed) if isinstance(parsed, dict) else {"value": parsed}
    return _normalize_payload(value)


def _coerce_uuid(value: Any) -> Any:
    if value is None or isinstance(value, UUID):
        return value
    if isinstance(value, str):
        try:
            return UUID(value)
        except ValueError:
            return None
    return value


class ResearchMemoryService:
    def __init__(
        self,
        session_factory: sessionmaker[Session] | None,
        *,
        write_candidates: bool = True,
        write_metrics: bool = True,
        write_promotions: bool = True,
    ) -> None:
        self.session_factory = session_factory
        self.write_candidates = bool(write_candidates)
        self.write_metrics = bool(write_metrics)
        self.write_promotions = bool(write_promotions)

    @property
    def enabled(self) -> bool:
        return self.session_factory is not None

    def init_schema(self, *, schema_name: str | None = None) -> bool:
        if not self.enabled or self.session_factory is None:
            return False
        bind = self.session_factory.kw.get("bind")
        if bind is None:
            return False
        if bind.dialect.name != "sqlite" and schema_name:
            with bind.begin() as connection:
                connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"'))
        Base.metadata.create_all(bind)
        return True

    def attach_run_metadata(
        self,
        *,
        run_id: Any,
        artifacts_root: str | None,
        output_dir: str | None,
        universe: str | None,
        config_path: str | None,
    ) -> None:
        if not self.enabled or run_id is None or self.session_factory is None:
            return
        with self.session_factory() as session:
            row = session.get(ResearchRun, run_id)
            if row is None:
                return
            row.artifacts_root = artifacts_root
            row.output_dir = output_dir
            row.universe = universe
            row.config_path = config_path
            session.commit()

    def persist_alpha_research_outputs(
        self,
        *,
        run_id: Any,
        leaderboard_df: pd.DataFrame,
        composite_runtime_df: pd.DataFrame | None = None,
    ) -> None:
        if not self.enabled or run_id is None or self.session_factory is None:
            return
        if leaderboard_df.empty:
            return
        with self.session_factory() as session:
            self._persist_candidates(session=session, run_id=run_id, leaderboard_df=leaderboard_df)
            self._persist_metrics(session=session, run_id=run_id, leaderboard_df=leaderboard_df)
            self._persist_composite_runtime_summary(session=session, run_id=run_id, composite_runtime_df=composite_runtime_df)
            session.commit()

    def _persist_composite_runtime_summary(
        self,
        *,
        session: Session,
        run_id: Any,
        composite_runtime_df: pd.DataFrame | None,
    ) -> None:
        if composite_runtime_df is None or composite_runtime_df.empty:
            return
        row = session.get(ResearchRun, run_id)
        if row is None:
            return
        selected = composite_runtime_df.copy()
        if "composite_runtime_computable_symbol_count" in selected.columns:
            selected["composite_runtime_computable_symbol_count"] = pd.to_numeric(
                selected["composite_runtime_computable_symbol_count"],
                errors="coerce",
            )
            selected = selected.sort_values(
                ["composite_runtime_computable_symbol_count", "selected_member_count"],
                ascending=[False, False],
            )
        top = selected.iloc[0].to_dict()
        row.composite_runtime_computability_pass = (
            bool(top.get("composite_runtime_computability_pass"))
            if top.get("composite_runtime_computability_pass") is not None
            else None
        )
        row.composite_runtime_computability_reason = _safe_text(top.get("composite_runtime_computability_reason"))
        row.composite_runtime_computable_symbol_count = _safe_int(top.get("composite_runtime_computable_symbol_count"))

    def _persist_candidates(self, *, session: Session, run_id: Any, leaderboard_df: pd.DataFrame) -> None:
        if not self.write_candidates:
            return
        for row in leaderboard_df.to_dict(orient="records"):
            candidate_key = _safe_text(row.get("candidate_id"))
            if not candidate_key:
                continue
            existing = session.scalar(
                select(SignalCandidate).where(
                    SignalCandidate.research_run_id == run_id,
                    SignalCandidate.candidate_id == candidate_key,
                )
            )
            payload = {
                "research_run_id": run_id,
                "candidate_id": candidate_key,
                "candidate_name": _safe_text(row.get("candidate_name")) or candidate_key,
                "signal_family": _safe_text(row.get("signal_family")) or "unknown",
                "signal_variant": _safe_text(row.get("signal_variant")) or "base",
                "lookback": _safe_int(row.get("lookback")),
                "horizon": _safe_int(row.get("horizon")),
                "variant_parameters_json": _parse_variant_parameters(row.get("variant_parameters_json")),
            }
            if existing is None:
                session.add(SignalCandidate(**payload))
                continue
            for key, value in payload.items():
                setattr(existing, key, value)

    def _persist_metrics(self, *, session: Session, run_id: Any, leaderboard_df: pd.DataFrame) -> None:
        if not self.write_metrics:
            return
        for row in leaderboard_df.to_dict(orient="records"):
            candidate_key = _safe_text(row.get("candidate_id"))
            if not candidate_key:
                continue
            existing = session.scalar(
                select(SignalMetric).where(
                    SignalMetric.research_run_id == run_id,
                    SignalMetric.candidate_id == candidate_key,
                )
            )
            payload = {
                "research_run_id": run_id,
                "candidate_id": candidate_key,
                "folds_tested": _safe_int(row.get("folds_tested")),
                "symbols_tested": _safe_int(row.get("symbols_tested")),
                "mean_dates_evaluated": _safe_float(row.get("mean_dates_evaluated")),
                "mean_pearson_ic": _safe_float(row.get("mean_pearson_ic")),
                "mean_spearman_ic": _safe_float(row.get("mean_spearman_ic")),
                "mean_hit_rate": _safe_float(row.get("mean_hit_rate")),
                "mean_long_short_spread": _safe_float(row.get("mean_long_short_spread")),
                "mean_quantile_spread": _safe_float(row.get("mean_quantile_spread")),
                "mean_turnover": _safe_float(row.get("mean_turnover")),
                "worst_fold_spearman_ic": _safe_float(row.get("worst_fold_spearman_ic")),
                "total_obs": _safe_int(row.get("total_obs")),
                "rejection_reason": _safe_text(row.get("rejection_reason")),
                "promotion_status": _safe_text(row.get("promotion_status")),
                "runtime_computability_pass": bool(row.get("runtime_computability_pass")) if row.get("runtime_computability_pass") is not None else None,
                "runtime_computability_reason": _safe_text(row.get("runtime_computability_reason")),
                "runtime_computable_symbol_count": _safe_int(row.get("runtime_computable_symbol_count")),
            }
            if existing is None:
                session.add(SignalMetric(**payload))
                continue
            for key, value in payload.items():
                setattr(existing, key, value)

    def persist_promotions(
        self,
        *,
        run_id: Any | None,
        promoted_rows: list[dict[str, Any]],
    ) -> None:
        if not self.enabled or self.session_factory is None or not self.write_promotions:
            return
        with self.session_factory() as session:
            for row in promoted_rows:
                preset_name = _safe_text(row.get("preset_name"))
                if not preset_name:
                    continue
                promoted_strategy_id = _coerce_uuid(row.get("promoted_strategy_id"))
                existing = session.get(PromotedStrategy, promoted_strategy_id) if promoted_strategy_id is not None else None
                if existing is None:
                    existing = session.scalar(
                        select(PromotedStrategy).where(
                            PromotedStrategy.research_run_id == run_id,
                            PromotedStrategy.preset_name == preset_name,
                        )
                    )
                strategy_definition_id = _coerce_uuid(row.get("strategy_definition_id"))
                payload = {
                    "research_run_id": run_id,
                    "candidate_id": _safe_text(row.get("candidate_id")),
                    "preset_name": preset_name,
                    "signal_family": _safe_text(row.get("signal_family")),
                    "strategy_name": _safe_text(row.get("strategy_name")),
                    "ranking_metric": _safe_text(row.get("ranking_metric")),
                    "ranking_value": _safe_float(row.get("ranking_value")),
                    "validation_status": _safe_text(row.get("validation_status")),
                    "promotion_variant": _safe_text(row.get("promotion_variant")),
                    "condition_id": _safe_text(row.get("condition_id")),
                    "condition_type": _safe_text(row.get("condition_type")),
                    "rationale": _safe_text(row.get("rationale")),
                    "runtime_score_validation_pass": bool(row.get("runtime_score_validation_pass")) if row.get("runtime_score_validation_pass") is not None else None,
                    "runtime_score_validation_reason": _safe_text(row.get("runtime_score_validation_reason")),
                    "runtime_computable_symbol_count": _safe_int(row.get("runtime_computable_symbol_count")),
                    "promotion_timestamp_text": _safe_text(row.get("promotion_timestamp")),
                    "generated_preset_path": _safe_text(row.get("generated_preset_path")),
                    "generated_registry_path": _safe_text(row.get("generated_registry_path")),
                    "generated_pipeline_config_path": _safe_text(row.get("generated_pipeline_config_path")),
                    "status": _safe_text(row.get("status")) or "inactive",
                }
                if existing is None:
                    if strategy_definition_id is None:
                        continue
                    existing = PromotedStrategy(strategy_definition_id=strategy_definition_id)
                    session.add(existing)
                elif strategy_definition_id is not None:
                    existing.strategy_definition_id = strategy_definition_id
                for key, value in payload.items():
                    setattr(existing, key, value)
            session.commit()

    def list_recent_runs(self, *, limit: int = 20) -> list[dict[str, Any]]:
        if not self.enabled or self.session_factory is None:
            return []
        with self.session_factory() as session:
            rows = session.execute(
                select(ResearchRun)
                .order_by(ResearchRun.started_at.desc())
                .limit(limit)
            ).scalars().all()
        return [
            {
                "run_key": row.run_key,
                "status": row.status,
                "started_at": row.started_at.isoformat() if row.started_at else None,
                "completed_at": row.completed_at.isoformat() if row.completed_at else None,
                "universe": row.universe,
                "output_dir": row.output_dir,
            }
            for row in rows
        ]

    def top_candidates(self, *, metric: str, limit: int = 20) -> list[dict[str, Any]]:
        if not self.enabled or self.session_factory is None:
            return []
        metric_column = getattr(SignalMetric, metric, None)
        if metric_column is None:
            raise ValueError(f"Unsupported metric column: {metric}")
        with self.session_factory() as session:
            rows = session.execute(
                select(
                    SignalMetric.candidate_id,
                    SignalCandidate.candidate_name,
                    SignalCandidate.signal_family,
                    metric_column.label("metric_value"),
                    ResearchRun.run_key,
                )
                .join(SignalCandidate, (SignalCandidate.research_run_id == SignalMetric.research_run_id) & (SignalCandidate.candidate_id == SignalMetric.candidate_id))
                .join(ResearchRun, ResearchRun.id == SignalMetric.research_run_id)
                .order_by(metric_column.desc().nullslast())
                .limit(limit)
            ).all()
        return [
            {
                "candidate_id": candidate_id,
                "candidate_name": candidate_name,
                "signal_family": signal_family,
                "metric_value": metric_value,
                "run_key": run_key,
            }
            for candidate_id, candidate_name, signal_family, metric_value, run_key in rows
        ]

    def family_summary(self) -> list[dict[str, Any]]:
        if not self.enabled or self.session_factory is None:
            return []
        with self.session_factory() as session:
            promotion_counts = (
                select(
                    PromotedStrategy.signal_family.label("signal_family"),
                    func.count(PromotedStrategy.id).label("promoted_count"),
                )
                .group_by(PromotedStrategy.signal_family)
                .subquery()
            )
            rows = session.execute(
                select(
                    SignalCandidate.signal_family,
                    func.count(func.distinct(SignalCandidate.research_run_id)).label("run_count"),
                    func.avg(SignalMetric.mean_spearman_ic).label("avg_mean_spearman_ic"),
                    func.max(SignalMetric.mean_spearman_ic).label("max_mean_spearman_ic"),
                    func.coalesce(promotion_counts.c.promoted_count, 0).label("promoted_count"),
                )
                .join(SignalMetric, (SignalMetric.research_run_id == SignalCandidate.research_run_id) & (SignalMetric.candidate_id == SignalCandidate.candidate_id))
                .outerjoin(promotion_counts, promotion_counts.c.signal_family == SignalCandidate.signal_family)
                .where(SignalCandidate.signal_family.is_not(None))
                .group_by(SignalCandidate.signal_family, promotion_counts.c.promoted_count)
                .order_by(func.avg(SignalMetric.mean_spearman_ic).desc().nullslast())
            ).all()
        return [
            {
                "signal_family": signal_family,
                "run_count": int(run_count or 0),
                "avg_mean_spearman_ic": avg_mean_spearman_ic,
                "max_mean_spearman_ic": max_mean_spearman_ic,
                "promoted_count": int(promoted_count or 0),
            }
            for signal_family, run_count, avg_mean_spearman_ic, max_mean_spearman_ic, promoted_count in rows
        ]

    def list_promotions(self, *, limit: int = 20) -> list[dict[str, Any]]:
        if not self.enabled or self.session_factory is None:
            return []
        with self.session_factory() as session:
            rows = session.execute(
                select(PromotedStrategy, ResearchRun.run_key)
                .outerjoin(ResearchRun, ResearchRun.id == PromotedStrategy.research_run_id)
                .where(PromotedStrategy.preset_name.is_not(None))
                .order_by(PromotedStrategy.created_at.desc())
                .limit(limit)
            ).all()
        return [
            {
                "preset_name": row.preset_name,
                "run_key": run_key,
                "signal_family": row.signal_family,
                "ranking_metric": row.ranking_metric,
                "ranking_value": row.ranking_value,
                "status": row.status,
                "promotion_variant": row.promotion_variant,
                "condition_id": row.condition_id,
                "condition_type": row.condition_type,
                "rationale": row.rationale,
                "runtime_score_validation_pass": row.runtime_score_validation_pass,
                "runtime_score_validation_reason": row.runtime_score_validation_reason,
                "runtime_computable_symbol_count": row.runtime_computable_symbol_count,
            }
            for row, run_key in rows
        ]


def build_research_memory_service(
    *,
    enable_database_metadata: bool | None = None,
    database_url: str | None = None,
    database_schema: str | None = None,
    write_candidates: bool = True,
    write_metrics: bool = True,
    write_promotions: bool = True,
) -> ResearchMemoryService:
    lineage_service = DatabaseLineageService.from_config(
        enable_database_metadata=enable_database_metadata,
        database_url=database_url,
        database_schema=database_schema,
    )
    return ResearchMemoryService(
        lineage_service.session_factory,
        write_candidates=write_candidates,
        write_metrics=write_metrics,
        write_promotions=write_promotions,
    )
