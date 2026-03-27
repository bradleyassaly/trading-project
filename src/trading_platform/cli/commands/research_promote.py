from __future__ import annotations

from pathlib import Path

from trading_platform.config.loader import load_promotion_policy_config
from trading_platform.db.services import DatabaseLineageService, build_research_memory_service
from trading_platform.research.promotion_pipeline import PromotionPolicyConfig, apply_research_promotions
from trading_platform.research.registry import (
    refresh_research_registry_bundle,
    refresh_run_local_registry_bundle,
    resolve_latest_research_run_dir,
)


def _resolve_promotion_registry_scope(args) -> tuple[Path, Path, dict[str, object]]:
    artifacts_root = Path(args.artifacts_root)
    registry_scope = str(getattr(args, "registry_scope", "run_local") or "run_local").strip().lower()
    if bool(getattr(args, "use_global_registry", False)):
        registry_scope = "global"

    if registry_scope == "global":
        registry_dir = Path(getattr(args, "registry_dir", None) or (artifacts_root / "research_registry"))
        bundle = refresh_research_registry_bundle(
            artifacts_root=artifacts_root,
            output_dir=registry_dir,
        )
        return artifacts_root, registry_dir, bundle

    run_dir = Path(getattr(args, "run_dir", None)) if getattr(args, "run_dir", None) else resolve_latest_research_run_dir(artifacts_root)
    if run_dir is None:
        raise ValueError(
            f"No research run manifests found under {artifacts_root}. "
            "Provide --run-dir explicitly or use --registry-scope global."
        )
    registry_dir = Path(getattr(args, "registry_dir", None) or (run_dir / "research_registry"))
    bundle = refresh_run_local_registry_bundle(
        run_dir=run_dir,
        output_dir=registry_dir,
    )
    return run_dir, registry_dir, bundle


def cmd_research_promote(args) -> None:
    db_service = DatabaseLineageService.from_config(
        enable_database_metadata=getattr(args, "enable_database_metadata", None),
        database_url=getattr(args, "database_url", None),
        database_schema=getattr(args, "database_schema", None),
    )
    research_memory = build_research_memory_service(
        enable_database_metadata=getattr(args, "enable_database_metadata", None),
        database_url=getattr(args, "database_url", None),
        database_schema=getattr(args, "database_schema", None),
        write_promotions=bool(getattr(args, "tracking_write_promotions", True)),
    )
    research_memory.init_schema(schema_name=getattr(args, "database_schema", None))
    policy = (
        load_promotion_policy_config(args.policy_config)
        if getattr(args, "policy_config", None)
        else PromotionPolicyConfig()
    )
    scoped_artifacts_root, registry_dir, registry_bundle = _resolve_promotion_registry_scope(args)
    result = apply_research_promotions(
        artifacts_root=scoped_artifacts_root,
        registry_dir=registry_dir,
        output_dir=Path(args.output_dir),
        policy=policy,
        top_n=getattr(args, "top_n", None),
        allow_overwrite=bool(getattr(args, "allow_overwrite", False)),
        dry_run=bool(getattr(args, "dry_run", False)),
        inactive=bool(getattr(args, "inactive", False)),
        validation_path=Path(args.validation) if getattr(args, "validation", None) else None,
        override_validation=bool(getattr(args, "override_validation", False)),
    )
    print(f"Registry scope: {str(getattr(args, 'registry_scope', 'run_local') or 'run_local').strip().lower()}")
    if getattr(args, "run_dir", None):
        print(f"Run dir: {Path(args.run_dir)}")
    elif scoped_artifacts_root != Path(args.artifacts_root):
        print(f"Run dir: {scoped_artifacts_root}")
    print(f"Research registry: {registry_bundle['registry_json_path']}")
    print(f"Promotion candidates: {registry_bundle['promotion_candidates_json_path']}")
    print(f"Selected promotions: {result['selected_count']}")
    print(f"Dry run: {result['dry_run']}")
    print(f"Promoted index: {result['promoted_index_path']}")
    enriched_promoted_rows: list[dict[str, object]] = []
    for row in result["promoted_rows"]:
        strategy_definition_id = db_service.upsert_strategy_definition(
            name=str(row["preset_name"]),
            version=str(row.get("timestamp") or row.get("source_run_id") or "v1"),
            config_payload=row,
            code_hash=None,
            is_active=row.get("status") == "active",
        )
        promotion_decision_id = db_service.record_promotion_decision(
            strategy_definition_id=strategy_definition_id,
            source_research_run_id=db_service.find_research_run_id(str(row.get("source_run_id") or "")),
            decision=str(row.get("status") or "promoted"),
            reason=str(row.get("rationale") or row.get("reason") or ""),
            metrics_json={
                "ranking_metric": row.get("ranking_metric"),
                "ranking_value": row.get("ranking_value"),
                "promotion_variant": row.get("promotion_variant"),
                "condition_id": row.get("condition_id"),
                "condition_type": row.get("condition_type"),
                "runtime_score_validation_pass": row.get("runtime_score_validation_pass"),
                "runtime_score_validation_reason": row.get("runtime_score_validation_reason"),
                "runtime_computable_symbol_count": row.get("runtime_computable_symbol_count"),
            },
        )
        promoted_strategy_id = db_service.record_promoted_strategy(
            strategy_definition_id=strategy_definition_id,
            promotion_decision_id=promotion_decision_id,
            status=str(row.get("status") or "inactive"),
        )
        enriched_promoted_rows.append(
            {
                **row,
                "strategy_definition_id": strategy_definition_id,
                "promoted_strategy_id": promoted_strategy_id,
                "candidate_id": row.get("candidate_id"),
            }
        )
        print(
            f"- {row['preset_name']}: source_run_id={row['source_run_id']} "
            f"status={row['status']} metric={row['ranking_metric']}={row['ranking_value']} "
            f"runtime={row.get('runtime_score_validation_pass')}"
        )
    source_run_id = None
    if len({str(row.get("source_run_id") or "") for row in result["promoted_rows"]}) == 1 and result["promoted_rows"]:
        source_run_id = db_service.find_research_run_id(str(result["promoted_rows"][0].get("source_run_id") or ""))
    research_memory.persist_promotions(run_id=source_run_id, promoted_rows=enriched_promoted_rows)
