from __future__ import annotations

from pathlib import Path

from trading_platform.config.loader import load_promotion_policy_config
from trading_platform.db.services import DatabaseLineageService
from trading_platform.research.promotion_pipeline import PromotionPolicyConfig, apply_research_promotions


def cmd_research_promote(args) -> None:
    db_service = DatabaseLineageService.from_config()
    policy = (
        load_promotion_policy_config(args.policy_config)
        if getattr(args, "policy_config", None)
        else PromotionPolicyConfig()
    )
    result = apply_research_promotions(
        artifacts_root=Path(args.artifacts_root),
        registry_dir=Path(args.registry_dir),
        output_dir=Path(args.output_dir),
        policy=policy,
        top_n=getattr(args, "top_n", None),
        allow_overwrite=bool(getattr(args, "allow_overwrite", False)),
        dry_run=bool(getattr(args, "dry_run", False)),
        inactive=bool(getattr(args, "inactive", False)),
        validation_path=Path(args.validation) if getattr(args, "validation", None) else None,
        override_validation=bool(getattr(args, "override_validation", False)),
    )
    print(f"Selected promotions: {result['selected_count']}")
    print(f"Dry run: {result['dry_run']}")
    print(f"Promoted index: {result['promoted_index_path']}")
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
            reason=str(row.get("reason") or ""),
            metrics_json={"ranking_metric": row.get("ranking_metric"), "ranking_value": row.get("ranking_value")},
        )
        db_service.record_promoted_strategy(
            strategy_definition_id=strategy_definition_id,
            promotion_decision_id=promotion_decision_id,
            status=str(row.get("status") or "inactive"),
        )
        print(
            f"- {row['preset_name']}: source_run_id={row['source_run_id']} "
            f"status={row['status']} metric={row['ranking_metric']}={row['ranking_value']}"
        )
