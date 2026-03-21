from __future__ import annotations

from pathlib import Path

from trading_platform.governance.persistence import (
    get_registry_entry,
    load_governance_criteria_config,
    load_strategy_registry,
)
from trading_platform.governance.service import evaluate_promotion, write_decision_artifacts


def cmd_registry_evaluate_promotion(args) -> None:
    registry = load_strategy_registry(args.registry)
    entry = get_registry_entry(registry, args.strategy_id)
    criteria = load_governance_criteria_config(args.config).promotion
    report, snapshot = evaluate_promotion(entry=entry, criteria=criteria)
    paths = write_decision_artifacts(
        report=report,
        snapshot=snapshot,
        output_dir=Path(args.output_dir),
        prefix="promotion_decision",
    )

    print(f"Strategy: {entry.strategy_id}")
    print(f"Passed: {report.passed}")
    print(f"Recommendation: {report.recommendation}")
    if report.failed_criteria:
        print(f"Failed criteria: {', '.join(report.failed_criteria)}")
    print("Artifacts:")
    for name, path in sorted(paths.items()):
        print(f"  {name}: {path}")
