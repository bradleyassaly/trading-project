from __future__ import annotations

from trading_platform.governance.persistence import load_strategy_registry
from trading_platform.governance.service import promote_registry_entry, save_mutated_registry


def cmd_registry_promote(args) -> None:
    registry = load_strategy_registry(args.registry)
    updated = promote_registry_entry(
        registry=registry,
        strategy_id=args.strategy_id,
        note=getattr(args, "note", None),
    )
    save_mutated_registry(updated, args.registry)
    print(f"Promoted {args.strategy_id}")
