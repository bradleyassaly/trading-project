from __future__ import annotations

from trading_platform.governance.persistence import load_strategy_registry


def cmd_registry_list(args) -> None:
    registry = load_strategy_registry(args.registry)
    if not registry.entries:
        print("No strategy registry entries found.")
        return

    headers = [
        "strategy_id",
        "family",
        "version",
        "status",
        "stage",
        "preset_name",
        "universe",
    ]
    rows = [
        [
            entry.strategy_id,
            entry.family,
            entry.version,
            entry.status,
            entry.current_deployment_stage,
            entry.preset_name,
            entry.universe or "",
        ]
        for entry in sorted(registry.entries, key=lambda item: item.strategy_id)
    ]
    widths = [
        max(len(header), *(len(str(row[index])) for row in rows))
        for index, header in enumerate(headers)
    ]
    print(" ".join(header.ljust(widths[index]) for index, header in enumerate(headers)))
    print(" ".join("-" * widths[index] for index in range(len(headers))))
    for row in rows:
        print(" ".join(str(value).ljust(widths[index]) for index, value in enumerate(row)))
