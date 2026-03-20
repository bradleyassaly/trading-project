from __future__ import annotations

from pathlib import Path

from trading_platform.universes.definitions import export_universe_definitions


def cmd_export_universes(args) -> None:
    output_path = export_universe_definitions(Path(args.output))
    print(f"Exported universes to {output_path}")
