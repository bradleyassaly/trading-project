from __future__ import annotations

from pathlib import Path

from trading_platform.research.registry import build_research_registry


def cmd_research_registry_build(args) -> None:
    result = build_research_registry(
        artifacts_root=Path(args.artifacts_root),
        output_dir=Path(args.output_dir),
    )
    print(f"Research runs indexed: {result['run_count']}")
    print(f"Research registry JSON: {result['registry_json_path']}")
    print(f"Research registry CSV: {result['registry_csv_path']}")
