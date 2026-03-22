from __future__ import annotations

from pathlib import Path

from trading_platform.research.registry import compare_research_runs


def cmd_research_compare_runs(args) -> None:
    result = compare_research_runs(
        artifacts_root=Path(args.artifacts_root),
        run_id_a=args.run_id_a,
        run_id_b=args.run_id_b,
        output_dir=Path(args.output_dir),
    )
    print(f"Research comparison JSON: {result['comparison_json_path']}")
    print(f"Research comparison Markdown: {result['comparison_md_path']}")
