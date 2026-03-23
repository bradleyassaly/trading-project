from __future__ import annotations

from pathlib import Path

from trading_platform.system_evaluation.service import build_system_evaluation_history


def cmd_system_eval_build(args) -> None:
    result = build_system_evaluation_history(
        runs_root=Path(args.runs_root),
        output_dir=Path(args.output_dir),
    )
    print(f"Run count: {result['run_count']}")
    print(f"Latest evaluation JSON: {result['system_evaluation_json_path']}")
    print(f"Latest evaluation CSV: {result['system_evaluation_csv_path']}")
    print(f"History JSON: {result['system_evaluation_history_json_path']}")
    print(f"History CSV: {result['system_evaluation_history_csv_path']}")
