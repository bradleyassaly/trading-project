from __future__ import annotations

from pathlib import Path

from trading_platform.config.loader import load_execution_config
from trading_platform.execution.realism import (
    load_execution_requests_from_csv,
    simulate_execution,
    write_execution_artifacts,
)


def cmd_execution_simulate(args) -> None:
    config = load_execution_config(args.config)
    requests = load_execution_requests_from_csv(args.targets)
    result = simulate_execution(requests=requests, config=config)
    paths = write_execution_artifacts(result, Path(args.output_dir))
    print(f"Requested orders: {result.summary.requested_order_count}")
    print(f"Executable orders: {result.summary.executable_order_count}")
    print(f"Rejected orders: {result.summary.rejected_order_count}")
    print(f"Expected total cost: {result.summary.expected_total_cost:.6f}")
    print("Artifacts:")
    for name, path in sorted(paths.items()):
        print(f"  {name}: {path}")
