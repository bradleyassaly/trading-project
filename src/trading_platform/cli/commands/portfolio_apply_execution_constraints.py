from __future__ import annotations

from pathlib import Path

import pandas as pd

from trading_platform.config.loader import load_execution_config
from trading_platform.execution.realism import ExecutionOrderRequest, simulate_execution, write_execution_artifacts


def cmd_portfolio_apply_execution_constraints(args) -> None:
    allocation_dir = Path(args.allocation_dir)
    config = load_execution_config(args.config)
    combined = pd.read_csv(allocation_dir / "combined_target_weights.csv")
    requests = [
        ExecutionOrderRequest(
            symbol=str(row["symbol"]),
            side="BUY" if float(row["target_weight"]) >= 0 else "SELL",
            requested_quantity=max(int(abs(float(row["target_weight"])) * 100), 1),
            reference_price=float(row["latest_price"]) if pd.notna(row.get("latest_price")) else 1.0,
            target_weight=float(row["target_weight"]),
            target_quantity=int(float(row["target_weight"]) * 100),
            average_dollar_volume=float(row["average_dollar_volume"])
            if pd.notna(row.get("average_dollar_volume"))
            else None,
            reason="allocation_target",
        )
        for row in combined.to_dict(orient="records")
    ]
    result = simulate_execution(requests=requests, config=config)
    paths = write_execution_artifacts(result, Path(args.output_dir))
    print(f"Executable orders: {result.summary['executable_order_count']}")
    print(f"Rejected orders: {result.summary['rejected_order_count']}")
    print("Artifacts:")
    for name, path in sorted(paths.items()):
        print(f"  {name}: {path}")
