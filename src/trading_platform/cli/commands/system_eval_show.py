from __future__ import annotations

from trading_platform.system_evaluation.service import load_system_evaluation


def cmd_system_eval_show(args) -> None:
    payload = load_system_evaluation(args.evaluation)
    row = payload.get("row", {})
    print(f"Run id: {row.get('run_id')}")
    print(f"Experiment: {row.get('experiment_name') or 'n/a'}")
    print(f"Status: {row.get('status')}")
    print(f"Total return: {row.get('total_return')}")
    print(f"Sharpe: {row.get('sharpe')}")
    print(f"Max drawdown: {row.get('max_drawdown')}")
    print(f"Regime: {row.get('regime') or 'n/a'}")
