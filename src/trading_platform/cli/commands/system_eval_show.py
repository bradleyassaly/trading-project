from __future__ import annotations

from trading_platform.system_evaluation.service import load_system_evaluation


def cmd_system_eval_show(args) -> None:
    payload = load_system_evaluation(args.evaluation)
    row = payload.get("row", {})
    diagnostic = payload.get("diagnostic", {})
    history_metrics = payload.get("history_metrics", {})
    print(f"Run id: {row.get('run_id')}")
    print(f"Experiment: {row.get('experiment_name') or 'n/a'}")
    print(f"Status: {row.get('status')}")
    print(f"Total return: {row.get('total_return')}")
    print(f"Sharpe: {row.get('sharpe')}")
    print(f"Max drawdown: {row.get('max_drawdown')}")
    print(f"Regime: {row.get('regime') or 'n/a'}")
    if row.get("equity_observation_count") is not None:
        print(f"Equity observations: {row.get('equity_observation_count')}")
    if row.get("return_observation_count") is not None:
        print(f"Return observations: {row.get('return_observation_count')}")
    metric_warnings = row.get("metric_warnings") or diagnostic.get("metric_warnings") or []
    if metric_warnings:
        if isinstance(metric_warnings, str):
            metric_warnings = [item for item in metric_warnings.split("|") if item]
        print(f"Metric warnings: {', '.join(metric_warnings)}")
    if history_metrics:
        print(f"History total return: {history_metrics.get('total_return')}")
        print(f"History sharpe: {history_metrics.get('sharpe')}")
        print(f"History max drawdown: {history_metrics.get('max_drawdown')}")
