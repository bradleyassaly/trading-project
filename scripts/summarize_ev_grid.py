from __future__ import annotations

import json
from pathlib import Path


RUNS = [
    ("baseline", Path("artifacts/daily_replay/ev_baseline/replay_summary.json")),
    ("hard", Path("artifacts/daily_replay/ev_hard/replay_summary.json")),
    ("soft5", Path("artifacts/daily_replay/ev_soft5/replay_summary.json")),
    ("soft10", Path("artifacts/daily_replay/ev_soft10/replay_summary.json")),
]


def _load_summary(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _fmt(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.6f}"
    return str(value)


def main() -> None:
    rows = []
    for run_name, path in RUNS:
        payload = _load_summary(path)
        rows.append(
            {
                "run_name": run_name,
                "net_total_pnl": payload.get("net_total_pnl"),
                "gross_total_pnl": payload.get("gross_total_pnl"),
                "total_execution_cost": payload.get("total_execution_cost"),
                "cost_drag_pct": payload.get("cost_drag_pct"),
                "total_order_count": payload.get("total_order_count"),
                "trade_day_count": payload.get("trade_day_count"),
                "avg_ev_executed_trades": payload.get("avg_ev_executed_trades"),
                "avg_ev_weight_multiplier": payload.get("avg_ev_weight_multiplier"),
                "ev_weighted_exposure": payload.get("ev_weighted_exposure"),
            }
        )

    headers = [
        "run_name",
        "net_total_pnl",
        "gross_total_pnl",
        "total_execution_cost",
        "cost_drag_pct",
        "total_order_count",
        "trade_day_count",
        "avg_ev_executed_trades",
        "avg_ev_weight_multiplier",
        "ev_weighted_exposure",
    ]
    widths = {
        header: max(len(header), max(len(_fmt(row.get(header))) for row in rows))
        for header in headers
    }
    header_line = " | ".join(header.ljust(widths[header]) for header in headers)
    separator = "-+-".join("-" * widths[header] for header in headers)
    print(header_line)
    print(separator)
    for row in rows:
        print(" | ".join(_fmt(row.get(header)).ljust(widths[header]) for header in headers))


if __name__ == "__main__":
    main()
