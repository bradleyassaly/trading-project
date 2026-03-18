from __future__ import annotations

import pandas as pd

from trading_platform.simulation.metrics import summarize_equity_curve


def summarize_portfolio_result(result: pd.DataFrame) -> dict[str, float]:
    summary = {}
    summary.update(
        summarize_equity_curve(
            returns=result["portfolio_return_net"],
            equity=result["portfolio_equity"],
            prefix="portfolio_",
        )
    )
    summary.update(
        summarize_equity_curve(
            returns=result["benchmark_return"],
            equity=result["benchmark_equity"],
            prefix="benchmark_",
        )
    )
    summary["excess_total_return"] = (
        summary["portfolio_total_return"] - summary["benchmark_total_return"]
    )
    return summary