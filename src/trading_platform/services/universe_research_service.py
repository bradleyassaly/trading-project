from __future__ import annotations

from dataclasses import replace
from typing import Any

from trading_platform.config.models import ResearchWorkflowConfig
from trading_platform.data.providers.base import BarDataProvider
from trading_platform.services.research_service import run_research_workflow


def run_universe_research_workflow(
    symbols: list[str],
    base_config: ResearchWorkflowConfig,
    provider: BarDataProvider | None = None,
    continue_on_error: bool = True,
) -> dict[str, Any]:
    """
    Run the single-symbol research workflow across a list of symbols.

    Returns:
    - results: successful outputs keyed by symbol
    - errors: failures keyed by symbol
    """
    if not symbols:
        raise ValueError("symbols must be a non-empty list")

    results: dict[str, dict[str, object]] = {}
    errors: dict[str, str] = {}

    for symbol in symbols:
        symbol_config = replace(base_config, symbol=symbol)

        try:
            results[symbol] = run_research_workflow(
                config=symbol_config,
                provider=provider,
            )
        except Exception as exc:
            errors[symbol] = f"{type(exc).__name__}: {exc}"
            if not continue_on_error:
                raise

    return {
        "results": results,
        "errors": errors,
    }