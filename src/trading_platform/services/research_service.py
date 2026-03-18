from __future__ import annotations

from trading_platform.config.models import (
    BacktestConfig,
    FeatureConfig,
    IngestConfig,
    ResearchWorkflowConfig,
)
from trading_platform.data.providers.base import BarDataProvider
from trading_platform.services.backtest_service import run_backtest_workflow
from trading_platform.services.pipeline_service import run_research_prep_pipeline


def run_research_workflow(
    config: ResearchWorkflowConfig,
    provider: BarDataProvider | None = None,
) -> dict[str, object]:
    """
    Run the full single-symbol research workflow.
    """
    ingest_config = IngestConfig(
        symbol=config.symbol,
        start=config.start,
        end=config.end,
        interval=config.interval,
    )
    feature_config = FeatureConfig(
        symbol=config.symbol,
        feature_groups=config.feature_groups,
    )
    backtest_config = BacktestConfig(
        symbol=config.symbol,
        strategy=config.strategy,
        fast=config.fast,
        slow=config.slow,
        lookback=config.lookback,
        cash=config.cash,
        commission=config.commission,
    )

    prep_outputs = run_research_prep_pipeline(
        ingest_config=ingest_config,
        feature_config=feature_config,
        provider=provider,
    )

    backtest_outputs = run_backtest_workflow(
        config=backtest_config,
    )

    return {
        "normalized_path": prep_outputs["normalized_path"],
        "features_path": prep_outputs["features_path"],
        "stats": backtest_outputs["stats"],
        "experiment_id": backtest_outputs["experiment_id"],
    }