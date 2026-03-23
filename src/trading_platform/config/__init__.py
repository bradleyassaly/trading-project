from trading_platform.config.models import (
    BacktestConfig,
    FeatureConfig,
    IngestConfig,
    ParameterSweepConfig,
    ResearchWorkflowConfig,
    WalkForwardConfig,
)

__all__ = [
    "IngestConfig",
    "FeatureConfig",
    "BacktestConfig",
    "ResearchWorkflowConfig",
    "ParameterSweepConfig",
    "WalkForwardConfig",
    "load_research_workflow_config",
    "load_parameter_sweep_config",
    "load_walk_forward_config",
]


def load_research_workflow_config(*args, **kwargs):
    from trading_platform.config.loader import load_research_workflow_config as _impl

    return _impl(*args, **kwargs)


def load_parameter_sweep_config(*args, **kwargs):
    from trading_platform.config.loader import load_parameter_sweep_config as _impl

    return _impl(*args, **kwargs)


def load_walk_forward_config(*args, **kwargs):
    from trading_platform.config.loader import load_walk_forward_config as _impl

    return _impl(*args, **kwargs)
