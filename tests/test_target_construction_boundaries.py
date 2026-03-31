from __future__ import annotations

import pandas as pd

from trading_platform.execution.policies import ExecutionPolicy
from trading_platform.paper.models import PaperTradingConfig
from trading_platform.services import target_construction_service
from trading_platform.services.target_construction_service import (
    TargetConstructionRuntimeDependencies,
    configure_runtime_dependencies,
)


def test_configure_runtime_dependencies_supports_explicit_target_construction_boundary() -> None:
    original_dependencies = TargetConstructionRuntimeDependencies(
        load_feature_frame_fn=target_construction_service.load_feature_frame,
        resolve_feature_frame_path_fn=target_construction_service.resolve_feature_frame_path,
        run_xsec_momentum_topn_fn=target_construction_service.run_xsec_momentum_topn,
        normalize_price_frame_fn=target_construction_service.normalize_price_frame,
        signal_registry=target_construction_service.SIGNAL_REGISTRY,
        build_group_series_fn=target_construction_service.build_group_series,
        build_top_n_portfolio_weights_fn=target_construction_service.build_top_n_portfolio_weights,
        normalize_paper_weighting_scheme_fn=target_construction_service.normalize_paper_weighting_scheme,
        execution_policy_cls=target_construction_service.ExecutionPolicy,
        build_executed_weights_fn=target_construction_service.build_executed_weights,
    )
    frames = {
        "AAPL": pd.DataFrame(
            {
                "timestamp": pd.date_range("2025-01-01", periods=4, freq="D"),
                "close": [100.0, 101.0, 102.0, 103.0],
            }
        )
    }

    def fake_load_feature_frame(symbol: str) -> pd.DataFrame:
        return frames[symbol]

    def fake_signal_fn(df: pd.DataFrame, **_: object) -> pd.DataFrame:
        out = df.copy()
        out["asset_return"] = out["close"].pct_change().fillna(0.0)
        out["score"] = [1.0, 2.0, 3.0, 4.0]
        return out

    try:
        configure_runtime_dependencies(
            dependencies=TargetConstructionRuntimeDependencies(
                load_feature_frame_fn=fake_load_feature_frame,
                signal_registry={"sma_cross": fake_signal_fn},
                build_group_series_fn=lambda symbols, path=None: pd.Series(index=symbols, data="all"),
                build_top_n_portfolio_weights_fn=target_construction_service.build_top_n_portfolio_weights,
                normalize_paper_weighting_scheme_fn=lambda scheme: scheme,
                execution_policy_cls=ExecutionPolicy,
                build_executed_weights_fn=target_construction_service.build_executed_weights,
            )
        )
        result = target_construction_service.build_target_construction_result(
            config=PaperTradingConfig(
                symbols=["AAPL"],
                strategy="sma_cross",
                top_n=1,
                initial_cash=10_000.0,
            )
        )

        assert result.as_of == "2025-01-04"
        assert result.effective_target_weights == {"AAPL": 1.0}
    finally:
        configure_runtime_dependencies(dependencies=original_dependencies)
