from __future__ import annotations

from typing import Any

from trading_platform.kalshi.signals import (
    KALSHI_CALIBRATION_DRIFT,
    KALSHI_TIME_DECAY,
    KALSHI_VOLUME_SPIKE,
    KalshiSignalFamily,
)
from trading_platform.kalshi.signals_base_rate import KALSHI_BASE_RATE
from trading_platform.kalshi.signals_informed_flow import (
    InformedFlowSignalConfig,
    make_informed_flow_signal_families,
)
from trading_platform.kalshi.signals_metaculus import KALSHI_METACULUS_DIVERGENCE


def known_kalshi_signal_families(
    *,
    informed_flow_config: dict[str, Any] | None = None,
) -> dict[str, KalshiSignalFamily]:
    flow_families = make_informed_flow_signal_families(
        InformedFlowSignalConfig(**(informed_flow_config or {}))
    )
    flow_family_map = {family.name: family for family in flow_families}
    families = [
        KALSHI_CALIBRATION_DRIFT,
        KALSHI_VOLUME_SPIKE,
        KALSHI_TIME_DECAY,
        KALSHI_BASE_RATE,
        KALSHI_METACULUS_DIVERGENCE,
        flow_family_map["kalshi_taker_imbalance"],
        flow_family_map["kalshi_large_order"],
        flow_family_map["kalshi_unexplained_move"],
    ]
    return {family.name: family for family in families}
