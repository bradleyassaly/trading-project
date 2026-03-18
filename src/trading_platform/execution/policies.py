from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


ExecutionTiming = Literal["next_bar"]
RebalanceFrequency = Literal["daily", "weekly", "monthly"]


@dataclass(frozen=True)
class ExecutionPolicy:
    timing: ExecutionTiming = "next_bar"
    rebalance_frequency: RebalanceFrequency = "daily"