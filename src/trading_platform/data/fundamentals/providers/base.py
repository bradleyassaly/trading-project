from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class ProviderFetchResult:
    company_master_df: pd.DataFrame
    filing_metadata_df: pd.DataFrame
    fundamental_values_df: pd.DataFrame
    diagnostics: dict[str, Any] = field(default_factory=dict)


class FundamentalsProvider(ABC):
    provider_name: str = "base"

    @abstractmethod
    def is_configured(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def fetch(self, *, symbols: list[str]) -> ProviderFetchResult:
        raise NotImplementedError
