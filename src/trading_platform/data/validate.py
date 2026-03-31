from __future__ import annotations

import pandas as pd

from trading_platform.ingestion.validation import (
    raise_for_validation_errors,
    validate_basic_bar_frame,
)


def validate_bars(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate canonical OHLCV bar data.

    Returns the dataframe unchanged if valid.
    Raises ValueError on failure.
    """
    report = validate_basic_bar_frame(df)
    raise_for_validation_errors(report)
    return df
