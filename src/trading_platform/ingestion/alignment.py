from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class TimeAlignmentConfig:
    left_timestamp_column: str = "timestamp"
    right_timestamp_column: str = "timestamp"
    group_columns: tuple[str, ...] = ("symbol",)
    right_timestamp_mode: str = "event"
    direction: str = "backward"
    allow_exact_matches: bool = True
    tolerance: str | pd.Timedelta | None = None
    right_prefix: str = "aligned_"

    def __post_init__(self) -> None:
        if self.right_timestamp_mode not in {"event", "period_end_effective_next"}:
            raise ValueError("right_timestamp_mode must be 'event' or 'period_end_effective_next'")
        if self.direction != "backward":
            raise ValueError("direction must be 'backward' to prevent forward-looking leakage")
        if not self.group_columns:
            raise ValueError("group_columns must not be empty")


def _infer_group_step(timestamps: pd.Series) -> pd.Timedelta:
    deltas = timestamps.sort_values().diff().dropna()
    if deltas.empty:
        return pd.Timedelta(days=1)
    return deltas.min()


def _effective_right_frame(
    frame: pd.DataFrame,
    *,
    timestamp_column: str,
    group_columns: tuple[str, ...],
    mode: str,
) -> pd.DataFrame:
    prepared = frame.copy()
    prepared[timestamp_column] = pd.to_datetime(prepared[timestamp_column], errors="coerce")
    prepared = prepared.dropna(subset=[timestamp_column]).sort_values(list(group_columns) + [timestamp_column]).reset_index(drop=True)
    prepared["_alignment_timestamp"] = prepared[timestamp_column]
    if mode == "event":
        return prepared

    prepared["_alignment_timestamp"] = prepared.groupby(list(group_columns))[timestamp_column].shift(-1)
    for group_key, group_index in prepared.groupby(list(group_columns)).groups.items():
        index_values = list(group_index)
        last_index = index_values[-1]
        if pd.isna(prepared.loc[last_index, "_alignment_timestamp"]):
            group_timestamps = prepared.loc[index_values, timestamp_column]
            prepared.loc[last_index, "_alignment_timestamp"] = prepared.loc[last_index, timestamp_column] + _infer_group_step(group_timestamps)
    return prepared


def align_timeframe_frames(
    left_frame: pd.DataFrame,
    right_frame: pd.DataFrame,
    *,
    config: TimeAlignmentConfig | None = None,
) -> pd.DataFrame:
    resolved = config or TimeAlignmentConfig()
    if left_frame.empty:
        return left_frame.copy()
    if right_frame.empty:
        result = left_frame.copy()
        for column in right_frame.columns:
            if column in set(resolved.group_columns) | {resolved.right_timestamp_column}:
                continue
            result[f"{resolved.right_prefix}{column}"] = pd.NA
        return result

    left = left_frame.copy()
    left[resolved.left_timestamp_column] = pd.to_datetime(left[resolved.left_timestamp_column], errors="coerce")
    left = left.dropna(subset=[resolved.left_timestamp_column]).sort_values(
        list(resolved.group_columns) + [resolved.left_timestamp_column]
    ).reset_index(drop=True)

    right = _effective_right_frame(
        right_frame,
        timestamp_column=resolved.right_timestamp_column,
        group_columns=resolved.group_columns,
        mode=resolved.right_timestamp_mode,
    )
    rename_map = {
        column: f"{resolved.right_prefix}{column}"
        for column in right.columns
        if column not in set(resolved.group_columns) | {resolved.right_timestamp_column, "_alignment_timestamp"}
    }
    right = right.rename(columns=rename_map)

    tolerance = pd.Timedelta(resolved.tolerance) if isinstance(resolved.tolerance, str) else resolved.tolerance
    merged = pd.merge_asof(
        left,
        right.sort_values(list(resolved.group_columns) + ["_alignment_timestamp"]),
        left_on=resolved.left_timestamp_column,
        right_on="_alignment_timestamp",
        by=list(resolved.group_columns),
        direction=resolved.direction,
        allow_exact_matches=resolved.allow_exact_matches,
        tolerance=tolerance,
    )
    return merged.drop(columns=["_alignment_timestamp"], errors="ignore")


def align_daily_to_intraday_without_lookahead(
    intraday_frame: pd.DataFrame,
    daily_frame: pd.DataFrame,
    *,
    right_prefix: str = "daily_",
) -> pd.DataFrame:
    return align_timeframe_frames(
        intraday_frame,
        daily_frame,
        config=TimeAlignmentConfig(
            group_columns=("symbol",),
            right_timestamp_mode="period_end_effective_next",
            right_prefix=right_prefix,
        ),
    )
