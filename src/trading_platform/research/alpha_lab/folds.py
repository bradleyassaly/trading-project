from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class WalkForwardFold:
    fold_id: int
    train_start: pd.Timestamp
    train_end: pd.Timestamp
    test_start: pd.Timestamp
    test_end: pd.Timestamp


def build_walk_forward_folds(
    timestamps: pd.Series,
    *,
    train_size: int,
    test_size: int,
    step_size: int | None = None,
    min_train_size: int | None = None,
) -> list[WalkForwardFold]:
    """
    Build rolling walk-forward folds from an ordered timestamp series.

    Parameters
    ----------
    timestamps
        Sorted timestamp series.
    train_size
        Number of rows in each training window.
    test_size
        Number of rows in each test window.
    step_size
        Number of rows to advance after each fold. Defaults to test_size.
    min_train_size
        Optional minimum train size. Defaults to train_size.
    """
    if timestamps.empty:
        return []

    ts = pd.Series(pd.to_datetime(timestamps)).sort_values().reset_index(drop=True)
    step = test_size if step_size is None else step_size
    min_train = train_size if min_train_size is None else min_train_size

    folds: list[WalkForwardFold] = []
    fold_id = 1

    train_end_idx = train_size - 1

    while True:
        train_start_idx = max(0, train_end_idx - train_size + 1)
        current_train_len = train_end_idx - train_start_idx + 1
        test_start_idx = train_end_idx + 1
        test_end_idx = test_start_idx + test_size - 1

        if current_train_len < min_train:
            train_end_idx += step
            continue

        if test_end_idx >= len(ts):
            break

        folds.append(
            WalkForwardFold(
                fold_id=fold_id,
                train_start=ts.iloc[train_start_idx],
                train_end=ts.iloc[train_end_idx],
                test_start=ts.iloc[test_start_idx],
                test_end=ts.iloc[test_end_idx],
            )
        )

        fold_id += 1
        train_end_idx += step

    return folds