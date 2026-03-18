from __future__ import annotations

from pathlib import Path

import pandas as pd

from trading_platform.settings import METADATA_DIR


DEFAULT_GROUPS_PATH = METADATA_DIR / "symbol_groups.csv"


def load_symbol_groups(
    *,
    path: str | Path | None = None,
) -> dict[str, str]:
    csv_path = Path(path) if path is not None else DEFAULT_GROUPS_PATH

    if not csv_path.exists():
        raise FileNotFoundError(f"Symbol groups file not found: {csv_path}")

    df = pd.read_csv(csv_path)

    required = {"symbol", "group"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"Symbol groups file missing required columns: {sorted(missing)}"
        )

    df = df.copy()
    df["symbol"] = df["symbol"].astype(str).str.upper()
    df["group"] = df["group"].astype(str)

    mapping = dict(zip(df["symbol"], df["group"]))
    return mapping


def build_group_series(
    symbols: list[str],
    *,
    group_map: dict[str, str] | None = None,
    path: str | Path | None = None,
    default_group: str = "UNGROUPED",
) -> pd.Series:
    if group_map is not None:
        mapping = group_map
    else:
        try:
            mapping = load_symbol_groups(path=path)
        except FileNotFoundError:
            mapping = {}

    return pd.Series(
        {symbol: mapping.get(symbol.upper(), default_group) for symbol in symbols},
        dtype="object",
    )