from __future__ import annotations

from trading_platform.universes.static import STATIC_UNIVERSES


def list_universes() -> list[str]:
    return sorted(STATIC_UNIVERSES.keys())


def get_universe_symbols(name: str) -> list[str]:
    try:
        return list(STATIC_UNIVERSES[name])
    except KeyError as exc:
        available = ", ".join(list_universes())
        raise ValueError(
            f"Unknown universe '{name}'. Available universes: {available}"
        ) from exc
