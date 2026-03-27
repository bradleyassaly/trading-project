from __future__ import annotations

import importlib
from dataclasses import dataclass

OPTIONAL_DEPENDENCY_EXTRAS: dict[str, str] = {
    "financedatabase": "classification",
    "alphalens": "research_diagnostics",
    "quantstats": "research_diagnostics",
    "pypfopt": "portfolio_optimizers",
    "vectorbt": "validation",
}


class OptionalDependencyError(ImportError):
    """Raised when an optional integration dependency is unavailable."""


@dataclass(frozen=True)
class OptionalDependencyStatus:
    package_name: str
    available: bool
    extra_name: str | None = None


def dependency_available(package_name: str) -> bool:
    try:
        importlib.import_module(package_name)
    except ImportError:
        return False
    return True


def dependency_status(package_name: str) -> OptionalDependencyStatus:
    return OptionalDependencyStatus(
        package_name=package_name,
        available=dependency_available(package_name),
        extra_name=OPTIONAL_DEPENDENCY_EXTRAS.get(package_name),
    )


def require_dependency(package_name: str, *, purpose: str, package_override=None):
    if package_override is not None:
        return package_override
    try:
        return importlib.import_module(package_name)
    except ImportError as exc:  # pragma: no cover - exercised via tests with package_override/mocking
        extra_name = OPTIONAL_DEPENDENCY_EXTRAS.get(package_name)
        install_hint = (
            f"Install with `pip install trading-platform[{extra_name}]`."
            if extra_name
            else f"Install the `{package_name}` package."
        )
        raise OptionalDependencyError(
            f"Optional dependency `{package_name}` is required for {purpose}. {install_hint}"
        ) from exc
