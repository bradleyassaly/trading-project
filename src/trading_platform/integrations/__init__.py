"""Optional third-party integrations behind thin platform-owned adapters."""

from trading_platform.integrations.optional_dependencies import (
    OptionalDependencyError,
    dependency_available,
    require_dependency,
)

__all__ = [
    "OptionalDependencyError",
    "dependency_available",
    "require_dependency",
]
