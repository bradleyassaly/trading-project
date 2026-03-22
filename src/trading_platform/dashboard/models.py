from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class DashboardConfig:
    artifacts_root: str = "artifacts"
    host: str = "127.0.0.1"
    port: int = 8000

    def __post_init__(self) -> None:
        if not self.artifacts_root or not str(self.artifacts_root).strip():
            raise ValueError("artifacts_root must be a non-empty string")
        if not self.host or not str(self.host).strip():
            raise ValueError("host must be a non-empty string")
        if self.port <= 0:
            raise ValueError("port must be > 0")

    def to_dict(self) -> dict[str, object]:
        return asdict(self)
