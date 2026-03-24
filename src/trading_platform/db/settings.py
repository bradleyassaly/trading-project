from __future__ import annotations

from dataclasses import dataclass
import os


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class DatabaseSettings:
    enable_database_metadata: bool = False
    database_url: str | None = None
    database_schema: str | None = None
    echo_sql: bool = False

    @property
    def enabled(self) -> bool:
        return bool(self.enable_database_metadata and self.database_url)


def resolve_database_settings(
    *,
    enable_database_metadata: bool | None = None,
    database_url: str | None = None,
    database_schema: str | None = None,
    echo_sql: bool | None = None,
) -> DatabaseSettings:
    env_enable = _env_flag("TRADING_PLATFORM_ENABLE_DATABASE_METADATA", default=False)
    env_echo = _env_flag("TRADING_PLATFORM_DATABASE_ECHO", default=False)
    resolved_enable = env_enable if enable_database_metadata is None else bool(enable_database_metadata)
    resolved_url = database_url or os.getenv("TRADING_PLATFORM_DATABASE_URL")
    resolved_schema = database_schema or os.getenv("TRADING_PLATFORM_DATABASE_SCHEMA")
    resolved_echo = env_echo if echo_sql is None else bool(echo_sql)
    return DatabaseSettings(
        enable_database_metadata=resolved_enable,
        database_url=resolved_url,
        database_schema=resolved_schema,
        echo_sql=resolved_echo,
    )
