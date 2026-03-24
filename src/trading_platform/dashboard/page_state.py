from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import parse_qs, urlencode


def _first_value(query: dict[str, list[str]], key: str) -> str | None:
    values = query.get(key) or []
    if not values:
        return None
    value = values[0]
    return value if value not in ("", None) else None


def _positive_int(value: str | None, default: int) -> int:
    try:
        parsed = int(value or default)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


@dataclass(frozen=True)
class FilterChip:
    key: str
    label: str
    value: str
    clear_url: str


@dataclass(frozen=True)
class PageState:
    path: str
    filters: dict[str, str]
    limit: int
    offset: int
    default_limit: int

    @classmethod
    def from_query(
        cls,
        path: str,
        query: dict[str, list[str]],
        *,
        filter_keys: list[str],
        default_limit: int,
    ) -> PageState:
        filters: dict[str, str] = {}
        for key in filter_keys:
            value = _first_value(query, key)
            if value is not None:
                filters[key] = value
        return cls(
            path=path,
            filters=filters,
            limit=_positive_int(_first_value(query, "limit"), default_limit),
            offset=max(_positive_int(_first_value(query, "offset"), 0), 0),
            default_limit=default_limit,
        )

    @classmethod
    def from_url(cls, path: str, *, default_limit: int) -> PageState:
        path_only, _, query_string = path.partition("?")
        return cls.from_query(path_only, parse_qs(query_string), filter_keys=[], default_limit=default_limit)

    def to_service_filters(self) -> dict[str, str | int]:
        payload: dict[str, str | int] = dict(self.filters)
        payload["limit"] = self.limit
        payload["offset"] = self.offset
        return payload

    def with_updates(self, **updates: str | int | None) -> PageState:
        filters = dict(self.filters)
        limit = self.limit
        offset = self.offset
        for key, value in updates.items():
            if key == "limit":
                limit = _positive_int(str(value) if value is not None else None, self.default_limit)
                continue
            if key == "offset":
                offset = max(int(value or 0), 0)
                continue
            if value in (None, ""):
                filters.pop(key, None)
            else:
                filters[key] = str(value)
        return PageState(path=self.path, filters=filters, limit=limit, offset=offset, default_limit=self.default_limit)

    def query_params(self) -> list[tuple[str, str]]:
        params = sorted(self.filters.items())
        if self.limit != self.default_limit:
            params.append(("limit", str(self.limit)))
        if self.offset:
            params.append(("offset", str(self.offset)))
        return params

    def url(self, **updates: str | int | None) -> str:
        state = self.with_updates(**updates) if updates else self
        query_string = urlencode(state.query_params())
        return state.path if not query_string else f"{state.path}?{query_string}"

    def clear_url(self) -> str:
        return self.path

    def active_chips(self, labels: dict[str, str]) -> list[FilterChip]:
        chips: list[FilterChip] = []
        for key, value in self.filters.items():
            chips.append(
                FilterChip(
                    key=key,
                    label=labels.get(key, key.replace("_", " ").title()),
                    value=value,
                    clear_url=self.url(offset=0, **{key: None}),
                )
            )
        return chips

    def current_url(self) -> str:
        return self.url()

