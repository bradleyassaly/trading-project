from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from trading_platform.metadata.groups import build_group_series
from trading_platform.signals.loaders import load_feature_frame
from trading_platform.universe_provenance.models import (
    BaseUniverseDefinition,
    SubUniverseDefinition,
    UniverseBuildBundle,
    UniverseBuildSummary,
    UniverseFilterDefinition,
    UniverseFilterResult,
    UniverseMembershipRecord,
)


def _normalize_symbols(symbols: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for symbol in symbols:
        cleaned = str(symbol).strip().upper()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        ordered.append(cleaned)
    return ordered


def parse_universe_filter_definitions(
    raw_filters: list[dict[str, Any] | UniverseFilterDefinition] | None,
) -> list[UniverseFilterDefinition]:
    parsed: list[UniverseFilterDefinition] = []
    for item in raw_filters or []:
        if isinstance(item, UniverseFilterDefinition):
            parsed.append(item)
            continue
        payload = dict(item)
        filter_name = str(payload.get("filter_name") or payload.get("name") or payload.get("filter_type") or "").strip()
        filter_type = str(payload.get("filter_type") or filter_name).strip()
        if not filter_name or not filter_type:
            raise ValueError("universe filter definitions require filter_name and filter_type")
        params = dict(payload.get("params", {}))
        for key in ("symbols", "min_price", "min_bars", "min_avg_dollar_volume", "window", "groups", "min_volatility", "max_volatility"):
            if key in payload and key not in params:
                params[key] = payload[key]
        parsed.append(
            UniverseFilterDefinition(
                filter_name=filter_name,
                filter_type=filter_type,
                stage_name=str(payload.get("stage_name", "screening")),
                enabled=bool(payload.get("enabled", True)),
                threshold=payload.get("threshold"),
                rule_description=payload.get("rule_description"),
                params=params,
            )
        )
    return parsed


def _feature_metrics(
    symbol: str,
    feature_cache: dict[str, pd.DataFrame | None],
    loader: Callable[[str], pd.DataFrame],
) -> tuple[pd.DataFrame | None, str | None]:
    if symbol in feature_cache:
        cached = feature_cache[symbol]
        return cached, None if cached is not None else "missing_feature_frame"
    try:
        frame = loader(symbol)
        feature_cache[symbol] = frame
        return frame, None
    except Exception:
        feature_cache[symbol] = None
        return None, "missing_feature_frame"


def _latest_close(frame: pd.DataFrame | None) -> float | None:
    if frame is None or "close" not in frame.columns:
        return None
    close = pd.to_numeric(frame["close"], errors="coerce").dropna()
    if close.empty:
        return None
    return float(close.iloc[-1])


def _feature_history_bars(frame: pd.DataFrame | None) -> int | None:
    if frame is None:
        return None
    if "close" in frame.columns:
        clean = pd.to_numeric(frame["close"], errors="coerce").dropna()
        return int(clean.shape[0])
    return int(frame.shape[0])


def _avg_dollar_volume(frame: pd.DataFrame | None, *, window: int) -> float | None:
    if frame is None or "close" not in frame.columns or "volume" not in frame.columns:
        return None
    close = pd.to_numeric(frame["close"], errors="coerce")
    volume = pd.to_numeric(frame["volume"], errors="coerce")
    dollar_volume = (close * volume).dropna()
    if dollar_volume.empty:
        return None
    tail = dollar_volume.tail(max(int(window), 1))
    return float(tail.mean())


def _volatility(frame: pd.DataFrame | None, *, window: int) -> float | None:
    if frame is None or "close" not in frame.columns:
        return None
    close = pd.to_numeric(frame["close"], errors="coerce").dropna()
    if close.shape[0] < 2:
        return None
    returns = close.pct_change().dropna().tail(max(int(window), 2))
    if returns.empty:
        return None
    return float(returns.std(ddof=0))


def _evaluate_filter(
    *,
    definition: UniverseFilterDefinition,
    symbol: str,
    as_of: str,
    universe_id: str | None,
    base_universe_id: str | None,
    sub_universe_id: str | None,
    active: bool,
    feature_cache: dict[str, pd.DataFrame | None],
    feature_loader: Callable[[str], pd.DataFrame],
    group_map: dict[str, str],
) -> UniverseFilterResult:
    if not definition.enabled:
        return UniverseFilterResult(
            universe_id=universe_id,
            base_universe_id=base_universe_id,
            sub_universe_id=sub_universe_id,
            symbol=symbol,
            as_of=as_of,
            stage_name=definition.stage_name,
            filter_name=definition.filter_name,
            filter_type=definition.filter_type,
            status="disabled",
            metadata={"params": definition.params},
        )
    if not active:
        return UniverseFilterResult(
            universe_id=universe_id,
            base_universe_id=base_universe_id,
            sub_universe_id=sub_universe_id,
            symbol=symbol,
            as_of=as_of,
            stage_name=definition.stage_name,
            filter_name=definition.filter_name,
            filter_type=definition.filter_type,
            status="skipped",
            metadata={"reason": "previously_excluded"},
        )

    filter_type = definition.filter_type
    params = definition.params
    threshold = definition.threshold
    frame: pd.DataFrame | None = None
    feature_error: str | None = None
    observed_value: float | str | None = None
    passed: bool | None = None
    inclusion_reason: str | None = None
    exclusion_reason: str | None = None
    status = "pass"

    if filter_type == "symbol_include_list":
        allowed = {str(item).upper() for item in params.get("symbols", [])}
        passed = symbol in allowed
        threshold = "|".join(sorted(allowed))
        inclusion_reason = "symbol_in_include_list" if passed else None
        exclusion_reason = None if passed else "excluded_by_symbol_include_list"
    elif filter_type == "symbol_exclude_list":
        blocked = {str(item).upper() for item in params.get("symbols", [])}
        passed = symbol not in blocked
        threshold = "|".join(sorted(blocked))
        inclusion_reason = "symbol_not_in_exclude_list" if passed else None
        exclusion_reason = None if passed else "excluded_by_symbol_exclude_list"
    elif filter_type == "sector_include_list":
        allowed = {str(item) for item in params.get("groups", [])}
        observed_value = group_map.get(symbol)
        if observed_value is None:
            status = "unavailable"
            passed = None
            exclusion_reason = "group_metadata_unavailable"
        else:
            passed = observed_value in allowed
            threshold = "|".join(sorted(allowed))
            inclusion_reason = "group_included" if passed else None
            exclusion_reason = None if passed else "excluded_by_sector_include_list"
    elif filter_type == "sector_exclude_list":
        blocked = {str(item) for item in params.get("groups", [])}
        observed_value = group_map.get(symbol)
        if observed_value is None:
            status = "unavailable"
            passed = None
            exclusion_reason = "group_metadata_unavailable"
        else:
            passed = observed_value not in blocked
            threshold = "|".join(sorted(blocked))
            inclusion_reason = "group_not_excluded" if passed else None
            exclusion_reason = None if passed else "excluded_by_sector_exclude_list"
    else:
        frame, feature_error = _feature_metrics(symbol, feature_cache, feature_loader)
        if filter_type == "min_price":
            threshold = threshold if threshold is not None else params.get("min_price")
            observed_value = _latest_close(frame)
            if observed_value is None:
                passed = False
                exclusion_reason = feature_error or "missing_price_data"
            else:
                passed = float(observed_value) >= float(threshold)
                inclusion_reason = "passed_min_price" if passed else None
                exclusion_reason = None if passed else "excluded_by_min_price"
        elif filter_type == "min_feature_history":
            threshold = threshold if threshold is not None else params.get("min_bars")
            observed_value = _feature_history_bars(frame)
            if observed_value is None:
                passed = False
                exclusion_reason = feature_error or "missing_feature_history"
            else:
                passed = int(observed_value) >= int(threshold)
                inclusion_reason = "passed_min_feature_history" if passed else None
                exclusion_reason = None if passed else "excluded_by_min_feature_history"
        elif filter_type == "min_avg_dollar_volume":
            threshold = threshold if threshold is not None else params.get("min_avg_dollar_volume")
            observed_value = _avg_dollar_volume(frame, window=int(params.get("window", 20)))
            if observed_value is None:
                status = "unavailable"
                passed = None
                exclusion_reason = "avg_dollar_volume_unavailable"
            else:
                passed = float(observed_value) >= float(threshold)
                inclusion_reason = "passed_min_avg_dollar_volume" if passed else None
                exclusion_reason = None if passed else "excluded_by_min_avg_dollar_volume"
        elif filter_type == "max_volatility":
            threshold = threshold if threshold is not None else params.get("max_volatility")
            observed_value = _volatility(frame, window=int(params.get("window", 20)))
            if observed_value is None:
                status = "unavailable"
                passed = None
                exclusion_reason = "volatility_unavailable"
            else:
                passed = float(observed_value) <= float(threshold)
                inclusion_reason = "passed_max_volatility" if passed else None
                exclusion_reason = None if passed else "excluded_by_max_volatility"
        elif filter_type == "min_volatility":
            threshold = threshold if threshold is not None else params.get("min_volatility")
            observed_value = _volatility(frame, window=int(params.get("window", 20)))
            if observed_value is None:
                status = "unavailable"
                passed = None
                exclusion_reason = "volatility_unavailable"
            else:
                passed = float(observed_value) >= float(threshold)
                inclusion_reason = "passed_min_volatility" if passed else None
                exclusion_reason = None if passed else "excluded_by_min_volatility"
        else:
            status = "unavailable"
            passed = None
            exclusion_reason = f"unsupported_filter_type:{filter_type}"

    if passed is False:
        status = "fail"
    elif passed is True:
        status = "pass"

    return UniverseFilterResult(
        universe_id=universe_id,
        base_universe_id=base_universe_id,
        sub_universe_id=sub_universe_id,
        symbol=symbol,
        as_of=as_of,
        stage_name=definition.stage_name,
        filter_name=definition.filter_name,
        filter_type=definition.filter_type,
        status=status,
        passed=passed,
        threshold=threshold,
        observed_value=observed_value,
        inclusion_reason=inclusion_reason,
        exclusion_reason=exclusion_reason,
        metadata={"rule_description": definition.rule_description, "params": params},
    )


def build_universe_provenance_bundle(
    *,
    symbols: list[str],
    base_universe_id: str | None,
    sub_universe_id: str | None = None,
    filter_definitions: list[dict[str, Any] | UniverseFilterDefinition] | None = None,
    feature_loader: Callable[[str], pd.DataFrame] = load_feature_frame,
    group_map_path: str | None = None,
) -> UniverseBuildBundle:
    base_symbols = _normalize_symbols(symbols)
    parsed_filters = parse_universe_filter_definitions(filter_definitions)
    feature_cache: dict[str, pd.DataFrame | None] = {}
    timestamps: list[pd.Timestamp] = []
    for symbol in base_symbols:
        frame, _error = _feature_metrics(symbol, feature_cache, feature_loader)
        if frame is not None and "timestamp" in frame.columns:
            ts = pd.to_datetime(frame["timestamp"], errors="coerce").dropna()
            if not ts.empty:
                timestamps.append(pd.Timestamp(ts.iloc[-1]))
    as_of = max(timestamps).date().isoformat() if timestamps else pd.Timestamp.utcnow().date().isoformat()
    resolved_base_universe_id = base_universe_id or "custom_symbols"
    resolved_sub_universe_id = sub_universe_id or (f"{resolved_base_universe_id}_eligible" if parsed_filters else resolved_base_universe_id)
    universe_id = resolved_sub_universe_id or resolved_base_universe_id
    group_series = build_group_series(base_symbols, path=group_map_path) if group_map_path else pd.Series(dtype="object")
    group_map = {str(symbol): str(value) for symbol, value in group_series.items() if pd.notna(value)}

    active = {symbol: True for symbol in base_symbols}
    filter_results: list[UniverseFilterResult] = []
    failures_by_symbol: dict[str, list[str]] = {symbol: [] for symbol in base_symbols}
    passes_by_symbol: dict[str, list[str]] = {symbol: [] for symbol in base_symbols}
    metadata_by_symbol: dict[str, dict[str, Any]] = {}

    for symbol in base_symbols:
        frame, _ = _feature_metrics(symbol, feature_cache, feature_loader)
        metadata_by_symbol[symbol] = {
            "latest_price": _latest_close(frame),
            "feature_history_bars": _feature_history_bars(frame),
            "avg_dollar_volume_20": _avg_dollar_volume(frame, window=20),
            "volatility_20": _volatility(frame, window=20),
        }
        if symbol in group_map:
            metadata_by_symbol[symbol]["group_label"] = group_map[symbol]

    for definition in parsed_filters:
        for symbol in base_symbols:
            result = _evaluate_filter(
                definition=definition,
                symbol=symbol,
                as_of=as_of,
                universe_id=universe_id,
                base_universe_id=resolved_base_universe_id,
                sub_universe_id=resolved_sub_universe_id,
                active=active[symbol],
                feature_cache=feature_cache,
                feature_loader=feature_loader,
                group_map=group_map,
            )
            filter_results.append(result)
            if result.passed is True:
                passes_by_symbol[symbol].append(result.filter_name)
            elif result.passed is False:
                active[symbol] = False
                failures_by_symbol[symbol].append(result.exclusion_reason or result.filter_name)

    membership_records: list[UniverseMembershipRecord] = []
    for symbol in base_symbols:
        included = active[symbol]
        inclusion_reason = "passed_all_active_filters" if included and parsed_filters else "base_universe_member"
        if included and parsed_filters:
            inclusion_reason = f"passed_sub_universe:{resolved_sub_universe_id}"
        exclusion_reason = failures_by_symbol[symbol][0] if failures_by_symbol[symbol] else None
        membership_records.append(
            UniverseMembershipRecord(
                universe_id=universe_id,
                base_universe_id=resolved_base_universe_id,
                sub_universe_id=resolved_sub_universe_id,
                symbol=symbol,
                as_of=as_of,
                inclusion_status="included" if included else "excluded",
                inclusion_reason=inclusion_reason if included else None,
                exclusion_reason=exclusion_reason,
                filter_failures=list(failures_by_symbol[symbol]),
                filter_passes=list(passes_by_symbol[symbol]),
                group_label=group_map.get(symbol),
                metadata=metadata_by_symbol[symbol],
            )
        )

    failure_counts_by_filter: dict[str, int] = {}
    unavailable_filter_count = 0
    for row in filter_results:
        if row.status == "fail":
            failure_counts_by_filter[row.filter_name] = failure_counts_by_filter.get(row.filter_name, 0) + 1
        elif row.status == "unavailable":
            unavailable_filter_count += 1

    summary = UniverseBuildSummary(
        as_of=as_of,
        universe_id=universe_id,
        base_universe_id=resolved_base_universe_id,
        sub_universe_id=resolved_sub_universe_id,
        base_symbol_count=len(base_symbols),
        eligible_symbol_count=sum(1 for row in membership_records if row.inclusion_status == "included"),
        excluded_symbol_count=sum(1 for row in membership_records if row.inclusion_status == "excluded"),
        unavailable_filter_count=unavailable_filter_count,
        active_filter_count=sum(1 for row in parsed_filters if row.enabled),
        active_filters=[row.filter_name for row in parsed_filters if row.enabled],
        failure_counts_by_filter=failure_counts_by_filter,
        status_counts={
            "included": sum(1 for row in membership_records if row.inclusion_status == "included"),
            "excluded": sum(1 for row in membership_records if row.inclusion_status == "excluded"),
        },
        metadata={"group_map_path": group_map_path},
    )
    return UniverseBuildBundle(
        base_definition=BaseUniverseDefinition(
            universe_id=resolved_base_universe_id,
            source_type="named_universe" if base_universe_id else "custom_symbols",
            symbols=base_symbols,
            rule_description="Configured base universe before sub-universe screening.",
        ),
        sub_universe_definition=SubUniverseDefinition(
            sub_universe_id=resolved_sub_universe_id,
            base_universe_id=resolved_base_universe_id,
            rule_description="Eligible symbols after sequential universe filters.",
        ),
        filter_definitions=parsed_filters,
        filter_results=filter_results,
        membership_records=membership_records,
        summary=summary,
    )


def summarize_universe_membership(record: UniverseMembershipRecord | dict[str, Any]) -> str:
    payload = record.to_dict() if hasattr(record, "to_dict") else dict(record)
    if payload.get("inclusion_status") == "included":
        return str(payload.get("inclusion_reason") or "included")
    return str(payload.get("exclusion_reason") or "excluded")


def summarize_filter_failures(
    symbol: str,
    bundle: UniverseBuildBundle | None,
) -> str:
    if bundle is None:
        return "no universe provenance"
    failures = [
        row.exclusion_reason or row.filter_name
        for row in bundle.filter_results
        if row.symbol == symbol and row.status == "fail"
    ]
    return " | ".join(failures) if failures else "no filter failures"


def summarize_sub_universe_reason(record: UniverseMembershipRecord | dict[str, Any]) -> str:
    payload = record.to_dict() if hasattr(record, "to_dict") else dict(record)
    sub_universe_id = payload.get("sub_universe_id") or payload.get("universe_id")
    summary = summarize_universe_membership(payload)
    if sub_universe_id:
        return f"{sub_universe_id} | {summary}"
    return summary


def summarize_universe_build(bundle: UniverseBuildBundle | None) -> str:
    if bundle is None or bundle.summary is None:
        return "no universe build summary"
    summary = bundle.summary
    return (
        f"base={summary.base_symbol_count} | "
        f"eligible={summary.eligible_symbol_count} | "
        f"excluded={summary.excluded_symbol_count} | "
        f"filters={summary.active_filter_count}"
    )


def summarize_candidate_provenance(symbol: str, bundle: UniverseBuildBundle | None) -> str:
    if bundle is None:
        return "no universe provenance"
    membership = next((row for row in bundle.membership_records if row.symbol == symbol), None)
    if membership is None:
        return "symbol not in base universe"
    failure_summary = summarize_filter_failures(symbol, bundle)
    return f"{summarize_sub_universe_reason(membership)} | {failure_summary}"


def write_universe_provenance_artifacts(
    *,
    bundle: UniverseBuildBundle | None,
    output_dir: str | Path,
) -> dict[str, Path]:
    if bundle is None:
        return {}
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}

    def _write_json(name: str, payload: Any) -> None:
        if payload in (None, [], {}):
            return
        path = output_path / name
        path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        paths[name.replace(".", "_")] = path

    def _write_csv(name: str, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        path = output_path / name
        pd.DataFrame(rows).to_csv(path, index=False)
        paths[name.replace(".", "_")] = path

    _write_json("universe_membership.json", [row.to_dict() for row in bundle.membership_records])
    _write_csv("universe_membership.csv", [row.flat_dict() for row in bundle.membership_records])
    _write_json("universe_filter_results.json", [row.to_dict() for row in bundle.filter_results])
    _write_csv("universe_filter_results.csv", [row.flat_dict() for row in bundle.filter_results])
    _write_json("universe_build_summary.json", bundle.summary.to_dict() if bundle.summary is not None else None)
    _write_csv(
        "sub_universe_snapshot.csv",
        [
            {
                "symbol": row.symbol,
                "base_universe_id": row.base_universe_id,
                "sub_universe_id": row.sub_universe_id,
                "inclusion_status": row.inclusion_status,
                "inclusion_reason": row.inclusion_reason,
                "exclusion_reason": row.exclusion_reason,
                "group_label": row.group_label,
            }
            for row in bundle.membership_records
            if row.inclusion_status == "included"
        ],
    )
    return paths
