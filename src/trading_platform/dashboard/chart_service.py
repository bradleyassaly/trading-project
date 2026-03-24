from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import json

from trading_platform.data.canonical import load_research_symbol_frame


TIMESTAMP_COLUMNS = ("timestamp", "ts", "date", "Date", "Datetime", "as_of", "updated_at", "rebalance_timestamp")
DEFAULT_INDICATOR_COLUMNS = ("sma_20", "sma_50", "sma_100", "sma_200", "breakout_high", "breakout_low")
PROVENANCE_FILENAMES = (
    "decision_provenance.csv",
    "decision_provenance.json",
    "selection_decisions.csv",
    "selection_decisions.json",
    "portfolio_selection.csv",
    "portfolio_selection.json",
    "order_intents.csv",
    "order_intents.json",
)


def _safe_read_csv(path: str | Path | None) -> pd.DataFrame:
    if path is None:
        return pd.DataFrame()
    file_path = Path(path)
    if not file_path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(file_path)
    except (pd.errors.EmptyDataError, pd.errors.ParserError):
        return pd.DataFrame()


def _safe_read_json(path: str | Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    file_path = Path(path)
    if not file_path.exists():
        return {}
    try:
        return json.loads(file_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _safe_float(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: object) -> int | None:
    if value is None or pd.isna(value):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _isoformat(value: object) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    timestamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(timestamp):
        return None
    return timestamp.isoformat()


def _apply_lookback(frame: pd.DataFrame, lookback: int | None) -> pd.DataFrame:
    if lookback is None or lookback <= 0 or frame.empty:
        return frame
    return frame.tail(int(lookback)).reset_index(drop=True)


def _coerce_timestamp_series(frame: pd.DataFrame) -> tuple[pd.DataFrame, str | None]:
    working = frame.copy()
    for column in TIMESTAMP_COLUMNS:
        if column in working.columns:
            working[column] = pd.to_datetime(working[column], errors="coerce")
            working = working.dropna(subset=[column]).sort_values(column).reset_index(drop=True)
            return working, column
    if not working.empty and "Unnamed: 0" in working.columns:
        working["Unnamed: 0"] = pd.to_datetime(working["Unnamed: 0"], errors="coerce")
        working = working.dropna(subset=["Unnamed: 0"]).sort_values("Unnamed: 0").reset_index(drop=True)
        return working, "Unnamed: 0"
    return working, None


def _path_run_id(path: Path, root: Path) -> str | None:
    for part in reversed(path.relative_to(root).parts[:-1]):
        if "T" in part and ("+" in part or "-" in part):
            return part
    return path.parent.name if path.parent != root else None


def _path_source_name(path: Path, root: Path) -> str:
    relative_parts = path.relative_to(root).parts
    return relative_parts[0] if relative_parts else path.parent.name


def _run_metadata_for_path(path: Path, root: Path) -> dict[str, Any]:
    current = path.parent
    while True:
        metadata_path = current / "run_metadata.json"
        if metadata_path.exists():
            payload = _safe_read_json(metadata_path)
            if payload:
                payload["metadata_path"] = str(metadata_path)
                return payload
        if current == root or current.parent == current:
            break
        current = current.parent
    return {}


def artifact_context(path: Path, root: Path, kind: str) -> dict[str, Any]:
    metadata = _run_metadata_for_path(path, root)
    return {
        "kind": kind,
        "path": str(path),
        "source": metadata.get("source") or _path_source_name(path, root),
        "run_id": metadata.get("run_id") or _path_run_id(path, root),
        "name": path.name,
        "mode": metadata.get("mode"),
        "strategy_id": metadata.get("strategy_id"),
        "timeframe": metadata.get("timeframe"),
        "lookback": metadata.get("lookback"),
        "artifact_group": metadata.get("artifact_group"),
        "metadata_path": metadata.get("metadata_path"),
    }


def _candidate_metadata(path: Path, root: Path, kind: str) -> dict[str, Any]:
    return artifact_context(path, root, kind)


def _filter_candidate_paths(
    paths: list[Path],
    *,
    root: Path,
    run_id: str | None,
    source: str | None,
    mode: str | None,
) -> list[Path]:
    filtered = list(paths)
    if run_id:
        filtered = [path for path in filtered if _candidate_metadata(path, root, "candidate").get("run_id") == run_id]
    if source:
        filtered = [path for path in filtered if _candidate_metadata(path, root, "candidate").get("source") == source]
    if mode:
        lowered = mode.lower()
        filtered = [
            path for path in filtered
            if lowered in str(path).lower() or str(_candidate_metadata(path, root, "candidate").get("mode") or "").lower() == lowered
        ]
    return filtered


def _newest_path(paths: list[Path]) -> Path | None:
    existing = [path for path in paths if path.exists()]
    if not existing:
        return None
    return max(existing, key=lambda path: path.stat().st_mtime)


def _candidate_files(root: Path, names: list[str]) -> list[Path]:
    found: list[Path] = []
    for name in names:
        direct = root / name
        if direct.exists():
            found.append(direct)
        found.extend(root.rglob(name))
    seen: set[str] = set()
    unique: list[Path] = []
    for path in found:
        key = str(path.resolve())
        if key in seen:
            continue
        seen.add(key)
        unique.append(path)
    return unique


def _json_records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        for key in ("records", "rows", "items", "decisions", "order_intents"):
            value = payload.get(key)
            if isinstance(value, list):
                return [row for row in value if isinstance(row, dict)]
        return [payload]
    return []


def _discover_trade_ledger_candidates(root: Path, symbol: str) -> list[Path]:
    candidates = []
    patterns = [f"{symbol}_trades.csv", "*_trades.csv", "paper_trades.csv", "live_trades.csv", "trades.csv"]
    for pattern in patterns:
        candidates.extend(root.rglob(pattern))
    seen: set[str] = set()
    unique: list[Path] = []
    for path in candidates:
        key = str(path.resolve())
        if key in seen:
            continue
        seen.add(key)
        unique.append(path)
    return unique


def _normalize_constraint_hits(value: object) -> list[str]:
    if value in (None, "", []):
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        if text.startswith("[") and text.endswith("]"):
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                parsed = None
            if isinstance(parsed, list):
                return [str(item) for item in parsed if str(item)]
        return [item.strip() for item in text.split("|") if item.strip()] or [text]
    return [str(value)]


def _safe_bool(value: object) -> bool | None:
    if value in (None, "") or pd.isna(value):
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y", "included", "selected"}:
        return True
    if text in {"false", "0", "no", "n", "excluded", "rejected"}:
        return False
    return None


def _normalize_provenance_row(row: dict[str, Any], *, path: Path, root: Path) -> dict[str, Any]:
    context = artifact_context(path, root, "decision_provenance")
    included = _safe_bool(row.get("selection_included"))
    if included is None:
        included = _safe_bool(row.get("included"))
    if included is None:
        included = _safe_bool(row.get("selected"))
    selection_status = row.get("selection_status")
    if not selection_status:
        if included is True:
            selection_status = "included"
        elif included is False:
            selection_status = "excluded"
    return {
        "ts": _isoformat(
            row.get("timestamp")
            or row.get("ts")
            or row.get("decision_ts")
            or row.get("signal_ts")
            or row.get("order_ts")
            or row.get("created_at")
            or row.get("updated_at")
        ),
        "symbol": str(row.get("symbol") or "").upper() or None,
        "trade_id": str(row.get("trade_id") or "").strip() or None,
        "strategy_id": row.get("strategy_id") or context.get("strategy_id"),
        "run_id": row.get("run_id") or context.get("run_id"),
        "source": row.get("source") or context.get("source"),
        "mode": row.get("mode") or context.get("mode"),
        "signal_type": row.get("signal_type") or row.get("type"),
        "signal_value": _safe_float(row.get("signal_value") or row.get("signal_strength")),
        "ranking_score": _safe_float(row.get("ranking_score") or row.get("score") or row.get("rank_score")),
        "universe_rank": _safe_int(row.get("universe_rank") or row.get("rank")),
        "selection_included": included,
        "selection_status": str(selection_status) if selection_status not in (None, "") else None,
        "exclusion_reason": row.get("exclusion_reason") or row.get("reason"),
        "target_weight": _safe_float(row.get("target_weight") or row.get("weight")),
        "sizing_rationale": row.get("sizing_rationale") or row.get("sizing_reason"),
        "constraint_hits": _normalize_constraint_hits(row.get("constraint_hits") or row.get("constraints")),
        "order_intent_summary": row.get("order_intent_summary") or row.get("order_intent") or row.get("intent_summary"),
        "label": row.get("label"),
        "regime_context": row.get("regime_context") or row.get("regime"),
        "artifact_path": str(path),
        "metadata_path": context.get("metadata_path"),
    }


def _load_provenance_rows(path: Path, root: Path) -> list[dict[str, Any]]:
    if path.suffix.lower() == ".csv":
        frame = _safe_read_csv(path)
        if frame.empty:
            return []
        return [_normalize_provenance_row(row, path=path, root=root) for row in frame.to_dict(orient="records")]
    payload = _safe_read_json(path)
    return [_normalize_provenance_row(row, path=path, root=root) for row in _json_records(payload)]


def _row_matches_context(
    row: dict[str, Any],
    *,
    symbol: str,
    run_id: str | None,
    source: str | None,
    mode: str | None,
) -> bool:
    row_symbol = row.get("symbol")
    if row_symbol not in (None, "", symbol.upper()):
        return False
    if run_id and row.get("run_id") not in (None, "", run_id):
        return False
    if source and row.get("source") not in (None, "", source):
        return False
    if mode and str(row.get("mode") or "").lower() not in ("", mode.lower()):
        return False
    return True


def load_symbol_bars(
    *,
    symbol: str,
    feature_dir: str | Path,
    lookback: int | None = None,
) -> tuple[list[dict[str, Any]], pd.DataFrame, str | None]:
    try:
        frame = load_research_symbol_frame(feature_dir, symbol)
    except FileNotFoundError:
        return [], pd.DataFrame(), None

    frame = _apply_lookback(frame, lookback)
    bars: list[dict[str, Any]] = []
    for row in frame.to_dict(orient="records"):
        bars.append(
            {
                "ts": _isoformat(row.get("timestamp")),
                "open": _safe_float(row.get("open")) if "open" in frame.columns else None,
                "high": _safe_float(row.get("high")) if "high" in frame.columns else None,
                "low": _safe_float(row.get("low")) if "low" in frame.columns else None,
                "close": _safe_float(row.get("close")),
                "volume": _safe_float(row.get("volume")),
            }
        )
    return bars, frame, str(Path(feature_dir) / f"{symbol}.parquet")


def load_symbol_indicators(
    *,
    frame: pd.DataFrame,
    lookback: int | None = None,
) -> dict[str, list[dict[str, Any]]]:
    if frame.empty:
        return {}
    working = _apply_lookback(frame, lookback)
    indicators: dict[str, list[dict[str, Any]]] = {}
    for column in DEFAULT_INDICATOR_COLUMNS:
        if column not in working.columns:
            continue
        rows = []
        for row in working[["timestamp", column]].dropna().to_dict(orient="records"):
            rows.append({"ts": _isoformat(row.get("timestamp")), "value": _safe_float(row.get(column))})
        if rows:
            indicators[column] = rows
    return indicators


def _load_signal_frame(path: Path) -> pd.DataFrame:
    frame, timestamp_col = _coerce_timestamp_series(_safe_read_csv(path))
    if timestamp_col is None or "position" not in frame.columns:
        return pd.DataFrame()
    return frame.rename(columns={timestamp_col: "timestamp"})


def discover_signal_sources(
    *,
    artifacts_root: str | Path,
    symbol: str,
    run_id: str | None = None,
    source: str | None = None,
    mode: str | None = None,
) -> list[dict[str, Any]]:
    root = Path(artifacts_root)
    candidates = sorted(root.rglob(f"{symbol}_*_signals.csv"))
    filtered = _filter_candidate_paths(candidates, root=root, run_id=run_id, source=source, mode=mode)
    return [_candidate_metadata(path, root, "signal") for path in sorted(filtered, key=lambda item: item.stat().st_mtime, reverse=True)]


def load_symbol_signals(
    *,
    artifacts_root: str | Path,
    symbol: str,
    lookback: int | None = None,
    run_id: str | None = None,
    source: str | None = None,
    mode: str | None = None,
) -> tuple[list[dict[str, Any]], str | None, list[dict[str, Any]]]:
    root = Path(artifacts_root)
    candidates = sorted(root.rglob(f"{symbol}_*_signals.csv"))
    filtered = _filter_candidate_paths(candidates, root=root, run_id=run_id, source=source, mode=mode)
    chosen = _newest_path(filtered)
    available = [_candidate_metadata(path, root, "signal") for path in sorted(candidates, key=lambda item: item.stat().st_mtime, reverse=True)]
    if chosen is None:
        return [], None, available
    frame = _load_signal_frame(chosen)
    if frame.empty:
        return [], str(chosen), available
    frame = _apply_lookback(frame, lookback)
    position = pd.to_numeric(frame["position"], errors="coerce").fillna(0.0)
    previous = position.shift(1, fill_value=0.0)
    rows: list[dict[str, Any]] = []
    for index, current_value in position.items():
        prior_value = float(previous.loc[index])
        current_value = float(current_value)
        if current_value == prior_value:
            continue
        signal_type = "signal"
        label = "Signal"
        if prior_value <= 0.0 < current_value:
            signal_type = "entry_long_signal"
            label = "Long signal"
        elif prior_value > 0.0 >= current_value:
            signal_type = "exit_long_signal"
            label = "Exit long"
        elif prior_value >= 0.0 > current_value:
            signal_type = "entry_short_signal"
            label = "Short signal"
        elif prior_value < 0.0 <= current_value:
            signal_type = "exit_short_signal"
            label = "Exit short"
        rows.append(
            {
                "ts": _isoformat(frame.loc[index, "timestamp"]),
                "type": signal_type,
                "price": _safe_float(frame.loc[index, "close"] if "close" in frame.columns else None),
                "label": label,
                "score": _safe_float(frame.loc[index, "score"] if "score" in frame.columns else None),
            }
        )
    return rows, str(chosen), available


def _normalize_order_or_fill_rows(
    frame: pd.DataFrame,
    *,
    symbol: str,
    kind: str,
) -> list[dict[str, Any]]:
    if frame.empty or "symbol" not in frame.columns:
        return []
    working, timestamp_col = _coerce_timestamp_series(frame)
    working = working[working["symbol"].astype(str).str.upper() == symbol.upper()].reset_index(drop=True)
    if working.empty:
        return []
    rows: list[dict[str, Any]] = []
    for row in working.to_dict(orient="records"):
        price = (
            _safe_float(row.get("fill_price"))
            or _safe_float(row.get("reference_price"))
            or _safe_float(row.get("estimated_fill_price"))
            or _safe_float(row.get("price"))
        )
        quantity = (
            _safe_int(row.get("quantity"))
            or _safe_int(row.get("qty"))
            or _safe_int(row.get("adjusted_shares"))
            or _safe_int(row.get("requested_shares"))
        )
        rows.append(
            {
                "ts": _isoformat(row.get(timestamp_col)) if timestamp_col is not None else None,
                "symbol": symbol.upper(),
                "side": str(row.get("side") or "").lower() or None,
                "qty": quantity,
                "price": price,
                "order_id": str(row.get("client_order_id") or row.get("order_id") or row.get("run_key") or ""),
                "status": str(row.get("status") or ("filled" if kind == "fill" else "recorded")).lower(),
                "reason": row.get("reason"),
                "source_type": kind,
            }
        )
    return rows


def _load_candidate_records(
    *,
    root: Path,
    symbol: str,
    names: list[str],
    kind: str,
    run_id: str | None = None,
    source: str | None = None,
    mode: str | None = None,
) -> tuple[list[dict[str, Any]], str | None, list[dict[str, Any]]]:
    candidates = _candidate_files(root, names)
    filtered = _filter_candidate_paths(candidates, root=root, run_id=run_id, source=source, mode=mode)
    chosen = _newest_path(filtered)
    available = [_candidate_metadata(path, root, kind) for path in sorted(candidates, key=lambda item: item.stat().st_mtime, reverse=True)]
    if chosen is None:
        return [], None, available
    rows = _normalize_order_or_fill_rows(_safe_read_csv(chosen), symbol=symbol, kind=kind)
    return rows, str(chosen), available


def load_symbol_fills(
    *,
    artifacts_root: str | Path,
    symbol: str,
    run_id: str | None = None,
    source: str | None = None,
    mode: str | None = None,
) -> tuple[list[dict[str, Any]], str | None, list[dict[str, Any]]]:
    root = Path(artifacts_root)
    return _load_candidate_records(
        root=root,
        symbol=symbol,
        names=["paper_fills.csv"],
        kind="fill",
        run_id=run_id,
        source=source,
        mode=mode,
    )


def load_symbol_orders(
    *,
    artifacts_root: str | Path,
    symbol: str,
    run_id: str | None = None,
    source: str | None = None,
    mode: str | None = None,
) -> tuple[list[dict[str, Any]], str | None, list[dict[str, Any]]]:
    root = Path(artifacts_root)
    return _load_candidate_records(
        root=root,
        symbol=symbol,
        names=[
            "paper_orders_history.csv",
            "paper_orders.csv",
            "live_dry_run_proposed_orders.csv",
            "live_proposed_orders_history.csv",
            "executable_orders.csv",
            "requested_orders.csv",
        ],
        kind="order",
        run_id=run_id,
        source=source,
        mode=mode,
    )


def build_trade_records_from_fills(fills: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ordered = sorted(
        [row for row in fills if row.get("ts") and row.get("qty") and row.get("price") is not None],
        key=lambda row: str(row["ts"]),
    )
    trades: list[dict[str, Any]] = []
    current_qty = 0
    average_price = 0.0
    trade_index = 0
    current_trade: dict[str, Any] | None = None

    for fill in ordered:
        side = str(fill.get("side") or "").lower()
        qty = int(fill["qty"])
        price = float(fill["price"])
        signed_qty = qty if side == "buy" else -qty

        if current_qty == 0:
            trade_index += 1
            current_trade = {
                "trade_id": f"T{trade_index}",
                "side": "long" if signed_qty > 0 else "short",
                "qty": abs(signed_qty),
                "entry_ts": fill["ts"],
                "entry_price": price,
                "exit_ts": None,
                "exit_price": None,
                "realized_pnl": 0.0,
                "status": "open",
            }
            trades.append(current_trade)
            current_qty = signed_qty
            average_price = price
            continue

        same_direction = (current_qty > 0 and signed_qty > 0) or (current_qty < 0 and signed_qty < 0)
        if same_direction:
            new_qty = current_qty + signed_qty
            average_price = ((abs(current_qty) * average_price) + (abs(signed_qty) * price)) / abs(new_qty)
            current_qty = new_qty
            if current_trade is not None:
                current_trade["qty"] = abs(current_qty)
            continue

        closing_qty = min(abs(current_qty), abs(signed_qty))
        pnl = (price - average_price) * closing_qty if current_qty > 0 else (average_price - price) * closing_qty
        if current_trade is not None:
            current_trade["realized_pnl"] = float(current_trade.get("realized_pnl", 0.0)) + pnl
        current_qty += signed_qty

        if current_qty == 0:
            if current_trade is not None:
                current_trade["exit_ts"] = fill["ts"]
                current_trade["exit_price"] = price
                current_trade["qty"] = closing_qty
                current_trade["status"] = "closed"
            average_price = 0.0
            current_trade = None
        else:
            if current_trade is not None:
                current_trade["qty"] = abs(current_qty)
            if abs(signed_qty) > closing_qty:
                trade_index += 1
                opening_qty = abs(signed_qty) - closing_qty
                current_trade = {
                    "trade_id": f"T{trade_index}",
                    "side": "long" if current_qty > 0 else "short",
                    "qty": opening_qty,
                    "entry_ts": fill["ts"],
                    "entry_price": price,
                    "exit_ts": None,
                    "exit_price": None,
                    "realized_pnl": 0.0,
                    "status": "open",
                }
                trades.append(current_trade)
                average_price = price

    return trades


def _normalize_trade_rows(frame: pd.DataFrame, *, symbol: str) -> list[dict[str, Any]]:
    if frame.empty or "symbol" not in frame.columns:
        return []
    working = frame[frame["symbol"].astype(str).str.upper() == symbol.upper()].copy()
    if working.empty:
        return []
    rows: list[dict[str, Any]] = []
    for index, row in enumerate(working.to_dict(orient="records"), start=1):
        qty = _safe_int(row.get("qty")) or _safe_int(row.get("quantity")) or _safe_int(row.get("shares"))
        entry_ts = _isoformat(row.get("entry_ts") or row.get("entry_timestamp") or row.get("opened_at") or row.get("timestamp"))
        exit_ts = _isoformat(row.get("exit_ts") or row.get("exit_timestamp") or row.get("closed_at"))
        rows.append(
            {
                "trade_id": str(row.get("trade_id") or row.get("id") or f"T{index}"),
                "side": str(row.get("side") or row.get("direction") or "long").lower(),
                "qty": qty,
                "entry_ts": entry_ts,
                "entry_price": _safe_float(row.get("entry_price") or row.get("open_price") or row.get("price")),
                "exit_ts": exit_ts,
                "exit_price": _safe_float(row.get("exit_price") or row.get("close_price")),
                "realized_pnl": _safe_float(row.get("realized_pnl") or row.get("pnl") or row.get("profit_loss")) or 0.0,
                "status": str(row.get("status") or ("closed" if exit_ts else "open")).lower(),
            }
        )
    return rows


def load_symbol_trade_ledger(
    *,
    artifacts_root: str | Path,
    symbol: str,
    run_id: str | None = None,
    source: str | None = None,
    mode: str | None = None,
) -> tuple[list[dict[str, Any]], str | None, list[dict[str, Any]]]:
    root = Path(artifacts_root)
    candidates = _discover_trade_ledger_candidates(root, symbol)
    filtered = _filter_candidate_paths(candidates, root=root, run_id=run_id, source=source, mode=mode)
    chosen = _newest_path(filtered)
    available = [_candidate_metadata(path, root, "trade_ledger") for path in sorted(candidates, key=lambda item: item.stat().st_mtime, reverse=True)]
    if chosen is None:
        return [], None, available
    trades = _normalize_trade_rows(_safe_read_csv(chosen), symbol=symbol)
    return trades, str(chosen), available


def load_symbol_provenance(
    *,
    artifacts_root: str | Path,
    symbol: str,
    run_id: str | None = None,
    source: str | None = None,
    mode: str | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    root = Path(artifacts_root)
    candidates = _candidate_files(root, list(PROVENANCE_FILENAMES))
    filtered = _filter_candidate_paths(candidates, root=root, run_id=run_id, source=source, mode=mode)
    available = [_candidate_metadata(path, root, "decision_provenance") for path in sorted(candidates, key=lambda item: item.stat().st_mtime, reverse=True)]
    rows: list[dict[str, Any]] = []
    for path in sorted(filtered, key=lambda item: item.stat().st_mtime, reverse=True):
        for row in _load_provenance_rows(path, root):
            if _row_matches_context(row, symbol=symbol, run_id=run_id, source=source, mode=mode):
                rows.append(row)
    rows.sort(key=lambda row: str(row.get("ts") or ""), reverse=True)
    return rows, available


def build_position_summary(
    *,
    artifacts_root: str | Path,
    symbol: str,
    latest_price: float | None,
    run_id: str | None = None,
    source: str | None = None,
    mode: str | None = None,
) -> tuple[dict[str, Any], str | None]:
    root = Path(artifacts_root)
    candidates = _candidate_files(root, ["paper_positions.csv", "paper_positions_history.csv", "live_dry_run_current_positions.csv"])
    chosen = _newest_path(_filter_candidate_paths(candidates, root=root, run_id=run_id, source=source, mode=mode))
    if chosen is None:
        return {"qty": 0, "avg_price": None, "market_value": None, "unrealized_pnl": None, "updated_at": None}, None

    frame, timestamp_col = _coerce_timestamp_series(_safe_read_csv(chosen))
    if "symbol" not in frame.columns:
        return {"qty": 0, "avg_price": None, "market_value": None, "unrealized_pnl": None, "updated_at": None}, str(chosen)
    working = frame[frame["symbol"].astype(str).str.upper() == symbol.upper()].copy()
    if working.empty:
        return {"qty": 0, "avg_price": None, "market_value": None, "unrealized_pnl": None, "updated_at": None}, str(chosen)
    if timestamp_col is not None:
        working = working.sort_values(timestamp_col)
    latest = working.iloc[-1]
    qty = _safe_int(latest.get("quantity") if "quantity" in latest else latest.get("current_qty")) or 0
    avg_price = _safe_float(latest.get("avg_price"))
    market_value = _safe_float(latest.get("market_value"))
    if market_value is None and latest_price is not None:
        market_value = float(qty) * float(latest_price)
    unrealized_pnl = None
    if avg_price is not None and latest_price is not None:
        unrealized_pnl = (float(latest_price) - float(avg_price)) * float(qty)
    return {
        "qty": qty,
        "avg_price": avg_price,
        "market_value": market_value,
        "unrealized_pnl": unrealized_pnl,
        "updated_at": _isoformat(latest.get(timestamp_col)) if timestamp_col is not None else None,
    }, str(chosen)


def _merge_source_options(*groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen: set[tuple[str | None, str | None]] = set()
    for group in groups:
        for item in group:
            key = (str(item.get("source")), str(item.get("run_id")))
            if key in seen:
                continue
            seen.add(key)
            merged.append({"source": item.get("source"), "run_id": item.get("run_id")})
    return merged


def _bars_have_ohlc(bars: list[dict[str, Any]]) -> bool:
    return any(row.get("open") is not None and row.get("high") is not None and row.get("low") is not None and row.get("close") is not None for row in bars)


def build_chart_payload(
    *,
    artifacts_root: str | Path,
    feature_dir: str | Path,
    symbol: str,
    timeframe: str = "1d",
    lookback: int | None = 200,
    run_id: str | None = None,
    source: str | None = None,
    mode: str | None = None,
) -> dict[str, Any]:
    bars, frame, bar_source = load_symbol_bars(symbol=symbol, feature_dir=feature_dir, lookback=lookback)
    indicators = load_symbol_indicators(frame=frame, lookback=lookback)
    signals, signal_source, available_signal_sources = load_symbol_signals(
        artifacts_root=artifacts_root,
        symbol=symbol,
        lookback=lookback,
        run_id=run_id,
        source=source,
        mode=mode,
    )
    fills, fill_source, available_fill_sources = load_symbol_fills(
        artifacts_root=artifacts_root,
        symbol=symbol,
        run_id=run_id,
        source=source,
        mode=mode,
    )
    orders, order_source, available_order_sources = load_symbol_orders(
        artifacts_root=artifacts_root,
        symbol=symbol,
        run_id=run_id,
        source=source,
        mode=mode,
    )
    explicit_trades, explicit_trade_source, available_trade_sources = load_symbol_trade_ledger(
        artifacts_root=artifacts_root,
        symbol=symbol,
        run_id=run_id,
        source=source,
        mode=mode,
    )
    provenance_rows, available_provenance_sources = load_symbol_provenance(
        artifacts_root=artifacts_root,
        symbol=symbol,
        run_id=run_id,
        source=source,
        mode=mode,
    )
    trades = explicit_trades if explicit_trades else build_trade_records_from_fills(fills)
    trade_source_mode = "explicit_ledger" if explicit_trades else "reconstructed_from_fills"
    latest_price = bars[-1]["close"] if bars else None
    position, position_source = build_position_summary(
        artifacts_root=artifacts_root,
        symbol=symbol,
        latest_price=latest_price,
        run_id=run_id,
        source=source,
        mode=mode,
    )
    chart_style = "candlestick" if _bars_have_ohlc(bars) else "line"

    return {
        "symbol": symbol.upper(),
        "timeframe": timeframe,
        "bars": bars,
        "indicators": indicators,
        "signals": signals,
        "orders": orders,
        "fills": fills,
        "trades": trades,
        "position": position,
        "provenance": provenance_rows[:20],
        "meta": {
            "bar_source": bar_source,
            "signal_source": signal_source,
            "order_source": order_source,
            "fill_source": fill_source,
            "trade_source": explicit_trade_source or fill_source,
            "trade_source_mode": trade_source_mode,
            "position_source": position_source,
            "has_indicators": bool(indicators),
            "has_ohlc": _bars_have_ohlc(bars),
            "chart_style_default": chart_style,
            "selected_run_id": run_id,
            "selected_source": source,
            "selected_mode": mode,
            "bar_count": len(bars),
            "signal_count": len(signals),
            "fill_count": len(fills),
            "trade_count": len(trades),
            "available_signal_sources": available_signal_sources,
            "available_fill_sources": available_fill_sources,
            "available_order_sources": available_order_sources,
            "available_trade_sources": available_trade_sources,
            "available_provenance_sources": available_provenance_sources,
            "available_chart_sources": _merge_source_options(available_signal_sources, available_trade_sources, available_fill_sources),
        },
    }


def build_trades_payload(
    *,
    artifacts_root: str | Path,
    symbol: str,
    run_id: str | None = None,
    source: str | None = None,
    mode: str | None = None,
) -> dict[str, Any]:
    fills, fill_source, available_fill_sources = load_symbol_fills(
        artifacts_root=artifacts_root,
        symbol=symbol,
        run_id=run_id,
        source=source,
        mode=mode,
    )
    explicit_trades, explicit_trade_source, available_trade_sources = load_symbol_trade_ledger(
        artifacts_root=artifacts_root,
        symbol=symbol,
        run_id=run_id,
        source=source,
        mode=mode,
    )
    return {
        "symbol": symbol.upper(),
        "trades": explicit_trades if explicit_trades else build_trade_records_from_fills(fills),
        "fills": fills,
        "meta": {
            "trade_source": explicit_trade_source or fill_source,
            "trade_source_mode": "explicit_ledger" if explicit_trades else "reconstructed_from_fills",
            "fill_count": len(fills),
            "selected_run_id": run_id,
            "selected_source": source,
            "selected_mode": mode,
            "available_fill_sources": available_fill_sources,
            "available_trade_sources": available_trade_sources,
        },
    }


def build_signals_payload(
    *,
    artifacts_root: str | Path,
    symbol: str,
    lookback: int | None = 200,
    run_id: str | None = None,
    source: str | None = None,
    mode: str | None = None,
) -> dict[str, Any]:
    signals, signal_source, available_signal_sources = load_symbol_signals(
        artifacts_root=artifacts_root,
        symbol=symbol,
        lookback=lookback,
        run_id=run_id,
        source=source,
        mode=mode,
    )
    return {
        "symbol": symbol.upper(),
        "signals": signals,
        "meta": {
            "signal_source": signal_source,
            "signal_count": len(signals),
            "selected_run_id": run_id,
            "selected_source": source,
            "selected_mode": mode,
            "available_signal_sources": available_signal_sources,
        },
    }
