"""
Artifact readers for the trading platform FastAPI backend.

Reads from the same file-based artifacts as the Flask dashboard on port 8000.
All functions degrade gracefully — they never raise; they return
``{"available": False, "reason": "..."}`` when files are missing or corrupt.
"""
from __future__ import annotations

import json
import math
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.monitoring.provider_monitoring import (
    read_latest_monitoring_summary,
    read_latest_provider_health_summary,
    read_latest_registry_summary,
)
from trading_platform.monitoring.drilldown import (
    load_dataset_drilldown,
    load_dataset_timeline,
    load_provider_drilldown,
    load_provider_timeline,
)
from trading_platform.monitoring.history_summary import summarize_dataset_history, summarize_provider_history
from trading_platform.research.dataset_reader import (
    ResearchDatasetReadRequest,
    list_research_datasets,
    load_research_dataset,
    resolve_research_dataset,
)
from trading_platform.research.replay_evaluation import build_replay_evaluation_request, run_replay_evaluation
from trading_platform.research.replay_assembly import ReplayAssemblyRequest, assemble_replay_dataset
from trading_platform.research.replay_consumer import ReplayConsumerRequest, load_replay_consumer_input


# Roots — override via environment for testing or non-default layouts
ARTIFACTS_ROOT = Path(os.environ.get("ARTIFACTS_ROOT", "artifacts"))
DATA_ROOT = Path(os.environ.get("DATA_ROOT", "data"))


# ── Low-level helpers ────────────────────────────────────────────────────────


def _safe(value: Any) -> Any:
    """Coerce a value to a JSON-serializable type."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, str)):
        return value
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if isinstance(value, (pd.Timestamp, datetime)):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    if hasattr(value, "item"):  # numpy scalar
        return _safe(value.item())
    return value


def _safe_row(row: dict[str, Any]) -> dict[str, Any]:
    return {k: _safe(v) for k, v in row.items()}


def _read_csv(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    try:
        return pd.read_csv(path)
    except Exception:
        return None


def _read_jsonl(path: Path) -> list[dict[str, Any]] | None:
    if not path.exists():
        return None
    try:
        lines: list[dict[str, Any]] = []
        with path.open(encoding="utf-8") as fh:
            for line in fh:
                stripped = line.strip()
                if stripped:
                    lines.append(json.loads(stripped))
        return lines
    except Exception:
        return None


def _read_json(path: Path) -> Any:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _research_registry_path() -> Path:
    return DATA_ROOT / "research" / "dataset_registry.json"


def _provider_monitoring_root() -> Path:
    return ARTIFACTS_ROOT / "provider_monitoring"


def _compute_sharpe(returns: pd.Series) -> float | None:
    clean = pd.to_numeric(returns, errors="coerce").dropna()
    if len(clean) < 2:
        return None
    std = float(clean.std())
    if std == 0.0 or math.isnan(std):
        return None
    return float(clean.mean()) / std * math.sqrt(min(len(clean), 252))


def _compute_max_drawdown(equity: pd.Series) -> float | None:
    clean = pd.to_numeric(equity, errors="coerce").dropna()
    if clean.empty:
        return None
    running_max = clean.cummax()
    dd = (clean - running_max) / running_max.replace(0.0, float("nan"))
    val = dd.min()
    return float(val) if not math.isnan(val) else None


# ── System status ────────────────────────────────────────────────────────────


def read_system_status() -> dict[str, Any]:
    control_dir = ARTIFACTS_ROOT / "control"
    kill_active = (control_dir / "KILL_SWITCH").exists()
    trigger_pending = (control_dir / "TRIGGER_NOW").exists()

    if kill_active:
        loop_state = "stopped"
    elif trigger_pending:
        loop_state = "trigger_pending"
    else:
        loop_state = "running"

    last_run_timestamp: str | None = None
    next_scheduled_run: str | None = None

    decision_log_path = ARTIFACTS_ROOT / "decision_journal" / "decision_log.jsonl"
    entries = _read_jsonl(decision_log_path)
    if entries:
        last = entries[-1]
        last_run_timestamp = _safe(last.get("timestamp"))
        next_scheduled_run = _safe(last.get("next_run"))

    active_strategy_count = 0
    portfolio = _read_json(ARTIFACTS_ROOT / "strategy_portfolio" / "strategy_portfolio.json")
    if portfolio and isinstance(portfolio, dict) and "strategies" in portfolio:
        active_strategy_count = len(portfolio["strategies"])
    if active_strategy_count == 0:
        portfolio_csv = _read_csv(ARTIFACTS_ROOT / "strategy_portfolio" / "strategy_portfolio.csv")
        if portfolio_csv is not None:
            active_strategy_count = len(portfolio_csv)

    return {
        "available": True,
        "loop_state": loop_state,
        "last_run_timestamp": last_run_timestamp,
        "next_scheduled_run": next_scheduled_run,
        "active_strategy_count": active_strategy_count,
        "kill_switch_active": kill_active,
        "trigger_now_pending": trigger_pending,
    }


# ── P&L / equity ─────────────────────────────────────────────────────────────


def read_equity_curve() -> dict[str, Any]:
    path = ARTIFACTS_ROOT / "paper" / "paper_equity_curve.csv"
    df = _read_csv(path)
    if df is None or df.empty:
        return {"available": False, "reason": "No paper equity curve found", "data": []}

    records: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        records.append(_safe_row(dict(row)))

    return {"available": True, "data": records}


def read_pnl_summary() -> dict[str, Any]:
    equity_path = ARTIFACTS_ROOT / "paper" / "paper_equity_curve.csv"
    positions_path = ARTIFACTS_ROOT / "paper" / "paper_positions.csv"

    df = _read_csv(equity_path)
    if df is None or df.empty:
        return {"available": False, "reason": "No P&L data found"}

    result: dict[str, Any] = {"available": True}

    equity_col = next((c for c in ["equity", "portfolio_value", "value"] if c in df.columns), None)
    if equity_col:
        equity = pd.to_numeric(df[equity_col], errors="coerce").dropna()
        if not equity.empty:
            result["total_pnl"] = _safe(float(equity.iloc[-1] - equity.iloc[0]))
            result["current_equity"] = _safe(float(equity.iloc[-1]))
            result["max_drawdown"] = _safe(_compute_max_drawdown(equity))

    daily_col = next((c for c in ["daily_return", "daily_pnl", "return"] if c in df.columns), None)
    if daily_col:
        daily = pd.to_numeric(df[daily_col], errors="coerce").dropna()
        if not daily.empty:
            result["today_pnl"] = _safe(float(daily.iloc[-1]))
            result["sharpe"] = _safe(_compute_sharpe(daily))
    elif equity_col and "equity" in result:
        equity_s = pd.to_numeric(df[equity_col], errors="coerce").dropna()
        if len(equity_s) > 1:
            daily_returns = equity_s.pct_change().dropna()
            result["today_pnl"] = _safe(float(daily_returns.iloc[-1]))
            result["sharpe"] = _safe(_compute_sharpe(daily_returns))

    positions_df = _read_csv(positions_path)
    if positions_df is not None and not positions_df.empty:
        result["open_positions_count"] = int(len(positions_df))
        val_col = next((c for c in ["market_value", "value", "notional"] if c in positions_df.columns), None)
        if val_col:
            result["open_positions_value"] = _safe(
                float(pd.to_numeric(positions_df[val_col], errors="coerce").sum())
            )
    else:
        result["open_positions_count"] = 0
        result["open_positions_value"] = 0.0

    return result


# ── Signal performance ───────────────────────────────────────────────────────


def read_signals_performance() -> dict[str, Any]:
    # Check multiple result paths, pick the one with the most useful data
    candidates = [
        ("full_backtest_results", ARTIFACTS_ROOT / "kalshi_research" / "full_backtest_results.csv"),
        ("manifold_backtest", ARTIFACTS_ROOT / "kalshi_research" / "manifold_backtest_results.csv"),
        ("polymarket_backtest", ARTIFACTS_ROOT / "kalshi_research" / "polymarket_backtest_results.csv"),
        ("backtest_results", ARTIFACTS_ROOT / "kalshi_research" / "backtest" / "backtest_results.csv"),
        ("leaderboard", ARTIFACTS_ROOT / "kalshi_research" / "leaderboard.csv"),
    ]

    best_df = None
    best_source = ""
    best_trades = 0

    for source_name, path in candidates:
        df = _read_csv(path)
        if df is None or df.empty:
            continue
        if "n_trades" in df.columns:
            total = int(df["n_trades"].sum())
        else:
            total = len(df)
        if total > best_trades:
            best_df = df
            best_source = source_name
            best_trades = total

    if best_df is None or best_df.empty:
        return {"available": False, "reason": "No signal performance data found", "data": []}

    records = [_safe_row(dict(row)) for _, row in best_df.iterrows()]
    return {"available": True, "source": best_source, "data": records}


def read_signals_correlation() -> dict[str, Any]:
    # Try multiple possible feature directory locations
    candidates = [
        DATA_ROOT / "kalshi" / "features" / "real",
        DATA_ROOT / "kalshi" / "features",
        DATA_ROOT / "kalshi" / "synthetic",
    ]
    features_dir: Path | None = next((p for p in candidates if p.exists()), None)

    if features_dir is None:
        return {"available": False, "reason": "No Kalshi feature directory found", "matrix": [], "signals": []}

    signal_cols = [
        "calibration_drift_z",
        "volume_spike_z",
        "tension",
        "taker_imbalance",
        "large_order_direction",
        "base_rate_edge",
        "signal_value",
        "volume_z",
    ]

    frames: list[pd.DataFrame] = []
    for fpath in sorted(features_dir.glob("*.parquet"))[:100]:
        try:
            df = pd.read_parquet(fpath)
            available = [c for c in signal_cols if c in df.columns]
            if available:
                frames.append(df[available].tail(20))  # last 20 rows per market
        except Exception:
            continue

    if not frames:
        return {"available": False, "reason": "No signal features found in parquets", "matrix": [], "signals": []}

    combined = pd.concat(frames, ignore_index=True)
    numeric_cols = [
        c for c in combined.columns
        if combined[c].dtype.kind in ("f", "i") and combined[c].notna().sum() >= 5
    ]
    if len(numeric_cols) < 2:
        return {"available": False, "reason": "Insufficient numeric signals for correlation", "matrix": [], "signals": []}

    corr = combined[numeric_cols].corr()
    matrix = [[_safe(corr.loc[r, c]) for c in corr.columns] for r in corr.index]
    return {"available": True, "signals": list(corr.index), "matrix": matrix}


# ── Kalshi markets ────────────────────────────────────────────────────────────


def _features_dir() -> Path | None:
    candidates = [
        DATA_ROOT / "kalshi" / "features" / "real",
        DATA_ROOT / "kalshi" / "features",
        DATA_ROOT / "kalshi" / "synthetic",
    ]
    return next((p for p in candidates if p.exists()), None)


def read_kalshi_markets() -> dict[str, Any]:
    fdir = _features_dir()
    if fdir is None:
        return {"available": False, "reason": "No Kalshi feature data found", "data": []}

    markets: list[dict[str, Any]] = []
    for fpath in sorted(fdir.glob("*.parquet"))[:200]:
        ticker = fpath.stem
        try:
            df = pd.read_parquet(fpath)
            if df.empty:
                continue
            last = df.iloc[-1]
            market: dict[str, Any] = {
                "ticker": ticker,
                "title": _safe(last.get("title", ticker)),
                "yes_price": _safe(last.get("close")),
                "volume": _safe(last.get("volume")),
                "days_to_close": _safe(last.get("days_to_close")),
                "signals": {
                    "calibration_drift_z": _safe(last.get("calibration_drift_z")),
                    "volume_spike_z": _safe(last.get("volume_spike_z")),
                    "tension": _safe(last.get("tension")),
                    "taker_imbalance": _safe(last.get("taker_imbalance")),
                    "large_order_direction": _safe(last.get("large_order_direction")),
                    "base_rate_edge": _safe(last.get("base_rate_edge")),
                },
            }
            markets.append(market)
        except Exception:
            continue

    if not markets:
        return {"available": False, "reason": "No Kalshi market data found", "data": []}

    return {"available": True, "data": markets}


def read_kalshi_market_history(ticker: str) -> dict[str, Any]:
    fdir = _features_dir()
    if fdir is None:
        return {"available": False, "reason": "No Kalshi feature directory found", "data": []}

    fpath = fdir / f"{ticker}.parquet"
    if not fpath.exists():
        return {"available": False, "reason": f"No history for market {ticker}", "data": []}

    try:
        df = pd.read_parquet(fpath)
    except Exception as exc:
        return {"available": False, "reason": str(exc), "data": []}

    if df.empty:
        return {"available": False, "reason": f"Empty feature file for {ticker}", "data": []}

    ts_col = next((c for c in ["timestamp", "date", "time"] if c in df.columns), None)
    records: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        record: dict[str, Any] = {
            "timestamp": _safe(row.get(ts_col)) if ts_col else None,
            "yes_price": _safe(row.get("close")),
            "calibration_drift_z": _safe(row.get("calibration_drift_z")),
            "volume_spike_z": _safe(row.get("volume_spike_z")),
            "volume_z": _safe(row.get("volume_z")),
        }
        records.append(record)

    return {"available": True, "ticker": ticker, "data": records}


# ── Reasoning / trade decisions ───────────────────────────────────────────────


def read_reasoning_trades() -> dict[str, Any]:
    decisions_path = ARTIFACTS_ROOT / "decision_journal" / "trade_decisions.csv"
    candidates_path = ARTIFACTS_ROOT / "decision_journal" / "candidate_snapshot.csv"

    df_dec = _read_csv(decisions_path)
    df_cand = _read_csv(candidates_path)

    if (df_dec is None or df_dec.empty) and (df_cand is None or df_cand.empty):
        return {"available": False, "reason": "No trade decision data found", "data": []}

    records: list[dict[str, Any]] = []
    if df_dec is not None and not df_dec.empty:
        for _, row in df_dec.tail(50).iterrows():
            records.append(_safe_row(dict(row)))
    elif df_cand is not None and not df_cand.empty:
        for _, row in df_cand.tail(50).iterrows():
            records.append(_safe_row(dict(row)))

    return {"available": True, "data": records}


# ── Loop decisions ────────────────────────────────────────────────────────────


def read_loop_decisions() -> dict[str, Any]:
    path = ARTIFACTS_ROOT / "decision_journal" / "decision_log.jsonl"
    entries = _read_jsonl(path)
    if entries is None:
        return {"available": False, "reason": "No loop decision log found", "data": []}

    safe_entries = [_safe_row(e) for e in entries[-20:]]
    return {"available": True, "data": safe_entries}


def _parse_ws_timestamp(ts: str | None) -> str | None:
    """Convert a WebSocket timestamp to ISO-8601.

    Polymarket sends Unix millisecond strings (e.g. ``"1729084877448"``).
    """
    if not ts:
        return None
    try:
        val = int(ts)
        # Unix milliseconds → seconds
        if val > 1e12:
            val = val / 1000
        from datetime import datetime, timezone as tz
        return datetime.fromtimestamp(val, tz=tz.utc).isoformat()
    except (ValueError, TypeError, OSError):
        return ts  # already ISO or unknown format


def read_polymarket_live_markets() -> dict[str, Any]:
    db_path = DATA_ROOT / "polymarket" / "live" / "prices.db"
    if not db_path.exists():
        return {"available": False, "reason": "Live collector not running",
                "data": [], "count": 0, "markets_subscribed": 0, "started_at": None}
    try:
        import sqlite3
        conn = sqlite3.connect(str(db_path), check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        # Latest trade price per market (exclude orderbook price_change ticks)
        price_rows = conn.execute("""
            SELECT market_id, price, timestamp
            FROM ticks
            WHERE msg_type IN ('last_trade_price', 'book')
              AND id IN (
                SELECT MAX(id) FROM ticks
                WHERE msg_type IN ('last_trade_price', 'book')
                GROUP BY market_id
              )
        """).fetchall()
        # Market metadata (question text, end date)
        market_rows = conn.execute(
            "SELECT market_id, question, volume, end_date_iso FROM markets"
        ).fetchall()
        # Tick counts
        tick_count_rows = conn.execute(
            "SELECT market_id, COUNT(*) FROM ticks GROUP BY market_id"
        ).fetchall()
        conn.close()
    except Exception:
        return {"available": False, "reason": "Failed to read live DB",
                "data": [], "count": 0, "markets_subscribed": 0, "started_at": None}

    market_meta = {r[0]: {"question": r[1], "volume": r[2], "end_date_iso": r[3]} for r in market_rows}
    tick_counts = {r[0]: r[1] for r in tick_count_rows}

    markets = []
    for market_id, price, ts in price_rows:
        meta = market_meta.get(market_id, {})
        markets.append({
            "market_id": market_id,
            "question": meta.get("question", ""),
            "volume": meta.get("volume", 0),
            "end_date_iso": meta.get("end_date_iso"),
            "yes_price": round(price * 100, 2),
            "last_tick_at": _parse_ws_timestamp(ts),
            "tick_count": tick_counts.get(market_id, 0),
            "live": True,
        })

    # Read stats file for subscribed count and started_at
    stats_path = Path(os.environ.get("ARTIFACTS_ROOT", "artifacts")) / "polymarket_live" / "stats.json"
    stats: dict[str, Any] = {}
    if stats_path.exists():
        try:
            stats = json.loads(stats_path.read_text(encoding="utf-8"))
        except Exception:
            pass

    return {
        "available": True,
        "data": markets,
        "count": len(markets),
        "markets_subscribed": stats.get("markets_subscribed", len(market_meta)),
        "started_at": stats.get("started_at"),
    }


def read_polymarket_market_ticks(market_id: str) -> dict[str, Any]:
    db_path = DATA_ROOT / "polymarket" / "live" / "prices.db"
    if not db_path.exists():
        return {"available": False, "reason": "Live collector not running"}
    try:
        import sqlite3
        conn = sqlite3.connect(str(db_path), check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")

        # Market metadata
        meta_row = conn.execute(
            "SELECT question, volume, end_date_iso FROM markets WHERE market_id = ?",
            (market_id,),
        ).fetchone()

        # Ticks — last 500 by id descending, then reverse for ascending
        tick_rows = conn.execute(
            "SELECT price, timestamp FROM ticks WHERE market_id = ? ORDER BY id DESC LIMIT 500",
            (market_id,),
        ).fetchall()

        # Total tick count
        count_row = conn.execute(
            "SELECT COUNT(*) FROM ticks WHERE market_id = ?", (market_id,),
        ).fetchone()

        # First tick timestamp for ticks_per_hour calculation
        first_row = conn.execute(
            "SELECT timestamp FROM ticks WHERE market_id = ? ORDER BY id ASC LIMIT 1",
            (market_id,),
        ).fetchone()

        conn.close()
    except Exception as exc:
        return {"available": False, "reason": f"DB error: {exc}"}

    if not tick_rows:
        return {"available": False, "reason": "No ticks for this market"}

    # Reverse to ascending order
    tick_rows = list(reversed(tick_rows))
    prices = [r[0] for r in tick_rows]

    # Compute stats
    total_ticks = count_row[0] if count_row else len(tick_rows)
    first_ts = _parse_ws_timestamp(first_row[0]) if first_row else None
    hours_collected = 0.0
    if first_ts:
        try:
            from datetime import datetime as _dt, timezone as _tz
            first_dt = _dt.fromisoformat(first_ts)
            hours_collected = (_dt.now(tz=_tz.utc) - first_dt).total_seconds() / 3600.0
        except Exception:
            pass

    ticks_per_hour = round(total_ticks / max(hours_collected, 0.1), 1)

    ticks_out = [
        {"timestamp": _parse_ws_timestamp(r[1]), "price": round(r[0] * 100, 2)}
        for r in tick_rows
    ]

    return {
        "available": True,
        "market_id": market_id,
        "question": meta_row[0] if meta_row else "",
        "volume": meta_row[1] if meta_row else 0,
        "end_date_iso": meta_row[2] if meta_row else None,
        "ticks": ticks_out,
        "stats": {
            "min": round(min(prices) * 100, 2),
            "max": round(max(prices) * 100, 2),
            "first": round(prices[0] * 100, 2),
            "last": round(prices[-1] * 100, 2),
            "tick_count": total_ticks,
            "hours_collected": round(hours_collected, 1),
            "ticks_per_hour": ticks_per_hour,
        },
    }


# ── Paper trading ────────────────────────────────────────────────────────────


def read_paper_portfolio() -> dict[str, Any]:
    db_path = DATA_ROOT / "kalshi" / "paper_trades.db"
    if not db_path.exists():
        return {"available": False, "reason": "No paper trading DB found"}
    try:
        from trading_platform.kalshi.paper_executor import KalshiPaperExecutor
        executor = KalshiPaperExecutor(db_path)
        summary = executor.get_summary()
        executor.close()
        return {"available": True, **summary}
    except Exception as exc:
        return {"available": False, "reason": str(exc)}


def read_paper_trades() -> dict[str, Any]:
    db_path = DATA_ROOT / "kalshi" / "paper_trades.db"
    if not db_path.exists():
        return {"available": False, "reason": "No paper trading DB found", "data": []}
    try:
        from trading_platform.kalshi.paper_executor import KalshiPaperExecutor
        executor = KalshiPaperExecutor(db_path)
        trades = executor.get_recent_trades(limit=50)
        executor.close()
        return {"available": True, "data": trades, "count": len(trades)}
    except Exception as exc:
        return {"available": False, "reason": str(exc), "data": []}


def read_paper_scan() -> dict[str, Any]:
    scan_dir = ARTIFACTS_ROOT / "kalshi_paper"
    if not scan_dir.exists():
        return {"available": False, "reason": "No scan artifacts found", "data": []}
    scans = sorted(scan_dir.glob("scan_*.json"), reverse=True)
    if not scans:
        return {"available": False, "reason": "No scan files found", "data": []}
    try:
        data = json.loads(scans[0].read_text(encoding="utf-8"))
        return {"available": True, "scan_file": scans[0].name, **data}
    except Exception as exc:
        return {"available": False, "reason": str(exc), "data": []}


# Shared research registry and provider monitoring


def read_research_dataset_registry(
    *,
    provider: str | None = None,
    asset_class: str | None = None,
    dataset_name: str | None = None,
) -> dict[str, Any]:
    registry_path = _research_registry_path()
    if not registry_path.exists():
        return {"available": False, "reason": "No shared dataset registry found", "data": []}
    entries = list_research_datasets(
        registry_path=registry_path,
        provider=provider,
        asset_class=asset_class,
        dataset_name=dataset_name,
    )
    return {
        "available": True,
        "registry_path": str(registry_path),
        "count": len(entries),
        "data": [
            {
                "dataset_key": entry.dataset_key,
                "provider": entry.provider,
                "asset_class": entry.asset_class,
                "dataset_name": entry.dataset_name,
                "dataset_path": entry.dataset_path,
                "storage_type": entry.storage_type,
                "available_symbols": entry.available_symbols,
                "available_intervals": entry.available_intervals,
                "target_horizons": entry.target_horizons,
                "schema_version": entry.schema_version,
                "summary_path": entry.summary_path,
                "latest_materialized_at": entry.latest_materialized_at,
                "latest_event_time": entry.latest_event_time,
                "time_column": entry.time_column,
                "primary_keys": entry.primary_keys,
                "schema_columns": entry.schema_columns,
                "manifest_references": entry.manifest_references,
                "health_references": entry.health_references,
                "metadata": entry.metadata,
            }
            for entry in entries
        ],
    }


def read_research_dataset_detail(dataset_key: str) -> dict[str, Any]:
    registry_path = _research_registry_path()
    if not registry_path.exists():
        return {"available": False, "reason": "No shared dataset registry found"}
    try:
        entry = resolve_research_dataset(registry_path=registry_path, dataset_key=dataset_key)
    except KeyError as exc:
        return {"available": False, "reason": str(exc)}
    return {
        "available": True,
        "registry_path": str(registry_path),
        "data": {
            "dataset_key": entry.dataset_key,
            "provider": entry.provider,
            "asset_class": entry.asset_class,
            "dataset_name": entry.dataset_name,
            "dataset_path": entry.dataset_path,
            "storage_type": entry.storage_type,
            "available_symbols": entry.available_symbols,
            "available_intervals": entry.available_intervals,
            "target_horizons": entry.target_horizons,
            "schema_version": entry.schema_version,
            "summary_path": entry.summary_path,
            "latest_materialized_at": entry.latest_materialized_at,
            "latest_event_time": entry.latest_event_time,
            "time_column": entry.time_column,
            "primary_keys": entry.primary_keys,
            "schema_columns": entry.schema_columns,
            "manifest_references": entry.manifest_references,
            "health_references": entry.health_references,
            "metadata": entry.metadata,
        },
    }


def read_research_dataset_rows(
    *,
    dataset_key: str,
    symbols: list[str] | None = None,
    intervals: list[str] | None = None,
    start: str | None = None,
    end: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    registry_path = _research_registry_path()
    if not registry_path.exists():
        return {"available": False, "reason": "No shared dataset registry found", "data": []}
    try:
        result = load_research_dataset(
            ResearchDatasetReadRequest(
                registry_path=registry_path,
                dataset_key=dataset_key,
                symbols=list(symbols or []),
                intervals=list(intervals or []),
                start=start,
                end=end,
            )
        )
    except (KeyError, ValueError) as exc:
        return {"available": False, "reason": str(exc), "data": []}
    frame = result.frame.head(max(int(limit), 0))
    rows = [_safe_row(dict(row)) for _, row in frame.iterrows()]
    return {
        "available": True,
        "registry_path": str(registry_path),
        "descriptor": {
            "dataset_key": result.descriptor.dataset_key,
            "provider": result.descriptor.provider,
            "asset_class": result.descriptor.asset_class,
            "dataset_name": result.descriptor.dataset_name,
            "time_column": result.descriptor.time_column,
            "primary_keys": result.descriptor.primary_keys,
            "schema_columns": result.descriptor.schema_columns,
        },
        "filters_applied": result.filters_applied,
        "row_count": int(len(result.frame.index)),
        "returned_row_count": len(rows),
        "data": rows,
    }


def read_registry_publication_summary() -> dict[str, Any]:
    summary = read_latest_registry_summary(summary_path=_provider_monitoring_root() / "latest_registry_summary.json")
    if not summary:
        return {"available": False, "reason": "No registry publication summary found"}
    return {"available": True, **summary}


def read_provider_monitoring_summary() -> dict[str, Any]:
    summary = read_latest_monitoring_summary(output_root=_provider_monitoring_root())
    if not summary:
        return {"available": False, "reason": "No provider monitoring summary found"}
    return {"available": True, **summary}


def read_provider_health_summary() -> dict[str, Any]:
    summary = read_latest_provider_health_summary(output_root=_provider_monitoring_root())
    if not summary:
        return {"available": False, "reason": "No provider health summary found"}
    return {"available": True, **summary}


def read_provider_drilldown(provider: str) -> dict[str, Any]:
    registry_path = _research_registry_path()
    monitoring_root = _provider_monitoring_root()
    if not registry_path.exists():
        return {"available": False, "reason": "No shared dataset registry found"}
    result = load_provider_drilldown(
        registry_path=registry_path,
        monitoring_output_root=monitoring_root,
        provider=provider,
    )
    if not result.datasets and not result.monitoring_records and not result.health_summary:
        return {"available": False, "reason": f"No shared drill-down data found for provider '{provider}'"}
    return {"available": True, **result.to_dict()}


def read_dataset_drilldown(dataset_key: str) -> dict[str, Any]:
    registry_path = _research_registry_path()
    monitoring_root = _provider_monitoring_root()
    if not registry_path.exists():
        return {"available": False, "reason": "No shared dataset registry found"}
    try:
        result = load_dataset_drilldown(
            registry_path=registry_path,
            monitoring_output_root=monitoring_root,
            dataset_key=dataset_key,
        )
    except KeyError as exc:
        return {"available": False, "reason": str(exc)}
    return {"available": True, **result.to_dict()}


def read_replay_assembly_preview(
    *,
    dataset_keys: list[str] | None = None,
    providers: list[str] | None = None,
    dataset_names: list[str] | None = None,
    symbols: list[str] | None = None,
    intervals: list[str] | None = None,
    start: str | None = None,
    end: str | None = None,
    alignment_mode: str = "outer_union",
    anchor_dataset_key: str | None = None,
    tolerance: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    registry_path = _research_registry_path()
    if not registry_path.exists():
        return {"available": False, "reason": "No shared dataset registry found", "data": []}
    try:
        result = assemble_replay_dataset(
            ReplayAssemblyRequest(
                registry_path=registry_path,
                dataset_keys=list(dataset_keys or []),
                providers=list(providers or []),
                dataset_names=list(dataset_names or []),
                symbols=list(symbols or []),
                intervals=list(intervals or []),
                start=start,
                end=end,
                alignment_mode=alignment_mode,
                anchor_dataset_key=anchor_dataset_key,
                tolerance=tolerance,
            )
        )
    except (KeyError, ValueError) as exc:
        return {"available": False, "reason": str(exc), "data": []}

    preview = result.frame.head(max(int(limit), 0))
    rows = [_safe_row(dict(row)) for _, row in preview.iterrows()]
    return {
        "available": True,
        "row_count": int(len(result.frame.index)),
        "returned_row_count": len(rows),
        "summary": result.to_summary(),
        "data": rows,
    }


def read_replay_consumer_preview(
    *,
    dataset_keys: list[str] | None = None,
    providers: list[str] | None = None,
    dataset_names: list[str] | None = None,
    symbols: list[str] | None = None,
    intervals: list[str] | None = None,
    start: str | None = None,
    end: str | None = None,
    alignment_mode: str = "outer_union",
    anchor_dataset_key: str | None = None,
    tolerance: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    registry_path = _research_registry_path()
    if not registry_path.exists():
        return {"available": False, "reason": "No shared dataset registry found", "data": []}
    try:
        result = load_replay_consumer_input(
            ReplayConsumerRequest(
                assembly_request=ReplayAssemblyRequest(
                    registry_path=registry_path,
                    dataset_keys=list(dataset_keys or []),
                    providers=list(providers or []),
                    dataset_names=list(dataset_names or []),
                    symbols=list(symbols or []),
                    intervals=list(intervals or []),
                    start=start,
                    end=end,
                    alignment_mode=alignment_mode,
                    anchor_dataset_key=anchor_dataset_key,
                    tolerance=tolerance,
                ),
                limit=limit,
            )
        )
    except (KeyError, ValueError) as exc:
        return {"available": False, "reason": str(exc), "data": []}
    rows = [_safe_row(dict(row)) for _, row in result.frame.iterrows()]
    return {
        "available": True,
        "row_count": int(len(result.frame.index)),
        "returned_row_count": len(rows),
        "summary": result.to_summary(),
        "data": rows,
    }


def read_provider_timeline(provider: str) -> dict[str, Any]:
    result = load_provider_timeline(
        monitoring_output_root=_provider_monitoring_root(),
        provider=provider,
    )
    return {"available": True, **result.to_dict()}


def read_dataset_timeline(dataset_key: str) -> dict[str, Any]:
    result = load_dataset_timeline(
        monitoring_output_root=_provider_monitoring_root(),
        dataset_key=dataset_key,
    )
    return {"available": True, **result.to_dict()}


def read_provider_history_summary(provider: str) -> dict[str, Any]:
    result = summarize_provider_history(output_root=_provider_monitoring_root(), provider=provider)
    return {"available": True, **result.to_dict()}


def read_dataset_history_summary(dataset_key: str) -> dict[str, Any]:
    result = summarize_dataset_history(output_root=_provider_monitoring_root(), dataset_key=dataset_key)
    return {"available": True, **result.to_dict()}


def read_replay_evaluation_preview(
    *,
    dataset_keys: list[str] | None = None,
    providers: list[str] | None = None,
    dataset_names: list[str] | None = None,
    symbols: list[str] | None = None,
    intervals: list[str] | None = None,
    start: str | None = None,
    end: str | None = None,
    alignment_mode: str = "outer_union",
    anchor_dataset_key: str | None = None,
    tolerance: str | None = None,
    limit: int | None = None,
    feature_columns: list[str] | None = None,
    target_columns: list[str] | None = None,
) -> dict[str, Any]:
    registry_path = _research_registry_path()
    if not registry_path.exists():
        return {"available": False, "reason": "No shared dataset registry found"}
    try:
        result = run_replay_evaluation(
            build_replay_evaluation_request(
                registry_path=registry_path,
                dataset_keys=list(dataset_keys or []),
                providers=list(providers or []),
                dataset_names=list(dataset_names or []),
                symbols=list(symbols or []),
                intervals=list(intervals or []),
                start=start,
                end=end,
                alignment_mode=alignment_mode,
                anchor_dataset_key=anchor_dataset_key,
                tolerance=tolerance,
                limit=limit,
                feature_columns=list(feature_columns or []),
                target_columns=list(target_columns or []),
            )
        )
    except (KeyError, ValueError) as exc:
        return {"available": False, "reason": str(exc)}
    return {"available": True, **result.to_summary()}
