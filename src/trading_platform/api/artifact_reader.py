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
from trading_platform.research.replay_history import filter_replay_history, load_replay_history
from trading_platform.research.replay_comparison import ReplayComparisonRequest, run_replay_comparison
from trading_platform.research.replay_assembly import ReplayAssemblyRequest, assemble_replay_dataset
from trading_platform.research.replay_consumer import ReplayConsumerRequest, load_replay_consumer_input


# Roots — override via environment for testing or non-default layouts
ARTIFACTS_ROOT = Path(os.environ.get("ARTIFACTS_ROOT", "artifacts"))
DATA_ROOT = Path(os.environ.get("DATA_ROOT", "data"))


# ── SQLite connection helper ─────────────────────────────────────────────────


def _safe_connect(db_path: Path | str, **kwargs) -> "sqlite3.Connection":
    """Open a SQLite connection via the centralized db module.

    Handles WAL mode, busy_timeout, retry, and NTFS /tmp fallback.
    """
    from trading_platform.polymarket.db import connect_wallet_db
    check = kwargs.pop("check_same_thread", False)
    return connect_wallet_db(db_path, check_same_thread=check)


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


def _research_replay_root() -> Path:
    return ARTIFACTS_ROOT / "research_replay"


def _research_replay_history_root() -> Path:
    return _research_replay_root() / "history"


def _research_replay_gating_root() -> Path:
    return _research_replay_root() / "gating"


def _research_replay_review_root() -> Path:
    return _research_replay_root() / "review"


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
        conn = _safe_connect(db_path, check_same_thread=False)
        try:
            conn.execute("PRAGMA busy_timeout=60000")
            conn.execute("PRAGMA journal_mode=WAL")
        except Exception:
            pass
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
        # Market metadata (question text, end date, token ID)
        market_rows = conn.execute(
            "SELECT market_id, question, volume, end_date_iso, yes_token_id FROM markets"
        ).fetchall()
        # Tick counts
        tick_count_rows = conn.execute(
            "SELECT market_id, COUNT(*) FROM ticks GROUP BY market_id"
        ).fetchall()
        conn.close()
    except Exception:
        return {"available": False, "reason": "Failed to read live DB",
                "data": [], "count": 0, "markets_subscribed": 0, "started_at": None}

    market_meta = {r[0]: {"question": r[1], "volume": r[2], "end_date_iso": r[3],
                          "yes_token_id": r[4] if len(r) > 4 else None} for r in market_rows}
    tick_counts = {r[0]: r[1] for r in tick_count_rows}

    markets = []
    for market_id, price, ts in price_rows:
        meta = market_meta.get(market_id, {})
        markets.append({
            "market_id": market_id,
            "question": meta.get("question", ""),
            "volume": meta.get("volume", 0),
            "end_date_iso": meta.get("end_date_iso"),
            "yes_token_id": meta.get("yes_token_id"),
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
        conn = _safe_connect(db_path, check_same_thread=False)
        try:
            conn.execute("PRAGMA busy_timeout=60000")
            conn.execute("PRAGMA journal_mode=WAL")
        except Exception:
            pass

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


# ── Smart money ──────────────────────────────────────────────────────────────


def read_smart_money_wallets() -> dict[str, Any]:
    path = DATA_ROOT / "polymarket" / "wallet_profiles.parquet"
    if not path.exists():
        return {"available": False, "reason": "No wallet profiles found", "data": []}
    try:
        import pandas as _pd
        df = _pd.read_parquet(path)
        if "edge" not in df.columns:
            return {"available": False, "reason": "Profile schema missing edge", "data": []}
        top = df.nlargest(100, "edge")
        wallets = []
        for _, row in top.iterrows():
            wallets.append({
                "wallet": row.get("wallet", ""),
                "edge": _safe(row.get("edge")),
                "early_win_rate": _safe(row.get("early_win_rate")),
                "uncertain_early_trades": int(row.get("uncertain_early_trades", row.get("early_trades", 0))),
                "total_volume_usdc": _safe(row.get("total_volume_usdc")),
                "is_early_informed": bool(row.get("is_early_informed", False)),
                "win_rate": _safe(row.get("win_rate")),
                "resolved_trades": int(row.get("resolved_trades", 0)),
            })
        return {"available": True, "count": len(wallets), "data": wallets}
    except Exception as exc:
        return {"available": False, "reason": str(exc), "data": []}


def read_smart_money_signals() -> dict[str, Any]:
    profiles = DATA_ROOT / "polymarket" / "wallet_profiles.parquet"
    fills_dir = DATA_ROOT / "polymarket" / "orderflow"
    if not profiles.exists() or not fills_dir.exists():
        return {"available": False, "reason": "Missing profiles or fills", "data": []}
    try:
        from trading_platform.polymarket.smart_money_signal import SmartMoneySignalGenerator
        import pandas as _pd
        from datetime import datetime as _dt, timedelta, timezone as _tz

        gen = SmartMoneySignalGenerator(profiles)
        cutoff = _dt.now(tz=_tz.utc) - timedelta(hours=12)
        dfs = []
        for p in fills_dir.glob("*.parquet"):
            try:
                df = _pd.read_parquet(p)
                if "timestamp" in df.columns:
                    df = df[df["timestamp"] >= cutoff]
                if not df.empty:
                    dfs.append(df)
            except Exception:
                continue
        if not dfs:
            return {"available": True, "data": [], "count": 0}
        fills = _pd.concat(dfs, ignore_index=True)
        signals = gen.compute(fills)
        data = [
            {
                "token_id": s.token_id[:24],
                "direction": s.direction,
                "confidence": s.confidence,
                "weighted_net_volume": s.weighted_net_volume,
                "net_smart_volume": s.net_smart_volume,
                "top_wallet_edge": s.top_wallet_edge,
                "hours_since_last_trade": s.hours_since_last_smart_trade,
                "smart_trade_count": s.smart_trade_count,
            }
            for s in signals[:50]
        ]
        return {"available": True, "count": len(data), "data": data}
    except Exception as exc:
        return {"available": False, "reason": str(exc), "data": []}


def read_smart_money_mirror() -> dict[str, Any]:
    profiles = DATA_ROOT / "polymarket" / "wallet_profiles.parquet"
    if not profiles.exists():
        return {"available": False, "reason": "No profiles", "data": []}
    try:
        from trading_platform.polymarket.wallet_mirror import WalletMirror
        mirror = WalletMirror(profiles, top_n=30)
        signals = mirror.get_mirror_signals(since_minutes=90, min_fill_usdc=100)
        data = [
            {
                "wallet": s.trigger_wallet[:16],
                "token_id": s.token_id[:24],
                "question": s.question[:60] if s.question else "",
                "direction": s.direction,
                "fill_amount": s.fill_amount_usdc,
                "minutes_since_fill": s.minutes_since_fill,
                "current_price": round(s.current_price * 100, 1) if s.current_price else None,
                "spread": round(s.spread * 100, 1) if s.spread else None,
                "wallet_edge": s.wallet_edge,
                "tradeable": s.tradeable,
            }
            for s in signals[:30]
        ]
        return {"available": True, "count": len(data), "data": data}
    except Exception as exc:
        return {"available": False, "reason": str(exc), "data": []}


def read_backtest_wallet_positions(wallet_address: str) -> dict[str, Any]:
    """Return all wallet_market_positions for backtest visualization."""
    db = _get_wallet_db()
    if not db:
        return {"available": False, "positions": []}
    try:
        with db._lock:
            rows = db._conn.execute(
                """SELECT condition_id, question, category, side,
                          first_entry_ts, last_entry_ts, exit_ts,
                          avg_entry_price, avg_exit_price,
                          total_shares, total_cost_usdc, realized_pnl,
                          market_resolved, market_outcome, resolution_price,
                          end_date_ts, hours_before_close
                   FROM wallet_market_positions
                   WHERE wallet = ?
                   ORDER BY first_entry_ts DESC""",
                (wallet_address.lower(),),
            ).fetchall()
        cols = ["condition_id", "question", "category", "side",
                "first_entry_ts", "last_entry_ts", "exit_ts",
                "avg_entry_price", "avg_exit_price",
                "total_shares", "total_cost_usdc", "realized_pnl",
                "market_resolved", "market_outcome", "resolution_price",
                "end_date_ts", "hours_before_close"]
        positions = [dict(zip(cols, r)) for r in rows]
        return {
            "available": True,
            "wallet": wallet_address,
            "count": len(positions),
            "resolved": sum(1 for p in positions if p["market_resolved"]),
            "positions": positions,
        }
    except Exception as exc:
        return {"available": False, "reason": str(exc), "positions": []}


def read_backtest_price_history(condition_id: str) -> dict[str, Any]:
    """Return price history for a market. Auto-fetches if not cached."""
    db = _get_wallet_db()
    if not db:
        return {"available": False, "history": []}
    try:
        with db._lock:
            rows = db._conn.execute(
                "SELECT t, p FROM market_price_history WHERE condition_id = ? ORDER BY t ASC",
                (condition_id,),
            ).fetchall()

        # Auto-fetch if not cached
        if not rows:
            from trading_platform.polymarket.price_history_fetcher import MarketPriceHistoryFetcher
            MarketPriceHistoryFetcher().fetch_and_store(condition_id, str(db._path))
            with db._lock:
                rows = db._conn.execute(
                    "SELECT t, p FROM market_price_history WHERE condition_id = ? ORDER BY t ASC",
                    (condition_id,),
                ).fetchall()

        return {
            "available": True,
            "condition_id": condition_id,
            "count": len(rows),
            "history": [{"t": r[0], "p": r[1]} for r in rows],
        }
    except Exception as exc:
        return {"available": False, "reason": str(exc), "history": []}


def read_backtest_wallet_market(wallet_address: str, condition_id: str) -> dict[str, Any]:
    """Combined view: price history + wallet trades on a single market."""
    db = _get_wallet_db()
    if not db:
        return {"available": False}
    try:
        wallet_lower = wallet_address.lower()

        # Price history (auto-fetch)
        ph = read_backtest_price_history(condition_id)

        # Wallet trades on this market
        with db._lock:
            trade_rows = db._conn.execute(
                """SELECT timestamp, side, price, size FROM wallet_trades
                   WHERE wallet = ? AND condition_id = ?
                   ORDER BY timestamp ASC""",
                (wallet_lower, condition_id),
            ).fetchall()
            pos_row = db._conn.execute(
                """SELECT first_entry_ts, last_entry_ts, exit_ts,
                          avg_entry_price, avg_exit_price, total_shares,
                          total_cost_usdc, realized_pnl, resolution_price,
                          market_outcome, end_date_ts, question, category
                   FROM wallet_market_positions
                   WHERE wallet = ? AND condition_id = ?""",
                (wallet_lower, condition_id),
            ).fetchone()

        wallet_entries = [
            {"ts": r[0], "side": r[1], "price": r[2], "size": r[3]}
            for r in trade_rows
            if (r[1] or "").upper() == "BUY"
        ]
        wallet_exits = [
            {"ts": r[0], "side": r[1], "price": r[2], "size": r[3]}
            for r in trade_rows
            if (r[1] or "").upper() == "SELL"
        ]

        position_summary = None
        backtest_entry = None
        if pos_row:
            pcols = ["first_entry_ts", "last_entry_ts", "exit_ts",
                     "avg_entry_price", "avg_exit_price", "total_shares",
                     "total_cost_usdc", "realized_pnl", "resolution_price",
                     "market_outcome", "end_date_ts", "question", "category"]
            position_summary = dict(zip(pcols, pos_row))
            # Simulated backtest entry: 5 minutes after wallet's first entry
            if pos_row[0]:
                backtest_entry = {
                    "ts": pos_row[0] + 300,
                    "price": round((pos_row[3] or 0.5) * 1.02, 4),
                }

        return {
            "available": True,
            "wallet": wallet_address,
            "condition_id": condition_id,
            "price_history": ph.get("history", []),
            "wallet_entries": wallet_entries,
            "wallet_exits": wallet_exits,
            "position_summary": position_summary,
            "backtest_entry": backtest_entry,
        }
    except Exception as exc:
        return {"available": False, "reason": str(exc)}


def read_market_order_book(condition_id: str) -> dict[str, Any]:
    """Live CLOB order book + recent anomalies for a market.

    Resolves the YES token via Gamma, fetches the current order book via
    OrderBookMonitor.fetch_snapshot, and returns it alongside any anomalies
    persisted in market_anomalies for this market in the last 24 hours.
    """
    from trading_platform.polymarket.db_connection import get_connection
    import json as _json
    import time as _time
    try:
        import requests as _req
    except Exception:
        return {"available": False, "error": "requests not available"}

    db = _get_wallet_db()
    if not db:
        return {"available": False, "error": "wallet_db unavailable"}
    try:
        # Resolve token from Gamma (using the verified condition_ids param)
        token_id = None
        question = ""
        category = ""
        try:
            r = _req.get(
                "https://gamma-api.polymarket.com/markets",
                params={"condition_ids": condition_id},
                timeout=10,
            )
            data = r.json()
            m = data[0] if isinstance(data, list) and data else None
            if m and (m.get("conditionId") or "").lower() == condition_id.lower():
                tids_raw = m.get("clobTokenIds", "[]")
                tids = _json.loads(tids_raw) if isinstance(tids_raw, str) else tids_raw
                token_id = tids[0] if tids else None
                question = m.get("question") or ""
                category = m.get("category") or ""
        except Exception:
            pass

        if not token_id:
            return {
                "available": False,
                "error": "could not resolve token_id from gamma",
                "condition_id": condition_id,
            }

        from trading_platform.polymarket.order_book_monitor import OrderBookMonitor
        mon = OrderBookMonitor(db_path=str(db._path))
        snap = mon.fetch_snapshot(condition_id, token_id)
        if not snap:
            return {
                "available": False,
                "error": "could not fetch order book",
                "condition_id": condition_id,
                "token_id": token_id,
            }

        large_trades = mon.check_large_trades(condition_id, minutes_back=60)

        # Recent persisted anomalies for this market
        recent_anomalies: list[dict[str, Any]] = []
        try:
            conn = get_connection(str(db._path))
            cutoff = int(_time.time()) - 86400
            rows = conn.execute(
                """SELECT detected_at, anomaly_type, severity, detail,
                          imbalance, bid_usd, ask_usd
                   FROM market_anomalies
                   WHERE condition_id = ? AND detected_at >= ?
                   ORDER BY detected_at DESC LIMIT 50""",
                (condition_id, cutoff),
            ).fetchall()
            conn.close()
            recent_anomalies = [
                {
                    "detected_at": r[0],
                    "type": r[1],
                    "severity": r[2],
                    "detail": r[3],
                    "imbalance": r[4],
                    "bid_usd": r[5],
                    "ask_usd": r[6],
                }
                for r in rows
            ]
        except Exception:
            pass

        # Format levels: top 20 each with USD
        def _fmt(levels: list[dict]) -> list[dict]:
            out = []
            for lvl in levels[:20]:
                try:
                    p = float(lvl.get("price") or 0)
                    s = float(lvl.get("size") or 0)
                except (TypeError, ValueError):
                    continue
                out.append({"price": p, "size": s, "usd": round(p * s, 2)})
            return out

        return {
            "available": True,
            "condition_id": condition_id,
            "token_id": token_id,
            "question": question,
            "category": category,
            "timestamp": snap.timestamp,
            "mid_price": snap.mid_price,
            "best_bid": snap.best_bid,
            "best_ask": snap.best_ask,
            "spread": snap.spread,
            "bid_depth_usd": snap.bid_depth_usd,
            "ask_depth_usd": snap.ask_depth_usd,
            "imbalance_ratio": snap.imbalance_ratio,
            "top_bid_usd": snap.top_bid_usd,
            "top_ask_usd": snap.top_ask_usd,
            "bids": _fmt(snap.bids),
            "asks": _fmt(snap.asks),
            "large_trades_1h": large_trades,
            "recent_anomalies": recent_anomalies,
        }
    except Exception as exc:
        return {"available": False, "error": str(exc)}


def read_anomaly_alerts(limit: int = 50, severity: str | None = None) -> dict[str, Any]:
    """Recent anomalies from the market_anomalies table."""
    from trading_platform.polymarket.db_connection import get_connection
    db = _get_wallet_db()
    if not db:
        return {"available": False, "alerts": []}
    try:
        conn = get_connection(str(db._path))
        sql = """SELECT id, detected_at, condition_id, question, category,
                        anomaly_type, severity, detail, imbalance, bid_usd, ask_usd
                 FROM market_anomalies"""
        params: list[Any] = []
        if severity:
            sql += " WHERE severity = ?"
            params.append(severity)
        sql += " ORDER BY detected_at DESC LIMIT ?"
        params.append(int(limit))
        rows = conn.execute(sql, params).fetchall()
        conn.close()
        alerts = [
            {
                "id": r[0],
                "detected_at": r[1],
                "condition_id": r[2],
                "question": r[3],
                "category": r[4],
                "type": r[5],
                "severity": r[6],
                "detail": r[7],
                "imbalance": r[8],
                "bid_usd": r[9],
                "ask_usd": r[10],
            }
            for r in rows
        ]
        return {"available": True, "count": len(alerts), "alerts": alerts}
    except Exception as exc:
        return {"available": False, "error": str(exc), "alerts": []}


_BUCKET_SIZE_SECONDS = {
    "1h": 3600,
    "4h": 4 * 3600,
    "1d": 86400,
    "1w": 7 * 86400,
}

# Module-level cache for synthesized candles. Key = (cid, interval, from_ts, to_ts).
# 5-minute TTL — synthesizing OHLCV from 700+ price points is cheap but
# doing it on every chart timeframe change is wasteful.
_CANDLE_CACHE: dict[tuple, tuple[float, dict[str, Any]]] = {}
_CANDLE_TTL_SECONDS = 300


# Module-level cache for the top-markets browser. Key = (sort, category, limit).
# 3-minute TTL — Gamma data refresh roughly aligns with this.
_TOP_MARKETS_CACHE: dict[tuple, tuple[float, dict[str, Any]]] = {}
_TOP_MARKETS_TTL_SECONDS = 180


def read_top_markets(
    category: str | None = None,
    sort: str = "volume24h",
    limit: int = 20,
) -> dict[str, Any]:
    """Browse markets for the Market Monitor landing page.

    ``sort`` options:
      * ``volume24h`` — top by 24h trading volume (Gamma API)
      * ``trending``  — biggest active markets by liquidity (Gamma API proxy)
      * ``new``       — most recently created active markets (Gamma API)
      * ``whale``     — most whale activity (our DB)

    ``category`` filters Gamma's ``category`` field (case-insensitive).
    Pass ``None`` to disable. Note that Gamma frequently returns
    ``category=null`` so a strict filter can drop everything; we keep
    null-category markets when no category is specified and skip them
    when one is.
    """
    from trading_platform.polymarket.db_connection import get_connection
    import time as _time

    cache_key = (sort, (category or "").lower(), int(limit))
    cached = _TOP_MARKETS_CACHE.get(cache_key)
    if cached and (_time.time() - cached[0]) < _TOP_MARKETS_TTL_SECONDS:
        return cached[1]

    db = _get_wallet_db()
    db_path = str(db._path) if db else None

    results: list[dict[str, Any]] = []

    # ── Gamma-backed sorts ──────────────────────────────────────────────────
    if sort in ("volume24h", "trending", "new"):
        params: dict[str, Any] = {
            "active": "true",
            "closed": "false",
            "limit": min(int(limit) * 3, 100),  # over-fetch for category filter
        }
        if sort == "volume24h":
            params["order"] = "volume24hr"
            params["ascending"] = "false"
        elif sort == "trending":
            # Gamma has no native "trending" param; liquidity is a stable proxy
            params["order"] = "liquidity"
            params["ascending"] = "false"
        elif sort == "new":
            params["order"] = "startDate"
            params["ascending"] = "false"

        gamma_markets: list[dict] = []
        try:
            import requests as _req
            r = _req.get(
                "https://gamma-api.polymarket.com/markets",
                params=params,
                timeout=10,
            )
            data = r.json()
            gamma_markets = data if isinstance(data, list) else (data.get("data", []) if isinstance(data, dict) else [])
        except Exception as exc:
            logger = __import__("logging").getLogger(__name__)
            logger.debug("top markets gamma fetch failed: %s", exc)

        for m in gamma_markets:
            mcat_raw = m.get("category")
            mcat = (mcat_raw or "").lower()
            if category:
                # Strict filter: skip rows whose category doesn't match
                if mcat != category.lower():
                    continue

            try:
                vol = float(m.get("volume24hr") or m.get("volume") or 0)
            except (TypeError, ValueError):
                vol = 0.0
            try:
                liquidity = float(m.get("liquidity") or 0)
            except (TypeError, ValueError):
                liquidity = 0.0

            # Resolve last price: lastTradePrice → outcomePrices[0] → 0.5
            last_price: float | None = None
            ltp = m.get("lastTradePrice")
            if ltp is not None:
                try:
                    last_price = float(ltp)
                except (TypeError, ValueError):
                    last_price = None
            if last_price is None:
                op_raw = m.get("outcomePrices")
                if op_raw:
                    try:
                        op = json.loads(op_raw) if isinstance(op_raw, str) else op_raw
                        if op:
                            last_price = float(op[0])
                    except Exception:
                        last_price = None

            results.append({
                "condition_id": m.get("conditionId", "") or "",
                "question": m.get("question", "") or "",
                "category": mcat or "other",
                "volume_24h": round(vol, 2),
                "liquidity": round(liquidity, 2),
                "last_price": last_price,
                "price_change_24h": None,
                "whale_count": 0,
                "is_active": True,
                "end_date": m.get("endDate") or m.get("endDateIso"),
            })
            if len(results) >= int(limit):
                break

    # ── Whale-mode: read straight from our DB ───────────────────────────────
    if sort == "whale" and db_path:
        try:
            conn = get_connection(db_path)
            try:
                if category:
                    rows = conn.execute(
                        """SELECT mph.condition_id, mph.question, mph.category,
                                  COUNT(DISTINCT wmp.wallet) whale_count
                           FROM market_price_history mph
                           JOIN wallet_market_positions wmp ON mph.condition_id = wmp.condition_id
                           WHERE LOWER(mph.category) = ?
                           GROUP BY mph.condition_id
                           ORDER BY whale_count DESC LIMIT ?""",
                        (category.lower(), int(limit)),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        """SELECT mph.condition_id, mph.question, mph.category,
                                  COUNT(DISTINCT wmp.wallet) whale_count
                           FROM market_price_history mph
                           JOIN wallet_market_positions wmp ON mph.condition_id = wmp.condition_id
                           GROUP BY mph.condition_id
                           ORDER BY whale_count DESC LIMIT ?""",
                        (int(limit),),
                    ).fetchall()
            finally:
                conn.close()
            results = [{
                "condition_id": r[0],
                "question": r[1] or "",
                "category": (r[2] or "other").lower(),
                "whale_count": r[3] or 0,
                "volume_24h": 0.0,
                "liquidity": 0.0,
                "last_price": None,
                "price_change_24h": None,
                "is_active": True,
                "end_date": None,
            } for r in rows]
        except Exception as exc:
            logger = __import__("logging").getLogger(__name__)
            logger.debug("whale top markets failed: %s", exc)

    # ── Fallback: if Gamma fetch returned nothing AND we have DB, fall back
    # to whale-sorted markets so the GUI never shows an empty grid.
    if not results and db_path and sort != "whale":
        try:
            return read_top_markets(category=category, sort="whale", limit=limit)
        except Exception:
            pass

    # ── Enrich with whale_count from our DB ─────────────────────────────────
    if results and db_path and sort != "whale":
        try:
            conn = get_connection(db_path)
            try:
                cids = [r["condition_id"] for r in results if r["condition_id"]]
                if cids:
                    placeholders = ",".join("?" * len(cids))
                    rows = conn.execute(
                        f"""SELECT condition_id, COUNT(DISTINCT wallet) AS wc
                            FROM wallet_market_positions
                            WHERE condition_id IN ({placeholders})
                            GROUP BY condition_id""",
                        cids,
                    ).fetchall()
                    wc_map = {row[0]: row[1] for row in rows}
                    for r in results:
                        r["whale_count"] = wc_map.get(r["condition_id"], 0)
            finally:
                conn.close()
        except Exception:
            pass

    response = {
        "available": True,
        "sort": sort,
        "category": category,
        "count": len(results),
        "markets": results[: int(limit)],
    }
    _TOP_MARKETS_CACHE[cache_key] = (_time.time(), response)
    return response


def read_market_candles(
    condition_id: str,
    interval: str = "1d",
    from_ts: int | None = None,
    to_ts: int | None = None,
) -> dict[str, Any]:
    """Synthesize OHLCV candles for a market.

    Strategy: bucket ``market_price_history`` points into ``interval``
    sized buckets to compute O/H/L/C, then layer ``wallet_trades`` from
    the same window for buy/sell volume. Buckets with no price points
    use the previous close (flat candle, no body).
    """
    from trading_platform.polymarket.db_connection import get_connection
    import time as _time

    if interval not in _BUCKET_SIZE_SECONDS:
        return {"available": False, "error": f"invalid interval '{interval}'", "candles": []}
    bucket = _BUCKET_SIZE_SECONDS[interval]

    cache_key = (condition_id, interval, from_ts, to_ts)
    cached = _CANDLE_CACHE.get(cache_key)
    if cached and (_time.time() - cached[0]) < _CANDLE_TTL_SECONDS:
        return cached[1]

    db = _get_wallet_db()
    if not db:
        return {"available": False, "error": "wallet_db unavailable", "candles": []}

    try:
        conn = get_connection(str(db._path))
        try:
            # Price points
            ph_query = "SELECT t, p FROM market_price_history WHERE condition_id = ?"
            ph_params: list[Any] = [condition_id]
            if from_ts is not None:
                ph_query += " AND t >= ?"
                ph_params.append(from_ts)
            if to_ts is not None:
                ph_query += " AND t <= ?"
                ph_params.append(to_ts)
            ph_query += " ORDER BY t ASC"
            price_rows = conn.execute(ph_query, ph_params).fetchall()

            # Merge in live ticks (recorded by the live collector). These give
            # us denser intra-day data than the Gamma snapshot. Deduplicate
            # by timestamp so the merged stream is well-formed.
            try:
                tick_query = "SELECT timestamp, price FROM market_ticks WHERE condition_id = ?"
                tick_params: list[Any] = [condition_id]
                if from_ts is not None:
                    tick_query += " AND timestamp >= ?"
                    tick_params.append(from_ts)
                if to_ts is not None:
                    tick_query += " AND timestamp <= ?"
                    tick_params.append(to_ts)
                tick_query += " ORDER BY timestamp ASC"
                tick_rows = conn.execute(tick_query, tick_params).fetchall()
                if tick_rows:
                    seen_ts = {ts for ts, _ in price_rows}
                    merged: list[tuple] = list(price_rows)
                    for ts, p in tick_rows:
                        if ts not in seen_ts:
                            merged.append((ts, p))
                            seen_ts.add(ts)
                    merged.sort(key=lambda x: x[0])
                    price_rows = merged
            except Exception:
                pass

            # Question + category metadata
            meta = conn.execute(
                """SELECT question, category FROM market_price_history
                   WHERE condition_id = ? LIMIT 1""",
                (condition_id,),
            ).fetchone()
            question = (meta[0] if meta else "") or ""
            category = (meta[1] if meta else "") or ""

            # Wallet trades for volume — joined with leaderboard tier
            wt_query = (
                """SELECT timestamp, price, size, side
                   FROM wallet_trades
                   WHERE condition_id = ?"""
            )
            wt_params: list[Any] = [condition_id]
            if from_ts is not None:
                wt_query += " AND timestamp >= ?"
                wt_params.append(from_ts)
            if to_ts is not None:
                wt_query += " AND timestamp <= ?"
                wt_params.append(to_ts)
            wt_query += " ORDER BY timestamp ASC"
            trade_rows = conn.execute(wt_query, wt_params).fetchall()
        finally:
            conn.close()

        if not price_rows and not trade_rows:
            result = {
                "available": True,
                "condition_id": condition_id,
                "question": question,
                "category": category,
                "interval": interval,
                "candles": [],
                "total_candles": 0,
                "price_range": {"min": None, "max": None},
                "from_ts": from_ts,
                "to_ts": to_ts,
            }
            _CANDLE_CACHE[cache_key] = (_time.time(), result)
            return result

        # Determine the time span: union of price + trade timestamps
        all_ts = [r[0] for r in price_rows] + [r[0] for r in trade_rows]
        span_min = min(all_ts)
        span_max = max(all_ts)
        if from_ts is not None:
            span_min = max(span_min, from_ts)
        if to_ts is not None:
            span_max = min(span_max, to_ts)

        first_bucket = (span_min // bucket) * bucket
        last_bucket = (span_max // bucket) * bucket

        # Bin prices and trades
        prices_by_bucket: dict[int, list[float]] = {}
        for ts, p in price_rows:
            if p is None:
                continue
            b = (int(ts) // bucket) * bucket
            prices_by_bucket.setdefault(b, []).append(float(p))

        trades_by_bucket: dict[int, list[tuple]] = {}
        for ts, price, size, side in trade_rows:
            if price is None or size is None:
                continue
            b = (int(ts) // bucket) * bucket
            trades_by_bucket.setdefault(b, []).append((float(price), float(size), side or "BUY"))

        candles: list[dict[str, Any]] = []
        prev_close: float | None = None
        all_prices: list[float] = []

        b = first_bucket
        while b <= last_bucket:
            bucket_prices = prices_by_bucket.get(b, [])
            bucket_trades = trades_by_bucket.get(b, [])

            if bucket_prices:
                o = bucket_prices[0]
                c = bucket_prices[-1]
                h = max(bucket_prices)
                l_ = min(bucket_prices)
                prev_close = c
                all_prices.extend(bucket_prices)
            elif prev_close is not None:
                # Flat candle: no price movement in this bucket
                o = h = l_ = c = prev_close
            else:
                # No data yet — skip until we see the first price
                b += bucket
                continue

            v_buy = 0.0
            v_sell = 0.0
            for tp, ts_, side in bucket_trades:
                notional = tp * ts_
                if (side or "").upper() == "SELL":
                    v_sell += notional
                else:
                    v_buy += notional

            candles.append({
                "t": b,
                "o": round(o, 4),
                "h": round(h, 4),
                "l": round(l_, 4),
                "c": round(c, 4),
                "v_buy": round(v_buy, 2),
                "v_sell": round(v_sell, 2),
                "v_total": round(v_buy + v_sell, 2),
                "trade_count": len(bucket_trades),
            })
            b += bucket

        result = {
            "available": True,
            "condition_id": condition_id,
            "question": question,
            "category": category,
            "interval": interval,
            "candles": candles,
            "total_candles": len(candles),
            "price_range": {
                "min": round(min(all_prices), 4) if all_prices else None,
                "max": round(max(all_prices), 4) if all_prices else None,
            },
            "from_ts": from_ts,
            "to_ts": to_ts,
        }
        _CANDLE_CACHE[cache_key] = (_time.time(), result)
        return result
    except Exception as exc:
        return {"available": False, "error": str(exc), "candles": []}


def read_market_trade_flow(
    condition_id: str,
    from_ts: int | None = None,
    to_ts: int | None = None,
    limit: int = 500,
) -> dict[str, Any]:
    """Order-flow trades for a market.

    Tracked trades come from ``wallet_trades`` joined with ``leaderboard``
    so we get tier, pseudonym, directional_win_rate and conviction_score.
    Untracked trades come from ``data-api.polymarket.com/trades?market={cid}``
    and are deduplicated by ``transaction_hash``.
    """
    from trading_platform.polymarket.db_connection import get_connection
    db = _get_wallet_db()
    if not db:
        return {"available": False, "error": "wallet_db unavailable", "trades": []}

    try:
        conn = get_connection(str(db._path))
        try:
            wt_query = (
                """SELECT wt.timestamp, wt.price, wt.size, wt.side, wt.outcome,
                          wt.wallet, wt.transaction_hash,
                          l.tier, l.pseudonym,
                          l.directional_win_rate, l.conviction_score
                   FROM wallet_trades wt
                   LEFT JOIN leaderboard l ON wt.wallet = l.wallet
                   WHERE wt.condition_id = ?"""
            )
            params: list[Any] = [condition_id]
            if from_ts is not None:
                wt_query += " AND wt.timestamp >= ?"
                params.append(from_ts)
            if to_ts is not None:
                wt_query += " AND wt.timestamp <= ?"
                params.append(to_ts)
            wt_query += " ORDER BY wt.timestamp DESC LIMIT ?"
            params.append(int(limit))
            tracked_rows = conn.execute(wt_query, params).fetchall()

            meta = conn.execute(
                """SELECT question FROM market_price_history
                   WHERE condition_id = ? LIMIT 1""",
                (condition_id,),
            ).fetchone()
            question = (meta[0] if meta else "") or ""
            if not question:
                meta2 = conn.execute(
                    "SELECT title FROM wallet_trades WHERE condition_id = ? AND title != '' LIMIT 1",
                    (condition_id,),
                ).fetchone()
                question = (meta2[0] if meta2 else "") or ""
        finally:
            conn.close()

        seen_hashes: set[str] = set()
        trades: list[dict[str, Any]] = []
        for r in tracked_rows:
            tx = r[6] or ""
            if tx:
                seen_hashes.add(tx)
            price = float(r[1] or 0)
            size = float(r[2] or 0)
            trades.append({
                "ts": r[0],
                "price": round(price, 4),
                "size_usd": round(price * size, 2),
                "size": round(size, 2),
                "side": r[3] or "",
                "outcome": r[4] or "",
                "wallet": r[5] or "",
                "pseudonym": r[8] or "",
                "tier": r[7] or "unknown",
                "win_rate": float(r[9]) if r[9] is not None else 0.0,
                "conviction": float(r[10]) if r[10] is not None else 0.0,
                "is_tracked": True,
                "transaction_hash": tx,
            })
        tracked_count = len(trades)

        # Best-effort: fetch recent untracked from data-api. Cap at half the
        # requested limit (and at most 100) so a small ``limit`` query never
        # has its tracked rows swamped by a flood of recent untracked
        # trades during the final sort+slice.
        live_cap = max(5, min(int(limit) // 2, 100))
        try:
            import requests as _req
            r = _req.get(
                "https://data-api.polymarket.com/trades",
                params={"market": condition_id, "limit": live_cap},
                timeout=10,
            )
            if r.status_code == 200:
                data = r.json()
                live_trades = data if isinstance(data, list) else data.get("data", [])
                for t in live_trades:
                    tx = t.get("transactionHash") or ""
                    if tx and tx in seen_hashes:
                        continue
                    if tx:
                        seen_hashes.add(tx)
                    try:
                        price = float(t.get("price") or 0)
                        size = float(t.get("size") or 0)
                    except (TypeError, ValueError):
                        continue
                    trades.append({
                        "ts": t.get("timestamp") or 0,
                        "price": round(price, 4),
                        "size_usd": round(price * size, 2),
                        "size": round(size, 2),
                        "side": t.get("side") or "",
                        "outcome": t.get("outcome") or "",
                        "wallet": (t.get("proxyWallet") or "").lower(),
                        "pseudonym": t.get("pseudonym") or "",
                        "tier": "unknown",
                        "win_rate": 0.0,
                        "conviction": 0.0,
                        "is_tracked": False,
                        "transaction_hash": tx,
                    })
        except Exception:
            pass

        trades.sort(key=lambda x: x["ts"], reverse=True)
        trades = trades[: int(limit)]
        # Recount tracked after sort+slice so the metric matches the
        # returned list (otherwise live trades can push tracked ones out).
        final_tracked = sum(1 for t in trades if t["is_tracked"])

        return {
            "available": True,
            "condition_id": condition_id,
            "question": question,
            "trades": trades,
            "tracked_count": final_tracked,
            "total_count": len(trades),
        }
    except Exception as exc:
        return {"available": False, "error": str(exc), "trades": []}


def read_market_signals_and_trades(condition_id: str) -> dict[str, Any]:
    """Signals + paper trades fired for a market."""
    from trading_platform.polymarket.db_connection import get_connection
    db = _get_wallet_db()
    if not db:
        return {"available": False, "signals": [], "paper_trades": []}
    try:
        conn = get_connection(str(db._path))
        try:
            sig_cols_present = {r[1] for r in conn.execute("PRAGMA table_info(market_signals)").fetchall()}
            select_pt = "paper_trade_id" if "paper_trade_id" in sig_cols_present else "NULL"
            sig_rows = conn.execute(
                f"""SELECT fired_at, signal_type, direction, confidence, wallet,
                           {select_pt}
                    FROM market_signals
                    WHERE condition_id = ?
                    ORDER BY fired_at ASC""",
                (condition_id,),
            ).fetchall()

            pt_cols = {r[1] for r in conn.execute("PRAGMA table_info(polymarket_paper_trades)").fetchall()}
            if pt_cols:
                pt_rows = conn.execute(
                    """SELECT id, entry_ts, side, size_usd, entry_price,
                              exit_price, exit_ts, signal_type, confidence,
                              outcome, return_pct, realized_pnl
                       FROM polymarket_paper_trades
                       WHERE condition_id = ?
                       ORDER BY entry_ts ASC""",
                    (condition_id,),
                ).fetchall()
            else:
                pt_rows = []
        finally:
            conn.close()

        signals = [
            {
                "ts": r[0],
                "signal_type": r[1] or "",
                "direction": r[2] or "",
                "confidence": r[3],
                "wallet": r[4] or "",
                "paper_trade_id": r[5],
            }
            for r in sig_rows
        ]
        paper_trades = [
            {
                "id": r[0],
                "ts": r[1],
                "side": r[2] or "",
                "size_usd": r[3],
                "entry_price": r[4],
                "exit_price": r[5],
                "exit_ts": r[6],
                "signal_type": r[7] or "",
                "confidence": r[8],
                "outcome": r[9],
                "return_pct": r[10],
                "realized_pnl": r[11],
            }
            for r in pt_rows
        ]
        return {
            "available": True,
            "condition_id": condition_id,
            "signals": signals,
            "paper_trades": paper_trades,
            "signal_count": len(signals),
            "paper_trade_count": len(paper_trades),
        }
    except Exception as exc:
        return {"available": False, "error": str(exc), "signals": [], "paper_trades": []}


def read_market_intelligence(condition_id: str) -> dict[str, Any]:
    """Return full market intelligence: price history, all tier1/1h wallet
    activity, aggregated consensus, and convergence signal.
    """
    from trading_platform.polymarket.db_connection import get_connection
    db = _get_wallet_db()
    if not db:
        return {"available": False}
    try:
        conn = get_connection(str(db._path))
        try:
            meta = conn.execute(
                """SELECT question, category, token_id FROM market_price_history
                   WHERE condition_id = ? LIMIT 1""",
                (condition_id,),
            ).fetchone()
            if not meta:
                # Fall back to wallet_market_positions / wallet_trades for question/category
                meta = conn.execute(
                    """SELECT question, category, NULL FROM wallet_market_positions
                       WHERE condition_id = ? LIMIT 1""",
                    (condition_id,),
                ).fetchone()
            if not meta:
                meta = conn.execute(
                    """SELECT title, category, NULL FROM wallet_trades
                       WHERE condition_id = ? LIMIT 1""",
                    (condition_id,),
                ).fetchone()
            question = (meta[0] if meta else "") or ""
            category = (meta[1] if meta else "") or ""
            token_id = (meta[2] if meta and len(meta) > 2 else None)

            prices = conn.execute(
                """SELECT t, p FROM market_price_history
                   WHERE condition_id = ? ORDER BY t ASC""",
                (condition_id,),
            ).fetchall()
            price_history = [{"t": r[0], "p": r[1]} for r in prices]

            pos_rows = conn.execute(
                """SELECT wmp.wallet, wmp.side, wmp.avg_entry_price,
                          wmp.first_entry_ts, wmp.last_entry_ts, wmp.exit_ts,
                          wmp.total_cost_usdc, wmp.realized_pnl,
                          wmp.resolution_price, wmp.market_resolved,
                          wmp.market_outcome, wmp.end_date_ts,
                          l.pseudonym, l.tier,
                          wmp.net_shares, wmp.total_sold, wmp.sell_volume_usdc,
                          wmp.is_fully_exited, wmp.avg_exit_price,
                          wmp.total_shares
                   FROM wallet_market_positions wmp
                   JOIN leaderboard l ON wmp.wallet = l.wallet
                   WHERE wmp.condition_id = ?
                     AND l.tier IN ('tier1h', 'tier1')
                   ORDER BY wmp.first_entry_ts ASC""",
                (condition_id,),
            ).fetchall()
            pos_cols = ["wallet", "side", "avg_entry_price", "first_entry_ts",
                        "last_entry_ts", "exit_ts", "total_cost_usdc",
                        "realized_pnl", "resolution_price", "market_resolved",
                        "market_outcome", "end_date_ts", "pseudonym", "tier",
                        "net_shares", "total_sold", "sell_volume_usdc",
                        "is_fully_exited", "avg_exit_price", "total_shares"]

            # All fills for the tier1/1h wallets on this market — with outcome
            # so we can tag each fill BUY_YES / BUY_NO / SELL_YES / SELL_NO.
            wallets_in = sorted({r[0] for r in pos_rows})
            fills_by_wallet: dict[str, list[tuple]] = {w: [] for w in wallets_in}
            if wallets_in:
                ph_marks = ",".join("?" * len(wallets_in))
                fill_rows = conn.execute(
                    f"""SELECT wallet, side, price, size, timestamp, outcome
                        FROM wallet_trades
                        WHERE condition_id = ? AND wallet IN ({ph_marks})
                        ORDER BY timestamp ASC""",
                    (condition_id, *wallets_in),
                ).fetchall()
                for fr in fill_rows:
                    fills_by_wallet.setdefault(fr[0], []).append(fr)

            def _classify(side: str, outcome: str) -> str:
                s = (side or "").upper()
                o = (outcome or "").strip().lower()
                # Treat 'no'/'down'/'under' as the bearish (NO) side; everything else
                # (yes/up/over/team names) as the bullish (YES) side.
                is_no = o in ("no", "down", "under")
                token = "NO" if is_no else "YES"
                return f"{s}_{token}" if s in ("BUY", "SELL") else f"OTHER_{token}"

            wallet_activity = []
            market_resolved = 0
            resolution_price = None
            end_date = None
            # Group rows by wallet so we can roll multi-side positions
            # (e.g., a wallet trading both Lakers and Heat) into one entry.
            by_wallet: dict[str, list[dict]] = {}
            for row in pos_rows:
                p = dict(zip(pos_cols, row))
                if p["market_resolved"]:
                    market_resolved = 1
                if p["resolution_price"] is not None:
                    resolution_price = p["resolution_price"]
                if p["end_date_ts"]:
                    end_date = p["end_date_ts"]
                by_wallet.setdefault(p["wallet"], []).append(p)

            for wallet, positions in by_wallet.items():
                fills = fills_by_wallet.get(wallet, [])
                entry_fills = []
                exit_fills = []
                yes_buy_usdc = no_buy_usdc = 0.0
                yes_sell_usdc = no_sell_usdc = 0.0
                for f in fills:
                    side = (f[1] or "").upper()
                    price = f[2] or 0
                    size = f[3] or 0
                    ts = f[4]
                    outcome = f[5] or ""
                    cls = _classify(side, outcome)
                    notional = price * size
                    rec = {"ts": ts, "price": price, "size": size,
                           "notional_usdc": round(notional, 2),
                           "type": cls, "outcome": outcome}
                    if side == "BUY":
                        entry_fills.append(rec)
                        if cls.endswith("_NO"):
                            no_buy_usdc += notional
                        else:
                            yes_buy_usdc += notional
                    elif side == "SELL":
                        exit_fills.append(rec)
                        if cls.endswith("_NO"):
                            no_sell_usdc += notional
                        else:
                            yes_sell_usdc += notional

                # Hedge detection
                yes_exposure = max(0.0, yes_buy_usdc - yes_sell_usdc)
                no_exposure = max(0.0, no_buy_usdc - no_sell_usdc)
                total_exp = yes_exposure + no_exposure
                if yes_exposure > 0 and no_exposure > 0:
                    smaller = min(yes_exposure, no_exposure)
                    larger = max(yes_exposure, no_exposure)
                    hedge_ratio = round(smaller / larger, 3) if larger else 0.0
                    is_hedged = hedge_ratio >= 0.20
                    net_direction = "YES" if yes_exposure > no_exposure else "NO"
                else:
                    hedge_ratio = 0.0
                    is_hedged = False
                    net_direction = "YES" if yes_exposure > 0 else ("NO" if no_exposure > 0 else "NEUTRAL")

                # Pick the dominant position row for headline fields (largest cost basis)
                primary = max(positions, key=lambda x: x.get("total_cost_usdc") or 0)

                # Aggregate stats across this wallet's positions on this market
                total_cost = sum((x.get("total_cost_usdc") or 0) for x in positions)
                total_realized = sum((x.get("realized_pnl") or 0) for x in positions)
                total_sold = sum((x.get("total_sold") or 0) for x in positions)
                total_bought_shares = sum((x.get("total_shares") or 0) for x in positions)
                pct_sold = round(total_sold / total_bought_shares, 3) if total_bought_shares > 0 else 0.0
                fully_exited = pct_sold >= 0.99 or all((x.get("is_fully_exited") or 0) for x in positions)
                if fully_exited:
                    status = "Closed"
                elif pct_sold >= 0.20:
                    status = "Partially Closed"
                else:
                    status = "Open"

                wallet_activity.append({
                    "wallet": wallet,
                    "pseudonym": primary.get("pseudonym"),
                    "tier": primary.get("tier"),
                    "side": primary.get("side"),
                    "avg_entry_price": primary.get("avg_entry_price"),
                    "avg_exit_price": primary.get("avg_exit_price"),
                    "first_entry_ts": min((x.get("first_entry_ts") or 0) for x in positions) or None,
                    "last_entry_ts": max((x.get("last_entry_ts") or 0) for x in positions) or None,
                    "exit_ts": max((x.get("exit_ts") or 0) for x in positions) or None,
                    "total_cost_usdc": round(total_cost, 2),
                    "realized_pnl": round(total_realized, 2),
                    "resolution_price": primary.get("resolution_price"),
                    "market_resolved": primary.get("market_resolved"),
                    "market_outcome": primary.get("market_outcome"),
                    "end_date_ts": primary.get("end_date_ts"),
                    "entry_fills": entry_fills,
                    "exit_fills": exit_fills,
                    "yes_exposure_usdc": round(yes_exposure, 2),
                    "no_exposure_usdc": round(no_exposure, 2),
                    "net_position_usdc": round(total_exp, 2),
                    "hedge_ratio": hedge_ratio,
                    "is_hedged": is_hedged,
                    "net_direction": net_direction,
                    "pct_sold": pct_sold,
                    "is_closed": fully_exited,
                    "status": status,
                })

            # Count wallets by side label (works for both binary YES/NO and
            # multi-outcome markets where side is a team/option name).
            side_counts: dict[str, int] = {}
            for w in wallet_activity:
                s = (w["side"] or "YES").upper()
                side_counts[s] = side_counts.get(s, 0) + 1
            yes_wallets = [w for w in wallet_activity if (w["side"] or "").upper() == "YES"]
            no_wallets = [w for w in wallet_activity if (w["side"] or "").upper() == "NO"]
            total = len(wallet_activity)
            if not side_counts:
                consensus = "SPLIT"
                majority = []
            else:
                top_side, top_n = max(side_counts.items(), key=lambda x: x[1])
                # Tie => SPLIT
                ties = [s for s, n in side_counts.items() if n == top_n]
                if len(ties) > 1:
                    consensus = "SPLIT"
                    majority = [w for w in wallet_activity if (w["side"] or "").upper() in ties]
                else:
                    consensus = top_side
                    majority = [w for w in wallet_activity if (w["side"] or "").upper() == top_side]

            tier1h_majority = [w for w in majority if w["tier"] == "tier1h"]
            avg_majority_entry = (
                sum((w["avg_entry_price"] or 0) for w in majority) / len(majority)
                if majority else None
            )

            convergence_ts = None
            convergence_price = None
            convergence_outcome = None
            convergence_fired = len(tier1h_majority) >= 2 if tier1h_majority else len(majority) >= 2
            if convergence_fired:
                converging = tier1h_majority if len(tier1h_majority) >= 2 else majority
                sorted_maj = sorted(converging, key=lambda w: w["first_entry_ts"] or 0)
                convergence_ts = sorted_maj[1]["first_entry_ts"]
                if convergence_ts and price_history:
                    before = [pt for pt in price_history if pt["t"] <= convergence_ts]
                    if before:
                        convergence_price = before[-1]["p"]
                if resolution_price is not None and convergence_price is not None:
                    # If majority side is YES, we win when resolution >= convergence (price went up)
                    side_upper = (sorted_maj[0]["side"] or "YES").upper()
                    final = resolution_price if side_upper == "YES" else 1.0 - resolution_price
                    entry = convergence_price if side_upper == "YES" else 1.0 - convergence_price
                    convergence_outcome = "win" if final > entry else "loss"

            return {
                "available": True,
                "condition_id": condition_id,
                "question": question,
                "category": category,
                "token_id": token_id,
                "market_resolved": market_resolved,
                "resolution_price": resolution_price,
                "end_date": end_date,
                "price_history": price_history,
                "wallet_activity": wallet_activity,
                "aggregated": {
                    "yes_wallets": len(yes_wallets),
                    "no_wallets": len(no_wallets),
                    "consensus_side": consensus,
                    "consensus_strength": (len(majority) / total) if total else 0,
                    "first_entry_ts": min((w["first_entry_ts"] for w in wallet_activity if w["first_entry_ts"]), default=None),
                    "tier1h_count": len(tier1h_majority),
                    "avg_majority_entry": avg_majority_entry,
                    "total_volume_usdc": sum((w["total_cost_usdc"] or 0) for w in wallet_activity),
                },
                "convergence_signal": {
                    "fired": convergence_fired,
                    "wallet_count": len(majority),
                    "tier1h_count": len(tier1h_majority),
                    "first_convergence_ts": convergence_ts,
                    "convergence_price": convergence_price,
                    "outcome": convergence_outcome,
                },
            }
        finally:
            conn.close()
    except Exception as exc:
        return {"available": False, "reason": str(exc)}


def search_markets(
    q: str = "",
    category: str | None = None,
    has_whale_activity: bool = False,
    limit: int = 50,
) -> dict[str, Any]:
    """Search markets, ordered by tier1/1h whale count desc."""
    from trading_platform.polymarket.db_connection import get_connection
    db = _get_wallet_db()
    if not db:
        return {"available": False, "results": []}
    try:
        conn = get_connection(str(db._path))
        try:
            sql = """
                SELECT mph.condition_id,
                       MAX(mph.question) AS question,
                       MAX(mph.category) AS category,
                       COUNT(DISTINCT CASE WHEN l.tier IN ('tier1h','tier1')
                                           THEN wmp.wallet END) AS whale_count,
                       MAX(wmp.resolution_price) AS resolution_price,
                       MAX(wmp.market_resolved) AS resolved
                FROM market_price_history mph
                LEFT JOIN wallet_market_positions wmp ON mph.condition_id = wmp.condition_id
                LEFT JOIN leaderboard l ON wmp.wallet = l.wallet
            """
            where: list[str] = []
            params: list[Any] = []
            if q:
                where.append("LOWER(mph.question) LIKE ?")
                params.append(f"%{q.lower()}%")
            if category:
                where.append("LOWER(mph.category) = ?")
                params.append(category.lower())
            if where:
                sql += " WHERE " + " AND ".join(where)
            sql += " GROUP BY mph.condition_id"
            if has_whale_activity:
                sql += " HAVING whale_count > 0"
            sql += " ORDER BY whale_count DESC, mph.condition_id LIMIT ?"
            params.append(int(limit))

            rows = conn.execute(sql, params).fetchall()
            results = [
                {
                    "condition_id": r[0],
                    "question": r[1] or "",
                    "category": r[2] or "",
                    "whale_count": r[3] or 0,
                    "resolution_price": r[4],
                    "resolved": bool(r[5]),
                }
                for r in rows
            ]
            return {"available": True, "count": len(results), "results": results}
        finally:
            conn.close()
    except Exception as exc:
        return {"available": False, "reason": str(exc), "results": []}


def run_convergence_backtest(request: dict[str, Any]) -> dict[str, Any]:
    """Backtest the 'wait for N wallets to converge' strategy.

    For each market where >= min_wallets tier1/tier1h wallets are on the
    same side, simulate entering at convergence_ts + delay_seconds using
    the price from market_price_history at that timestamp, and exiting at
    the market's resolution_price.
    """
    import datetime as _dt
    from trading_platform.polymarket.db_connection import get_connection
    from collections import defaultdict as _dd

    db = _get_wallet_db()
    if not db:
        return {"available": False, "error": "wallet_db not available", "trades": []}

    try:
        start_date = request.get("start_date", "2025-01-01")
        end_date = request.get("end_date", "2026-04-07")
        min_wallets = max(2, int(request.get("min_wallets", 2)))
        delay_seconds = int(request.get("delay_seconds", 86400))
        slippage_pct = float(request.get("slippage_pct", 0.02))
        starting_bankroll = float(request.get("starting_bankroll", 100_000))
        stake_per_trade_pct = float(request.get("stake_per_trade_pct", 0.02))
        wallet_tier = request.get("wallet_tier", "tier1h")
        categories = request.get("categories")
        max_markets = int(request.get("max_markets", 200))

        start_ts = int(_dt.datetime.strptime(start_date, "%Y-%m-%d").timestamp())
        end_ts = int(_dt.datetime.strptime(end_date, "%Y-%m-%d").timestamp())

        if wallet_tier == "tier1h":
            tier_filter = "l.tier = 'tier1h'"
        elif wallet_tier == "tier1":
            tier_filter = "l.tier IN ('tier1h','tier1')"
        else:
            tier_filter = "l.tier IN ('tier1h','tier1','tier2')"

        cat_clause = ""
        cat_params: list[Any] = []
        if categories:
            cat_clause = f" AND wmp.category IN ({','.join('?'*len(categories))})"
            cat_params.extend(categories)

        conn = get_connection(str(db._path))
        try:
            # Group resolved positions by (condition_id, side); collect ordered entry timestamps
            rows = conn.execute(
                f"""SELECT wmp.condition_id, wmp.side, wmp.first_entry_ts,
                          wmp.avg_entry_price, wmp.resolution_price,
                          wmp.market_resolved, wmp.question, wmp.category,
                          wmp.realized_pnl, l.tier
                   FROM wallet_market_positions wmp
                   JOIN leaderboard l ON wmp.wallet = l.wallet
                   WHERE wmp.market_resolved = 1
                     AND wmp.resolution_price IS NOT NULL
                     AND wmp.first_entry_ts BETWEEN ? AND ?
                     AND {tier_filter}
                     {cat_clause}
                   ORDER BY wmp.first_entry_ts ASC""",
                (start_ts, end_ts, *cat_params),
            ).fetchall()

            grouped: dict[tuple[str, str], list[tuple]] = _dd(list)
            for r in rows:
                key = (r[0], (r[1] or "YES").upper())
                grouped[key].append(r)

            signals: list[dict[str, Any]] = []
            for (cid, side), entries in grouped.items():
                if len(entries) < min_wallets:
                    continue
                # Convergence ts = the moment the Nth wallet entered
                entries_sorted = sorted(entries, key=lambda x: x[2] or 0)
                convergence_entry = entries_sorted[min_wallets - 1]
                convergence_ts = convergence_entry[2]
                if not convergence_ts:
                    continue
                signals.append({
                    "cid": cid,
                    "side": side,
                    "convergence_ts": convergence_ts,
                    "wallet_count": len(entries),
                    "res_price": entries_sorted[0][4],
                    "question": entries_sorted[0][6] or "",
                    "category": entries_sorted[0][7] or "",
                    "wallet_realized_pnl": sum((e[8] or 0) for e in entries_sorted),
                })

            signals.sort(key=lambda s: s["convergence_ts"])
            signals = signals[:max_markets]

            # Simulate
            bankroll = starting_bankroll
            trades: list[dict[str, Any]] = []
            for sig in signals:
                entry_ts = sig["convergence_ts"] + delay_seconds
                # Look up market price at entry_ts
                row = conn.execute(
                    """SELECT p FROM market_price_history
                       WHERE condition_id = ? AND t <= ?
                       ORDER BY t DESC LIMIT 1""",
                    (sig["cid"], entry_ts),
                ).fetchone()
                market_price = row[0] if row else None
                if market_price is None or market_price <= 0:
                    continue

                # Clamp prices into [0.05, 0.95] — outside this range outcome
                # tokens are too thinly traded for realistic fills, and the
                # leverage math (shares = stake/price) explodes.
                if sig["side"] == "NO":
                    our_price = max(market_price * (1 - slippage_pct), 0.05)
                    our_exit = 1.0 - sig["res_price"]
                else:
                    our_price = min(market_price * (1 + slippage_pct), 0.95)
                    our_exit = sig["res_price"]
                our_price = max(min(our_price, 0.95), 0.05)

                # Constant sizing based on STARTING bankroll — convergence
                # signals fire rarely so compounding distorts results.
                stake = max(starting_bankroll * stake_per_trade_pct, 25)
                if stake <= 0:
                    continue

                pnl = (our_exit - our_price) * stake / our_price if our_price > 0 else 0
                bankroll += pnl
                trades.append({
                    "condition_id": sig["cid"],
                    "question": sig["question"][:100],
                    "category": sig["category"],
                    "side": sig["side"],
                    "wallet_count": sig["wallet_count"],
                    "convergence_ts": sig["convergence_ts"],
                    "entry_ts": entry_ts,
                    "our_entry_price": round(our_price, 4),
                    "our_exit_price": round(our_exit, 4),
                    "stake": round(stake, 2),
                    "pnl": round(pnl, 2),
                    "won": pnl > 0,
                })
        finally:
            conn.close()

        if not trades:
            return {
                "available": True,
                "total_trades": 0,
                "trades": [],
                "convergence_stats": {
                    "markets_with_convergence": len(grouped),
                    "signals_meeting_threshold": len(signals),
                    "trades_simulated": 0,
                },
                "config": {
                    "min_wallets": min_wallets,
                    "delay_seconds": delay_seconds,
                    "slippage_pct": slippage_pct,
                    "starting_bankroll": starting_bankroll,
                    "wallet_tier": wallet_tier,
                },
            }

        wins = sum(1 for t in trades if t["won"])
        total_pnl = sum(t["pnl"] for t in trades)

        # By wallet count breakdown
        by_wc: dict[int, dict[str, float]] = _dd(lambda: {"trades": 0, "wins": 0, "pnl": 0.0})
        for t in trades:
            wc = t["wallet_count"]
            by_wc[wc]["trades"] += 1
            by_wc[wc]["wins"] += 1 if t["won"] else 0
            by_wc[wc]["pnl"] += t["pnl"]
        for wc in by_wc:
            by_wc[wc]["pnl"] = round(by_wc[wc]["pnl"], 2)
            by_wc[wc]["win_rate"] = round(by_wc[wc]["wins"] / by_wc[wc]["trades"], 3) if by_wc[wc]["trades"] else 0

        by_month: dict[str, dict[str, float]] = _dd(lambda: {"pnl": 0.0, "trades": 0, "wins": 0})
        by_cat: dict[str, dict[str, float]] = _dd(lambda: {"pnl": 0.0, "trades": 0, "wins": 0})
        for t in trades:
            mo = _dt.datetime.fromtimestamp(t["entry_ts"]).strftime("%Y-%m")
            by_month[mo]["pnl"] += t["pnl"]
            by_month[mo]["trades"] += 1
            by_month[mo]["wins"] += 1 if t["won"] else 0
            cat = t["category"] or "other"
            by_cat[cat]["pnl"] += t["pnl"]
            by_cat[cat]["trades"] += 1
            by_cat[cat]["wins"] += 1 if t["won"] else 0

        avg_wallets = sum(t["wallet_count"] for t in trades) / len(trades)

        return {
            "available": True,
            "total_trades": len(trades),
            "winning_trades": wins,
            "win_rate": round(wins / len(trades), 3),
            "total_pnl": round(total_pnl, 2),
            "total_pnl_pct": round(total_pnl / starting_bankroll * 100, 2),
            "trades": trades,
            "by_month": {k: {kk: (round(vv, 2) if isinstance(vv, float) else vv) for kk, vv in v.items()}
                         for k, v in sorted(by_month.items())},
            "by_category": {k: {kk: (round(vv, 2) if isinstance(vv, float) else vv) for kk, vv in v.items()}
                            for k, v in by_cat.items()},
            "convergence_stats": {
                "markets_with_convergence": len(grouped),
                "signals_meeting_threshold": len(signals),
                "trades_simulated": len(trades),
                "avg_wallets_per_signal": round(avg_wallets, 2),
                "by_wallet_count": dict(by_wc),
            },
            "config": {
                "min_wallets": min_wallets,
                "delay_seconds": delay_seconds,
                "slippage_pct": slippage_pct,
                "starting_bankroll": starting_bankroll,
                "stake_per_trade_pct": stake_per_trade_pct,
                "wallet_tier": wallet_tier,
                "date_range": f"{start_date} to {end_date}",
            },
        }
    except Exception as exc:
        return {"available": False, "error": str(exc), "trades": []}


def read_wallet_positions(wallet_address: str) -> dict[str, Any]:
    """Return current open positions from wallet_positions table."""
    db = _get_wallet_db()
    if not db:
        return {"available": False, "wallet": wallet_address, "count": 0, "positions": []}
    try:
        with db._lock:
            rows = db._conn.execute(
                """SELECT condition_id, title, outcome, side, size, avg_price,
                          initial_value, current_value, cur_price, cash_pnl,
                          percent_pnl, realized_pnl, total_bought, redeemable,
                          end_date, updated_at
                   FROM wallet_positions
                   WHERE wallet = ?
                   ORDER BY ABS(COALESCE(cash_pnl, 0)) DESC""",
                (wallet_address,),
            ).fetchall()
        cols = ["condition_id", "title", "outcome", "side", "size", "avg_price",
                "initial_value", "current_value", "cur_price", "cash_pnl",
                "percent_pnl", "realized_pnl", "total_bought", "redeemable",
                "end_date", "updated_at"]
        positions = [dict(zip(cols, r)) for r in rows]
        return {
            "available": True,
            "wallet": wallet_address,
            "count": len(positions),
            "positions": positions,
            "total_unrealized": round(sum((p.get("cash_pnl") or 0) for p in positions), 2),
            "total_value": round(sum((p.get("current_value") or 0) for p in positions), 2),
        }
    except Exception as exc:
        return {"available": False, "reason": str(exc), "wallet": wallet_address, "count": 0, "positions": []}


def read_smart_money_wallet_detail(address: str) -> dict[str, Any]:
    """Wallet detail from wallet_intelligence.db + leaderboard."""
    db = _get_wallet_db()
    if not db:
        return {"available": False, "reason": "Wallet intelligence DB not found"}

    try:
        from trading_platform.polymarket.db_connection import get_connection
        conn = get_connection(str(db._path))

        # Profile + leaderboard join (RIGHT JOIN-style: include PM-only wallets)
        row = conn.execute("""
            SELECT
                COALESCE(l.tier, 'unranked') as tier,
                COALESCE(l.directional_win_rate, p.directional_win_rate) as directional_win_rate,
                l.rolling_20_wr,
                COALESCE(l.conviction_score, p.conviction_score) as conviction_score,
                COALESCE(l.resolved_trades, p.resolved_trades) as resolved_trades,
                COALESCE(l.total_volume_usdc, p.total_volume_usdc) as total_volume_usdc,
                COALESCE(l.wallet_type, p.wallet_type) as wallet_type,
                l.wallet_bucket,
                p.net_pnl_usdc, p.profit_factor,
                p.avg_win_size_usdc, p.avg_loss_size_usdc,
                p.large_bet_threshold, p.volume_tier, p.last_trade_ts,
                p.politics_win_rate, p.crypto_win_rate,
                p.category_trades, p.equity_score,
                l.primary_category, l.profit_factor as lb_pf,
                l.pm_pnl, l.pm_rank, l.leaderboard_source, l.pseudonym,
                p.pm_pnl_usdc, p.pm_volume_usdc, p.realized_pnl_total,
                p.unrealized_pnl_estimate, p.closed_trades_count,
                p.pnl_7d, p.pnl_30d, p.pnl_90d, p.pnl_trend, p.pnl_consistency,
                p.pnl_volatility, p.sharpe_ratio, p.sortino_ratio,
                p.max_drawdown_pct, p.max_drawdown_usd, p.recovery_factor,
                p.calmar_ratio, p.win_streak_max, p.loss_streak_max,
                p.expected_value, p.kelly_fraction,
                p.ev_politics, p.ev_sports, p.ev_crypto, p.ev_economics,
                p.edge_vs_market, p.avg_hours_before_resolution,
                p.early_entry_rate, p.category_specialization,
                p.open_positions_count, p.open_positions_value
            FROM wallet_profiles p
            LEFT JOIN leaderboard l ON p.wallet = l.wallet
            WHERE p.wallet = ?
        """, (address,)).fetchone()

        if not row:
            return {"available": False, "reason": f"Wallet {address[:16]}... not found"}

        cols = ["tier", "directional_win_rate", "rolling_20_wr", "conviction_score",
                "resolved_trades", "total_volume_usdc", "wallet_type", "wallet_bucket",
                "net_pnl_usdc", "profit_factor", "avg_win_size_usdc", "avg_loss_size_usdc",
                "large_bet_threshold", "volume_tier", "last_trade_ts",
                "politics_win_rate", "crypto_win_rate", "category_trades", "equity_score",
                "primary_category", "lb_profit_factor",
                "pm_pnl", "pm_rank", "leaderboard_source", "pseudonym",
                "pm_pnl_usdc", "pm_volume_usdc", "realized_pnl_total",
                "unrealized_pnl_estimate", "closed_trades_count",
                "pnl_7d", "pnl_30d", "pnl_90d", "pnl_trend", "pnl_consistency",
                "pnl_volatility", "sharpe_ratio", "sortino_ratio",
                "max_drawdown_pct", "max_drawdown_usd", "recovery_factor",
                "calmar_ratio", "win_streak_max", "loss_streak_max",
                "expected_value", "kelly_fraction",
                "ev_politics", "ev_sports", "ev_crypto", "ev_economics",
                "edge_vs_market", "avg_hours_before_resolution",
                "early_entry_rate", "category_specialization",
                "open_positions_count", "open_positions_value"]
        profile = dict(zip(cols, row))
        profile["wallet"] = address
        # Display PnL preference: PM authoritative > FIFO reconstructed > legacy
        # resolved-only. `pm_pnl_usdc` is the newest (from pm_leaderboard_sync),
        # falls back to the older `pm_pnl` on the leaderboard row, then to
        # our FIFO-reconstructed realized_pnl_total which captures pre-resolution
        # sells, and finally to the legacy net_pnl_usdc (resolved trades only).
        pm_authoritative = profile.get("pm_pnl_usdc") or profile.get("pm_pnl")
        reconstructed = profile.get("realized_pnl_total")
        if pm_authoritative is not None:
            profile["display_pnl"] = pm_authoritative
            profile["display_pnl_source"] = "pm_authoritative"
        elif reconstructed is not None:
            profile["display_pnl"] = reconstructed
            profile["display_pnl_source"] = "reconstructed_fifo"
        else:
            profile["display_pnl"] = profile.get("net_pnl_usdc")
            profile["display_pnl_source"] = "resolved_only"

        # Category breakdown from category_trades JSON
        cat_breakdown = {}
        try:
            ct = json.loads(profile.get("category_trades") or "{}") or {}
            total = sum(ct.values()) or 1
            cat_breakdown = {k: round(v / total, 3) for k, v in sorted(ct.items(), key=lambda x: -x[1])}
        except Exception:
            pass
        profile["category_breakdown"] = cat_breakdown

        # PnL history from wallet_trades
        pnl_rows = conn.execute("""
            SELECT timestamp, pnl FROM wallet_trades
            WHERE wallet = ? AND market_resolved = 1 AND pnl IS NOT NULL
            ORDER BY timestamp ASC
        """, (address,)).fetchall()

        cumulative = 0.0
        pnl_history = []
        for ts, pnl in pnl_rows:
            cumulative += (pnl or 0)
            pnl_history.append({"ts": ts, "pnl": round(pnl or 0, 2), "cumulative": round(cumulative, 2)})

        # Signals triggered by this wallet
        sig_rows = conn.execute("""
            SELECT signal_type, COUNT(*) as n FROM market_signals
            WHERE wallet = ? AND signal_type IS NOT NULL
            GROUP BY signal_type
        """, (address,)).fetchall()
        signals_triggered = {r[0]: r[1] for r in sig_rows} if sig_rows else {}

        # Recent trades
        trade_rows = conn.execute("""
            SELECT side, size, price, timestamp, category, market_resolved,
                   market_outcome, pnl, title, slug
            FROM wallet_trades
            WHERE wallet = ?
            ORDER BY timestamp DESC LIMIT 50
        """, (address,)).fetchall()
        trades = []
        for tr in trade_rows:
            side, size, price, ts, cat, resolved, outcome, pnl, title, slug = tr
            won = None
            if resolved and pnl is not None:
                won = pnl > 0
            trades.append({
                "side": side, "size": round(size or 0, 2), "price": round(price or 0, 4),
                "timestamp": ts, "category": cat,
                "resolved": bool(resolved), "outcome": outcome,
                "pnl": round(pnl, 2) if pnl is not None else None,
                "won": won,
                "question": title or slug or "",
            })

        # Open positions
        pos_rows = conn.execute("""
            SELECT asset, title, outcome, size, avg_price, current_value,
                   cash_pnl, percent_pnl, cur_price
            FROM wallet_positions
            WHERE wallet = ? AND size > 0
            ORDER BY current_value DESC
        """, (address,)).fetchall()
        open_positions = []
        for pr in pos_rows:
            open_positions.append({
                "asset": pr[0], "title": pr[1], "outcome": pr[2],
                "size": pr[3], "avg_price": pr[4], "current_value": pr[5],
                "cash_pnl": pr[6], "percent_pnl": pr[7], "cur_price": pr[8],
            })

        conn.close()

        return {
            "available": True,
            "wallet": address,
            **{k: v for k, v in profile.items() if k != "category_trades"},
            "pnl_history": pnl_history,
            "signals_triggered": signals_triggered,
            "recent_trades": trades,
            "open_positions": open_positions,
            "trade_count": len(trades),
        }
    except Exception as exc:
        return {"available": False, "reason": str(exc)}


def _get_wallet_db() -> Any:
    from trading_platform.polymarket.wallet_db import WalletDB
    db_path = DATA_ROOT / "polymarket" / "wallet_intelligence.db"
    if not db_path.exists():
        return None
    try:
        return WalletDB(db_path)
    except Exception:
        # On Windows Docker bind mounts, SQLite can't open the DB
        # directly due to NTFS-over-9P locking limitations. Fall back
        # to a temporary copy on the container's native filesystem.
        import shutil, tempfile
        tmp = Path(tempfile.gettempdir()) / "wallet_intelligence_ro.db"
        try:
            src_mtime = db_path.stat().st_mtime
            # Re-copy only if source is newer (avoid redundant 600MB copies)
            if not tmp.exists() or tmp.stat().st_mtime < src_mtime - 60:
                shutil.copy2(str(db_path), str(tmp))
            return WalletDB(tmp)
        except Exception:
            return None


def read_smart_money_actionable_signals() -> dict[str, Any]:
    db = _get_wallet_db()
    if not db:
        return {"available": False, "reason": "Wallet intelligence DB not found", "data": []}
    try:
        from trading_platform.polymarket.signal_engine import SignalEngine
        engine = SignalEngine(db=db)
        signals = engine.get_actionable_signals()
        return {"available": True, "data": signals, "count": len(signals)}
    except Exception as exc:
        return {"available": False, "reason": str(exc), "data": []}


def read_smart_money_leaderboard(*, sort_by: str = "equity_score") -> dict[str, Any]:
    db = _get_wallet_db()
    if not db:
        return {"available": False, "reason": "Wallet intelligence DB not found", "data": []}
    try:
        rows = db.get_leaderboard(limit=50, sort_by=sort_by)
        return {"available": True, "data": rows, "count": len(rows), "sort_by": sort_by}
    except Exception as exc:
        return {"available": False, "reason": str(exc), "data": []}


def read_smart_money_winners(*, window: str = "all") -> dict[str, Any]:
    db = _get_wallet_db()
    if not db:
        return {"available": False, "reason": "Wallet intelligence DB not found", "data": []}
    try:
        # Read directly from leaderboard table (includes polymarket-sourced tier1h)
        # Sort: tier1h first (by PM PnL DESC), then tier1 (by conviction DESC), then tier2
        with db._lock:
            lb_rows = db._conn.execute(
                """SELECT l.wallet, l.tier, l.leaderboard_source, l.pseudonym,
                          l.directional_win_rate, l.rolling_20_wr, l.conviction_score,
                          l.profit_factor, l.primary_category, l.wallet_bucket,
                          l.resolved_trades, l.total_volume_usdc, l.net_pnl_usdc,
                          l.rank, l.pm_pnl, l.pm_rank, l.wallet_type,
                          p.pm_pnl_usdc, p.realized_pnl_total
                   FROM leaderboard l
                   LEFT JOIN wallet_profiles p ON p.wallet = l.wallet
                   ORDER BY
                     CASE l.tier WHEN 'tier1h' THEN 1 WHEN 'tier1' THEN 2 WHEN 'tier2' THEN 3 ELSE 4 END,
                     COALESCE(p.pm_pnl_usdc, l.pm_pnl, p.realized_pnl_total, l.net_pnl_usdc, 0) DESC,
                     COALESCE(l.conviction_score, 0) DESC
                   LIMIT 100"""
            ).fetchall()

        cols = ["wallet", "tier", "leaderboard_source", "pseudonym",
                "directional_win_rate", "rolling_20_wr", "conviction_score",
                "profit_factor", "primary_category", "wallet_bucket",
                "resolved_trades", "total_volume_usdc", "net_pnl_usdc",
                "rank", "pm_pnl", "pm_rank", "wallet_type",
                "pm_pnl_usdc", "realized_pnl_total"]
        rows = []
        for r in lb_rows:
            row = dict(zip(cols, r))
            # Display PnL preference: PM authoritative > leaderboard pm_pnl >
            # FIFO reconstructed > legacy resolved-only.
            pm_auth = row.get("pm_pnl_usdc") or row.get("pm_pnl")
            rec = row.get("realized_pnl_total")
            if pm_auth is not None:
                row["display_pnl"] = pm_auth
                row["display_pnl_source"] = "pm_authoritative"
            elif rec is not None:
                row["display_pnl"] = rec
                row["display_pnl_source"] = "reconstructed_fifo"
            else:
                row["display_pnl"] = row.get("net_pnl_usdc")
                row["display_pnl_source"] = "resolved_only"
            # Compat fields for GUI
            row["profit"] = row["display_pnl"]
            row["volume"] = row.get("total_volume_usdc")
            row["trades"] = row.get("resolved_trades")
            row["win_rate"] = row.get("directional_win_rate")
            rows.append(row)

        # Fall back to wallet_profiles if leaderboard is empty
        if not rows:
            rows = db.get_winners(window=window, limit=50)

        # Enrich each row with tier, rolling_20_wr, conviction_score, profit_factor,
        # primary_category from the leaderboard table
        if rows:
            wallets = [r["wallet"] for r in rows]
            placeholders = ",".join("?" * len(wallets))
            with db._lock:
                lb_rows = db._conn.execute(
                    f"""SELECT wallet, tier, rolling_20_wr, conviction_score,
                               profit_factor, primary_category, wallet_bucket,
                               leaderboard_source, pseudonym, net_pnl_usdc
                        FROM leaderboard WHERE wallet IN ({placeholders})""",
                    wallets,
                ).fetchall()
                pf_rows = db._conn.execute(
                    f"""SELECT wallet, profit_factor, net_pnl_usdc, pseudonym
                        FROM wallet_profiles WHERE wallet IN ({placeholders})""",
                    wallets,
                ).fetchall()
            lb_map = {r[0]: {
                "tier": r[1], "rolling_20_wr": r[2], "conviction_score": r[3],
                "profit_factor": r[4], "primary_category": r[5], "wallet_bucket": r[6],
                "leaderboard_source": r[7], "pseudonym": r[8], "lb_net_pnl": r[9],
            } for r in lb_rows}
            pf_map = {r[0]: {"profit_factor": r[1], "net_pnl_usdc": r[2], "pseudonym": r[3]} for r in pf_rows}
            for r in rows:
                lb = lb_map.get(r["wallet"], {})
                pf = pf_map.get(r["wallet"], {})
                r["tier"] = lb.get("tier") or "unranked"
                r["rolling_20_wr"] = lb.get("rolling_20_wr")
                r["conviction_score"] = lb.get("conviction_score")
                r["profit_factor"] = lb.get("profit_factor") or pf.get("profit_factor")
                r["primary_category"] = lb.get("primary_category") or "unknown"
                r["wallet_bucket"] = lb.get("wallet_bucket") or r.get("wallet_type") or "unknown"
                r["net_pnl_usdc"] = lb.get("lb_net_pnl") or pf.get("net_pnl_usdc") or r.get("profit")
                r["leaderboard_source"] = lb.get("leaderboard_source") or "local"
                r["pseudonym"] = lb.get("pseudonym") or pf.get("pseudonym") or ""

        return {"available": True, "data": rows, "window": window, "count": len(rows)}
    except Exception as exc:
        return {"available": False, "reason": str(exc), "data": []}


def read_smart_money_wallet_positions(address: str) -> dict[str, Any]:
    db = _get_wallet_db()
    if not db:
        return {"available": False, "reason": "Wallet intelligence DB not found", "data": []}
    try:
        positions = db.get_wallet_positions(address)
        return {"available": True, "data": positions, "count": len(positions)}
    except Exception as exc:
        return {"available": False, "reason": str(exc), "data": []}


def read_smart_money_wallet_trades(address: str, *, page: int = 1, limit: int = 50) -> dict[str, Any]:
    db = _get_wallet_db()
    if not db:
        return {"available": False, "reason": "Wallet intelligence DB not found", "data": []}
    try:
        offset = (page - 1) * limit
        trades = db.get_wallet_trades(address, limit=limit, offset=offset)
        return {"available": True, "data": trades, "page": page, "count": len(trades)}
    except Exception as exc:
        return {"available": False, "reason": str(exc), "data": []}


_SIGNAL_ALLOCATIONS = {
    "wallet_reversal":    {"allocated": 18000, "stake_per_trade": 9000},
    "cascade":            {"allocated": 15000, "stake_per_trade": 7500},
    "oversized_bet":      {"allocated": 15000, "stake_per_trade": 7500},
    "accumulation":       {"allocated": 12000, "stake_per_trade": 6000},
    "market_maker_flip":  {"allocated": 12000, "stake_per_trade": 6000},
    "convergence":        {"allocated": 12000, "stake_per_trade": 6000},
    "specialist_entry":   {"allocated":  8000, "stake_per_trade": 4000},
    "pre_deadline_surge": {"allocated":  5000, "stake_per_trade": 2500},
    "whale_entry":        {"allocated":  3000, "stake_per_trade": 1500},
}


def read_smart_money_universe_stats() -> dict[str, Any]:
    db = _get_wallet_db()
    if not db:
        return {"available": False, "reason": "Wallet intelligence DB not found"}
    try:
        return {"available": True, **db.universe_stats()}
    except Exception as exc:
        return {"available": False, "reason": str(exc)}


def run_signal_backtest(
    *,
    hours: int = 168,
    signal_types: list[str] | None = None,
    min_confidence: float = 0.30,
    min_fusion: float = 0.05,
    min_entry_price: float = 0.10,
    max_entry_price: float = 0.85,
    stake_usd: float = 50.0,
    apply_spread: float = 0.015,
) -> dict[str, Any]:
    """Replay paper trades as a backtest with configurable gates.

    Reads resolved paper trades in the window, re-applies the supplied
    gates (min confidence, fusion floor, price band), simulates fills
    with a spread haircut, and reports aggregate + per-signal PnL.

    This is NOT a forward-looking backtest — it replays historical
    fills under alternate thresholds to answer "what would PnL look
    like with a looser fusion floor?" or "what if we only traded
    signals with confidence >= 0.5?".
    """
    db = _get_wallet_db()
    if not db:
        return {"available": False}
    from trading_platform.polymarket.db_connection import get_connection
    cutoff_sql = f"EXTRACT(EPOCH FROM (NOW() - INTERVAL '{int(hours)} hours'))::BIGINT"
    conn = get_connection(str(db._path))
    try:
        where_parts = [
            f"archived = 0", f"exit_ts IS NOT NULL",
            f"entry_ts >= {cutoff_sql}",
            f"entry_price >= {float(min_entry_price)}",
            f"entry_price <= {float(max_entry_price)}",
            f"confidence >= {float(min_confidence)}",
            f"fusion_score >= {float(min_fusion)}",
        ]
        if signal_types:
            types_list = ",".join(f"'{s}'" for s in signal_types if s)
            if types_list:
                where_parts.append(f"signal_type IN ({types_list})")
        sql = f"""
            SELECT signal_type, side, entry_price, exit_price, size_usd,
                   outcome, realized_pnl, entry_ts
            FROM polymarket_paper_trades
            WHERE {' AND '.join(where_parts)}
            ORDER BY entry_ts
        """
        rows = conn.execute(sql).fetchall()
    finally:
        try: conn.close()
        except Exception: pass

    by_signal: dict[str, dict] = {}
    total_pnl = 0.0
    total_wins = 0
    total_losses = 0
    for sig_type, side, ep, xp, sz, outcome, pnl, ts in rows:
        ep_f = float(ep or 0)
        xp_f = float(xp or 0)
        if ep_f <= 0 or xp_f < 0: continue
        # Re-simulate at stake_usd with a spread haircut on entry
        eff_entry = ep_f + apply_spread if side == "YES" else ep_f - apply_spread
        eff_entry = max(0.01, min(0.99, eff_entry))
        # PnL for BUY YES: shares = stake/eff_entry; payout = shares × xp
        if side in ("YES", "BUY"):
            shares = stake_usd / eff_entry
            sim_pnl = shares * (xp_f - eff_entry)
        else:
            shares = stake_usd / max(1 - eff_entry, 0.01)
            sim_pnl = shares * ((1 - xp_f) - (1 - eff_entry))
        entry = by_signal.setdefault(sig_type, {
            "signal_type": sig_type, "trades": 0, "wins": 0, "losses": 0,
            "net_pnl": 0.0, "volume": 0.0,
        })
        entry["trades"] += 1
        entry["volume"] += stake_usd
        if sim_pnl > 0:
            entry["wins"] += 1
            total_wins += 1
        else:
            entry["losses"] += 1
            total_losses += 1
        entry["net_pnl"] += sim_pnl
        total_pnl += sim_pnl
    by_signal_list = sorted(
        [
            {
                **v,
                "net_pnl": round(v["net_pnl"], 2),
                "avg_pnl": round(v["net_pnl"] / max(v["trades"], 1), 2),
                "win_rate": round(v["wins"] / max(v["trades"], 1), 3),
            }
            for v in by_signal.values()
        ],
        key=lambda r: r["net_pnl"], reverse=True,
    )
    return {
        "available": True,
        "config": {
            "hours": hours, "signal_types": signal_types,
            "min_confidence": min_confidence, "min_fusion": min_fusion,
            "min_entry_price": min_entry_price, "max_entry_price": max_entry_price,
            "stake_usd": stake_usd, "apply_spread": apply_spread,
        },
        "totals": {
            "trades": len(rows),
            "wins": total_wins,
            "losses": total_losses,
            "net_pnl": round(total_pnl, 2),
            "win_rate": round(total_wins / max(total_wins + total_losses, 1), 3),
            "roi": round(total_pnl / max(len(rows) * stake_usd, 1), 4),
        },
        "by_signal": by_signal_list,
    }


def read_signal_attribution(hours: int = 168) -> dict[str, Any]:
    """Per-signal PnL attribution over the given window.

    Answers the question "which signals make vs lose money?". For each
    signal_type, reports:
      - trades placed
      - resolved count + WR
      - gross wins / gross losses
      - net PnL + PnL-per-trade
      - profit factor
      - sample size indicator for trust
    """
    db = _get_wallet_db()
    if not db:
        return {"available": False}
    from trading_platform.polymarket.db_connection import get_connection
    cutoff_sql = f"EXTRACT(EPOCH FROM (NOW() - INTERVAL '{int(hours)} hours'))::BIGINT"
    conn = get_connection(str(db._path))
    try:
        rows = conn.execute(f"""
            SELECT signal_type,
                   COUNT(*) AS placed,
                   COUNT(*) FILTER (WHERE exit_ts IS NOT NULL) AS resolved,
                   COUNT(*) FILTER (WHERE outcome='win') AS wins,
                   COUNT(*) FILTER (WHERE outcome='loss') AS losses,
                   ROUND(COALESCE(SUM(realized_pnl), 0)::numeric, 2) AS net_pnl,
                   ROUND(COALESCE(AVG(realized_pnl), 0)::numeric, 2) AS avg_pnl_per_trade,
                   ROUND(COALESCE(SUM(CASE WHEN realized_pnl > 0 THEN realized_pnl END), 0)::numeric, 2) AS gross_wins,
                   ROUND(ABS(COALESCE(SUM(CASE WHEN realized_pnl < 0 THEN realized_pnl END), 0))::numeric, 2) AS gross_losses,
                   ROUND(COALESCE(SUM(size_usd), 0)::numeric, 2) AS volume
            FROM polymarket_paper_trades
            WHERE archived = 0 AND entry_ts >= {cutoff_sql}
            GROUP BY signal_type
            ORDER BY net_pnl DESC NULLS LAST
        """).fetchall()
    finally:
        try: conn.close()
        except Exception: pass

    attribution = []
    total_net = 0.0
    total_wins = 0
    total_losses = 0
    for r in rows:
        (sig_type, placed, resolved, wins, losses, net_pnl,
         avg_pnl, gross_wins, gross_losses, volume) = r
        net = float(net_pnl or 0)
        gw = float(gross_wins or 0)
        gl = float(gross_losses or 0)
        pf = round(gw / gl, 2) if gl > 0 else (999.0 if gw > 0 else 0.0)
        wr = wins / resolved if resolved else 0.0
        attribution.append({
            "signal_type": sig_type,
            "placed": placed,
            "resolved": resolved,
            "wins": wins,
            "losses": losses,
            "win_rate": round(wr, 3),
            "net_pnl": net,
            "avg_pnl_per_trade": float(avg_pnl or 0),
            "gross_wins": gw,
            "gross_losses": gl,
            "profit_factor": pf,
            "volume_usd": float(volume or 0),
            "confidence_tier": (
                "high" if resolved >= 20 else "medium" if resolved >= 5 else "low"
            ),
        })
        total_net += net
        total_wins += wins or 0
        total_losses += losses or 0
    return {
        "available": True,
        "window_hours": hours,
        "totals": {
            "net_pnl": round(total_net, 2),
            "wins": total_wins,
            "losses": total_losses,
            "win_rate": round(total_wins / (total_wins + total_losses), 3)
                        if (total_wins + total_losses) else 0,
        },
        "by_signal": attribution,
    }


def read_mae_mfe_analysis(min_sample: int = 10) -> dict[str, Any]:
    """Per-signal MAE/MFE distribution → recommended TP/SL adjustments.

    Compares the realized exit (current SL/TP) against MFE peaks (gains
    we left on the table) and MAE troughs (how close we got to a worse SL).
    Recommends:
      tp_optimal: median MFE for winners — captures most upside
      sl_tightest: 90th percentile MAE for winners — wouldn't have killed wins
    """
    db = _get_wallet_db()
    if not db:
        return {"available": False}
    from trading_platform.polymarket.db_connection import get_connection
    conn = get_connection(str(db._path))
    try:
        rows = conn.execute(f"""
            WITH normalized AS (
              SELECT signal_type, outcome, realized_pnl,
                     -- Convert MAE/MFE from $ to fraction-of-stake
                     mae / NULLIF(size_usd, 0) AS mae_pct,
                     mfe / NULLIF(size_usd, 0) AS mfe_pct,
                     realized_pnl / NULLIF(size_usd, 0) AS pnl_pct
              FROM polymarket_paper_trades
              WHERE archived = 0 AND exit_ts IS NOT NULL
                AND mae IS NOT NULL AND mfe IS NOT NULL
                AND size_usd > 0
            )
            SELECT signal_type,
                   COUNT(*) n,
                   COUNT(*) FILTER (WHERE outcome = 'win') wins,
                   ROUND(AVG(mae_pct)::numeric, 4) avg_mae,
                   ROUND(AVG(mfe_pct)::numeric, 4) avg_mfe,
                   ROUND(AVG(pnl_pct)::numeric, 4) avg_pnl,
                   ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY mfe_pct)
                         FILTER (WHERE outcome = 'win')::numeric, 4) median_winner_mfe,
                   ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY mae_pct)
                         FILTER (WHERE outcome = 'win')::numeric, 4) p90_winner_mae
            FROM normalized
            GROUP BY signal_type
            HAVING COUNT(*) >= {int(min_sample)}
            ORDER BY n DESC
        """).fetchall()

        cols = ["signal_type", "n", "wins", "avg_mae", "avg_mfe", "avg_pnl",
                "median_winner_mfe", "p90_winner_mae"]
        rows_dict = [dict(zip(cols, r)) for r in rows]
        for r in rows_dict:
            wmfe = r["median_winner_mfe"]
            wmae = r["p90_winner_mae"]
            r["recommended_tp"] = round(float(wmfe), 3) if wmfe is not None else None
            # Tightest SL = 90th percentile of winner MAE (10% of wins drew down
            # this far before recovering — anything tighter would kill 10%+ of
            # winners). Negate sign so it's a positive "X% drawdown" threshold.
            r["recommended_sl"] = round(float(wmae), 3) if wmae is not None else None
            r["wr"] = round(r["wins"] / r["n"], 3) if r["n"] else None
    finally:
        try: conn.close()
        except Exception: pass
    return {"available": True, "min_sample": min_sample, "signals": rows_dict}


def read_trade_journal(limit: int = 50) -> dict[str, Any]:
    """Trade journal with full gate-value snapshot per trade + per-signal
    attribution for evaluating the system's decision-making.
    """
    db = _get_wallet_db()
    if not db:
        return {"available": False}
    from trading_platform.polymarket.db_connection import get_connection
    conn = get_connection(str(db._path))
    try:
        trades = conn.execute(f"""
            SELECT id, signal_type, category, side, entry_price, exit_price,
                   size_usd, confidence, fusion_score, wallet_tier_at_fire,
                   alpha_score_at_fire, entry_context,
                   outcome, realized_pnl, exit_reason, mae, mfe,
                   entry_ts, exit_ts, question
            FROM polymarket_paper_trades
            WHERE archived = 0
            ORDER BY entry_ts DESC
            LIMIT {int(limit)}
        """).fetchall()
        cols = ["id", "signal_type", "category", "side", "entry_price", "exit_price",
                "size_usd", "confidence", "fusion_score", "wallet_tier",
                "alpha_score", "entry_context",
                "outcome", "realized_pnl", "exit_reason", "mae", "mfe",
                "entry_ts", "exit_ts", "question"]
        journal = [dict(zip(cols, r)) for r in trades]

        # Per-signal attribution (all-time)
        attribution = conn.execute("""
            SELECT signal_type,
                   COUNT(*) trades,
                   COUNT(*) FILTER (WHERE exit_ts IS NOT NULL) resolved,
                   SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END) wins,
                   ROUND(SUM(realized_pnl)::numeric, 2) total_pnl,
                   ROUND(AVG(realized_pnl)::numeric, 2) avg_pnl,
                   ROUND(AVG(mae)::numeric, 2) avg_mae,
                   ROUND(AVG(mfe)::numeric, 2) avg_mfe
            FROM polymarket_paper_trades
            WHERE archived = 0
            GROUP BY signal_type ORDER BY total_pnl DESC NULLS LAST
        """).fetchall()
        attr_cols = ["signal_type", "trades", "resolved", "wins", "total_pnl",
                     "avg_pnl", "avg_mae", "avg_mfe"]
        attr = [dict(zip(attr_cols, r)) for r in attribution]
    finally:
        try: conn.close()
        except Exception: pass
    return {
        "available": True,
        "trades": journal,
        "attribution": attr,
    }


def read_http_circuit_state() -> dict[str, Any]:
    """Return the outbound HTTP circuit breaker state for diagnostics."""
    try:
        from trading_platform.polymarket.http_circuit import snapshot
        return {"available": True, "hosts": snapshot()}
    except Exception as exc:
        return {"available": False, "error": str(exc)}


def read_pipeline_funnel(hours: int = 24) -> dict[str, Any]:
    """Return the signal→paper-trade conversion funnel for the last `hours`.

    Added after the 915-signals/0-trades incident. Makes gate drop-off
    visible so category/tier/disable filters that are rejecting the full
    signal flow get noticed immediately.
    """
    db = _get_wallet_db()
    if not db:
        return {"available": False}
    from trading_platform.polymarket.db_connection import get_connection
    cutoff_sql = f"EXTRACT(EPOCH FROM (NOW() - INTERVAL '{int(hours)} hours'))::BIGINT"
    db_path = str(db._path)
    try:
        conn = get_connection(db_path)
        # placed count from paper_trades table directly (not via the
        # signal_outcomes.paper_trade_id FK, which was inconsistently set
        # — we discovered 0/3216 linkage on 24h window). Paper trades are
        # the source of truth for "did this signal actually place?".
        by_signal = conn.execute(f"""
            SELECT so.signal_type,
                   COUNT(so.id) AS fires,
                   COALESCE(pt.placed, 0) AS placed
            FROM signal_outcomes so
            LEFT JOIN (
                SELECT signal_type, COUNT(*) AS placed
                FROM polymarket_paper_trades
                WHERE archived = 0 AND entry_ts >= {cutoff_sql}
                GROUP BY signal_type
            ) pt ON pt.signal_type = so.signal_type
            WHERE so.fired_at >= {cutoff_sql}
            GROUP BY so.signal_type, pt.placed
            ORDER BY fires DESC
        """).fetchall()
        by_category = conn.execute(f"""
            SELECT COALESCE(so.category, 'unknown') AS category,
                   COUNT(so.id) AS fires,
                   COALESCE(pt.placed, 0) AS placed
            FROM signal_outcomes so
            LEFT JOIN (
                SELECT category, COUNT(*) AS placed
                FROM polymarket_paper_trades
                WHERE archived = 0 AND entry_ts >= {cutoff_sql}
                GROUP BY category
            ) pt ON pt.category = so.category
            WHERE so.fired_at >= {cutoff_sql}
            GROUP BY so.category, pt.placed
            ORDER BY fires DESC
        """).fetchall()
        resolved_trades = conn.execute(f"""
            SELECT COUNT(*)
            FROM polymarket_paper_trades
            WHERE archived = 0 AND exit_ts >= {cutoff_sql}
        """).fetchone()
        open_trades = conn.execute("""
            SELECT COUNT(*)
            FROM polymarket_paper_trades
            WHERE archived = 0 AND exit_ts IS NULL
        """).fetchone()
    finally:
        try:
            conn.close()
        except Exception:
            pass
    total_fires = sum(r[1] for r in by_signal)
    total_placed = sum(r[2] for r in by_signal)
    return {
        "available": True,
        "window_hours": hours,
        "totals": {
            "signals_fired": total_fires,
            "paper_trades_placed": total_placed,
            "conversion_rate": round(total_placed / total_fires, 4) if total_fires else 0,
            "paper_trades_resolved_in_window": resolved_trades[0] if resolved_trades else 0,
            "paper_trades_currently_open": open_trades[0] if open_trades else 0,
        },
        "by_signal": [
            {"signal_type": r[0], "fires": r[1], "placed": r[2]}
            for r in by_signal
        ],
        "by_category": [
            {"category": r[0], "fires": r[1], "placed": r[2]}
            for r in by_category
        ],
    }


def read_paper_bankroll() -> dict[str, Any]:
    """Return bankroll allocation structure + actual paper trade performance."""
    db_path = DATA_ROOT / "kalshi" / "paper_trades.db"
    by_signal = {}
    total_pnl = 0.0
    total_resolved = 0
    total_wins = 0
    total_open = 0

    for sig_type, alloc in _SIGNAL_ALLOCATIONS.items():
        by_signal[sig_type] = {
            **alloc, "trades": 0, "resolved": 0, "wins": 0, "win_rate": None, "pnl": 0.0,
        }

    if db_path.exists():
        try:
            import sqlite3
            conn = _safe_connect(db_path, check_same_thread=False)
            rows = conn.execute("""
                SELECT COALESCE(signal_family, 'unknown'), COUNT(*),
                       SUM(CASE WHEN status='closed' THEN 1 ELSE 0 END),
                       SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END),
                       COALESCE(SUM(CASE WHEN status='closed' THEN return_pct ELSE 0 END), 0)
                FROM trades GROUP BY 1
            """).fetchall()
            for sig, trades, resolved, wins, ret in rows:
                total_resolved += (resolved or 0)
                total_wins += (wins or 0)
                total_pnl += (ret or 0)
                if sig in by_signal:
                    by_signal[sig]["trades"] = trades or 0
                    by_signal[sig]["resolved"] = resolved or 0
                    by_signal[sig]["wins"] = wins or 0
                    by_signal[sig]["win_rate"] = round((wins or 0) / resolved, 3) if resolved else None
                    by_signal[sig]["pnl"] = round(ret or 0, 2)
            total_open = conn.execute("SELECT COUNT(*) FROM trades WHERE status='open'").fetchone()[0]
            conn.close()
        except Exception:
            pass

    # Compute Polymarket-specific stats (non-archived)
    poly_open = 0
    poly_deployed = 0.0
    kalshi_open = 0
    kalshi_cash = 0.0
    if db_path.exists():
        try:
            from trading_platform.polymarket.db_connection import get_connection
            conn = get_connection(str(db_path), check_same_thread=False)
            poly_open = conn.execute(
                "SELECT COUNT(*) FROM trades WHERE platform='polymarket' AND status='open' AND (archived=0 OR archived IS NULL)"
            ).fetchone()[0]
            poly_deployed = conn.execute(
                "SELECT COALESCE(SUM(size_usd), 0) FROM trades WHERE platform='polymarket' AND status='open' AND (archived=0 OR archived IS NULL)"
            ).fetchone()[0]
            kalshi_open = conn.execute(
                "SELECT COUNT(*) FROM trades WHERE (platform='kalshi' OR platform IS NULL) AND status='open'"
            ).fetchone()[0]
            cash_row = conn.execute("SELECT cash_usd FROM portfolio ORDER BY id DESC LIMIT 1").fetchone()
            kalshi_cash = float(cash_row[0]) if cash_row else 0
            conn.close()
        except Exception:
            pass

    # Anchor to STARTING_BANKROLL (real paper book) instead of the old
    # $100k placeholder. Stale hardcode missed in the first GUI sweep.
    try:
        from trading_platform.polymarket.polymarket_paper_executor import STARTING_BANKROLL as _SB
        _bankroll = float(_SB)
    except Exception:
        _bankroll = 10_000.0
    return {
        "available": True,
        "total_bankroll": _bankroll,
        "starting_bankroll": _bankroll,
        "polymarket_cash": round(_bankroll - poly_deployed, 2),
        "polymarket_open_count": poly_open,
        "polymarket_deployed": round(poly_deployed, 2),
        "kalshi_cash": round(kalshi_cash, 2),
        "kalshi_open_count": kalshi_open,
        "net_pnl": round(total_pnl, 2),
        "win_rate": round(total_wins / total_resolved, 3) if total_resolved > 0 else None,
        "resolved": total_resolved,
        "open": total_open,
        "by_signal": by_signal,
    }


def read_paper_pnl_history() -> dict[str, Any]:
    """Daily P&L from Postgres polymarket_paper_trades."""
    from trading_platform.polymarket.db_connection import db as _db
    by_day: list[dict] = []
    by_signal_total: dict[str, dict] = {}

    try:
        with _db() as conn:
            rows = conn.execute("""
                SELECT TO_CHAR(TO_TIMESTAMP(exit_ts), 'YYYY-MM-DD') AS d,
                       signal_type, SUM(realized_pnl), COUNT(*),
                       SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END)
                FROM polymarket_paper_trades
                WHERE archived = 0 AND exit_ts IS NOT NULL AND realized_pnl IS NOT NULL
                GROUP BY d, signal_type ORDER BY d
            """).fetchall()

            day_totals: dict[str, float] = {}
            for d, sig, pnl, count, wins in rows:
                if d:
                    day_totals[d] = day_totals.get(d, 0) + float(pnl or 0)
                sig = sig or "unknown"
                if sig not in by_signal_total:
                    by_signal_total[sig] = {"total_pnl": 0.0, "trades": 0, "wins": 0}
                by_signal_total[sig]["total_pnl"] += float(pnl or 0)
                by_signal_total[sig]["trades"] += count
                by_signal_total[sig]["wins"] += (wins or 0)

            cumulative = 0.0
            for d in sorted(day_totals):
                cumulative += day_totals[d]
                by_day.append({"date": d, "pnl": round(day_totals[d], 2), "cumulative_pnl": round(cumulative, 2)})
    except Exception:
        pass

    return {
        "available": True,
        "has_data": len(by_day) > 0,
        "by_day": by_day,
        "by_signal_total": {k: {kk: round(vv, 4) if isinstance(vv, float) else vv for kk, vv in v.items()} for k, v in by_signal_total.items()},
    }


def read_market_detail(condition_id: str) -> dict[str, Any]:
    """Full detail for a specific market: whale trades, signals, paper position."""
    import time as _t
    now = int(_t.time())

    # Market metadata from universe
    question = ""
    category = "other"
    end_date_iso = ""
    volume = 0
    try:
        from trading_platform.polymarket.market_universe import MarketUniverse
        u = MarketUniverse()
        if u.load_cached():
            question = u.get_question(condition_id)
            category = u.get_category(condition_id)
            entry = u._by_condition.get(condition_id, {})
            end_date_iso = entry.get("end_date_iso", "")
            volume = entry.get("volume", 0)
    except Exception:
        pass

    hours_to_close = None
    if end_date_iso:
        try:
            from datetime import datetime, timezone
            end_dt = datetime.fromisoformat(end_date_iso.replace("Z", "+00:00"))
            hours_to_close = round((end_dt - datetime.now(tz=timezone.utc)).total_seconds() / 3600, 1)
        except Exception:
            pass

    # Current price
    current_price = None
    try:
        import urllib.request as _ur
        url = f"https://gamma-api.polymarket.com/markets?conditionId={condition_id}&limit=1"
        with _ur.urlopen(url, timeout=5) as resp:
            data = json.loads(resp.read())
            if data and isinstance(data, list):
                prices_str = data[0].get("outcomePrices")
                if prices_str:
                    prices = json.loads(prices_str) if isinstance(prices_str, str) else prices_str
                    if prices:
                        current_price = float(prices[0])
                if not question:
                    question = data[0].get("question", "")
    except Exception:
        pass

    # Whale trades from wallet_alerts
    whale_trades = []
    whale_summary = {"tier1_count": 0, "tier2_count": 0, "buy_volume_tier1": 0, "sell_volume_tier1": 0}
    db = _get_wallet_db()
    if db:
        try:
            with db._lock:
                rows = db._conn.execute("""
                    SELECT a.wallet, a.side, a.size, a.price, a.detected_at, a.tier,
                           a.directional_win_rate, a.category, a.question,
                           l.wallet_type, l.conviction_score
                    FROM wallet_alerts a
                    LEFT JOIN leaderboard l ON a.wallet = l.wallet
                    WHERE a.token_id = ?
                    ORDER BY a.detected_at DESC LIMIT 100
                """, (condition_id,)).fetchall()

            for r in rows:
                w, side, size, price, ts, tier, wr, cat, q, wtype, conv = r
                whale_trades.append({
                    "wallet": (w or "")[:10] + "...",
                    "wallet_full": w,
                    "side": side, "size": size, "price": price,
                    "detected_at": ts, "tier": tier,
                    "directional_win_rate": wr,
                    "wallet_type": wtype, "conviction_score": conv,
                    "time_ago": _human_age(ts, now),
                })
                if tier == 1:
                    whale_summary["tier1_count"] += 1
                    if side == "BUY":
                        whale_summary["buy_volume_tier1"] += (size or 0)
                    else:
                        whale_summary["sell_volume_tier1"] += (size or 0)
                elif tier == 2:
                    whale_summary["tier2_count"] += 1

            buy_t1 = whale_summary["buy_volume_tier1"]
            sell_t1 = whale_summary["sell_volume_tier1"]
            whale_summary["net_direction"] = "BUY" if buy_t1 > sell_t1 else "SELL" if sell_t1 > buy_t1 else "MIXED"
        except Exception:
            pass

    # Signals on this market
    signals = []
    if db:
        try:
            with db._lock:
                sig_rows = db._conn.execute("""
                    SELECT signal_type, direction, confidence, wallet, fired_at, size
                    FROM market_signals WHERE condition_id = ?
                    ORDER BY fired_at DESC LIMIT 20
                """, (condition_id,)).fetchall()
            for r in sig_rows:
                signals.append({
                    "signal_type": r[0], "direction": r[1], "confidence": r[2],
                    "wallet": (r[3] or "")[:10] + "...", "fired_at": r[4], "size": r[5],
                    "time_ago": _human_age(r[4], now),
                })
        except Exception:
            pass

    # Paper position
    paper_position = None
    try:
        from trading_platform.polymarket.db_connection import get_connection
        paper_db = DATA_ROOT / "kalshi" / "paper_trades.db"
        if paper_db.exists():
            conn = get_connection(str(paper_db))
            pr = conn.execute("""
                SELECT ticker, side, entry_price, size_usd, confidence, signal_type, entry_ts
                FROM trades
                WHERE (full_token_id = ? OR ticker LIKE ?)
                AND status = 'open' AND (archived = 0 OR archived IS NULL)
                LIMIT 1
            """, (condition_id, condition_id[:20] + "%")).fetchone()
            if pr:
                paper_position = {
                    "side": pr[1], "entry_price": pr[2], "size_usd": pr[3],
                    "confidence": pr[4], "signal_type": pr[5], "entry_ts": pr[6],
                    "current_price": current_price,
                    "unrealized_pnl": round(pr[3] * (current_price / pr[2] - 1), 2) if current_price and pr[2] else None,
                }
            conn.close()
    except Exception:
        pass

    return {
        "available": True,
        "condition_id": condition_id,
        "question": question,
        "category": category,
        "end_date_iso": end_date_iso,
        "hours_to_close": hours_to_close,
        "current_price": current_price,
        "total_volume": volume,
        "whale_trades": whale_trades,
        "whale_summary": whale_summary,
        "signals": signals,
        "paper_position": paper_position,
    }


def _human_age(ts: int | None, now: int) -> str:
    if not ts:
        return "?"
    d = now - ts
    if d < 60:
        return f"{d}s ago"
    if d < 3600:
        return f"{d // 60}m ago"
    if d < 86400:
        return f"{d // 3600}h ago"
    return f"{d // 86400}d ago"


# Module-level cache: condition_id -> (timestamp, price). 5-minute TTL.
_PAPER_PRICE_CACHE: dict[str, tuple[float, float | None]] = {}
_PAPER_PRICE_TTL_SECONDS = 300


def _fetch_polymarket_current_price(condition_id: str) -> float | None:
    """Look up the current YES-side price for a Polymarket market.

    Uses the verified ``condition_ids`` (plural) Gamma API param — passing
    ``conditionId`` is silently ignored and returns the default first
    market. Cached for 5 minutes per condition_id. Returns None on
    failure rather than raising.
    """
    import time as _t
    now = _t.time()
    cached = _PAPER_PRICE_CACHE.get(condition_id)
    if cached and (now - cached[0]) < _PAPER_PRICE_TTL_SECONDS:
        return cached[1]

    price: float | None = None
    try:
        import requests as _req
        r = _req.get(
            "https://gamma-api.polymarket.com/markets",
            params={"condition_ids": condition_id},
            timeout=5,
        )
        if r.status_code == 200:
            data = r.json()
            m = data[0] if isinstance(data, list) and data else None
            if m and (m.get("conditionId") or "").lower() == condition_id.lower():
                # Try in priority order: lastTradePrice, midpoint of bid/ask, outcomePrices[0]
                ltp = m.get("lastTradePrice")
                if ltp is not None:
                    price = float(ltp)
                if price is None:
                    bb = m.get("bestBid")
                    ba = m.get("bestAsk")
                    if bb is not None and ba is not None:
                        price = (float(bb) + float(ba)) / 2
                if price is None:
                    op_raw = m.get("outcomePrices")
                    if op_raw:
                        op = json.loads(op_raw) if isinstance(op_raw, str) else op_raw
                        if op:
                            price = float(op[0])
    except Exception as exc:
        logger = __import__("logging").getLogger(__name__)
        logger.debug("price fetch failed for %s: %s", condition_id[:20], exc)

    _PAPER_PRICE_CACHE[condition_id] = (now, price)
    return price


def read_paper_positions_enriched() -> dict[str, Any]:
    """Open Polymarket paper positions with live current prices.

    Reads from ``polymarket_paper_trades`` (the canonical Polymarket
    paper-trade table inside ``wallet_intelligence.db``), enriches each
    position with the current Gamma price, and computes unrealized PnL.
    """
    from trading_platform.polymarket.db_connection import get_connection
    db = _get_wallet_db()
    if not db:
        return {
            "available": True, "positions": [], "count": 0,
            "unrealized_pnl_total": 0, "starting_bankroll": 100_000,
        }

    try:
        conn = get_connection(str(db._path))
        rows = conn.execute(
            """SELECT id, condition_id, question, category, side,
                      entry_price, size_usd, signal_type, confidence,
                      wallet, entry_ts
               FROM polymarket_paper_trades
               WHERE archived = 0
               ORDER BY entry_ts DESC"""
        ).fetchall()
        conn.close()
    except Exception as exc:
        return {"available": False, "error": str(exc), "positions": []}

    cols = ["id", "condition_id", "question", "category", "side",
            "entry_price", "size_usd", "signal_type", "confidence",
            "wallet", "entry_ts"]

    positions: list[dict[str, Any]] = []
    total_unrealized = 0.0

    for row in rows:
        p = dict(zip(cols, row))
        cid = p["condition_id"] or ""

        current_price: float | None = None
        unrealized_pnl: float | None = None
        unrealized_pct: float | None = None
        if cid:
            current_price = _fetch_polymarket_current_price(cid)
            entry = float(p.get("entry_price") or 0)
            size = float(p.get("size_usd") or 0)
            if current_price is not None and entry > 0 and size > 0:
                # Long this outcome at entry → return = (current - entry) / entry
                ret = (current_price / entry) - 1.0
                unrealized_pct = round(ret * 100, 2)
                unrealized_pnl = round(size * ret, 2)
                total_unrealized += unrealized_pnl

        sig_type = p.get("signal_type") or ""
        conf = float(p.get("confidence") or 0)
        justification = f"{sig_type}: conf {conf:.0%}" if sig_type else "unknown"

        positions.append({
            "id": p["id"],
            "condition_id": cid,
            "question": p.get("question") or "",
            "category": p.get("category") or "",
            "side": p.get("side") or "YES",
            "entry_price": p.get("entry_price"),
            "size_usd": p.get("size_usd"),
            "signal_type": sig_type,
            "confidence": conf,
            "wallet": p.get("wallet"),
            "entry_ts": p.get("entry_ts"),
            "platform": "polymarket",
            "current_price": round(current_price, 4) if current_price is not None else None,
            "unrealized_pnl": unrealized_pnl,
            "unrealized_pct": unrealized_pct,
            "justification": justification,
        })

    return {
        "available": True,
        "positions": positions,
        "count": len(positions),
        "unrealized_pnl_total": round(total_unrealized, 2),
        "starting_bankroll": 100_000,
    }


def read_circuit_breaker_status() -> dict[str, Any]:
    """Cumulative-drawdown circuit breaker state + recent log."""
    db = _get_wallet_db()
    if not db:
        return {"available": False}
    try:
        from trading_platform.polymarket.circuit_breaker import CircuitBreaker
        return CircuitBreaker(str(db._path)).get_status()
    except Exception as exc:
        return {"available": False, "error": str(exc)}


def read_circuit_breaker_log(limit: int = 50) -> dict[str, Any]:
    db = _get_wallet_db()
    if not db:
        return {"available": False, "events": []}
    try:
        from trading_platform.polymarket.circuit_breaker import CircuitBreaker
        events = CircuitBreaker(str(db._path)).get_log(limit=limit)
        return {"available": True, "events": events, "count": len(events)}
    except Exception as exc:
        return {"available": False, "error": str(exc), "events": []}


def trigger_circuit_breaker_reset(
    confirm: bool = False,
    reset_peak: bool = False,
    reset_by: str = "api",
) -> dict[str, Any]:
    if not confirm:
        return {"error": "reset requires confirm=true"}
    db = _get_wallet_db()
    if not db:
        return {"error": "wallet_db unavailable"}
    try:
        from trading_platform.polymarket.circuit_breaker import CircuitBreaker
        return CircuitBreaker(str(db._path)).reset_drawdown(
            reset_by=reset_by, reset_peak=reset_peak,
        )
    except Exception as exc:
        return {"error": str(exc)}


def trigger_circuit_breaker_configure(
    max_drawdown_pct: float | None = None,
    daily_loss_limit: float | None = None,
) -> dict[str, Any]:
    db = _get_wallet_db()
    if not db:
        return {"error": "wallet_db unavailable"}
    try:
        from trading_platform.polymarket.circuit_breaker import CircuitBreaker
        return CircuitBreaker(str(db._path)).configure(
            max_drawdown_pct=max_drawdown_pct,
            daily_loss_limit=daily_loss_limit,
        )
    except Exception as exc:
        return {"error": str(exc)}


def trigger_circuit_breaker_initialize(
    starting_capital: float = 100_000.0,
    max_drawdown_pct: float = 0.20,
    daily_loss_limit: float = 0.05,
) -> dict[str, Any]:
    db = _get_wallet_db()
    if not db:
        return {"error": "wallet_db unavailable"}
    try:
        from trading_platform.polymarket.circuit_breaker import CircuitBreaker
        return CircuitBreaker(str(db._path)).initialize(
            starting_capital=starting_capital,
            max_drawdown_pct=max_drawdown_pct,
            daily_loss_limit=daily_loss_limit,
        )
    except Exception as exc:
        return {"error": str(exc)}


def read_scheduler_status() -> dict[str, Any]:
    """Reads ``data/scheduler/state.json`` written by task_scheduler.py."""
    from pathlib import Path as _P
    state_path = _P("data/scheduler/state.json")
    if not state_path.exists():
        return {
            "available": False,
            "reason": "scheduler state file not found — task_scheduler.py not running?",
            "tasks": [],
        }
    try:
        data = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"available": False, "error": str(exc), "tasks": []}
    return {
        "available": True,
        "updated_at": data.get("updated_at"),
        "tasks": data.get("tasks", []),
    }


def read_tiers_summary() -> dict[str, Any]:
    """Per-category tier distribution + recent changes."""
    from trading_platform.polymarket.db_connection import get_connection
    db = _get_wallet_db()
    if not db:
        return {"available": False}
    try:
        conn = get_connection(str(db._path))
        try:
            dist_rows = conn.execute(
                """SELECT category, tier, COUNT(*)
                   FROM wallet_category_profiles
                   GROUP BY category, tier
                   ORDER BY category, tier"""
            ).fetchall()
            recent_rows = conn.execute(
                """SELECT wallet, category, old_tier, new_tier, reason,
                          trigger_metric, changed_at
                   FROM wallet_tier_history
                   ORDER BY changed_at DESC LIMIT 10"""
            ).fetchall()
            last_eval_row = conn.execute(
                "SELECT MAX(last_evaluated) FROM wallet_category_profiles"
            ).fetchone()
        finally:
            conn.close()
    except Exception as exc:
        return {"available": False, "error": str(exc)}

    categories: dict[str, dict[str, int]] = {}
    for cat, tier, n in dist_rows:
        cat = cat or "other"
        categories.setdefault(cat, {"S": 0, "A": 0, "B": 0, "C": 0, "D": 0, "total": 0})
        categories[cat][tier or "D"] = n
        categories[cat]["total"] += n

    return {
        "available": True,
        "categories": categories,
        "recent_changes": [
            {
                "wallet": r[0], "category": r[1], "old_tier": r[2],
                "new_tier": r[3], "reason": r[4], "trigger_metric": r[5],
                "changed_at": r[6],
            }
            for r in recent_rows
        ],
        "last_full_rebuild": last_eval_row[0] if last_eval_row and last_eval_row[0] else None,
    }


def read_tiers_category(category: str) -> dict[str, Any]:
    """Per-category leaderboard sorted by tier_score DESC."""
    from trading_platform.polymarket.db_connection import get_connection
    db = _get_wallet_db()
    if not db:
        return {"available": False}
    try:
        conn = get_connection(str(db._path))
        try:
            rows = conn.execute(
                """SELECT wallet, tier, tier_score, win_rate, win_rate_30d,
                          win_rate_90d, trend_score, consistency_score,
                          resolved_trades, avg_bet_size, monthly_pnl,
                          category_purity, last_trade_at,
                          last_evaluated, resolved_since_last_change, last_change_at
                   FROM wallet_category_profiles
                   WHERE LOWER(category) = ?
                   ORDER BY tier_score DESC""",
                (category.lower(),),
            ).fetchall()
        finally:
            conn.close()
    except Exception as exc:
        return {"available": False, "error": str(exc)}

    cols = [
        "wallet", "tier", "tier_score", "win_rate", "win_rate_30d",
        "win_rate_90d", "trend_score", "consistency_score", "resolved_trades",
        "avg_bet_size", "monthly_pnl", "category_purity", "last_trade_at",
        "last_evaluated", "resolved_since_last_change", "last_change_at",
    ]
    return {
        "available": True,
        "category": category,
        "wallets": [dict(zip(cols, r)) for r in rows],
    }


def read_tiers_wallet(wallet: str) -> dict[str, Any]:
    """All category profiles for a single wallet + tier history."""
    from trading_platform.polymarket.db_connection import get_connection
    db = _get_wallet_db()
    if not db:
        return {"available": False}
    try:
        conn = get_connection(str(db._path))
        try:
            wallet = (wallet or "").lower()
            prof_rows = conn.execute(
                """SELECT category, tier, tier_score, win_rate, win_rate_30d,
                          win_rate_90d, trend_score, consistency_score,
                          resolved_trades, monthly_pnl, last_trade_at, last_evaluated
                   FROM wallet_category_profiles
                   WHERE wallet = ?
                   ORDER BY tier_score DESC""",
                (wallet,),
            ).fetchall()
            hist_rows = conn.execute(
                """SELECT category, old_tier, new_tier, reason, trigger_metric,
                          old_tier_score, new_tier_score, changed_at
                   FROM wallet_tier_history
                   WHERE wallet = ?
                   ORDER BY changed_at DESC LIMIT 20""",
                (wallet,),
            ).fetchall()
        finally:
            conn.close()
    except Exception as exc:
        return {"available": False, "error": str(exc)}

    pcols = [
        "category", "tier", "tier_score", "win_rate", "win_rate_30d",
        "win_rate_90d", "trend_score", "consistency_score", "resolved_trades",
        "monthly_pnl", "last_trade_at", "last_evaluated",
    ]
    hcols = [
        "category", "old_tier", "new_tier", "reason", "trigger_metric",
        "old_tier_score", "new_tier_score", "changed_at",
    ]
    return {
        "available": True,
        "wallet": wallet,
        "categories": {row[0]: dict(zip(pcols, row)) for row in prof_rows},
        "tier_history": [dict(zip(hcols, r)) for r in hist_rows],
    }


def read_tiers_history(category: str | None = None, days: int = 30) -> dict[str, Any]:
    """Tier change history for the GUI's tier movement chart."""
    from trading_platform.polymarket.db_connection import get_connection
    import time as _time
    db = _get_wallet_db()
    if not db:
        return {"available": False}
    cutoff = int(_time.time()) - int(days) * 86400
    try:
        conn = get_connection(str(db._path))
        try:
            if category:
                rows = conn.execute(
                    """SELECT wallet, category, old_tier, new_tier, reason,
                              trigger_metric, changed_at
                       FROM wallet_tier_history
                       WHERE LOWER(category) = ? AND changed_at >= ?
                       ORDER BY changed_at DESC""",
                    (category.lower(), cutoff),
                ).fetchall()
            else:
                rows = conn.execute(
                    """SELECT wallet, category, old_tier, new_tier, reason,
                              trigger_metric, changed_at
                       FROM wallet_tier_history
                       WHERE changed_at >= ?
                       ORDER BY changed_at DESC""",
                    (cutoff,),
                ).fetchall()
        finally:
            conn.close()
    except Exception as exc:
        return {"available": False, "error": str(exc), "changes": []}

    cols = [
        "wallet", "category", "old_tier", "new_tier", "reason",
        "trigger_metric", "changed_at",
    ]
    return {
        "available": True,
        "category": category,
        "days": days,
        "changes": [dict(zip(cols, r)) for r in rows],
        "count": len(rows),
    }


def trigger_tier_rebuild() -> dict[str, Any]:
    """Run a full nightly rebuild (also exposed as POST /api/tiers/rebuild)."""
    db = _get_wallet_db()
    if not db:
        return {"available": False}
    try:
        from trading_platform.polymarket.wallet_tiering import WalletTieringEngine
        return WalletTieringEngine(str(db._path)).build_all_profiles()
    except Exception as exc:
        return {"available": False, "error": str(exc)}


def read_market_data(condition_id: str, direction: str = "YES") -> dict[str, Any]:
    """Full pmxt microstructure for a market (or empty dict if pmxt down)."""
    db = _get_wallet_db()
    if not db:
        return {"available": False, "error": "wallet_db unavailable"}
    try:
        from trading_platform.polymarket.market_data_service import MarketDataService
        mds = MarketDataService(str(db._path))
        return mds.get_market_microstructure(
            condition_id=condition_id, direction=direction,
        )
    except Exception as exc:
        return {"available": False, "error": str(exc)}


def read_market_data_candles(
    condition_id: str,
    direction: str = "YES",
    resolution: str = "1h",
    hours: int = 24,
) -> dict[str, Any]:
    """Raw OHLCV candles for charting."""
    db = _get_wallet_db()
    if not db:
        return {"available": False}
    try:
        from trading_platform.polymarket.market_data_service import MarketDataService
        mds = MarketDataService(str(db._path))
        outcome_id = mds.resolve_outcome_id(
            condition_id=condition_id, direction=direction,
        )
        if not outcome_id:
            return {"available": False, "error": "outcome_id not resolved", "candles": []}
        ex = mds._get_exchange()
        if ex is None:
            return {"available": False, "error": "pmxt unavailable", "candles": []}
        try:
            candles = ex.fetch_ohlcv(outcome_id, resolution=resolution, limit=int(hours))
            mds._record_call(success=True)
        except Exception as exc:
            mds._record_call(success=False, error=str(exc))
            return {"available": False, "error": str(exc), "candles": []}
        velocity = mds._velocity_from_candles(candles)
        return {
            "available": True,
            "outcome_id": outcome_id,
            "resolution": resolution,
            "hours": hours,
            "candles": velocity.get("candles", []),
        }
    except Exception as exc:
        return {"available": False, "error": str(exc), "candles": []}


def read_market_data_health() -> dict[str, Any]:
    """pmxt sidecar status + cache hit rate + error count."""
    db = _get_wallet_db()
    if not db:
        return {"available": False}
    try:
        from trading_platform.polymarket.market_data_service import MarketDataService
        return MarketDataService(str(db._path)).get_health()
    except Exception as exc:
        return {"available": False, "error": str(exc)}


def read_calibration_status() -> dict[str, Any]:
    """Per-signal calibration + current allocation plan + bankroll snapshot."""
    from trading_platform.polymarket.db_connection import get_connection
    db = _get_wallet_db()
    if not db:
        return {"available": False, "error": "wallet_db unavailable"}
    db_path = str(db._path)

    try:
        conn = get_connection(db_path)
        try:
            rows = conn.execute(
                """SELECT signal_type, sample_size, wins, losses, bayesian_wr,
                          ev_per_trade, profit_factor, sharpe_ratio,
                          kelly_fraction, rolling_10_wr, rolling_20_ev,
                          consecutive_losses, recommended_allocation_pct,
                          recommended_stake_usd, allocated_usd, status,
                          last_updated,
                          max_drawdown_pct, max_consec_losses, sortino_ratio
                   FROM signal_calibration
                   ORDER BY CASE status
                        WHEN 'live' THEN 1
                        WHEN 'weak' THEN 2
                        WHEN 'building' THEN 3
                        WHEN 'disabled' THEN 4
                        ELSE 5 END,
                       sample_size DESC"""
            ).fetchall()
        finally:
            conn.close()
    except Exception as exc:
        return {"available": False, "error": str(exc)}

    cols = [
        "signal_type", "sample_size", "wins", "losses", "bayesian_wr",
        "ev_per_trade", "profit_factor", "sharpe_ratio", "kelly_fraction",
        "rolling_10_wr", "rolling_20_ev", "consecutive_losses",
        "recommended_allocation_pct", "recommended_stake_usd",
        "allocated_usd", "status", "last_updated",
        "max_drawdown_pct", "max_consec_losses", "sortino_ratio",
    ]
    by_type = [dict(zip(cols, r)) for r in rows]

    # Live allocation plan + bankroll snapshot
    allocation_plan = None
    try:
        from trading_platform.polymarket.bankroll_allocator import BankrollAllocator
        plan = BankrollAllocator(db_path).compute_plan()
        allocation_plan = plan.to_dict()
    except Exception as exc:
        allocation_plan = {"error": str(exc)}

    return {
        "available": True,
        "by_type": by_type,
        "n_live": sum(1 for s in by_type if s["status"] == "live"),
        "n_weak": sum(1 for s in by_type if s["status"] == "weak"),
        "n_building": sum(1 for s in by_type if s["status"] == "building"),
        "n_disabled": sum(1 for s in by_type if s["status"] == "disabled"),
        "allocation_plan": allocation_plan,
    }


def read_calibration_report(date: str | None = None) -> dict[str, Any]:
    """Read a stored calibration report by date, or the latest if date is None."""
    from trading_platform.polymarket.db_connection import get_connection
    import json as _json
    db = _get_wallet_db()
    if not db:
        return {"available": False}
    try:
        conn = get_connection(str(db._path))
        try:
            if date:
                row = conn.execute(
                    """SELECT report_date, created_at, report_json,
                              total_bankroll, total_pnl_today
                       FROM calibration_reports
                       WHERE report_date = ?
                       ORDER BY created_at DESC LIMIT 1""",
                    (date,),
                ).fetchone()
            else:
                row = conn.execute(
                    """SELECT report_date, created_at, report_json,
                              total_bankroll, total_pnl_today
                       FROM calibration_reports
                       ORDER BY created_at DESC LIMIT 1"""
                ).fetchone()
        finally:
            conn.close()
    except Exception as exc:
        return {"available": False, "error": str(exc)}
    if not row:
        return {"available": True, "report": None}
    try:
        body = _json.loads(row[2])
    except Exception:
        body = {}
    return {
        "available": True,
        "report_date": row[0],
        "created_at": row[1],
        "total_bankroll": row[3],
        "total_pnl_today": row[4],
        "report": body,
    }


def write_calibration_report() -> dict[str, Any]:
    """Generate and persist a daily calibration report. Returns the report."""
    from trading_platform.polymarket.db_connection import get_connection
    import json as _json
    import time as _time
    from datetime import datetime as _dt, timezone as _tz
    db = _get_wallet_db()
    if not db:
        return {"available": False}
    db_path = str(db._path)

    status = read_calibration_status()
    if not status.get("available"):
        return {"available": False, "error": "calibration unavailable"}

    # Day's PnL from polymarket_paper_trades
    today_start = int(_time.time()) - 86400
    total_pnl_today = 0.0
    bankroll = 100_000.0
    try:
        conn = get_connection(db_path)
        try:
            row = conn.execute(
                """SELECT COALESCE(SUM(realized_pnl), 0)
                   FROM polymarket_paper_trades
                   WHERE archived = 0 AND exit_ts >= ?""",
                (today_start,),
            ).fetchone()
            total_pnl_today = float(row[0] or 0.0)

            # Trend per signal: compare last-10 cum PnL vs prior-10
            trends: dict[str, str] = {}
            for s in status["by_type"]:
                sig = s["signal_type"]
                trades = conn.execute(
                    """SELECT realized_pnl FROM polymarket_paper_trades
                       WHERE signal_type = ? AND archived = 0
                         AND exit_ts IS NOT NULL AND realized_pnl IS NOT NULL
                       ORDER BY exit_ts DESC LIMIT 20""",
                    (sig,),
                ).fetchall()
                pnls = [float(r[0]) for r in trades]
                if len(pnls) >= 10:
                    last10 = sum(pnls[:10])
                    prev10 = sum(pnls[10:20]) if len(pnls) >= 20 else 0.0
                    trends[sig] = (
                        "improving" if last10 > prev10 * 1.1
                        else "degrading" if last10 < prev10 * 0.9
                        else "stable"
                    )
                else:
                    trends[sig] = "insufficient_data"
        finally:
            conn.close()
    except Exception:
        trends = {}

    report_body = {
        "by_type": status["by_type"],
        "trends": trends,
        "allocation_plan": status.get("allocation_plan"),
        "summary": {
            "n_live": status["n_live"],
            "n_weak": status["n_weak"],
            "n_building": status["n_building"],
            "n_disabled": status["n_disabled"],
        },
    }
    report_date = _dt.now(tz=_tz.utc).strftime("%Y-%m-%d")
    try:
        conn = get_connection(db_path)
        try:
            conn.execute(
                """INSERT INTO calibration_reports
                    (report_date, created_at, report_json, total_bankroll, total_pnl_today)
                   VALUES (?, ?, ?, ?, ?)""",
                (
                    report_date, int(_time.time()), _json.dumps(report_body),
                    bankroll, round(total_pnl_today, 2),
                ),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception as exc:
        return {"available": False, "error": str(exc)}

    return {
        "available": True,
        "report_date": report_date,
        "report": report_body,
        "total_bankroll": bankroll,
        "total_pnl_today": round(total_pnl_today, 2),
    }


def trigger_calibration_rebalance(dry_run: bool = False) -> dict[str, Any]:
    """Recompute calibration from scratch and rebalance the bankroll."""
    db = _get_wallet_db()
    if not db:
        return {"available": False}
    try:
        from trading_platform.polymarket.signal_evaluator import SignalEvaluator
        from trading_platform.polymarket.bankroll_allocator import BankrollAllocator
    except Exception as exc:
        return {"available": False, "error": f"calibration modules unavailable: {exc}"}
    db_path = str(db._path)
    # Anchor the allocator to the real paper bankroll (STARTING_BANKROLL)
    # instead of the allocator's $100k default. Otherwise every signal gets
    # a 10× allocation relative to actual capital.
    try:
        from trading_platform.polymarket.polymarket_paper_executor import STARTING_BANKROLL
        bankroll = float(STARTING_BANKROLL)
    except Exception:
        bankroll = 10_000.0
    SignalEvaluator(db_path).update_all()
    plan = BankrollAllocator(db_path, bankroll=bankroll).rebalance(dry_run=dry_run)
    return {
        "available": True,
        "dry_run": dry_run,
        "bankroll": bankroll,
        "plan": plan.to_dict(),
    }


def read_live_readiness() -> dict[str, Any]:
    """All live readiness gates computed from live data.

    Aggregates: per-signal gates from polymarket_paper_trades, system
    gates from env + CLOB connectivity + emergency stop file, and the
    current KillSwitch hard limits for display.
    """
    import os as _os
    from trading_platform.polymarket.db_connection import get_connection
    db = _get_wallet_db()
    if not db:
        return {"available": False, "error": "wallet_db unavailable"}

    try:
        from trading_platform.polymarket.kill_switch import KillSwitch
        from trading_platform.polymarket.kelly_sizer import KellySizer
        from trading_platform.polymarket.clob_client import ClobClient
    except Exception as exc:
        return {"available": False, "error": f"live modules unavailable: {exc}"}

    db_path = str(db._path)
    ks = KillSwitch(db_path)
    sizer = KellySizer(db_path)
    clob = ClobClient()

    # Per-signal performance from polymarket_paper_trades
    sig_rows: list[tuple] = []
    live_trades_count = 0
    try:
        conn = get_connection(db_path)
        try:
            # EV from realized_pnl/size_usd — the authoritative source of
            # truth. return_pct column has historical mixed units (some
            # rows ratio, some rows percentage-number) that skewed the
            # average; whale_entry EV was reading -2.6% when real avg
            # return was +103%. realized_pnl is always USDC dollars,
            # size_usd is always USDC dollars, so their ratio is always
            # a correct fractional return.
            sig_rows = conn.execute(
                """SELECT signal_type,
                          COUNT(*) AS n,
                          AVG(realized_pnl / NULLIF(size_usd, 0)) AS avg_ev,
                          SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END) AS wins
                   FROM polymarket_paper_trades
                   WHERE archived = 0 AND exit_ts IS NOT NULL
                     AND realized_pnl IS NOT NULL AND size_usd > 0
                   GROUP BY signal_type"""
            ).fetchall()
            try:
                live_trades_count = conn.execute(
                    "SELECT COUNT(*) FROM live_trades WHERE dry_run = 0"
                ).fetchone()[0]
            except Exception:
                live_trades_count = 0
        finally:
            conn.close()
    except Exception:
        pass

    signal_gates: list[dict[str, Any]] = []
    for sig_type, n, avg_ev, wins in sig_rows:
        n = n or 0
        ev = float(avg_ev or 0.0)
        wr = (wins or 0) / n if n > 0 else 0.0
        kelly = sizer.compute_kelly(sig_type)
        sample_ok = n >= ks.MIN_RESOLVED_HARD
        ev_ok = ev > 0
        wr_ok = wr >= ks.MIN_WIN_RATE if n >= 20 else True
        probation_ok = (
            n >= ks.PROBATION_MIN_RESOLVED and n < ks.MIN_RESOLVED_HARD
            and ev_ok and wr_ok
        )
        ready = sample_ok and ev_ok and wr_ok
        signal_gates.append({
            "signal_type": sig_type,
            "n_resolved": n,
            "ev": round(ev, 4),
            "win_rate": round(wr, 3) if n > 0 else None,
            "kelly_recommended_usd": kelly.get("recommended_usd"),
            "kelly_fraction": kelly.get("kelly_fractional"),
            "gates": {
                "sample_size": sample_ok,
                "ev_positive": ev_ok,
                "win_rate_ok": wr_ok,
            },
            "ready": ready,
            "probation_eligible": probation_ok,
            "probation_cap_usd": ks.PROBATION_MAX_STAKE_USD if probation_ok else None,
        })
    signal_gates.sort(key=lambda s: (-int(s["ready"]), -s["n_resolved"]))

    # CLOB + system state
    clob_test = clob.test_connection()
    is_stopped, stop_reason = ks.is_emergency_stopped()
    master_switch = _os.getenv("POLYMARKET_LIVE_ENABLED", "").lower() in ("1", "true", "yes")

    system_gates = {
        "master_switch": master_switch,
        "clob_reachable": clob_test.get("public_endpoint", False),
        "clob_configured": clob_test.get("credentials_configured", False),
        "wallet_set": clob_test.get("wallet_set", False),
        "py_clob_client": clob_test.get("py_clob_client", False),
        "emergency_stop_clear": not is_stopped,
        "live_trades_executed": live_trades_count,
    }

    ready_signals = [s for s in signal_gates if s["ready"]]
    return {
        "available": True,
        "system_gates": system_gates,
        "signal_gates": signal_gates,
        "ready_signals": [s["signal_type"] for s in ready_signals],
        "n_ready": len(ready_signals),
        "emergency_stopped": is_stopped,
        "stop_reason": stop_reason,
        "kill_switch_limits": {
            "max_trade_usd": ks.MAX_TRADE_USD,
            "max_daily_loss_pct": ks.MAX_DAILY_LOSS_PCT,
            "max_open_positions": ks.MAX_OPEN_POSITIONS,
            "min_win_rate": ks.MIN_WIN_RATE,
            "min_resolved_hard": ks.MIN_RESOLVED_HARD,
            "preferred_min_resolved": ks.PREFERRED_MIN_RESOLVED,
            "bankroll": ks.BANKROLL,
        },
    }


def read_live_trades(limit: int = 50) -> dict[str, Any]:
    """Recent rows from the live_trades audit table."""
    from trading_platform.polymarket.db_connection import get_connection
    db = _get_wallet_db()
    if not db:
        return {"available": False, "trades": []}
    try:
        conn = get_connection(str(db._path))
        try:
            rows = conn.execute(
                """SELECT id, attempted_at, signal_type, condition_id, question,
                          direction, confidence, size_usd, entry_price,
                          order_id, status, dry_run, error_msg
                   FROM live_trades
                   ORDER BY attempted_at DESC LIMIT ?""",
                (int(limit),),
            ).fetchall()
        finally:
            conn.close()
    except Exception:
        return {"available": True, "trades": [], "count": 0}
    cols = ["id", "attempted_at", "signal_type", "condition_id", "question",
            "direction", "confidence", "size_usd", "entry_price",
            "order_id", "status", "dry_run", "error_msg"]
    return {
        "available": True,
        "count": len(rows),
        "trades": [dict(zip(cols, r)) for r in rows],
    }


_SIGNAL_BANKROLL_MAP: dict[str, int] = {
    "wallet_reversal": 18_000, "cascade": 15_000, "oversized_bet": 15_000,
    "accumulation": 12_000, "market_maker_flip": 12_000, "convergence": 12_000,
    "price_velocity": 10_000, "specialist_entry": 8_000, "whale_exit": 8_000,
    "no_position_entry": 8_000, "pre_deadline_surge": 5_000,
    "position_reduction": 5_000, "whale_entry": 3_000,
}


def read_signals_performance() -> dict[str, Any]:
    """Per-signal performance computed from polymarket_paper_trades.

    Reports on the **paper trades** that signals actually placed (the
    ground truth for paper-trading P&L), not on raw signal-fire counts.
    Returns per-signal stats including win rate, profit factor, max
    drawdown, equity curve and recent trades, plus a portfolio summary.
    """
    from trading_platform.polymarket.db_connection import get_connection
    db = _get_wallet_db()
    if not db:
        return {"available": False, "by_type": [], "portfolio": {}}

    try:
        conn = get_connection(str(db._path))
        try:
            resolved_rows = conn.execute(
                """SELECT signal_type, side, entry_price, exit_price, size_usd,
                          realized_pnl, return_pct, outcome, entry_ts, exit_ts,
                          condition_id, question
                   FROM polymarket_paper_trades
                   WHERE archived = 0 AND exit_ts IS NOT NULL
                     AND realized_pnl IS NOT NULL
                   ORDER BY entry_ts ASC"""
            ).fetchall()
            open_rows = conn.execute(
                """SELECT signal_type, COUNT(*), SUM(size_usd)
                   FROM polymarket_paper_trades
                   WHERE archived = 0 AND exit_ts IS NULL
                   GROUP BY signal_type"""
            ).fetchall()
        finally:
            conn.close()
    except Exception as exc:
        return {"available": False, "error": str(exc), "by_type": []}

    rcols = ["signal_type", "side", "entry_price", "exit_price", "size_usd",
             "realized_pnl", "return_pct", "outcome", "entry_ts", "exit_ts",
             "condition_id", "question"]
    resolved = [dict(zip(rcols, r)) for r in resolved_rows]
    open_by_type: dict[str, dict[str, float]] = {
        r[0] or "": {"n_open": int(r[1] or 0), "deployed": float(r[2] or 0)}
        for r in open_rows
    }

    by_type: list[dict[str, Any]] = []
    for sig_type, allocated in _SIGNAL_BANKROLL_MAP.items():
        group = [t for t in resolved if (t.get("signal_type") or "") == sig_type]
        n_resolved = len(group)
        opened = open_by_type.get(sig_type, {"n_open": 0, "deployed": 0.0})

        wins = sum(1 for t in group if (t.get("outcome") or "") == "win")
        losses = n_resolved - wins
        win_rate = round(wins / n_resolved, 3) if n_resolved >= 3 else None

        cum_pnl = round(sum(float(t.get("realized_pnl") or 0) for t in group), 2)
        win_pnls = [float(t.get("realized_pnl") or 0) for t in group if (t.get("outcome") or "") == "win"]
        loss_pnls = [float(t.get("realized_pnl") or 0) for t in group if (t.get("outcome") or "") == "loss"]
        avg_win = round(sum(win_pnls) / len(win_pnls), 2) if win_pnls else None
        avg_loss = round(sum(loss_pnls) / len(loss_pnls), 2) if loss_pnls else None

        profit_factor = None
        if avg_loss is not None and losses > 0 and avg_win is not None and wins > 0:
            denom = abs(avg_loss * losses)
            if denom > 0:
                profit_factor = round((avg_win * wins) / denom, 2)

        max_drawdown = None
        if n_resolved >= 3:
            cum = 0.0
            peak = 0.0
            min_dd = 0.0
            for t in group:
                cum += float(t.get("realized_pnl") or 0)
                peak = max(peak, cum)
                dd = cum - peak
                if dd < min_dd:
                    min_dd = dd
            max_drawdown = round(min_dd, 2)

        avg_return_pct = (
            round(sum(float(t.get("return_pct") or 0) for t in group) / n_resolved, 2)
            if n_resolved else None
        )

        equity_curve: list[dict[str, Any]] = []
        if n_resolved >= 2:
            cum = 0.0
            for t in group:
                cum += float(t.get("realized_pnl") or 0)
                equity_curve.append({"ts": int(t.get("exit_ts") or 0), "pnl": round(cum, 2)})

        recent: list[dict[str, Any]] = []
        for t in group[-5:]:
            try:
                recent.append({
                    "entry_ts": int(t.get("entry_ts") or 0),
                    "exit_ts": int(t.get("exit_ts") or 0),
                    "question": (t.get("question") or "")[:80],
                    "side": t.get("side"),
                    "entry_price": round(float(t.get("entry_price") or 0), 4),
                    "exit_price": round(float(t.get("exit_price") or 0), 4),
                    "size_usd": round(float(t.get("size_usd") or 0), 2),
                    "pnl": round(float(t.get("realized_pnl") or 0), 2),
                    "return_pct": round(float(t.get("return_pct") or 0), 2),
                    "outcome": t.get("outcome"),
                    "condition_id": t.get("condition_id"),
                })
            except Exception:
                continue

        if n_resolved == 0:
            status = "no_data"
        elif n_resolved < 10:
            status = "building"
        elif n_resolved < 50:
            status = "active"
        else:
            status = "mature"

        by_type.append({
            "signal_type": sig_type,
            "allocated": allocated,
            "n_resolved": n_resolved,
            "n_open": opened["n_open"],
            "deployed_usd": round(opened["deployed"], 2),
            "wins": wins,
            "losses": losses,
            "win_rate": win_rate,
            "cum_pnl": cum_pnl,
            "avg_win": avg_win,
            "avg_loss": avg_loss,
            "profit_factor": profit_factor,
            "max_drawdown": max_drawdown,
            "avg_return_pct": avg_return_pct,
            "equity_curve": equity_curve,
            "recent_trades": recent,
            "status": status,
        })

    by_type.sort(key=lambda x: (x["cum_pnl"], x["n_resolved"]), reverse=True)

    total_resolved = len(resolved)
    total_pnl = round(sum(float(t.get("realized_pnl") or 0) for t in resolved), 2)
    total_wins = sum(1 for t in resolved if (t.get("outcome") or "") == "win")
    overall_wr = round(total_wins / total_resolved, 3) if total_resolved >= 3 else None
    total_open = sum(v["n_open"] for v in open_by_type.values())
    total_deployed = round(sum(v["deployed"] for v in open_by_type.values()), 2)

    return {
        "available": True,
        "by_type": by_type,
        "portfolio": {
            "bankroll": 100_000,
            "total_resolved": total_resolved,
            "total_open": total_open,
            "total_deployed": total_deployed,
            "total_pnl": total_pnl,
            "overall_win_rate": overall_wr,
            "pnl_pct": round(total_pnl / 100_000 * 100, 2),
        },
    }


def read_smart_money_alerts(*, limit: int = 50, wallet: str | None = None,
                            tier: int | None = None) -> dict[str, Any]:
    """Read recent alerts from the JSONL alert log."""
    try:
        from trading_platform.polymarket.alert_log import AlertLog
        log = AlertLog(DATA_ROOT / "polymarket" / "alerts.jsonl")
        alerts = log.query(limit=limit, wallet=wallet, tier=tier)
        return {"available": True, "data": alerts, "count": len(alerts)}
    except Exception as exc:
        return {"available": False, "reason": str(exc), "data": []}


def read_smart_money_open_positions() -> dict[str, Any]:
    """Read precomputed open positions from parquet."""
    path = DATA_ROOT / "polymarket" / "wallet_open_positions.parquet"
    if not path.exists():
        return {"available": False, "reason": "No open positions file. Run: compute-open-positions", "data": []}
    try:
        import pandas as _pd
        df = _pd.read_parquet(path)
        rows = []
        for _, r in df.head(200).iterrows():
            rows.append({k: _safe(v) for k, v in r.to_dict().items()})
        return {"available": True, "data": rows, "count": len(rows)}
    except Exception as exc:
        return {"available": False, "reason": str(exc), "data": []}


_WHALE_SIGNAL_TYPES = [
    "wallet_reversal", "cascade", "oversized_bet", "accumulation",
    "market_maker_flip", "convergence", "specialist_entry",
    "pre_deadline_surge", "whale_entry",
]


def read_paper_dashboard() -> dict[str, Any]:
    """Comprehensive paper trading dashboard from Postgres polymarket_paper_trades."""
    from trading_platform.polymarket.db_connection import db as _db
    try:
        with _db() as conn:
            starting_bankroll = 10_000

            open_rows = conn.execute("""
                SELECT id, condition_id, question, category, side, entry_price,
                       size_usd, signal_type, confidence, wallet, entry_ts
                FROM polymarket_paper_trades
                WHERE exit_ts IS NULL AND archived = 0
                ORDER BY entry_ts DESC
            """).fetchall()
            positions = []
            deployed = 0.0
            for r in open_rows:
                positions.append({
                    "id": r[0], "ticker": r[1], "side": r[4],
                    "entry_price": r[5], "size_usd": r[6],
                    "signal_type": r[7] or "unknown", "confidence": r[8],
                    "entry_ts": r[10], "platform": "polymarket",
                    "full_token_id": r[1], "question": r[2],
                    "category": r[3], "wallet": r[9],
                })
                deployed += (r[6] or 0)

            attr_rows = conn.execute("""
                SELECT signal_type,
                       COUNT(*) AS total,
                       SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END) AS wins,
                       SUM(CASE WHEN exit_ts IS NOT NULL THEN 1 ELSE 0 END) AS closed,
                       AVG(size_usd) AS avg_stake,
                       SUM(COALESCE(realized_pnl, 0)) AS total_pnl
                FROM polymarket_paper_trades WHERE archived = 0
                GROUP BY signal_type ORDER BY total_pnl DESC
            """).fetchall()
            attribution = []
            for r in attr_rows:
                closed = r[3] or 0
                attribution.append({
                    "signal_type": r[0], "total_trades": r[1],
                    "wins": r[2] or 0, "closed": closed,
                    "win_rate": round((r[2] or 0) / closed, 3) if closed > 0 else 0.0,
                    "avg_stake": round(r[4], 2) if r[4] else 0.0,
                    "total_pnl": round(r[5], 2) if r[5] else 0.0,
                })

            recent_rows = conn.execute("""
                SELECT condition_id, side, entry_price, exit_price, size_usd,
                       signal_type, outcome, return_pct, exit_ts, question,
                       realized_pnl, exit_reason
                FROM polymarket_paper_trades
                WHERE exit_ts IS NOT NULL AND archived = 0
                ORDER BY exit_ts DESC LIMIT 20
            """).fetchall()
            recent = []
            for r in recent_rows:
                recent.append({
                    "ticker": r[0], "side": r[1], "entry_price": r[2],
                    "exit_price": r[3], "size_usd": r[4],
                    "signal_type": r[5], "outcome": r[6],
                    "return_pct": r[7], "exit_ts": r[8],
                    "platform": "polymarket", "question": r[9],
                    "realized_pnl": r[10], "exit_reason": r[11],
                })

            total_realized = conn.execute(
                "SELECT COALESCE(SUM(realized_pnl), 0) FROM polymarket_paper_trades WHERE archived=0 AND exit_ts IS NOT NULL"
            ).fetchone()[0]

        cash = round(starting_bankroll - deployed, 2)
        return {
            "available": True,
            "starting_bankroll": starting_bankroll,
            "polymarket_cash": cash,
            "polymarket_open_count": len(positions),
            "polymarket_deployed": round(deployed, 2),
            "total_trades": len(positions) + len(recent),
            "open_trades": len(positions),
            "portfolio": {
                "cash": cash,
                "total_value": cash + deployed,
                "realized_pnl": round(float(total_realized), 2),
            },
            "positions": positions,
            "attribution": attribution,
            "signal_attribution": attribution,
            "recent_closed": recent,
            "platforms": {"polymarket": {"total": len(positions) + len(recent), "wins": sum(1 for r in recent if r["outcome"] == "win"), "open": len(positions)}},
        }
    except Exception as exc:
        return {"available": False, "reason": str(exc)}


# ── Paper trading ────────────────────────────────────────────────────────────


def read_paper_portfolio() -> dict[str, Any]:
    """Portfolio summary — non-archived only with $100K Polymarket bankroll."""
    return read_paper_dashboard()


def read_paper_trades() -> dict[str, Any]:
    """Recent paper trades from Postgres polymarket_paper_trades."""
    from trading_platform.polymarket.db_connection import db as _db
    try:
        with _db() as conn:
            rows = conn.execute("""
                SELECT id, condition_id, question, category, side, entry_price,
                       exit_price, size_usd, signal_type, confidence, wallet,
                       entry_ts, exit_ts, outcome, realized_pnl, exit_reason
                FROM polymarket_paper_trades
                WHERE archived = 0
                ORDER BY entry_ts DESC LIMIT 50
            """).fetchall()
            cols = ["id", "condition_id", "question", "category", "side",
                    "entry_price", "exit_price", "size_usd", "signal_type",
                    "confidence", "wallet", "entry_ts", "exit_ts", "outcome",
                    "realized_pnl", "exit_reason"]
            trades = [dict(zip(cols, r)) for r in rows]
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


def read_latest_replay_comparison_summary() -> dict[str, Any]:
    summary_path = _research_replay_root() / "comparison" / "latest_replay_comparison_summary.json"
    summary = _read_json(summary_path)
    if not summary:
        return {"available": False, "reason": "No replay comparison summary found"}
    return {"available": True, **summary}


def read_replay_comparison_preview(
    *,
    evaluation_summary_paths: list[str] | None = None,
    dataset_keys: list[str] | None = None,
    providers: list[str] | None = None,
    dataset_names: list[str] | None = None,
    symbols: list[str] | None = None,
    intervals: list[str] | None = None,
    start: str | None = None,
    end: str | None = None,
    alignment_modes: list[str] | None = None,
    anchor_dataset_key: str | None = None,
    tolerance: str | None = None,
    limit: int | None = None,
    feature_columns: list[str] | None = None,
    target_columns: list[str] | None = None,
    comparison_mode: str = "provider",
    min_row_count: int = 25,
    max_candidates: int = 10,
) -> dict[str, Any]:
    registry_path = _research_registry_path()
    if not list(evaluation_summary_paths or []) and not registry_path.exists():
        return {"available": False, "reason": "No shared dataset registry found"}
    try:
        result = run_replay_comparison(
            ReplayComparisonRequest(
                registry_path=registry_path if not list(evaluation_summary_paths or []) else None,
                evaluation_summary_paths=list(evaluation_summary_paths or []),
                dataset_keys=list(dataset_keys or []),
                providers=list(providers or []),
                dataset_names=list(dataset_names or []),
                symbols=list(symbols or []),
                intervals=list(intervals or []),
                start=start,
                end=end,
                alignment_modes=list(alignment_modes or ["outer_union"]),
                anchor_dataset_key=anchor_dataset_key,
                tolerance=tolerance,
                limit=limit,
                feature_columns=list(feature_columns or []),
                target_columns=list(target_columns or []),
                comparison_mode=comparison_mode,
                min_row_count=min_row_count,
                max_candidates=max_candidates,
            )
        )
    except (KeyError, ValueError, FileNotFoundError) as exc:
        return {"available": False, "reason": str(exc)}
    return {"available": True, **result.to_summary()}


def read_latest_research_gating_summary() -> dict[str, Any]:
    summary_path = _research_replay_gating_root() / "latest_research_gating_summary.json"
    summary = _read_json(summary_path)
    if not summary:
        return {"available": False, "reason": "No research gating summary found"}
    return {"available": True, **summary}


def read_replay_history_view(
    *,
    candidate_id: str | None = None,
    provider: str | None = None,
    limit: int = 50,
) -> dict[str, Any]:
    history_path = _research_replay_history_root() / "shared_replay_history.jsonl"
    records = load_replay_history(history_path)
    filtered = filter_replay_history(
        records,
        candidate_id=candidate_id,
        provider=provider,
        limit=limit,
    )
    return {
        "available": bool(records),
        "history_path": str(history_path),
        "total_record_count": len(records),
        "returned_record_count": len(filtered),
        "candidate_id": candidate_id,
        "provider": provider,
        "records": [record.to_dict() for record in filtered],
    }


def read_latest_replay_review_queue_summary() -> dict[str, Any]:
    summary_path = _research_replay_review_root() / "latest_review_queue_summary.json"
    summary = _read_json(summary_path)
    if not summary:
        return {"available": False, "reason": "No replay review queue summary found"}
    return {"available": True, **summary}


def read_latest_replay_drift_summary() -> dict[str, Any]:
    summary_path = _research_replay_review_root() / "latest_replay_drift_summary.json"
    summary = _read_json(summary_path)
    if not summary:
        return {"available": False, "reason": "No replay drift summary found"}
    return {"available": True, **summary}


# ── Polymarket whale monitoring ──────────────────────────────────────────────


def read_polymarket_whale_feed() -> dict[str, Any]:
    """Recent whale alerts for the live feed."""
    try:
        db = _get_wallet_db()
        if not db:
            return {"available": False, "reason": "Wallet DB not available", "data": []}
        alerts = db.get_alerts(limit=50)
        import time
        now = int(time.time())
        data = []
        for a in alerts:
            fired_at = a.get("detected_at") or a.get("trade_ts") or 0
            age_secs = max(now - fired_at, 0) if fired_at else 0
            if age_secs < 60:
                time_ago = f"{age_secs}s ago"
            elif age_secs < 3600:
                time_ago = f"{age_secs // 60}m ago"
            elif age_secs < 86400:
                time_ago = f"{age_secs // 3600}h ago"
            else:
                time_ago = f"{age_secs // 86400}d ago"

            data.append({
                "wallet": (a.get("wallet") or "")[:10] + "...",
                "wallet_full": a.get("wallet", ""),
                "condition_id": a.get("token_id", ""),
                "question": a.get("question") or a.get("market_title", ""),
                "category": a.get("category", "other"),
                "side": a.get("side", ""),
                "price": a.get("price"),
                "size": a.get("size"),
                "tier": a.get("tier"),
                "directional_win_rate": a.get("directional_win_rate"),
                "fired_at": fired_at,
                "signal_fired": bool(a.get("signal_fired") or a.get("paper_trade_fired")),
                "time_ago": time_ago,
            })
        return {"available": True, "count": len(data), "data": data}
    except Exception as exc:
        return {"available": False, "reason": str(exc), "data": []}


def read_polymarket_subscription_status() -> dict[str, Any]:
    """Read WebSocket subscription status from ws_status.json."""
    ws_path = DATA_ROOT / "polymarket" / "ws_status.json"
    data = _read_json(ws_path)
    if not data:
        return {
            "available": True,
            "connected": False,
            "markets_subscribed": 0,
            "watched_wallets": 0,
            "tier1_wallets": 0,
            "tier2_wallets": 0,
            "signals_today": 0,
            "last_event_ts": 0,
            "categories": {},
        }
    import time
    age = time.time() - (data.get("written_at") or 0)
    data["connected"] = age < 120  # Stale if >2 minutes
    data["available"] = True
    return data


def read_polymarket_signals_feed() -> dict[str, Any]:
    """Recent signals for the signal feed panel."""
    try:
        db = _get_wallet_db()
        if not db:
            return {"available": False, "reason": "Wallet DB not available", "data": []}
        from trading_platform.polymarket.whale_signal_engine import WhaleSignalEngine
        engine = WhaleSignalEngine(db=db)
        signals = engine.get_recent_signals(hours=24.0)
        import time
        now = int(time.time())

        # Check which signals had paper trades (may fail on NTFS — not critical)
        open_tickers = set()
        try:
            from trading_platform.polymarket.polymarket_paper_executor import PolymarketPaperExecutor
            with PolymarketPaperExecutor() as executor:
                with executor._lock:
                    rows = executor._conn.execute(
                        "SELECT ticker FROM trades WHERE platform='polymarket' AND status='open'"
                    ).fetchall()
                open_tickers = {r[0] for r in rows}
        except Exception:
            pass

        data = []
        for s in signals[:50]:
            fired_at = s.get("fired_at") or s.get("computed_at") or 0
            age_secs = max(now - fired_at, 0) if fired_at else 0
            if age_secs < 60:
                time_ago = f"{age_secs}s ago"
            elif age_secs < 3600:
                time_ago = f"{age_secs // 60}m ago"
            elif age_secs < 86400:
                time_ago = f"{age_secs // 3600}h ago"
            else:
                time_ago = f"{age_secs // 86400}d ago"

            cid = s.get("condition_id") or s.get("token_id", "")
            ticker = cid[:20]
            data.append({
                "fired_at": fired_at,
                "signal_type": s.get("signal_type", "smart_money"),
                "category": s.get("category", "other"),
                "question": (s.get("market_title") or "")[:60],
                "direction": s.get("direction", ""),
                "confidence": s.get("confidence"),
                "size": s.get("size") or s.get("net_smart_volume"),
                "wallet": (s.get("wallet") or s.get("top_wallet_address") or "")[:10] + "...",
                "executed": ticker in open_tickers,
                "stake": None,
                "time_ago": time_ago,
            })
        return {"available": True, "count": len(data), "data": data}
    except Exception as exc:
        return {"available": False, "reason": str(exc), "data": []}


def read_polymarket_category_performance() -> dict[str, Any]:
    """Category-level signal performance."""
    try:
        db = _get_wallet_db()
        if not db:
            raise Exception("Wallet DB not available")
        rows = db.get_category_performance()
        return {"available": True, "data": rows}
    except Exception as exc:
        return {"available": False, "reason": str(exc), "data": []}


def read_intelligence_health() -> dict[str, Any]:
    """Complete system health for the dashboard."""
    import time as _time

    result: dict[str, Any] = {"available": True}

    # Intelligence pipeline health
    try:
        db = _get_wallet_db()
        if not db:
            raise Exception("Wallet DB not available")
        now = int(_time.time())

        # Leaderboard status
        with db._lock:
            meta = db._conn.execute(
                "SELECT current_version, wallets_tier1, wallets_tier2, built_at, is_valid FROM leaderboard_meta WHERE id=1"
            ).fetchone()

        if meta:
            built_at = meta[3] or 0
            result["intelligence"] = {
                "leaderboard_version": meta[0],
                "wallets_tier1": meta[1],
                "wallets_tier2": meta[2],
                "leaderboard_age_hours": round((now - built_at) / 3600, 1) if built_at else None,
                "leaderboard_valid": bool(meta[4]),
            }
        else:
            result["intelligence"] = {"leaderboard_version": 0, "wallets_tier1": 0, "wallets_tier2": 0}

        # Last pipeline run
        runs_path = DATA_ROOT / "polymarket" / "pipeline_runs.jsonl"
        if runs_path.exists():
            lines = runs_path.read_text(encoding="utf-8").strip().split("\n")
            if lines:
                import json as _json
                last_run = _json.loads(lines[-1])
                result["intelligence"]["last_run_at"] = last_run.get("completed_at")
                result["intelligence"]["last_run_success"] = last_run.get("success", False)
    except Exception as exc:
        result["intelligence"] = {"error": str(exc)}

    # Monitor health
    ws_path = DATA_ROOT / "polymarket" / "ws_status.json"
    ws_data = _read_json(ws_path) or {}
    written_at = ws_data.get("written_at", 0)
    result["monitor"] = {
        "connected": bool(ws_data.get("connected") and (_time.time() - written_at < 120)),
        "markets_subscribed": ws_data.get("markets_subscribed", 0),
        "last_event_age_minutes": round((_time.time() - written_at) / 60, 1) if written_at else None,
        "signals_today": ws_data.get("signals_today", 0),
    }

    # Paper trading health. 2026-04-30: was reading the legacy
    # data/kalshi/paper_trades.db (a stale SQLite) and falling back to
    # a hard-coded $500 — producing a ladder/intelligence bankroll
    # disagreement ($356 vs $500) for any caller. Now reads the same
    # canonical source as /api/ladder/status (bankroll module +
    # polymarket_paper_trades open positions).
    try:
        from trading_platform.polymarket.bankroll import get_bankroll
        from trading_platform.polymarket.db_connection import get_connection
        bankroll = float(get_bankroll() or 0.0)
        try:
            conn = get_connection()
            open_count = conn.execute(
                """SELECT COUNT(*) FROM polymarket_paper_trades
                    WHERE archived = 0 AND exit_ts IS NULL"""
            ).fetchone()[0]
            try: conn.close()
            except Exception: pass
        except Exception:
            open_count = 0
        result["paper_trading"] = {
            "open_positions": int(open_count or 0),
            "bankroll_current": round(bankroll, 2),
        }
    except Exception:
        result["paper_trading"] = {"open_positions": 0, "bankroll_current": None}

    # Category status
    try:
        db = _get_wallet_db()
        if not db:
            raise Exception("Wallet DB not available")
        cats = db.get_category_performance()
        result["categories"] = [
            {
                "name": c.get("category", "?"),
                "signals_resolved": c.get("signals_resolved", 0),
                "win_rate": c.get("win_rate"),
                "status": (
                    "live_candidate" if (c.get("win_rate") or 0) > 0.58 and (c.get("signals_resolved") or 0) >= 50
                    else "underperforming" if (c.get("win_rate") or 0) < 0.45 and (c.get("signals_resolved") or 0) >= 20
                    else "testing"
                ),
            }
            for c in cats
        ]
    except Exception:
        result["categories"] = []

    # Live readiness gates
    paper = result.get("paper_trading", {})
    cats_with_edge = sum(
        1 for c in result.get("categories", [])
        if c.get("status") == "live_candidate"
    )
    result["live_readiness"] = {
        "gate_1_resolved_trades": {"required": 50, "current": 0, "passed": False},
        "gate_2_categories_with_edge": {"required": 2, "current": cats_with_edge, "passed": cats_with_edge >= 2},
        "gate_3_max_drawdown": {"required": 0.20, "current": 0.0, "passed": False},
        "gate_4_human_approval": {"passed": False},
        "gate_5_capital_allocated": {"passed": False},
        "all_passed": False,
    }

    return result


_POLICY_PATH = DATA_ROOT / "system" / "execution_policy.json"
_VALID_POLICIES = {"monitor_only", "paper_t1", "paper_t1t2", "live_t1"}


def read_execution_policy() -> dict[str, Any]:
    data = _read_json(_POLICY_PATH)
    return {"available": True, "policy": (data or {}).get("policy", "paper_t1")}


def write_execution_policy(policy: str) -> dict[str, Any]:
    if policy not in _VALID_POLICIES:
        return {"success": False, "error": f"Invalid policy: {policy}. Must be one of {_VALID_POLICIES}"}
    _POLICY_PATH.parent.mkdir(parents=True, exist_ok=True)
    import json as _j
    _POLICY_PATH.write_text(_j.dumps({"policy": policy}), encoding="utf-8")
    return {"success": True, "policy": policy}


# ── Pipeline monitoring ──────────────────────────────────────────────────────


_SYSTEM_DIR = DATA_ROOT / "system"


def read_pipeline_status() -> dict[str, Any]:
    """Read pipeline + monitor status, compute overall health."""
    import time as _t
    now = int(_t.time())

    # Pipeline status
    pipeline_data = _read_json(_SYSTEM_DIR / "pipeline_status.json") or {}
    pipeline_last = pipeline_data.get("started_at") or 0
    pipeline_next = pipeline_data.get("next_run_at") or 0
    pipeline_age = round((now - pipeline_last) / 60, 1) if pipeline_last else None

    pipeline = {
        "last_run_at": pipeline_last or None,
        "last_run_success": pipeline_data.get("success"),
        "last_run_duration": pipeline_data.get("duration_seconds"),
        "age_minutes": pipeline_age,
        "next_run_at": pipeline_next or None,
        "overdue": bool(pipeline_next and now > pipeline_next + 1800),
        "steps": pipeline_data.get("steps", {}),
    }

    # Monitor status
    monitor_data = _read_json(_SYSTEM_DIR / "monitor_status.json") or {}
    ws = monitor_data.get("websocket", {})
    ws_age = now - (ws.get("last_event_ts") or now)

    monitor = {
        "running": bool(monitor_data.get("last_heartbeat") and now - monitor_data["last_heartbeat"] < 120),
        "uptime_seconds": now - (monitor_data.get("started_at") or now),
        "restarts_today": monitor_data.get("restarts_today", 0),
        "websocket_connected": ws.get("connected", False),
        "websocket_age_seconds": ws_age,
        "websocket_stale": ws_age > 300,
        "tier1_poll_age_seconds": (monitor_data.get("tier1_poll", {}).get("last_poll_age_seconds")),
        "tier1_poll_overdue": bool(
            monitor_data.get("tier1_poll", {}).get("last_poll_age_seconds")
            and monitor_data["tier1_poll"]["last_poll_age_seconds"] > 1200
        ),
        "signals_fired_today": (monitor_data.get("signals", {}).get("fired_today", 0)),
    }

    # File ages
    def _file_age_hours(path: Path) -> float | None:
        if not path.exists():
            return None
        return round((_t.time() - path.stat().st_mtime) / 3600, 2)

    uni_age = _file_age_hours(DATA_ROOT / "polymarket" / "market_universe.json")
    lb_age = None
    lb_ver = 0
    try:
        db = _get_wallet_db()
        if not db:
            raise Exception("Wallet DB not available")
        with db._lock:
            meta = db._conn.execute("SELECT current_version, built_at FROM leaderboard_meta WHERE id=1").fetchone()
        if meta:
            lb_ver = meta[0]
            lb_age = round((now - (meta[1] or now)) / 3600, 2) if meta[1] else None
    except Exception:
        pass

    files = {
        "market_universe_age_hours": uni_age,
        "market_universe_stale": bool(uni_age and uni_age > 7),
        "leaderboard_version": lb_ver,
        "leaderboard_age_hours": lb_age,
        "leaderboard_stale": bool(lb_age and lb_age > 8),
    }

    # Telegram
    telegram_configured = bool(os.environ.get("TELEGRAM_BOT_TOKEN") and os.environ.get("TELEGRAM_CHAT_ID"))
    alerts = {
        "telegram_configured": telegram_configured,
        "telegram_healthy": telegram_configured,
        "last_alert_at": None,
    }

    # Overall health
    if monitor.get("websocket_stale") or (pipeline_age and pipeline_age > 480):
        health = "critical"
    elif pipeline.get("overdue") or monitor.get("tier1_poll_overdue"):
        health = "degraded"
    else:
        health = "healthy"

    return {
        "pipeline": pipeline,
        "monitor": monitor,
        "files": files,
        "alerts": alerts,
        "overall_health": health,
    }


def read_pipeline_runs() -> list[dict[str, Any]]:
    """Read last 20 pipeline runs from JSONL."""
    runs_path = _SYSTEM_DIR / "pipeline_runs.jsonl"
    if not runs_path.exists():
        # Fallback to old location
        runs_path = DATA_ROOT / "polymarket" / "pipeline_runs.jsonl"
    if not runs_path.exists():
        return []
    try:
        lines = runs_path.read_text(encoding="utf-8").strip().split("\n")
        runs = []
        for line in lines[-20:]:
            if line.strip():
                runs.append(json.loads(line))
        runs.reverse()
        return runs
    except Exception:
        return []
