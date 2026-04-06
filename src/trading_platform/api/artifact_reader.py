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


def read_smart_money_wallet_detail(address: str) -> dict[str, Any]:
    profiles_path = DATA_ROOT / "polymarket" / "wallet_profiles.parquet"
    fills_dir = DATA_ROOT / "polymarket" / "goldsky_resolved_fills"
    res_path = DATA_ROOT / "polymarket" / "gamma_resolution.csv"

    if not profiles_path.exists():
        return {"available": False, "reason": "No wallet profiles"}

    try:
        import pandas as _pd
        from trading_platform.polymarket.resolution_resolver import ResolutionResolver

        # Profile
        df = _pd.read_parquet(profiles_path)
        wallet_row = df[df["wallet"] == address]
        if wallet_row.empty:
            return {"available": False, "reason": f"Wallet {address[:16]}... not found"}
        profile = {k: _safe(v) for k, v in wallet_row.iloc[0].to_dict().items()}

        # Load resolved fills
        resolver = ResolutionResolver(res_path) if res_path.exists() else None
        trades = []
        if fills_dir.exists():
            for path in fills_dir.glob("*.parquet"):
                if path.name == "combined.parquet":
                    continue
                try:
                    fdf = _pd.read_parquet(path)
                    wallet_fills = fdf[fdf["maker_wallet"] == address]
                    if wallet_fills.empty:
                        continue
                    for _, row in wallet_fills.head(100).iterrows():
                        maker_asset = str(row.get("maker_asset_id", ""))
                        taker_asset = str(row.get("taker_asset_id", ""))
                        direction = "YES" if maker_asset == "0" else "NO"
                        token_id = taker_asset if maker_asset == "0" else maker_asset
                        rp = resolver.resolve(token_id) if resolver else None
                        won = None
                        if rp is not None:
                            won = (rp >= 99.0 and direction == "YES") or (rp < 1.0 and direction == "NO")
                        trades.append({
                            "token_id": token_id[:24],
                            "direction": direction,
                            "amount_usdc": _safe(row.get("maker_amount")),
                            "timestamp": str(row.get("timestamp", "")),
                            "resolution_price": rp,
                            "won": won,
                        })
                except Exception:
                    continue

        # Load recent alerts for this wallet
        alerts = []
        try:
            from trading_platform.polymarket.alert_log import AlertLog
            log = AlertLog(DATA_ROOT / "polymarket" / "alerts.jsonl")
            alerts = log.query(limit=50, wallet=address)
        except Exception:
            pass

        # Load open positions for this wallet
        open_pos = []
        pos_path = DATA_ROOT / "polymarket" / "wallet_open_positions.parquet"
        if pos_path.exists():
            try:
                pos_df = _pd.read_parquet(pos_path)
                wallet_pos = pos_df[pos_df["wallet"] == address]
                for _, r in wallet_pos.iterrows():
                    open_pos.append({k: _safe(v) for k, v in r.to_dict().items()})
            except Exception:
                pass

        return {
            "available": True,
            "profile": profile,
            "resolved_trades": trades[:200],
            "trade_count": len(trades),
            "alerts": alerts,
            "open_positions": open_pos,
        }
    except Exception as exc:
        return {"available": False, "reason": str(exc)}


def _get_wallet_db() -> Any:
    from trading_platform.polymarket.wallet_db import WalletDB
    db_path = DATA_ROOT / "polymarket" / "wallet_intelligence.db"
    if not db_path.exists():
        return None
    return WalletDB(db_path)


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
        rows = db.get_winners(window=window, limit=50)
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
    "wallet_reversal":    {"allocated": 1800, "stake_per_trade": 90},
    "cascade":            {"allocated": 1500, "stake_per_trade": 75},
    "oversized_bet":      {"allocated": 1500, "stake_per_trade": 75},
    "accumulation":       {"allocated": 1200, "stake_per_trade": 60},
    "market_maker_flip":  {"allocated": 1200, "stake_per_trade": 60},
    "convergence":        {"allocated": 1200, "stake_per_trade": 60},
    "specialist_entry":   {"allocated":  800, "stake_per_trade": 40},
    "pre_deadline_surge": {"allocated":  500, "stake_per_trade": 25},
    "whale_entry":        {"allocated":  300, "stake_per_trade": 15},
}


def read_smart_money_universe_stats() -> dict[str, Any]:
    db = _get_wallet_db()
    if not db:
        return {"available": False, "reason": "Wallet intelligence DB not found"}
    try:
        return {"available": True, **db.universe_stats()}
    except Exception as exc:
        return {"available": False, "reason": str(exc)}


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
            conn = sqlite3.connect(str(db_path), check_same_thread=False)
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

    return {
        "available": True,
        "total_bankroll": 10000,
        "net_pnl": round(total_pnl, 2),
        "win_rate": round(total_wins / total_resolved, 3) if total_resolved > 0 else None,
        "resolved": total_resolved,
        "open": total_open,
        "by_signal": by_signal,
    }


def read_paper_pnl_history() -> dict[str, Any]:
    """Daily P&L snapshots for charting."""
    db_path = DATA_ROOT / "kalshi" / "paper_trades.db"
    by_day: list[dict] = []
    by_signal_total: dict[str, dict] = {}

    if db_path.exists():
        try:
            import sqlite3
            conn = sqlite3.connect(str(db_path), check_same_thread=False)
            rows = conn.execute("""
                SELECT DATE(exit_ts) as d, COALESCE(signal_family, 'unknown'),
                       SUM(return_pct), COUNT(*)
                FROM trades WHERE outcome IS NOT NULL AND exit_ts IS NOT NULL
                GROUP BY d, signal_family ORDER BY d
            """).fetchall()

            day_totals: dict[str, float] = {}
            for d, sig, pnl, count in rows:
                if d:
                    day_totals[d] = day_totals.get(d, 0) + (pnl or 0)
                if sig not in by_signal_total:
                    by_signal_total[sig] = {"total_pnl": 0, "trades": 0, "wins": 0}
                by_signal_total[sig]["total_pnl"] += (pnl or 0)
                by_signal_total[sig]["trades"] += count

            cumulative = 0.0
            for d in sorted(day_totals):
                cumulative += day_totals[d]
                by_day.append({"date": d, "pnl": round(day_totals[d], 4), "cumulative_pnl": round(cumulative, 4)})

            conn.close()
        except Exception:
            pass

    return {
        "available": True,
        "has_data": len(by_day) > 0,
        "by_day": by_day,
        "by_signal_total": {k: {kk: round(vv, 4) if isinstance(vv, float) else vv for kk, vv in v.items()} for k, v in by_signal_total.items()},
        "next_resolution": "April 15, 2026 (KXCPI/KXFED markets)",
    }


def read_signals_performance() -> dict[str, Any]:
    """Signal type performance from market_signals table."""
    db = _get_wallet_db()
    by_type: list[dict] = []
    total_fired = 0
    total_resolved = 0

    for sig_type, alloc in _SIGNAL_ALLOCATIONS.items():
        entry = {
            "signal_type": sig_type,
            "allocated": alloc["allocated"],
            "stake_per_trade": alloc["stake_per_trade"],
            "fired": 0, "resolved": 0, "wins": 0,
            "win_rate": None, "avg_ev": None, "profit_factor": None,
            "cumulative_pnl": 0, "status": "building",
        }

        if db:
            try:
                with db._lock:
                    row = db._conn.execute(
                        """SELECT COUNT(*) as fired,
                                  SUM(CASE WHEN status='resolved' THEN 1 ELSE 0 END) as resolved,
                                  SUM(CASE WHEN status='resolved' AND confidence > 0.5 THEN 1 ELSE 0 END) as wins,
                                  AVG(CASE WHEN status='resolved' THEN confidence END) as avg_conf
                           FROM market_signals WHERE signal_type = ?""",
                        (sig_type,),
                    ).fetchone()
                if row:
                    entry["fired"] = row[0] or 0
                    entry["resolved"] = row[1] or 0
                    entry["wins"] = row[2] or 0
                    total_fired += entry["fired"]
                    total_resolved += entry["resolved"]
            except Exception:
                pass

        r = entry["resolved"]
        if r >= 10:
            wr = entry["wins"] / r if r > 0 else 0
            entry["win_rate"] = round(wr, 3)
            if wr >= 0.52:
                entry["status"] = "active"
            elif wr < 0.45:
                entry["status"] = "underperforming"
            else:
                entry["status"] = "monitoring"
        elif r > 0:
            entry["win_rate"] = round(entry["wins"] / r, 3)

        by_type.append(entry)

    return {
        "available": True,
        "by_type": by_type,
        "total_fired": total_fired,
        "total_resolved": total_resolved,
        "overall_ev": None,
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


def read_paper_dashboard() -> dict[str, Any]:
    """Comprehensive paper trading dashboard data."""
    db_path = DATA_ROOT / "kalshi" / "paper_trades.db"
    if not db_path.exists():
        return {"available": False, "reason": "No paper trading DB found"}
    try:
        import sqlite3
        conn = sqlite3.connect(str(db_path), check_same_thread=False)

        # Portfolio summary
        port = conn.execute("SELECT cash_usd, open_value, total_value, realized_pnl FROM portfolio ORDER BY id DESC LIMIT 1").fetchone()
        cash = float(port[0]) if port else 500.0
        total_val = float(port[2]) if port else 500.0
        realized = float(port[3]) if port else 0.0

        # Open positions
        open_trades = conn.execute("""
            SELECT id, ticker, side, entry_price, size_usd, signal_family, confidence,
                   entry_ts, COALESCE(platform, 'kalshi') as platform
            FROM trades WHERE status = 'open'
            ORDER BY entry_ts DESC
        """).fetchall()
        positions = []
        for t in open_trades:
            positions.append({
                "id": t[0], "ticker": t[1], "side": t[2],
                "entry_price": t[3], "size_usd": t[4],
                "signal_type": t[5] or "unknown", "confidence": t[6],
                "entry_ts": t[7], "platform": t[8],
            })

        # Signal attribution
        attr_rows = conn.execute("""
            SELECT COALESCE(signal_family, 'unknown') as sig,
                   COUNT(*) as total,
                   SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END) as wins,
                   SUM(CASE WHEN status='closed' THEN 1 ELSE 0 END) as closed,
                   AVG(size_usd) as avg_stake
            FROM trades
            GROUP BY sig ORDER BY total DESC
        """).fetchall()
        attribution = []
        for r in attr_rows:
            closed = r[3]
            attribution.append({
                "signal_type": r[0],
                "total_trades": r[1],
                "wins": r[2],
                "closed": closed,
                "win_rate": round(r[2] / closed, 3) if closed > 0 else 0.0,
                "avg_stake": round(r[4], 2) if r[4] else 0.0,
            })

        # Recent closed trades
        closed_trades = conn.execute("""
            SELECT ticker, side, entry_price, exit_price, size_usd,
                   signal_family, outcome, return_pct, exit_ts,
                   COALESCE(platform, 'kalshi') as platform
            FROM trades WHERE status = 'closed'
            ORDER BY exit_ts DESC LIMIT 20
        """).fetchall()
        recent = []
        for t in closed_trades:
            recent.append({
                "ticker": t[0], "side": t[1],
                "entry_price": t[2], "exit_price": t[3],
                "size_usd": t[4], "signal_type": t[5],
                "outcome": t[6], "return_pct": t[7],
                "exit_ts": t[8], "platform": t[9],
            })

        # Platform breakdown
        platform_rows = conn.execute("""
            SELECT COALESCE(platform, 'kalshi'),
                   COUNT(*), SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END),
                   SUM(CASE WHEN status='open' THEN 1 ELSE 0 END)
            FROM trades GROUP BY 1
        """).fetchall()
        platforms = {}
        for r in platform_rows:
            platforms[r[0]] = {"total": r[1], "wins": r[2] or 0, "open": r[3]}

        conn.close()

        return {
            "available": True,
            "portfolio": {"cash": cash, "total_value": total_val, "realized_pnl": realized},
            "positions": positions,
            "attribution": attribution,
            "recent_closed": recent,
            "platforms": platforms,
        }
    except Exception as exc:
        return {"available": False, "reason": str(exc)}


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
        from trading_platform.polymarket.wallet_db import WalletDB
        db = WalletDB()
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
        from trading_platform.polymarket.whale_signal_engine import WhaleSignalEngine
        engine = WhaleSignalEngine()
        signals = engine.get_recent_signals(hours=24.0)
        import time
        now = int(time.time())

        # Check which signals had paper trades
        from trading_platform.polymarket.polymarket_paper_executor import PolymarketPaperExecutor
        executor = PolymarketPaperExecutor()
        open_tickers = set()
        try:
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
        from trading_platform.polymarket.wallet_db import WalletDB
        db = WalletDB()
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
        from trading_platform.polymarket.wallet_db import WalletDB
        db = WalletDB()
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

    # Paper trading health
    try:
        import sqlite3
        paper_db = DATA_ROOT.parent / "kalshi" / "paper_trades.db"
        if paper_db.exists():
            conn = sqlite3.connect(str(paper_db))
            open_count = conn.execute(
                "SELECT COUNT(*) FROM trades WHERE platform='polymarket' AND status='open'"
            ).fetchone()[0]
            cash_row = conn.execute("SELECT cash_usd FROM portfolio ORDER BY id DESC LIMIT 1").fetchone()
            conn.close()
            result["paper_trading"] = {
                "open_positions": open_count,
                "bankroll_current": round(cash_row[0], 2) if cash_row else 500.0,
            }
        else:
            result["paper_trading"] = {"open_positions": 0, "bankroll_current": 500.0}
    except Exception:
        result["paper_trading"] = {"open_positions": 0, "bankroll_current": 500.0}

    # Category status
    try:
        from trading_platform.polymarket.wallet_db import WalletDB
        db = WalletDB()
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
