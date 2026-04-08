"""
Polymarket wallet-following backtester.

Simulates following a single wallet's trades with configurable delay
and slippage. Reads from wallet_market_positions and market_price_history.
Pure read + compute — never writes to DB.

Usage::

    from trading_platform.polymarket.backtester import Backtester, BacktestConfig
    config = BacktestConfig(wallet="0x...", start_date="2025-10-01", end_date="2026-04-07")
    result = Backtester().run(config, db_path)
"""
from __future__ import annotations

import datetime
import logging
import sqlite3
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class BacktestConfig:
    wallet: str
    start_date: str = "2025-01-01"
    end_date: str = "2026-04-07"
    delay_seconds: int = 300
    slippage_pct: float = 0.02
    starting_bankroll: float = 100_000
    stake_per_trade_pct: float = 0.02
    max_open_positions: int = 20
    min_position_size: float = 25
    categories: list[str] | None = None
    signal_types: list[str] | None = None


class Backtester:
    """Simulate wallet-following strategy with delay + slippage."""

    def run(self, config: BacktestConfig, db_path: str) -> dict[str, Any]:
        try:
            return self._run(config, db_path)
        except Exception as exc:
            logger.warning("backtest failed: %s", exc)
            return {"error": str(exc), "total_trades": 0, "trades": []}

    def _run(self, config: BacktestConfig, db_path: str) -> dict[str, Any]:
        conn = sqlite3.connect(db_path)
        try:
            start_ts = int(datetime.datetime.strptime(config.start_date, "%Y-%m-%d").timestamp())
            end_ts = int(datetime.datetime.strptime(config.end_date, "%Y-%m-%d").timestamp())

            query = """
                SELECT condition_id, question, category, side,
                       first_entry_ts, avg_entry_price,
                       resolution_price, realized_pnl,
                       market_resolved, end_date_ts
                FROM wallet_market_positions
                WHERE wallet = ?
                  AND market_resolved = 1
                  AND resolution_price IS NOT NULL
                  AND first_entry_ts BETWEEN ? AND ?
            """
            params: list[Any] = [config.wallet.lower(), start_ts, end_ts]
            if config.categories:
                ph = ",".join("?" * len(config.categories))
                query += f" AND category IN ({ph})"
                params.extend(config.categories)
            query += " ORDER BY first_entry_ts ASC"

            pos_rows = conn.execute(query, params).fetchall()
            pos_cols = ["cid", "question", "category", "side", "entry_ts",
                        "wallet_price", "res_price", "wallet_pnl",
                        "resolved", "end_ts"]
            positions = [dict(zip(pos_cols, p)) for p in pos_rows]

            if len(positions) < 5:
                return {
                    "error": f"Insufficient resolved positions ({len(positions)}). Minimum 5 required.",
                    "total_trades": 0,
                    "trades": [],
                    "config": self._config_summary(config),
                }

            bankroll = config.starting_bankroll
            trades: list[dict[str, Any]] = []
            open_positions: set[str] = set()
            peak_bankroll = bankroll

            for pos in positions:
                if pos["res_price"] is None:
                    continue
                if len(open_positions) >= config.max_open_positions:
                    continue

                our_entry_ts = (pos["entry_ts"] or 0) + config.delay_seconds
                wallet_price = pos["wallet_price"] or 0.5

                # Apply slippage to wallet's entry price
                if pos["side"] == "NO":
                    our_price = max(wallet_price * (1 - config.slippage_pct), 0.01)
                else:
                    our_price = min(wallet_price * (1 + config.slippage_pct), 0.99)

                # Override with actual market price at our entry time if available
                market_price = self._get_price_at_ts(conn, pos["cid"], our_entry_ts)
                if market_price is not None and market_price > 0:
                    if pos["side"] == "NO":
                        our_price = max(market_price * (1 - config.slippage_pct), 0.01)
                    else:
                        our_price = min(market_price * (1 + config.slippage_pct), 0.99)

                # Stake sizing
                stake = max(
                    bankroll * config.stake_per_trade_pct,
                    config.min_position_size,
                )
                stake = min(stake, bankroll * 0.15)
                if stake < config.min_position_size or stake > bankroll:
                    continue

                # Resolution
                our_exit = pos["res_price"]
                if pos["side"] == "NO":
                    our_exit = 1.0 - pos["res_price"]

                # PnL: shares = stake / our_price, payoff = shares * (our_exit - our_price)
                pnl = (our_exit - our_price) * stake / our_price if our_price > 0 else 0
                bankroll += pnl
                peak_bankroll = max(peak_bankroll, bankroll)

                hours_before = None
                if pos.get("end_ts") and pos.get("entry_ts"):
                    hours_before = round((pos["end_ts"] - pos["entry_ts"]) / 3600, 1)

                trades.append({
                    "condition_id": pos["cid"],
                    "question": (pos["question"] or "")[:100],
                    "category": pos["category"],
                    "side": pos["side"],
                    "entry_ts": our_entry_ts,
                    "wallet_entry_price": round(wallet_price, 4),
                    "our_entry_price": round(our_price, 4),
                    "our_exit_price": round(our_exit, 4),
                    "stake": round(stake, 2),
                    "pnl": round(pnl, 2),
                    "won": pnl > 0,
                    "hours_before_close": hours_before,
                })

            # Authoritative wallet PnL: pm_pnl from leaderboard (Polymarket
            # reported) is preferred over sum of local realized_pnl, which
            # is incomplete for wallets with partial trade history.
            pm_pnl = None
            try:
                lb_row = conn.execute(
                    "SELECT pm_pnl FROM leaderboard WHERE wallet = ?",
                    (config.wallet.lower(),),
                ).fetchone()
                if lb_row and lb_row[0] is not None:
                    pm_pnl = float(lb_row[0])
            except Exception:
                pm_pnl = None

            return self._aggregate(trades, config, positions, pm_pnl=pm_pnl)
        finally:
            conn.close()

    def _get_price_at_ts(self, conn: sqlite3.Connection, condition_id: str, ts: int) -> float | None:
        row = conn.execute(
            """SELECT p FROM market_price_history
               WHERE condition_id = ? AND t <= ?
               ORDER BY t DESC LIMIT 1""",
            (condition_id, ts),
        ).fetchone()
        return row[0] if row else None

    def _config_summary(self, config: BacktestConfig) -> dict[str, Any]:
        return {
            "delay_seconds": config.delay_seconds,
            "slippage_pct": config.slippage_pct,
            "starting_bankroll": config.starting_bankroll,
            "stake_per_trade_pct": config.stake_per_trade_pct,
            "date_range": f"{config.start_date} to {config.end_date}",
        }

    def _aggregate(
        self,
        trades: list[dict],
        config: BacktestConfig,
        positions: list[dict],
        pm_pnl: float | None = None,
    ) -> dict[str, Any]:
        if not trades:
            return {
                "total_trades": 0,
                "trades": [],
                "wallet_pm_pnl": pm_pnl,
                "config": self._config_summary(config),
            }

        wins = [t for t in trades if t["won"]]
        total_pnl = sum(t["pnl"] for t in trades)
        local_wallet_pnl = sum((p.get("wallet_pnl") or 0) for p in positions)
        # Prefer Polymarket-authoritative pm_pnl when available — local
        # realized_pnl is incomplete for wallets with partial trade history.
        if pm_pnl is not None and pm_pnl > 0:
            wallet_pnl = pm_pnl
            wallet_pnl_source = "pm_authoritative"
        else:
            wallet_pnl = local_wallet_pnl
            wallet_pnl_source = "local_realized_sum"

        # Drawdown
        running = config.starting_bankroll
        peak = running
        max_dd = 0.0
        for t in sorted(trades, key=lambda x: x["entry_ts"]):
            running += t["pnl"]
            if running > peak:
                peak = running
            if peak > 0:
                dd = (peak - running) / peak
                if dd > max_dd:
                    max_dd = dd

        # Monthly + category breakdown
        monthly: dict[str, dict[str, float]] = defaultdict(lambda: {"pnl": 0.0, "trades": 0, "wins": 0})
        by_cat: dict[str, dict[str, float]] = defaultdict(lambda: {"pnl": 0.0, "trades": 0, "wins": 0})
        for t in trades:
            month = datetime.datetime.fromtimestamp(t["entry_ts"]).strftime("%Y-%m")
            monthly[month]["pnl"] += t["pnl"]
            monthly[month]["trades"] += 1
            monthly[month]["wins"] += 1 if t["won"] else 0
            cat = t["category"] or "other"
            by_cat[cat]["pnl"] += t["pnl"]
            by_cat[cat]["trades"] += 1
            by_cat[cat]["wins"] += 1 if t["won"] else 0

        pnl_values = [t["pnl"] for t in trades]
        mean_pnl = sum(pnl_values) / len(pnl_values)
        std_pnl = (sum((p - mean_pnl) ** 2 for p in pnl_values) / len(pnl_values)) ** 0.5

        alpha_capture = None
        if wallet_pnl > 0:
            alpha_capture = round(total_pnl / wallet_pnl * 100, 1)

        return {
            "total_trades": len(trades),
            "winning_trades": len(wins),
            "win_rate": round(len(wins) / len(trades), 3),
            "total_pnl": round(total_pnl, 2),
            "total_pnl_pct": round(total_pnl / config.starting_bankroll * 100, 2),
            "max_drawdown": round(max_dd, 3),
            "sharpe_ratio": round(mean_pnl / std_pnl, 3) if std_pnl > 0 else None,
            "alpha_capture_pct": alpha_capture,
            "wallet_pnl_in_period": round(wallet_pnl, 2),
            "wallet_pm_pnl": round(pm_pnl, 2) if pm_pnl is not None else None,
            "wallet_local_pnl": round(local_wallet_pnl, 2),
            "wallet_pnl_source": wallet_pnl_source,
            "by_month": {k: {kk: round(vv, 2) if isinstance(vv, float) else vv for kk, vv in v.items()}
                         for k, v in sorted(monthly.items())},
            "by_category": {k: {kk: round(vv, 2) if isinstance(vv, float) else vv for kk, vv in v.items()}
                            for k, v in by_cat.items()},
            "config": self._config_summary(config),
            "trades": trades,
        }
