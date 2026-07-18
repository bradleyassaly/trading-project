"""
Wallet analytics computation engine.

Computes risk, edge, behavioral, and PnL dynamics metrics from
wallet_trades. Read-only — never writes to DB. The profile rebuild
pipeline calls compute_all() and merges the result into wallet_profiles.

Usage::

    from trading_platform.polymarket.wallet_analytics import WalletAnalyticsEngine
    engine = WalletAnalyticsEngine()
    metrics = engine.compute_all(wallet_address, db_path)
"""
from __future__ import annotations

import json
import logging
import math
import sqlite3
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_UNIVERSE_PATH = _PROJECT_ROOT / "data" / "polymarket" / "market_universe.json"

_MIN_TRADES = 5


class WalletAnalyticsEngine:
    """Compute analytics metrics for a single wallet."""

    def __init__(self) -> None:
        self._end_dates_cache: dict[str, int] | None = None

    def compute_all(self, wallet: str, db_path: str | Path) -> dict[str, Any]:
        """Compute all analytics metrics. Returns {} if insufficient data."""
        try:
            return self._compute(wallet, str(db_path))
        except Exception as exc:
            logger.warning("compute_all failed for %s: %s", wallet[:12], exc)
            return {}

    def _compute(self, wallet: str, db_path: str) -> dict[str, Any]:
        now = int(time.time())
        conn = sqlite3.connect(db_path)
        try:
            trade_rows = conn.execute(
                """SELECT timestamp, pnl, market_resolved, market_outcome,
                          side, price, size, condition_id, category
                   FROM wallet_trades
                   WHERE wallet = ?
                   ORDER BY timestamp ASC""",
                (wallet,),
            ).fetchall()

            cols = ["ts", "pnl", "resolved", "outcome", "side", "price", "size", "cid", "category"]
            trades = [dict(zip(cols, t)) for t in trade_rows]

            if len(trades) < _MIN_TRADES:
                return {}

            resolved = [t for t in trades if t["resolved"] == 1 and t["pnl"] is not None]
            pnls = [t["pnl"] for t in resolved]

            results: dict[str, Any] = {}

            # PnL dynamics
            results.update(self._pnl_dynamics(resolved, now))

            # Risk / volatility
            if len(pnls) >= _MIN_TRADES:
                results.update(self._risk_metrics(pnls, resolved, now))

            # Expected value / edge
            results.update(self._ev_metrics(resolved))

            # Edge vs market
            edge = self._edge_vs_market(resolved)
            if edge is not None:
                results["edge_vs_market"] = edge

            # Behavioral / timing
            results.update(self._behavioral_metrics(wallet, conn, resolved, now))

            # Category specialization
            spec = self._category_specialization(resolved)
            if spec is not None:
                results["category_specialization"] = spec

            # Open positions
            results.update(self._open_positions(wallet, conn))

            return results
        finally:
            conn.close()

    # ── PnL dynamics ─────────────────────────────────────────────────────────

    def _pnl_dynamics(self, resolved: list[dict], now: int) -> dict[str, Any]:
        out: dict[str, Any] = {}
        day7 = now - 86400 * 7
        day30 = now - 86400 * 30
        day90 = now - 86400 * 90

        pnl_7d = sum(t["pnl"] for t in resolved if t["ts"] >= day7)
        pnl_30d = sum(t["pnl"] for t in resolved if t["ts"] >= day30)
        pnl_90d = sum(t["pnl"] for t in resolved if t["ts"] >= day90)
        out["pnl_7d"] = round(pnl_7d, 2)
        out["pnl_30d"] = round(pnl_30d, 2)
        out["pnl_90d"] = round(pnl_90d, 2)

        prior_60d = pnl_90d - pnl_30d
        if abs(prior_60d) > 1:
            out["pnl_trend"] = round(pnl_30d / prior_60d, 3)

        # Consistency: rolling 30d windows that were profitable
        if len(resolved) >= 10:
            window_results = []
            for i in range(len(resolved)):
                window_start = resolved[i]["ts"]
                window_end = window_start + 86400 * 30
                window_pnl = sum(
                    t["pnl"] for t in resolved
                    if window_start <= t["ts"] <= window_end
                )
                window_results.append(window_pnl > 0)
            out["pnl_consistency"] = round(sum(window_results) / len(window_results), 3)

        # Performance decay: monthly slope
        if len(resolved) >= 20:
            earliest = resolved[0]["ts"]
            n_months = max(int((now - earliest) / (30 * 86400)), 2)
            monthly_pnls = []
            for m in range(n_months):
                ms = earliest + m * 30 * 86400
                me = ms + 30 * 86400
                monthly_pnls.append(
                    sum(t["pnl"] for t in resolved if ms <= t["ts"] < me)
                )
            if len(monthly_pnls) >= 2:
                slope = (monthly_pnls[-1] - monthly_pnls[0]) / len(monthly_pnls)
                out["performance_decay"] = round(slope, 2)

        return out

    # ── Risk / volatility ────────────────────────────────────────────────────

    def _risk_metrics(self, pnls: list[float], resolved: list[dict], now: int) -> dict[str, Any]:
        out: dict[str, Any] = {}
        n = len(pnls)
        mean_pnl = sum(pnls) / n
        variance = sum((p - mean_pnl) ** 2 for p in pnls) / n
        std_dev = variance ** 0.5
        out["pnl_volatility"] = round(std_dev, 2)

        if std_dev > 0:
            out["sharpe_ratio"] = round(mean_pnl / std_dev, 3)

        losses = [p for p in pnls if p < 0]
        if losses:
            downside_var = sum(p ** 2 for p in losses) / n
            downside_std = downside_var ** 0.5
            if downside_std > 0:
                out["sortino_ratio"] = round(mean_pnl / downside_std, 3)

        # Max drawdown from cumulative PnL
        cumulative = []
        running = 0.0
        for p in pnls:
            running += p
            cumulative.append(running)

        peak = cumulative[0]
        max_dd_usd = 0.0
        max_dd_pct = 0.0
        for c in cumulative:
            if c > peak:
                peak = c
            dd_usd = peak - c
            if dd_usd > max_dd_usd:
                max_dd_usd = dd_usd
                if abs(peak) > 0.01:
                    max_dd_pct = dd_usd / abs(peak)
        out["max_drawdown_usd"] = round(max_dd_usd, 2)
        out["max_drawdown_pct"] = round(max_dd_pct, 4)

        total_pnl = cumulative[-1] if cumulative else 0
        if max_dd_usd > 0:
            out["recovery_factor"] = round(total_pnl / max_dd_usd, 3)
        if max_dd_pct > 0:
            earliest = resolved[0]["ts"]
            years = (now - earliest) / (86400 * 365)
            if years > 0:
                annual_return = total_pnl / years
                out["calmar_ratio"] = round(
                    annual_return / (max_dd_pct * abs(total_pnl + 1)), 3
                )

        # Win/loss streaks
        max_win = max_lose = cur_win = cur_lose = 0
        for p in pnls:
            if p > 0:
                cur_win += 1
                cur_lose = 0
                max_win = max(max_win, cur_win)
            else:
                cur_lose += 1
                cur_win = 0
                max_lose = max(max_lose, cur_lose)
        out["win_streak_max"] = max_win
        out["loss_streak_max"] = max_lose

        return out

    # ── Expected value / edge ────────────────────────────────────────────────

    def _ev_metrics(self, resolved: list[dict]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        wins = [t for t in resolved if t["pnl"] > 0]
        losses = [t for t in resolved if t["pnl"] <= 0]
        if not (wins and losses and len(resolved) >= _MIN_TRADES):
            return out

        wr = len(wins) / len(resolved)
        avg_win = sum(t["pnl"] for t in wins) / len(wins)
        avg_loss = abs(sum(t["pnl"] for t in losses) / len(losses))
        ev = wr * avg_win - (1 - wr) * avg_loss
        out["expected_value"] = round(ev, 2)

        if avg_loss > 0:
            odds = avg_win / avg_loss
            kelly = (wr * odds - (1 - wr)) / odds if odds > 0 else 0
            out["kelly_fraction"] = round(max(0, min(kelly, 0.5)), 4)

        for cat in ("politics", "sports", "crypto", "economics"):
            cat_resolved = [t for t in resolved if (t.get("category") or "").lower() == cat]
            if len(cat_resolved) >= 3:
                cat_wins = [t for t in cat_resolved if t["pnl"] > 0]
                cat_losses = [t for t in cat_resolved if t["pnl"] <= 0]
                if cat_wins and cat_losses:
                    cat_wr = len(cat_wins) / len(cat_resolved)
                    caw = sum(t["pnl"] for t in cat_wins) / len(cat_wins)
                    cal = abs(sum(t["pnl"] for t in cat_losses) / len(cat_losses))
                    out[f"ev_{cat}"] = round(cat_wr * caw - (1 - cat_wr) * cal, 2)

        return out

    def _edge_vs_market(self, resolved: list[dict]) -> float | None:
        edges = []
        for t in resolved:
            if not (t.get("outcome") and t.get("price") and t.get("side")):
                continue
            try:
                entry = float(t["price"])
                outcome_yes = (t["outcome"] or "").upper() == "YES"
                if t["side"] == "BUY":
                    edge = (1.0 if outcome_yes else 0.0) - entry
                else:
                    edge = entry - (1.0 if outcome_yes else 0.0)
                edges.append(edge)
            except (TypeError, ValueError):
                continue
        if not edges:
            return None
        return round(sum(edges) / len(edges), 4)

    # ── Behavioral / timing ──────────────────────────────────────────────────

    def _load_end_dates(self) -> dict[str, int]:
        if self._end_dates_cache is not None:
            return self._end_dates_cache
        end_dates: dict[str, int] = {}
        try:
            if _UNIVERSE_PATH.exists():
                from datetime import datetime as _dt
                data = json.loads(_UNIVERSE_PATH.read_text(encoding="utf-8"))
                for cat_markets in (data.get("by_category") or {}).values():
                    for m in cat_markets:
                        cid = m.get("condition_id", "")
                        end_iso = m.get("end_date_iso", "")
                        if not cid or not end_iso:
                            continue
                        try:
                            dt = _dt.fromisoformat(end_iso.replace("Z", "+00:00"))
                            end_dates[cid] = int(dt.timestamp())
                        except Exception:
                            pass
        except Exception:
            pass
        self._end_dates_cache = end_dates
        return end_dates

    def _behavioral_metrics(self, wallet: str, conn: sqlite3.Connection,
                             resolved: list[dict], now: int) -> dict[str, Any]:
        out: dict[str, Any] = {}
        end_dates = self._load_end_dates()
        if not end_dates:
            return out

        market_ends = conn.execute(
            """SELECT DISTINCT condition_id, MAX(timestamp) as last_trade_ts
               FROM wallet_trades WHERE wallet = ?
               GROUP BY condition_id""",
            (wallet,),
        ).fetchall()

        hours_list = []
        for cid, last_ts in market_ends:
            if cid in end_dates and last_ts:
                hours = (end_dates[cid] - last_ts) / 3600
                if 0 < hours < 8760:
                    hours_list.append(hours)
        if hours_list:
            out["avg_hours_before_resolution"] = round(sum(hours_list) / len(hours_list), 1)
            out["early_entry_rate"] = round(
                sum(1 for h in hours_list if h > 48) / len(hours_list), 3
            )
        return out

    def _category_specialization(self, resolved: list[dict]) -> float | None:
        cat_wrs = []
        for cat in ("politics", "sports", "crypto", "economics", "other"):
            cat_r = [t for t in resolved if (t.get("category") or "").lower() == cat]
            if len(cat_r) >= 3:
                cat_wr = len([t for t in cat_r if t["pnl"] > 0]) / len(cat_r)
                cat_wrs.append(cat_wr)
        if len(cat_wrs) < 2:
            return None
        return round(max(cat_wrs) - sum(cat_wrs) / len(cat_wrs), 3)

    def _open_positions(self, wallet: str, conn: sqlite3.Connection) -> dict[str, Any]:
        # Prefer wallet_positions table (fresh data from positions API)
        try:
            row = conn.execute(
                """SELECT COUNT(*) as pos_count,
                          COALESCE(SUM(initial_value), 0) as total_cost,
                          COALESCE(SUM(current_value), 0) as total_current,
                          COALESCE(SUM(cash_pnl), 0) as total_unrealized,
                          COALESCE(SUM(CASE WHEN redeemable=1 THEN cash_pnl ELSE 0 END), 0) as redeemable_pnl,
                          SUM(CASE WHEN redeemable=1 THEN 1 ELSE 0 END) as redeemable_count
                   FROM wallet_positions
                   WHERE wallet = ?
                     AND COALESCE(updated_at, 0) > strftime('%s','now') - 86400""",
                (wallet,),
            ).fetchone()
        except Exception:
            row = None

        if row and row[0]:
            # Position concentration: top 3 / total
            top3 = conn.execute(
                """SELECT COALESCE(SUM(current_value), 0) FROM (
                       SELECT current_value FROM wallet_positions
                       WHERE wallet=? ORDER BY current_value DESC LIMIT 3
                   )""",
                (wallet,),
            ).fetchone()[0] or 0
            total = row[2] or 1
            concentration = min(top3 / total, 1.0) if total > 0 else None

            return {
                "open_positions_count": row[0] or 0,
                "open_positions_value": round(row[2] or 0, 2),
                "unrealized_pnl_estimate": round(row[3] or 0, 2),
                "position_concentration": round(concentration, 3) if concentration is not None else None,
            }

        # Fallback: count from wallet_trades. size is SHARES — value must
        # be size*price (USDC entry notional), not raw share count.
        fb_row = conn.execute(
            """SELECT COUNT(DISTINCT condition_id), COALESCE(SUM(size * price), 0)
               FROM wallet_trades
               WHERE wallet = ? AND market_resolved = 0""",
            (wallet,),
        ).fetchone()
        return {
            "open_positions_count": fb_row[0] or 0,
            "open_positions_value": round(fb_row[1] or 0, 2),
        }
