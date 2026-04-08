"""
Polymarket paper trade executor.

Two execution paths:
  1. on_signal() / execute_trade() — legacy path, writes to kalshi/paper_trades.db
     with platform='polymarket' (preserved for back-compat).
  2. execute_signal() — new path, writes to wallet_intelligence.db
     polymarket_paper_trades table with $100K bankroll and SIGNAL_BANKROLL
     allocations per signal type.

Usage::

    from trading_platform.polymarket.polymarket_paper_executor import PolymarketPaperExecutor
    executor = PolymarketPaperExecutor()
    executor.execute_signal(signal_dict)
"""
from __future__ import annotations

import logging
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_STARTING_CASH = 500.0
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_WALLET_DB_PATH = _PROJECT_ROOT / "data" / "polymarket" / "wallet_intelligence.db"

SIGNAL_BANKROLL = {
    "wallet_reversal":    18_000,
    "cascade":            15_000,
    "oversized_bet":      15_000,
    "accumulation":       12_000,
    "market_maker_flip":  12_000,
    "convergence":        12_000,
    "price_velocity":     10_000,
    "specialist_entry":    8_000,
    "whale_exit":          8_000,
    "no_position_entry":   8_000,
    "pre_deadline_surge":  5_000,
    "position_reduction":  5_000,
    "whale_entry":         3_000,
}

MIN_CONFIDENCE = 0.35

# Per-signal-type confidence floor overrides. Velocity signals top out at
# 0.85 with the 30%-move ceiling and start at 0.30 for a 10% move; the
# default 0.35 cap drops ~75% of them, so we lower the floor for them.
MIN_CONFIDENCE_BY_TYPE = {
    "price_velocity": 0.25,
}
MAX_POSITION_PCT = 0.15
MIN_STAKE = 25.0
STARTING_BANKROLL = 100_000

_PAPER_TRADES_SCHEMA = """
CREATE TABLE IF NOT EXISTS polymarket_paper_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    condition_id TEXT NOT NULL,
    question TEXT,
    category TEXT,
    side TEXT NOT NULL,
    entry_price REAL,
    size_usd REAL NOT NULL,
    signal_type TEXT NOT NULL,
    confidence REAL,
    wallet TEXT,
    entry_ts INTEGER,
    exit_price REAL,
    exit_ts INTEGER,
    outcome TEXT,
    return_pct REAL,
    realized_pnl REAL,
    archived INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_ppt_open ON polymarket_paper_trades(condition_id, exit_ts);
CREATE INDEX IF NOT EXISTS idx_ppt_signal ON polymarket_paper_trades(signal_type);
"""


class PolymarketPaperExecutor:
    """Paper trade executor for Polymarket smart money signals."""

    def __init__(self, db_path: str | Path = "data/kalshi/paper_trades.db") -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._migrate()

        # New polymarket_paper_trades table lives in wallet_intelligence.db
        self._wallet_db_path = _WALLET_DB_PATH
        self._wallet_lock = threading.Lock()
        self._wallet_conn = sqlite3.connect(str(self._wallet_db_path), check_same_thread=False)
        self._wallet_conn.executescript(_PAPER_TRADES_SCHEMA)
        self._wallet_conn.commit()

    def _migrate(self) -> None:
        """Create tables if missing, add columns for Polymarket fields."""
        now = datetime.now(tz=timezone.utc).isoformat()
        self._conn.executescript(f"""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL, side TEXT NOT NULL,
                entry_price REAL NOT NULL, size_usd REAL NOT NULL,
                signal_family TEXT, confidence REAL, news_context TEXT,
                entry_ts TEXT NOT NULL, exit_price REAL, exit_ts TEXT,
                outcome TEXT, return_pct REAL, status TEXT NOT NULL DEFAULT 'open'
            );
            CREATE TABLE IF NOT EXISTS portfolio (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL, cash_usd REAL NOT NULL,
                open_value REAL NOT NULL DEFAULT 0, total_value REAL NOT NULL,
                realized_pnl REAL NOT NULL DEFAULT 0
            );
        """)
        # Seed portfolio if empty
        if not self._conn.execute("SELECT 1 FROM portfolio LIMIT 1").fetchone():
            self._conn.execute(
                "INSERT INTO portfolio (ts, cash_usd, open_value, total_value, realized_pnl) VALUES (?, ?, 0, ?, 0)",
                (now, _STARTING_CASH, _STARTING_CASH),
            )
            self._conn.commit()
        for col, default in [
            ("platform TEXT DEFAULT 'kalshi'", None),
            ("full_token_id TEXT", None),
            ("signal_type TEXT", None),
            ("smart_money_confidence REAL", None),
            ("smart_money_edge REAL", None),
            ("weighted_net_volume REAL", None),
        ]:
            try:
                self._conn.execute(f"ALTER TABLE trades ADD COLUMN {col}")
            except sqlite3.OperationalError:
                pass

    def execute_trade(self, signal: Any, market_price: float) -> bool:
        """Place a paper trade from a SmartMoneySignal. Returns True if placed."""
        # Validation
        if signal.confidence < 0.60:
            return False
        if signal.top_wallet_edge < 0.50:
            return False
        if market_price < 0.05 or market_price > 0.95:
            return False
        if signal.direction == "NEUTRAL":
            return False

        token_short = signal.token_id[:20]

        with self._lock:
            # Check for existing position
            existing = self._conn.execute(
                "SELECT id FROM trades WHERE ticker = ? AND status = 'open'",
                (token_short,),
            ).fetchone()
            if existing:
                return False

            # Kelly sizing
            edge = signal.top_wallet_edge
            kelly = max(0, (edge - (1 - edge)))
            stake = min(max(kelly * _STARTING_CASH * 0.25, 5.0), 15.0)

            # Check cash
            cash_row = self._conn.execute(
                "SELECT cash_usd FROM portfolio ORDER BY id DESC LIMIT 1"
            ).fetchone()
            cash = float(cash_row[0]) if cash_row else _STARTING_CASH
            if cash < stake:
                return False

            now = datetime.now(tz=timezone.utc).isoformat()
            self._conn.execute(
                """INSERT INTO trades
                   (ticker, side, entry_price, size_usd, signal_family, confidence,
                    news_context, entry_ts, status, platform, full_token_id,
                    signal_type, smart_money_confidence, smart_money_edge,
                    weighted_net_volume)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'open', 'polymarket', ?, ?, ?, ?, ?)""",
                (token_short, signal.direction, market_price, round(stake, 2),
                 "smart_money", signal.confidence, "smart_money_scan", now,
                 signal.token_id, "smart_money",
                 signal.confidence, signal.top_wallet_edge,
                 signal.weighted_net_volume),
            )
            # Update portfolio
            new_cash = cash - stake
            self._conn.execute(
                "INSERT INTO portfolio (ts, cash_usd, open_value, total_value, realized_pnl) "
                "SELECT ?, ?, open_value + ?, total_value, realized_pnl "
                "FROM portfolio ORDER BY id DESC LIMIT 1",
                (now, round(new_cash, 2), round(stake, 2)),
            )
            self._conn.commit()

        logger.info(
            "Polymarket paper trade: %s %s @ %.2f size=$%.2f edge=%.1f%% wt_vol=$%.0f",
            signal.direction, token_short, market_price, stake,
            signal.top_wallet_edge * 100, signal.weighted_net_volume,
        )
        return True

    # ── New $100K bankroll path (writes to wallet_intelligence.db) ──────────

    def _compute_stake(self, signal_type: str, confidence: float, wallet: str) -> float:
        """Compute stake using wallet's Kelly fraction and Sharpe ratio.

        Falls back to confidence-based sizing if analytics unavailable.
        """
        bankroll = SIGNAL_BANKROLL.get(signal_type, 3000)
        try:
            with self._wallet_lock:
                row = self._wallet_conn.execute(
                    """SELECT kelly_fraction, sharpe_ratio, pnl_trend
                       FROM wallet_profiles WHERE wallet = ?""",
                    (wallet,),
                ).fetchone()
        except Exception:
            row = None

        if not row or row[0] is None:
            # No analytics yet — fall back to confidence-based sizing
            return round(bankroll * min(confidence, MAX_POSITION_PCT), 2)

        kelly, sharpe, trend = row[0], row[1], row[2]
        base_pct = kelly if kelly is not None else min(confidence, MAX_POSITION_PCT)

        # Sharpe multiplier (conservative when unknown or poor)
        if sharpe is not None and sharpe > 0:
            sharpe_mult = min(max(sharpe / 1.0, 0.5), 1.5)
        else:
            sharpe_mult = 0.75

        # Trend multiplier
        if trend is not None:
            if trend > 1.2:
                trend_mult = 1.1
            elif trend < 0.5:
                trend_mult = 0.8
            else:
                trend_mult = 1.0
        else:
            trend_mult = 1.0

        final_pct = base_pct * sharpe_mult * trend_mult
        final_pct = max(0.01, min(final_pct, MAX_POSITION_PCT))
        return max(MIN_STAKE, round(bankroll * final_pct, 2))

    def execute_signal(self, signal: dict[str, Any]) -> dict[str, Any] | None:
        """Place a paper trade for a fired signal in polymarket_paper_trades.

        Returns the trade dict if placed, None if skipped.
        """
        confidence = signal.get("confidence", 0) or 0
        signal_type = signal.get("signal_type", "")
        # Allow per-signal floors so velocity-style signals (which top out
        # at 0.85 by formula) aren't filtered out by the global threshold.
        floor = MIN_CONFIDENCE_BY_TYPE.get(signal_type, MIN_CONFIDENCE)
        if confidence < floor:
            return None

        if signal_type not in SIGNAL_BANKROLL:
            return None

        direction = (signal.get("direction") or "").upper()
        if direction not in ("BUY", "SELL"):
            return None

        condition_id = signal.get("condition_id") or signal.get("token_id", "")
        if not condition_id:
            return None

        # Check for existing open position on same market
        with self._wallet_lock:
            existing = self._wallet_conn.execute(
                """SELECT id FROM polymarket_paper_trades
                   WHERE condition_id = ? AND exit_ts IS NULL AND archived = 0""",
                (condition_id,),
            ).fetchone()
        if existing:
            return None

        wallet = signal.get("wallet", "")

        # Calibrated stake: prefer the bankroll allocator's recommendation
        # (Kelly-weighted from real outcomes) over the legacy compute_stake
        # path. Fall back to legacy if calibration hasn't been built yet.
        try:
            from trading_platform.polymarket.bankroll_allocator import BankrollAllocator
            calibrated_stake = BankrollAllocator(str(self._wallet_db_path)).get_stake_for(signal_type)
        except Exception:
            calibrated_stake = 0.0
        if calibrated_stake and calibrated_stake > 0:
            stake = calibrated_stake
        else:
            stake = self._compute_stake(signal_type, confidence, wallet)
        if stake < MIN_STAKE:
            return None

        # Tri-factor fusion gate. Skip the trade entirely if the fusion
        # score is below the floor; halve the stake when the score is
        # mid-range; full stake when high. Pulls pmxt microstructure when
        # available for the enhanced market signal.
        fusion_dict: dict[str, Any] | None = None
        microstructure: dict[str, Any] | None = None
        try:
            from trading_platform.polymarket.market_data_service import MarketDataService
            mds = MarketDataService(str(self._wallet_db_path))
            if mds.is_available():
                microstructure = mds.get_market_microstructure(
                    condition_id=condition_id,
                    direction="YES" if direction == "BUY" else "NO",
                    whale_entry_ts=int(signal.get("timestamp") or 0) or None,
                )
        except Exception as exc:
            logger.debug("microstructure lookup failed: %s", exc)

        # Dynamic per-wallet per-category tier multiplier (replaces the
        # static tier1h/tier1/tier2 mapping when a profile exists).
        dynamic_tier_mult = None
        try:
            from trading_platform.polymarket.wallet_tiering import WalletTieringEngine
            dynamic_tier_mult = WalletTieringEngine(str(self._wallet_db_path)).get_tier_multiplier(
                wallet, signal.get("category", "other"),
            )
        except Exception as exc:
            logger.debug("dynamic tier lookup failed: %s", exc)

        try:
            from trading_platform.polymarket.fusion_score import compute_fusion
            fusion = compute_fusion(
                wallet_wr=signal.get("directional_win_rate"),
                wallet_tier=signal.get("wallet_tier"),
                trade_size_usd=float(signal.get("size") or stake),
                wallet_avg_bet_usd=float(signal.get("wallet_avg_bet_usd") or 0),
                market_volume_usd=signal.get("market_volume_usd"),
                current_price=signal.get("price"),
                days_since_last_trade=signal.get("days_since_last_trade"),
                minutes_since_whale_entry=signal.get("minutes_since_whale_entry"),
                convergence_count=int(signal.get("converging_wallets") or 0),
                microstructure=microstructure,
                dynamic_tier_multiplier=dynamic_tier_mult,
            )
            fusion_dict = fusion.to_dict()
            if fusion.decision == "skip":
                logger.info(
                    "[FUSION_SKIP] %s score=%.2f w=%.2f m=%.2f t=%.2f | %s",
                    signal_type, fusion.score, fusion.wallet_signal,
                    fusion.market_signal, fusion.timing_signal,
                    (signal.get("question") or "")[:40],
                )
                return None
            stake = round(stake * fusion.stake_multiplier, 2)
            if stake < MIN_STAKE:
                return None
        except Exception as exc:
            logger.debug("fusion gate skipped: %s", exc)

        side = "YES" if direction == "BUY" else "NO"
        category = signal.get("category", "other")
        question = signal.get("question", "")
        entry_price = signal.get("price")
        now_ts = int(time.time())

        try:
            import json as _json
            fusion_blob = _json.dumps(fusion_dict) if fusion_dict else None
            fusion_score_val = fusion_dict.get("score") if fusion_dict else None
            with self._wallet_lock:
                cursor = self._wallet_conn.execute(
                    """INSERT INTO polymarket_paper_trades
                       (condition_id, question, category, side, entry_price,
                        size_usd, signal_type, confidence, wallet, entry_ts,
                        fusion_score, fusion_components, wallet_tier_at_fire,
                        archived)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)""",
                    (condition_id, question, category, side, entry_price,
                     stake, signal_type, confidence, wallet, now_ts,
                     fusion_score_val, fusion_blob, signal.get("wallet_tier")),
                )
                trade_id = cursor.lastrowid
                self._wallet_conn.commit()

            print(f"[PAPER] Placed ${stake:.0f} {side} {signal_type} conf={confidence:.2f} | {question[:40]}")

            return {
                "id": trade_id, "condition_id": condition_id, "question": question,
                "category": category, "side": side, "entry_price": entry_price,
                "size_usd": stake, "signal_type": signal_type, "confidence": confidence,
                "wallet": wallet, "entry_ts": now_ts,
            }
        except Exception as exc:
            logger.warning("execute_signal failed: %s", exc)
            return None

    def check_and_resolve_open_trades(self) -> dict[str, int]:
        """Resolve any open paper trades whose underlying market has settled.

        For each row in ``polymarket_paper_trades`` with ``exit_ts IS NULL``,
        query Gamma for the market's resolution state. If the market is
        resolved, write back ``exit_ts``, ``exit_price``, ``realized_pnl``,
        ``return_pct``, and ``outcome``, and fire a Telegram alert (best
        effort). Returns ``{checked, resolved}``.
        """
        import json as _json
        try:
            import requests as _req
        except Exception:
            return {"checked": 0, "resolved": 0}

        with self._wallet_lock:
            open_trades = self._wallet_conn.execute(
                """SELECT id, condition_id, side, entry_price, size_usd,
                          signal_type, entry_ts, question
                   FROM polymarket_paper_trades
                   WHERE archived = 0 AND exit_ts IS NULL"""
            ).fetchall()

        resolved_count = 0
        for trade in open_trades:
            trade_id, cid, side, entry_price, size_usd, sig_type, entry_ts, question = trade
            if not cid or entry_price is None or not size_usd:
                continue
            try:
                # Use the verified ``condition_ids`` (plural) param. The
                # singular ``conditionId`` is silently ignored by Gamma.
                r = _req.get(
                    "https://gamma-api.polymarket.com/markets",
                    params={"condition_ids": cid},
                    timeout=10,
                )
                if r.status_code != 200:
                    time.sleep(0.2)
                    continue
                data = r.json()
                m = data[0] if isinstance(data, list) and data else None
                if not m or (m.get("conditionId") or "").lower() != cid.lower():
                    time.sleep(0.2)
                    continue

                # Settlement detection: ``closed`` flag + outcomePrices at
                # extremes (0 / 1) is the most reliable Gamma signal.
                closed = bool(m.get("closed"))
                resolution_price: float | None = None
                op_raw = m.get("outcomePrices")
                if op_raw:
                    try:
                        op = _json.loads(op_raw) if isinstance(op_raw, str) else op_raw
                        if op:
                            resolution_price = float(op[0])
                    except Exception:
                        resolution_price = None
                if resolution_price is None and m.get("lastTradePrice") is not None:
                    try:
                        resolution_price = float(m["lastTradePrice"])
                    except (TypeError, ValueError):
                        resolution_price = None

                # Treat as resolved only when the market is closed AND the
                # YES price is at an extreme (>0.99 or <0.01).
                is_resolved = (
                    closed
                    and resolution_price is not None
                    and (resolution_price >= 0.99 or resolution_price <= 0.01)
                )
                if not is_resolved:
                    time.sleep(0.2)
                    continue

                # Compute P&L. Long the chosen outcome at entry_price.
                # YES side: pays out resolution_price; NO side: pays out 1-resolution_price.
                if side == "YES":
                    final_value = resolution_price
                else:
                    final_value = 1.0 - resolution_price
                # shares = size_usd / entry_price; pnl = shares * (final - entry)
                if entry_price <= 0:
                    time.sleep(0.2)
                    continue
                shares = size_usd / entry_price
                pnl = round(shares * (final_value - entry_price), 2)
                return_pct = round(((final_value - entry_price) / entry_price) * 100, 2)
                outcome = "win" if pnl > 0 else "loss"

                with self._wallet_lock:
                    self._wallet_conn.execute(
                        """UPDATE polymarket_paper_trades
                           SET exit_ts = ?, exit_price = ?, realized_pnl = ?,
                               return_pct = ?, outcome = ?
                           WHERE id = ?""",
                        (int(time.time()), round(final_value, 4), pnl,
                         return_pct, outcome, trade_id),
                    )
                    self._wallet_conn.commit()
                resolved_count += 1
                logger.info(
                    "[RESOLVE] %s %s pnl=$%.2f (%.1f%%) — %s",
                    sig_type, outcome, pnl, return_pct, (question or "")[:40],
                )

                # Telegram alert (best effort)
                try:
                    from trading_platform.polymarket.telegram_alerts import get_alerter
                    alerter = get_alerter()
                    if alerter.enabled:
                        signal_stats = self._get_signal_stats(sig_type)
                        alerter.send_trade_resolved({
                            "signal_type": sig_type,
                            "side": side,
                            "size_usd": size_usd,
                            "entry_price": entry_price,
                            "exit_price": final_value,
                            "return_pct": return_pct / 100,
                            "outcome": outcome,
                            "question": question,
                        }, signal_stats)
                except Exception:
                    pass

                time.sleep(0.2)
            except Exception as exc:
                logger.debug("resolve check failed for %s: %s", (cid or "")[:18], exc)
                time.sleep(0.2)
                continue

        # After any resolution, refresh calibration. If a status transition
        # occurred for any signal type, fire a Telegram alert. Also
        # incrementally re-evaluate the dynamic tiers for the wallets
        # whose paper trades just resolved.
        if resolved_count > 0:
            try:
                from trading_platform.polymarket.wallet_tiering import WalletTieringEngine
                tiering = WalletTieringEngine(str(self._wallet_db_path))
                seen_wallets: set[str] = set()
                for trade in open_trades:
                    wallet_addr = (trade[3] if len(trade) > 3 else "") or ""
                    # ``open_trades`` tuple positions: (id, cid, side, entry, size, sig, ts, q)
                    # but the executor's selection includes wallet via signal context elsewhere;
                    # safer to walk the wallets via the resolved IDs.
                # Pull wallets from resolved trades directly
                with self._wallet_lock:
                    rows = self._wallet_conn.execute(
                        """SELECT DISTINCT wallet, category FROM polymarket_paper_trades
                           WHERE archived = 0 AND exit_ts IS NOT NULL
                             AND id IN (SELECT id FROM polymarket_paper_trades
                                        WHERE archived = 0 AND exit_ts IS NOT NULL
                                        ORDER BY exit_ts DESC LIMIT ?)""",
                        (resolved_count,),
                    ).fetchall()
                tier_changes_total: list[dict[str, Any]] = []
                for wallet_addr, cat in rows:
                    if not wallet_addr:
                        continue
                    try:
                        result = tiering.evaluate_single_wallet(wallet_addr, cat or None)
                        for ch in result.get("changes", []):
                            tier_changes_total.append(ch)
                    except Exception as exc:
                        logger.debug("tier eval failed for %s: %s", (wallet_addr or "")[:14], exc)

                # Telegram on significant tier changes
                if tier_changes_total:
                    try:
                        from trading_platform.polymarket.telegram_alerts import get_alerter
                        alerter = get_alerter()
                        if alerter.enabled:
                            for ch in tier_changes_total:
                                self._send_tier_change_alert(alerter, ch)
                    except Exception as exc:
                        logger.debug("tier alert dispatch failed: %s", exc)
            except Exception as exc:
                logger.debug("incremental tier evaluation failed: %s", exc)

            try:
                from trading_platform.polymarket.signal_evaluator import SignalEvaluator
                from trading_platform.polymarket.bankroll_allocator import BankrollAllocator
                evaluator = SignalEvaluator(str(self._wallet_db_path))
                results = evaluator.update_all()
                # Rebalance allocations
                try:
                    BankrollAllocator(str(self._wallet_db_path)).rebalance(dry_run=False)
                except Exception as exc:
                    logger.debug("rebalance failed: %s", exc)
                # Telegram on transitions
                try:
                    from trading_platform.polymarket.telegram_alerts import get_alerter
                    alerter = get_alerter()
                    if alerter.enabled:
                        for row, prev_status in results:
                            if prev_status and prev_status != row.status:
                                self._send_status_transition_alert(
                                    alerter, row, prev_status,
                                )
                except Exception as exc:
                    logger.debug("transition alert dispatch failed: %s", exc)
            except Exception as exc:
                logger.debug("post-resolution calibration failed: %s", exc)

        logger.info("Resolution check: %d open, %d resolved", len(open_trades), resolved_count)
        return {"checked": len(open_trades), "resolved": resolved_count}

    def _send_tier_change_alert(self, alerter: Any, change: dict[str, Any]) -> None:
        """Telegram alert for significant wallet tier movements.

        Only fires for S/A demotions and promotions to B or above —
        avoids spamming on C/D oscillations.
        """
        old = (change.get("old_tier") or "").upper()
        new = (change.get("new_tier") or "").upper()
        wallet = (change.get("wallet") or "")[:14]
        category = change.get("category") or ""
        trigger = change.get("trigger_metric") or ""
        wr30 = change.get("win_rate_30d_at_change")

        rank = {"S": 0, "A": 1, "B": 2, "C": 3, "D": 4}
        old_rank = rank.get(old, 5)
        new_rank = rank.get(new, 5)
        is_demotion = new_rank > old_rank
        is_promotion = new_rank < old_rank

        # Filter: significant only
        if is_demotion and old not in ("S", "A"):
            return
        if is_promotion and new not in ("S", "A", "B"):
            return

        emoji = "📉" if is_demotion else "📈"
        msg = (
            f"{emoji} <b>WALLET TIER {'DEMOTION' if is_demotion else 'PROMOTION'}</b>\n"
            f"<code>{wallet}…</code> in <b>{category}</b>\n"
            f"{old or '?'} → {new}\n\n"
        )
        if trigger:
            msg += f"Trigger: {trigger}\n"
        if wr30 is not None:
            msg += f"30d WR: {wr30 * 100:.0f}%\n"
        if change.get("pnl_at_change") is not None:
            msg += f"Cumulative PnL: ${change['pnl_at_change']:,.0f}\n"
        msg += "\n──────────\n🖥 localhost:5173/signals"
        try:
            alerter._send(msg, disable_notification=is_promotion)
        except Exception:
            pass

    def _send_status_transition_alert(self, alerter: Any, row: Any, prev_status: str) -> None:
        """Telegram alert when a signal type changes calibration status."""
        emoji = {
            "live": "🟢",
            "weak": "🟡",
            "disabled": "🔴",
            "building": "🔵",
        }.get(row.status, "⚪")
        msg = (
            f"{emoji} <b>SIGNAL CALIBRATION CHANGE</b>\n"
            f"<b>{row.signal_type}</b>: {prev_status.upper()} → {row.status.upper()}\n\n"
            f"Sample: {row.sample_size} ({row.wins}W / {row.losses}L)\n"
            f"Bayesian WR: {(row.bayesian_wr or 0)*100:.0f}%\n"
            f"EV/trade: {(row.ev_per_trade or 0)*100:+.1f}%\n"
            f"Profit factor: {row.profit_factor}\n"
            f"Kelly: {(row.kelly_fraction or 0)*100:.1f}%\n"
            f"Rolling 10 WR: {(row.rolling_10_wr or 0)*100:.0f}%\n"
            f"\n\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\n"
            f"\U0001f5a5 localhost:5173/signals"
        )
        try:
            alerter._send(msg, disable_notification=(row.status not in ("disabled", "live")))
        except Exception:
            pass

    def _get_signal_stats(self, signal_type: str) -> dict[str, Any]:
        """Aggregate stats for one signal type for inclusion in resolution alerts."""
        with self._wallet_lock:
            row = self._wallet_conn.execute(
                """SELECT COUNT(*), SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END),
                          SUM(realized_pnl)
                   FROM polymarket_paper_trades
                   WHERE signal_type = ? AND archived = 0 AND exit_ts IS NOT NULL""",
                (signal_type,),
            ).fetchone()
        return {
            "total_resolved": (row[0] or 0) if row else 0,
            "wins": (row[1] or 0) if row else 0,
            "total_pnl": float(row[2] or 0) if row else 0.0,
        }

    def get_open_positions(self) -> list[dict[str, Any]]:
        """Return open paper trades from polymarket_paper_trades."""
        with self._wallet_lock:
            rows = self._wallet_conn.execute(
                """SELECT id, condition_id, question, category, side, entry_price,
                          size_usd, signal_type, confidence, wallet, entry_ts
                   FROM polymarket_paper_trades
                   WHERE exit_ts IS NULL AND archived = 0
                   ORDER BY entry_ts DESC"""
            ).fetchall()
        cols = ["id", "condition_id", "question", "category", "side", "entry_price",
                "size_usd", "signal_type", "confidence", "wallet", "entry_ts"]
        return [dict(zip(cols, r)) for r in rows]

    def check_resolutions_v2(self) -> list[dict[str, Any]]:
        """Check open positions against wallet_trades for resolution."""
        open_positions = self.get_open_positions()
        resolved: list[dict[str, Any]] = []
        for pos in open_positions:
            cid = pos["condition_id"]
            with self._wallet_lock:
                row = self._wallet_conn.execute(
                    """SELECT pnl, market_outcome FROM wallet_trades
                       WHERE condition_id = ? AND market_resolved = 1
                       AND market_outcome IS NOT NULL LIMIT 1""",
                    (cid,),
                ).fetchone()
            if not row:
                continue
            pnl_per_dollar, outcome = row
            outcome_yes = (outcome or "").upper() in ("YES", "1", "TRUE")
            won = (pos["side"] == "YES" and outcome_yes) or (pos["side"] == "NO" and not outcome_yes)
            entry = pos.get("entry_price") or 0.5
            if won:
                exit_price = 1.0
                return_pct = (1.0 / entry - 1) if entry > 0 else 0
            else:
                exit_price = 0.0
                return_pct = -1.0
            realized = pos["size_usd"] * return_pct
            now_ts = int(time.time())
            try:
                with self._wallet_lock:
                    self._wallet_conn.execute(
                        """UPDATE polymarket_paper_trades
                           SET exit_price = ?, exit_ts = ?, outcome = ?,
                               return_pct = ?, realized_pnl = ?
                           WHERE id = ?""",
                        (exit_price, now_ts, "win" if won else "loss",
                         round(return_pct, 4), round(realized, 2), pos["id"]),
                    )
                    self._wallet_conn.commit()
                resolved.append({**pos, "outcome": "win" if won else "loss",
                                 "realized_pnl": round(realized, 2)})
            except Exception as exc:
                logger.warning("Could not update resolved trade %d: %s", pos["id"], exc)
        return resolved

    def get_summary_v2(self) -> dict[str, Any]:
        """Portfolio summary from polymarket_paper_trades."""
        with self._wallet_lock:
            row = self._wallet_conn.execute(
                """SELECT COUNT(*) total,
                          SUM(CASE WHEN exit_ts IS NULL THEN 1 ELSE 0 END) open_count,
                          SUM(CASE WHEN outcome IS NOT NULL THEN 1 ELSE 0 END) resolved,
                          SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END) wins,
                          COALESCE(SUM(realized_pnl), 0) realized_pnl,
                          COALESCE(SUM(CASE WHEN exit_ts IS NULL THEN size_usd ELSE 0 END), 0) deployed
                   FROM polymarket_paper_trades WHERE archived = 0"""
            ).fetchone()

        total, open_count, resolved, wins, realized_pnl, deployed = row
        return {
            "starting_bankroll": STARTING_BANKROLL,
            "current_bankroll": round(STARTING_BANKROLL - (deployed or 0) + (realized_pnl or 0), 2),
            "total_trades": total or 0,
            "open_trades": open_count or 0,
            "resolved_trades": resolved or 0,
            "wins": wins or 0,
            "realized_pnl": round(realized_pnl or 0, 2),
            "deployed": round(deployed or 0, 2),
            "win_rate": round(wins / resolved, 3) if resolved else None,
        }

    def on_signal(self, signal: dict[str, Any]) -> bool:
        """Place paper trade from a whale signal dict. Returns True if placed."""
        confidence = signal.get("confidence", 0)
        if confidence < 0.40:
            return False

        signal_type = signal.get("signal_type", "whale_entry")
        wallet_tier = signal.get("wallet_tier", "tier2")
        condition_id = signal.get("condition_id", "")
        direction = signal.get("direction", "BUY")
        price = signal.get("price", 0)
        question = signal.get("question", "")[:80]
        category = signal.get("category", "other")

        # Bankroll allocation per signal type
        if signal_type == "convergence":
            max_stake = 500.0 * 0.05  # 5% of default $10K... but we use $500 actual
        elif wallet_tier == "tier1":
            max_stake = 500.0 * 0.03  # 3%
        else:
            max_stake = 500.0 * 0.015  # 1.5%

        stake = round(max_stake * confidence, 2)
        if stake < 1.0:
            return False

        token_short = condition_id[:20] if condition_id else "unknown"

        with self._lock:
            # Skip if already have open position on same side
            existing = self._conn.execute(
                "SELECT id FROM trades WHERE ticker = ? AND side = ? AND status = 'open' AND platform = 'polymarket'",
                (token_short, direction),
            ).fetchone()
            if existing:
                return False

            # Check cash
            cash_row = self._conn.execute(
                "SELECT cash_usd FROM portfolio ORDER BY id DESC LIMIT 1"
            ).fetchone()
            cash = float(cash_row[0]) if cash_row else _STARTING_CASH
            if cash < stake:
                return False

            now = datetime.now(tz=timezone.utc).isoformat()
            self._conn.execute(
                """INSERT INTO trades
                   (ticker, side, entry_price, size_usd, signal_family, confidence,
                    news_context, entry_ts, status, platform, full_token_id,
                    signal_type, smart_money_confidence, smart_money_edge,
                    weighted_net_volume)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'open', 'polymarket', ?, ?, ?, ?, ?)""",
                (token_short, direction, price, stake,
                 f"polymarket_whale", confidence, f"{signal_type}|{category}", now,
                 condition_id, signal_type,
                 confidence, signal.get("directional_win_rate", 0),
                 signal.get("size", 0)),
            )
            new_cash = cash - stake
            self._conn.execute(
                "INSERT INTO portfolio (ts, cash_usd, open_value, total_value, realized_pnl) "
                "SELECT ?, ?, open_value + ?, total_value, realized_pnl "
                "FROM portfolio ORDER BY id DESC LIMIT 1",
                (now, round(new_cash, 2), round(stake, 2)),
            )
            self._conn.commit()

        print(
            f"[PAPER] {direction} | ${stake:.0f} | {category} | "
            f"{question}... | conf={confidence:.2f}"
        )
        return True

    def check_resolutions(self, resolver: Any) -> list[dict[str, Any]]:
        """Check open Polymarket trades for resolution. Returns resolved trades."""
        with self._lock:
            open_trades = self._conn.execute(
                "SELECT id, ticker, side, entry_price, size_usd, full_token_id "
                "FROM trades WHERE platform = 'polymarket' AND status = 'open'"
            ).fetchall()

        resolved: list[dict[str, Any]] = []
        for trade_id, ticker, side, entry_price, size_usd, full_token_id in open_trades:
            token = full_token_id or ticker
            rp = resolver.resolve(token)
            if rp is None:
                continue

            resolved_yes = rp >= 99.0
            won = (side == "YES" and resolved_yes) or (side == "NO" and not resolved_yes)
            if won:
                payout = size_usd * (1.0 / entry_price) if entry_price > 0 else size_usd
                return_pct = (payout - size_usd) / size_usd
            else:
                payout = 0.0
                return_pct = -1.0

            now = datetime.now(tz=timezone.utc).isoformat()
            with self._lock:
                self._conn.execute(
                    "UPDATE trades SET exit_price=?, exit_ts=?, outcome=?, return_pct=?, status='closed' WHERE id=?",
                    (100.0 if resolved_yes else 0.0, now, "win" if won else "loss",
                     round(return_pct, 4), trade_id),
                )
                cash_row = self._conn.execute("SELECT cash_usd, realized_pnl FROM portfolio ORDER BY id DESC LIMIT 1").fetchone()
                new_cash = float(cash_row[0]) + payout
                new_pnl = float(cash_row[1]) + (payout - size_usd)
                self._conn.execute(
                    "INSERT INTO portfolio (ts, cash_usd, open_value, total_value, realized_pnl) VALUES (?, ?, 0, ?, ?)",
                    (now, round(new_cash, 2), round(new_cash, 2), round(new_pnl, 2)),
                )
                self._conn.commit()

            resolved.append({
                "ticker": ticker, "side": side, "outcome": "win" if won else "loss",
                "return_pct": round(return_pct, 4), "payout": round(payout, 2),
            })

        return resolved

    def get_summary(self) -> dict[str, Any]:
        """Summary of Polymarket paper trades."""
        with self._lock:
            total = self._conn.execute("SELECT COUNT(*) FROM trades WHERE platform='polymarket'").fetchone()[0]
            open_count = self._conn.execute("SELECT COUNT(*) FROM trades WHERE platform='polymarket' AND status='open'").fetchone()[0]
            closed = self._conn.execute("SELECT COUNT(*) FROM trades WHERE platform='polymarket' AND status='closed'").fetchone()[0]
            wins = self._conn.execute("SELECT COUNT(*) FROM trades WHERE platform='polymarket' AND outcome='win'").fetchone()[0]
            avg_conf = self._conn.execute("SELECT AVG(smart_money_confidence) FROM trades WHERE platform='polymarket'").fetchone()[0]
            avg_edge = self._conn.execute("SELECT AVG(smart_money_edge) FROM trades WHERE platform='polymarket'").fetchone()[0]

        return {
            "platform": "polymarket",
            "total_trades": total,
            "open_trades": open_count,
            "closed_trades": closed,
            "wins": wins,
            "win_rate": round(wins / closed, 3) if closed > 0 else 0.0,
            "avg_confidence": round(avg_conf, 3) if avg_conf else 0.0,
            "avg_edge": round(avg_edge, 3) if avg_edge else 0.0,
        }

    def close(self) -> None:
        with self._lock:
            self._conn.close()
