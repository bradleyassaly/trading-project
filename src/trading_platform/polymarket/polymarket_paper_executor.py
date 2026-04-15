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
    # ─── Backtest-validated (EV > 0 on n ≥ 20 over 60d) ───
    # Values set from 2026-04-14 signal_engine_backtest results:
    #   specialist_entry  n=32  EV=+0.411  ← highest validated EV
    #   wallet_reversal   n=98  EV=+0.147
    #   network_leader    n=81  EV=+0.078
    #   tier_entry        n=98  EV=+0.057
    "specialist_entry":        20_000,  # 60d-backtest EV=+0.411 strongest
    "wallet_reversal":         18_000,  # 60d-backtest EV=+0.147
    "tier_entry":              15_000,  # 60d-backtest EV=+0.057
    "network_leader_entry":    15_000,  # 60d-backtest EV=+0.078

    # ─── New strategy-specific signals (small sample, positive early) ───
    "copyable_contrarian":     12_000,  # n=10 EV=+0.18 (strong early)
    "strategy_specialist":     10_000,  # n=5 EV=+0.11 (early)

    # ─── Historical validations (may be stale) ───
    "accumulation":            20_000,  # structural signal (historical WR 80%, EV +0.28)
    "late_conviction":         15_000,  # late entry + large size (N=173, WR 73%, EV +0.23)
    "whale_entry_filtered":    15_000,  # copyable wallets only (WR 72%, EV +0.15)
    "insider_entry":           15_000,  # disabled: sampling bias

    # ─── Awaiting data ───
    "consensus_follower":      12_000,  # 3+ leaderboard wallets same-side 24h
    "news_reactor":            10_000,  # trade within 1h of ≥20pp spike
    "cascade":                 10_000,  # no backtest data
    "convergence":             10_000,  # no backtest data

    # ─── Backtest-negative or demoted ───
    "oversized_bet":           3_000,   # 60d-backtest EV=-0.127 — capped
    "market_maker_flip":       2_000,   # 60d-backtest EV=-0.268 — near-disabled
    "whale_entry":             1_000,   # 60d-backtest EV=-0.047 — baseline, demoted
    "no_position_entry":       3_000,   # small sample, unproven
    "pre_deadline_surge":      5_000,
    "price_velocity":          3_000,   # velocity noise; already excluded at gate
    "whale_exit":              3_000,   # informational — small alloc
    "position_reduction":      3_000,   # informational
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
STARTING_BANKROLL = 10_000  # Paper validation phase — not production size

# Fillability floor: don't trade markets priced in the extreme tails. The
# previous executor produced 11/16 "winning" trades by buying NO at
# 0.001-0.023 on degenerate long-tail markets ("Will random unknown win
# election X"); the +$4.97M PnL was mathematically valid but would not
# fill at those prices on the real CLOB. The Bayesian win-rate / Kelly
# fraction downstream then read those fictional fills as evidence the
# strategy worked. See reports/data_validation.md DV-5.
# Entry price bounds — validated via scripts/wallet_deep_dive.py on 3,418
# resolved top-wallet trades. Bucketed PnL (full sample):
#   (0.05, 0.10]:  WR=86% PnL=-$4.8K   (longshot noise, losers just exceed winners)
#   (0.10, 0.20]:  WR=63% PnL=+$229K   *** sweet spot
#   (0.20, 0.35]:  WR=58% PnL=+$176K   *** sweet spot
#   (0.35, 0.50]:  WR=61% PnL=+$83K    good
#   (0.50, 0.65]:  WR=60% PnL=+$41K    good
#   (0.65, 0.80]:  WR=67% PnL=-$18K    <-- high WR BUT loses money (late-momentum trap)
#   (0.80, 0.90]:  WR=72% PnL=-$4.5K
# So 0.65-0.80 is a win-rate trap: looks good on paper, bleeds in practice.
MIN_ENTRY_PRICE = 0.10
MAX_ENTRY_PRICE = 0.65

# Optimal band where PnL-per-trade is densest ($0.10-$0.50 earned $488K combined).
OPTIMAL_BAND_LOW = 0.10
OPTIMAL_BAND_HIGH = 0.50
OPTIMAL_BAND_BOOST = 1.15  # confidence multiplier for sweet spot

# Category gate — only paper-trade where we have proven positive EV.
# Signals still fire and record to signal_outcomes for every category
# so we can flip this list as new categories mature. See Fix 6.
# Sports moved OUT of EXCLUDE into TIER1-ONLY on 2026-04-14 after
# wallet_deep_dive.py showed tier1-only sports is +$55K/863 trades @ 67% WR,
# while the full sports population is -$44K. See TIER1_ONLY_CATEGORIES below.
PAPER_TRADE_CATEGORIES = {"politics", "geopolitics", "crypto", "economics", "sports"}
TIER1_ONLY_CATEGORIES = {"sports"}  # require wallet_tier='tier1' for these
EXCLUDE_CATEGORIES: set[str] = set()  # no hard exclusions; tier-gating handles it
# Live trading: strict allowlist (statistically significant positive EV only).
LIVE_TRADE_CATEGORIES = {"politics", "geopolitics"}
# Signal types to exclude from paper bankroll (fire+record only, no capital).
EXCLUDE_SIGNAL_TYPES = {"price_velocity"}  # 95% WR, EV +0.004 = noise

# Kelly sizing — Half-Kelly from validated 0.10-0.80 data (WR=73%, odds=0.82)
HALF_KELLY = 0.05  # 5% of available bankroll (quarter-Kelly, conservative for paper phase)
MIN_STAKE_USD = 5.0
MAX_STAKE_USD = 500.0
MAX_PORTFOLIO_PCT = 0.30  # never deploy >30% of bankroll
MAX_CATEGORY_PCT = 0.40   # max 40% of bankroll in any single category
MAX_SLIPPAGE = 0.05       # reject if price moved >5% since whale traded

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
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False, timeout=60)
        for stmt in ("PRAGMA busy_timeout=60000", "PRAGMA journal_mode=WAL", "PRAGMA synchronous=NORMAL"):
            try:
                self._conn.execute(stmt)
            except sqlite3.OperationalError:
                pass
        self._migrate()

        # New polymarket_paper_trades table lives in wallet_intelligence.db
        self._wallet_db_path = _WALLET_DB_PATH
        self._wallet_lock = threading.Lock()
        try:
            from trading_platform.polymarket.db import connect_wallet_db
            self._wallet_conn = connect_wallet_db(self._wallet_db_path, check_same_thread=False)
        except Exception:
            # Fallback for tests or when db module isn't available
            self._wallet_conn = sqlite3.connect(str(self._wallet_db_path), check_same_thread=False, timeout=60)
            for stmt in ("PRAGMA busy_timeout=60000", "PRAGMA journal_mode=WAL", "PRAGMA synchronous=NORMAL"):
                try:
                    self._wallet_conn.execute(stmt)
                except sqlite3.OperationalError:
                    pass
        # The schema is idempotent (CREATE TABLE IF NOT EXISTS) and is
        # already present in the production DB. Tests construct multiple
        # executors against a wallet DB another process holds open, which
        # makes this executescript contend on the global write lock. The
        # schema check itself isn't load-bearing on init — the table
        # exists in any deployed environment — so swallowing the lock
        # error here is safe.
        try:
            self._wallet_conn.executescript(_PAPER_TRADES_SCHEMA)
            self._wallet_conn.commit()
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower():
                raise
            logger.debug("paper_trades schema executescript skipped (db locked): %s", exc)

        # Self-heal circuit breaker anchor. The breaker row was historically
        # initialized against a $100k placeholder while the paper book runs
        # at STARTING_BANKROLL; any drawdown math against the wrong anchor
        # is meaningless. Rebase when the gap exceeds ~5%.
        try:
            from trading_platform.polymarket.circuit_breaker import CircuitBreaker
            cb = CircuitBreaker(str(self._wallet_db_path))
            state = cb.initialize(starting_capital=STARTING_BANKROLL)
            sc = float(state.get("starting_capital") or 0)
            if sc > 0 and abs(sc - STARTING_BANKROLL) / sc > 0.05:
                logger.info(
                    "[CB] rebasing starting_capital %.0f → %.0f", sc, STARTING_BANKROLL,
                )
                cb.rebase(STARTING_BANKROLL)
        except Exception as exc:
            logger.debug("circuit breaker rebase check failed: %s", exc)

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

    def _compute_stake(self, signal_type: str, confidence: float,
                        wallet: str, category: str = "other") -> tuple[float, str]:
        """Compute stake using half-Kelly from alpha_scores distribution.

        Returns (stake, reason) where reason explains the sizing decision.
        Falls back to confidence-based sizing if analytics unavailable.
        """
        bankroll = SIGNAL_BANKROLL.get(signal_type, 3000)

        # Try alpha_scores Kelly first (per wallet×category)
        try:
            alpha_row = self._wallet_conn.execute(
                """SELECT kelly_fraction, sharpe_ratio, avg_win_pnl,
                          avg_loss_pnl, profit_factor
                   FROM wallet_alpha_scores
                   WHERE wallet = ? AND category = ? AND is_copyable = 1""",
                (wallet, category),
            ).fetchone()
        except Exception:
            alpha_row = None

        if alpha_row and alpha_row[0] is not None and alpha_row[0] > 0:
            kelly = alpha_row[0]
            fraction = kelly * 0.5  # half-Kelly for safety
            raw = bankroll * fraction
            stake = max(MIN_STAKE, min(raw, 50.0))  # $10 min, $50 max for paper
            reason = (
                f"half-Kelly: K={kelly*100:.1f}%, hK={fraction*100:.1f}%, "
                f"raw=${raw:.0f}, bounded=${stake:.0f} "
                f"(bankroll=${bankroll:,}, PF={alpha_row[4] or 0:.1f}x)"
            )
            return round(stake, 2), reason

        # Fallback: wallet_profiles Kelly
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
            stake = round(bankroll * min(confidence, MAX_POSITION_PCT), 2)
            return max(MIN_STAKE, stake), f"confidence-based: {confidence:.2f} * ${bankroll:,}"

        kelly, sharpe, trend = row[0], row[1], row[2]
        base_pct = kelly if kelly is not None else min(confidence, MAX_POSITION_PCT)

        if sharpe is not None and sharpe > 0:
            sharpe_mult = min(max(sharpe / 1.0, 0.5), 1.5)
        else:
            sharpe_mult = 0.75

        if trend is not None:
            trend_mult = 1.1 if trend > 1.2 else 0.8 if trend < 0.5 else 1.0
        else:
            trend_mult = 1.0

        final_pct = max(0.01, min(base_pct * sharpe_mult * trend_mult, MAX_POSITION_PCT))
        stake = max(MIN_STAKE, round(bankroll * final_pct, 2))
        return stake, f"profile-Kelly: K={kelly}, S={sharpe}, T={trend}"

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

        # Signal-type gate (global): exclude low-edge / high-volume types
        # from bankroll deployment. They still fire and record for analysis.
        if signal_type in EXCLUDE_SIGNAL_TYPES:
            logger.debug("[CAT_GATE] SKIP excluded signal_type=%s", signal_type)
            return None

        # Category gate (global): proven positive-EV categories only.
        # Resolve unknown categories via the classifier BEFORE gating —
        # previously, a missing category bypassed the allowlist entirely
        # and got INSERTed as "other", letting non-allowlist trades sneak
        # in (130/134 resolved trades landed in "other" this way).
        sig_cat_raw = signal.get("category") or ""
        sig_cat = sig_cat_raw.lower() if isinstance(sig_cat_raw, str) else ""
        if not sig_cat or sig_cat == "other":
            try:
                from trading_platform.polymarket.market_categorizer import classify_keywords
                resolved, _src = classify_keywords(
                    signal.get("slug") or "",
                    signal.get("question") or "",
                )
                if resolved and resolved != "other":
                    sig_cat = resolved.lower()
                    signal["category"] = sig_cat
            except Exception as exc:
                logger.debug("category classifier failed: %s", exc)
        if sig_cat in EXCLUDE_CATEGORIES:
            logger.info("[CAT_GATE] SKIP %s in excluded category %s", signal_type, sig_cat)
            return None
        if sig_cat not in PAPER_TRADE_CATEGORIES:
            logger.info("[CAT_GATE] SKIP %s in unproven category %s", signal_type, sig_cat or "<empty>")
            return None

        # Tier gate for TIER1_ONLY_CATEGORIES (e.g. sports): the full
        # sports population is -$44K over 4,707 trades, but tier1-only
        # sports is +$55K over 863 trades at 67% WR. Require tier1.
        if sig_cat in TIER1_ONLY_CATEGORIES:
            tier = signal.get("wallet_tier") or ""
            if tier not in ("tier1", "tier1h"):
                logger.info(
                    "[TIER_GATE] SKIP %s in %s — wallet_tier=%s (need tier1/tier1h)",
                    signal_type, sig_cat, tier or "NULL",
                )
                return None

        # Category exclusions per signal type. Crypto accumulation: 7 signals,
        # 0 wins, EV=-0.50 on corrected data. Sports/entertainment are also
        # negative. See reports/category_grouping_analysis_2026-04-12.md.
        _EXCLUDED_CATS = {
            "accumulation": {"crypto", "entertainment", "sports"},
            "insider_entry": {"crypto", "entertainment", "sports"},
        }
        category = signal.get("category") or "other"
        excluded = _EXCLUDED_CATS.get(signal_type, set())
        if category in excluded:
            logger.info("[PAPER] SKIP %s in excluded category %s", signal_type, category)
            return None

        # Layer 3: cumulative drawdown circuit breaker. Blocks ALL trades
        # — paper or live — once cumulative drawdown from peak crosses
        # the threshold. Initialized lazily; absent state means no block.
        try:
            from trading_platform.polymarket.circuit_breaker import CircuitBreaker
            cb = CircuitBreaker(str(self._wallet_db_path))
            allowed, reason = cb.can_trade()
            if not allowed:
                logger.warning("[CIRCUIT_BREAKER] Trade blocked: %s", reason)
                return None
        except Exception as exc:
            logger.debug("circuit breaker check failed (proceeding): %s", exc)

        # Layer 4: KillSwitch. Battle-tested on paper now so the exact
        # same gate guards live trades once POLYMARKET_LIVE_ENABLED=1.
        # In paper mode the master-switch check is bypassed — we want
        # to keep collecting paper data to let the edge gates mature —
        # but the emergency stop file, position cap, and size cap still
        # apply so the switch can be exercised.
        try:
            from trading_platform.polymarket.kill_switch import KillSwitch
            if not hasattr(self, "_kill_switch"):
                self._kill_switch = KillSwitch(str(self._wallet_db_path))
            stopped, stop_reason = self._kill_switch.is_emergency_stopped()
            if stopped:
                logger.warning("[KILL_SWITCH] BLOCKED: emergency stop active: %s", stop_reason)
                return None
        except Exception as exc:
            logger.debug("kill switch check failed (proceeding): %s", exc)

        direction = (signal.get("direction") or "").upper()
        if direction not in ("BUY", "SELL"):
            return None

        condition_id = signal.get("condition_id") or signal.get("token_id", "")
        if not condition_id:
            return None

        # Alpha gate — only execute if the firing wallet has proven
        # category-specific edge in our clean-data alpha scoring. Synthetic
        # wallets (velocity_detector / order_book_monitor) bypass the gate
        # because they don't have wallet-derived alpha — they're already
        # rate-limited by the fillability floor + per-signal-type floors.
        # See reports/signal_analysis_clean.md and alpha_scores.py.
        gate_wallet = signal.get("wallet") or ""
        gate_category = signal.get("category") or "other"
        if gate_wallet and gate_wallet not in ("velocity_detector", "order_book_monitor"):
            try:
                from trading_platform.polymarket.alpha_scores import get_wallet_alpha
                alpha = get_wallet_alpha(str(self._wallet_db_path), gate_wallet, gate_category)
                gate_tier = signal.get("wallet_tier")
                if alpha <= 0:
                    if gate_tier in ("tier1h", "tier1"):
                        signal["alpha_score"] = 0.0
                        logger.info(
                            "[ALPHA_GATE] wallet %s %s bypass (no alpha row), signal=%s",
                            gate_wallet[:14], gate_tier, signal_type,
                        )
                    else:
                        logger.info(
                            "[ALPHA_GATE] wallet %s NOT copyable in %s, SKIP %s",
                            gate_wallet[:14], gate_category, signal_type,
                        )
                        return None
                else:
                    logger.info(
                        "[ALPHA_GATE] wallet %s copyable in %s, score=%.3f, signal=%s",
                        gate_wallet[:14], gate_category, alpha, signal_type,
                    )
                    signal["alpha_score"] = alpha
            except Exception as exc:
                logger.debug("alpha gate lookup failed: %s", exc)

        # Entry price filter — validated bounds from win_rate_validation.md.
        # Hard-reject missing or out-of-band prices so penny-market noise
        # (0.001 entries resolving to 1.0 = $1.5M phantom PnL) can't slip in.
        entry_price_check = signal.get("price")
        try:
            ep = float(entry_price_check) if entry_price_check is not None else None
        except (TypeError, ValueError):
            ep = None
        if ep is None or ep < MIN_ENTRY_PRICE or ep > MAX_ENTRY_PRICE:
            logger.info(
                "[SIGNAL\u2192TRADE] SKIP: %s entry_price=%s outside fillable band [%s, %s]",
                signal_type, ep, MIN_ENTRY_PRICE, MAX_ENTRY_PRICE,
            )
            return None
        if OPTIMAL_BAND_LOW <= ep <= OPTIMAL_BAND_HIGH:
            confidence = min(0.95, confidence * OPTIMAL_BAND_BOOST)

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

        # ── Execution gates ─────────────────────────────────────────────
        # Check spread, depth, staleness, exposure, drawdown BEFORE sizing.
        # All gates fail-open on API errors (don't block on flaky CLOB).
        gate_results: dict = {}
        try:
            from trading_platform.polymarket.execution_gates import ExecutionGates
            gates = ExecutionGates(db_path=str(self._wallet_db_path), mode="paper")
            ev = signal.get("alpha_score", 0) or 0  # rough EV proxy
            # Pick the side-correct clob token for depth/spread checks.
            # Signal now carries yes_token_id / no_token_id explicitly;
            # falls back to the generic token_id field if missing.
            _want_yes = (signal.get("direction") or "BUY").upper() == "BUY"
            _tok = (
                signal.get("yes_token_id") if _want_yes else signal.get("no_token_id")
            ) or signal.get("token_id") or signal.get("asset_id")
            should_trade, gate_results = gates.run_all_gates(
                token_id=_tok,
                expected_ev=ev,
                stake=MIN_STAKE,  # pre-sizing check with minimum
                category=category,
                bankroll=STARTING_BANKROLL,
                starting_bankroll=STARTING_BANKROLL,
                whale_trade_price=signal.get("price"),
                whale_trade_ts=signal.get("timestamp") or signal.get("trade_ts"),
            )
            if not should_trade:
                failed = [k for k, v in gate_results.items()
                          if isinstance(v, dict) and not v.get("passed", True)]
                logger.info("[EXEC_GATE] SKIP: %s — %s", failed, signal_type)
                return None
        except Exception as exc:
            logger.debug("execution gates failed (pass-through): %s", exc)

        # Kelly-based position sizing — validated from win_rate_validation.md.
        # PROBATION signals get minimum stake; others use half-Kelly.
        from trading_platform.polymarket.whale_signal_engine import PROBATION_SIGNAL_TYPES
        ep = float(signal.get("price") or 0.50)

        with self._wallet_lock:
            deployed = self._wallet_conn.execute(
                "SELECT COALESCE(SUM(size_usd), 0) FROM polymarket_paper_trades WHERE exit_ts IS NULL AND archived = 0"
            ).fetchone()[0]

        if signal_type in PROBATION_SIGNAL_TYPES:
            stake = MIN_STAKE_USD
            size_reason = f"probation: ${MIN_STAKE_USD:.0f} (gathering data)"
        else:
            available = max(0, STARTING_BANKROLL - deployed)
            base = available * HALF_KELLY * confidence
            # Optimal band gets full size; edge band gets 70%
            if OPTIMAL_BAND_LOW <= ep <= OPTIMAL_BAND_HIGH:
                band_adj = 1.0
            else:
                band_adj = 0.7
            stake = round(max(MIN_STAKE_USD, min(base * band_adj, MAX_STAKE_USD)), 2)
            # Check portfolio concentration
            if deployed + stake > STARTING_BANKROLL * MAX_PORTFOLIO_PCT:
                stake = max(0, round(STARTING_BANKROLL * MAX_PORTFOLIO_PCT - deployed, 2))
            size_reason = (
                f"half-Kelly={HALF_KELLY:.0%} conf={confidence:.2f} "
                f"band={'optimal' if band_adj == 1.0 else 'edge'} "
                f"stake=${stake:.0f}"
            )

        if stake < MIN_STAKE_USD:
            return None

        # Category concentration limit
        try:
            with self._wallet_lock:
                cat_deployed = self._wallet_conn.execute(
                    "SELECT COALESCE(SUM(size_usd), 0) FROM polymarket_paper_trades WHERE exit_ts IS NULL AND archived=0 AND category=?",
                    (category,),
                ).fetchone()[0]
            if cat_deployed + stake > STARTING_BANKROLL * MAX_CATEGORY_PCT:
                logger.info("[GATE] %s category at $%.0f (%.0f%%), cap is %.0f%% — rejected",
                            category, cat_deployed, cat_deployed / STARTING_BANKROLL * 100, MAX_CATEGORY_PCT * 100)
                return None
        except Exception:
            pass

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
        # Wallet-signal multiplier source: prefer the alpha score (clean
        # data, per-wallet, per-category) when available; fall back to the
        # legacy WalletTieringEngine multiplier for wallets we haven't
        # scored yet (sample size < MIN_SAMPLE in alpha_scores).
        dynamic_tier_mult = signal.get("alpha_score")  # set by the alpha gate above
        if dynamic_tier_mult is None or dynamic_tier_mult <= 0:
            try:
                from trading_platform.polymarket.wallet_tiering import WalletTieringEngine
                dynamic_tier_mult = WalletTieringEngine(str(self._wallet_db_path)).get_tier_multiplier(
                    wallet, signal.get("category", "other"),
                )
            except Exception as exc:
                logger.debug("dynamic tier lookup failed: %s", exc)
                dynamic_tier_mult = None

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
            if stake < MIN_STAKE_USD:
                return None
        except Exception as exc:
            logger.debug("fusion gate skipped: %s", exc)

        side = "YES" if direction == "BUY" else "NO"
        category = signal.get("category", "other")
        question = signal.get("question", "")
        raw_entry_price = signal.get("price")
        now_ts = int(time.time())

        # Apply CostModel on entry so paper P&L mirrors real execution.
        # Previously: raw signal price was stored; costs only applied on
        # _close_position_early (exits). That left resolution-exit trades
        # uncosted at entry, systematically over-reporting paper EV by
        # ~2% per trade — inflating the kill switch's EV gate above reality.
        entry_price = raw_entry_price
        entry_spread_cost = None
        entry_slippage_cost = None
        try:
            if raw_entry_price is not None:
                from trading_platform.polymarket.cost_model import CostModel
                cm = CostModel()
                ec = cm.entry_cost(float(raw_entry_price), side, float(stake))
                entry_price = ec.effective_price
                entry_spread_cost = ec.spread_cost
                entry_slippage_cost = ec.slippage_cost
        except Exception as exc:
            logger.debug("entry cost model failed: %s", exc)

        try:
            import json as _json
            fusion_blob = _json.dumps(fusion_dict) if fusion_dict else None
            fusion_score_val = fusion_dict.get("score") if fusion_dict else None
            with self._wallet_lock:
                cursor = self._wallet_conn.execute(
                    """INSERT INTO polymarket_paper_trades
                       (condition_id, question, category, side, entry_price,
                        raw_entry_price, spread_cost, slippage_cost,
                        size_usd, signal_type, confidence, wallet, entry_ts,
                        fusion_score, fusion_components, wallet_tier_at_fire,
                        archived)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)""",
                    (condition_id, question, category, side, entry_price,
                     raw_entry_price, entry_spread_cost, entry_slippage_cost,
                     stake, signal_type, confidence, wallet, now_ts,
                     fusion_score_val, fusion_blob, signal.get("wallet_tier")),
                )
                trade_id = cursor.lastrowid
                self._wallet_conn.commit()

            print(f"[PAPER] Placed ${stake:.0f} {side} {signal_type} conf={confidence:.2f} | {question[:40]}")

            # Telegram trade alert + persisted trade hypothesis.
            # Skipped for technical-scanner trades (velocity_detector /
            # order_book_monitor) — those have no wallet basis and the
            # hypothesis generator has nothing meaningful to say.
            if wallet and wallet not in ("velocity_detector", "order_book_monitor"):
                hypothesis_text = None
                try:
                    from trading_platform.polymarket.trade_hypotheses import (
                        build_hypothesis, persist_hypothesis,
                    )
                    hypo = build_hypothesis(
                        str(self._wallet_db_path),
                        wallet=wallet,
                        category=category,
                        signal_type=signal_type,
                        market_slug=signal.get("slug") or "",
                        market_question=question or "",
                        direction=side,
                        entry_price=float(entry_price or 0),
                        convergence_count=int(signal.get("converging_wallets") or 0),
                    )
                    # Attach execution context to hypothesis
                    hypo.position_size_reason = size_reason
                    if signal.get("trade_ts") and signal.get("fired_at"):
                        hypo.time_since_whale_trade = int(signal["fired_at"]) - int(signal["trade_ts"])
                    elif signal.get("timestamp") and signal.get("fired_at"):
                        hypo.time_since_whale_trade = int(signal["fired_at"]) - int(signal["timestamp"])
                    # Attach gate results for post-trade analysis
                    if gate_results:
                        import json as _json
                        hypo.gate_results_json = _json.dumps(gate_results, default=str)
                        spread_data = gate_results.get("spread", {})
                        hypo.market_spread_at_entry = spread_data.get("spread_pct")
                        dd_data = gate_results.get("drawdown", {})
                        hypo.drawdown_at_entry = dd_data.get("drawdown")
                        hypo.size_multiplier = dd_data.get("size_multiplier")
                    hypothesis_text = hypo.thesis
                    persist_hypothesis(str(self._wallet_db_path), hypo, trade_id=trade_id)
                except Exception as exc:
                    logger.debug("hypothesis generation failed: %s", exc)

                try:
                    from trading_platform.polymarket.alert_manager import get_alert_manager
                    # Build enhanced context for the Telegram alert
                    extra_context = ""
                    try:
                        if hypo and hypo.expected_avg_win:
                            extra_context = (
                                f"\nEXPECTED: avg win +${hypo.expected_avg_win:.0f} "
                                f"avg loss ${hypo.expected_avg_loss:.0f}"
                            )
                            if hypo.expected_profit_factor and hypo.expected_profit_factor < 99:
                                extra_context += f" PF:{hypo.expected_profit_factor:.1f}x"
                            if hypo.expected_kelly:
                                extra_context += f" Kelly:{hypo.expected_kelly*100:.0f}%"
                            if hypo.wallet_streak:
                                extra_context += f" streak:{hypo.wallet_streak}"
                        if size_reason:
                            extra_context += f"\nSIZING: {size_reason}"
                    except Exception:
                        pass
                    full_hypothesis = (hypothesis_text or "") + extra_context
                    get_alert_manager().alert_trade_placed(
                        signal_type=signal_type,
                        wallet=wallet,
                        market=question,
                        direction=side,
                        stake=float(stake),
                        entry_price=float(entry_price or 0),
                        fusion_score=fusion_score_val,
                        wallet_tier=signal.get("wallet_tier"),
                        hypothesis=full_hypothesis,
                        alpha_score=signal.get("alpha_score"),
                    )
                except Exception as exc:
                    logger.debug("AlertManager trade_placed failed: %s", exc)

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
        # Trades older than this with no Gamma record are written off
        # as expired (pnl=0). Without this, paper trades placed against
        # markets that later get deindexed sit "open" forever and the
        # whole bankroll stays locked. See data_validation.md DV-12.
        EXPIRY_GRACE_SECONDS = 7 * 24 * 3600

        expired_count = 0
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
                    # Market vanished from Gamma. If the trade is older
                    # than the grace period, mark it expired so it doesn't
                    # sit open forever.
                    age = int(time.time()) - int(entry_ts or 0)
                    if age > EXPIRY_GRACE_SECONDS:
                        with self._wallet_lock:
                            self._wallet_conn.execute(
                                """UPDATE polymarket_paper_trades
                                   SET exit_ts = ?, exit_price = NULL,
                                       realized_pnl = 0, return_pct = 0,
                                       outcome = 'expired'
                                   WHERE id = ?""",
                                (int(time.time()), trade_id),
                            )
                            self._wallet_conn.commit()
                        expired_count += 1
                        logger.info(
                            "[EXPIRE] %s id=%d age=%dd — Gamma deindexed market %s",
                            sig_type, trade_id, age // 86400, (cid or "")[:18],
                        )
                        # Mark hypothesis as expired so the scorecard
                        # doesn't count it as either a win or a loss.
                        try:
                            from trading_platform.polymarket.trade_hypotheses import mark_resolved
                            mark_resolved(
                                str(self._wallet_db_path),
                                trade_id=trade_id,
                                outcome="expired",
                                realized_pnl=0,
                            )
                        except Exception:
                            pass
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

                # Mark the trade's hypothesis (if any) as resolved so the
                # KPI tracker can update the thesis scorecard. Hypothesis
                # rows only exist for real-wallet trades — synthetic trades
                # never generated one.
                try:
                    from trading_platform.polymarket.trade_hypotheses import mark_resolved
                    mark_resolved(
                        str(self._wallet_db_path),
                        trade_id=trade_id,
                        outcome=outcome,
                        realized_pnl=pnl,
                    )
                except Exception as exc:
                    logger.debug("hypothesis mark_resolved failed: %s", exc)

                # Telegram trade-resolved alert via AlertManager.
                try:
                    from trading_platform.polymarket.alert_manager import get_alert_manager
                    # Compute cumulative P&L + win rate from the wallet DB.
                    with self._wallet_lock:
                        agg = self._wallet_conn.execute(
                            """SELECT COALESCE(SUM(realized_pnl), 0),
                                      SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END),
                                      COUNT(*)
                               FROM polymarket_paper_trades
                               WHERE archived = 0 AND exit_ts IS NOT NULL"""
                        ).fetchone()
                    cum_pnl = float(agg[0] or 0)
                    wins = int(agg[1] or 0)
                    total = int(agg[2] or 0)
                    wr = (wins / total) if total else None
                    get_alert_manager().alert_trade_resolved(
                        signal_type=sig_type,
                        market=question or "",
                        direction=side,
                        entry_price=float(entry_price or 0),
                        exit_price=float(final_value),
                        pnl=float(pnl),
                        cumulative_pnl=cum_pnl,
                        win_rate=wr,
                        resolved_count=total,
                    )
                except Exception as exc:
                    logger.debug("AlertManager trade_resolved failed: %s", exc)

                # Feed the cumulative-drawdown circuit breaker
                try:
                    from trading_platform.polymarket.circuit_breaker import CircuitBreaker
                    CircuitBreaker(str(self._wallet_db_path)).record_trade(
                        pnl_dollars=pnl,
                        trade_details={
                            "signal_type": sig_type,
                            "trade_id": trade_id,
                            "side": side,
                            "size_usd": size_usd,
                            "outcome": outcome,
                        },
                    )
                except Exception as exc:
                    logger.debug("circuit breaker record failed: %s", exc)

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

        logger.info(
            "Resolution check: %d open, %d resolved, %d expired",
            len(open_trades), resolved_count, expired_count,
        )
        return {
            "checked": len(open_trades),
            "resolved": resolved_count,
            "expired": expired_count,
        }

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

    # ── Exit logic + mark-to-market ────────────────────────────────────────

    STOP_LOSS = -0.25         # exit if unrealized loss > 25%
    TAKE_PROFIT = 0.40        # exit if unrealized gain > 40%
    TIME_DECAY_DAYS = 30      # exit if held > 30 days with no resolution
    TIME_DECAY_MIN_MOVE = 0.05
    # Market-life exit: a position past 80% of market lifetime rarely improves —
    # either resolve naturally (captured by check_and_resolve_open_trades) or
    # is stuck. Close and free the capital. See scripts/wallet_deep_dive.py.
    MARKET_LIFE_PCT_EXIT = 0.80
    # Implied-probability shift exit: YES-token moved 30pp since entry is a
    # strong signal the market has resolved directionally (even if UMA hasn't
    # finalized yet). Lock in the PnL at mark.
    IMPLIED_SHIFT_EXIT = 0.30

    def check_exits(self) -> dict[str, int]:
        """Check all open positions for exit conditions.

        Runs alongside check_and_resolve_open_trades(). Closes positions
        that hit stop-loss, take-profit, or time-decay thresholds.
        Returns {checked, exited, exit_reasons: {reason: count}}.
        """
        positions = self.get_open_positions()
        exited = 0
        reasons: dict[str, int] = {}

        for pos in positions:
            cid = pos.get("condition_id")
            side = pos.get("side", "YES")
            entry = float(pos.get("entry_price") or 0)
            if not cid or entry <= 0:
                continue

            current = self._fetch_mid_price(cid)
            if current is None:
                continue

            # Compute unrealized return
            if side in ("YES", "BUY"):
                unrealized = (current - entry) / entry
            else:
                unrealized = (entry - current) / max(1 - entry, 0.01)

            # Update mark-to-market
            self._update_mark(pos["id"], current, unrealized * float(pos.get("size_usd") or 0))

            exit_reason = None

            if unrealized <= self.STOP_LOSS:
                exit_reason = "stop_loss"
            elif unrealized >= self.TAKE_PROFIT:
                exit_reason = "take_profit"
            else:
                age_days = (time.time() - (pos.get("entry_ts") or time.time())) / 86400
                if age_days > self.TIME_DECAY_DAYS and abs(unrealized) < self.TIME_DECAY_MIN_MOVE:
                    exit_reason = "time_decay"

                # Implied-probability shift: YES-token price moved 30pp+ since
                # entry — likely directionally resolved even if UMA hasn't
                # finalized. Exit to lock in PnL at mark.
                if not exit_reason:
                    implied_shift = abs(current - entry)
                    if implied_shift >= self.IMPLIED_SHIFT_EXIT:
                        exit_reason = "implied_shift"

                # Market-life % exit: position is past 80% of market lifetime
                # and hasn't resolved. Free the capital rather than tie it up
                # waiting for a late resolution. Needs endDate from universe.
                if not exit_reason:
                    try:
                        end_iso = self._lookup_market_end_date(cid)
                        if end_iso:
                            from datetime import datetime, timezone
                            clean = end_iso.replace("Z", "+00:00") if end_iso.endswith("Z") else end_iso
                            end_dt = datetime.fromisoformat(clean)
                            if end_dt.tzinfo is None:
                                end_dt = end_dt.replace(tzinfo=timezone.utc)
                            entry_ts = pos.get("entry_ts") or time.time()
                            entry_dt = datetime.fromtimestamp(float(entry_ts), tz=timezone.utc)
                            now_dt = datetime.fromtimestamp(time.time(), tz=timezone.utc)
                            life_total = (end_dt - entry_dt).total_seconds()
                            life_elapsed = (now_dt - entry_dt).total_seconds()
                            if life_total > 0:
                                life_pct = life_elapsed / life_total
                                if life_pct >= self.MARKET_LIFE_PCT_EXIT:
                                    exit_reason = "market_life_expired"
                    except Exception as exc:
                        logger.debug("market_life check failed for %s: %s", cid[:14], exc)

            if exit_reason:
                self._close_position_early(pos, current, exit_reason)
                exited += 1
                reasons[exit_reason] = reasons.get(exit_reason, 0) + 1

        return {"checked": len(positions), "exited": exited, "exit_reasons": reasons}

    def _lookup_market_end_date(self, condition_id: str) -> str | None:
        """Return the market's endDate ISO string.

        Two-tier lookup:
        1. MarketUniverse cached JSON (cheap, covers subscribed markets)
        2. Gamma API on-demand (for paper-trade markets outside the universe).
           Result cached on the executor instance so subsequent calls are free.
        """
        if not hasattr(self, "_universe_end_dates"):
            self._universe_end_dates: dict[str, str | None] = {}
            try:
                from trading_platform.polymarket.market_universe import MarketUniverse
                mu = MarketUniverse()
                mu.load_cached(max_age_hours=24.0)
                for cat, items in mu._by_category.items():
                    for m in items:
                        cid = m.get("condition_id")
                        ed = m.get("end_date_iso")
                        if cid and ed:
                            self._universe_end_dates[cid] = ed
            except Exception as exc:
                logger.debug("universe lookup init failed: %s", exc)

        if condition_id in self._universe_end_dates:
            return self._universe_end_dates[condition_id]

        # Fallback: Gamma API on-demand. Cache both hits and None-misses.
        try:
            import requests
            r = requests.get(
                "https://gamma-api.polymarket.com/markets",
                params={"condition_ids": condition_id},
                timeout=5,
            )
            if r.status_code == 200:
                data = r.json()
                m = data[0] if isinstance(data, list) and data else None
                if isinstance(m, dict):
                    ed = m.get("endDate") or m.get("endDateIso")
                    self._universe_end_dates[condition_id] = ed or None
                    return ed or None
        except Exception as exc:
            logger.debug("gamma endDate fallback failed: %s", exc)

        self._universe_end_dates[condition_id] = None
        return None

    def _fetch_mid_price(self, condition_id: str) -> float | None:
        """Fetch current mid-price for a market from signals or live ticks."""
        try:
            with self._wallet_lock:
                row = self._wallet_conn.execute(
                    """SELECT price FROM market_signals
                       WHERE condition_id = ?
                       ORDER BY fired_at DESC LIMIT 1""",
                    (condition_id,),
                ).fetchone()
                if row and row[0]:
                    return float(row[0])
        except Exception:
            pass
        return None

    def _update_mark(self, trade_id: int, current_price: float, unrealized: float) -> None:
        """Update mark-to-market on an open position, including MAE/MFE.

        MAE (Maximum Adverse Excursion) and MFE (Maximum Favorable Excursion)
        are the worst and best unrealized PnL points the position reached
        during its life. Tracking these enables risk-adjusted EV analysis
        per signal type and exit-rule tuning. Schema columns auto-created
        if missing — lazy migration pattern.
        """
        try:
            with self._wallet_lock:
                # Lazy-add the mae/mfe columns; idempotent via IF NOT EXISTS
                for col in ("mae", "mfe"):
                    try:
                        self._wallet_conn.execute(
                            f"ALTER TABLE polymarket_paper_trades ADD COLUMN {col} REAL"
                        )
                    except Exception:
                        pass  # already exists
                # MAE = minimum unrealized (most negative). MFE = maximum.
                unr_rounded = round(unrealized, 2)
                self._wallet_conn.execute(
                    """UPDATE polymarket_paper_trades
                       SET last_mark_price = ?, last_mark_ts = ?, unrealized_pnl = ?,
                           mae = CASE WHEN mae IS NULL OR ? < mae THEN ? ELSE mae END,
                           mfe = CASE WHEN mfe IS NULL OR ? > mfe THEN ? ELSE mfe END
                       WHERE id = ?""",
                    (current_price, int(time.time()), unr_rounded,
                     unr_rounded, unr_rounded, unr_rounded, unr_rounded, trade_id),
                )
                self._wallet_conn.commit()
        except Exception as exc:
            logger.debug("mark update failed: %s", exc)

    def _close_position_early(self, pos: dict, exit_price: float, reason: str) -> None:
        """Close a paper trade early (not via resolution)."""
        from trading_platform.polymarket.cost_model import CostModel
        cm = CostModel()

        side = pos.get("side", "YES")
        size = float(pos.get("size_usd") or 0)
        entry = float(pos.get("entry_price") or 0)

        exit_cost = cm.exit_cost(exit_price, side, size)

        if side in ("YES", "BUY"):
            pnl = (exit_cost.effective_price - entry) * size / max(entry, 0.01)
        else:
            pnl = (entry - exit_cost.effective_price) * size / max(1 - entry, 0.01)

        return_pct = round((pnl / size) * 100, 2) if size > 0 else 0
        outcome = "win" if pnl > 0 else "loss"

        try:
            with self._wallet_lock:
                self._wallet_conn.execute(
                    """UPDATE polymarket_paper_trades
                       SET exit_price = ?, exit_ts = ?, outcome = ?,
                           return_pct = ?, realized_pnl = ?, exit_reason = ?,
                           raw_exit_price = ?, exit_spread_cost = ?,
                           exit_slippage_cost = ?, total_costs = ?
                       WHERE id = ?""",
                    (exit_cost.effective_price, int(time.time()), outcome,
                     round(return_pct, 4), round(pnl, 2), reason,
                     exit_price, exit_cost.spread_cost, exit_cost.slippage_cost,
                     round(exit_cost.total_cost + float(pos.get("spread_cost") or 0) + float(pos.get("slippage_cost") or 0), 4),
                     pos["id"]),
                )
                self._wallet_conn.commit()
            logger.info(
                "[EXIT] %s trade #%d: %s @ %.3f → %.3f pnl=$%.2f (%s)",
                reason, pos["id"], side, entry, exit_cost.effective_price, pnl, outcome,
            )
        except Exception as exc:
            logger.debug("close_position_early failed: %s", exc)

    def snapshot_equity(self) -> dict[str, Any]:
        """Record a point on the equity curve."""
        positions = self.get_open_positions()
        realized = 0.0
        positions_value = 0.0

        with self._wallet_lock:
            row = self._wallet_conn.execute(
                """SELECT COALESCE(SUM(realized_pnl), 0)
                   FROM polymarket_paper_trades
                   WHERE exit_ts IS NOT NULL AND archived = 0"""
            ).fetchone()
            realized = float(row[0]) if row else 0

        unrealized = 0.0
        for pos in positions:
            mark = pos.get("last_mark_price") or pos.get("entry_price") or 0
            entry = pos.get("entry_price") or 0
            size = pos.get("size_usd") or 0
            side = pos.get("side", "YES")
            if side in ("YES", "BUY"):
                u = (float(mark) - float(entry)) * float(size) / max(float(entry), 0.01)
            else:
                u = (float(entry) - float(mark)) * float(size) / max(1 - float(entry), 0.01)
            unrealized += u
            positions_value += float(size) + u

        starting = STARTING_BANKROLL
        cash = starting + realized - sum(float(p.get("size_usd") or 0) for p in positions)
        total_equity = cash + positions_value

        try:
            with self._wallet_lock:
                self._wallet_conn.execute(
                    """INSERT OR REPLACE INTO paper_equity_snapshots
                       (ts, cash, positions_value, total_equity, open_count,
                        realized_pnl_cumulative, unrealized_pnl)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (int(time.time()), round(cash, 2), round(positions_value, 2),
                     round(total_equity, 2), len(positions),
                     round(realized, 2), round(unrealized, 2)),
                )
                self._wallet_conn.commit()
        except Exception as exc:
            logger.debug("equity snapshot failed: %s", exc)

        return {
            "cash": round(cash, 2),
            "positions_value": round(positions_value, 2),
            "total_equity": round(total_equity, 2),
            "open_positions": len(positions),
            "realized_pnl": round(realized, 2),
            "unrealized_pnl": round(unrealized, 2),
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
