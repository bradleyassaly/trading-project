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

# 2026-04-25: register Phase B independent signal so it passes the
# `signal_type not in SIGNAL_BANKROLL` reject at the top of
# execute_signal. Bankroll allocation modest until validation.
# (Constant defined alongside the others below.)
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
    "order_flow_imbalance":   10_000,   # order book bid/ask imbalance — new alpha source
    "price_momentum":         10_000,   # 24h price trend continuation/reversal
    "resolution_proximity":   10_000,   # high-confidence markets near resolution
    # 2026-04-25: Phase B independent signal. Decoupled from whale flow.
    # Modest 5K bankroll until validated.
    "resolution_decay":        5_000,
}

MIN_CONFIDENCE = 0.35

# Per-signal-type confidence floor overrides. Signal types with validated
# edge get a lower floor so we collect more resolution data — otherwise
# whale_entry_filtered at 0.29 confidence is silently dropped. Learned
# from the 1929-fires/1-placed ratio over a 24h window.
MIN_CONFIDENCE_BY_TYPE = {
    "price_velocity": 0.25,
    "whale_entry_filtered": 0.25,
    "high_conviction_insider": 0.30,
    "insider_entry": 0.30,
    "specialist_entry": 0.30,
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
# Raised from 0.65 → 0.85 to catch Pattern A (Late Big Favorite). Top-
# category insider wallets (0x96489abcb9 econ PF 3.8, 0x5bffcf561b
# politics PF 10) enter at 0.70–0.85 on liquid markets right before
# resolution. The 0.65 cap was originally set to avoid penny-odds bugs
# at 0.95+, which are still excluded. 0.85 is comfortably liquid.
MAX_ENTRY_PRICE = 0.85

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
# Added entertainment, science, tech on 2026-04-15. Insider backtest
# showed entertainment +140% ROI/trade (176 trades), science +182% (191
# trades), and tech/tweet_count had meaningful positive PnL on insider
# wallets. Previously these got silently rejected at the CAT_GATE and
# we missed a large chunk of copyable signal flow.
PAPER_TRADE_CATEGORIES = {
    "politics", "geopolitics", "crypto", "economics", "sports",
    "entertainment", "science", "tech", "tweet_count", "finance",
}
TIER1_ONLY_CATEGORIES = {"sports"}  # require wallet_tier='tier1' for these
EXCLUDE_CATEGORIES: set[str] = set()  # no hard exclusions; tier-gating handles it
# Live trading: strict allowlist (statistically significant positive EV only).
# 2026-04-18 expansion rationale (paper-trade data, archived=0, post-price_velocity cleanup):
#   sports   : 75 closed, 46.7% WR, +49% avg return, +$965 PnL  ← added
#   crypto   : 10 closed, 60.0% WR, +109% avg return, +$11 PnL   ← added
#   politics : 3 closed, 0% WR, -$10 PnL (tiny sample, kept)
#   geopolitics: 11 closed, 27.3% WR, -$124 PnL (tiny sample, kept — under review)
# Excluded (negative or marginal): entertainment, science, other
# Sports WR < 52% but positive skew (right-tail wins) gives positive EV;
# Kelly sizing handles that correctly. Kill switch enforces per-trade cap.
LIVE_TRADE_CATEGORIES = {"politics", "geopolitics", "sports", "crypto"}
# Signal types to exclude from paper bankroll (fire+record only, no capital).
# 2026-04-27: max-hours-to-resolve preference. Resolved-hypothesis
# throughput is the binding constraint on ladder progression. Markets
# resolving within MAX_HOURS_TO_RESOLVE_PREF give same-day calibration
# data; longer-horizon markets are accepted but only when the
# wallet/signal slice is high-conviction (passes the strict filter
# below). Effect: bias toward fast-resolving markets without dropping
# real conviction signals on long-horizon markets.
MAX_HOURS_TO_RESOLVE_PREF = 168  # 7 days
MAX_HOURS_TO_RESOLVE_HARD = 720  # 30 days (above this = always reject)


EXCLUDE_SIGNAL_TYPES = {
    "price_velocity",   # 95% WR, EV +0.004 = noise — 1300/day, pure noise
    # 2026-04-18: signals with ≥5 resolved at <30% WR or strongly negative
    # PnL. Kept from firing at all until diagnosed and fixed.
    "copyable_contrarian",  # 0/7 resolved, 0% WR
    "no_position_entry",    # 1/5 resolved, 20% WR, EV -0.1
    "news_reactor",         # 10 resolved, 20% WR, EV -0.236
    # 2026-04-24: 30d audit (reports/signal_audit_2026-04-24.md) added.
    "consensus_follower",   # 3 resolved, 33% WR, EV -0.40 — too small + negative
}

# 2026-04-24: per-(signal_type, side) block list. The 8d post-Apr-18 cohort
# revealed clean signal types whose NO-side inversions don't preserve alpha.
# Side semantics on Polymarket: (a) "BUY YES" = long the YES contract at the
# YES price, (b) "BUY NO" = long the NO contract at the NO price. A whale-
# wallet signal that's profitable when we mirror its YES buy may be net-
# negative when we infer the contrarian NO from the same wallet's behavior,
# because the inferred-direction edge is weaker than the directly-observed
# one. Filter these tuples until each combo earns ≥45% WR on n≥30.
EXCLUDE_SIGNAL_SIDE = {
    # (signal_type, side): reason — n=, WR=
    ("cascade",          "NO"),  # 1/5 win, 20% WR, -$1.92 PnL
    ("wallet_reversal",  "NO"),  # 7/20 win, 35% WR, -$2.87 PnL
    ("accumulation",     "NO"),  # 0/5 win, 0% WR
    # NOTE: whale_entry_filtered NO is 67% WR (n=6) — explicitly NOT excluded.
}

# Signal clusters — correlated signals share a cluster. TRUE confluence
# is cross-cluster (wallet + microstructure agreeing). Intra-cluster
# signals (whale_entry + accumulation) are the same underlying event
# seen through different lenses — counting them as independent is wrong.
SIGNAL_CLUSTERS = {
    # Wallet cluster — all derived from watching smart money trade
    "whale_entry": "wallet", "whale_entry_filtered": "wallet",
    "accumulation": "wallet", "cascade": "wallet",
    "convergence": "wallet", "oversized_bet": "wallet",
    "wallet_reversal": "wallet", "no_position_entry": "wallet",
    "specialist_entry": "wallet", "network_leader_entry": "wallet",
    "market_maker_flip": "wallet", "copyable_contrarian": "wallet",
    "consensus_follower": "wallet", "strategy_specialist": "wallet",
    "insider_entry": "wallet", "high_conviction_insider": "wallet",
    "late_conviction": "wallet", "tier_entry": "wallet",
    "whale_exit": "wallet", "news_reactor": "wallet",
    # Microstructure cluster — order book / market mechanics
    "order_flow_imbalance": "microstructure",
    "price_velocity": "microstructure",
    # Fundamental cluster — price/time patterns, no wallet dependency
    "price_momentum": "fundamental",
    "resolution_proximity": "fundamental",
}

# Discovery mode: signal types with fewer than this many resolved paper
# trades take a simplified $1-stake path that bypasses Kelly, fusion,
# execution gates, and most category restrictions. This lets us collect
# resolution data on untested signals without bankroll risk.
DISCOVERY_THRESHOLD = 15       # need 15 resolved before graduating to full sizing
DISCOVERY_STAKE_USD = 1.0      # $1 flat — purely informational

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

        # 2026-04-27: HORIZON gate. Hard-rejects markets resolving
        # >30d out (signal data lands too slow to inform calibration).
        # Soft-rejects (downweight confidence) markets resolving in
        # 7-30d unless signal is high-conviction (raw confidence
        # already >= 0.70). Markets <7d to resolve preferred and
        # passed unchanged.
        try:
            with self._wallet_lock:
                _h_row = self._wallet_conn.execute(
                    "SELECT end_date_iso FROM markets WHERE condition_id = ?",
                    (condition_id,),
                ).fetchone()
            if _h_row and _h_row[0]:
                from datetime import datetime, timezone as _tz
                _iso = _h_row[0].replace("Z", "+00:00") if _h_row[0].endswith("Z") else _h_row[0]
                _end = datetime.fromisoformat(_iso)
                if _end.tzinfo is None:
                    _end = _end.replace(tzinfo=_tz.utc)
                _hours = (_end.timestamp() - time.time()) / 3600
                if _hours > MAX_HOURS_TO_RESOLVE_HARD:
                    logger.info(
                        "[HORIZON_GATE] SKIP %s — resolves in %.0fh (>%dh hard cap)",
                        signal_type, _hours, MAX_HOURS_TO_RESOLVE_HARD,
                    )
                    try:
                        from trading_platform.polymarket.decision_trace import trace as _dt
                        _dt(signal=signal, gate="HORIZON_GATE", passed=False,
                            value=_hours, threshold=MAX_HOURS_TO_RESOLVE_HARD,
                            detail=f"horizon {_hours:.0f}h", surface="paper",
                            db_path=str(self._wallet_db_path))
                    except Exception:
                        pass
                    return None
                if _hours > MAX_HOURS_TO_RESOLVE_PREF:
                    # Soft-rejection only when conviction is low
                    if confidence < 0.70:
                        logger.info(
                            "[HORIZON_GATE] SKIP %s — %.0fh out + conf %.2f<0.70",
                            signal_type, _hours, confidence,
                        )
                        try:
                            from trading_platform.polymarket.decision_trace import trace as _dt
                            _dt(signal=signal, gate="HORIZON_GATE_SOFT",
                                passed=False, value=_hours,
                                threshold=MAX_HOURS_TO_RESOLVE_PREF,
                                detail=f"low-conf {confidence:.2f}",
                                surface="paper",
                                db_path=str(self._wallet_db_path))
                        except Exception:
                            pass
                        return None
        except Exception:
            pass

        # 2026-04-25: dynamic decay-flag gate. Originally blocked on
        # decay_flag=1 alone (IC<0 on n>=10). 2026-04-27 retuned: also
        # require lifetime PnL <= 0 in the same signal type. Reason —
        # whale_entry was decay-flagged today (IC=-0.15) but still has
        # +$889 lifetime PnL on n=6 sports trades. IC measures rank
        # correlation between alpha_score and outcome; a profitable
        # signal can have negative IC if the alpha_score isn't well-
        # calibrated yet. Killing it based on IC alone destroys real
        # alpha. Both conditions = real decay; one alone = noise.
        try:
            with self._wallet_lock:
                _row = self._wallet_conn.execute(
                    """SELECT sh.decay_flag,
                              COALESCE((SELECT SUM(pt.realized_pnl)
                                          FROM polymarket_paper_trades pt
                                         WHERE pt.signal_type = sh.signal_type
                                           AND pt.archived = 0
                                           AND pt.realized_pnl IS NOT NULL), 0) AS lifetime_pnl
                         FROM signal_health sh
                        WHERE sh.signal_type = ?""",
                    (signal_type,),
                ).fetchone()
            if _row and int(_row[0] or 0) == 1 and float(_row[1] or 0) <= 0:
                logger.info(
                    "[DECAY_GATE] SKIP %s — IC<0 AND lifetime PnL=%.2f<=0",
                    signal_type, float(_row[1] or 0),
                )
                try:
                    from trading_platform.polymarket.decision_trace import trace as _dt
                    _dt(signal=signal, gate="DECAY_GATE", passed=False,
                        detail=f"{signal_type} IC<0 + PnL<=0", surface="paper",
                        db_path=str(self._wallet_db_path))
                except Exception:
                    pass
                return None
        except Exception:
            pass

        # Per-(signal_type, side) gate: a few signal types preserve alpha
        # only on one side. See EXCLUDE_SIGNAL_SIDE for the rationale per
        # tuple. Block here so the trade is never opened, but signals still
        # log for accuracy tracking.
        _signal_side = (signal.get("side") or "").upper()
        if _signal_side and (signal_type, _signal_side) in EXCLUDE_SIGNAL_SIDE:
            logger.info(
                "[SIDE_GATE] SKIP %s side=%s — not profitable on this side",
                signal_type, _signal_side,
            )
            try:
                from trading_platform.polymarket.decision_trace import trace as _dt
                _dt(signal=signal, gate="SIDE_GATE", passed=False,
                    detail=f"{signal_type}/{_signal_side}",
                    surface="paper", db_path=str(self._wallet_db_path))
            except Exception:
                pass
            return None

        # ── Discovery mode ──────────────────────────────────────────────
        # Signal types with < DISCOVERY_THRESHOLD resolved paper trades
        # take a simplified $1-stake path. Bypasses Kelly, fusion, exec
        # gates, and most category restrictions — keeps entry-price bounds
        # and duplicate-position check for correctness.
        n_resolved = self._count_resolved(signal_type)
        if n_resolved < DISCOVERY_THRESHOLD:
            return self._execute_discovery(signal, signal_type, n_resolved)

        # Category gate (global): proven positive-EV categories only.
        # Resolve unknown categories via the classifier BEFORE gating —
        # previously, a missing category bypassed the allowlist entirely
        # and got INSERTed as "other", letting non-allowlist trades sneak
        # in (130/134 resolved trades landed in "other" this way).
        sig_cat_raw = signal.get("category") or ""
        sig_cat = sig_cat_raw.lower() if isinstance(sig_cat_raw, str) else ""
        # wallet_derived is an internal marker (this market was discovered
        # via wallet trade activity) NOT a real category — if we don't
        # reclassify, every wallet-derived signal gets rejected here and
        # we lose the signal flow (0 paper trades in 24h happened from
        # exactly this). Re-classify on empty/other/wallet_derived.
        if not sig_cat or sig_cat in ("other", "wallet_derived"):
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
            # 2026-04-25: fallback. classify_keywords misses sports
            # markets that `_is_sports_market` catches via "Spread:",
            # "vs.", "NBA" etc. — different keyword sets. Without
            # this fallback every wallet_derived sports signal hit
            # CAT_GATE despite sports being in PAPER_TRADE_CATEGORIES.
            # Cause of the persistent 0.4% conversion ratio.
            if sig_cat in ("", "other", "wallet_derived"):
                try:
                    from trading_platform.polymarket.whale_signal_engine import _is_sports_market
                    if _is_sports_market(sig_cat, signal.get("question") or ""):
                        sig_cat = "sports"
                        signal["category"] = "sports"
                except Exception as exc:
                    logger.debug("sports fallback failed: %s", exc)
        if sig_cat in EXCLUDE_CATEGORIES:
            logger.info("[CAT_GATE] SKIP %s in excluded category %s", signal_type, sig_cat)
            try:
                from trading_platform.polymarket.decision_trace import trace as _dt
                _dt(signal=signal, gate="CAT_GATE_EXCLUDED", passed=False,
                    detail=sig_cat, surface="paper",
                    db_path=str(self._wallet_db_path))
            except Exception:
                pass
            return None
        if sig_cat not in PAPER_TRADE_CATEGORIES:
            logger.info("[CAT_GATE] SKIP %s in unproven category %s", signal_type, sig_cat or "<empty>")
            try:
                from trading_platform.polymarket.decision_trace import trace as _dt
                _dt(signal=signal, gate="CAT_GATE_UNPROVEN", passed=False,
                    detail=sig_cat or "<empty>", surface="paper",
                    db_path=str(self._wallet_db_path))
            except Exception:
                pass
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
        # 2026-04-26: manual blocklist override. Wallets blocked via
        # Telegram inline button or /api/wallet/block are rejected
        # here regardless of alpha_score state. Highest-priority
        # gate; runs before alpha gate. Fail-open if table missing.
        if gate_wallet and gate_wallet not in ("velocity_detector", "order_book_monitor"):
            try:
                with self._wallet_lock:
                    _bl = self._wallet_conn.execute(
                        "SELECT reason FROM wallet_blocklist WHERE wallet = ?",
                        (gate_wallet.lower(),),
                    ).fetchone()
                if _bl:
                    logger.info(
                        "[BLOCKLIST] BLOCK wallet %s (reason=%s, signal=%s)",
                        gate_wallet[:14], (_bl[0] or "")[:60], signal_type,
                    )
                    try:
                        from trading_platform.polymarket.decision_trace import trace as _dt
                        _dt(signal=signal, gate="BLOCKLIST", passed=False,
                            detail=f"manual block: {(_bl[0] or '')[:60]}",
                            surface="paper", db_path=str(self._wallet_db_path))
                    except Exception:
                        pass
                    return None
            except Exception as exc:
                logger.debug("[BLOCKLIST] check failed: %s", exc)

        if gate_wallet and gate_wallet not in ("velocity_detector", "order_book_monitor"):
            try:
                from trading_platform.polymarket.alpha_scores import get_wallet_alpha_status
                status, alpha = get_wallet_alpha_status(
                    str(self._wallet_db_path), gate_wallet, gate_category,
                )
                gate_tier = signal.get("wallet_tier")
                if status == "not_copyable":
                    # 2026-04-18: wallet has scored data in this category
                    # AND is explicitly not copyable — BLOCK the signal.
                    # Root-cause of the accumulation 0/6: a -$70K lifetime
                    # wallet (0x7ea571c4...) with is_copyable=0 in sports
                    # fired 6 signals on Real Madrid YES that all lost,
                    # because the old alpha gate collapsed not_copyable
                    # into the same "bypass with alpha=0" path as unscored.
                    logger.info(
                        "[ALPHA_GATE] BLOCK wallet %s not copyable in %s (signal=%s)",
                        gate_wallet[:14], gate_category, signal_type,
                    )
                    try:
                        from trading_platform.polymarket.decision_trace import trace as _dt
                        _dt(signal=signal, gate="ALPHA_GATE", passed=False,
                            detail=f"not_copyable in {gate_category}",
                            surface="paper", db_path=str(self._wallet_db_path))
                    except Exception:
                        pass
                    return None
                if status == "unscored":
                    # Tier1/1h bypass: known-quality wallets pass even without
                    # per-category alpha scores. Tier2: previously hard-rejected,
                    # but that killed 90%+ of signal flow and blocked learning.
                    # Bypass with zero score (fusion learn-tier handles
                    # the sizing — 25% of already-small stakes).
                    signal["alpha_score"] = 0.0
                    logger.info(
                        "[ALPHA_GATE] wallet %s %s unscored-bypass, signal=%s",
                        gate_wallet[:14], gate_tier or "unknown", signal_type,
                    )
                else:
                    logger.info(
                        "[ALPHA_GATE] wallet %s copyable in %s, score=%.3f, signal=%s",
                        gate_wallet[:14], gate_category, alpha, signal_type,
                    )
                    signal["alpha_score"] = alpha
            except Exception as exc:
                logger.debug("alpha gate lookup failed: %s", exc)

        # 2026-04-18: specialist boost. Pure-train walk-forward on
        # wallet_trades: 65 specialists identified from their first 60%
        # of resolved trades, measured on their last 40% (4,806 OOS
        # trades). Result: 63.4% WR / +$1.8M PnL vs 38.1% / -$94M
        # baseline across 117K trades — a durable 25pp edge, not
        # a look-ahead artifact. When the firing wallet is a specialist
        # in the traded category (>=75% train concentration, >=55% train
        # WR, >=20 resolved, positive train PnL, is_copyable=1), boost
        # confidence 1.25x (cap 0.99) and alpha_score 2x (cap 1.0).
        # Downstream Kelly sizing + fusion scoring both read these,
        # so both stake and gate-passing probability lift for
        # specialist signals. Copy-trade latency is expected to erode
        # the 63.4% specialist WR to ~45-55% in live conditions;
        # that's still well above the 38% baseline.
        if gate_wallet and gate_wallet not in ("velocity_detector", "order_book_monitor"):
            try:
                from trading_platform.polymarket.alpha_scores import is_specialist
                spec = is_specialist(str(self._wallet_db_path), gate_wallet, gate_category)
                if spec["is_specialist"]:
                    signal["is_specialist_source"] = True
                    signal["specialist_concentration"] = spec["concentration"]
                    signal["specialist_win_rate"] = spec["win_rate"]
                    old_conf = confidence
                    confidence = min(0.99, old_conf * 1.25)
                    signal["confidence"] = confidence
                    signal["confidence_pre_specialist_boost"] = old_conf
                    old_alpha = float(signal.get("alpha_score") or 0)
                    signal["alpha_score"] = min(1.0, old_alpha * 2.0)
                    logger.info(
                        "[SPECIALIST_BOOST] %s in %s (conc=%.0f%% wr=%.0f%% n=%d pnl=$%.0f); conf %.2f→%.2f alpha %.3f→%.3f",
                        gate_wallet[:14], gate_category,
                        spec["concentration"] * 100, spec["win_rate"] * 100,
                        spec["resolved_trades"], spec["total_pnl"],
                        old_conf, confidence, old_alpha, signal["alpha_score"],
                    )
            except Exception as exc:
                logger.debug("specialist boost lookup failed: %s", exc)

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

        # 2026-04-18: YES-favorite guard. Original analysis ("18 trades
        # at >=0.7 was -$335") motivated a 0.50 threshold, but the
        # 8d post-Apr-18 cohort showed mid-band 0.50-0.70 is essentially
        # break-even (+$2 / 53 trades / 36% WR), only the >=0.65 favorite
        # band tanks (-$6 / 39 trades / 15% WR). Lifting paper threshold
        # 0.50 → 0.65 to match the live-executor change made earlier;
        # without this match every mid-band BUY signal that reaches the
        # paper executor still hits the 0.50 wall. Cause of the residual
        # high SKIP rate after the sports/category fixes shipped today.
        YES_FAVORITE_BLOCK_PRICE = 0.65
        _dir_upper = (signal.get("direction") or "").upper()
        if _dir_upper == "BUY" and ep is not None and ep >= YES_FAVORITE_BLOCK_PRICE:
            logger.info(
                "[YES_FAV_GATE] BLOCK %s BUY@%.3f (>= %.2f) — copy-trade tail risk",
                signal_type, ep, YES_FAVORITE_BLOCK_PRICE,
            )
            try:
                from trading_platform.polymarket.decision_trace import trace as _dt
                _dt(signal=signal, gate="FAV_GATE", passed=False,
                    value=ep, threshold=YES_FAVORITE_BLOCK_PRICE,
                    detail=f"BUY@{ep:.3f}", surface="paper",
                    db_path=str(self._wallet_db_path))
            except Exception:
                pass
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

        # 2026-04-24: re-entry cap. Allow reopening the same market after
        # an exit, but only twice per (market, source-wallet, side) within
        # the market's lifetime. Without this cap, a fluttering whale
        # can drag us in and out repeatedly. With it, we still benefit
        # from the "whale came back" pattern that motivated lifting the
        # blanket dup check, but we don't pay 5× the spread for 1× edge.
        MAX_REENTRIES = 2
        wallet_for_cap = (signal.get("wallet") or "").lower()
        side_for_cap = (signal.get("side") or "").upper()
        if wallet_for_cap and side_for_cap:
            with self._wallet_lock:
                prior_n = self._wallet_conn.execute(
                    """SELECT COUNT(*) FROM polymarket_paper_trades
                       WHERE condition_id = ? AND wallet = ? AND side = ?
                         AND archived = 0""",
                    (condition_id, wallet_for_cap, side_for_cap),
                ).fetchone()
            if prior_n and int(prior_n[0]) >= MAX_REENTRIES:
                logger.info(
                    "[REENTRY_CAP] %s %s/%s already opened %d×, skipping",
                    signal_type, wallet_for_cap[:14], side_for_cap, int(prior_n[0]),
                )
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

        # ── Ensemble scoring ──────────────────────────────────────────
        # Combine all available features into a single conviction score
        # (0-1) that drives sizing. Replaces the previous independent
        # multiplier chain (confluence × band × time_mult × confidence).
        ensemble = self._compute_ensemble(signal, condition_id, signal_type)
        signal["ensemble_score"] = round(ensemble["score"], 4)
        signal["ensemble_components"] = ensemble["components"]
        confidence = ensemble["score"]

        # 2026-04-25: isotonic calibration on raw confidence. The first
        # calibration run measured Brier=0.35 / miscal=0.31 — alpha_score
        # is systematically overconfident. Without correction, every
        # downstream multiplier (Kelly, STAKE_MULTIPLIERS, behavioral
        # boosts) compounds the bias. apply_calibration() falls back to
        # identity until the curve has been fit at least once.
        try:
            from trading_platform.polymarket.isotonic_calibration import apply_calibration
            raw_conf = confidence
            # Pass category so per-category curve takes precedence;
            # falls back to global curve if no per-category fit exists.
            confidence = apply_calibration(
                confidence, category=category,
                db_path=str(self._wallet_db_path),
            )
            if abs(confidence - raw_conf) > 0.02:
                logger.info(
                    "[CALIB] %s raw=%.3f → calibrated=%.3f",
                    signal_type, raw_conf, confidence,
                )
            signal["confidence_raw"] = round(raw_conf, 4)
            signal["confidence_calibrated"] = round(confidence, 4)
        except Exception as exc:
            logger.debug("[CALIB] apply failed: %s", exc)

        # 2026-04-24: stake concentration on proven (signal, side) winners.
        # Eight-day post-Apr-18 cohort showed three combos earning their
        # capital and one structural price band carrying the entire PnL.
        # Scale confidence by a 1.0-1.5x multiplier so the Kelly-derived
        # stake follows. Cap at 0.95 to stay inside the SignalEvaluator's
        # confidence band; reject anything that would amplify a low-conf
        # signal past its calibration.
        ep = float(signal.get("price") or 0.50)
        _side = (signal.get("side") or "").upper()
        STAKE_MULTIPLIERS = {
            ("wallet_reversal",      "YES"): 1.5,  # 11 trades, 72.7% WR, +$19.74
            ("cascade",              "YES"): 1.3,  # 18 trades, 55.6% WR, +$12.96
            # 2026-04-25: whale_entry_filtered drop from 1.2/1.3× → 0.7×
            # for both sides. 30d data shows 58% WR but -$113 PnL: the
            # SL trades had avg stake $31 vs TP avg $7 — high-confidence
            # trades with biggest stakes are the losers. Calibration is
            # the actual fix (Brier 0.35 = overconfident); until isotonic
            # correction lands, downweight the multiplier so the over-
            # confidence doesn't double-up via STAKE_MULTIPLIERS.
            ("whale_entry_filtered", "NO"):  0.7,
            ("whale_entry_filtered", "YES"): 0.7,
        }
        # Long-shot YES (entry < 0.30) carried 50% WR / +$67 over the week —
        # structurally positive EV at those prices. Layer a 1.25x boost on
        # top of any signal-type multiplier when entry is in this band.
        mult = STAKE_MULTIPLIERS.get((signal_type, _side), 1.0)
        if _side == "YES" and ep > 0 and ep < 0.30:
            mult *= 1.25
        # 2026-04-24: wallet earliness boost. Wallets whose 7d WR exceeds
        # their lifetime WR in this category get up to 1.3× extra; the
        # reverse direction down to 0.7×. Captures "currently smart" vs
        # "was smart" without rewriting the alpha-score table.
        src_wallet = signal.get("wallet") or ""
        try:
            from trading_platform.polymarket.wallet_earliness import get_earliness_boost
            if src_wallet and src_wallet not in ("velocity_detector", "order_book_monitor"):
                eboost = get_earliness_boost(src_wallet, category, db_path=str(self._wallet_db_path))
                if eboost != 1.0:
                    mult *= eboost
                    logger.info(
                        "[EARLINESS] %s in %s ×%.2f",
                        src_wallet[:14], category, eboost,
                    )
        except Exception as exc:
            logger.debug("[EARLINESS] lookup failed: %s", exc)

        # 2026-04-25: behavioral-metrics gates. Three new signals from
        # wallet_behavior_metrics (run daily, see wallet_behavior_metrics.py):
        #   (a) is_likely_farmer = 1 → BLOCK entirely (sybil/wash defense)
        #   (b) z-score specialist in this category → up to 1.4× boost
        #   (c) sizing_p90_pct >= 0.10 (conviction whale) → 1.15× boost,
        #       sizing_p90_pct < 0.02 (mechanical farmer) → 0.85× cut
        # Fail-open on lookup errors so a missing table never starves
        # signal flow.
        if src_wallet and src_wallet not in ("velocity_detector", "order_book_monitor"):
            try:
                with self._wallet_lock:
                    bm_row = self._wallet_conn.execute(
                        "SELECT is_likely_farmer, sizing_p90_pct FROM wallet_behavior_metrics WHERE wallet = ?",
                        (src_wallet.lower(),),
                    ).fetchone()
                    z_row = self._wallet_conn.execute(
                        "SELECT z_score FROM wallet_category_zscore WHERE wallet = ? AND category = ?",
                        (src_wallet.lower(), category),
                    ).fetchone()
                if bm_row and bm_row[0]:
                    logger.info(
                        "[FARMER_GATE] BLOCK %s wallet=%s — flagged farmer/wash",
                        signal_type, src_wallet[:14],
                    )
                    try:
                        from trading_platform.polymarket.decision_trace import trace as _dt
                        _dt(signal=signal, gate="FARMER_GATE", passed=False,
                            detail=f"wallet={src_wallet[:14]}", surface="paper",
                            db_path=str(self._wallet_db_path))
                    except Exception:
                        pass
                    return None
                if z_row and z_row[0] is not None and float(z_row[0]) >= 1.0:
                    z_boost = min(1.4, 1.0 + 0.15 * (float(z_row[0]) - 1.0))
                    if z_boost > 1.0:
                        mult *= z_boost
                        logger.info(
                            "[Z_SPECIALIST] %s in %s z=%.2f ×%.2f",
                            src_wallet[:14], category, float(z_row[0]), z_boost,
                        )

                # 2026-04-27: per-subdomain z-score boost (more granular).
                # Looks up wallet_subdomain_zscore for the SPECIFIC sport
                # / region of this market (sports/tennis vs sports/nba).
                # Stacks multiplicatively on top of the category z-boost
                # but capped — together they can reach max 1.6×.
                signal_subdomain = signal.get("subcategory")
                if signal_subdomain:
                    try:
                        with self._wallet_lock:
                            sz_row = self._wallet_conn.execute(
                                "SELECT z_score, sub_pnl, n_in_subdomain "
                                "FROM wallet_subdomain_zscore "
                                "WHERE wallet = ? AND subdomain = ?",
                                (src_wallet.lower(), signal_subdomain),
                            ).fetchone()
                        if (sz_row and sz_row[0] is not None
                                and float(sz_row[0]) >= 1.0
                                and float(sz_row[1] or 0) > 0):
                            sub_boost = min(1.3, 1.0 + 0.10 * (float(sz_row[0]) - 1.0))
                            mult *= sub_boost
                            logger.info(
                                "[Z_SUBDOMAIN] %s in %s z=%.2f pnl=%.0f ×%.2f",
                                src_wallet[:14], signal_subdomain,
                                float(sz_row[0]), float(sz_row[1] or 0), sub_boost,
                            )
                    except Exception:
                        pass
                if bm_row and bm_row[1] is not None:
                    p90 = float(bm_row[1])
                    if p90 >= 0.10:
                        mult *= 1.15
                        logger.info(
                            "[CONVICTION] %s p90_size=%.1f%% ×1.15",
                            src_wallet[:14], p90 * 100,
                        )
                    elif p90 < 0.02:
                        mult *= 0.85
                        logger.info(
                            "[MECHANICAL] %s p90_size=%.1f%% ×0.85",
                            src_wallet[:14], p90 * 100,
                        )
            except Exception as exc:
                logger.debug("[BEHAVIOR_METRICS] lookup failed: %s", exc)

        if mult != 1.0:
            old_conf = confidence
            confidence = min(0.95, max(0.10, confidence * mult))
            if abs(confidence - old_conf) > 0.01:
                logger.info(
                    "[STAKE_BOOST] %s/%s ep=%.3f conf %.2f→%.2f (×%.2f)",
                    signal_type, _side, ep, old_conf, confidence, mult,
                )

        from trading_platform.polymarket.whale_signal_engine import PROBATION_SIGNAL_TYPES

        with self._wallet_lock:
            deployed = self._wallet_conn.execute(
                "SELECT COALESCE(SUM(size_usd), 0) FROM polymarket_paper_trades WHERE exit_ts IS NULL AND archived = 0"
            ).fetchone()[0]

        if signal_type in PROBATION_SIGNAL_TYPES:
            stake = MIN_STAKE_USD
            size_reason = f"probation: ${MIN_STAKE_USD:.0f} (gathering data)"
        else:
            available = max(0, STARTING_BANKROLL - deployed)
            stake = round(max(MIN_STAKE_USD, min(available * HALF_KELLY * confidence, MAX_STAKE_USD)), 2)
            if deployed + stake > STARTING_BANKROLL * MAX_PORTFOLIO_PCT:
                stake = max(0, round(STARTING_BANKROLL * MAX_PORTFOLIO_PCT - deployed, 2))
            size_reason = f"ensemble={confidence:.2f} stake=${stake:.0f}"

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
                # Instead of hard-rejecting, place at MIN_STAKE_USD for
                # data collection. The multiplicative fusion score collapses
                # to near-zero whenever any component lacks data (missing
                # market vol, untiered wallet, etc.), causing a 1929/1 fire-
                # to-placed ratio over 24h. The gates upstream (confidence,
                # category, price, alpha) already filter for signal quality;
                # fusion was adding a redundant barrier that starved the
                # calibration loop of samples.
                stake = MIN_STAKE_USD
                logger.info(
                    "[FUSION_LEARN] %s score=%.2f → placing at $%.0f min-stake for data | %s",
                    signal_type, fusion.score, stake,
                    (signal.get("question") or "")[:40],
                )
            else:
                stake = round(stake * fusion.stake_multiplier, 2)
                if stake < MIN_STAKE_USD:
                    stake = MIN_STAKE_USD
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
            # Gate-value snapshot: everything the system "saw" at entry time.
            # Enables post-trade evaluation: "which gate values predict wins?"
            entry_ctx = _json.dumps({
                "alpha_score": signal.get("alpha_score"),
                "insider_score": signal.get("insider_score"),
                "wr_at_fire": signal.get("directional_win_rate"),
                "minutes_since_whale": signal.get("minutes_since_whale_entry"),
                "market_volume_usd": signal.get("market_volume_usd"),
                "converging_wallets": signal.get("converging_wallets"),
                "fusion_decision": fusion_dict.get("decision") if fusion_dict else None,
            }, default=str) if True else None
            alpha_at_fire = signal.get("alpha_score")
            with self._wallet_lock:
                cursor = self._wallet_conn.execute(
                    """INSERT INTO polymarket_paper_trades
                       (condition_id, question, category, side, entry_price,
                        raw_entry_price, spread_cost, slippage_cost,
                        size_usd, signal_type, confidence, wallet, entry_ts,
                        fusion_score, fusion_components, wallet_tier_at_fire,
                        alpha_score_at_fire, entry_context,
                        archived)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)""",
                    (condition_id, question, category, side, entry_price,
                     raw_entry_price, entry_spread_cost, entry_slippage_cost,
                     stake, signal_type, confidence, wallet, now_ts,
                     fusion_score_val, fusion_blob, signal.get("wallet_tier"),
                     alpha_at_fire, entry_ctx),
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

    # Default exit thresholds (overridden by _EXIT_PROFILES per signal type).
    STOP_LOSS = -0.25
    TAKE_PROFIT = 0.40
    TRAILING_STOP_ACTIVATE = 0.20   # start trailing after +20% unrealized
    TRAILING_STOP_DRAWBACK = 0.10   # close if price pulls back 10pp from peak (MFE)
    TIME_DECAY_DAYS = 30

    # Per-signal exit profiles — short-horizon signals (sports whale copies)
    # get tighter stops; thesis-driven signals get wider bands.
    _EXIT_PROFILES: dict[str, dict] = {
        "whale_entry_filtered": {"sl": -0.20, "tp": 0.35, "trail_act": 0.15, "trail_back": 0.08, "time_days": 14},
        "high_conviction_insider": {"sl": -0.15, "tp": 0.50, "trail_act": 0.25, "trail_back": 0.12, "time_days": 30},
        "insider_entry": {"sl": -0.20, "tp": 0.40, "trail_act": 0.20, "trail_back": 0.10, "time_days": 21},
        "specialist_entry": {"sl": -0.25, "tp": 0.45, "trail_act": 0.20, "trail_back": 0.10, "time_days": 21},
        "network_leader_entry": {"sl": -0.20, "tp": 0.35, "trail_act": 0.15, "trail_back": 0.08, "time_days": 14},
        "copyable_contrarian": {"sl": -0.30, "tp": 0.50, "trail_act": 0.25, "trail_back": 0.12, "time_days": 30},
        # Discovery signals get wide stops — we want resolution data, not
        # early exits. Let them ride to market resolution where possible.
        "oversized_bet": {"sl": -0.40, "tp": 0.60, "trail_act": 0.30, "trail_back": 0.15, "time_days": 21},
        "cascade": {"sl": -0.40, "tp": 0.60, "trail_act": 0.30, "trail_back": 0.15, "time_days": 21},
        "market_maker_flip": {"sl": -0.40, "tp": 0.60, "trail_act": 0.30, "trail_back": 0.15, "time_days": 21},
        "accumulation": {"sl": -0.40, "tp": 0.60, "trail_act": 0.30, "trail_back": 0.15, "time_days": 21},
        "convergence": {"sl": -0.50, "tp": 0.70, "trail_act": 0.35, "trail_back": 0.20, "time_days": 30},
        "wallet_reversal": {"sl": -0.40, "tp": 0.60, "trail_act": 0.30, "trail_back": 0.15, "time_days": 21},
        "no_position_entry": {"sl": -0.40, "tp": 0.60, "trail_act": 0.30, "trail_back": 0.15, "time_days": 21},
    }
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

            # Whale-mirror exit: if the source wallet has SOLD this market
            # since we entered, they're out — we should be too. The whale's
            # alpha is in timing both sides, not just entry. Previously
            # whale_exit fired as informational-only (59 exits all HOLD).
            source_wallet = pos.get("wallet") or ""
            entry_ts_val = int(pos.get("entry_ts") or 0)
            if source_wallet and source_wallet not in ("velocity_detector", "order_book_monitor") and entry_ts_val:
                try:
                    with self._wallet_lock:
                        sell_row = self._wallet_conn.execute(
                            """SELECT 1 FROM wallet_trades
                               WHERE wallet = ? AND condition_id = ?
                                 AND side = 'SELL' AND timestamp > ?
                               LIMIT 1""",
                            (source_wallet.lower(), cid, entry_ts_val),
                        ).fetchone()
                    if sell_row:
                        exit_reason = "whale_mirror_exit"
                except Exception as exc:
                    logger.debug("whale-mirror check failed for %s: %s", cid[:14], exc)

            # Look up per-signal exit profile (tighter for short-horizon,
            # wider for thesis-driven). Falls back to class-level defaults.
            sig_type = pos.get("signal_type") or ""
            prof = self._EXIT_PROFILES.get(sig_type, {})
            sl = prof.get("sl", self.STOP_LOSS)
            tp = prof.get("tp", self.TAKE_PROFIT)
            trail_act = prof.get("trail_act", self.TRAILING_STOP_ACTIVATE)
            trail_back = prof.get("trail_back", self.TRAILING_STOP_DRAWBACK)
            time_days = prof.get("time_days", self.TIME_DECAY_DAYS)

            # Trailing stop: once MFE exceeds activation, close if price
            # has fallen trail_back from the peak. MFE is tracked by
            # _update_mark → mfe column on each paper_trade row.
            mfe_pct = float(pos.get("mfe") or 0) / max(float(pos.get("size_usd") or 1), 1)
            if exit_reason is None and mfe_pct >= trail_act and unrealized <= (mfe_pct - trail_back):
                exit_reason = "trailing_stop"

            if exit_reason is None and unrealized <= sl:
                exit_reason = "stop_loss"
            elif exit_reason is None and unrealized >= tp:
                exit_reason = "take_profit"
            elif exit_reason is None:
                age_days = (time.time() - (pos.get("entry_ts") or time.time())) / 86400
                if age_days > time_days and abs(unrealized) < self.TIME_DECAY_MIN_MOVE:
                    exit_reason = "time_decay"
                # 2026-04-24: tighter pre-resolution time-decay exit. The
                # 8d cohort showed `market_life_expired` at 24/212 (~11%
                # of closes) hitting near-zero PnL — we were renting the
                # resolution lottery. New rule: if market resolves within
                # 8h AND position PnL is in the "stuck" band [-10%, +5%],
                # close at mark rather than wait for the binary outcome.
                if not exit_reason:
                    try:
                        end_iso = self._lookup_market_end_date(cid)
                        if end_iso:
                            from datetime import datetime, timezone
                            clean = end_iso.replace("Z", "+00:00") if end_iso.endswith("Z") else end_iso
                            end_dt = datetime.fromisoformat(clean)
                            if end_dt.tzinfo is None:
                                end_dt = end_dt.replace(tzinfo=timezone.utc)
                            now_dt = datetime.fromtimestamp(time.time(), tz=timezone.utc)
                            hours_to_resolve = (end_dt - now_dt).total_seconds() / 3600
                            stake = float(pos.get("size_usd") or 1.0)
                            unreal_pct = (float(unrealized) / max(stake, 1)) if stake else 0
                            if 0 < hours_to_resolve <= 8 and -0.10 <= unreal_pct <= 0.05:
                                exit_reason = "pre_resolve_decay"
                    except Exception as exc:
                        logger.debug("pre_resolve_decay check failed for %s: %s", cid[:14], exc)

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
        """Fetch current mid-price for a market from multiple sources.

        Tries in order: market_ticks (freshest), market_signals (signal
        engine prices), market_price_history (historical). Returns None
        only if all sources fail — previously only checked market_signals,
        which caused 10/12 positions to be skipped in check_exits().
        """
        try:
            from trading_platform.polymarket.db_connection import get_connection
            conn = get_connection()
            try:
                # market_ticks — freshest source (live WebSocket data)
                row = conn.execute(
                    "SELECT price FROM market_ticks WHERE condition_id = ? ORDER BY timestamp DESC LIMIT 1",
                    (condition_id,),
                ).fetchone()
                if row and row[0]:
                    return float(row[0])
                # market_signals — signal engine prices
                row = conn.execute(
                    "SELECT price FROM market_signals WHERE condition_id = ? ORDER BY fired_at DESC LIMIT 1",
                    (condition_id,),
                ).fetchone()
                if row and row[0]:
                    return float(row[0])
                # market_price_history — CLOB historical
                row = conn.execute(
                    "SELECT p FROM market_price_history WHERE condition_id = ? ORDER BY t DESC LIMIT 1",
                    (condition_id,),
                ).fetchone()
                if row and row[0]:
                    return float(row[0])
            finally:
                conn.close()
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

    # ── Category EV priors from our signal-level analysis (fillable band) ──
    _CATEGORY_EV = {
        "geopolitics": 0.44, "entertainment": 0.32, "politics": 0.31,
        "sports": -0.06, "crypto": -0.27, "other": -0.27,
        "economics": 0.0, "science": 0.0, "tech": 0.0, "finance": 0.0,
    }

    def _compute_ensemble(
        self, signal: dict, condition_id: str, signal_type: str,
    ) -> dict:
        """Combine all features into a single 0-1 conviction score.

        Weighted sum of normalized components. Each component contributes
        a 0-1 sub-score; weights reflect relative importance from our
        historical EV analysis. The final score drives Kelly sizing
        directly — higher score → bigger stake.

        Returns {score: float, components: dict} for logging/analysis.
        """
        import math
        components: dict[str, float] = {}

        # 1. Raw signal confidence (0-1, already computed by signal engine)
        raw_conf = float(signal.get("confidence") or 0.5)
        components["confidence"] = raw_conf

        # 2. Wallet tier (tier1h=1.0, tier1=0.8, tier2=0.5, unknown=0.3)
        tier = signal.get("wallet_tier") or ""
        tier_scores = {"tier1h": 1.0, "tier1": 0.8, "tier2": 0.5, "market": 0.3}
        components["wallet_tier"] = tier_scores.get(tier, 0.3)

        # 3. Alpha score (wallet's proven category-specific edge, 0-1)
        components["alpha"] = min(1.0, float(signal.get("alpha_score") or 0))

        # 4. Cross-cluster confluence (different signal clusters on same market)
        confluence = self._count_confluence(condition_id, signal_type)
        signal["confluence_count"] = confluence
        signal["signal_cluster"] = SIGNAL_CLUSTERS.get(signal_type, "wallet")
        components["confluence"] = min(1.0, confluence * 0.50)

        # 5. Time efficiency (prefer faster-resolving markets)
        days = self._days_to_close(condition_id)
        if days is not None and days > 0:
            time_score = min(1.0, 1.0 / math.sqrt(max(days, 0.5)))
            signal["days_to_close"] = round(days, 1)
        else:
            time_score = 0.5
        components["time_efficiency"] = time_score

        # 6. Entry price band (sweet spot 0.10-0.50 from wallet_deep_dive)
        ep = float(signal.get("price") or 0.5)
        if OPTIMAL_BAND_LOW <= ep <= OPTIMAL_BAND_HIGH:
            components["price_band"] = 1.0
        elif MIN_ENTRY_PRICE <= ep <= MAX_ENTRY_PRICE:
            components["price_band"] = 0.7
        else:
            components["price_band"] = 0.3

        # 7. Category prior (historical EV by category)
        cat = (signal.get("category") or "other").lower()
        cat_ev = self._CATEGORY_EV.get(cat, 0.0)
        components["category"] = max(0.0, min(1.0, 0.5 + cat_ev))

        # 8. Directional win rate of firing wallet
        wr = float(signal.get("directional_win_rate") or 0)
        components["wallet_wr"] = min(1.0, wr)

        # Weighted combination
        weights = {
            "confidence": 0.20,
            "wallet_tier": 0.15,
            "alpha": 0.15,
            "confluence": 0.10,
            "time_efficiency": 0.10,
            "price_band": 0.10,
            "category": 0.10,
            "wallet_wr": 0.10,
        }
        score = sum(components[k] * weights[k] for k in weights)
        score = max(0.05, min(0.95, score))

        return {"score": score, "components": components}

    def _days_to_close(self, condition_id: str) -> float | None:
        """Return estimated days until market closes, or None if unknown."""
        try:
            from trading_platform.polymarket.db_connection import get_connection
            conn = get_connection()
            try:
                row = conn.execute(
                    "SELECT end_date_iso FROM markets WHERE condition_id = ?",
                    (condition_id,),
                ).fetchone()
                if not row or not row[0]:
                    return None
                from datetime import datetime, timezone
                end = datetime.fromisoformat(row[0].replace("Z", "+00:00"))
                now = datetime.now(tz=timezone.utc)
                delta = (end - now).total_seconds() / 86400.0
                return max(0.0, delta)
            finally:
                conn.close()
        except Exception:
            return None

    def _count_confluence(self, condition_id: str, current_signal_type: str) -> int:
        """Count distinct signal CLUSTERS that fired on the same market in the last hour.

        Only cross-cluster signals count as true confluence. whale_entry +
        accumulation are both 'wallet' cluster — that's one event, not two.
        whale_entry + order_flow_imbalance are different clusters — that's
        real independent confirmation.
        """
        try:
            from trading_platform.polymarket.db_connection import get_connection
            import time as _t
            cutoff = int(_t.time()) - 3600
            conn = get_connection()
            try:
                rows = conn.execute(
                    "SELECT DISTINCT signal_type FROM signal_outcomes "
                    "WHERE condition_id = ? AND fired_at >= ?",
                    (condition_id, cutoff),
                ).fetchall()
                current_cluster = SIGNAL_CLUSTERS.get(current_signal_type, "wallet")
                other_clusters = set()
                for (st,) in rows:
                    cluster = SIGNAL_CLUSTERS.get(st, "wallet")
                    if cluster != current_cluster:
                        other_clusters.add(cluster)
                return len(other_clusters)
            finally:
                conn.close()
        except Exception:
            return 0

    def _count_resolved(self, signal_type: str) -> int:
        """Count resolved paper trades for a signal type (cached per cycle)."""
        if not hasattr(self, "_resolved_cache"):
            self._resolved_cache = {}
            self._resolved_cache_ts = 0
        import time as _t
        now = _t.time()
        if now - self._resolved_cache_ts > 300:
            self._resolved_cache.clear()
            self._resolved_cache_ts = now
        if signal_type in self._resolved_cache:
            return self._resolved_cache[signal_type]
        try:
            from trading_platform.polymarket.db_connection import get_connection
            conn = get_connection()
            try:
                n = conn.execute(
                    "SELECT COUNT(*) FROM polymarket_paper_trades "
                    "WHERE signal_type = ? AND exit_ts IS NOT NULL AND archived = 0",
                    (signal_type,),
                ).fetchone()[0]
            finally:
                conn.close()
        except Exception:
            n = 0
        self._resolved_cache[signal_type] = n
        return n

    def _execute_discovery(
        self, signal: dict[str, Any], signal_type: str, n_resolved: int,
    ) -> dict[str, Any] | None:
        """Place a $1 discovery paper trade — simplified gate path.

        Keeps entry-price bounds and duplicate-position checks (correctness),
        but skips Kelly, fusion, execution gates, and most category
        restrictions so we collect resolution data on untested signals.
        """
        direction = (signal.get("direction") or "").upper()
        if direction not in ("BUY", "SELL"):
            return None
        condition_id = signal.get("condition_id") or signal.get("token_id", "")
        if not condition_id:
            return None

        ep = None
        try:
            ep = float(signal.get("price") or 0)
        except (TypeError, ValueError):
            pass
        if ep is None or ep < MIN_ENTRY_PRICE or ep > MAX_ENTRY_PRICE:
            return None

        with self._wallet_lock:
            existing = self._wallet_conn.execute(
                "SELECT id FROM polymarket_paper_trades "
                "WHERE condition_id = ? AND exit_ts IS NULL AND archived = 0",
                (condition_id,),
            ).fetchone()
        if existing:
            return None

        # Emergency stop still applies
        try:
            if hasattr(self, "_kill_switch"):
                stopped, _ = self._kill_switch.is_emergency_stopped()
                if stopped:
                    return None
        except Exception:
            pass

        side = "YES" if direction == "BUY" else "NO"
        category = signal.get("category") or "other"
        if not category or category in ("other", "wallet_derived"):
            try:
                from trading_platform.polymarket.market_categorizer import classify_keywords
                resolved_cat, _ = classify_keywords(
                    signal.get("slug") or "", signal.get("question") or "",
                )
                if resolved_cat and resolved_cat != "other":
                    category = resolved_cat.lower()
            except Exception:
                pass

        wallet = signal.get("wallet", "")
        question = signal.get("question") or ""
        confidence = signal.get("confidence") or 0
        stake = DISCOVERY_STAKE_USD

        now_ts = int(__import__("time").time())
        with self._wallet_lock:
            self._wallet_conn.execute(
                """INSERT INTO polymarket_paper_trades
                   (condition_id, question, category, side, entry_price,
                    size_usd, signal_type, confidence, wallet, entry_ts,
                    archived, wallet_tier_at_fire, source_wallet,
                    detection_lag_seconds, whale_entry_price, alpha_score_at_fire)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?)""",
                (condition_id, question[:200], category, side, ep,
                 stake, signal_type, confidence, wallet[:42], now_ts,
                 signal.get("wallet_tier", ""), signal.get("wallet", ""),
                 signal.get("detection_lag_seconds"), signal.get("price"),
                 signal.get("alpha_score", 0)),
            )
            self._wallet_conn.commit()
            row = self._wallet_conn.execute(
                "SELECT MAX(id) FROM polymarket_paper_trades WHERE condition_id = ? AND signal_type = ? AND entry_ts = ?",
                (condition_id, signal_type, now_ts),
            ).fetchone()
            trade_id = row[0] if row else 0

        print(
            f"[DISCOVERY] {signal_type} ${stake:.0f} {side} @ {ep:.2f} "
            f"| {question[:50]} | n_resolved={n_resolved}",
            flush=True,
        )

        # 2026-04-18: discovery trades also generate a hypothesis so the
        # thesis scorecard can measure their outcomes. Previously only
        # full-path trades got hypotheses, which meant the scorecard
        # saw 10 of 157 paper trades and was blind to the rest.
        # Wallet-less scanner signals (velocity/order-book) have no
        # meaningful wallet basis — skip hypothesis for those.
        if wallet and wallet not in ("velocity_detector", "order_book_monitor"):
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
                    entry_price=float(ep or 0),
                    convergence_count=int(signal.get("converging_wallets") or 0),
                )
                hypo.position_size_reason = "discovery_mode"
                persist_hypothesis(str(self._wallet_db_path), hypo, trade_id=trade_id)
            except Exception as exc:
                logger.debug("discovery hypothesis generation failed: %s", exc)

        return {
            "id": trade_id,
            "signal_type": signal_type,
            "side": side,
            "entry_price": ep,
            "size_usd": stake,
            "category": category,
            "discovery": True,
            "n_resolved": n_resolved,
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
        # Release both the legacy SQLite conn AND the pooled Postgres
        # wallet conn. Skipping the wallet conn was the root cause of
        # pool exhaustion: every API handler that did
        # `PolymarketPaperExecutor().foo()` without `with` leaked one
        # pool slot permanently. See 2026-04-18 audit.
        with self._lock:
            try:
                self._conn.close()
            except Exception:
                pass
            try:
                self._wallet_conn.close()
            except Exception:
                pass

    def __enter__(self) -> "PolymarketPaperExecutor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass
