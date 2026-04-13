"""
SQLite database for the wallet intelligence system.

Central store for wallet profiles, trades, positions, signals, and alerts.
Runs alongside the existing parquet-based pipeline.

Usage::

    from trading_platform.polymarket.wallet_db import WalletDB
    db = WalletDB()
    db.upsert_trade(wallet="0xabc...", asset="tok1", side="BUY", ...)
"""
from __future__ import annotations

import json
import logging
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_DEFAULT_PATH = str(_PROJECT_ROOT / "data" / "polymarket" / "wallet_intelligence.db")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS wallet_profiles (
    wallet TEXT PRIMARY KEY,
    edge REAL,
    early_win_rate REAL,
    win_rate REAL,
    resolved_trades INTEGER,
    total_volume_usdc REAL,
    is_early_informed INTEGER,
    uncertain_early_trades INTEGER,
    directional_win_rate REAL,
    crypto_win_rate REAL,
    politics_win_rate REAL,
    category_trades TEXT,
    wallet_type TEXT DEFAULT 'unknown',
    avg_entry_hours_before_close REAL,
    avg_position_size_usdc REAL,
    total_realized_pnl REAL,
    equity_score REAL,
    last_trade_ts INTEGER,
    last_synced_ts INTEGER,
    first_seen_ts INTEGER,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS wallet_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet TEXT NOT NULL,
    proxy_wallet TEXT,
    asset TEXT,
    condition_id TEXT,
    side TEXT,
    size REAL,
    price REAL,
    timestamp INTEGER,
    title TEXT,
    slug TEXT,
    outcome TEXT,
    event_slug TEXT,
    transaction_hash TEXT UNIQUE,
    category TEXT,
    market_resolved INTEGER DEFAULT 0,
    market_outcome TEXT,
    pnl REAL,
    synced_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_wt_wallet ON wallet_trades(wallet);
CREATE INDEX IF NOT EXISTS idx_wt_asset ON wallet_trades(asset);
CREATE INDEX IF NOT EXISTS idx_wt_timestamp ON wallet_trades(timestamp);
CREATE INDEX IF NOT EXISTS idx_wt_category ON wallet_trades(category);

CREATE TABLE IF NOT EXISTS wallet_positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet TEXT NOT NULL,
    asset TEXT NOT NULL,
    condition_id TEXT,
    title TEXT,
    slug TEXT,
    outcome TEXT,
    side TEXT,
    size REAL,
    avg_price REAL,
    initial_value REAL,
    current_value REAL,
    cash_pnl REAL,
    percent_pnl REAL,
    realized_pnl REAL,
    cur_price REAL,
    total_bought REAL,
    redeemable INTEGER,
    end_date TEXT,
    last_updated INTEGER,
    updated_at INTEGER,
    UNIQUE(wallet, asset)
);
CREATE INDEX IF NOT EXISTS idx_wp_wallet ON wallet_positions(wallet);
CREATE INDEX IF NOT EXISTS idx_wp_asset ON wallet_positions(asset);

CREATE TABLE IF NOT EXISTS market_signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    token_id TEXT,
    market_title TEXT,
    slug TEXT,
    category TEXT,
    direction TEXT,
    confidence REAL,
    net_smart_volume REAL,
    weighted_net_volume REAL,
    smart_wallet_count INTEGER,
    top_wallet_edge REAL,
    top_wallet_address TEXT,
    hours_since_first_entry REAL,
    hours_since_last_trade REAL,
    directional_wallet_count INTEGER,
    computed_at INTEGER,
    UNIQUE(token_id, computed_at)
);
CREATE INDEX IF NOT EXISTS idx_ms_token ON market_signals(token_id);

CREATE TABLE IF NOT EXISTS wallet_alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet TEXT,
    token_id TEXT,
    market_title TEXT,
    side TEXT,
    size REAL,
    price REAL,
    wallet_edge REAL,
    wallet_type TEXT,
    directional_win_rate REAL,
    tier INTEGER,
    trade_ts INTEGER,
    detected_at INTEGER,
    paper_trade_fired INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_wa_wallet ON wallet_alerts(wallet);
CREATE INDEX IF NOT EXISTS idx_wa_detected ON wallet_alerts(detected_at);

CREATE TABLE IF NOT EXISTS category_performance (
    category TEXT PRIMARY KEY,
    signals_fired INTEGER DEFAULT 0,
    signals_resolved INTEGER DEFAULT 0,
    signals_won INTEGER DEFAULT 0,
    win_rate REAL,
    total_pnl REAL DEFAULT 0,
    last_updated INTEGER
);

CREATE TABLE IF NOT EXISTS market_price_history (
    condition_id TEXT NOT NULL,
    token_id TEXT,
    question TEXT,
    category TEXT,
    t INTEGER NOT NULL,
    p REAL NOT NULL,
    PRIMARY KEY (condition_id, t)
);
CREATE INDEX IF NOT EXISTS idx_mph_cid ON market_price_history(condition_id);

CREATE TABLE IF NOT EXISTS wallet_market_positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    token_id TEXT,
    question TEXT,
    category TEXT,
    side TEXT,
    first_entry_ts INTEGER,
    last_entry_ts INTEGER,
    exit_ts INTEGER,
    avg_entry_price REAL,
    avg_exit_price REAL,
    total_shares REAL,
    total_cost_usdc REAL,
    total_proceeds_usdc REAL,
    realized_pnl REAL,
    market_resolved INTEGER DEFAULT 0,
    market_outcome TEXT,
    resolution_price REAL,
    end_date_ts INTEGER,
    hours_before_close REAL,
    UNIQUE(wallet, condition_id, side)
);
CREATE INDEX IF NOT EXISTS idx_wmp_wallet ON wallet_market_positions(wallet);
CREATE INDEX IF NOT EXISTS idx_wmp_cid ON wallet_market_positions(condition_id);

CREATE TABLE IF NOT EXISTS leaderboard (
    wallet TEXT PRIMARY KEY,
    category TEXT,
    rank INTEGER,
    tier TEXT,
    directional_win_rate REAL,
    rolling_20_wr REAL,
    conviction_score REAL,
    resolved_trades INTEGER,
    total_volume_usdc REAL,
    wallet_type TEXT,
    wallet_bucket TEXT,
    last_trade_ts INTEGER,
    version INTEGER,
    updated_at INTEGER
);

CREATE TABLE IF NOT EXISTS leaderboard_meta (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    current_version INTEGER DEFAULT 0,
    wallets_tier1 INTEGER DEFAULT 0,
    wallets_tier2 INTEGER DEFAULT 0,
    built_at INTEGER,
    is_valid INTEGER DEFAULT 0
);

INSERT OR IGNORE INTO leaderboard_meta (id, current_version, is_valid) VALUES (1, 0, 0);
"""


def _now_ts() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp())


def categorize_slug(slug: str) -> str:
    """Classify a market slug into a category."""
    s = (slug or "").lower()
    if any(k in s for k in [
        "bitcoin", "btc", "ethereum", "eth-", "crypto", "solana", "sol-",
        "xrp", "coinbase", "binance", "doge", "dogecoin", "matic", "polygon",
        "defi", "will-bitcoin", "will-ethereum", "will-btc", "will-eth",
        "price-above", "price-below", "market-cap",
    ]):
        return "crypto"
    if any(k in s for k in [
        "election", "president", "senate", "congress", "democrat", "republican",
        "trump", "biden", "harris", "vance", "vote", "ballot", "approval",
        "will-trump", "will-biden", "polling", "impeach", "cabinet",
    ]):
        return "politics"
    if any(k in s for k in [
        "nba", "nfl", "nhl", "mlb", "nascar", "f1-", "formula-1", "ufc",
        "boxing", "championship", "super-bowl", "world-series", "stanley-cup",
        "playoffs", "tournament", "soccer", "epl-", "match-", "game-winner",
    ]):
        return "sports"
    if any(k in s for k in ["elon", "tweet", "musk", "twitter", "x-posts"]):
        return "tweet_count"
    if any(k in s for k in [
        "cpi", "fed", "gdp", "jobs", "pce", "inflation", "interest-rate",
        "fomc", "recession", "unemployment", "payroll", "treasury",
        "will-cpi", "will-fed", "will-gdp", "economic", "rate-hike",
        "rate-cut", "basis-points", "nonfarm",
    ]):
        return "economics"
    return "other"


class WalletDB:
    """SQLite-backed wallet intelligence database."""

    def __init__(self, db_path: str | Path = _DEFAULT_PATH) -> None:
        self._path = Path(db_path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(self._path), check_same_thread=False, timeout=60)
        # DELETE journal mode: WAL broke repeatedly on the Docker Desktop
        # Windows bind mount — a single orphaned handle from live-collect
        # would exclusive-lock the DB and block every short-lived scheduler
        # task with "unable to open database file". DELETE mode has no
        # -wal/-shm sidecars to get stuck, and the busy_timeout serialises
        # writers cleanly for this workload. See
        # reports/system_diagnostic_2026-04-11.md for the post-mortem.
        for stmt in (
            "PRAGMA busy_timeout=60000",
            "PRAGMA journal_mode=DELETE",
            "PRAGMA synchronous=NORMAL",
        ):
            try:
                self._conn.execute(stmt)
            except sqlite3.OperationalError:
                pass
        try:
            self._conn.executescript(_SCHEMA)
            self._conn.commit()
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower():
                raise
            logger.debug("WalletDB schema executescript skipped (db locked): %s", exc)
        # Migrations open additional write locks; same defensive
        # try/except as the schema bootstrap above so a constructor
        # racing the live api/scheduler doesn't hard-fail.
        for _migrate in (self._migrate_profiles, self._migrate_signals, self._migrate_positions):
            try:
                _migrate()
            except sqlite3.OperationalError as exc:
                # Only swallow lock contention — let real schema bugs raise.
                if "locked" not in str(exc).lower():
                    raise
                logger.debug("%s skipped (db locked): %s", _migrate.__name__, exc)
        for _migrate in (
            self._migrate_anomalies,
            self._migrate_calibration,
            self._migrate_market_data_cache,
            self._migrate_wallet_tiering,
            self._migrate_circuit_breaker,
            self._migrate_equity_snapshots,
            self._migrate_fill_tracking,
        ):
            try:
                _migrate()
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).lower():
                    raise
                logger.debug("%s skipped (db locked): %s", _migrate.__name__, exc)

    def _migrate_circuit_breaker(self) -> None:
        """Singleton circuit_breaker_state row + append-only log."""
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS circuit_breaker_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                starting_capital REAL NOT NULL,
                peak_equity REAL NOT NULL,
                current_equity REAL NOT NULL,
                max_drawdown_pct REAL NOT NULL DEFAULT 0.20,
                current_drawdown_pct REAL NOT NULL DEFAULT 0.0,
                is_halted INTEGER NOT NULL DEFAULT 0,
                halted_at INTEGER,
                halted_reason TEXT,
                last_reset_at INTEGER,
                last_reset_by TEXT,
                last_updated INTEGER,
                daily_pnl REAL DEFAULT 0.0,
                daily_loss_limit REAL DEFAULT 0.05,
                daily_halted INTEGER DEFAULT 0,
                daily_reset_at INTEGER
            )
        """)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS circuit_breaker_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                equity_before REAL,
                equity_after REAL,
                drawdown_pct REAL,
                daily_pnl REAL,
                details TEXT,
                created_at INTEGER NOT NULL
            )
        """)
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_cb_log_created ON circuit_breaker_log(created_at DESC)"
        )
        self._conn.commit()

    def _migrate_wallet_tiering(self) -> None:
        """Per-wallet per-category dynamic tier table + change history."""
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS wallet_category_profiles (
                wallet TEXT NOT NULL,
                category TEXT NOT NULL,
                tier TEXT NOT NULL DEFAULT 'D',
                tier_score REAL,
                win_rate REAL,
                win_rate_30d REAL,
                win_rate_90d REAL,
                resolved_trades INTEGER DEFAULT 0,
                avg_bet_size REAL,
                monthly_pnl REAL,
                consistency_score REAL,
                category_purity REAL,
                trend_score REAL,
                last_trade_at INTEGER,
                last_evaluated INTEGER,
                resolved_since_last_change INTEGER DEFAULT 0,
                last_change_at INTEGER,
                PRIMARY KEY (wallet, category)
            )
        """)
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_wcp_category ON wallet_category_profiles(category)"
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_wcp_tier ON wallet_category_profiles(tier, category)"
        )
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS wallet_tier_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                wallet TEXT NOT NULL,
                category TEXT NOT NULL,
                old_tier TEXT,
                new_tier TEXT NOT NULL,
                reason TEXT NOT NULL,
                trigger_metric TEXT,
                old_tier_score REAL,
                new_tier_score REAL,
                win_rate_at_change REAL,
                win_rate_30d_at_change REAL,
                pnl_at_change REAL,
                changed_at INTEGER NOT NULL
            )
        """)
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_tier_hist_wallet ON wallet_tier_history(wallet, changed_at DESC)"
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_tier_hist_changed ON wallet_tier_history(changed_at DESC)"
        )
        self._conn.commit()

    def _migrate_market_data_cache(self) -> None:
        """Cache for pmxt outcome_id lookups + health metrics."""
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS outcome_id_cache (
                condition_id TEXT NOT NULL,
                market_slug TEXT,
                direction TEXT NOT NULL,
                outcome_id TEXT,
                cached_at INTEGER NOT NULL,
                PRIMARY KEY (condition_id, direction)
            )
        """)
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_outcome_cache_slug ON outcome_id_cache(market_slug)"
        )
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS pmxt_health (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                last_call_at INTEGER,
                last_success_at INTEGER,
                total_calls INTEGER DEFAULT 0,
                total_errors INTEGER DEFAULT 0,
                cache_hits INTEGER DEFAULT 0,
                cache_misses INTEGER DEFAULT 0,
                last_error TEXT
            )
        """)
        self._conn.execute(
            "INSERT OR IGNORE INTO pmxt_health (id) VALUES (1)"
        )
        self._conn.commit()

    def _migrate_calibration(self) -> None:
        """Create signal_calibration + calibration_reports tables for the
        Bayesian signal evaluator and bankroll allocator. Also adds
        fusion-score / context columns to polymarket_paper_trades.
        """
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS signal_calibration (
                signal_type TEXT PRIMARY KEY,
                sample_size INTEGER DEFAULT 0,
                wins INTEGER DEFAULT 0,
                losses INTEGER DEFAULT 0,
                bayesian_wr REAL,
                ev_per_trade REAL,
                profit_factor REAL,
                sharpe_ratio REAL,
                kelly_fraction REAL,
                rolling_10_wr REAL,
                rolling_20_ev REAL,
                consecutive_losses INTEGER DEFAULT 0,
                recommended_allocation_pct REAL,
                recommended_stake_usd REAL,
                allocated_usd REAL,
                status TEXT DEFAULT 'building',
                last_updated INTEGER
            )
        """)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS calibration_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                report_date TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                report_json TEXT NOT NULL,
                total_bankroll REAL,
                total_pnl_today REAL
            )
        """)
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_cal_reports_date ON calibration_reports(report_date DESC)"
        )
        # Optional fusion / context columns on paper trades. ALTER is
        # idempotent here because we only add when missing.
        ppt_cols = {r[1] for r in self._conn.execute(
            "PRAGMA table_info(polymarket_paper_trades)"
        ).fetchall()}
        for col, typedef in [
            ("fusion_score", "REAL"),
            ("fusion_components", "TEXT"),
            ("wallet_tier_at_fire", "TEXT"),
            ("wallet_bucket_at_fire", "TEXT"),
            ("kelly_stake_pct", "REAL"),
            ("source_wallet", "TEXT"),
            # Cost modeling + exit tracking (paper trading overhaul)
            ("exit_reason", "TEXT"),
            ("raw_entry_price", "REAL"),
            ("spread_cost", "REAL"),
            ("slippage_cost", "REAL"),
            ("raw_exit_price", "REAL"),
            ("exit_spread_cost", "REAL"),
            ("exit_slippage_cost", "REAL"),
            ("total_costs", "REAL"),
            ("last_mark_price", "REAL"),
            ("last_mark_ts", "INTEGER"),
            ("unrealized_pnl", "REAL"),
            ("detection_lag_seconds", "INTEGER"),
            ("whale_entry_price", "REAL"),
        ]:
            if col not in ppt_cols:
                try:
                    self._conn.execute(
                        f"ALTER TABLE polymarket_paper_trades ADD COLUMN {col} {typedef}"
                    )
                except sqlite3.OperationalError:
                    pass
        self._conn.commit()

    def _migrate_equity_snapshots(self) -> None:
        """Create paper_equity_snapshots for portfolio equity tracking."""
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS paper_equity_snapshots (
                ts INTEGER PRIMARY KEY,
                cash REAL,
                positions_value REAL,
                total_equity REAL,
                open_count INTEGER,
                realized_pnl_cumulative REAL,
                unrealized_pnl REAL
            )
        """)
        self._conn.commit()

    def _migrate_fill_tracking(self) -> None:
        """Create trade_fills table for live order fill tracking."""
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS trade_fills (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_id INTEGER,
                order_id TEXT,
                order_type TEXT,
                intended_price REAL,
                actual_fill_price REAL,
                intended_size REAL,
                actual_fill_size REAL,
                slippage REAL,
                slippage_pct REAL,
                fees REAL,
                fill_time_ms INTEGER,
                order_status TEXT,
                raw_response TEXT,
                created_at INTEGER
            )
        """)
        self._conn.commit()

    def _migrate_anomalies(self) -> None:
        """Create market_anomalies + market_ticks tables for live monitor pipelines."""
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS market_ticks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                condition_id TEXT NOT NULL,
                token_id TEXT,
                timestamp INTEGER NOT NULL,
                price REAL NOT NULL,
                UNIQUE(condition_id, timestamp)
            )
        """)
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ticks_cid_ts ON market_ticks(condition_id, timestamp DESC)"
        )
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS market_anomalies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                detected_at INTEGER NOT NULL,
                condition_id TEXT NOT NULL,
                token_id TEXT,
                question TEXT,
                category TEXT,
                anomaly_type TEXT NOT NULL,
                severity TEXT,
                detail TEXT,
                imbalance REAL,
                bid_usd REAL,
                ask_usd REAL,
                signal_fired INTEGER DEFAULT 0
            )
        """)
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_anom_detected ON market_anomalies(detected_at DESC)"
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_anom_cid ON market_anomalies(condition_id)"
        )
        self._conn.commit()

    def _migrate_positions(self) -> None:
        """Add columns to wallet_positions for full position fetcher payload."""
        existing = {r[1] for r in self._conn.execute("PRAGMA table_info(wallet_positions)").fetchall()}
        for col, typedef in [
            ("side", "TEXT"),
            ("total_bought", "REAL"),
            ("redeemable", "INTEGER"),
            ("updated_at", "INTEGER"),
        ]:
            if col not in existing:
                self._conn.execute(f"ALTER TABLE wallet_positions ADD COLUMN {col} {typedef}")
        # Add net/exit columns to wallet_market_positions for buy+sell aware reconstruction.
        wmp_existing = {r[1] for r in self._conn.execute("PRAGMA table_info(wallet_market_positions)").fetchall()}
        for col, typedef in [
            ("net_shares", "REAL"),
            ("total_sold", "REAL"),
            ("sell_volume_usdc", "REAL"),
            ("is_fully_exited", "INTEGER DEFAULT 0"),
            ("asset", "TEXT"),
        ]:
            if col not in wmp_existing:
                self._conn.execute(f"ALTER TABLE wallet_market_positions ADD COLUMN {col} {typedef}")
        self._conn.commit()

    def _migrate_profiles(self) -> None:
        """Add columns that may not exist in older DBs."""
        existing = {r[1] for r in self._conn.execute("PRAGMA table_info(wallet_profiles)").fetchall()}
        new_cols = [
            ("total_won_usdc", "REAL"),
            ("total_lost_usdc", "REAL"),
            ("net_pnl_usdc", "REAL"),
            ("avg_win_size_usdc", "REAL"),
            ("avg_loss_size_usdc", "REAL"),
            ("profit_factor", "REAL"),
            ("conviction_score", "REAL"),
            ("volume_tier", "TEXT"),
            ("large_bet_threshold", "REAL"),
            ("profit_score", "REAL"),
            ("wallet_bucket", "TEXT"),
            ("pseudonym", "TEXT"),
            # PnL Dynamics
            ("pnl_7d", "REAL"), ("pnl_30d", "REAL"), ("pnl_90d", "REAL"),
            ("pnl_trend", "REAL"), ("pnl_consistency", "REAL"),
            ("performance_decay", "REAL"),
            # Risk / Volatility
            ("pnl_volatility", "REAL"), ("sharpe_ratio", "REAL"),
            ("sortino_ratio", "REAL"), ("max_drawdown_pct", "REAL"),
            ("max_drawdown_usd", "REAL"), ("recovery_factor", "REAL"),
            ("calmar_ratio", "REAL"), ("win_streak_max", "INTEGER"),
            ("loss_streak_max", "INTEGER"),
            # Expected Value / Edge
            ("expected_value", "REAL"), ("kelly_fraction", "REAL"),
            ("ev_politics", "REAL"), ("ev_sports", "REAL"),
            ("ev_crypto", "REAL"), ("ev_economics", "REAL"),
            ("edge_vs_market", "REAL"),
            # Behavioral / Timing
            ("avg_hours_before_resolution", "REAL"),
            ("early_entry_rate", "REAL"),
            ("position_concentration", "REAL"),
            ("avg_hold_hours", "REAL"),
            ("size_scaling_quality", "REAL"),
            ("category_specialization", "REAL"),
            # Position-level
            ("open_positions_count", "INTEGER"),
            ("open_positions_value", "REAL"),
            ("unrealized_pnl_estimate", "REAL"),
        ]
        for col, typedef in new_cols:
            if col not in existing:
                self._conn.execute(f"ALTER TABLE wallet_profiles ADD COLUMN {col} {typedef}")
        self._conn.commit()

    def _migrate_signals(self) -> None:
        """Add columns to market_signals for whale signal engine."""
        existing = {r[1] for r in self._conn.execute("PRAGMA table_info(market_signals)").fetchall()}
        new_cols = [
            ("signal_type", "TEXT"),
            ("wallet", "TEXT"),
            ("price", "REAL"),
            ("size", "REAL"),
            ("fired_at", "INTEGER"),
            ("condition_id", "TEXT"),
            ("status", "TEXT DEFAULT 'open'"),
            ("updated_confidence", "REAL"),
            ("convergence_count", "INTEGER DEFAULT 0"),
            ("question", "TEXT"),
            ("stake_usd", "REAL"),
            ("paper_trade_id", "INTEGER"),
            ("executed", "INTEGER DEFAULT 0"),
        ]
        for col, typedef in new_cols:
            if col not in existing:
                self._conn.execute(f"ALTER TABLE market_signals ADD COLUMN {col} {typedef}")
        # Also add signal_fired to wallet_alerts
        alert_cols = {r[1] for r in self._conn.execute("PRAGMA table_info(wallet_alerts)").fetchall()}
        for col, typedef in [("signal_fired", "INTEGER DEFAULT 0"), ("question", "TEXT"), ("category", "TEXT")]:
            if col not in alert_cols:
                self._conn.execute(f"ALTER TABLE wallet_alerts ADD COLUMN {col} {typedef}")
        self._conn.commit()

    # ── Wallet profiles ──────────────────────────────────────────────────────

    def upsert_profile(self, wallet: str, **fields: Any) -> None:
        """Insert or update a wallet profile."""
        with self._lock:
            existing = self._conn.execute(
                "SELECT wallet FROM wallet_profiles WHERE wallet = ?", (wallet,)
            ).fetchone()
            if existing:
                sets = ", ".join(f"{k} = ?" for k in fields)
                vals = list(fields.values()) + [wallet]
                self._conn.execute(f"UPDATE wallet_profiles SET {sets} WHERE wallet = ?", vals)
            else:
                fields["wallet"] = wallet
                cols = ", ".join(fields.keys())
                placeholders = ", ".join("?" for _ in fields)
                self._conn.execute(
                    f"INSERT INTO wallet_profiles ({cols}) VALUES ({placeholders})",
                    list(fields.values()),
                )
            self._conn.commit()

    def get_profile(self, wallet: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM wallet_profiles WHERE wallet = ?", (wallet,)
            ).fetchone()
        if not row:
            return None
        cols = [d[0] for d in self._conn.execute("SELECT * FROM wallet_profiles LIMIT 0").description]
        return dict(zip(cols, row))

    def get_profiles_needing_sync(self, max_age_hours: float = 2.0) -> list[str]:
        """Return wallets whose last_synced_ts is older than max_age_hours."""
        cutoff = _now_ts() - int(max_age_hours * 3600)
        with self._lock:
            rows = self._conn.execute(
                "SELECT wallet FROM wallet_profiles WHERE last_synced_ts IS NULL OR last_synced_ts < ?",
                (cutoff,),
            ).fetchall()
        return [r[0] for r in rows]

    def get_all_wallets(self) -> list[str]:
        with self._lock:
            return [r[0] for r in self._conn.execute("SELECT wallet FROM wallet_profiles").fetchall()]

    # ── Trades ───────────────────────────────────────────────────────────────

    def upsert_trade(self, **fields: Any) -> bool:
        """Insert a trade, skip if transaction_hash already exists. Returns True if inserted.

        Auto-classifies the trade's category from its slug+title if the
        caller didn't provide one. This ensures every new fill ingested
        by the data-api-fetch pipeline gets a category at insert time —
        no more null-category accumulation.
        """
        tx = fields.get("transaction_hash")
        if not tx:
            return False
        # Auto-classify category if missing
        if not fields.get("category"):
            try:
                from trading_platform.polymarket.market_categorizer import classify_keywords
                cat, _ = classify_keywords(
                    fields.get("slug") or "", fields.get("title") or "",
                )
                fields["category"] = cat
            except Exception:
                fields["category"] = "other"
        with self._lock:
            existing = self._conn.execute(
                "SELECT 1 FROM wallet_trades WHERE transaction_hash = ?", (tx,)
            ).fetchone()
            if existing:
                return False
            try:
                cols = ", ".join(fields.keys())
                placeholders = ", ".join("?" for _ in fields)
                self._conn.execute(
                    f"INSERT INTO wallet_trades ({cols}) VALUES ({placeholders})",
                    list(fields.values()),
                )
                self._conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    def get_wallet_trades(self, wallet: str, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM wallet_trades WHERE wallet = ? ORDER BY timestamp DESC LIMIT ? OFFSET ?",
                (wallet, limit, offset),
            ).fetchall()
        cols = [d[0] for d in self._conn.execute("SELECT * FROM wallet_trades LIMIT 0").description]
        return [dict(zip(cols, r)) for r in rows]

    def get_trades_by_category(self, wallet: str, category: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM wallet_trades WHERE wallet = ? AND category = ? ORDER BY timestamp DESC",
                (wallet, category),
            ).fetchall()
        cols = [d[0] for d in self._conn.execute("SELECT * FROM wallet_trades LIMIT 0").description]
        return [dict(zip(cols, r)) for r in rows]

    # ── Positions ────────────────────────────────────────────────────────────

    def upsert_position(self, wallet: str, asset: str, **fields: Any) -> None:
        fields["wallet"] = wallet
        fields["asset"] = asset
        fields["last_updated"] = _now_ts()
        with self._lock:
            self._conn.execute(
                """INSERT INTO wallet_positions (wallet, asset, condition_id, title, slug, outcome,
                   size, avg_price, initial_value, current_value, cash_pnl, percent_pnl,
                   realized_pnl, cur_price, end_date, last_updated)
                   VALUES (:wallet, :asset, :condition_id, :title, :slug, :outcome,
                   :size, :avg_price, :initial_value, :current_value, :cash_pnl, :percent_pnl,
                   :realized_pnl, :cur_price, :end_date, :last_updated)
                   ON CONFLICT(wallet, asset) DO UPDATE SET
                   size=:size, avg_price=:avg_price, initial_value=:initial_value,
                   current_value=:current_value, cash_pnl=:cash_pnl, percent_pnl=:percent_pnl,
                   realized_pnl=:realized_pnl, cur_price=:cur_price, end_date=:end_date,
                   last_updated=:last_updated""",
                {
                    "wallet": wallet, "asset": asset,
                    "condition_id": fields.get("condition_id"),
                    "title": fields.get("title"), "slug": fields.get("slug"),
                    "outcome": fields.get("outcome"),
                    "size": fields.get("size", 0), "avg_price": fields.get("avg_price"),
                    "initial_value": fields.get("initial_value"),
                    "current_value": fields.get("current_value"),
                    "cash_pnl": fields.get("cash_pnl"),
                    "percent_pnl": fields.get("percent_pnl"),
                    "realized_pnl": fields.get("realized_pnl"),
                    "cur_price": fields.get("cur_price"),
                    "end_date": fields.get("end_date"),
                    "last_updated": _now_ts(),
                },
            )
            self._conn.commit()

    def get_wallet_positions(self, wallet: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM wallet_positions WHERE wallet = ? AND size > 0 ORDER BY current_value DESC",
                (wallet,),
            ).fetchall()
        cols = [d[0] for d in self._conn.execute("SELECT * FROM wallet_positions LIMIT 0").description]
        return [dict(zip(cols, r)) for r in rows]

    def get_positions_by_asset(self, asset: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM wallet_positions WHERE asset = ? AND size > 0",
                (asset,),
            ).fetchall()
        cols = [d[0] for d in self._conn.execute("SELECT * FROM wallet_positions LIMIT 0").description]
        return [dict(zip(cols, r)) for r in rows]

    # ── Market signals ───────────────────────────────────────────────────────

    def insert_signal(self, **fields: Any) -> None:
        fields["computed_at"] = _now_ts()
        with self._lock:
            cols = ", ".join(fields.keys())
            placeholders = ", ".join("?" for _ in fields)
            self._conn.execute(
                f"INSERT OR REPLACE INTO market_signals ({cols}) VALUES ({placeholders})",
                list(fields.values()),
            )
            self._conn.commit()

    def get_latest_signals(self, limit: int = 20) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """SELECT * FROM market_signals
                   WHERE id IN (SELECT MAX(id) FROM market_signals GROUP BY token_id)
                   ORDER BY weighted_net_volume DESC LIMIT ?""",
                (limit,),
            ).fetchall()
        cols = [d[0] for d in self._conn.execute("SELECT * FROM market_signals LIMIT 0").description]
        return [dict(zip(cols, r)) for r in rows]

    # ── Alerts ───────────────────────────────────────────────────────────────

    def insert_alert(self, **fields: Any) -> None:
        fields["detected_at"] = _now_ts()
        with self._lock:
            cols = ", ".join(fields.keys())
            placeholders = ", ".join("?" for _ in fields)
            self._conn.execute(
                f"INSERT INTO wallet_alerts ({cols}) VALUES ({placeholders})",
                list(fields.values()),
            )
            self._conn.commit()

    def get_alerts(self, *, limit: int = 50, tier: int | None = None,
                   hours: float | None = None, wallet: str | None = None) -> list[dict[str, Any]]:
        clauses = []
        params: list[Any] = []
        if tier is not None:
            clauses.append("tier = ?")
            params.append(tier)
        if hours is not None:
            cutoff = _now_ts() - int(hours * 3600)
            clauses.append("detected_at >= ?")
            params.append(cutoff)
        if wallet:
            clauses.append("wallet = ?")
            params.append(wallet)
        where = " AND ".join(clauses) if clauses else "1=1"
        params.append(limit)
        with self._lock:
            rows = self._conn.execute(
                f"SELECT * FROM wallet_alerts WHERE {where} ORDER BY detected_at DESC LIMIT ?",
                params,
            ).fetchall()
        cols = [d[0] for d in self._conn.execute("SELECT * FROM wallet_alerts LIMIT 0").description]
        return [dict(zip(cols, r)) for r in rows]

    # ── Leaderboard ──────────────────────────────────────────────────────────

    def get_leaderboard(self, limit: int = 50, sort_by: str = "equity_score") -> list[dict[str, Any]]:
        allowed_sorts = {"equity_score", "profit_score", "conviction_score",
                         "directional_win_rate", "resolved_trades", "net_pnl_usdc"}
        col = sort_by if sort_by in allowed_sorts else "equity_score"
        with self._lock:
            rows = self._conn.execute(
                f"""SELECT wallet, wallet_type, equity_score, directional_win_rate,
                          win_rate, resolved_trades, total_volume_usdc,
                          category_trades, avg_position_size_usdc,
                          net_pnl_usdc, profit_factor, conviction_score,
                          volume_tier, total_won_usdc, total_lost_usdc,
                          profit_score, notes
                   FROM wallet_profiles
                   WHERE {col} IS NOT NULL
                   ORDER BY {col} DESC LIMIT ?""",
                (limit,),
            ).fetchall()
        cols = ["wallet", "wallet_type", "equity_score", "directional_win_rate",
                "win_rate", "resolved_trades", "total_volume_usdc",
                "category_trades", "avg_position_size_usdc",
                "net_pnl_usdc", "profit_factor", "conviction_score",
                "volume_tier", "total_won_usdc", "total_lost_usdc",
                "profit_score", "notes"]
        return [dict(zip(cols, r)) for r in rows]

    # ── Winners ───────────────────────────────────────────────────────────────

    def get_winners(self, *, window: str = "all", limit: int = 50) -> list[dict[str, Any]]:
        """Return wallets ranked by profit for a given time window."""
        now = _now_ts()
        if window == "today":
            # Midnight UTC today
            from datetime import datetime, timezone
            midnight = int(datetime.now(tz=timezone.utc).replace(hour=0, minute=0, second=0).timestamp())
            time_filter = f"AND wt.timestamp >= {midnight}"
        elif window == "weekly":
            time_filter = f"AND wt.timestamp >= {now - 7 * 86400}"
        elif window == "monthly":
            time_filter = f"AND wt.timestamp >= {now - 30 * 86400}"
        else:
            time_filter = ""

        with self._lock:
            if time_filter:
                # Compute profit from trades in window
                rows = self._conn.execute(f"""
                    SELECT wt.wallet,
                           SUM(wt.pnl) as profit,
                           SUM(wt.size) as volume,
                           COUNT(*) as trades,
                           wp.wallet_type, wp.equity_score, wp.directional_win_rate,
                           wp.category_trades, wp.notes
                    FROM wallet_trades wt
                    LEFT JOIN wallet_profiles wp ON wt.wallet = wp.wallet
                    WHERE wt.market_resolved = 1 AND wt.pnl IS NOT NULL
                    {time_filter}
                    GROUP BY wt.wallet
                    ORDER BY profit DESC
                    LIMIT ?
                """, (limit,)).fetchall()
                cols = ["wallet", "profit", "volume", "trades",
                        "wallet_type", "equity_score", "directional_win_rate",
                        "category_trades", "notes"]
            else:
                # All-time: use precomputed net_pnl_usdc
                rows = self._conn.execute("""
                    SELECT wallet, net_pnl_usdc as profit, total_volume_usdc as volume,
                           resolved_trades as trades,
                           wallet_type, equity_score, directional_win_rate,
                           category_trades, notes
                    FROM wallet_profiles
                    WHERE net_pnl_usdc IS NOT NULL
                    ORDER BY net_pnl_usdc DESC
                    LIMIT ?
                """, (limit,)).fetchall()
                cols = ["wallet", "profit", "volume", "trades",
                        "wallet_type", "equity_score", "directional_win_rate",
                        "category_trades", "notes"]

        return [dict(zip(cols, r)) for r in rows]

    # ── Universe Stats ────────────────────────────────────────────────────────

    def universe_stats(self) -> dict[str, Any]:
        """Comprehensive stats for the wallet universe."""
        now = _now_ts()
        with self._lock:
            total = self._conn.execute("SELECT COUNT(*) FROM wallet_profiles").fetchone()[0]

            # By type
            type_rows = self._conn.execute(
                "SELECT COALESCE(wallet_type, 'unknown'), COUNT(*) FROM wallet_profiles GROUP BY 1"
            ).fetchall()
            by_type = {r[0]: r[1] for r in type_rows}

            # By volume tier
            tier_rows = self._conn.execute(
                "SELECT COALESCE(volume_tier, 'unknown'), COUNT(*) FROM wallet_profiles GROUP BY 1"
            ).fetchall()
            by_tier = {r[0]: r[1] for r in tier_rows}

            # Total deployed
            deployed = self._conn.execute(
                "SELECT COALESCE(SUM(total_volume_usdc), 0) FROM wallet_profiles"
            ).fetchone()[0]

            # Active last 7 days
            cutoff_7d = now - 7 * 86400
            active_7d = self._conn.execute(
                "SELECT COUNT(*) FROM wallet_profiles WHERE last_synced_ts > ?", (cutoff_7d,)
            ).fetchone()[0]

            # Alerts today
            from datetime import datetime, timezone
            midnight = int(datetime.now(tz=timezone.utc).replace(hour=0, minute=0, second=0).timestamp())
            alerts_today = self._conn.execute(
                "SELECT COUNT(*) FROM wallet_alerts WHERE detected_at >= ?", (midnight,)
            ).fetchone()[0]
            t1_today = self._conn.execute(
                "SELECT COUNT(*) FROM wallet_alerts WHERE detected_at >= ? AND tier = 1", (midnight,)
            ).fetchone()[0]
            t2_today = self._conn.execute(
                "SELECT COUNT(*) FROM wallet_alerts WHERE detected_at >= ? AND tier = 2", (midnight,)
            ).fetchone()[0]

            last_sync = self._conn.execute(
                "SELECT MAX(last_synced_ts) FROM wallet_profiles"
            ).fetchone()[0]

        return {
            "total_wallets": total,
            "by_type": by_type,
            "by_tier": by_tier,
            "total_deployed_usdc": round(deployed, 0),
            "active_last_7d": active_7d,
            "signals_today": alerts_today,
            "tier1_today": t1_today,
            "tier2_today": t2_today,
            "last_sync_ts": last_sync,
        }

    # ── Leaderboard ──────────────────────────────────────────────────────────

    def get_leaderboard_version(self) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT current_version FROM leaderboard_meta WHERE id=1"
            ).fetchone()
        return row[0] if row else 0

    def begin_leaderboard_rebuild(self, version: int) -> None:
        with self._lock:
            self._conn.execute("UPDATE leaderboard_meta SET is_valid=0 WHERE id=1")
            self._conn.commit()

    def commit_leaderboard(self, version: int, tier1_count: int, tier2_count: int) -> None:
        with self._lock:
            self._conn.execute(
                """UPDATE leaderboard_meta
                   SET current_version=?, wallets_tier1=?, wallets_tier2=?,
                       built_at=?, is_valid=1
                   WHERE id=1""",
                (version, tier1_count, tier2_count, _now_ts()),
            )
            self._conn.commit()

    def get_watched_wallets(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            meta = self._conn.execute(
                "SELECT is_valid FROM leaderboard_meta WHERE id=1"
            ).fetchone()
            if not meta or not meta[0]:
                return {}
            rows = self._conn.execute("SELECT * FROM leaderboard").fetchall()
        if not rows:
            return {}
        cols = [d[0] for d in self._conn.execute("SELECT * FROM leaderboard LIMIT 0").description]
        return {r[0]: dict(zip(cols, r)) for r in rows}

    def update_signal_confidence(self, signal_id: int, new_confidence: float, convergence_count: int) -> None:
        """Update an existing signal's confidence and convergence count."""
        with self._lock:
            self._conn.execute(
                """UPDATE market_signals
                   SET updated_confidence = ?, convergence_count = ?
                   WHERE id = ?""",
                (new_confidence, convergence_count, signal_id),
            )
            self._conn.commit()

    def compute_rolling_wr(self, wallet: str, n: int = 20) -> float | None:
        with self._lock:
            rows = self._conn.execute(
                """SELECT pnl FROM wallet_trades
                   WHERE wallet=? AND market_resolved=1 AND pnl IS NOT NULL
                   ORDER BY timestamp DESC LIMIT ?""",
                (wallet, n),
            ).fetchall()
        if len(rows) < n:
            return None
        wins = sum(1 for r in rows if r[0] > 0)
        return wins / len(rows)

    # ── Signal helpers ──────────────────────────────────────────────────────

    def get_prior_position(self, wallet: str, condition_id: str) -> dict[str, Any] | None:
        """Check if wallet has a prior recorded trade on this market."""
        with self._lock:
            row = self._conn.execute(
                """SELECT side, size, price, timestamp FROM wallet_trades
                   WHERE wallet = ? AND (asset = ? OR condition_id = ?)
                   ORDER BY timestamp DESC LIMIT 1""",
                (wallet, condition_id, condition_id),
            ).fetchone()
        if not row:
            # Also check wallet_alerts
            with self._lock:
                row = self._conn.execute(
                    """SELECT side, size, price, detected_at as timestamp FROM wallet_alerts
                       WHERE wallet = ? AND token_id = ?
                       ORDER BY detected_at DESC LIMIT 1""",
                    (wallet, condition_id),
                ).fetchone()
        if not row:
            return None
        return {"side": row[0], "size": row[1], "price": row[2], "timestamp": row[3]}

    def get_market_whale_activity(self, condition_id: str, hours: float = 6.0) -> dict[str, Any]:
        """Whale trade count, distinct wallets, side breakdown for a market."""
        cutoff = _now_ts() - int(hours * 3600)
        with self._lock:
            rows = self._conn.execute(
                """SELECT wallet, side, size FROM wallet_alerts
                   WHERE token_id = ? AND detected_at >= ?""",
                (condition_id, cutoff),
            ).fetchall()
        buy_count = sum(1 for r in rows if r[1] == "BUY")
        sell_count = sum(1 for r in rows if r[1] == "SELL")
        wallets = {r[0] for r in rows}
        return {
            "total_trades": len(rows),
            "distinct_wallets": len(wallets),
            "buy_count": buy_count,
            "sell_count": sell_count,
            "tier1_wallets": wallets,
        }

    # ── Category performance ────────────────────────────────────────────────

    def increment_category_signal(self, category: str) -> None:
        with self._lock:
            self._conn.execute(
                """INSERT INTO category_performance (category, signals_fired, last_updated)
                   VALUES (?, 1, ?)
                   ON CONFLICT(category) DO UPDATE SET
                   signals_fired = signals_fired + 1, last_updated = ?""",
                (category, _now_ts(), _now_ts()),
            )
            self._conn.commit()

    def get_category_performance(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM category_performance ORDER BY win_rate DESC"
            ).fetchall()
        cols = [d[0] for d in self._conn.execute("SELECT * FROM category_performance LIMIT 0").description]
        return [dict(zip(cols, r)) for r in rows]

    # ── Stats ────────────────────────────────────────────────────────────────

    def stats(self) -> dict[str, int]:
        with self._lock:
            profiles = self._conn.execute("SELECT COUNT(*) FROM wallet_profiles").fetchone()[0]
            trades = self._conn.execute("SELECT COUNT(*) FROM wallet_trades").fetchone()[0]
            positions = self._conn.execute("SELECT COUNT(*) FROM wallet_positions").fetchone()[0]
            signals = self._conn.execute("SELECT COUNT(*) FROM market_signals").fetchone()[0]
            alerts = self._conn.execute("SELECT COUNT(*) FROM wallet_alerts").fetchone()[0]
        return {"profiles": profiles, "trades": trades, "positions": positions,
                "signals": signals, "alerts": alerts}

    def close(self) -> None:
        with self._lock:
            self._conn.close()
