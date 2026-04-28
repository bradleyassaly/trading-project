"""
In-process task scheduler for the trading platform.

Runs as a long-lived service inside Docker (or as a background
process via PowerShell). Each scheduled task is a small dict with a
``cmd`` (the shell command to invoke), a frequency in seconds, and
the last_run / next_run timestamps. After every run we capture stdout
and stderr to a per-task log file under ``logs/scheduler/`` and write
the result to ``data/scheduler/state.json`` so the health watchdog
and the GUI can see what's happening.

Failures fire a Telegram alert via the existing TelegramAlerter
(non-blocking; if Telegram is unconfigured the failure is just logged).

The schedule below has been verified against the actual CLI commands
that exist (see ``trading-cli`` polymarket subcommand list). Tasks
that reference commands that don't exist yet are commented out with
TODO markers.
"""
from __future__ import annotations

import json
import logging
import os
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass

LOG_DIR = PROJECT_ROOT / "logs" / "scheduler"
STATE_DIR = PROJECT_ROOT / "data" / "scheduler"
STATE_PATH = STATE_DIR / "state.json"
LOG_DIR.mkdir(parents=True, exist_ok=True)
STATE_DIR.mkdir(parents=True, exist_ok=True)

try:
    from trading_platform.polymarket.logging_config import setup_logging
    setup_logging(service="scheduler")
except Exception:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("scheduler")


@dataclass
class Task:
    name: str
    cmd: str
    interval_seconds: int
    description: str
    last_run_at: float | None = None
    last_status: str | None = None  # 'ok' | 'failed' | None
    last_duration_s: float | None = None
    last_error: str | None = None
    enabled: bool = True
    consecutive_failures: int = 0
    first_failure_at: float | None = None

    def next_run_at(self, now: float) -> float:
        if self.last_run_at is None:
            return now  # run immediately on startup
        return self.last_run_at + self.interval_seconds


# 2026-04-24: alert escalation ladder. The Apr-19 → Apr-24 idle-session
# bug failed the same six tasks every 15-30 minutes for 5 days; the
# per-failure alert path generated ~480 messages and was effectively
# noise. This ladder fires Telegram on:
#   - 1st failure  → immediate ("a thing broke")
#   - 5th in-a-row → "this is sustained, look at it"
#   - 25th in-a-row → "this is now a multi-hour incident"
#   - then every 100th → tail-spam suppression
# Recoveries always fire once.
ALERT_ON_FAILURE_COUNT = (1, 5, 25)
ALERT_TAIL_SPAM_EVERY = 100


# ── Schedule definitions ────────────────────────────────────────────────────
#
# Verified against the actual `trading-cli data polymarket *` commands:
#   backfill, blockchain_ingest, build_leaderboard, clob_fetch,
#   collect_history, daily_refresh, data_api_fetch, diagnose,
#   fetch_resolutions, goldsky_*, ingest, ingest_top_wallets, live,
#   mirror_scan, open_positions, orderflow, performance_review,
#   realtime_monitor, refresh_positions, refresh_universe,
#   smart_money_scan, smart_money_trade, test_telegram, wallet_profiles
#
# Tasks that depend on commands not present in the CLI are commented
# out with a TODO marker until the command is implemented.

SCHEDULE: list[Task] = [
    Task(
        name="paper_resolutions",
        cmd="curl -fsS -X POST http://api:8001/api/paper/check-resolutions",
        interval_seconds=30 * 60,
        description="Resolve any paper trades whose underlying market settled",
    ),
    Task(
        name="data_api_fetch",
        cmd="trading-cli data polymarket data-api-fetch --hours-back 6",
        interval_seconds=2 * 3600,
        description="Pull recent fills from the Polymarket data-api",
    ),
    Task(
        name="refresh_positions",
        cmd="trading-cli data polymarket refresh-positions",
        interval_seconds=3600,
        description="Sync open wallet positions",
    ),
    Task(
        name="open_positions",
        cmd="trading-cli data polymarket compute-open-positions",
        interval_seconds=2 * 3600,
        description="Compute open-position P&L snapshots",
        # DISABLED 2026-04-18: reads goldsky_resolved_fills/ (2.5GB of 2028
        # parquets, last updated 2026-04-05) and concats all files into
        # memory before filtering. OOM-killed every 2h for 36+ hours;
        # even 2GB mem_limit wasn't enough. The input data is 13 days
        # stale (legacy Goldsky path) and the downstream consumer
        # (/api/smart-money/open-positions) can serve the existing
        # wallet_open_positions.parquet until a streaming rewrite lands.
        enabled=False,
    ),
    Task(
        name="fetch_resolutions",
        cmd="trading-cli data polymarket fetch-resolutions",
        interval_seconds=24 * 3600,
        description="Mark resolved markets in wallet_trades (daily)",
    ),
    Task(
        name="signal_resolver",
        cmd="python -m trading_platform.polymarket.signal_resolver",
        interval_seconds=4 * 3600,
        description="Resolve signal_outcomes from gamma CSV (every 4h)",
    ),
    Task(
        name="sync_wallet_trades",
        # Persists Data-API trades into the wallet_trades SQLite table.
        # Separate from `poll-wallet-trades` (signal-firing) and
        # `data-api-fetch` (directory dump). Without this, wallet_trades
        # goes stale and backtest/analytics queries lie — see 2026-04-14
        # audit where the table sat 31h old despite poller running.
        cmd="trading-cli data polymarket sync-wallet-trades --top-n 200",
        interval_seconds=2 * 3600,
        description="Persist recent trades for tracked wallets into wallet_trades",
    ),
    Task(
        name="signal_engine_backtest",
        # Replays last 60d of wallet_trades through the current signal
        # engine and computes per-signal-type EV from gamma resolutions.
        # Faster validation than waiting for live fires to resolve —
        # lets us reject negative-EV signals before they consume paper
        # bankroll. Writes to signal_backtest_results.
        cmd="python scripts/signal_engine_backtest.py --days 60 --write",
        interval_seconds=7 * 24 * 3600,
        description="Weekly signal-engine backtest (EV validation)",
    ),
    Task(
        name="wallet_copy_graph",
        # Mines wallet_trades for copy-relationships (wallet B follows A
        # within 2h on same market/side). Populates wallet_copy_relationships
        # — used by network_leader_entry signal. Runs daily; relationships
        # shift slowly and 90d lookback is ~plenty.
        cmd="python -m trading_platform.polymarket.wallet_copy_graph",
        interval_seconds=24 * 3600,
        description="Detect wallet copy-relationships (leader/follower graph)",
    ),
    Task(
        name="wallet_strategy_observer",
        # Scans wallet_trades and tags each per behavioral strategy
        # (accumulator, flipper, longshot, etc.), then writes per-wallet
        # per-strategy PnL to `wallet_strategy_profiles`. Underlying data
        # for: strategy-level alpha attribution, copyable-wallet discovery,
        # and weekly digest highlights. Runs every 12h — data shifts slowly.
        cmd="python -m trading_platform.polymarket.wallet_strategy_observer",
        interval_seconds=12 * 3600,
        description="Per-wallet × per-strategy alpha attribution",
    ),
    Task(
        name="bankroll_refresh",
        # Pulls live USDC balance from the Polymarket CLOB + open-position
        # notional from the Data API, caches to data/polymarket/bankroll.json.
        # KellySizer and KillSwitch both read from that cache so sizing
        # and daily-loss limits always reflect actual account balance.
        cmd="python -m trading_platform.polymarket.bankroll",
        interval_seconds=15 * 60,
        description="Refresh live Polymarket bankroll (CLOB + env fallback)",
    ),
    Task(
        name="markets_refresh",
        # Pulls Gamma metadata (endDate, volume, outcomes, tags, UMA
        # status) for every market we care about — open paper positions
        # first, then pending signals, then recently-active trade markets.
        # Back-fills the gap where 65% of wallet_trades markets had no
        # persistent metadata (2026-04-14 audit). Enables badge, exit
        # timing, and volume-filter queries to join instantly.
        cmd="python -m trading_platform.polymarket.markets_table",
        interval_seconds=6 * 3600,
        description="Refresh per-market metadata cache (endDate/volume/tags)",
    ),
    Task(
        name="orphan_wallet_onboarder",
        # Auto-ingest wallets that live-collect has been firing signals on
        # but which we don't track in wallet_trades yet. 10+ signals in 7d
        # is the bar. Turns "orphan" signal noise into candidate tier1/2
        # wallets once their trade history is profiled.
        cmd="python -m trading_platform.polymarket.orphan_wallet_onboarder",
        interval_seconds=12 * 3600,
        description="Auto-onboard recurring orphan wallets into wallet_trades",
    ),
    Task(
        name="ingest_top_wallets_daily",
        # Discovers new top whales each day via Polymarket Data API
        # leaderboard (1d window). Seeds wallet_profiles for wallets we
        # haven't seen before. Without this, the tracked set goes stale
        # as top performers rotate. Was part of run_daily_intelligence.py
        # but never wired into the container scheduler — pipeline gap
        # surfaced in the 2026-04-14 audit.
        cmd="trading-cli data polymarket ingest-top-wallets --limit 50 --window 1d",
        interval_seconds=24 * 3600,
        description="Ingest top-50 wallets over last 24h (daily whale discovery)",
    ),
    Task(
        name="ingest_top_wallets_weekly",
        cmd="trading-cli data polymarket ingest-top-wallets --limit 50 --window 7d",
        interval_seconds=7 * 24 * 3600,
        description="Ingest top-50 wallets over last 7 days (weekly discovery)",
    ),
    Task(
        name="daily_digest",
        cmd="python -m trading_platform.polymarket.daily_digest",
        interval_seconds=24 * 3600,
        description="Telegram digest showing live-gate progress (daily)",
    ),
    Task(
        name="weekly_digest",
        cmd="python -m trading_platform.polymarket.weekly_digest",
        interval_seconds=7 * 24 * 3600,
        description="Weekly strategic digest: category EV, signal volume, wallet ranking",
    ),
    Task(
        # 2026-04-18: self-heal hypothesis-resolution drift. Every 2h,
        # scan for closed paper trades whose matching hypothesis row
        # was never updated (actual_outcome IS NULL) and call
        # mark_resolved on each. Covers the silent-scorecard-drift
        # mode that went undetected for weeks before being found.
        name="hypothesis_backfill",
        cmd="python scripts/hypothesis_backfill.py",
        interval_seconds=2 * 3600,
        description="Backfill hypothesis resolutions for closed paper trades",
    ),
    Task(
        name="wallet_profiles_rebuild",
        cmd="trading-cli data polymarket wallet-profiles --from-db",
        interval_seconds=24 * 3600,
        description="Recompute wallet metrics from the trade history",
    ),
    Task(
        name="build_leaderboard",
        cmd="trading-cli data polymarket build-leaderboard",
        interval_seconds=24 * 3600,
        description="Rebuild the static tier1h/tier1/tier2 leaderboard",
    ),
    Task(
        name="refresh_universe",
        cmd="trading-cli data polymarket refresh-universe",
        interval_seconds=24 * 3600,
        description="Refresh the active market universe (daily)",
    ),
    Task(
        name="calibration_rebalance",
        cmd="curl -fsS -X POST http://api:8001/api/calibration/rebalance -H 'Content-Type: application/json' -d '{}'",
        interval_seconds=24 * 3600,
        description="Recompute SignalEvaluator + bankroll allocator",
    ),
    Task(
        name="tiers_rebuild",
        cmd="curl -fsS -X POST http://api:8001/api/tiers/rebuild",
        interval_seconds=24 * 3600,
        description="Full WalletTieringEngine rebuild (daily)",
    ),
    Task(
        name="calibration_report",
        cmd="curl -fsS -X POST http://api:8001/api/calibration/report/generate",
        interval_seconds=24 * 3600,
        description="Persist daily calibration report snapshot",
    ),
    Task(
        name="circuit_breaker_daily_reset",
        # Inline Python so we don't depend on a CLI command — clears
        # daily_pnl + daily_halted on the cumulative-drawdown breaker.
        # Drawdown halt itself is NEVER auto-reset.
        cmd=(
            "python -c \""
            "from trading_platform.polymarket.circuit_breaker import CircuitBreaker; "
            "CircuitBreaker().reset_daily()\""
        ),
        interval_seconds=24 * 3600,
        description="Midnight reset of circuit breaker daily loss state",
    ),
    Task(
        name="db_backup",
        # Daily SQLite backup using sqlite3's native .backup() API,
        # which is safe to run while the DB is open and being written
        # to (unlike a file copy which can race the WAL checkpoint).
        # Keeps the last 7 daily backups; older ones are pruned.
        cmd=(
            "python -c \""
            "import sqlite3, os, glob, time; "
            "src='/app/data/polymarket/wallet_intelligence.db'; "
            "os.makedirs('/app/data/backups', exist_ok=True); "
            "dst=f'/app/data/backups/wallet_intelligence_{time.strftime(\\\"%Y%m%d\\\")}.db'; "
            "s=sqlite3.connect(src); d=sqlite3.connect(dst); "
            "s.backup(d); s.close(); d.close(); "
            "files=sorted(glob.glob('/app/data/backups/wallet_intelligence_*.db')); "
            "[os.remove(f) for f in files[:-7]]; "
            "print(f'backup ok: {dst}')\""
        ),
        interval_seconds=24 * 3600,
        description="Daily SQLite backup (rotates last 7 days)",
    ),
    Task(
        name="thesis_daily_snapshot",
        # Daily KPI snapshot for the thesis scorecard. Runs after the
        # daily digest so the digest's "today" numbers and the snapshot's
        # historical row use the same compute pass.
        cmd="curl -fsS -X POST http://api:8001/api/thesis/snapshot",
        interval_seconds=24 * 3600,
        description="Daily thesis scorecard snapshot",
    ),
    Task(
        name="alpha_score_recompute",
        # Daily recomputation of per-wallet, per-category copyability
        # alpha scores. Reads only pnl_reliable=1 trades; output drives
        # the get_wallet_alpha() lookup in the signal engine. See
        # reports/pnl_investigation.md and src/.../alpha_scores.py.
        cmd="curl -fsS -X POST http://api:8001/api/alpha/recompute",
        interval_seconds=24 * 3600,
        description="Daily alpha-score recompute (per-wallet, per-category copyability)",
    ),
    Task(
        name="enrich_trade_resolution",
        # Weekly PnL enrichment for backfilled trades. Without this,
        # leaderboard wallets that came in via the PM scrape end up
        # with `directional_win_rate=NULL` because their trades sit
        # in wallet_trades with `pnl=NULL` until enrichment runs. The
        # one-shot ad-hoc run closed the WR gap from 51% to 63% on
        # the active leaderboard — keeping it weekly catches new
        # backfills + new resolutions automatically.
        cmd="trading-cli data polymarket enrich-trade-resolution",
        interval_seconds=7 * 24 * 3600,
        description="Weekly trade-resolution enrichment (computes pnl on resolved trades)",
    ),
    Task(
        name="pm_leaderboard_sync",
        # Pulls Polymarket's authoritative PnL + volume leaderboard into
        # wallet_profiles.pm_pnl_usdc and promotes wallets with
        # pm_pnl >= $100K to tier1h. Our internal net_pnl_usdc only
        # counts resolved trades and undercounts active traders by
        # 100-1000×, so the PM leaderboard is the ground truth for
        # whale identification.
        cmd="python -m trading_platform.polymarket.pm_leaderboard_sync",
        interval_seconds=24 * 3600,
        description="Daily PM authoritative leaderboard sync + tier1h promotion",
    ),
    Task(
        # 2026-04-25: behavioral metrics pipeline. Bootstrap-CI on per-
        # wallet ROI, per-(wallet,category) z-score, sizing distribution,
        # sybil/farmer detection, k-means strategy clusters. Replaces
        # the hand-coded `wallet_type` enum with data-driven flags. Runs
        # daily after pnl_reconstruction; idempotent upserts.
        name="wallet_behavior_metrics",
        cmd="python -m trading_platform.polymarket.wallet_behavior_metrics",
        interval_seconds=24 * 3600,
        description="Daily behavioral metrics: bootstrap-CI, z-score, sizing, sybil, clusters",
    ),
    Task(
        # 2026-04-25: signal health pipeline. Per-signal IC over 30d/14d
        # rolling windows + pairwise correlation across signal types.
        # Flags decaying (ic_14d < 0 on n>=10) and correlation-redundant
        # (>=0.7 with another type) signals. Daily.
        name="signal_health",
        cmd="python -m trading_platform.polymarket.signal_health",
        interval_seconds=24 * 3600,
        description="Daily signal IC + pairwise correlation; decay + redundancy flags",
    ),
    Task(
        # 2026-04-25: auto-promote insider wallets. Inserts into
        # insider_wallets any wallet with early_win_rate>=0.65 over n>=10
        # and avg_entry_hours_before_close>=12, excluding flagged farmers.
        # Closes the 82-wallet insider-pool gap that limited insider
        # signal volume to ~5/day vs 480/day for smart-money.
        name="insider_promote",
        cmd="python -m trading_platform.polymarket.insider_promote",
        interval_seconds=24 * 3600,
        description="Daily auto-promote of qualifying wallets into insider_wallets",
    ),
    Task(
        # 2026-04-27: sub-domain classifier. Polymarket markets aren't
        # "sports" — they're "UFC vs Y" or "Iran-US peace deal". Smart
        # money operates at event level. Classifier writes
        # markets.subcategory (sports/ufc, politics/iran, etc.) so
        # alpha-scoring + leaderboards + alerts can run at sport
        # granularity. Daily; idempotent (only re-classifies NULL rows
        # unless --force). Chained → wallet_subdomain_metrics so the
        # z-score table refreshes immediately after new sub-domain tags.
        name="subcategory_classifier",
        cmd=(
            "python -m trading_platform.polymarket.subcategory_classifier "
            "&& python -m trading_platform.polymarket.wallet_subdomain_metrics"
        ),
        interval_seconds=24 * 3600,
        description="Daily sub-domain classifier → subdomain z-score chain",
    ),
    Task(
        # 2026-04-25: isotonic calibration of alpha_score. First Brier
        # measurement showed 0.35 / miscal 0.31 (POOR_CALIBRATION) —
        # alpha_score predicts ~0.43 where reality is ~0.20 in the
        # 0.4-0.5 band. PAV regression refits a non-decreasing curve
        # daily; apply_calibration() lookup runs at signal time. Fits
        # require n>=20 resolved hypotheses; until then identity.
        name="isotonic_calibration",
        cmd="python -m trading_platform.polymarket.isotonic_calibration",
        interval_seconds=24 * 3600,
        description="Daily isotonic-regression refit of alpha_score → P(win)",
    ),
    Task(
        # 2026-04-25: Phase B independent signal. Resolution-time decay
        # — markets within 24h of resolve at $0.10-$0.30 with positive
        # liquidity. Decoupled from whale-flow alpha source. Behind
        # PHASE_B_RESOLUTION_DECAY_ENABLED feature flag. Disabled by
        # default until manually enabled in env.
        name="resolution_decay_signal",
        cmd="python -m trading_platform.polymarket.resolution_decay_signal",
        interval_seconds=15 * 60,
        description="Phase B: resolution-time decay independent signal",
    ),
    Task(
        name="pnl_reconstruction",
        # FIFO lot-matching across wallet_trades to compute realized PnL
        # from pre-resolution sells (not just resolved markets). Writes
        # realized_pnl_closed per fill and realized_pnl_total on
        # wallet_profiles. Must run after wallet_trade_sync.
        # 2026-04-25: chained → wallet_behavior_metrics so bootstrap-CI
        # + sizing distribution + farmer detection refresh in the same
        # cycle as the underlying trade data, instead of lagging a day.
        cmd=(
            "python -m trading_platform.polymarket.wallet_pnl_reconstruction "
            "&& python -m trading_platform.polymarket.wallet_behavior_metrics"
        ),
        interval_seconds=24 * 3600,
        description="Daily FIFO PnL reconstruction → behavior metrics chain",
    ),
    # `daily_digest` above (Python module) supersedes the earlier
    # curl-based API digest task — removed 2026-04-14 to eliminate the
    # duplicate name that was causing Task dict collisions.
    Task(
        name="categories_backfill_weekly",
        # NOTE: `backfill-categories` is not a polymarket CLI subcommand.
        # The category backfill is invoked via the API endpoint
        # /api/categories/backfill or directly via market_categorizer.
        # Disabled until the CLI command exists; categories are currently
        # at 0% null so the weekly catch-up is not load-bearing.
        cmd="trading-cli data polymarket backfill-categories --no-reclassify-other",
        interval_seconds=7 * 24 * 3600,
        description="Weekly catch-up for any null-category rows (DISABLED — missing CLI cmd)",
        enabled=False,
    ),

    Task(
        name="paper_check_exits",
        cmd="curl -fsS -X POST http://api:8001/api/paper/check-exits",
        interval_seconds=15 * 60,
        description="Check open paper trades for stop-loss, take-profit, time-decay exits (every 15min)",
    ),
    Task(
        name="paper_equity_snapshot",
        cmd="curl -fsS -X POST http://api:8001/api/paper/snapshot-equity",
        interval_seconds=60 * 60,
        description="Record equity curve data point (hourly)",
    ),
    Task(
        name="position_mark_to_market",
        cmd=(
            "python -c \""
            "from trading_platform.polymarket.position_monitor import mark_to_market; "
            "r = mark_to_market(); print(f'[MTM] {r}')\""
        ),
        interval_seconds=30 * 60,
        description="Snapshot open positions with current unrealized P&L (every 30 min)",
    ),
    Task(
        name="live_position_monitor",
        # Live trades have their own exit monitor (mirrors paper exits).
        # Runs every 5 min so SL/TP/trailing exits fire promptly. The
        # script is safe-by-default — sell failures leave position open
        # and log; never crash. No-op when there are zero live positions.
        cmd="python -m trading_platform.polymarket.live_position_monitor",
        interval_seconds=5 * 60,
        description="Live position auto-exit monitor (every 5min)",
    ),
    Task(
        name="signal_anomaly_detector",
        # Detect silent or spiking signal types every 30 min. Compares
        # last-hour fires vs 7-day per-hour baseline; alerts via Telegram
        # on >10× drop or >5× spike. Dedup'd per signal type for 6h.
        cmd="python -m trading_platform.polymarket.signal_anomaly_detector",
        interval_seconds=30 * 60,
        description="Signal flow anomaly detector (every 30min)",
    ),
    Task(
        name="pg_prune",
        # Weekly pruning: market_ticks >90d, service_health >7d,
        # circuit_breaker_log >90d. VACUUM ANALYZE at the end.
        cmd="sh /app/scripts/pg_prune.sh",
        interval_seconds=7 * 24 * 3600,
        description="Weekly Postgres pruning + VACUUM ANALYZE",
    ),
    Task(
        # 2026-04-24: daily smoke test — fires a $0.50 synthetic paper
        # trade through the full pipeline and verifies yesterday's
        # synthetic actually resolved. Catches silent pipeline gaps
        # (signals fire but no trades open, or trades open but never
        # resolve) within 24h instead of the weeks-long blind window
        # we used to have.
        name="synthetic_test_trade",
        cmd="python scripts/synthetic_test_trade.py",
        interval_seconds=24 * 3600,
        description="Daily synthetic test trade + yesterday's-resolution check",
    ),
    Task(
        name="pg_backup",
        # Nightly dump of the Postgres DB to /backups (bind-mounted).
        # Keeps 7 daily snapshots. pg_backup.sh handles retention pruning.
        cmd="sh /app/scripts/pg_backup.sh",
        interval_seconds=24 * 3600,
        description="Nightly Postgres dump with 7-day retention",
    ),

    # wallet_trade_poller moved to its own docker-compose service
    # (polymarket-wallet-poller) on 2026-04-15. A single cycle ~140s was
    # starving the scheduler's sequential queue and tripping stale-task
    # alerts. The dedicated container runs an explicit sleep loop at
    # POLLER_INTERVAL_SECONDS (default 600s). No Task() here.

    # ── TODO — wire when underlying commands are stable ────────────────
    # Task(
    #     name="signal_engine_loop",
    #     cmd="trading-cli data polymarket realtime-monitor",
    #     interval_seconds=3600,
    #     description="Run the live signal engine for an hour",
    #     enabled=False,
    # ),
    # Task(
    #     name="hot_market_scan",
    #     cmd="trading-cli data polymarket realtime-monitor --hot-scan-only",
    #     interval_seconds=300,
    #     description="HotMarketScanner cycle",
    #     enabled=False,
    # ),
]


def _persist_state(tasks: list[Task]) -> None:
    payload = {
        "updated_at": int(time.time()),
        "tasks": [asdict(t) for t in tasks],
    }
    try:
        STATE_PATH.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    except Exception as exc:
        logger.warning("scheduler state write failed: %s", exc)


def _should_alert_on_failure(consecutive_failures: int) -> bool:
    """Return True if the failure count crosses an escalation step."""
    if consecutive_failures in ALERT_ON_FAILURE_COUNT:
        return True
    if (
        consecutive_failures > max(ALERT_ON_FAILURE_COUNT)
        and consecutive_failures % ALERT_TAIL_SPAM_EVERY == 0
    ):
        return True
    return False


def _send_failure_alert(task: Task, error: str) -> None:
    if not _should_alert_on_failure(task.consecutive_failures):
        return
    try:
        from trading_platform.polymarket.telegram_alerts import get_alerter
        alerter = get_alerter()
        if not alerter.enabled:
            return
        if task.consecutive_failures == 1:
            severity = "\u26a0\ufe0f <b>SCHEDULER TASK FAILED</b>"
        elif task.consecutive_failures < 25:
            severity = (
                f"\U0001f7e0 <b>SUSTAINED FAILURE — {task.consecutive_failures}× in a row</b>"
            )
        else:
            duration_h = (
                (time.time() - (task.first_failure_at or time.time())) / 3600
            )
            severity = (
                f"\U0001f534 <b>INCIDENT — {task.consecutive_failures}× failures over {duration_h:.1f}h</b>"
            )
        msg = (
            f"{severity}\n"
            f"Task: <code>{task.name}</code>\n"
            f"Cmd: <code>{task.cmd[:80]}</code>\n"
            f"Error: {error[:300]}\n"
            f"────────\n\U0001f5a5 localhost:5173"
        )
        alerter._send(msg, disable_notification=(task.consecutive_failures < 5))
    except Exception:
        pass


def _send_recovery_alert(task: Task, prior_failures: int) -> None:
    """Single recovery alert when a task transitions failed → ok after >=5
    consecutive failures. Below 5 we don't bother — too quiet to be a real
    incident.
    """
    if prior_failures < 5:
        return
    try:
        from trading_platform.polymarket.telegram_alerts import get_alerter
        alerter = get_alerter()
        if not alerter.enabled:
            return
        msg = (
            f"\U0001f7e2 <b>SCHEDULER TASK RECOVERED</b>\n"
            f"Task: <code>{task.name}</code>\n"
            f"Was failing {prior_failures} cycles in a row.\n"
            f"────────\n\U0001f5a5 localhost:5173"
        )
        alerter._send(msg, disable_notification=False)
    except Exception:
        pass


def _is_transient_error(output: str) -> bool:
    """Return True for errors worth retrying once.

    Connection refused / curl exit 7 / (7) Failed to connect are the
    typical transient-API-hiccup patterns. Everything else we treat as
    a legitimate failure so we don't paper over real bugs.
    """
    if not output:
        return False
    o = output.lower()
    return any(p in o for p in (
        "connection refused",
        "curl: (7)",
        "curl: (28)",            # timeout
        "curl: (56)",            # recv failure
        "temporary failure in name resolution",
    ))


def _run_task(task: Task) -> None:
    started = time.time()
    log_path = LOG_DIR / f"{task.name}.log"
    logger.info("[run] %s — %s", task.name, task.cmd[:80])
    attempts = 0
    max_attempts = 2  # try once, retry once on transient errors
    result = None
    try:
        while attempts < max_attempts:
            attempts += 1
            result = subprocess.run(
                task.cmd,
                shell=True,
                cwd=str(PROJECT_ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=max(60, task.interval_seconds // 2),
            )
            if result.returncode == 0:
                break
            if attempts >= max_attempts:
                break
            if _is_transient_error(result.stdout or ""):
                logger.info("[retry] %s transient error, retrying in 3s", task.name)
                time.sleep(3)
                continue
            break  # non-transient failure — don't retry
        elapsed = time.time() - started
        ts = datetime.now(tz=timezone.utc).isoformat()
        with log_path.open("a", encoding="utf-8") as f:
            f.write(f"\n=== {ts} (exit={result.returncode}, {elapsed:.1f}s, attempts={attempts}) ===\n")
            f.write(result.stdout or "")
        task.last_run_at = started
        task.last_duration_s = round(elapsed, 1)
        if result.returncode == 0:
            prior_failures = task.consecutive_failures
            task.last_status = "ok"
            task.last_error = None
            task.consecutive_failures = 0
            task.first_failure_at = None
            logger.info("[ok ] %s in %.1fs%s", task.name, elapsed,
                        f" ({attempts} attempts)" if attempts > 1 else "")
            if prior_failures > 0:
                _send_recovery_alert(task, prior_failures)
        else:
            task.last_status = "failed"
            task.last_error = (result.stdout or "")[-500:]
            task.consecutive_failures += 1
            if task.first_failure_at is None:
                task.first_failure_at = started
            logger.warning(
                "[fail] %s exit=%d (after %d attempts, %d consecutive)",
                task.name, result.returncode, attempts, task.consecutive_failures,
            )
            _send_failure_alert(task, task.last_error or f"exit {result.returncode}")
    except subprocess.TimeoutExpired:
        task.last_run_at = started
        task.last_status = "failed"
        task.last_error = "timeout"
        task.last_duration_s = time.time() - started
        task.consecutive_failures += 1
        if task.first_failure_at is None:
            task.first_failure_at = started
        logger.warning("[timeout] %s (%d consecutive)", task.name, task.consecutive_failures)
        _send_failure_alert(task, "timeout")
    except Exception as exc:
        task.last_run_at = started
        task.last_status = "failed"
        task.last_error = str(exc)[:500]
        task.last_duration_s = time.time() - started
        task.consecutive_failures += 1
        if task.first_failure_at is None:
            task.first_failure_at = started
        logger.exception("[error] %s (%d consecutive)", task.name, task.consecutive_failures)
        _send_failure_alert(task, str(exc))


def _load_state(tasks: list[Task]) -> None:
    """Restore last_run_at and friends from the persisted state file.

    Wired 2026-04-25 after diagnosing a silent class of failure: a
    scheduler restart was wiping every task's `last_run_at`, which pushed
    each daily task's next run ~24h into the future. Multiple restarts
    in a session = perpetual postponement. `pm_leaderboard_sync` not
    running today (despite running yesterday) traced back to exactly
    this — the boundary kept sliding forward each time the container
    cycled.

    Now: on startup, hydrate timers from `state.json` (only fields that
    matter for cadence + alerting). Unknown tasks in state.json are
    ignored; tasks new to SCHEDULE start clean.
    """
    if not STATE_PATH.exists():
        return
    try:
        data = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("scheduler state load failed: %s", exc)
        return
    by_name = {t.name: t for t in tasks}
    restored = 0
    for row in data.get("tasks", []):
        name = row.get("name")
        t = by_name.get(name)
        if not t:
            continue
        t.last_run_at = row.get("last_run_at")
        t.last_status = row.get("last_status")
        t.last_duration_s = row.get("last_duration_s")
        t.last_error = row.get("last_error")
        t.consecutive_failures = int(row.get("consecutive_failures") or 0)
        t.first_failure_at = row.get("first_failure_at")
        if t.last_run_at is not None:
            restored += 1
    logger.info("scheduler restored %d task timers from state", restored)


def main() -> None:
    logger.info("scheduler starting with %d tasks", len([t for t in SCHEDULE if t.enabled]))
    _load_state(SCHEDULE)
    _persist_state(SCHEDULE)
    while True:
        now = time.time()
        ran_any = False
        for task in SCHEDULE:
            if not task.enabled:
                continue
            if now >= task.next_run_at(now):
                _run_task(task)
                ran_any = True
        if ran_any:
            _persist_state(SCHEDULE)
        # Sleep until the next due task or 30s, whichever is sooner
        next_due = min(
            (t.next_run_at(now) for t in SCHEDULE if t.enabled),
            default=now + 30,
        )
        sleep_for = max(5.0, min(30.0, next_due - now))
        time.sleep(sleep_for)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("scheduler stopped")
