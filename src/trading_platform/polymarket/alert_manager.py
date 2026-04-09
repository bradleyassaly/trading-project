"""
Centralized Telegram alert manager — three tiers, dedup, rate limit.

Design principles
-----------------
- **Silence = healthy.** No "all systems normal" messages.
- **Critical alerts** fire immediately, once per incident, never deduped or rate-limited.
- **Trade events** fire on every place / resolve with full context, subject to dedup.
- **Daily digest** fires once per day at 08:00 UTC with the overnight summary.
- **Dedup**: same message text within ``DEDUP_WINDOW_SECONDS`` is dropped.
- **Rate limit**: ``MAX_PER_HOUR`` non-critical messages per rolling hour.
- **Critical** tier bypasses both dedup and rate limit.

This module is the single entry point for Telegram from anywhere in the
codebase. ``TelegramAlerter`` (the low-level transport) still exists but
should only be called by ``AlertManager._send``. Direct callsites in
older code paths are being migrated incrementally — see
``reports/alert_system.md`` for the migration table.
"""
from __future__ import annotations

import logging
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# ── Tunables ────────────────────────────────────────────────────────────────

#: Suppress identical message text within this window.
DEDUP_WINDOW_SECONDS = 30 * 60

#: Max non-critical messages allowed per rolling 1h window.
MAX_PER_HOUR = 30

#: Max length we'll send to Telegram (4096 is the hard limit; leave headroom).
MAX_MESSAGE_CHARS = 4000

#: Number of recent messages to keep in the in-memory log (for /api/alerts/recent).
RECENT_LOG_SIZE = 100

#: Number of repeated daily-task failures before alerting.
TASK_FAILURE_THRESHOLD = 3


# ── Tier enum ───────────────────────────────────────────────────────────────

class Tier:
    """Alert tier classifier — controls dedup / rate-limit / notification."""
    CRITICAL = "critical"   # bypass dedup + rate limit, loud notification
    TRADE = "trade"         # subject to dedup, subject to rate limit, loud
    INFO = "info"           # subject to everything, silent notification
    DIGEST = "digest"       # daily summary, silent, bypasses dedup


# ── In-memory state (per process) ───────────────────────────────────────────

@dataclass
class _AlertManagerState:
    sent_log: deque = field(default_factory=lambda: deque(maxlen=RECENT_LOG_SIZE))
    dedup_cache: dict[str, float] = field(default_factory=dict)  # text -> last_sent_ts
    hourly_send_log: list[float] = field(default_factory=list)
    suppressed_today: int = 0
    sent_today: int = 0
    today_date: str = ""
    # Trade-event counters for the digest
    trades_placed_today: int = 0
    trades_resolved_today: int = 0
    trades_won_today: int = 0
    trades_lost_today: int = 0
    pnl_today: float = 0.0
    skipped_signals_today: dict[str, int] = field(default_factory=dict)  # reason -> count
    # Per-incident state
    failure_state: dict[str, float] = field(default_factory=dict)  # component -> first_failure_ts
    consecutive_task_failures: dict[str, int] = field(default_factory=dict)  # task -> count


_state = _AlertManagerState()
_lock = threading.Lock()


# ── Core class ──────────────────────────────────────────────────────────────

class AlertManager:
    """Centralized Telegram alert dispatcher with three tiers."""

    def __init__(self) -> None:
        # Lazy import so test suites that mock the transport keep working.
        from trading_platform.polymarket.telegram_alerts import get_alerter
        self._alerter = get_alerter()
        self._state = _state  # shared module-level state

    # ── Public API: TIER 1 — CRITICAL ───────────────────────────────────────

    def alert_circuit_breaker_triggered(
        self,
        equity: float,
        peak: float,
        drawdown_pct: float,
        reason: str,
    ) -> bool:
        msg = (
            "🛑 <b>CIRCUIT BREAKER TRIGGERED</b>\n"
            f"Equity: ${equity:,.0f} | Peak: ${peak:,.0f}\n"
            f"Drawdown: {drawdown_pct * 100:.1f}%\n"
            f"Reason: {reason}\n\n"
            "<b>ALL TRADING HALTED.</b> Manual reset required.\n"
            "<code>POST /api/circuit-breaker/reset</code>"
        )
        return self._dispatch(msg, tier=Tier.CRITICAL)

    def alert_service_down(
        self,
        service_name: str,
        details: str,
    ) -> bool:
        """Fires once per incident — second call for same service is a no-op."""
        with _lock:
            if service_name in self._state.failure_state:
                return False
            self._state.failure_state[service_name] = time.time()
        msg = (
            f"🔴 <b>SERVICE DOWN: {service_name}</b>\n"
            f"At: {self._now_str()}\n"
            f"Details: {details[:300]}"
        )
        return self._dispatch(msg, tier=Tier.CRITICAL)

    def alert_service_recovered(self, service_name: str) -> bool:
        """Fires when a previously-down service comes back."""
        with _lock:
            failed_at = self._state.failure_state.pop(service_name, None)
        if failed_at is None:
            return False
        downtime_min = (time.time() - failed_at) / 60
        msg = (
            f"🟢 <b>RECOVERED: {service_name}</b>\n"
            f"Was down for {downtime_min:.0f} minutes."
        )
        return self._dispatch(msg, tier=Tier.CRITICAL)

    def alert_execution_error(
        self,
        signal_type: str,
        market: str,
        error: str,
    ) -> bool:
        msg = (
            "🔴 <b>LIVE TRADE EXECUTION FAILED</b>\n"
            f"Signal: {signal_type}\n"
            f"Market: {market[:80]}\n"
            f"Error: {error[:300]}"
        )
        return self._dispatch(msg, tier=Tier.CRITICAL)

    def alert_daily_loss_limit(
        self,
        daily_pnl: float,
        limit: float,
        equity: float,
    ) -> bool:
        msg = (
            "⚠️ <b>DAILY LOSS LIMIT HIT</b>\n"
            f"Daily P&L: ${daily_pnl:.0f} | Limit: -${limit:.0f}\n"
            f"Equity: ${equity:,.0f}\n"
            "Trading paused until midnight."
        )
        return self._dispatch(msg, tier=Tier.CRITICAL)

    def alert_consecutive_task_failure(
        self,
        task_name: str,
        last_error: str,
    ) -> bool:
        """Tracks consecutive failures per task; alerts only at the threshold."""
        with _lock:
            count = self._state.consecutive_task_failures.get(task_name, 0) + 1
            self._state.consecutive_task_failures[task_name] = count
        if count < TASK_FAILURE_THRESHOLD:
            return False
        msg = (
            f"🟡 <b>TASK FAILING REPEATEDLY: {task_name}</b>\n"
            f"Failed {count} times in a row.\n"
            f"Last error: {last_error[:300]}"
        )
        return self._dispatch(msg, tier=Tier.CRITICAL)

    def task_recovered(self, task_name: str) -> None:
        """Reset the consecutive-failure counter when a task succeeds."""
        with _lock:
            self._state.consecutive_task_failures.pop(task_name, None)

    # ── Public API: TIER 2 — TRADE EVENTS ───────────────────────────────────

    def alert_trade_placed(
        self,
        signal_type: str,
        wallet: str,
        market: str,
        direction: str,
        stake: float,
        entry_price: float,
        fusion_score: float | None = None,
        wallet_tier: str | None = None,
        hypothesis: str | None = None,
        alpha_score: float | None = None,
    ) -> bool:
        """Send a TRADE PLACED alert.

        Per Part 4 of signal_analysis_clean.md, the body now embeds the
        ``hypothesis`` text — a 2–4 sentence rationale built from the
        wallet's alpha score, recent form, convergence count, and the
        signal type that fired. The header stays identical (one-line
        scan on a phone) but the body becomes a reasoned trade note
        rather than a bare facts dump.
        """
        with _lock:
            self._reset_day_if_needed()
            self._state.trades_placed_today += 1
        wallet_short = (wallet or "")[:14] + ("…" if len(wallet or "") > 14 else "")
        tier_badge = f" ({wallet_tier})" if wallet_tier else ""
        score_str = (
            f" | α={alpha_score:.2f}" if alpha_score is not None
            else (f" | Fusion: {fusion_score:.2f}" if fusion_score is not None else "")
        )
        lines = [
            "📈 <b>TRADE PLACED</b>",
            f"Signal: {signal_type}",
            f"Wallet: <code>{wallet_short}</code>{tier_badge}",
            f"Market: {(market or '')[:80]}",
            f"{direction} @ {entry_price:.3f} | Stake: ${stake:.0f}{score_str}",
        ]
        if hypothesis:
            lines.append("")
            lines.append(f"<b>Why:</b> {hypothesis}")
        return self._dispatch("\n".join(lines), tier=Tier.TRADE)

    def alert_trade_resolved(
        self,
        signal_type: str,
        market: str,
        direction: str,
        entry_price: float,
        exit_price: float,
        pnl: float,
        cumulative_pnl: float,
        win_rate: float | None = None,
        resolved_count: int | None = None,
    ) -> bool:
        won = pnl > 0
        with _lock:
            self._reset_day_if_needed()
            self._state.trades_resolved_today += 1
            if won:
                self._state.trades_won_today += 1
            else:
                self._state.trades_lost_today += 1
            self._state.pnl_today += pnl
        icon = "✅" if won else "❌"
        verdict = "TRADE WON" if won else "TRADE LOST"
        wr_str = f" | WR: {win_rate * 100:.0f}%" if win_rate is not None else ""
        resolved_str = f" ({resolved_count})" if resolved_count is not None else ""
        msg = (
            f"{icon} <b>{verdict}</b>\n"
            f"Signal: {signal_type}\n"
            f"Market: {(market or '')[:80]}\n"
            f"{direction} {entry_price:.3f} → {exit_price:.3f} | "
            f"P&L: {'+' if pnl >= 0 else ''}${pnl:.0f}\n"
            f"\n"
            f"Cumulative: ${cumulative_pnl:+,.0f}{wr_str}\n"
            f"Resolved today: {resolved_str}"
        )
        return self._dispatch(msg, tier=Tier.TRADE)

    def record_signal_skipped(self, reason: str) -> None:
        """Accumulate skipped-signal counts for the daily digest. No message sent."""
        with _lock:
            self._reset_day_if_needed()
            self._state.skipped_signals_today[reason] = (
                self._state.skipped_signals_today.get(reason, 0) + 1
            )

    # ── Public API: TIER 3 — DAILY DIGEST ──────────────────────────────────

    def send_daily_digest(self) -> bool:
        """Compose + send the once-per-day summary. Reads live state from
        the API endpoints we already have, plus this manager's in-memory
        per-day counters."""
        sections = []

        sections.append(f"📊 <b>DAILY DIGEST — {datetime.now(tz=timezone.utc).strftime('%Y-%m-%d')}</b>")

        # ── THESIS SCORECARD (lead) ─────────────────────────────────
        # The single most important section. Pulled from KPITracker so
        # the digest, dashboard, and /api/thesis/scorecard all see the
        # same numbers.
        try:
            from trading_platform.polymarket.kpi_tracker import KPITracker
            kpi = KPITracker().compute_all()
            sc = kpi.get("thesis_scorecard") or {}
            c1 = kpi.get("claim_1_persistence") or {}
            c2 = kpi.get("claim_2_category") or {}
            c3 = kpi.get("claim_3_identification") or {}
            c4 = kpi.get("claim_4_timing") or {}
            acc = sc.get("accuracy")
            acc_str = f"{acc * 100:.0f}%" if acc is not None else "—"
            sections.append(
                "\n<b>═══ THESIS SCORECARD ═══</b>\n"
                f"Hypothesis accuracy: {acc_str} ({sc.get('correct',0)}/{sc.get('total_hypotheses',0)})\n"
                f"Trend: {sc.get('trend') or 'n/a'}\n"
                f"Verdict: {sc.get('verdict','—')}"
            )
            sections.append(
                "\n<b>═══ CLAIMS ═══</b>\n"
                f"1. Persistent edge: {c1.get('persistence','?')} (cohort WR {(c1.get('clean_cohort_wr') or 0)*100:.0f}%)\n"
                f"2. Category-specific: {c2.get('categories_with_edge',0)} categories with edge\n"
                f"3. Identification: {c3.get('distinct_copyable_wallets',0)} copyable wallets ({c3.get('selectivity',0)*100:.0f}% selectivity)\n"
                f"4. Real-time copying: {c4.get('alpha_gated_trades',0)} alpha-gated trades placed\n"
                f"5. Transaction costs: NOT YET TESTED"
            )
        except Exception as exc:
            logger.debug("digest thesis section failed: %s", exc)

        # ── TRADES section (manager state) ──────────────────────────
        with _lock:
            placed = self._state.trades_placed_today
            resolved = self._state.trades_resolved_today
            won = self._state.trades_won_today
            lost = self._state.trades_lost_today
            pnl_today = self._state.pnl_today
            skipped = dict(self._state.skipped_signals_today)
            sent = self._state.sent_today
            suppressed = self._state.suppressed_today
        sections.append(
            "\n<b>TRADES</b>\n"
            f"Placed: {placed} | Resolved: {resolved} ({won}W / {lost}L)\n"
            f"Today P&L: {'+' if pnl_today >= 0 else ''}${pnl_today:.0f}"
        )

        # ── PORTFOLIO section (live API) ────────────────────────────
        try:
            br = self._fetch_json("/api/paper/bankroll") or {}
            cb = self._fetch_json("/api/circuit-breaker/status") or {}
            equity = br.get("current_equity") or br.get("bankroll") or 100000
            cum_pnl = (br.get("total_pnl") or (equity - (br.get("starting_capital") or 100000)))
            peak = cb.get("peak_equity") or equity
            dd_pct = (cb.get("current_drawdown_pct") or 0) * 100
            halted = cb.get("is_halted")
            sections.append(
                "\n<b>PORTFOLIO</b>\n"
                f"Equity: ${equity:,.0f} | Peak: ${peak:,.0f} | DD: {dd_pct:.1f}%\n"
                f"Cumulative P&L: {'+' if cum_pnl >= 0 else ''}${cum_pnl:,.0f}"
                + ("\n🛑 <b>HALTED</b>" if halted else "")
            )
        except Exception as exc:
            logger.debug("digest portfolio fetch failed: %s", exc)

        # ── SIGNALS section (manager state) ─────────────────────────
        skip_total = sum(skipped.values())
        top_skip = max(skipped.items(), key=lambda x: x[1]) if skipped else None
        sections.append(
            "\n<b>SIGNALS</b>\n"
            f"Skipped: {skip_total}"
            + (f" (top: {top_skip[0]} ×{top_skip[1]})" if top_skip else "")
        )

        # ── CALIBRATION section ─────────────────────────────────────
        try:
            cal = self._fetch_json("/api/calibration/status") or {}
            rows = cal.get("rows") or cal.get("calibration") or []
            cal_lines = []
            for r in rows[:6]:
                sig = r.get("signal_type", "?")
                status = r.get("status", "?")
                wr = r.get("bayesian_wr") or r.get("win_rate") or 0
                n = r.get("sample_size") or 0
                cal_lines.append(f"  {sig}: {status} (WR {wr * 100:.0f}%, n={n})")
            if cal_lines:
                sections.append("\n<b>CALIBRATION</b>\n" + "\n".join(cal_lines))
        except Exception as exc:
            logger.debug("digest calibration fetch failed: %s", exc)

        # ── SYSTEM section ──────────────────────────────────────────
        try:
            sched = self._fetch_json("/api/system/scheduler-status") or {}
            tasks = sched.get("tasks") or []
            healthy = sum(1 for t in tasks if t.get("last_status") == "ok")
            total = len(tasks)
            stats = self._fetch_json("/api/smart-money/universe-stats") or {}
            t1h = stats.get("tier1h", 0)
            t1 = stats.get("tier1", 0)
            t2 = stats.get("tier2", 0)
            sections.append(
                "\n<b>SYSTEM</b>\n"
                f"Scheduler: {healthy}/{total} tasks ok\n"
                f"Watched wallets: {t1h + t1 + t2} (t1h:{t1h} t1:{t1} t2:{t2})\n"
                f"Alerts sent today: {sent} | suppressed: {suppressed}"
            )
        except Exception as exc:
            logger.debug("digest system fetch failed: %s", exc)

        sections.append("\n—\nThis is the only routine daily message.")

        msg = "\n".join(sections)

        # Reset per-day counters AFTER composing the digest.
        with _lock:
            self._state.trades_placed_today = 0
            self._state.trades_resolved_today = 0
            self._state.trades_won_today = 0
            self._state.trades_lost_today = 0
            self._state.pnl_today = 0.0
            self._state.skipped_signals_today.clear()

        return self._dispatch(msg, tier=Tier.DIGEST)

    # ── Internals ───────────────────────────────────────────────────────────

    def _dispatch(self, message: str, *, tier: str) -> bool:
        """The single chokepoint. All callers route here."""
        if not self._alerter.enabled:
            return False
        message = message[:MAX_MESSAGE_CHARS]
        with _lock:
            self._reset_day_if_needed()
            now = time.time()

            # CRITICAL bypasses everything except the transport's loud notification flag
            if tier == Tier.CRITICAL:
                pass
            else:
                # Dedup check
                if self._is_duplicate(message, now):
                    self._state.suppressed_today += 1
                    return False
                # Rate limit check
                if self._is_rate_limited(now):
                    self._state.suppressed_today += 1
                    return False
                # Record for rate-limit counting
                self._state.hourly_send_log.append(now)
            self._state.dedup_cache[message] = now
            self._state.sent_today += 1
            self._state.sent_log.append({
                "ts": int(now),
                "tier": tier,
                "text": message[:200],
            })

        # Critical and trade tiers get loud notification, info/digest stay silent.
        loud = tier in (Tier.CRITICAL, Tier.TRADE)
        try:
            return self._alerter._send(message, disable_notification=not loud)
        except Exception as exc:
            logger.warning("AlertManager dispatch failed: %s", exc)
            return False

    def _is_duplicate(self, message: str, now: float) -> bool:
        last = self._state.dedup_cache.get(message)
        if last is None:
            return False
        return (now - last) < DEDUP_WINDOW_SECONDS

    def _is_rate_limited(self, now: float) -> bool:
        cutoff = now - 3600
        self._state.hourly_send_log = [t for t in self._state.hourly_send_log if t >= cutoff]
        return len(self._state.hourly_send_log) >= MAX_PER_HOUR

    def _reset_day_if_needed(self) -> None:
        """Roll the per-day counters at UTC midnight."""
        today = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
        if self._state.today_date != today:
            self._state.today_date = today
            self._state.sent_today = 0
            self._state.suppressed_today = 0

    @staticmethod
    def _now_str() -> str:
        return datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    @staticmethod
    def _fetch_json(path: str) -> Any:
        """GET helper for the digest builder. Talks to the local API.
        Returns None on any failure — never raises."""
        import os
        import urllib.request
        try:
            base = os.environ.get("ALERT_MANAGER_API_BASE", "http://api:8001")
            with urllib.request.urlopen(base + path, timeout=5) as resp:
                if resp.status == 200:
                    import json
                    return json.loads(resp.read().decode("utf-8", errors="ignore"))
        except Exception:
            return None
        return None

    # ── Introspection (for /api/alerts/* endpoints) ─────────────────────────

    def get_recent(self, limit: int = 20) -> list[dict[str, Any]]:
        with _lock:
            entries = list(self._state.sent_log)[-limit:]
        return list(reversed(entries))

    def get_config(self) -> dict[str, Any]:
        with _lock:
            self._reset_day_if_needed()
            return {
                "telegram_configured": bool(self._alerter.enabled),
                "rate_limit_per_hour": MAX_PER_HOUR,
                "dedup_window_minutes": DEDUP_WINDOW_SECONDS // 60,
                "digest_time_utc": "08:00",
                "alerts_sent_today": self._state.sent_today,
                "alerts_suppressed_today": self._state.suppressed_today,
                "trades_placed_today": self._state.trades_placed_today,
                "trades_resolved_today": self._state.trades_resolved_today,
                "skipped_signals_today": dict(self._state.skipped_signals_today),
                "active_failures": list(self._state.failure_state.keys()),
            }


# ── Module-level singleton ──────────────────────────────────────────────────

_alert_manager: AlertManager | None = None
_init_lock = threading.Lock()


def get_alert_manager() -> AlertManager:
    """Module-level singleton — must be used everywhere instead of constructing
    AlertManager() directly, so dedup/rate-limit state is shared process-wide."""
    global _alert_manager
    if _alert_manager is None:
        with _init_lock:
            if _alert_manager is None:
                _alert_manager = AlertManager()
    return _alert_manager
