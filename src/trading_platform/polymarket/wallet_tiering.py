"""
Dynamic per-wallet per-category tier engine.

Replaces the static tier1h/tier1/tier2 leaderboard tiers with rolling
S/A/B/C/D tiers that move based on each wallet's recent performance in
each category. Wallets that were great 6 months ago in politics but
have been losing for the last month get demoted; wallets that recover
get promoted (slowly).

Status flow
-----------
Static (leaderboard) tiers stay as-is. This engine writes to a *new*
table — ``wallet_category_profiles`` — that's queried by the fusion
score's ``tier_multiplier`` lookup. The fusion score now uses dynamic
tiers when available, falling back to the static tier1h/tier1/tier2
mapping when a category profile doesn't exist yet.

Data sources
------------
* ``polymarket_paper_trades`` — resolved paper-trade outcomes (the
  ground truth for win rate per signal/category)
* ``wallet_trades`` — raw fills (for category purity, last_trade_at,
  avg bet size)

Public API
----------
* :meth:`build_all_profiles` — full nightly rebuild
* :meth:`evaluate_single_wallet` — incremental update after a
  resolution
* :meth:`get_tier(wallet, category)` — fast lookup
* :meth:`get_tier_multiplier(wallet, category)` — fusion-score helper
"""
from __future__ import annotations

import logging
import math
import sqlite3
import time
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────

TIER_MULTIPLIERS = {
    "S": 1.00,
    "A": 0.85,
    "B": 0.70,
    "C": 0.50,
    "D": 0.30,
}

TIER_ORDER = ("S", "A", "B", "C", "D")
TIER_TO_RANK = {t: i for i, t in enumerate(TIER_ORDER)}  # 0=S, 4=D

# Cooldown between voluntary tier changes (forced demotions bypass)
COOLDOWN_SECONDS = 48 * 3600

# Once demoted from S, need this many resolved trades before returning
S_TIER_LOCKOUT_TRADES = 20


@dataclass
class TierProfile:
    wallet: str
    category: str
    tier: str
    tier_score: float | None
    win_rate: float | None
    win_rate_30d: float | None
    win_rate_90d: float | None
    resolved_trades: int
    avg_bet_size: float | None
    monthly_pnl: float | None
    consistency_score: float | None
    category_purity: float | None
    trend_score: float | None
    last_trade_at: int | None
    consecutive_losses: int = 0
    cumulative_pnl: float = 0.0
    pnl_30d: float = 0.0
    cumulative_positive_pnl: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {k: getattr(self, k) for k in self.__dataclass_fields__}


@dataclass
class TierChange:
    wallet: str
    category: str
    old_tier: str | None
    new_tier: str
    reason: str
    trigger_metric: str | None
    old_tier_score: float | None
    new_tier_score: float | None
    win_rate_at_change: float | None
    win_rate_30d_at_change: float | None
    pnl_at_change: float | None


# ── Pure scoring helpers ────────────────────────────────────────────────────


def _recency_factor(last_trade_at: int | None, now: int) -> float:
    if last_trade_at is None:
        return 0.1
    days = max(0.0, (now - last_trade_at) / 86400.0)
    if days < 7:
        return 1.0
    if days < 30:
        return 0.7
    if days < 60:
        return 0.4
    return 0.1


def compute_tier_score(
    *,
    win_rate: float | None,
    consistency_score: float | None,
    resolved_trades: int,
    trend_score: float | None,
    category_purity: float | None,
    last_trade_at: int | None,
    now: int | None = None,
) -> float:
    """Composite tier_score in [0, 1]."""
    if now is None:
        now = int(time.time())
    wr = max(0.0, min(1.0, win_rate or 0.0))
    cs = max(0.0, min(3.0, consistency_score or 0.0)) / 3.0
    sample = min(resolved_trades / 30.0, 1.0)
    trend = max(0.0, (trend_score or 0.0) + 0.1)  # offset so neutral trend ~0.1
    purity = max(0.0, min(category_purity or 0.0, 0.8)) / 0.8
    rec = _recency_factor(last_trade_at, now)
    score = (
        wr * 0.30
        + cs * 0.20
        + sample * 0.15
        + trend * 0.15
        + purity * 0.10
        + rec * 0.10
    )
    return round(max(0.0, min(1.0, score)), 4)


def assign_tier(
    *,
    tier_score: float,
    resolved_trades: int,
    win_rate: float | None,
    cumulative_pnl: float,
) -> str:
    """Map composite metrics to a tier letter (S..D)."""
    wr = win_rate or 0.0
    if cumulative_pnl < 0 and resolved_trades >= 2:
        return "D"
    if resolved_trades < 4:
        return "C" if tier_score >= 0.25 else "D"
    # Note on thresholds: the composite formula caps at ~0.65 in practice
    # for a 20-trade wallet with 80% WR (the trend component requires a
    # positive 30d-vs-90d delta). S must remain reachable, so the
    # tier_score gate is set to 0.60 — the WR + sample gates do most of
    # the discrimination.
    if tier_score >= 0.60 and resolved_trades >= 15 and wr >= 0.60:
        return "S"
    if tier_score >= 0.50 and resolved_trades >= 8 and wr >= 0.52:
        return "A"
    if tier_score >= 0.38:
        return "B"
    if tier_score >= 0.25:
        return "C"
    return "D"


def _demote(tier: str, levels: int = 1) -> str:
    rank = TIER_TO_RANK[tier]
    return TIER_ORDER[min(len(TIER_ORDER) - 1, rank + levels)]


def _promote(tier: str) -> str:
    rank = TIER_TO_RANK[tier]
    return TIER_ORDER[max(0, rank - 1)]


def apply_demotion_triggers(
    proposed_tier: str,
    *,
    consecutive_losses: int,
    win_rate_30d: float | None,
    cumulative_pnl: float,
    cumulative_positive_pnl: float,
    pnl_30d: float,
    last_trade_at: int | None,
    now: int,
) -> tuple[str, str | None]:
    """Apply forced demotion triggers. Returns (final_tier, trigger_metric).

    trigger_metric is the name of the rule that fired, or None when no
    trigger applied.
    """
    # Trigger 1: 8+ consecutive losses → forced D
    if consecutive_losses >= 8:
        return "D", "consecutive_losses_8"
    if consecutive_losses >= 5:
        return _demote(proposed_tier, 1), "consecutive_losses_5"

    # Trigger 2: rolling 30d WR collapse
    if win_rate_30d is not None and win_rate_30d < 0.35:
        return _demote(proposed_tier, 1), "rolling_wr_collapse"

    # Trigger 3: PnL decay
    if (
        cumulative_positive_pnl > 0
        and pnl_30d < 0
        and abs(pnl_30d) > 0.30 * cumulative_positive_pnl
    ):
        return _demote(proposed_tier, 1), "pnl_decay"

    # Trigger 4: inactivity decay
    if last_trade_at is not None:
        days_inactive = (now - last_trade_at) / 86400.0
        if days_inactive >= 90:
            return "D", "inactivity_90d"
        if days_inactive >= 60:
            # Force at least C
            target = "C" if TIER_TO_RANK[proposed_tier] < TIER_TO_RANK["C"] else proposed_tier
            return target, "inactivity_60d"
        if days_inactive >= 30:
            return _demote(proposed_tier, 1), "inactivity_30d"

    return proposed_tier, None


def apply_promotion_gate(
    *,
    proposed_tier: str,
    current_tier: str,
    resolved_since_last_change: int,
    win_rate_30d: float | None,
    consistency_score: float | None,
    trend_score: float | None,
    last_change_at: int | None,
    now: int,
    s_tier_lockout_remaining: int = 0,
) -> tuple[str, bool]:
    """Apply promotion gates + cooldown. Returns (final_tier, blocked_by_cooldown).

    Promotions are asymmetric — easier to fall, harder to rise. A wallet
    that was demoted from S cannot return to S until it has accumulated
    at least ``S_TIER_LOCKOUT_TRADES`` more resolved trades.
    """
    proposed_rank = TIER_TO_RANK[proposed_tier]
    current_rank = TIER_TO_RANK[current_tier]

    # No promotion (proposed equal or worse) — return as-is
    if proposed_rank >= current_rank:
        return proposed_tier, False

    # Cooldown — voluntary promotions blocked within COOLDOWN_SECONDS
    if last_change_at is not None and (now - last_change_at) < COOLDOWN_SECONDS:
        return current_tier, True

    # S-tier lockout
    if proposed_tier == "S" and s_tier_lockout_remaining > 0:
        # Cap at A
        return "A", False

    # Per-step gate requirements
    target = proposed_tier
    next_rank = current_rank - 1
    while next_rank >= proposed_rank:
        next_tier = TIER_ORDER[next_rank]
        if not _meets_promotion_requirements(
            next_tier=next_tier,
            resolved_since_last_change=resolved_since_last_change,
            win_rate_30d=win_rate_30d,
            consistency_score=consistency_score,
            trend_score=trend_score,
        ):
            target = TIER_ORDER[next_rank + 1] if next_rank + 1 < len(TIER_ORDER) else current_tier
            return target, False
        next_rank -= 1
    return target, False


def _meets_promotion_requirements(
    *,
    next_tier: str,
    resolved_since_last_change: int,
    win_rate_30d: float | None,
    consistency_score: float | None,
    trend_score: float | None,
) -> bool:
    wr30 = win_rate_30d or 0.0
    cs = consistency_score or 0.0
    trend = trend_score or 0.0
    if next_tier == "C":
        return resolved_since_last_change >= 3
    if next_tier == "B":
        return resolved_since_last_change >= 5 and wr30 > 0.50
    if next_tier == "A":
        return wr30 > 0.55 and cs > 0.8
    if next_tier == "S":
        return wr30 > 0.60 and trend > 0 and resolved_since_last_change >= 10
    return True


# ── Engine ───────────────────────────────────────────────────────────────────


class WalletTieringEngine:
    """Compute and persist per-wallet per-category dynamic tiers."""

    DEFAULT_TIER_MULT = 0.50  # used when no profile exists

    def __init__(self, db_path: str) -> None:
        self._db_path = str(db_path)

    # ── Public lookups ─────────────────────────────────────────────────────

    def get_tier(self, wallet: str, category: str) -> str:
        try:
            conn = sqlite3.connect(self._db_path)
            try:
                row = conn.execute(
                    "SELECT tier FROM wallet_category_profiles WHERE wallet = ? AND category = ?",
                    ((wallet or "").lower(), (category or "").lower()),
                ).fetchone()
            finally:
                conn.close()
            if row and row[0]:
                return row[0]
        except Exception:
            pass
        return "D"

    def get_tier_multiplier(self, wallet: str, category: str) -> float:
        tier = self.get_tier(wallet, category)
        return TIER_MULTIPLIERS.get(tier, self.DEFAULT_TIER_MULT)

    def get_profile(self, wallet: str, category: str) -> dict[str, Any] | None:
        try:
            conn = sqlite3.connect(self._db_path)
            try:
                row = conn.execute(
                    """SELECT wallet, category, tier, tier_score, win_rate,
                              win_rate_30d, win_rate_90d, resolved_trades,
                              avg_bet_size, monthly_pnl, consistency_score,
                              category_purity, trend_score, last_trade_at,
                              last_evaluated, resolved_since_last_change,
                              last_change_at
                       FROM wallet_category_profiles
                       WHERE wallet = ? AND category = ?""",
                    ((wallet or "").lower(), (category or "").lower()),
                ).fetchone()
            finally:
                conn.close()
        except Exception:
            return None
        if not row:
            return None
        cols = [
            "wallet", "category", "tier", "tier_score", "win_rate",
            "win_rate_30d", "win_rate_90d", "resolved_trades", "avg_bet_size",
            "monthly_pnl", "consistency_score", "category_purity", "trend_score",
            "last_trade_at", "last_evaluated", "resolved_since_last_change",
            "last_change_at",
        ]
        return dict(zip(cols, row))

    # ── Core compute (pure-ish, used by both build + evaluate) ─────────────

    def _compute_profile(
        self,
        conn: sqlite3.Connection,
        wallet: str,
        category: str,
        now: int,
    ) -> TierProfile | None:
        """Build a TierProfile from the DB. Returns None for empty wallets."""
        wallet = (wallet or "").lower()
        category = (category or "").lower()

        # Resolved paper trades for this wallet+category
        rows = conn.execute(
            """SELECT realized_pnl, return_pct, outcome, exit_ts, size_usd
               FROM polymarket_paper_trades
               WHERE LOWER(wallet) = ? AND LOWER(category) = ?
                 AND archived = 0 AND exit_ts IS NOT NULL
                 AND outcome IS NOT NULL
               ORDER BY exit_ts ASC""",
            (wallet, category),
        ).fetchall()
        if not rows:
            return None

        n = len(rows)
        outcomes = [(r[2] or "").lower() for r in rows]
        pnls = [float(r[0] or 0.0) for r in rows]
        returns = [float(r[1] or 0.0) for r in rows]
        exit_ts_list = [int(r[3] or 0) for r in rows]
        sizes = [float(r[4] or 0.0) for r in rows]

        wins = sum(1 for o in outcomes if o == "win")
        win_rate = wins / n if n > 0 else None

        cutoff_30 = now - 30 * 86400
        cutoff_90 = now - 90 * 86400
        last_30 = [(o, p) for o, p, t in zip(outcomes, pnls, exit_ts_list) if t >= cutoff_30]
        last_90 = [(o, p) for o, p, t in zip(outcomes, pnls, exit_ts_list) if t >= cutoff_90]
        wr_30d = (sum(1 for o, _ in last_30 if o == "win") / len(last_30)) if last_30 else None
        wr_90d = (sum(1 for o, _ in last_90 if o == "win") / len(last_90)) if last_90 else None
        pnl_30d = sum(p for _, p in last_30)

        cumulative_pnl = sum(pnls)
        cumulative_positive_pnl = sum(p for p in pnls if p > 0)

        # Consistency: monthly_pnl_mean / monthly_pnl_std
        monthly_buckets: dict[str, float] = {}
        from datetime import datetime, timezone
        for p, t in zip(pnls, exit_ts_list):
            key = datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y-%m")
            monthly_buckets[key] = monthly_buckets.get(key, 0.0) + p
        monthly_values = list(monthly_buckets.values())
        if len(monthly_values) >= 2:
            mean = sum(monthly_values) / len(monthly_values)
            var = sum((v - mean) ** 2 for v in monthly_values) / len(monthly_values)
            std = math.sqrt(var)
            consistency = (mean / std) if std > 0 else 0.0
            monthly_pnl = mean
        else:
            consistency = 0.0
            monthly_pnl = monthly_values[0] if monthly_values else 0.0

        # Category purity from raw wallet_trades counts
        try:
            row_total = conn.execute(
                """SELECT COUNT(*),
                          SUM(CASE WHEN LOWER(category) = ? THEN 1 ELSE 0 END)
                   FROM wallet_trades WHERE LOWER(wallet) = ?""",
                (category, wallet),
            ).fetchone()
            total_trades = row_total[0] or 0
            cat_trades = row_total[1] or 0
            category_purity = (cat_trades / total_trades) if total_trades > 0 else 0.0
            last_trade_row = conn.execute(
                "SELECT MAX(timestamp) FROM wallet_trades WHERE LOWER(wallet) = ? AND LOWER(category) = ?",
                (wallet, category),
            ).fetchone()
            last_trade_at = int(last_trade_row[0]) if last_trade_row and last_trade_row[0] else exit_ts_list[-1]
        except Exception:
            category_purity = 1.0
            last_trade_at = exit_ts_list[-1]

        trend_score = None
        if wr_30d is not None and wr_90d is not None:
            trend_score = round(wr_30d - wr_90d, 4)

        avg_bet = (sum(sizes) / len(sizes)) if sizes else None

        # Consecutive losses from the tail
        consec = 0
        for o in reversed(outcomes):
            if o == "loss":
                consec += 1
            else:
                break

        tier_score = compute_tier_score(
            win_rate=win_rate,
            consistency_score=consistency,
            resolved_trades=n,
            trend_score=trend_score,
            category_purity=category_purity,
            last_trade_at=last_trade_at,
            now=now,
        )

        proposed_tier = assign_tier(
            tier_score=tier_score,
            resolved_trades=n,
            win_rate=win_rate,
            cumulative_pnl=cumulative_pnl,
        )

        return TierProfile(
            wallet=wallet,
            category=category,
            tier=proposed_tier,
            tier_score=tier_score,
            win_rate=round(win_rate, 4) if win_rate is not None else None,
            win_rate_30d=round(wr_30d, 4) if wr_30d is not None else None,
            win_rate_90d=round(wr_90d, 4) if wr_90d is not None else None,
            resolved_trades=n,
            avg_bet_size=round(avg_bet, 2) if avg_bet else None,
            monthly_pnl=round(monthly_pnl, 2),
            consistency_score=round(consistency, 4),
            category_purity=round(category_purity, 4),
            trend_score=trend_score,
            last_trade_at=last_trade_at,
            consecutive_losses=consec,
            cumulative_pnl=round(cumulative_pnl, 2),
            pnl_30d=round(pnl_30d, 2),
            cumulative_positive_pnl=round(cumulative_positive_pnl, 2),
        )

    def _apply_state_machine(
        self,
        profile: TierProfile,
        existing: dict[str, Any] | None,
        now: int,
    ) -> tuple[str, str | None, str]:
        """Run trigger + promotion-gate logic on a freshly computed profile.

        Returns ``(final_tier, trigger_metric, reason)``. The reason is
        a short human-readable string used in the history table.
        """
        proposed = profile.tier
        old_tier = (existing or {}).get("tier") if existing else None
        last_change_at = (existing or {}).get("last_change_at")
        resolved_since_last_change = ((existing or {}).get("resolved_since_last_change") or 0)

        # Forced demotions
        forced_tier, trigger = apply_demotion_triggers(
            proposed,
            consecutive_losses=profile.consecutive_losses,
            win_rate_30d=profile.win_rate_30d,
            cumulative_pnl=profile.cumulative_pnl,
            cumulative_positive_pnl=profile.cumulative_positive_pnl,
            pnl_30d=profile.pnl_30d,
            last_trade_at=profile.last_trade_at,
            now=now,
        )
        if trigger and old_tier and TIER_TO_RANK[forced_tier] > TIER_TO_RANK[old_tier]:
            return forced_tier, trigger, f"forced demotion: {trigger}"

        # If existing tier is the same as forced and trigger is set, still
        # record (e.g. cold-start D → D from inactivity is uninteresting,
        # but a real demotion should be tracked).
        if trigger and forced_tier != old_tier:
            return forced_tier, trigger, f"forced demotion: {trigger}"

        # Promotion gate (only relevant when proposed > old)
        if old_tier and TIER_TO_RANK[proposed] < TIER_TO_RANK[old_tier]:
            # S-tier lockout: count resolved since last S→x demotion
            lockout_remaining = 0
            if proposed == "S" and old_tier != "S":
                lockout_remaining = max(0, S_TIER_LOCKOUT_TRADES - resolved_since_last_change)
            gated_tier, blocked = apply_promotion_gate(
                proposed_tier=proposed,
                current_tier=old_tier,
                resolved_since_last_change=resolved_since_last_change,
                win_rate_30d=profile.win_rate_30d,
                consistency_score=profile.consistency_score,
                trend_score=profile.trend_score,
                last_change_at=last_change_at,
                now=now,
                s_tier_lockout_remaining=lockout_remaining,
            )
            if blocked:
                return old_tier, None, "cooldown_active"
            if gated_tier != old_tier:
                return gated_tier, "promotion", f"promoted via tier_score={profile.tier_score}"
            return old_tier, None, "promotion_gated"

        # Threshold-based change with no triggers
        if old_tier is None:
            return proposed, None, "initial assignment"
        if proposed != old_tier:
            return proposed, "threshold", f"threshold reassignment to {proposed}"
        return old_tier, None, "unchanged"

    # ── Persistence ─────────────────────────────────────────────────────────

    def _persist(
        self,
        conn: sqlite3.Connection,
        profile: TierProfile,
        final_tier: str,
        old_tier: str | None,
        old_tier_score: float | None,
        now: int,
        existing: dict[str, Any] | None,
    ) -> None:
        tier_changed = (old_tier is not None and old_tier != final_tier) or old_tier is None
        last_change_at = (existing or {}).get("last_change_at") or 0
        resolved_since = (existing or {}).get("resolved_since_last_change") or 0
        if tier_changed:
            last_change_at = now
            resolved_since = 0
        elif existing:
            # Increment by trades-since-last-evaluated heuristic: just bump by 1
            # if the resolved_trades count grew. Compare against existing value.
            prev_resolved = (existing or {}).get("resolved_trades") or 0
            delta = max(0, profile.resolved_trades - prev_resolved)
            resolved_since = (resolved_since or 0) + delta

        conn.execute(
            """INSERT INTO wallet_category_profiles
                (wallet, category, tier, tier_score, win_rate, win_rate_30d,
                 win_rate_90d, resolved_trades, avg_bet_size, monthly_pnl,
                 consistency_score, category_purity, trend_score,
                 last_trade_at, last_evaluated, resolved_since_last_change,
                 last_change_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(wallet, category) DO UPDATE SET
                   tier=excluded.tier,
                   tier_score=excluded.tier_score,
                   win_rate=excluded.win_rate,
                   win_rate_30d=excluded.win_rate_30d,
                   win_rate_90d=excluded.win_rate_90d,
                   resolved_trades=excluded.resolved_trades,
                   avg_bet_size=excluded.avg_bet_size,
                   monthly_pnl=excluded.monthly_pnl,
                   consistency_score=excluded.consistency_score,
                   category_purity=excluded.category_purity,
                   trend_score=excluded.trend_score,
                   last_trade_at=excluded.last_trade_at,
                   last_evaluated=excluded.last_evaluated,
                   resolved_since_last_change=excluded.resolved_since_last_change,
                   last_change_at=excluded.last_change_at""",
            (
                profile.wallet, profile.category, final_tier, profile.tier_score,
                profile.win_rate, profile.win_rate_30d, profile.win_rate_90d,
                profile.resolved_trades, profile.avg_bet_size, profile.monthly_pnl,
                profile.consistency_score, profile.category_purity, profile.trend_score,
                profile.last_trade_at, now, resolved_since, last_change_at,
            ),
        )

    def _log_change(
        self,
        conn: sqlite3.Connection,
        change: TierChange,
        now: int,
    ) -> None:
        conn.execute(
            """INSERT INTO wallet_tier_history
                (wallet, category, old_tier, new_tier, reason, trigger_metric,
                 old_tier_score, new_tier_score, win_rate_at_change,
                 win_rate_30d_at_change, pnl_at_change, changed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                change.wallet, change.category, change.old_tier, change.new_tier,
                change.reason, change.trigger_metric, change.old_tier_score,
                change.new_tier_score, change.win_rate_at_change,
                change.win_rate_30d_at_change, change.pnl_at_change, now,
            ),
        )

    def evaluate_single_wallet(
        self,
        wallet: str,
        category: str | None = None,
        now: int | None = None,
    ) -> dict[str, Any]:
        """Re-evaluate one wallet (in one category, or all its categories).

        Returns ``{categories_evaluated, changes}`` where ``changes`` is
        a list of ``TierChange`` dicts so callers can fire alerts.
        """
        now = now or int(time.time())
        conn = sqlite3.connect(self._db_path)
        try:
            categories: list[str]
            if category:
                categories = [category.lower()]
            else:
                rows = conn.execute(
                    """SELECT DISTINCT LOWER(category) FROM polymarket_paper_trades
                       WHERE LOWER(wallet) = ? AND archived = 0
                         AND category IS NOT NULL AND category != ''""",
                    ((wallet or "").lower(),),
                ).fetchall()
                categories = [r[0] for r in rows]

            changes: list[dict[str, Any]] = []
            for cat in categories:
                profile = self._compute_profile(conn, wallet, cat, now)
                if profile is None:
                    continue
                existing = self.get_profile(wallet, cat)
                old_tier = (existing or {}).get("tier")
                old_score = (existing or {}).get("tier_score")
                final_tier, trigger, reason = self._apply_state_machine(profile, existing, now)
                self._persist(conn, profile, final_tier, old_tier, old_score, now, existing)
                if old_tier != final_tier:
                    change = TierChange(
                        wallet=wallet, category=cat,
                        old_tier=old_tier, new_tier=final_tier,
                        reason=reason, trigger_metric=trigger,
                        old_tier_score=old_score, new_tier_score=profile.tier_score,
                        win_rate_at_change=profile.win_rate,
                        win_rate_30d_at_change=profile.win_rate_30d,
                        pnl_at_change=profile.cumulative_pnl,
                    )
                    self._log_change(conn, change, now)
                    changes.append({
                        **change.__dict__,
                        "changed_at": now,
                    })
            conn.commit()
            return {"categories_evaluated": len(categories), "changes": changes}
        finally:
            conn.close()

    def build_all_profiles(self, now: int | None = None) -> dict[str, Any]:
        """Full nightly rebuild across every (wallet, category) with ≥3 trades."""
        now = now or int(time.time())
        conn = sqlite3.connect(self._db_path)
        try:
            pairs = conn.execute(
                """SELECT LOWER(wallet), LOWER(category), COUNT(*)
                   FROM polymarket_paper_trades
                   WHERE archived = 0 AND exit_ts IS NOT NULL
                     AND wallet IS NOT NULL AND category IS NOT NULL
                   GROUP BY LOWER(wallet), LOWER(category)
                   HAVING COUNT(*) >= 3"""
            ).fetchall()
            stats = {"promoted": 0, "demoted": 0, "unchanged": 0, "new": 0}
            changes: list[dict[str, Any]] = []
            for wallet, cat, _ in pairs:
                profile = self._compute_profile(conn, wallet, cat, now)
                if profile is None:
                    continue
                existing_row = conn.execute(
                    """SELECT tier, tier_score, last_change_at, resolved_since_last_change,
                              resolved_trades
                       FROM wallet_category_profiles
                       WHERE wallet = ? AND category = ?""",
                    (wallet, cat),
                ).fetchone()
                existing = (
                    {
                        "tier": existing_row[0], "tier_score": existing_row[1],
                        "last_change_at": existing_row[2],
                        "resolved_since_last_change": existing_row[3],
                        "resolved_trades": existing_row[4],
                    }
                    if existing_row else None
                )
                old_tier = (existing or {}).get("tier")
                old_score = (existing or {}).get("tier_score")
                final_tier, trigger, reason = self._apply_state_machine(profile, existing, now)
                self._persist(conn, profile, final_tier, old_tier, old_score, now, existing)
                if old_tier is None:
                    stats["new"] += 1
                elif old_tier == final_tier:
                    stats["unchanged"] += 1
                elif TIER_TO_RANK[final_tier] < TIER_TO_RANK[old_tier]:
                    stats["promoted"] += 1
                else:
                    stats["demoted"] += 1
                if old_tier != final_tier:
                    change = TierChange(
                        wallet=wallet, category=cat,
                        old_tier=old_tier, new_tier=final_tier,
                        reason=reason, trigger_metric=trigger,
                        old_tier_score=old_score, new_tier_score=profile.tier_score,
                        win_rate_at_change=profile.win_rate,
                        win_rate_30d_at_change=profile.win_rate_30d,
                        pnl_at_change=profile.cumulative_pnl,
                    )
                    self._log_change(conn, change, now)
                    changes.append({**change.__dict__, "changed_at": now})
            conn.commit()
            stats["changes"] = changes
            stats["evaluated"] = len(pairs)
            return stats
        finally:
            conn.close()
