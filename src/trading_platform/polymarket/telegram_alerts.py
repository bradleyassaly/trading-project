# Setup:
# 1. Message @BotFather on Telegram, create new bot, copy token
# 2. Message @userinfobot to get your chat_id
# 3. Add TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID to .env
# 4. Test: trading-cli data polymarket test-telegram
"""
Telegram alerter for the Polymarket whale monitoring system.

Sends rich, contextual alerts on whale detections, signals, paper trades,
position updates, and trade resolutions. Non-blocking — never fails the pipeline.
"""
from __future__ import annotations

import json
import logging
import os
import time
import urllib.request
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

_FOOTER = "\n\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\n\U0001f5a5 View: localhost:5173"

_SIGNAL_EMOJIS = {
    "wallet_reversal": "\U0001f504", "cascade": "\U0001f4c8",
    "oversized_bet": "\U0001f4a5", "accumulation": "\U0001f4e6",
    "market_maker_flip": "\U0001f3af", "convergence": "\U0001f91d",
    "specialist_entry": "\U0001f3af", "pre_deadline_surge": "\u23f0",
    "whale_entry": "\U0001f40b",
    "late_conviction": "\U0001f525", "tier_entry": "\U0001f451",
    "whale_entry_filtered": "\U0001f40b",
}

_BUCKET_LABELS = {
    "concentrated_whale": "concentrated whale", "contrarian_whale": "contrarian whale",
    "portfolio_diversifier": "diversifier", "position_builder": "builder",
    "domain_specialist": "domain specialist", "volume_trader": "volume trader",
    "directional": "directional", "market_maker": "market maker",
    "arb_bot": "arb bot", "noise": "noise", "unknown": "unknown",
}


def _fmt_usd(v: float) -> str:
    if abs(v) >= 1_000_000:
        return f"${v / 1_000_000:.1f}M"
    if abs(v) >= 1_000:
        return f"${v / 1_000:.1f}K"
    return f"${v:.0f}"


def _fmt_wallet(w: str) -> str:
    """Truncate to 0xAbCd...eF12 format."""
    if not w or len(w) < 10:
        return w or "?"
    return f"{w[:6]}...{w[-4:]}"


def _fmt_et(ts: int | float | None) -> str:
    """Unix timestamp to Eastern Time string."""
    if not ts:
        return ""
    try:
        from datetime import datetime, timezone, timedelta
        et = timezone(timedelta(hours=-4))
        dt = datetime.fromtimestamp(int(ts), tz=et)
        return dt.strftime("%b %d %I:%M%p ET")
    except Exception:
        return ""


def _dir_label(direction: str) -> str:
    """Direction label with emoji — safe for Python 3.11 f-strings."""
    if direction == "BUY":
        return "BUY \U0001f7e2"
    return "SELL \U0001f534"


def _yn_side(direction: str | None, side: str | None = None) -> str:
    """Return 'YES' or 'NO' token label with color emoji.

    In Polymarket convention a BUY on a condition buys the YES token and a
    SELL buys the NO token. Some signals pass ``BUY_YES`` / ``BUY_NO`` or
    already-normalized ``side`` fields ('YES'/'NO'); handle all shapes.
    """
    if side and isinstance(side, str):
        s = side.upper().strip()
        if s in ("YES", "NO"):
            return f"YES \U0001f7e2" if s == "YES" else f"NO \U0001f534"
    d = (direction or "").upper().strip()
    if d in ("BUY", "BUY_YES", "YES"):
        return "YES \U0001f7e2"
    return "NO \U0001f534"


def _price_implied(price: float | None) -> str:
    """Format price + implied probability, e.g. '$0.350 (35% implied)'."""
    if price is None or price <= 0:
        return "price unknown"
    try:
        p = float(price)
    except (TypeError, ValueError):
        return "price unknown"
    return f"${p:.3f} ({p*100:.0f}% implied)"


def _payout_line(stake_usd: float | None, entry_price: float | None) -> str:
    """Potential payout if the position resolves in your favor.

    For a YES bet of $S at entry $p, payout if YES wins = $S / $p (shares
    bought) * $1.00 (resolution payout per share). Profit = payout - stake.
    Risk is the full stake (contract price at entry is just mispriced
    probability, no leverage). Returns empty string on missing inputs.
    """
    try:
        s = float(stake_usd or 0)
        p = float(entry_price or 0)
    except (TypeError, ValueError):
        return ""
    if s <= 0 or p <= 0 or p >= 1:
        return ""
    payout = s / p
    profit = payout - s
    roi_pct = profit / s * 100
    return f"Payout if correct: {_fmt_usd(payout)} (profit {_fmt_usd(profit)}, +{roi_pct:.0f}%)"


def _market_link(slug: str | None, cid: str | None) -> str:
    if slug:
        return f"https://polymarket.com/event/{slug}"
    if cid:
        return f"https://polymarket.com/event/{cid[:16]}"
    return ""


def _resolution_badge(condition_id: str | None) -> str:
    """Return a short duration badge if the market resolves soon, else empty.

    Uses MarketUniverse's cached endDate lookup; gracefully degrades to ""
    if universe isn't loaded or the market isn't tracked. Covers the
    common case where operators want to spot short-duration trades at a
    glance without changing gate behavior.
    """
    if not condition_id:
        return ""
    try:
        from trading_platform.polymarket.market_universe import MarketUniverse
        from datetime import datetime, timezone
        mu = MarketUniverse()
        if not mu._by_category:
            mu.load_cached(max_age_hours=24.0)
        entry = mu._by_condition.get(condition_id) if hasattr(mu, "_by_condition") else None
        if not entry:
            return ""
        raw = entry.get("end_date_iso") or ""
        if not raw:
            return ""
        clean = raw.replace("Z", "+00:00") if raw.endswith("Z") else raw
        dt = datetime.fromisoformat(clean)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        days = (dt - datetime.now(tz=timezone.utc)).total_seconds() / 86400.0
        if days < 0:
            return "  \u23f1 past-due"
        if days < 1:
            return f"  \u23f1 {days*24:.0f}h"
        if days < 14:
            return f"  \u23f1 {days:.0f}d"
        return ""  # Long-dated: no badge
    except Exception:
        return ""


class TelegramAlerter:
    """Non-blocking Telegram alert sender with rich context.

    Supports a per-message ``disable_notification`` flag (silent vs loud)
    and a global 20/hour rate limit on non-critical messages so background
    pollers can't accidentally spam the chat. Messages with
    ``disable_notification=False`` (i.e. critical / time-sensitive alerts)
    bypass the rate limit.
    """

    MAX_NONCRITICAL_PER_HOUR = 20

    # Only alert on wallet-based signals — NOT technical scanners.
    # velocity_detector and order_book_monitor are noise.
    SIGNAL_WHITELIST = {
        "whale_entry", "whale_exit",
        "convergence", "cascade",
        "specialist_entry", "accumulation",
        "oversized_bet", "wallet_reversal",
        "market_maker_flip", "no_position_entry",
        "position_reduction", "pre_deadline_surge",
        "late_conviction", "tier_entry",
        "whale_entry_filtered", "insider_entry",
        "copyable_contrarian", "strategy_specialist",
        "consensus_follower", "network_leader_entry",
        "news_reactor",
        "order_flow_imbalance",
        "high_conviction_insider",
        "price_momentum",
        "resolution_proximity",
    }

    # Minimum trade size (USD) to alert on whale detection
    MIN_WHALE_SIZE = 50

    # Per-market cooldown to prevent spam when multiple whales pile in
    MARKET_COOLDOWN_SECONDS = 900  # 15 minutes

    def __init__(self) -> None:
        self.bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
        self.enabled = bool(self.bot_token and self.chat_id)
        self._last_pipeline_alert: dict[str, float] = {}
        self._last_daily_update: float = 0
        self._last_hot_market_alert: float = 0
        # Sliding 1h window of non-critical send timestamps
        self._noncrit_send_log: list[float] = []
        # Per-market cooldown to prevent alert spam
        self._market_alert_times: dict[str, float] = {}
        # 2026-04-25: per-message-fingerprint dedup. Same payload within
        # 1h is dropped (silent + non-critical only). Critical alerts
        # always go through. Closes the "I got 50 identical farmer-block
        # alerts in 5 minutes" failure mode.
        self._fingerprint_log: dict[str, float] = {}
        # Quiet hours (local UTC). 23:00-07:00 by default. Critical
        # alerts always bypass; non-critical messages are dropped.
        self._quiet_hours_start_utc = int(os.getenv("TELEGRAM_QUIET_START_UTC", "3") or 3)  # 11pm ET
        self._quiet_hours_end_utc   = int(os.getenv("TELEGRAM_QUIET_END_UTC",   "11") or 11)  # 7am ET

    def _check_market_cooldown(self, condition_id: str | None) -> bool:
        """True if we should send, False if this market is in cooldown."""
        if not condition_id:
            return True
        now = time.time()
        last = self._market_alert_times.get(condition_id, 0)
        if now - last < self.MARKET_COOLDOWN_SECONDS:
            return False
        self._market_alert_times[condition_id] = now
        return True

    def _within_noncrit_budget(self) -> bool:
        """True if a non-critical send is allowed under the 20/hour cap."""
        now = time.time()
        cutoff = now - 3600
        self._noncrit_send_log = [t for t in self._noncrit_send_log if t >= cutoff]
        return len(self._noncrit_send_log) < self.MAX_NONCRITICAL_PER_HOUR

    def _is_quiet_hours(self) -> bool:
        """True if current UTC hour falls in the configured quiet window."""
        from datetime import datetime, timezone
        h = datetime.now(timezone.utc).hour
        s, e = self._quiet_hours_start_utc, self._quiet_hours_end_utc
        if s == e:
            return False
        if s < e:
            return s <= h < e
        # Wraps midnight (e.g. 23 → 7)
        return h >= s or h < e

    def _is_dup_within_1h(self, message: str) -> bool:
        """Fingerprint-based dedup. Same first-150-chars within 1h = drop."""
        import hashlib
        now = time.time()
        # Garbage-collect old entries
        self._fingerprint_log = {
            k: v for k, v in self._fingerprint_log.items() if now - v < 3600
        }
        fp = hashlib.md5(message[:150].encode("utf-8")).hexdigest()
        if fp in self._fingerprint_log:
            return True
        self._fingerprint_log[fp] = now
        return False

    def _send(
        self,
        message: str,
        disable_notification: bool = False,
        reply_markup: dict | None = None,
    ) -> bool:
        """Send a message to Telegram.

        Parameters
        ----------
        message:
            HTML-formatted message text.
        disable_notification:
            When True the message arrives silently and is subject to the
            non-critical rate limit, fingerprint dedup, and quiet hours.
            When False (default behaviour for backwards compatibility)
            the message bypasses all of those and triggers a
            sound/vibration notification.
        """
        if not self.enabled:
            return False
        if disable_notification:
            # Quiet hours drop non-critical messages entirely
            if self._is_quiet_hours():
                logger.debug("Telegram quiet hours; dropping non-critical message")
                return False
            # Fingerprint dedup
            if self._is_dup_within_1h(message):
                logger.debug("Telegram dup within 1h; dropping non-critical message")
                return False
            if not self._within_noncrit_budget():
                logger.debug("Telegram non-critical rate limit hit; dropping message")
                return False
            self._noncrit_send_log.append(time.time())
        try:
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            body: dict[str, Any] = {
                "chat_id": self.chat_id,
                "text": message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
                "disable_notification": bool(disable_notification),
            }
            if reply_markup is not None:
                body["reply_markup"] = reply_markup
            payload = json.dumps(body).encode()
            req = urllib.request.Request(
                url, data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.status == 200
        except Exception as exc:
            logger.debug("Telegram send failed: %s", exc)
            return False

    # ── Whale detection ──────────────────────────────────────────────────────

    def send_whale_detection(self, trade: Any, profile: dict | None = None) -> bool:
        """Rich alert on tier-1 (or tier1h) whale detection."""
        tier = getattr(trade, "wallet_tier", "tier2")
        if tier not in ("tier1", "tier1h"):
            return False

        # Filter: minimum trade size
        size = getattr(trade, "size", 0) or 0
        if size < self.MIN_WHALE_SIZE:
            return False

        # Filter: per-market cooldown
        cid = getattr(trade, "condition_id", None)
        if not self._check_market_cooldown(cid):
            return False

        side = getattr(trade, "side", "BUY")
        side_e = "\U0001f7e2" if side == "BUY" else "\U0001f534"
        question = (getattr(trade, "question", "") or "")[:80]
        size = getattr(trade, "size", 0) or 0
        price = getattr(trade, "price", 0) or 0
        wallet = (getattr(trade, "wallet", "") or "")[:12]
        wr = getattr(trade, "directional_win_rate", 0) or 0
        conv = getattr(trade, "conviction_score", 0) or 0
        cat = (getattr(trade, "category", "") or "").upper()

        # Enrich from profile if available
        rolling_wr = ""
        pnl_line = ""
        bucket_line = ""
        if profile:
            rwr = profile.get("rolling_20_wr")
            if rwr is not None:
                rolling_wr = f" | Rolling: {rwr:.0%}"
            npnl = profile.get("net_pnl_usdc") or 0
            resolved = profile.get("resolved_trades") or 0
            if resolved:
                pnl_line = f"\n\U0001f4c8 {_fmt_usd(npnl)} lifetime PnL | {resolved} resolved trades"
            bucket = profile.get("wallet_type") or profile.get("wallet_bucket") or ""
            if bucket:
                bucket_line = f"\n\U0001f3f7\ufe0f Bucket: {_BUCKET_LABELS.get(bucket, bucket)}"

        # tier1h gets sperm whale, tier1 gets regular whale
        whale_emoji = "\U0001f433" if tier == "tier1h" else "\U0001f40b"
        tier_label = "TIER-1H WHALE (HIGH-CONVICTION)" if tier == "tier1h" else "TIER-1 WHALE DETECTED"

        msg = (
            f"{whale_emoji} <b>{tier_label}</b>\n"
            f"{side_e} <b>{side} \u2014 {cat}</b>\n\n"
            f"\U0001f4ca <b>Market:</b> {question}\n"
            f"\U0001f4b0 {_fmt_usd(size)} @ {price:.3f}\n\n"
            f"\U0001f464 <b>Wallet:</b> <code>{wallet}...</code>\n"
            f"\u2b50 WR: {wr:.0%}{rolling_wr} | Conv: {conv:.3f}"
            f"{bucket_line}{pnl_line}\n"
            f"\n\U0001f50d Checking signals..."
            f"{_FOOTER}"
        )
        return self._send(msg)

    # ── Signal fired ─────────────────────────────────────────────────────────

    def send_signal(self, signal: dict, paper_result: dict | None = None) -> bool:
        """Rich alert when a signal fires, with paper trade details if placed."""
        sig_type = signal.get("signal_type", "")

        # Only wallet-based signals — silently drop velocity/order_book noise
        if sig_type not in self.SIGNAL_WHITELIST:
            return False

        # Per-market cooldown
        cid = signal.get("condition_id")
        if not self._check_market_cooldown(cid):
            return False
        emoji = _SIGNAL_EMOJIS.get(sig_type, "\U0001f4e1")
        side_e = "\U0001f7e2" if signal.get("direction") == "BUY" else "\U0001f534"
        question = (signal.get("question") or signal.get("condition_id", ""))[:80]
        conf = signal.get("confidence", 0) or 0
        size = signal.get("size", 0) or 0
        wallet = (signal.get("wallet") or "")[:12]
        cat = (signal.get("category") or "").upper()
        wr = signal.get("directional_win_rate", 0) or 0

        # Dispatch to signal-type-specific formatters for the three
        # validated signals. Everything else gets the generic template.
        if sig_type == "specialist_entry":
            return self._send_specialist_entry(signal, conf, paper_result)
        if sig_type == "late_conviction":
            return self._send_late_conviction(signal, conf, paper_result)
        if sig_type == "tier_entry":
            return self._send_tier_entry(signal, conf, paper_result)

        # ── Generic template (accumulation, cascade, etc.) ──
        detail = ""
        if sig_type == "cascade":
            n = signal.get("cascade_wallets") or signal.get("converging_wallets", 0)
            detail = f"\u251c Type: cascade ({n} tier-1 wallets confirmed)\n"
        elif sig_type == "convergence":
            n = signal.get("converging_wallets", 0)
            detail = f"\u251c Type: convergence ({n} wallets same side)\n"
        elif sig_type == "accumulation":
            n = signal.get("accumulation_count", 0)
            detail = f"\u251c Type: accumulation ({n} entries)\n"
        else:
            detail = f"\u251c Type: {sig_type.replace('_', ' ')}\n"

        price = signal.get("price") or signal.get("entry_price") or 0
        yn = _yn_side(signal.get("direction"), signal.get("side"))
        slug = signal.get("slug")
        link = _market_link(slug, cid)
        link_line = (
            f"<a href=\"{link}\">View market</a>\n" if link else ""
        )

        badge = _resolution_badge(cid)
        msg = (
            f"{emoji} <b>SIGNAL: {sig_type.upper().replace('_', ' ')}</b>{badge}\n"
            f"<b>{yn} \u2014 {cat}</b>\n\n"
            f"\U0001f4ca <b>Market:</b> {question}\n"
            f"{link_line}\n"
            f"Entry: {_price_implied(price)}  |  Size: {_fmt_usd(size)}\n"
            f"Signal details:\n"
            f"{detail}"
            f"\u251c Confidence: {conf:.0%}\n"
            f"\u2514 Wallet: <code>{_fmt_wallet(signal.get('wallet',''))}</code> | WR: {wr:.0%}\n"
        )
        msg += self._paper_trade_block(signal, paper_result)
        msg += _FOOTER
        # 2026-04-25: inline-keyboard buttons. "View market" opens the
        # PM page, "Wallet" opens our profile page, "Block farmer"
        # records a manual override (advisory; the auto-detector
        # decides ground truth). Buttons add zero-cost ops affordances
        # without leaving Telegram. Skipped on paper-trade-only signals.
        slug = signal.get("slug") or ""
        wallet = signal.get("wallet") or ""
        kb_rows: list[list[dict]] = []
        if cid and slug:
            kb_rows.append([{
                "text": "🔗 Market",
                "url": f"https://polymarket.com/event/{slug}",
            }])
        if wallet and len(wallet) == 42:
            kb_rows.append([
                {"text": "👤 Wallet",
                 "url": f"http://localhost:5173/wallets/{wallet}"},
                {"text": "⚠ Block (advisory)",
                 "callback_data": f"block_wallet:{wallet[:42]}"},
            ])
        reply_markup = {"inline_keyboard": kb_rows} if kb_rows else None
        return self._send(msg, reply_markup=reply_markup)

    # ── Signal-type-specific templates ──────────────────────────────────

    def _send_specialist_entry(self, signal: dict, conf: float, paper: dict | None) -> bool:
        """Specialist wallet trading in their domain category."""
        w = _fmt_wallet(signal.get("wallet", ""))
        tier = signal.get("wallet_tier", "")
        cat = (signal.get("category") or "").upper()
        q = (signal.get("question") or "")[:80]
        direction = signal.get("direction", "BUY")
        price = signal.get("price") or signal.get("entry_price") or 0
        size = signal.get("size", 0) or 0
        cat_wr = signal.get("directional_win_rate") or 0
        slug = signal.get("slug")
        cid = signal.get("condition_id")

        loud = tier in ("tier1h",) or conf > 0.70
        link = _market_link(slug, cid)

        yn = _yn_side(direction, signal.get("side"))
        payout = _payout_line(size, price)
        badge = _resolution_badge(cid)
        msg = (
            f"\U0001f3af <b>SPECIALIST ENTRY</b>{badge}\n"
            f"Wallet <code>{w}</code> ({tier})\n"
            f"Domain: <b>{cat}</b> specialist\n\n"
            f"\U0001f4ca {q}\n"
            f"{('<a href=' + chr(34) + link + chr(34) + '>View market</a>') if link else ''}\n"
            f"\n<b>{yn}</b> @ {_price_implied(price)}  |  Size: {_fmt_usd(size)}\n"
            f"{payout + chr(10) if payout else ''}"
            f"Cat WR: {cat_wr:.0%}  |  Confidence: {conf:.0%}"
        )
        msg += self._paper_trade_block(signal, paper)
        msg += _FOOTER
        return self._send(msg, disable_notification=not loud)

    def _send_late_conviction(self, signal: dict, conf: float, paper: dict | None) -> bool:
        """Late-life high-conviction trade from a top-tier wallet."""
        w = _fmt_wallet(signal.get("wallet", ""))
        tier = signal.get("wallet_tier", "")
        q = (signal.get("question") or "")[:80]
        direction = signal.get("direction", "BUY")
        price = signal.get("price") or signal.get("entry_price") or 0
        size = signal.get("size", 0) or 0
        life_pct = signal.get("market_life_pct") or 0
        hours_left = signal.get("hours_remaining")
        size_vs_avg = signal.get("size_vs_avg")
        cat = (signal.get("category") or "").upper()
        slug = signal.get("slug")
        cid = signal.get("condition_id")

        loud = conf > 0.70
        link = _market_link(slug, cid)

        time_line = f"Market is <b>{life_pct*100:.0f}%</b> through its lifetime"
        if hours_left is not None:
            if hours_left < 24:
                time_line += f" ({hours_left:.0f}h remaining)"
            else:
                time_line += f" ({hours_left/24:.1f}d remaining)"

        size_line = _fmt_usd(size)
        if size_vs_avg is not None and size_vs_avg > 0:
            size_line += f" ({size_vs_avg:.1f}x avg)"

        yn = _yn_side(direction, signal.get("side"))
        payout = _payout_line(size, price)
        badge = _resolution_badge(cid)
        msg = (
            f"\U0001f525 <b>LATE CONVICTION</b>{badge}\n"
            f"Wallet <code>{w}</code> ({tier})\n"
            f"{time_line}\n\n"
            f"\U0001f4ca {q}\n"
            f"{('<a href=' + chr(34) + link + chr(34) + '>View market</a>') if link else ''}\n"
            f"\n<b>{yn}</b> @ {_price_implied(price)}  |  Size: {size_line}  |  {cat}\n"
            f"{payout + chr(10) if payout else ''}"
            f"Confidence: {conf:.0%}"
        )
        msg += self._paper_trade_block(signal, paper)
        msg += _FOOTER
        return self._send(msg, disable_notification=not loud)

    def _send_tier_entry(self, signal: dict, conf: float, paper: dict | None) -> bool:
        """S/A tier wallet entering an uncertain-zone market."""
        w = _fmt_wallet(signal.get("wallet", ""))
        cat_tier = signal.get("wallet_cat_tier") or "A"
        cat_wr = signal.get("wallet_cat_wr") or 0
        cat = (signal.get("category") or "").upper()
        q = (signal.get("question") or "")[:80]
        direction = signal.get("direction", "BUY")
        price = signal.get("price") or signal.get("entry_price") or 0
        size = signal.get("size", 0) or 0
        slug = signal.get("slug")
        cid = signal.get("condition_id")

        loud = cat_tier == "S"
        link = _market_link(slug, cid)

        yn = _yn_side(direction, signal.get("side"))
        payout = _payout_line(size, price)
        badge = _resolution_badge(cid)
        msg = (
            f"\U0001f451 <b>TIER ENTRY ({cat_tier}-TIER)</b>{badge}\n"
            f"Wallet <code>{w}</code>\n"
            f"{cat} WR: <b>{cat_wr:.0%}</b>\n\n"
            f"\U0001f4ca {q}\n"
            f"{('<a href=' + chr(34) + link + chr(34) + '>View market</a>') if link else ''}\n"
            f"\nUncertain zone: <b>{yn}</b> @ {_price_implied(price)}  |  Size: {_fmt_usd(size)}\n"
            f"{payout + chr(10) if payout else ''}"
            f"Confidence: {conf:.0%}"
        )
        msg += self._paper_trade_block(signal, paper)
        msg += _FOOTER
        return self._send(msg, disable_notification=not loud)

    @staticmethod
    def _paper_trade_block(signal: dict, paper: dict | None) -> str:
        if not paper or not paper.get("placed"):
            return ""
        stake = paper.get("stake", 0)
        price = signal.get("price") or signal.get("entry_price") or 0
        yn = _yn_side(signal.get("direction"), signal.get("side"))
        payout = _payout_line(stake, price)
        return (
            f"\n\n\U0001f4dd <b>PAPER TRADE PLACED</b>\n"
            f"{yn} {_fmt_usd(stake)} @ {_price_implied(price)}\n"
            f"{payout}" if payout else
            f"\n\n\U0001f4dd <b>PAPER TRADE PLACED</b>\n"
            f"{yn} {_fmt_usd(stake)} @ {_price_implied(price)}"
        )

    # ── Insider entry alert ──────────────────────────────────────────────

    def send_insider_entry(self, signal: dict) -> bool:
        """Alert when an insider-flagged wallet enters a market."""
        cid = signal.get("condition_id")
        if not self._check_market_cooldown(cid):
            return False

        wallet = (signal.get("wallet") or "")[:12]
        question = (signal.get("question") or "")[:90]
        direction = signal.get("direction") or ""
        price = signal.get("price") or signal.get("entry_price") or 0
        conf = signal.get("confidence") or 0
        acc = signal.get("insider_accuracy") or signal.get("wallet_accuracy") or 0
        n = signal.get("insider_sample") or signal.get("wallet_sample_size") or 0
        score = signal.get("insider_score") or signal.get("wallet_insider_score") or 0
        cat = (signal.get("category") or "").upper()

        loud = acc >= 0.80

        msg = (
            f"\U0001f50d <b>INSIDER ENTRY DETECTED</b>\n"
            f"Wallet: <code>{wallet}...</code> ({acc:.0%} accuracy, {n} trades)\n\n"
            f"\U0001f4ca <b>Market:</b> {question}\n"
            f"Category: <b>{cat}</b>\n"
            f"Action: <b>{direction}</b> @ ${price:.3f}\n"
            f"Confidence: {conf:.0%} | Score: {score:.3f}"
            f"{_FOOTER}"
        )
        return self._send(msg, disable_notification=not loud)

    # ── Political whale alert ───────────────────────────────────────────────

    def send_political_whale(
        self,
        signal: dict,
        tier: str,
        win_rate: float | None = None,
        pnl_30d: float | None = None,
    ) -> bool:
        """Loud alert: S/A tier political / geopolitical wallet just moved.

        B tier uses silent notification; C and below don't alert here.
        """
        tier_u = (tier or "").upper()
        if tier_u not in ("S", "A", "B"):
            return False

        sig_type = signal.get("signal_type", "")
        cid = signal.get("condition_id")
        if not self._check_market_cooldown(cid):
            return False

        loud = tier_u in ("S", "A")
        whale_emoji = "\U0001f40b"  # 🐋
        category = (signal.get("category") or "").upper()
        question = (signal.get("question") or "")[:90]
        wallet = (signal.get("wallet") or "")[:12]
        direction = signal.get("direction") or ""
        price = signal.get("price") or 0
        size = signal.get("size") or 0
        conf = signal.get("confidence") or 0

        wr_str = f"WR: {win_rate:.0%}" if win_rate is not None else "WR: n/a"
        pnl_str = f"30d: {_fmt_usd(pnl_30d)}" if pnl_30d is not None else ""
        stats = " | ".join(s for s in [f"{tier_u}-tier {category}", wr_str, pnl_str] if s)

        msg = (
            f"{whale_emoji} <b>POLITICAL WHALE ALERT</b>\n"
            f"<b>{stats}</b>\n\n"
            f"Wallet: <code>{wallet}...</code>\n"
            f"\U0001f4ca <b>Market:</b> {question}\n"
            f"Action: <b>{direction}</b> @ ${price:.3f}  \u2014  size {_fmt_usd(size)}\n"
            f"Signal: <code>{sig_type}</code>  |  Confidence: {conf:.0%}"
            f"{_FOOTER}"
        )
        return self._send(msg, disable_notification=not loud)

    # ── Paper trade ──────────────────────────────────────────────────────────

    def send_paper_trade(self, trade: dict) -> bool:
        """Alert when a paper trade is placed (standalone, without signal context)."""
        question = (trade.get("question") or trade.get("condition_id", ""))[:70]
        stake = trade.get("stake", 0) or 0
        conf = trade.get("confidence", 0) or 0
        sig = trade.get("signal_type", "")
        price = trade.get("price") or trade.get("entry_price") or 0
        yn = _yn_side(trade.get("direction"), trade.get("side"))
        cat = (trade.get("category") or "").upper()
        payout = _payout_line(stake, price)

        msg = (
            f"\U0001f4dd <b>PAPER TRADE: {sig}</b>\n"
            f"<b>{yn}</b>  {('[' + cat + ']') if cat else ''}\n\n"
            f"\U0001f4ca {question}\n"
            f"\U0001f4b0 Stake: {_fmt_usd(stake)} @ {_price_implied(price)}\n"
            f"{payout + chr(10) if payout else ''}"
            f"\U0001f3af Confidence: {conf:.0%}"
            f"{_FOOTER}"
        )
        return self._send(msg)

    # ── Trade resolution ─────────────────────────────────────────────────────

    def send_trade_resolved(self, trade: dict, signal_stats: dict | None = None) -> bool:
        """Alert when a paper trade resolves (market settles)."""
        won = trade.get("outcome") == "win"
        emoji = "\u2705" if won else "\u274c"
        sig = trade.get("signal_type") or trade.get("signal_family", "")
        question = (trade.get("question") or trade.get("ticker", ""))[:70]
        yn = _yn_side(trade.get("direction"), trade.get("side"))
        entry = trade.get("entry_price", 0) or 0
        exit_price = trade.get("exit_price")
        size = trade.get("size_usd") or trade.get("stake") or 0
        ret = trade.get("return_pct") or 0
        pnl = trade.get("realized_pnl")
        if pnl is None:
            pnl = size * ret if size else 0
        # Hold duration
        entry_ts = trade.get("entry_ts")
        exit_ts = trade.get("exit_ts")
        hold_line = ""
        if entry_ts and exit_ts:
            try:
                days = (float(exit_ts) - float(entry_ts)) / 86400.0
                hold_line = f"  (held {days:.1f}d)"
            except (TypeError, ValueError):
                pass
        # Resolution outcome label
        resolved_yn = ""
        if exit_price is not None:
            try:
                ep = float(exit_price)
                if ep >= 0.99:
                    resolved_yn = "Market resolved: YES \U0001f7e2"
                elif ep <= 0.01:
                    resolved_yn = "Market resolved: NO \U0001f534"
                else:
                    resolved_yn = f"Resolved @ ${ep:.3f}"
            except (TypeError, ValueError):
                pass

        lines = [
            f"{emoji} <b>TRADE {'WON' if won else 'LOST'}: {sig}</b>",
            question,
            f"Bet: <b>{yn}</b>  {_fmt_usd(size)} @ {_price_implied(entry)}{hold_line}",
        ]
        if resolved_yn:
            lines.append(resolved_yn)
        pnl_sign = "+" if pnl >= 0 else ""
        lines.append(f"P&L: {pnl_sign}{_fmt_usd(pnl)} ({ret*100:+.1f}%)")
        msg = "\n".join(lines) + "\n"

        if signal_stats:
            total = signal_stats.get("total_resolved", 0)
            wins = signal_stats.get("wins", 0)
            total_pnl = signal_stats.get("total_pnl", 0)
            if total > 0:
                msg += (
                    f"\n{sig} stats:\n"
                    f"\u251c All time: {wins} won / {total} resolved = {wins/total:.0%} WR\n"
                    f"\u2514 Total P&L from {sig}: {_fmt_usd(total_pnl)}"
                )

        msg += _FOOTER
        return self._send(msg)

    # ── Daily position update ────────────────────────────────────────────────

    def send_daily_update(self, positions: list[dict], stats: dict | None = None) -> bool:
        """Daily summary of open positions and P&L. Rate limited to every 3h.

        ``positions`` should come from ``/api/paper/positions-enriched``
        (each entry should have ``size_usd``, ``entry_price``,
        ``current_price``, ``unrealized_pnl``, ``unrealized_pct``,
        ``side``, ``question``, ``signal_type``).

        ``stats`` is an optional dict with ``signals_today``,
        ``whales_today``, ``markets_discovered``, ``pipeline_success``,
        ``pipeline_duration``.
        """
        now = time.time()
        if now - self._last_daily_update < 10800:  # 3 hours
            return False
        self._last_daily_update = now
        stats = stats or {}

        # Paper bankroll — match paper_executor.STARTING_BANKROLL so the
        # digest's "Cash: $X (Y%)" number reflects the actual paper budget.
        try:
            from trading_platform.polymarket.polymarket_paper_executor import (
                STARTING_BANKROLL,
            )
            bankroll = float(STARTING_BANKROLL)
        except Exception:
            bankroll = 10_000.0
        n_pos = len(positions)
        deployed = sum((p.get("size_usd") or 0) for p in positions)
        unrealized_total = sum((p.get("unrealized_pnl") or 0) for p in positions)
        # Sanity: never report a position larger than the bankroll
        deployed = min(deployed, bankroll)
        total_value = bankroll - deployed + unrealized_total

        signals_today = stats.get("signals_today", stats.get("signals_fired_this_cycle", 0))
        whales_today = stats.get("whales_today", stats.get("whales_detected_today", 0))
        markets_discovered = stats.get("markets_discovered", 0)
        pipeline_ok = stats.get("pipeline_success", True)
        pipeline_dur = stats.get("pipeline_duration", 0)
        _pipe_icon = "\u2705 OK" if pipeline_ok else "\u274c FAILED"
        pipeline_line = f"Pipeline: {_pipe_icon} ({pipeline_dur:.0f}s)"

        if not positions:
            msg = (
                f"\U0001f4ca <b>DAILY UPDATE</b> \u2014 No open positions\n"
                f"Bankroll: {_fmt_usd(bankroll)} (100%)\n"
                f"Today: {signals_today} signals | {whales_today} whales | {markets_discovered} markets found\n"
                f"{pipeline_line}"
                f"{_FOOTER}"
            )
            return self._send(msg, disable_notification=True)

        lines = [
            f"\U0001f4ca <b>DAILY PORTFOLIO UPDATE</b>",
            f"Cash: {_fmt_usd(bankroll - deployed)} ({(bankroll-deployed)/bankroll:.0%}) | {n_pos} open\n",
        ]

        for p in positions[:6]:
            unr = p.get("unrealized_pnl")
            cur = p.get("current_price")
            entry = float(p.get("entry_price") or 0)
            q = (p.get("question") or p.get("condition_id") or "")[:42]
            size = float(p.get("size_usd") or 0)
            sig = p.get("signal_type") or ""
            yn = _yn_side(p.get("direction"), p.get("side"))

            if unr is not None and cur is not None:
                icon = "\U0001f7e2" if unr >= 0 else "\U0001f534"
                pct = p.get("unrealized_pct") or 0
                # Implied probability move since entry
                implied_delta_pp = (cur - entry) * 100
                lines.append(
                    f"{icon} {yn}  {_fmt_usd(size)} @ {entry:.3f} ({entry*100:.0f}%)  ({sig})\n"
                    f"   \"{q}\"\n"
                    f"   Now: {cur:.3f} ({cur*100:.0f}%, {implied_delta_pp:+.0f}pp)  "
                    f"\u2192  {unr:+.1f} ({pct:+.1f}%)"
                )
            else:
                lines.append(
                    f"\u26aa {yn}  {_fmt_usd(size)} @ {entry:.3f} ({entry*100:.0f}%)  ({sig})\n"
                    f"   \"{q}\"\n"
                    f"   Price: fetching..."
                )

        if n_pos > 6:
            lines.append(f"  ...and {n_pos - 6} more")

        lines.extend([
            f"\nNet unrealized: {unrealized_total:+.1f}",
            f"Total value: {_fmt_usd(total_value)} ({total_value/bankroll:.0%})",
            f"\nToday: {signals_today} signals | {whales_today} whales | {markets_discovered} markets found",
            pipeline_line,
        ])
        msg = "\n".join(lines) + _FOOTER
        return self._send(msg, disable_notification=True)

    # ── Paper trade confirmation (LOW loudness) ─────────────────────────────

    def send_paper_trade_confirmation(self, signal: dict, trade: dict) -> bool:
        """Brief confirmation when the paper executor places a trade."""
        sig_type = signal.get("signal_type", "")
        q = (signal.get("question") or "")[:60]
        price = trade.get("entry_price") or signal.get("price") or 0
        size = trade.get("size_usd") or 0
        yn = _yn_side(signal.get("direction"), signal.get("side") or trade.get("side"))
        payout = _payout_line(size, price)

        msg = (
            f"\U0001f4dd <b>Paper trade placed</b>\n"
            f"<b>{yn}</b> \"{q}\" @ {_price_implied(price)}\n"
            f"Signal: {sig_type} | Size: {_fmt_usd(size)}"
        )
        if payout:
            msg += f"\n{payout}"
        return self._send(msg, disable_notification=True)

    # ── Signal activity digest (for daily report) ─────────────────────────

    def send_signal_digest(self, db_path: str | None = None) -> bool:
        """Daily activity digest — signals, paper trades, PnL, readiness.

        Restructured to use Postgres (canonical DB post-cutover) and to
        include paper-trade performance + live-readiness gate progress.
        """
        try:
            from trading_platform.polymarket.db_connection import get_connection
            conn = get_connection()
            cutoff = "EXTRACT(EPOCH FROM NOW() - INTERVAL '24 hours')::BIGINT"

            fired = conn.execute(f"""
                SELECT signal_type, COUNT(*) n
                FROM signal_outcomes WHERE fired_at > {cutoff}
                GROUP BY signal_type ORDER BY n DESC
            """).fetchall()

            placed = conn.execute(f"""
                SELECT signal_type, COUNT(*) n, ROUND(SUM(size_usd)::numeric, 0) vol
                FROM polymarket_paper_trades
                WHERE archived=0 AND entry_ts > {cutoff}
                GROUP BY signal_type ORDER BY n DESC
            """).fetchall()

            exited = conn.execute(f"""
                SELECT exit_reason, COUNT(*) n,
                       SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END) wins,
                       ROUND(SUM(realized_pnl)::numeric, 2) pnl
                FROM polymarket_paper_trades
                WHERE archived=0 AND exit_ts > {cutoff} AND exit_ts IS NOT NULL
                GROUP BY exit_reason ORDER BY n DESC
            """).fetchall()

            equity_row = conn.execute("""
                SELECT total_equity, realized_pnl_cumulative, open_count
                FROM paper_equity_snapshots ORDER BY ts DESC LIMIT 1
            """).fetchone()

            readiness = conn.execute("""
                SELECT signal_type, sample_size, status
                FROM signal_calibration WHERE status != 'disabled'
                ORDER BY sample_size DESC
            """).fetchall()

            conn.close()
        except Exception as exc:
            logger.debug("signal digest DB error: %s", exc)
            return False

        total_fired = sum(r[1] for r in fired)
        total_placed = sum(r[1] for r in placed)
        total_exited = sum(r[1] for r in exited)
        total_pnl = sum(float(r[3] or 0) for r in exited)
        equity = float(equity_row[0]) if equity_row else 0
        cum_pnl = float(equity_row[1]) if equity_row else 0
        open_count = int(equity_row[2]) if equity_row else 0
        conv = total_placed / total_fired * 100 if total_fired else 0

        # 2026-04-25: pull ladder + calibration + binding-gate context
        # so the morning digest answers "where are we vs the L5 goal"
        # not just "what fired in the last 24h".
        ladder_block = ""
        try:
            import urllib.request as _ur, json as _j
            with _ur.urlopen("http://localhost:8001/api/ladder/status", timeout=5) as r:
                _l = _j.loads(r.read())
            level = _l.get("level", "?")
            level_name = _l.get("level_name", "")
            bk = _l.get("bankroll_usd", 0)
            target = _l.get("target_bankroll_l5", 200000)
            pct = _l.get("progress_pct", 0)
            gates = _l.get("gates_to_next") or []
            gates_pass = sum(1 for g in gates if g.get("passed"))
            ladder_block = (
                f"\n<b>Ladder:</b> {level} {level_name} "
                f"(${bk:,.0f} / ${target:,.0f}, {pct:.1f}%)\n"
                f"  Gates to next: {gates_pass}/{len(gates)} passed"
            )
        except Exception:
            pass

        calib_block = ""
        try:
            with _ur.urlopen("http://localhost:8001/api/calibration", timeout=5) as r:
                _c = _j.loads(r.read())
            if _c.get("n"):
                v = (_c.get("verdict") or "?").replace("_", " ")
                calib_block = (
                    f"\n<b>Calibration:</b> Brier {_c.get('brier'):.3f}, "
                    f"miscal {(_c.get('mean_abs_miscalibration') or 0)*100:.1f}% — {v}"
                )
        except Exception:
            pass

        funnel_block = ""
        try:
            with _ur.urlopen(
                "http://localhost:8001/api/funnel/decisions?hours=24", timeout=5,
            ) as r:
                _f = _j.loads(r.read())
            top = (_f.get("by_gate") or [])[:1]
            if top:
                t = top[0]
                funnel_block = (
                    f"\n<b>Top binding gate:</b> {t.get('gate')} "
                    f"({t.get('rejected')} rejected on {t.get('surface')})"
                )
        except Exception:
            pass

        lines = [
            "\U0001f4ca <b>DAILY DIGEST</b>\n",
            f"<b>Pipeline (24h):</b>",
            f"  Signals fired: {total_fired}",
            f"  Paper trades placed: {total_placed} ({conv:.1f}%)",
            f"  Trades resolved: {total_exited}",
            f"  Day PnL: {'+' if total_pnl >= 0 else ''}{_fmt_usd(total_pnl)}",
            f"\n<b>Portfolio:</b>",
            f"  Equity: {_fmt_usd(equity)}",
            f"  Cum PnL: {'+' if cum_pnl >= 0 else ''}{_fmt_usd(cum_pnl)}",
            f"  Open positions: {open_count}",
            ladder_block,
            calib_block,
            funnel_block,
        ]

        if placed:
            lines.append(f"\n<b>Trades placed by signal:</b>")
            for st, n, vol in placed:
                lines.append(f"  {st}: {n} (${vol or 0:.0f})")

        if exited:
            lines.append(f"\n<b>Exits by reason:</b>")
            for reason, n, w, pnl in exited:
                icon = "\u2705" if (pnl or 0) >= 0 else "\u274c"
                lines.append(f"  {icon} {reason or 'resolution'}: {n} ({w}W, {'+' if (pnl or 0) >= 0 else ''}{_fmt_usd(float(pnl or 0))})")

        if readiness:
            lines.append(f"\n<b>Signal readiness:</b>")
            for st, n, status in readiness:
                bar = "\u2588" * min(int((n or 0) / 15 * 10), 10)
                lines.append(f"  {st}: {n or 0}/15 [{bar}] {status}")

        # Quiet warning (no fires in 48h) — use already-fetched data
        quiet = [st for st, n in fired if False]  # placeholder
        if quiet:
            lines.append("")
            for st, last in quiet:
                hours_ago = (time.time() - (last or 0)) / 3600
                lines.append(f"\u26a0\ufe0f {st}: silent for {hours_ago:.0f}h")

        msg = "\n".join(lines) + _FOOTER
        return self._send(msg, disable_notification=True)

    # ── Order book anomaly ──────────────────────────────────────────────────

    def send_order_book_anomaly(
        self,
        anomaly: dict,
        signal_fired: bool = False,
        paper_trade: dict | None = None,
    ) -> bool:
        """Suppressed — order book anomalies are noise, not actionable alpha."""
        return False  # silently drop
        """Immediate alert for a critical/high-severity order book anomaly.

        Critical alerts arrive loud (sound + vibration). High severity is
        sent silently to avoid spam from continuous polling. Medium and
        below are dropped entirely.
        """
        severity = (anomaly.get("severity") or "medium").lower()
        if severity not in ("critical", "high"):
            return False

        icon = "\U0001f534" if severity == "critical" else "\U0001f7e1"
        anomaly_type = anomaly.get("anomaly_type") or anomaly.get("type", "")
        question = (anomaly.get("question") or "")[:55]
        category = (anomaly.get("category") or "other").title()

        type_labels = {
            "order_book_imbalance": "Order book imbalance",
            "bid_wall": "Bid wall detected",
            "ask_removal": "Ask liquidity pulled",
            "sustained_imbalance": "Sustained buy pressure",
            "spread_compression": "Spread compression",
        }
        type_label = type_labels.get(anomaly_type, anomaly_type)

        bid_usd = anomaly.get("bid_usd") or 0
        ask_usd = anomaly.get("ask_usd") or 0
        detail = anomaly.get("detail") or ""

        lines = [
            f"{icon} <b>ORDER BOOK ANOMALY \u2014 {severity.upper()}</b>",
            f"<b>Market:</b> \"{question}\"",
            f"<b>Category:</b> {category}",
            "",
            f"<b>{type_label}</b>",
            detail,
        ]
        if bid_usd and ask_usd:
            lines.append(f"{_fmt_usd(bid_usd)} bids vs {_fmt_usd(ask_usd)} asks")

        if signal_fired and paper_trade:
            stake = paper_trade.get("stake") or paper_trade.get("size_usd") or 0
            sig_type = paper_trade.get("signal_type") or ""
            conf = paper_trade.get("confidence") or 0
            lines.append("")
            lines.append("\u26a1 Signal fired \u2192 paper trade placed")
            lines.append(f"\u251c {sig_type} confidence: {conf:.0%}")
            lines.append(f"\u2514 Stake: {_fmt_usd(stake)}")
        elif signal_fired:
            lines.append("")
            lines.append(f"\u26a1 Signal fired ({anomaly.get('signal_type', '')})")

        msg = "\n".join(lines) + _FOOTER
        # Critical = loud notification, high = silent
        return self._send(msg, disable_notification=(severity != "critical"))

    # ── Hot market discovery ────────────────────────────────────────────────

    def send_hot_market_discovered(self, markets: list[dict]) -> bool:
        """Alert when the hot scanner adds new high-volume markets.

        Batched: multiple markets in one message. Always silent. Rate
        limited to one alert per hour regardless of caller frequency.
        """
        if not markets:
            return False
        now = time.time()
        if now - self._last_hot_market_alert < 3600:
            return False
        self._last_hot_market_alert = now

        if len(markets) == 1:
            m = markets[0]
            vol = m.get("volume_24h") or 0
            reasons = ", ".join(m.get("reasons") or [])
            msg = (
                f"\U0001f525 <b>HOT MARKET DISCOVERED</b>\n"
                f"\"{(m.get('question') or '')[:55]}\"\n"
                f"{(m.get('category') or 'other').title()} \u00b7 {_fmt_usd(vol)} 24h volume\n\n"
                f"Reasons: {reasons}\n\n"
                f"Added to watchlist \u2014 monitoring for whale activity"
                f"{_FOOTER}"
            )
        else:
            lines = [f"\U0001f525 <b>{len(markets)} HOT MARKETS DISCOVERED</b>", ""]
            for m in markets[:5]:
                vol = m.get("volume_24h") or 0
                lines.append(f"\u2022 \"{(m.get('question') or '')[:45]}\" {_fmt_usd(vol)}")
            if len(markets) > 5:
                lines.append(f"  ...and {len(markets)-5} more")
            msg = "\n".join(lines) + _FOOTER

        return self._send(msg, disable_notification=True)

    # ── Velocity spike ──────────────────────────────────────────────────────

    def send_velocity_spike(
        self,
        question: str,
        category: str,
        price_change: float,
        velocity: float,
        current_price: float,
        minutes_elapsed: float,
        signal_fired: bool = False,
        paper_trade: dict | None = None,
    ) -> bool:
        """Suppressed — velocity spikes are noise, not actionable alpha."""
        return False  # silently drop all velocity alerts
        # Original code below kept for reference:
        """LOUD alert for a price velocity spike (>=10% move in <30 min)."""
        direction = "\U0001f4c8" if price_change > 0 else "\U0001f4c9"
        start_price = current_price - price_change

        lines = [
            f"\u26a1 <b>PRICE VELOCITY SPIKE</b>",
            f"\"{(question or '')[:55]}\"",
            f"{(category or 'other').title()}",
            "",
            f"{direction} {abs(price_change):.0%} in {minutes_elapsed:.0f} minutes",
            f"Price: {start_price:.3f} \u2192 {current_price:.3f}",
        ]

        if signal_fired and paper_trade:
            stake = paper_trade.get("stake") or paper_trade.get("size_usd") or 0
            side = paper_trade.get("side") or "?"
            lines.append("")
            lines.append("\u26a1 velocity signal fired")
            lines.append(f"\u251c Stake: {_fmt_usd(stake)} ({side})")
            lines.append(f"\u2514 Entry: ~{current_price:.3f} (+2% slippage)")
        elif signal_fired:
            lines.append("")
            lines.append("\u26a1 velocity signal fired")
        else:
            lines.append("")
            lines.append("\u26a0\ufe0f Below confidence threshold \u2014 monitoring")

        msg = "\n".join(lines) + _FOOTER
        # Time-sensitive: always loud, bypasses rate limit
        return self._send(msg, disable_notification=False)

    # ── Pipeline alerts ──────────────────────────────────────────────────────

    def send_pipeline_alert(self, component: str, message: str, level: str = "warning") -> bool:
        now = time.time()
        if level != "ok" and now - self._last_pipeline_alert.get(component, 0) < 1800:
            return False
        self._last_pipeline_alert[component] = now

        level_e = {
            "warning": "\u26a0\ufe0f", "critical": "\U0001f6a8", "ok": "\u2705",
        }.get(level, "\U0001f4e2")
        ts = datetime.now(tz=timezone.utc).strftime("%H:%M:%S")
        msg = (
            f"{level_e} <b>PIPELINE {level.upper()}</b>\n"
            f"Component: <code>{component}</code>\n"
            f"Message: {message}\n"
            f"Time: {ts}"
            f"{_FOOTER}"
        )
        return self._send(msg)

    # ── Test ─────────────────────────────────────────────────────────────────

    def send_test(self) -> bool:
        ts = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        msg = (
            f"\u2705 <b>Polymarket Whale Monitor</b>\n"
            f"Telegram alerts configured and working.\n"
            f"Time: {ts}\n\n"
            f"Alert types:\n"
            f"\U0001f40b Tier-1 whale detections\n"
            f"\U0001f4e1 9 signal types with context\n"
            f"\U0001f4dd Paper trades with justification\n"
            f"\u2705/\u274c Trade resolutions with P&L\n"
            f"\U0001f4ca Daily position updates"
            f"{_FOOTER}"
        )
        return self._send(msg)


# Module-level singleton
_alerter: TelegramAlerter | None = None


def get_alerter() -> TelegramAlerter:
    global _alerter
    if _alerter is None:
        _alerter = TelegramAlerter()
    return _alerter
