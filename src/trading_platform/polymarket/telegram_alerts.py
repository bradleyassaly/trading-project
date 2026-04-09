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
    "specialist_entry": "\U0001f393", "pre_deadline_surge": "\u23f0",
    "whale_entry": "\U0001f40b",
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


class TelegramAlerter:
    """Non-blocking Telegram alert sender with rich context.

    Supports a per-message ``disable_notification`` flag (silent vs loud)
    and a global 20/hour rate limit on non-critical messages so background
    pollers can't accidentally spam the chat. Messages with
    ``disable_notification=False`` (i.e. critical / time-sensitive alerts)
    bypass the rate limit.
    """

    MAX_NONCRITICAL_PER_HOUR = 20

    def __init__(self) -> None:
        self.bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
        self.enabled = bool(self.bot_token and self.chat_id)
        self._last_pipeline_alert: dict[str, float] = {}
        self._last_daily_update: float = 0
        self._last_hot_market_alert: float = 0
        # Sliding 1h window of non-critical send timestamps
        self._noncrit_send_log: list[float] = []

    def _within_noncrit_budget(self) -> bool:
        """True if a non-critical send is allowed under the 20/hour cap."""
        now = time.time()
        cutoff = now - 3600
        self._noncrit_send_log = [t for t in self._noncrit_send_log if t >= cutoff]
        return len(self._noncrit_send_log) < self.MAX_NONCRITICAL_PER_HOUR

    def _send(self, message: str, disable_notification: bool = False) -> bool:
        """Send a message to Telegram.

        Parameters
        ----------
        message:
            HTML-formatted message text.
        disable_notification:
            When True the message arrives silently and is subject to the
            non-critical rate limit. When False (default behaviour for
            backwards compatibility) the message bypasses the rate limit
            and triggers a sound/vibration notification.
        """
        if not self.enabled:
            return False
        if disable_notification:
            if not self._within_noncrit_budget():
                logger.debug("Telegram non-critical rate limit hit; dropping message")
                return False
            self._noncrit_send_log.append(time.time())
        try:
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            payload = json.dumps({
                "chat_id": self.chat_id,
                "text": message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
                "disable_notification": bool(disable_notification),
            }).encode()
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
        emoji = _SIGNAL_EMOJIS.get(sig_type, "\U0001f4e1")
        side_e = "\U0001f7e2" if signal.get("direction") == "BUY" else "\U0001f534"
        question = (signal.get("question") or signal.get("condition_id", ""))[:80]
        conf = signal.get("confidence", 0) or 0
        size = signal.get("size", 0) or 0
        wallet = (signal.get("wallet") or "")[:12]
        cat = (signal.get("category") or "").upper()
        wr = signal.get("directional_win_rate", 0) or 0

        # Signal-specific context
        detail = ""
        if sig_type == "cascade":
            n = signal.get("cascade_wallets") or signal.get("converging_wallets", 0)
            detail = f"\u251c Type: cascade ({n} tier-1 wallets confirmed)\n"
        elif sig_type == "convergence":
            n = signal.get("converging_wallets", 0)
            detail = f"\u251c Type: convergence ({n} wallets same side)\n"
        elif sig_type == "oversized_bet":
            detail = f"\u251c Type: oversized bet (above wallet threshold)\n"
        elif sig_type == "wallet_reversal":
            detail = f"\u251c Type: wallet reversal (flipped direction)\n"
        elif sig_type == "accumulation":
            n = signal.get("accumulation_count", 0)
            detail = f"\u251c Type: accumulation ({n} entries)\n"
        elif sig_type == "market_maker_flip":
            detail = f"\u251c Type: market maker went directional\n"
        else:
            detail = f"\u251c Type: {sig_type}\n"

        msg = (
            f"{emoji} <b>SIGNAL: {sig_type.upper().replace('_', ' ')}</b>\n"
            f"{side_e} <b>{signal.get('direction', '')} \u2014 {cat}</b>\n\n"
            f"\U0001f4ca <b>Market:</b> {question}\n\n"
            f"Signal details:\n"
            f"{detail}"
            f"\u251c Confidence: {conf:.0%}\n"
            f"\u2514 Wallet: <code>{wallet}...</code> | WR: {wr:.0%}\n"
        )

        # Paper trade info
        if paper_result and paper_result.get("placed"):
            stake = paper_result.get("stake", 0)
            bankroll = paper_result.get("bankroll_remaining", 0)
            msg += (
                f"\n\u2705 <b>PAPER TRADE PLACED</b>\n"
                f"\u2514 {signal.get('direction', 'BUY')} {_fmt_usd(stake)} @ {signal.get('price', 0):.3f}\n"
                f"   Bankroll: {_fmt_usd(bankroll)} remaining"
            )

        msg += _FOOTER
        return self._send(msg)

    # ── Paper trade ──────────────────────────────────────────────────────────

    def send_paper_trade(self, trade: dict) -> bool:
        """Alert when a paper trade is placed (standalone, without signal context)."""
        side_e = "\U0001f7e2" if trade.get("side") == "BUY" else "\U0001f534"
        question = (trade.get("question") or trade.get("condition_id", ""))[:70]
        stake = trade.get("stake", 0) or 0
        conf = trade.get("confidence", 0) or 0
        sig = trade.get("signal_type", "")

        msg = (
            f"\U0001f4dd <b>PAPER TRADE: {sig}</b>\n"
            f"{side_e} <b>{trade.get('side', 'BUY')}</b>\n\n"
            f"\U0001f4ca {question}\n"
            f"\U0001f4b0 Stake: {_fmt_usd(stake)} @ {trade.get('price', 0):.3f}\n"
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
        side = trade.get("side", "")
        entry = trade.get("entry_price", 0)
        size = trade.get("size_usd", 0)
        ret = trade.get("return_pct", 0) or 0
        pnl = size * ret if size else 0

        msg = (
            f"{emoji} <b>TRADE {'WON' if won else 'LOST'}: {sig}</b>\n"
            f"{question}\n"
            f"{side} {_fmt_usd(size)} @ {entry:.3f}\n"
            f"P&L: {'+'if pnl>=0 else ''}{_fmt_usd(pnl)} ({ret*100:+.1f}%)\n"
        )

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

        bankroll = 100_000
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
            side = p.get("side") or "YES"
            size = float(p.get("size_usd") or 0)
            sig = p.get("signal_type") or ""

            if unr is not None and cur is not None:
                icon = "\U0001f7e2" if unr >= 0 else "\U0001f534"
                pct = p.get("unrealized_pct") or 0
                lines.append(
                    f"{icon} {side} {_fmt_usd(size)} @ {entry:.3f}  ({sig})\n"
                    f"   \"{q}\"\n"
                    f"   Now: {cur:.3f}  \u2192  {unr:+.1f} ({pct:+.1f}%)"
                )
            else:
                lines.append(
                    f"\u26aa {side} {_fmt_usd(size)} @ {entry:.3f}  ({sig})\n"
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

    # ── Order book anomaly ──────────────────────────────────────────────────

    def send_order_book_anomaly(
        self,
        anomaly: dict,
        signal_fired: bool = False,
        paper_trade: dict | None = None,
    ) -> bool:
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
