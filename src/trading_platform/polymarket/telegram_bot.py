"""Telegram command bot — interactive queries + emergency stop.

Long-polling bot that accepts a small set of commands from the
authorised chat and responds with live system state. Designed to be
the "remote control" you reach for when you're away from the dashboard
(mobile signal isn't always strong enough for the web UI).

Commands::

    /status      — container health, last trades, live-readiness gates
    /positions   — open paper + live positions with unrealized PnL
    /readiness   — live-trade readiness detail (gates + probation)
    /funnel      — 24h signal → paper-trade funnel
    /insiders    — top insider wallets + signal activity
    /kill        — emergency-stop (blocks all trading until cleared)
    /unkill      — clear emergency stop
    /help        — list commands

Auth: only messages from TELEGRAM_CHAT_ID (the configured user) are
accepted. Every other sender is silently dropped.
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[3]

# Default to the API container's internal address; override via env.
API_URL = os.environ.get("TELEGRAM_BOT_API_URL", "http://api:8001")
POLL_TIMEOUT_S = 25  # long-poll window
REQUEST_TIMEOUT_S = 30


def _tg_request(method: str, params: dict | None = None, bot_token: str = "") -> dict | None:
    url = f"https://api.telegram.org/bot{bot_token}/{method}"
    data = urllib.parse.urlencode(params or {}).encode()
    try:
        with urllib.request.urlopen(url, data=data, timeout=REQUEST_TIMEOUT_S) as resp:
            return json.loads(resp.read())
    except Exception as exc:
        logger.debug("tg %s failed: %s", method, exc)
        return None


def _api_get(path: str) -> dict | None:
    try:
        with urllib.request.urlopen(f"{API_URL}{path}", timeout=10) as resp:
            return json.loads(resp.read())
    except Exception as exc:
        return {"_error": str(exc)[:120]}


def _api_post(path: str, body: dict | None = None) -> dict | None:
    try:
        req = urllib.request.Request(
            f"{API_URL}{path}",
            data=json.dumps(body or {}).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())
    except Exception as exc:
        return {"_error": str(exc)[:120]}


# ── Command handlers ──────────────────────────────────────────────────────

def _cmd_status() -> str:
    r = _api_get("/api/live/readiness") or {}
    gates = r.get("system_gates") or {}
    passed = sum(1 for v in gates.values() if v is True)
    total = sum(1 for v in gates.values() if isinstance(v, bool))
    ks = r.get("kill_switch_limits") or {}
    return (
        f"*SYSTEM STATUS*\n"
        f"Live gates: {passed}/{total}\n"
        f"Bankroll: ${ks.get('bankroll', 0):.0f}\n"
        f"Live trades exec'd: {gates.get('live_trades_executed', 0)}\n"
        f"Master switch: {'ON' if gates.get('master_switch') else 'OFF'}\n"
        f"Emergency stopped: {r.get('emergency_stopped', False)}"
    )


def _cmd_positions() -> str:
    lines = ["*POSITIONS*"]
    live = _api_get("/api/live/positions") or {}
    live_pos = live.get("positions") or []
    lines.append(f"_Live ({len(live_pos)}):_")
    for p in live_pos[:10]:
        unr = float(p.get("unrealized_pnl") or 0)
        sign = "+" if unr >= 0 else ""
        lines.append(
            f"  {p.get('signal_type', '?')} {p.get('side', '')} "
            f"${float(p.get('size_usd') or 0):.2f} {sign}${unr:.2f}"
        )
    if not live_pos:
        lines.append("  none")

    paper = _api_get("/api/pipeline/funnel?hours=24") or {}
    t = paper.get("totals") or {}
    lines.append(
        f"\n_Paper (24h):_ fired={t.get('signals_fired', 0)} "
        f"placed={t.get('paper_trades_placed', 0)} open={t.get('paper_trades_currently_open', 0)}"
    )
    return "\n".join(lines)


def _cmd_readiness() -> str:
    r = _api_get("/api/live/readiness") or {}
    lines = ["*LIVE READINESS*"]
    gates = r.get("system_gates") or {}
    for k, v in gates.items():
        icon = "✅" if v is True else ("❌" if v is False else "—")
        lines.append(f"  {icon} {k}")
    sig_gates = r.get("signal_gates") or []
    if sig_gates:
        lines.append("\n_Signals:_")
        for s in sig_gates[:6]:
            n = s.get("n_resolved", 0)
            prob = "🟢 PROBATION" if s.get("probation_eligible") else ("🟢 READY" if s.get("ready") else "—")
            lines.append(
                f"  {s.get('signal_type', '?')}: n={n} EV={s.get('ev', 0):+.4f} {prob}"
            )
    return "\n".join(lines)


def _cmd_funnel() -> str:
    r = _api_get("/api/pipeline/funnel?hours=24") or {}
    if not r.get("available"):
        return "Funnel unavailable: " + str(r.get("_error", "?"))
    t = r.get("totals") or {}
    fires = t.get("signals_fired", 0)
    placed = t.get("paper_trades_placed", 0)
    conv = placed / fires * 100 if fires else 0
    lines = [
        f"*SIGNAL FUNNEL (24h)*",
        f"Fires: {fires}",
        f"Placed: {placed} ({conv:.1f}%)",
        f"Resolved: {t.get('paper_trades_resolved_in_window', 0)}",
    ]
    for s in (r.get("by_signal") or [])[:5]:
        lines.append(f"  {s['signal_type']}: {s['fires']} → {s['placed']}")
    return "\n".join(lines)


def _cmd_insiders() -> str:
    r = _api_get("/api/insiders/list") or {}
    insiders = r.get("insiders") or []
    if not insiders:
        return "*INSIDERS*\nNone classified yet."
    lines = [f"*INSIDERS ({len(insiders)} wallets)*"]
    for i in insiders[:8]:
        lines.append(
            f"  {i.get('wallet', '')[:12]}… "
            f"WR={float(i.get('uncertain_accuracy', 0)) * 100:.0f}% "
            f"PnL=${float(i.get('total_pnl', 0)):.0f}"
        )
    return "\n".join(lines)


def _cmd_kill(reason: str = "remote-kill via telegram") -> str:
    r = _api_post("/api/system/emergency-stop", {"reason": reason})
    if r and not r.get("_error"):
        return f"*EMERGENCY STOP ENGAGED*\nReason: {reason}\nAll trading halted."
    return f"Kill failed: {r.get('_error') if r else 'no response'}"


def _cmd_unkill() -> str:
    r = _api_post("/api/system/clear-emergency-stop", {})
    if r and not r.get("_error"):
        return "*EMERGENCY STOP CLEARED*\nTrading resumes under normal gates."
    return f"Clear failed: {r.get('_error') if r else 'no response'}"


def _cmd_live_trade(args: str) -> str:
    """/live-trade <condition_id> <YES|NO> [stake_usd]

    Places a real $5-capped live trade. Thin wrapper over the
    force_live_test script so the same pre-flight checks run
    (USDC balance, Gamma lookup, book depth, tick alignment).
    Stake is hard-capped at $5 regardless of the arg.
    """
    parts = args.strip().split()
    if len(parts) < 2:
        return (
            "*USAGE*\n"
            "`/live-trade <condition_id> <YES|NO> [stake_usd]`\n\n"
            "Stake is capped at $5. Uses the same pre-flight checks "
            "as the force-live-test CLI."
        )
    cid = parts[0]
    side = parts[1].upper()
    if side not in ("YES", "NO"):
        return f"Side must be YES or NO, got `{side}`"
    if not cid.startswith("0x") or len(cid) != 66:
        return f"condition_id looks wrong: `{cid}` (expect 0x + 64 hex)"
    stake = 5.0
    if len(parts) >= 3:
        try:
            stake = min(float(parts[2]), 5.0)
        except ValueError:
            return f"stake must be a number, got `{parts[2]}`"

    # Invoke the force_live_test module via subprocess so we pick up its
    # full pre-flight chain. The module is importable but running it as
    # a subprocess keeps argv handling consistent with the CLI.
    import subprocess
    try:
        result = subprocess.run(
            [
                "python", "-m", "trading_platform.polymarket.force_live_test",
                "--condition-id", cid, "--side", side, "--stake", str(stake),
            ],
            capture_output=True, text=True, timeout=120,
            cwd="/app",
        )
    except subprocess.TimeoutExpired:
        return "Trade submission timed out (>120s). Check /positions to see if it landed."
    tail = "\n".join((result.stdout or "").splitlines()[-10:])
    if result.returncode == 0:
        return f"*LIVE TRADE SUBMITTED*\n```\n{tail[-500:]}\n```"
    return f"*TRADE FAILED* (exit {result.returncode})\n```\n{tail[-500:]}\n```"


def _cmd_help() -> str:
    return (
        "*COMMANDS*\n"
        "/status — system health + gates\n"
        "/positions — paper + live positions\n"
        "/readiness — live-trade gate detail\n"
        "/funnel — 24h signal → trade funnel\n"
        "/insiders — detected insider wallets\n"
        "/live-trade <cid> <YES|NO> [$stake] — place live trade ($5 cap)\n"
        "/kill [reason] — emergency stop\n"
        "/unkill — clear emergency stop\n"
        "/help — this menu"
    )


_HANDLERS = {
    "/status": lambda args: _cmd_status(),
    "/positions": lambda args: _cmd_positions(),
    "/readiness": lambda args: _cmd_readiness(),
    "/funnel": lambda args: _cmd_funnel(),
    "/insiders": lambda args: _cmd_insiders(),
    "/live-trade": lambda args: _cmd_live_trade(args),
    "/kill": lambda args: _cmd_kill(args or "remote-kill via telegram"),
    "/unkill": lambda args: _cmd_unkill(),
    "/help": lambda args: _cmd_help(),
    "/start": lambda args: _cmd_help(),
}


# ── Main loop ─────────────────────────────────────────────────────────────

def _handle_update(update: dict, bot_token: str, allowed_chat: str) -> None:
    msg = update.get("message") or update.get("edited_message") or {}
    chat_id = str(msg.get("chat", {}).get("id") or "")
    if allowed_chat and chat_id != str(allowed_chat):
        logger.info("ignoring msg from chat %s (not allowed)", chat_id)
        return
    text = (msg.get("text") or "").strip()
    if not text.startswith("/"):
        return
    parts = text.split(" ", 1)
    cmd = parts[0].split("@")[0].lower()  # strip @botname mention
    args = parts[1] if len(parts) > 1 else ""

    handler = _HANDLERS.get(cmd)
    if not handler:
        reply = f"Unknown command: `{cmd}`\nTry /help"
    else:
        try:
            reply = handler(args)
        except Exception as exc:
            logger.exception("handler %s failed", cmd)
            reply = f"Command error: {str(exc)[:200]}"

    _tg_request(
        "sendMessage",
        {"chat_id": chat_id, "text": reply, "parse_mode": "Markdown"},
        bot_token=bot_token,
    )


def main() -> int:
    try:
        from trading_platform.polymarket.logging_config import setup_logging
        setup_logging(service="telegram-bot")
    except Exception:
        logging.basicConfig(level=logging.INFO)

    from dotenv import load_dotenv
    load_dotenv(_PROJECT_ROOT / ".env")

    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN") or ""
    allowed_chat = os.environ.get("TELEGRAM_CHAT_ID") or ""
    if not bot_token:
        logger.error("TELEGRAM_BOT_TOKEN not set")
        return 2
    logger.info("[telegram-bot] starting long-poll (allowed chat: %s)", allowed_chat or "ANY")

    offset = 0
    while True:
        try:
            r = _tg_request(
                "getUpdates",
                {"timeout": POLL_TIMEOUT_S, "offset": offset},
                bot_token=bot_token,
            )
            if not r or not r.get("ok"):
                time.sleep(5)
                continue
            for update in r.get("result") or []:
                offset = max(offset, update.get("update_id", 0) + 1)
                _handle_update(update, bot_token, allowed_chat)
        except KeyboardInterrupt:
            return 0
        except Exception as exc:
            logger.warning("poll err: %s — backing off 10s", exc)
            time.sleep(10)


if __name__ == "__main__":
    raise SystemExit(main())
