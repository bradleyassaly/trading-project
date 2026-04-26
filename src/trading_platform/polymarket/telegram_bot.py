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


def _cmd_wallet(args: str) -> str:
    """Unified wallet profile via /api/wallet/{addr}/profile.
    Surfaces: PM PnL + lifetime + behavior metrics + insider tag +
    z-specialty + top alpha categories. Single screen."""
    addr = (args or "").strip().lower()
    if not addr.startswith("0x") or len(addr) != 42:
        return "Usage: /wallet 0xWALLETADDRESS (42 chars, 0x-prefixed)"
    r = _api_get(f"/api/wallet/{addr}/profile") or {}
    if r.get("error") or not r:
        return f"*WALLET {addr[:12]}…*\nlookup failed: {r.get('error', 'n/a')}"
    p = r.get("profile") or {}
    b = r.get("behavior") or {}
    ins = r.get("insider") or {}
    alpha = r.get("alpha_by_category") or []
    z = r.get("zscore_by_category") or []

    lines = [f"*WALLET {addr[:12]}…*"]
    if p.get("pseudonym"):
        lines.append(f"  alias: {p['pseudonym']}")
    pm = p.get("pm_pnl_usdc")
    if pm:
        lines.append(f"  PM PnL: ${float(pm):,.0f} | vol ${float(p.get('pm_volume_usdc') or 0):,.0f}")
    if p.get("net_pnl_usdc") is not None:
        lines.append(
            f"  resolved: {p.get('resolved_trades') or 0} trades, "
            f"WR {float(p.get('directional_win_rate') or 0) * 100:.0f}%, "
            f"PnL ${float(p.get('net_pnl_usdc') or 0):,.0f}"
        )

    if b:
        ci = "✓ copyable-CI" if b.get("is_copyable_ci") else "—"
        farmer = " ⚠ FARMER" if b.get("is_likely_farmer") else ""
        lines.append(
            f"  behavior: {ci}{farmer} | "
            f"ROI {float(b.get('roi_mean') or 0):+.3f} "
            f"[LB {float(b.get('roi_lower_95') or 0):+.3f}] | "
            f"sizing p90 {float(b.get('sizing_p90_pct') or 0) * 100:.1f}%"
        )

    if ins:
        lines.append(f"  insider: score {float(ins.get('insider_score') or 0):.2f}")

    specialists = [s for s in z if s.get("is_specialist_z")]
    if specialists:
        top_z = max(specialists, key=lambda s: s.get("z_score") or 0)
        lines.append(
            f"  top z-cat: {top_z['category']} z={float(top_z.get('z_score') or 0):.2f} "
            f"({float(top_z.get('cat_wr') or 0) * 100:.0f}% WR on n={top_z.get('n_in_cat')})"
        )

    if alpha:
        lines.append("  top alpha categories:")
        for a in alpha[:3]:
            cop = "✓" if a.get("is_copyable") else "—"
            lines.append(
                f"    {cop} {a['category']:<14s} "
                f"WR {float(a.get('win_rate') or 0) * 100:.0f}% "
                f"PnL ${float(a.get('total_pnl') or 0):,.0f} "
                f"n={a.get('resolved_trades')}"
            )
    return "\n".join(lines)


def _cmd_ladder(args: str = "") -> str:
    """L0→L5 ladder progress + gates remaining + lane state."""
    r = _api_get("/api/ladder/status") or {}
    if r.get("error"):
        return f"*LADDER*\nlookup failed: {r.get('error', 'n/a')}"
    level = r.get("level", "?")
    name = r.get("level_name", "?")
    bk = r.get("bankroll_usd", 0)
    target = r.get("target_bankroll_l5", 200000)
    pct = r.get("progress_pct", 0)
    paper = r.get("paper") or {}
    live = r.get("live") or {}
    hyp_acc = r.get("hypothesis_accuracy_30d")
    lanes = r.get("lanes") or {}

    lines = [f"*LADDER* — {level} {name}"]
    lines.append(f"  bankroll ${bk:,.0f} → target ${target:,.0f} ({pct:.1f}%)")
    lines.append(
        f"  paper 30d: n={paper.get('closed_30d', 0)} "
        f"WR {(paper.get('wr_30d') or 0) * 100:.0f}% "
        f"PnL ${paper.get('pnl_30d', 0):.0f}"
    )
    lines.append(
        f"  live  30d: {live.get('submitted_30d', 0)} submitted, "
        f"{live.get('closed_30d', 0)} closed"
    )
    if hyp_acc is not None:
        lines.append(f"  hypothesis acc 30d: {hyp_acc * 100:.1f}%")

    nx = r.get("next_level") or "—"
    if nx and nx != "—":
        lines.append(f"\n*GATES TO {nx.upper()}*")
        for g in r.get("gates_to_next") or []:
            mark = "✓" if g.get("passed") else "○"
            lines.append(f"  {mark} {g.get('name')} → {g.get('value')}")

    lane_bits = []
    if lanes.get("live_discovery_enabled"): lane_bits.append("DISCOVERY")
    if lanes.get("phase_b_enabled"): lane_bits.append("PHASE_B")
    if lanes.get("phase_d_enabled"): lane_bits.append("PHASE_D")
    lines.append(f"\n*LANES*: {', '.join(lane_bits) if lane_bits else 'baseline only'}")
    return "\n".join(lines)


def _cmd_lanes(args: str = "") -> str:
    """Just the lane state — quick check before enabling/disabling."""
    r = _api_get("/api/ladder/status") or {}
    lanes = r.get("lanes") or {}
    rows = [
        ("LIVE_DISCOVERY",  "live_discovery_enabled",  "$1 live trades enabled"),
        ("PHASE_B",         "phase_b_enabled",          "resolution_decay independent signal"),
        ("PHASE_D",         "phase_d_enabled",          "cross-platform divergence (Kalshi)"),
    ]
    out = ["*LANES*"]
    for label, key, desc in rows:
        on = lanes.get(key)
        out.append(f"  {'✓' if on else '○'} {label}: {desc}")
    return "\n".join(out)


def _cmd_calibration(args: str = "") -> str:
    """Brier score + verdict + top miscalibrated signals."""
    r = _api_get("/api/calibration") or {}
    if r.get("error") or not r.get("n"):
        return f"*CALIBRATION*\n{r.get('error', 'no resolved hypotheses yet')}"
    brier = r.get("brier")
    miscal = r.get("mean_abs_miscalibration")
    verdict = r.get("verdict") or "—"
    n = r.get("n", 0)
    lines = [
        f"*CALIBRATION* — {verdict.replace('_', ' ').upper()}",
        f"  Brier: {brier:.3f} | mean abs miscal: {(miscal or 0) * 100:.1f}%",
        f"  n resolved: {n} (last 30d)",
    ]
    by_sig = r.get("by_signal") or []
    if by_sig:
        lines.append("\n*PER SIGNAL (top 6 by n)*")
        for s in by_sig[:6]:
            mp = s.get("mean_pred") or 0
            ma = s.get("mean_actual") or 0
            gap = abs(mp - ma)
            arrow = "→" if abs(mp - ma) < 0.05 else ("↓" if mp > ma else "↑")
            lines.append(
                f"  {s.get('signal_type'):<22s} n={s.get('n'):<3d} "
                f"pred={mp:.2f} {arrow} act={ma:.2f}"
            )
    return "\n".join(lines)


def _cmd_signal_health(args: str = "") -> str:
    """Per-signal IC + decay flags."""
    r = _api_get("/api/signal-health") or {}
    sigs = r.get("signals") or []
    if not sigs:
        return "*SIGNAL HEALTH*\nNo data yet (signal_health task hasn't run)."
    decayed = [s for s in sigs if s.get("decay_flag")]
    lines = [f"*SIGNAL HEALTH* — {len(sigs)} tracked, {len(decayed)} decay-flagged"]
    lines.append("\n*TOP IC (14d)*")
    for s in sigs[:6]:
        ic = s.get("ic_14d") or 0
        flag = " ⚠ DECAY" if s.get("decay_flag") else ""
        corr = s.get("correlation_max") or 0
        corr_note = f" ⚠ corr {corr:.2f}" if corr >= 0.7 else ""
        lines.append(
            f"  {s.get('signal_type'):<22s} IC={ic:+.2f} n={s.get('n_resolved_30d')}{flag}{corr_note}"
        )
    if decayed:
        lines.append(f"\n*DECAY-FLAGGED* (auto-blocked at signal time)")
        for s in decayed[:5]:
            lines.append(f"  {s.get('signal_type')} (IC {s.get('ic_14d') or 0:+.2f})")
    return "\n".join(lines)


def _cmd_pnl(args: str = "") -> str:
    """Today's paper PnL + WR + open positions + last trade."""
    r = _api_get("/api/ladder/status") or {}
    paper = r.get("paper") or {}
    live = r.get("live") or {}
    bk = r.get("bankroll_usd", 0)
    pos = _api_get("/api/live/positions") or {}
    open_paper = (pos.get("paper") or {}).get("open_count", 0) if pos else 0

    lines = ["*PnL — 30d snapshot*"]
    lines.append(f"  bankroll: ${bk:,.0f}")
    lines.append(
        f"  paper: n={paper.get('closed_30d', 0)} "
        f"WR {(paper.get('wr_30d') or 0) * 100:.0f}% "
        f"PnL ${paper.get('pnl_30d', 0):+,.2f}"
    )
    lines.append(
        f"  live:  {live.get('submitted_30d', 0)} submitted, "
        f"{live.get('closed_30d', 0)} closed, "
        f"WR {(live.get('wr_30d') or 0) * 100:.0f}%"
    )
    lines.append(f"  open paper positions: {open_paper}")
    return "\n".join(lines)


def _cmd_portfolio(args: str = "") -> str:
    """Open positions grouped by category — paper + live."""
    r = _api_get("/api/portfolio/by-category") or {}
    if r.get("error"):
        return f"*PORTFOLIO*\n{r.get('error')}"
    paper = r.get("paper") or []
    live = r.get("live") or []
    totals = r.get("totals") or {}

    lines = ["*PORTFOLIO* — open positions"]
    if paper:
        lines.append("\n*PAPER (by category)*")
        for p in paper[:10]:
            unreal_str = (
                f" {'+' if p['unrealized'] >= 0 else ''}{p['unrealized']:.2f}"
                if p['unrealized'] else ''
            )
            lines.append(
                f"  {(p['category'] or '?'):<14s} "
                f"{p['n_open']:>3d} open  ${p['deployed']:.0f}{unreal_str}"
            )
        lines.append(
            f"  ─── totals: {totals.get('paper_n', 0)} open, "
            f"${totals.get('paper_deployed', 0):.0f} deployed, "
            f"unrealized {'+' if totals.get('paper_unrealized', 0) >= 0 else ''}"
            f"{totals.get('paper_unrealized', 0):.2f}"
        )
    else:
        lines.append("\n*PAPER*: no open positions")

    if live:
        lines.append("\n*LIVE*")
        for l in live:
            lines.append(f"  {l.get('n_open')} open, ${l.get('deployed', 0):.2f} deployed")
    else:
        lines.append("\n*LIVE*: no open positions")
    return "\n".join(lines)


def _cmd_leaderboard(args: str = "") -> str:
    """Top wallets by recent paper PnL. Args: optional <days> (default 7)."""
    try:
        days = int(args.strip()) if args and args.strip().isdigit() else 7
    except Exception:
        days = 7
    days = max(1, min(days, 30))
    r = _api_get(f"/api/leaderboard/recent?days={days}&limit=10") or {}
    if r.get("error"):
        return f"*LEADERBOARD*\n{r.get('error')}"
    rows = r.get("leaderboard") or []
    if not rows:
        return f"*LEADERBOARD ({days}d)*\nno qualifying wallets (need n≥2 resolved)"

    lines = [f"*LEADERBOARD — top by {days}d realized PnL*"]
    for i, row in enumerate(rows, 1):
        flags = []
        if row.get("is_copyable_ci"): flags.append("✓CI")
        if row.get("is_likely_farmer"): flags.append("⚠farmer")
        flag_str = f" [{','.join(flags)}]" if flags else ""
        pseudo = row.get("pseudonym")
        name_str = f" {pseudo}" if pseudo else ""
        pm_str = (
            f" PM ${row['pm_pnl_usdc'] / 1000:.0f}K" if row.get("pm_pnl_usdc")
            else ""
        )
        lines.append(
            f"  {i:>2d}. {row['wallet'][:12]}…{name_str}{flag_str}\n"
            f"      n={row['n']} WR={row['wr'] * 100:.0f}% "
            f"PnL ${row['pnl']:+.2f}{pm_str}"
        )
    return "\n".join(lines)


def _cmd_heatmap(args: str = "") -> str:
    """Per (signal, category) WR + PnL — surfaces best slices."""
    try:
        days = int(args.strip()) if args and args.strip().isdigit() else 30
    except Exception:
        days = 30
    days = max(7, min(days, 90))
    r = _api_get(f"/api/heatmap/signal-category?days={days}") or {}
    if r.get("error"):
        return f"*HEATMAP*\n{r.get('error')}"
    cells = r.get("cells") or []
    if not cells:
        return f"*HEATMAP ({days}d)*\nno resolved data"

    lines = [f"*HEATMAP — top (signal × category) tuples by PnL ({days}d)*"]
    pos = [c for c in cells if c["pnl"] > 0][:8]
    neg = [c for c in cells if c["pnl"] < 0][-5:]
    if pos:
        lines.append("\n*WINNERS*")
        for c in pos:
            lines.append(
                f"  {c['signal_type']:<22s} × {c['category']:<10s} "
                f"n={c['n']:<3d} WR={c['wr'] * 100:.0f}% "
                f"PnL ${c['pnl']:+.2f}"
            )
    if neg:
        lines.append("\n*LOSERS*")
        for c in neg:
            lines.append(
                f"  {c['signal_type']:<22s} × {c['category']:<10s} "
                f"n={c['n']:<3d} WR={c['wr'] * 100:.0f}% "
                f"PnL ${c['pnl']:+.2f}"
            )
    return "\n".join(lines)


def _cmd_promote(args: str = "") -> str:
    """Guard-railed ladder promotion. Two-phase:
       /promote          — show what would happen, no action
       /promote confirm  — record promotion to ladder_promotions table,
                           raise loud alert. CAPITAL deposit is manual.
    """
    r = _api_get("/api/ladder/status") or {}
    if r.get("error") or not r.get("level"):
        return f"*PROMOTE*\nladder lookup failed: {r.get('error', 'n/a')}"
    level = r.get("level")
    next_level = r.get("next_level") or "—"
    gates = r.get("gates_to_next") or []
    passed = sum(1 for g in gates if g.get("passed"))
    total = len(gates)
    confirm = (args or "").strip().lower() == "confirm"

    if total == 0 or next_level == "—":
        return f"*PROMOTE*\nno next level above {level} (already at top)"
    if passed < total:
        unmet = [g for g in gates if not g.get("passed")]
        lines = [
            f"*PROMOTE BLOCKED* — {passed}/{total} gates passed",
            f"  current: {level}, target: {next_level}",
            "  unmet gates:",
        ]
        for g in unmet:
            lines.append(f"    ○ {g.get('name')} → {g.get('value')}")
        lines.append("\nPromotion will unlock once all gates pass.")
        return "\n".join(lines)

    if not confirm:
        bk = r.get("bankroll_usd", 0)
        return (
            f"*PROMOTE READY* — {passed}/{total} gates ✓\n"
            f"  current: {level} (${bk:,.0f}) → target: {next_level}\n"
            f"  Required deposit (manual): see LIVE_TRADING_CHECKLIST.md\n\n"
            f"Type `/promote confirm` to record the promotion to the audit log + raise alert.\n"
            f"This does NOT deposit capital — that step is manual."
        )

    # Confirm path: record promotion + alert
    payload = {"from_level": level, "to_level": next_level,
               "passed_gates": passed, "total_gates": total}
    res = _api_post("/api/ladder/promote", payload) or {}
    if res.get("ok"):
        return (
            f"*PROMOTION RECORDED* {level} → {next_level}\n"
            f"  Audit row written. Deposit capital + update "
            f"POLYMARKET_LIVE_BANKROLL_USD when funded."
        )
    return f"*PROMOTE CONFIRM FAILED*\n{res.get('error') or res.get('_error') or 'no response'}"


def _cmd_diff(args: str = "") -> str:
    """What changed since 24h ago: paper trades, PnL, insider pool, ALPHA gate."""
    r = _api_get("/api/diff/24h") or {}
    if r.get("error"):
        return f"*DIFF*\n{r.get('error')}"
    bits = ["*DIFF — last 24h vs prior 24h*"]
    for k, label in (
        ("paper_trades", "paper trades"),
        ("pnl", "paper PnL"),
        ("hypotheses_resolved", "hypotheses resolved"),
        ("hypothesis_accuracy_pct", "hypothesis acc %"),
        ("insider_pool", "insider pool"),
        ("alpha_gate_rejections", "ALPHA_GATE rejections"),
    ):
        cur = r.get(f"{k}_today")
        prev = r.get(f"{k}_yesterday")
        if cur is None and prev is None:
            continue
        delta = (cur or 0) - (prev or 0)
        arrow = "↑" if delta > 0 else "↓" if delta < 0 else "→"
        bits.append(f"  {label}: {prev} {arrow} {cur} ({'+' if delta >= 0 else ''}{delta})")
    return "\n".join(bits)


def _cmd_explain(args: str = "") -> str:
    """Plain-language explanation of why a signal fired or was rejected."""
    sid = (args or "").strip()
    if not sid:
        return ("Usage: /explain <signal_outcome_id> | <decision_trace_id>\n"
                "Pulls the most recent gate decision for that id.")
    r = _api_get(f"/api/explain/{sid}") or {}
    if r.get("error"):
        return f"*EXPLAIN {sid}*\n{r.get('error')}"
    return r.get("explanation") or "no explanation available"


def _cmd_kill(reason: str = "remote-kill via telegram") -> str:
    r = _api_post("/api/live/emergency-stop", {"reason": reason})
    if r and r.get("ok"):
        return f"*EMERGENCY STOP ENGAGED*\nReason: {reason}\nAll trading halted."
    return f"Kill failed: {r.get('error') or r.get('_error') if r else 'no response'}"


def _cmd_unkill() -> str:
    r = _api_post("/api/live/clear-stop", {})
    if r and r.get("ok"):
        return "*EMERGENCY STOP CLEARED*\nTrading resumes under normal gates."
    return f"Clear failed: {r.get('error') or r.get('_error') if r else 'no response'}"


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
        "/ladder — L0→L5 progress + gates to next\n"
        "/lanes — feature-flag state (DISCOVERY / PHASE_B / PHASE_D)\n"
        "/pnl — 30d paper + live PnL summary\n"
        "/calibration — Brier score + miscalibration\n"
        "/signal-health — per-signal IC + decay flags\n"
        "/positions — paper + live positions\n"
        "/readiness — live-trade gate detail\n"
        "/funnel — 24h signal → trade funnel\n"
        "/insiders — detected insider wallets\n"
        "/wallet 0x... — full wallet profile\n"
        "/portfolio — open positions by category\n"
        "/leaderboard [days] — top wallets by recent PnL\n"
        "/heatmap [days] — (signal × category) WR + PnL matrix\n"
        "/promote [confirm] — guard-railed L0→L1 promotion\n"
        "/diff — what changed since yesterday\n"
        "/explain <id> — why a signal fired / was rejected\n"
        "/live-trade <cid> <YES|NO> [$stake] — place live trade ($5 cap)\n"
        "/kill [reason] — emergency stop\n"
        "/unkill — clear emergency stop\n"
        "/help — this menu"
    )


_HANDLERS = {
    "/status": lambda args: _cmd_status(),
    "/ladder": _cmd_ladder,
    "/lanes": _cmd_lanes,
    "/pnl": _cmd_pnl,
    "/calibration": _cmd_calibration,
    "/signal-health": _cmd_signal_health,
    "/signals": _cmd_signal_health,
    "/positions": lambda args: _cmd_positions(),
    "/readiness": lambda args: _cmd_readiness(),
    "/funnel": lambda args: _cmd_funnel(),
    "/insiders": lambda args: _cmd_insiders(),
    "/wallet": _cmd_wallet,
    "/portfolio": _cmd_portfolio,
    "/leaderboard": _cmd_leaderboard,
    "/heatmap": _cmd_heatmap,
    "/promote": _cmd_promote,
    "/diff": _cmd_diff,
    "/explain": _cmd_explain,
    "/live-trade": lambda args: _cmd_live_trade(args),
    "/kill": lambda args: _cmd_kill(args or "remote-kill via telegram"),
    "/unkill": lambda args: _cmd_unkill(),
    "/help": lambda args: _cmd_help(),
    "/start": lambda args: _cmd_help(),
}


# ── Main loop ─────────────────────────────────────────────────────────────

# 2026-04-25: multi-user auth + role split. TELEGRAM_CHAT_ID accepts a
# comma-separated list of chat ids. TELEGRAM_CHAT_ID_READONLY is a
# separate optional list whose users CAN run read commands but are
# blocked from /promote, /kill, /unkill, /live-trade. Backwards-compat:
# single chat id still works as before (full control).
_CONTROL_COMMANDS = {"/promote", "/kill", "/unkill", "/live-trade"}


def _parse_id_list(raw: str) -> set[str]:
    if not raw:
        return set()
    return {p.strip() for p in raw.split(",") if p.strip()}


def _handle_callback(cb: dict, bot_token: str,
                     allowed_full: set[str]) -> None:
    """Handle inline-keyboard callback_query. Currently supports:
       block_wallet:<addr> — record manual override into wallet_blocklist
       so the ALPHA_GATE rejects future signals from this wallet.
    Read-only users are blocked.
    """
    cb_id = cb.get("id") or ""
    chat_id = str((cb.get("message") or {}).get("chat", {}).get("id") or "")
    user_id = str((cb.get("from") or {}).get("id") or "")
    data = (cb.get("data") or "").strip()

    full_ok = (not allowed_full) or (chat_id in allowed_full) or (user_id in allowed_full)
    if not full_ok:
        _tg_request("answerCallbackQuery",
                    {"callback_query_id": cb_id,
                     "text": "Control role required.", "show_alert": True},
                    bot_token=bot_token)
        return

    if data.startswith("block_wallet:"):
        addr = data.split(":", 1)[1].strip().lower()
        if not addr.startswith("0x") or len(addr) != 42:
            _tg_request("answerCallbackQuery",
                        {"callback_query_id": cb_id,
                         "text": "Bad address format.", "show_alert": True},
                        bot_token=bot_token)
            return
        r = _api_post("/api/wallet/block", {"wallet": addr,
                                            "reason": "telegram_inline_block"}) or {}
        ok = r.get("ok")
        _tg_request("answerCallbackQuery",
                    {"callback_query_id": cb_id,
                     "text": (f"Blocked {addr[:14]}…" if ok else
                              f"Block failed: {r.get('error', 'n/a')}"),
                     "show_alert": True},
                    bot_token=bot_token)
        if ok and chat_id:
            _tg_request("sendMessage",
                        {"chat_id": chat_id,
                         "text": f"⚠ Wallet `{addr[:14]}…` blocked manually. "
                                 f"Future signals from this wallet will be "
                                 f"rejected at ALPHA_GATE."},
                        bot_token=bot_token)
        return

    # Unknown callback — acknowledge so Telegram clears the spinner
    _tg_request("answerCallbackQuery",
                {"callback_query_id": cb_id, "text": "no handler"},
                bot_token=bot_token)


def _handle_update(update: dict, bot_token: str,
                   allowed_full: set[str], allowed_readonly: set[str]) -> None:
    # Inline-keyboard callback queries (from button taps)
    if update.get("callback_query"):
        _handle_callback(update["callback_query"], bot_token, allowed_full)
        return

    msg = update.get("message") or update.get("edited_message") or {}
    chat_id = str(msg.get("chat", {}).get("id") or "")
    text = (msg.get("text") or "").strip()
    if not text.startswith("/"):
        return
    parts = text.split(" ", 1)
    cmd = parts[0].split("@")[0].lower()
    args = parts[1] if len(parts) > 1 else ""

    is_control = cmd in _CONTROL_COMMANDS
    full_ok = (not allowed_full) or (chat_id in allowed_full)
    readonly_ok = chat_id in allowed_readonly

    if not (full_ok or readonly_ok):
        logger.info("ignoring msg from chat %s (not allowed)", chat_id)
        return
    if is_control and not full_ok:
        # Read-only user attempting control command
        _tg_request(
            "sendMessage",
            {"chat_id": chat_id,
             "text": f"`{cmd}` requires control role. You have read-only access.",
             "parse_mode": "Markdown"},
            bot_token=bot_token,
        )
        return

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
    allowed_full = _parse_id_list(os.environ.get("TELEGRAM_CHAT_ID") or "")
    allowed_readonly = _parse_id_list(os.environ.get("TELEGRAM_CHAT_ID_READONLY") or "")
    if not bot_token:
        logger.error("TELEGRAM_BOT_TOKEN not set")
        return 2
    logger.info(
        "[telegram-bot] starting long-poll (control: %s, readonly: %s)",
        sorted(allowed_full) or "ANY", sorted(allowed_readonly) or "—",
    )

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
                _handle_update(update, bot_token, allowed_full, allowed_readonly)
        except KeyboardInterrupt:
            return 0
        except Exception as exc:
            logger.warning("poll err: %s — backing off 10s", exc)
            time.sleep(10)


if __name__ == "__main__":
    raise SystemExit(main())
