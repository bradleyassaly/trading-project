# Path to Consistent Live Trading
*2026-04-24 — companion to ROADMAP_2026-04-24.md*

The single binding question: **what does it take for live trades to fire every day, with positive expected return, without manual intervention?**

The answer has three orthogonal layers. All three must hold simultaneously. Failing any one of them is what we did all of last week.

---

## Layer 1 — Reliability (system is up 24/7, no silent failures)

| Status | Item |
|---|---|
| ✅ | Pool keepalives + check + max_lifetime — survives idle disconnects |
| ✅ | Escalating scheduler-failure alerts (1/5/25, recovery) |
| 🟡 | Watchdog monitors `consecutive_failures` from state.json (next session) |
| 🟡 | Daily synthetic test trade — fires a $0.50 trade through the full pipeline (next session) |
| 🟡 | Weekly chaos test — kill Postgres for 60s, verify auto-recovery (next sprint) |

**Bar:** 14 consecutive days with zero scheduler-task incidents and zero scheduler-failure-grade alerts.

---

## Layer 2 — Throughput (signals → paper → live without gaps)

| Status | Item |
|---|---|
| 🔴 | Paper trade freeze (last fire 13h ago despite 328 signals/h) — diagnosing now |
| 🔴 | 0 live trades for 7 days — fixed today: widened LIVE_SIGNAL_TYPES, lowered FAV_GATE, audit logging |
| ✅ | Per-(signal, side) gate to drop the 3 broken NO combos |
| ✅ | `[LIVE_BLOCKED]` log line on every live executor rejection |
| ✅ | `[DISPATCH]` log line on every signal that reaches paper executor |
| 🟡 | Reduce live min stake to $1 (discovery-mode style) — pending paper recovery (next session) |
| 🟡 | Live executor `_record_attempt` called on every early-exit gate — currently only kill-switch path records |
| 🟡 | `/api/live/funnel` endpoint — count rejections per gate, daily report |

**Bar:** 1+ live trade fires every 48h, organically, no manual intervention.

---

## Layer 3 — Accuracy (fired trades have positive EV)

The data backing each row is the 8-day post-Apr-18 cohort.

| Status | Slice | Evidence |
|---|---|---|
| ✅ | Long-shot YES (entry <0.30) | 74 trades, 50% WR, +$67.15. Structurally favorable; 1.25× stake boost shipped |
| ✅ | wallet_reversal YES | 11 trades, 72.7% WR, +$19.74. 1.5× stake boost shipped |
| ✅ | cascade YES | 18 trades, 55.6% WR, +$12.96. 1.3× stake boost shipped |
| ✅ | whale_entry_filtered (both sides) | 1.2-1.3× stake boost shipped |
| ✅ | Excluded: copyable_contrarian, news_reactor, no_position_entry | Last fire Apr-18 |
| ✅ | Excluded: cascade NO, wallet_reversal NO, accumulation NO | Side-gate shipped today |
| 🟡 | Per-signal Kelly calibration audit — verify SignalEvaluator p_win → sizer (next session) |
| 🟡 | 30-day specialist boost re-baseline — current 7d sample showed negative lift, need n |
| 🟡 | Hypothesis accuracy ≥ 60% on 2+ signal types n≥30 (Phase 3 exit gate) |

**Bar:** Aggregate WR ≥ 50% on the next 30 resolved trades under the new gates.

---

## 7-day execution sequence

The shortest credible path from "system is up but mostly idle" to "consistent live trading" is one week. Each day has a single concrete output and a measurable check.

| Day | Output | Check |
|---|---|---|
| 0 (today) | Pool patch, escalation alerts, side-gate, stake boosts, dispatch logging | All shipped |
| 1 | Diagnose paper freeze (read DISPATCH logs) + diagnose live blocks (read LIVE_BLOCKED) | Each log has root cause + fix queued |
| 2 | Watchdog reads consecutive_failures + Telegram alert path verified | Synthetic 5-fail trips alert |
| 3 | Drop live min stake to $1; daily synthetic test trade lands | ≥1 live trade auto-fires |
| 4 | Per-signal Kelly calibration audit | Trace 1 signal end-to-end through sizer |
| 5 | First L1 bankroll raise candidate ($345 → $1,000) IF 10 live trades and ≥55% WR | Decision day |
| 6 | If raised: 24h monitoring, slippage measurement, no-incident check | Slippage p50 ≤ 2% |
| 7 | Review week. Are layers 1/2/3 all holding? Promote? | One of: hold, promote, debug |

The sequence assumes each day is unblocked by the prior. If layer 1 cracks again (e.g. another silent-failure mode), the whole week resets — that is the cost of an unattended-reliability regression and is why layer 1 has top priority despite being the boring one.

---

## What "consistently trading live" actually means (and what it doesn't)

**Does mean:**
- 1-3 live fires per day, every day, with no manual intervention
- Each fire has a logged confidence, sizing rationale, and exit plan
- Aggregate live WR + slippage tracked and reviewable in <60s
- Watchdog catches deviation from the cadence (zero fires in 24h is itself an alert)

**Does not mean (yet):**
- High volume — we're at L1 ($1K bankroll). 1-3 trades/day is correct here.
- Aggressive sizing — Kelly + 7% bankroll cap. A bad day costs <$70.
- Multiple categories at once — single-category proof first, then expand.
- Edge bigger than copy-trade-with-discipline. The gap to insiders is real and only narrows after layers 1/2/3 plus the alpha-audit improvements (earliness score, MatchOrders WS, etc.) ship in subsequent sprints.

This is the unsexy, achievable plan. The exciting plan (15 alpha bets, 5 architecture rewrites) comes after this one is operational. **An L1 system that fires 5 trades/day with 55% WR in May is worth more than a hypothetical L3 system that's still being built.**
