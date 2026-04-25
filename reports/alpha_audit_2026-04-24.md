# Alpha Audit — Where We Lose to Polymarket Insiders & Smart Money
*2026-04-24*

This is a frank, top-to-bottom look at every layer of the system and the alpha gap between us and the wallets we're trying to copy. Each section has: what we observe, the gap, and the highest-leverage fix.

The unifying frame: **prediction-market alpha is mostly latency + selection.** Insiders are right because they know first; smart money is profitable because they pick the right wallet/market combinations. We have to beat them on at least one of those axes per trade or we're paying the spread for nothing.

---

## 1. Latency to wallet activity (ingestion)

**Observed:** wallet trade freshness is 9 min (median tier-1 lag). `live-collect` listens to CLOB websockets and `wallet-stream` to Polygon USDC Transfer logs (~2-5s claimed latency). The 10-min `wallet-poller` is the fallback.

**Gap:** A whale who buys YES at $0.42 and the price drifts to $0.45 in 3 minutes has already given us a worse entry. If we're 9 min behind on detection, our average mirror entry is ~$0.02-0.04 worse than the whale's. On a 50-cent contract that's 4-8% drag *before* any thesis edge.

**Fixes (ranked):**
1. **Verify wallet-stream is actually catching every fill.** Spot-audit: take 50 known whale fills from the last 24h, check if `wallet_stream` saw them within 30s. If not, our "fast path" is decorative.
2. Add Polygon log subscriptions for **CTFExchange MatchOrders** events directly (USDC Transfer is a proxy that misses some fills). One day of work, biggest measurable win.
3. Drop poller cadence from 10min → 60s as a belt-and-suspenders for any wallet the WS misses. Free; just adjust `POLLER_INTERVAL_SECONDS`.

---

## 2. Wallet selection (intelligence layer)

**Observed:** we have 200+ tracked wallets across tier1h/tier1/tier2; `pm_leaderboard_sync` promotes wallets with PM-reported PnL ≥ $100K to tier1h. Specialist scoring exists (`wallet_alpha_scores`) but the specialist boost showed *negative* lift this week (31% WR vs 37% generalist).

**Gap:** Three issues stack here.
- (a) We trust **post-resolution PnL** as the primary tier signal, but post-resolution PnL on Polymarket has selection bias — wallets that survive long enough to resolve are visible; the unlucky-but-skilled aren't. Survivorship is real on this platform.
- (b) Our `is_copyable` flag is computed from historical category overlap, not from forward-looking "this wallet is currently early." Wallets that *were* fast-and-correct in 2025 may not still be.
- (c) We don't differentiate between **alpha (correct contrarian)** vs **beta (riding the consensus into resolution)**. A wallet that bought YES at 0.85 and resolved YES looks profitable but contributed zero edge.

**Fixes:**
1. **Add an "earliness" score per wallet**: median time-to-correct-side (i.e. wallet bought YES at price X, market resolved YES at price Y; how early relative to the consensus migration). High earliness ≠ high lifetime PnL but is much more copy-actionable.
2. **Filter wallet trades by entry-price edge**: only mirror wallets whose entries beat the closing-30min price by ≥3¢. Drops a lot of "consensus follower" noise.
3. Re-run the specialist boost ablation on a 30-day window (not a 7d sample) before keeping or removing it.

---

## 3. Signal generation (signal engine)

**Observed:** 13 distinct signal types active. Top performers by PnL this week: `wallet_reversal` (+$16.87, 48% WR), `whale_entry_filtered` (+$12.24, 50%), `market_maker_flip` (+$13.34, 33%), `cascade` (+$11.04, 48%). Worst: `network_leader_entry` (−$5.43), `copyable_contrarian` (−$3.81), `news_reactor` (−$2.36).

**Gap:** We're firing too many signal types from the same underlying wallet event, leading to:
- (a) **Concentration risk** — a single whale buy can trip `whale_entry_filtered` + `oversized_bet` + `cascade` + `network_leader_entry` simultaneously, all entering the same position. We pay 4× the spread for 1× the edge.
- (b) The signal engine doesn't yet differentiate between **first-mover** and **echo** signals when multiple types fire on the same wallet/market within a short window.
- (c) Hypothesis accuracy on `whale_entry` raw is **70%** (n=10) — much better than the filtered version (58%). The filter may be over-aggressive.

**Fixes:**
1. **Signal deduplication window**: when ≥2 signals fire on the same (market, side) within 60s, take the highest-confidence one only. Hard cap at 1 entry per market per 30min.
2. **Re-introduce raw `whale_entry`** as a discovery-stake signal type, run it parallel to `whale_entry_filtered` for 14 days, A/B the WR/PnL.
3. **Drop `network_leader_entry`** from live (paper-only) until it can demonstrate ≥40% WR; current 25%/30% is below random.
4. **`cascade` is undervalued** — 48% WR, +$11. Promote to higher confidence boost.

---

## 4. Sizing & confidence (Kelly + boosts)

**Observed:** KellySizer reads from bankroll cache; specialist boost gives 1.25× confidence + 2× stake when source wallet is a category specialist. This week: avg trade size unknown without DB query, but 56 exits drained × ~$1-5 each ≈ <$300 risked.

**Gap:** Stakes are uniformly small ($1-5 paper) regardless of signal quality. We're not differentiating between a 70%-accuracy `whale_entry` (high conf) and a 25%-accuracy `network_leader_entry` (low conf). Fractional Kelly with a calibrated win-prob estimate would 3-5× the PnL on the good signals without scaling losses on the bad.

**Fixes:**
1. **Per-signal confidence calibration**: after every 30 resolved hypotheses for a signal type, re-estimate p_win and update the Kelly fraction. The infrastructure exists (`SignalEvaluator` + calibration_rebalance task) — verify it's actually feeding the sizer.
2. **Suppress sizing on signal types with n<30 resolved trades**, or stake $1 minimum. We don't have evidence yet, so bet small.
3. Add a **per-category PnL stop**: if a category goes -$50 in a 7d window, halve all stakes there for 7 days.

---

## 5. Execution & exits

**Observed (this week):** 54 stop_loss (-$50.11), 54 take_profit (+$98.13), 45 whale_mirror_exit (+$17.50), 24 market_life_expired (-$1.73). Take_profit:stop_loss ratio of $1.82 : $0.93 ≈ 2:1.

**Gap:**
- (a) The 2:1 TP/SL asymmetry is doing real work — but stop_loss still bleeds $50 in a 8d window, suggesting the SL threshold is wrong or the entry quality has too much variance.
- (b) `whale_mirror_exit` is +$17.50 / 45 trades = $0.39/trade. **This is the most promising exit type** — when the whale we copied themselves sells, we sell. Underexploited.
- (c) `market_life_expired` is essentially a coin flip — markets we held to expiry. We should be exiting these earlier.

**Fixes:**
1. **Tighten time-decay exit**: any open position with <8h to resolution and unrealized PnL between -10% and +5% should auto-close. Don't ride the resolution lottery.
2. **Audit stop_loss triggers**: which signal types contribute most to the −$50? If it's network_leader_entry/copyable_contrarian, those are already on the cull list; if it's a "good" signal type, the SL threshold is wrong for that type.
3. **Mirror-exit propagation latency**: when the whale sells, how fast do we sell? Same question as section 1 but on the exit side. If we lag 9 min on exit, half of our +$0.39/trade evaporates.

---

## 6. Live execution gating (the 0-live-trades problem)

**Observed:** zero live trades fired in 7 days despite the expanded category list. The single live exit Apr-18 was the trailing position from before the run.

**Gap:** The live executor's gates (Kelly ≥ $X, confidence ≥ Y, specialist tag, category in allowlist) are intersecting to zero approvals. This is the most concerning finding of the week — the entire upstream pipeline is decorative if nothing graduates to live.

**Fixes:**
1. **Run `/api/live/funnel` daily** and log how many signals are dropped at each stage. Target: ≥1 live trade per 48h, even if size is $1.
2. **Lower live-trade min stake to $1** and start firing — the goal at L0/L1 is *learning*, not *protecting capital we don't have at risk yet*.
3. Most likely fix: **specialist tag is required AND no signals are tagging specialist** — verify by running the gate logic against last week's signal stream offline.

---

## 7. Hypothesis scorecard / thesis validation

**Observed:** 39% accuracy on 164 resolved hypotheses (7d). Random ≈ 50%; we're below.

**Gap:** Two things may be conflating here.
- (a) Many of those hypotheses resolved while exits weren't being checked (the freeze) — so trades that *would* have hit take_profit and locked in a "correct" hypothesis instead held to a stop_loss or expiry, biasing accuracy down.
- (b) Even discounting the freeze, 39% is close enough to chance that we may have very little signal in the bottom-half signal types.

**Fixes:**
1. **Re-resolve hypotheses for the post-drain trades** and recompute accuracy on the trades that exited cleanly today.
2. **Filter hypothesis accuracy by signal type with n ≥ 30** — anything less is statistical noise.
3. The thesis scorecard should report **two numbers**: aggregate accuracy AND median accuracy across signal types weighted by trade volume. The aggregate alone is too coarse.

---

## 8. Data infrastructure (foundations)

**Observed:** Postgres + bind-mounted code is working. Backups daily 7-rotation. Pool patched today. Watchdog runs but didn't catch the freeze (it monitors live_collect/wallet_stream heartbeats, not scheduler-task health).

**Gap:** Resilience under unattended operation is unproven. We just learned that a single class of failure (idle disconnects) silently took out half the system for 5 days. There are likely 2-3 more silent-failure modes lurking.

**Fixes:**
1. **Chaos test**: weekly, deliberately kill Postgres for 60s and verify recovery is automatic across all services.
2. **Watchdog must monitor scheduler-task health** — extend `check_scheduler` to read `consecutive_failures` from state.json and alert if any task >5.
3. **Daily synthetic test trade** — fire a $0.50 paper trade through the full pipeline (signal → hypothesis → exit) and alert if it doesn't resolve within 24h. Detects pipeline gaps faster than waiting for organic flow.

---

## Prioritized backlog (highest-EV first)

| # | Bet | Why | Effort |
|---|---|---|---|
| 1 | Audit & fix the 0-live-trades gate intersection | Blocking everything past L1 | 1 day |
| 2 | Add wallet "earliness" score | Best single proxy for true alpha | 3 days |
| 3 | Signal dedup + raw `whale_entry` re-introduction | 70% WR on n=10 deserves more shots | 1 day |
| 4 | Per-signal Kelly calibration audit | 3-5× PnL on good signals | 2 days |
| 5 | Direct CTFExchange MatchOrders subscription | Cuts WS latency materially | 1 day |
| 6 | Time-decay exit tightening | Stop renting expiry variance | 0.5 day |
| 7 | Drop `network_leader_entry` from live | Already underperforming | 30 min |
| 8 | Daily synthetic test trade + scheduler-task watchdog | Catches the next silent failure | 1 day |

Top 4 alone, if executed in the next two weeks, take us from "promising paper performance" to "first defensible live edge." The rest unlock L2+.
