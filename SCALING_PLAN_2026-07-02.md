# Scaling & Alpha-Generation Plan — 2026-07-02

Successor to `ROADMAP_2026-04-24.md`. That document planned the path from
Phase 3 to L5. This one starts from a full project evaluation (code, git
history, reports) as of today and re-plans the path to **consistent,
scalable profit from whale behavior** — the goal in `THESIS.md`
($10–20K/month at L5).

---

## Part 1 — Evaluation: where the project actually is

### 1.1 What is genuinely strong

- **The intelligence stack is real and deep.** ~140 Polymarket modules:
  three-path detection (Polygon wallet-stream ~2–5s, CLOB WS, 10-min
  poller safety net), 16K+ tracked wallets, per-(wallet, category) alpha
  scores with bootstrap-CI copyability, farmer/sybil gating, archetype
  and earliness scoring, per-slice EV tables, isotonic calibration,
  Bayesian signal evaluation.
- **Risk plumbing is layered and tested by fire.** Kill switch → daily
  loss cap → 20% cumulative circuit breaker; stake ladder with symmetric
  demotion; topic-concentration caps; discovery-mode $1 stakes for
  unproven slices. These caught real incidents.
- **The feedback loops exist.** Auto tier promotion/demotion for wallets,
  signals, and slices; nightly on-chain reconciliation; hypothesis
  scorecard; synthetic daily test trade. Most shops never build these.
- **Live fire is proven mechanically.** 156+ real trades through the CLOB
  at $1–5 stakes. The pipes work end-to-end.

### 1.2 What the evidence actually says (candid)

| Claim (THESIS.md) | Bar | Reality (from git history + reports) |
|---|---|---|
| 1. Persistent edge exists | cohort WR > 55% | ✅ 77.9% WR on 61K clean historical trades — but this is *retrospective selection*, not forward proof |
| 2. Edge is category-specific, ≥2 categories | ≥2 categories positive | ❌ single-category-dominant (sports); geopolitics/other negative |
| 3. We can identify who has edge | copyable cohort outperforms | 🟡 selectivity in band (24.6%); forward test (Claim 7) started 04-25, result unrecorded |
| 4. We can copy in real time | ≥70% hypothesis accuracy on 50+ | ❌ 47% on 17, then 39% on 164. Phase 3 was **soft-bypassed**, not passed |
| 5. Edge survives costs | positive live EV | 🟡 claimed +$0.31/trade on 156 trades — **but** the 05-27 audit found ~$103 of fictitious realized P&L over 90d, so headline live P&L is not currently trustworthy |
| 6. Edge scales with capital | untested | ⏳ untested (expected — gated) |

Three further findings from the last 60 days of history:

1. **The strategy quietly pivoted and the docs never caught up.**
   2026-05-12: all BUY signals suspended (SELL-only mode). By 06-01 the
   two revenue lanes are `resolution_decay` "lottery-ticket" SELLs at
   NO ≤ 5¢ (n=78, WR 69%, +$143 realized — the one clean positive lane)
   and `whale_entry_filtered`. That is **only half a whale-copy thesis**:
   the profitable lane is a market-structure signal (resolution decay),
   not wallet-following. THESIS.md, ROADMAP.md and WALLET_INTELLIGENCE.md
   all still describe the pre-pivot system.
2. **Data integrity has been the #1 source of losses and false beliefs.**
   Fictitious take-profit closes (orders resting, not filled), stale
   shares columns (bit 3 times in 7 days), zero-balance P&L bugs,
   auto-demote firing on market-outcome losses and halting the top
   revenue signal for 72h. Every calibration, Kelly fraction, and
   promotion decision downstream consumes these contaminated outcomes.
3. **Operational fragility caps everything.** The week-long unattended
   run died at hour 6 (pool leak); scheduler tasks failed silently;
   collectors ran 21–26 days stale per the 05-02 briefing; no commits
   since 06-01 and the current run state is unknown. A system that needs
   a human rescue every ~2 weeks cannot hold $25K+ of open positions.

### 1.3 The binding constraints, ranked

1. **Trust in the P&L layer** — you cannot promote capital on numbers
   you know are inflated.
2. **Signal sprawl** — ~20 signal types firing from the same underlying
   whale events; two lanes make money, the rest dilute capital, spread
   costs, attention, and statistics.
3. **Detection latency** — 9-min median tier-1 lag vs the 2–5s design
   goal. For copy-trading, latency *is* the edge; at 9 minutes we buy
   the whale's exit liquidity.
4. **Statistical starvation** — at $5/trade and one dominant category,
   gates need months to accumulate n≥30 per slice.
5. **Single-category concentration** — sports-only edge fails Claim 2
   and will not survive past L3.

---

## Part 2 — The plan

Design principle: **one falsifiable milestone per phase, capital follows
evidence, and every number promoted must be on-chain-verifiable.**

### Phase 0 — Ground truth reset (weeks 1–2) 🔴 blocking

Nothing else matters until the books are trusted.

- **On-chain-first P&L rebuild.** Extend `scripts/validate_realized_pnl.py`
  into the canonical ledger: every closed trade's realized P&L recomputed
  from on-chain conditional-token balances + USDC flows. DB `realized_pnl`
  becomes a cache of this, never a source. Manually resolve the 63 known
  fictitious closes (`--apply` after checking resolution state).
- **One daily truth report.** Replace the dead Stack-B
  `daily_system_report` artifact (currently all-inputs-missing) with a
  Polymarket-native report: on-chain equity, per-signal per-slice EV from
  reconciled outcomes only, gate funnel counts, scheduler/collector
  heartbeats. This is the single document promotions read from.
- **Re-derive per-signal EV on the clean ledger** → go/paper/kill verdict
  for each of the ~20 signal types. Expect most to die.
- **Restart & watchdog hardening.** Bring services back up; extend the
  watchdog to scheduler-task consecutive-failure counts and collector
  freshness (the two known silent-failure classes); weekly chaos test
  (kill Postgres 60s, verify auto-recovery).
- Repo hygiene: fix the `tests/test_cli_grouping.py` collection error
  (imports deleted `trading_platform.artifacts`), archive Stack B
  (equity/Alpaca pipeline) under `legacy/` or a branch — it is skipped
  in tests and off-roadmap, but still costs comprehension.

**Exit gate:** 14 consecutive days where on-chain equity and DB equity
agree within $1, zero silent task failures, and a per-signal EV table
computed from reconciled outcomes only.

### Phase 1 — Consolidate to a provable book (weeks 2–4)

Run a **two-lane live book + one discovery lane**, everything else
paper-only:

- **Lane A — `resolution_decay`** (market-structure alpha). The one lane
  with clean positive live evidence (69% WR, +$143 on 78). Scale it
  first: widen candidate pool carefully, keep the fp=0.99 lottery-SELL
  gate as-is (the 06-01 audit explicitly warns not to tighten it).
- **Lane B — `whale_entry_filtered`** (the actual whale-copy thesis).
  Keep live at ladder size, but instrument it to answer *why* accuracy
  fell from 70% (raw, n=10) to sub-50%: log per-trade detection latency,
  entry-price slippage vs whale's entry, and whale's subsequent exit.
- **Lane C — discovery ($1 stakes):** at most 2 candidate signals at a
  time (start: `whale_exit` mirror — best per-trade exit economics in the
  04-24 audit — and `specialist_entry`, IC30=+0.05). Fixed 30-resolution
  audition, then promote or kill. All other signal types: paper or off.
- **Per-lane bankroll accounting** (from `scale_up_roadmap_2026-04-27.md`
  item 3): each lane has its own ladder rung, so Lane A scaling is not
  hostage to Lane B statistics.

**Exit gate:** 30+ reconciled live resolutions per live lane; lane EV
positive after costs; median slippage ≤2%.

### Phase 2 — Whale-alpha sharpening (weeks 3–8, overlaps Phase 1)

This is where "profit from whale behaviour" is actually won or lost.
Ranked by expected lift (carried over from the alpha audit, still unshipped):

1. **Latency: direct CTFExchange `OrderFilled`/MatchOrders log
   subscription** on Polygon (USDC-Transfer is a lossy proxy). Target:
   p50 whale-fill → our-order ≤ 10s, measured per trade. Also audit the
   existing wallet-stream: sample 50 known whale fills, verify detection
   ≤30s; drop poller cadence 10min → 60s as backstop.
2. **Earliness-gated copying.** Only mirror wallets whose entries beat
   the closing-30-min consensus price by ≥3¢ historically
   (`wallet_earliness.py` exists — wire it into the copy gate). This
   kills "consensus rider" wallets that look profitable but carry no
   copyable edge.
3. **Mirror-exit as a first-class rule.** When the copied whale exits,
   we exit — measured exit latency, same instrumentation as entries.
   Best per-trade exit economics observed (+$0.39/trade on 45).
4. **Crowding detection.** Use `wallet_copy_graph.py` to estimate how
   many followers a whale has; discount or skip heavily-copied whales
   (we'd be the marginal late copier). This is also the early-warning
   system for Claim 6 (edge decay under crowding).
5. **Per-slice calibrated Kelly.** The isotonic layer shows systematic
   overconfidence (Brier 0.35). Kelly fractions must come from the
   *reconciled* per-slice posterior, refit at 6h cadence, with n<15
   slices pinned to discovery stakes.

**Exit gate:** `whale_entry` lane hypothesis accuracy ≥60% on 50+
reconciled resolutions with p50 latency ≤10s — or a documented decision
that wallet-copying is dead and the book pivots to market-structure
lanes (that outcome is acceptable; renting the whale thesis forever is not).

### Phase 3 — Ladder climb L1 → L3 (months 2–4)

Resume the `THESIS.md` ladder, with two amendments:

- **Bayesian early-stop gates** (posterior P(edge ≥ threshold) > 0.95)
  instead of fixed-n thresholds — 30–50% faster promotions at equal
  safety.
- **Promotion reads only the daily truth report.** No promotion on any
  number that hasn't survived on-chain reconciliation.

| Rung | Bankroll | Gate (reconciled, per lane) |
|---|---:|---|
| L1 → $1,000 | after Phase 1 exit | 30+ resolutions, EV>0, slippage ≤2%, 14d zero-incident ops |
| L2 → $5,000 | +4–6 weeks | 50+ resolutions, **2+ categories** (or 2+ independent lanes) positive |
| L3 → $25,000 | +6–8 weeks | 150+ resolutions, Sharpe ≥1.0, DD <10%, slippage flat vs L2 (first real Claim-6 test) |

Capacity honesty at each rung: measure fillable depth at target size on
the actual traded markets *before* funding the rung. If Lane A's ≤5¢
lottery book can't absorb $150 orders, the lane caps out and the plan
needs Lane B/D scaled instead — better to know at L2 than at L4.

### Phase 4 — Diversification & new lanes (months 3–6, gated ≥L2)

Claim 2 requires ≥2 independent edges. Candidates in priority order:

1. **Second category for the whale lane** (politics/geopolitics have the
   richest insider structure; `insider_detector.py` backtested +0.283 EV).
2. **Order-flow imbalance** graduation (already firing informationally;
   promote to discovery stakes if 30d IC > 0.1).
3. **Cross-platform Kalshi↔PM divergence** (skeleton exists) — only at
   L2+, per the existing "don't add second-platform variance while
   uncalibrated" rule.

L4/L5 remain as specified in `THESIS.md` — no changes proposed until L3
data exists.

---

## Part 3 — Operating rules (standing)

1. **No number is real until reconciled on-chain.** Applies to P&L, WR,
   EV, calibration inputs, and promotion gates.
2. **Max 3 live lanes, max 2 discovery auditions at a time.** New signal
   ideas queue; they don't dilute.
3. **One variable per promotion** (bankroll OR signal set OR category
   gate), unchanged from the 04-24 roadmap.
4. **Symmetric demotion, but only on strategy-quality losses** — the
   05-31 fix (exclude `resolved_zero_balance`/`time_decay` from demote
   counts) becomes policy everywhere a WR gate exists.
5. **Docs follow the book.** Any live-whitelist change same-day updates
   THESIS.md's "current evidence" fields. The 7-week doc/reality gap
   found in this evaluation is itself an operational defect.
6. **Kill criteria stay armed.** If after Phase 2 the whale lane can't
   reach 60% on 50+ reconciled resolutions, the whale-copy thesis is
   rejected per THESIS.md's own framework — the platform then lives on
   market-structure alpha, which is a fine business but a different one.

## Part 4 — First 10 working days (concrete)

| Day | Ship |
|---|---|
| 1 | Restart stack; verify collectors/scheduler heartbeats; snapshot on-chain balances |
| 1–2 | Run `validate_realized_pnl.py` over full history; triage 63 fictitious closes |
| 2–3 | On-chain-first ledger rebuild + DB backfill; equity agreement check begins (14-day clock) |
| 3–4 | Daily truth report v1 (equity, per-lane EV, funnel, heartbeats) replacing dead daily_system_report |
| 4–5 | Per-signal EV verdicts on clean ledger → kill/paper/live list; shrink live whitelist to Lanes A+B |
| 5–6 | Watchdog: scheduler consecutive-failures + collector freshness alerts; first chaos test |
| 6–8 | Latency instrumentation on Lane B (whale fill ts → our order ts, per trade) |
| 8–10 | CTFExchange event subscription spike; wallet-stream 50-fill audit; fix `test_cli_grouping` collection error |

**Next review: 2026-07-16** — diff against this file, same convention as
prior roadmaps.
