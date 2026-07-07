# Pre-Registered Kill Rule: Mirror-Exit Copy Trading

**Registered:** 2026-07-07 (before running the diagnostic — this is the point).
**Author:** system owner + Claude (roadmap item N6 / top-5 #3).
**Registering commit:** the commit that adds this file and `mirror_copy_backtest.py`.

## Why this exists

Three independent pieces of reconciled evidence already point the same way:
zero wallets are copyable **hold-to-resolution** (best candidate −$0.058/trade
ex-top-3), calibration was worse than a coin flip, and exactly one signal
(`resolution_decay`) earns money live. The roadmap's sharpest finding is that
~35 of ~50 improvement proposals are conditionally worthless on a single
measurement nobody had scheduled: **is any wallet copyable once you model
mirror exits (not just hold-to-resolution) with a realistic latency haircut
and net of cost?**

Writing the stopping rule down *before* seeing the number is the whole point.
It converts a research program into something other than a sunk-cost machine.

## The one measurement

`python -m trading_platform.polymarket.mirror_copy_backtest --days 60`

It simulates copying each qualifying wallet's first BUY per market at a fixed
$5 stake, then **exiting when the leader exits** (their next SELL on that
market), falling back to hold-to-resolution only if they never sold before
resolution. It reports results in two **separate, never-blended** latency
cohorts — fast-lane (2–4 s, chain-direct) and poller (~5 min p50, the current
production reality) — and nets out a configurable round-trip cost.

The headline metric is the **top-decile cohort net avg PnL per trade under the
POLLER latency cohort** (the conservative lane, because live p50 is ~5 min per
`project_2026_07_05_overnight_check`). Top decile = rank qualifying wallets
(n≥30, WR≥55%, ex-top-3 EV>0) by ex-top-3 EV, take the best 10%.

## The rule (committed before the number)

- **KILL / stay-dead:** if the poller-cohort top-decile net avg PnL per trade is
  **≤ +$0.10/trade**, live copy-**entry** is formally retired within 30 days.
  The wallet graph survives ONLY as a feature source for the resolution engine
  (e.g. "is smart money net-long this near-resolution market"). Every
  [COPY]-entry roadmap item (leader-conviction sizing, forward-edge model,
  lead-lag alpha) is shelved until this measurement flips.
- **KEEP / investigate:** if it is **> +$0.10/trade** AND the fast-lane cohort
  is at least as good (so latency isn't carrying it), the mirror-exit copy lane
  is worth a shadow-mode pilot — but still gated by the standard promotion
  ladder (event-clustered n, FDR, net-of-cost), not turned live directly.

The +$0.10/trade floor (not $0.00) matches the existing naive-copy scheduler
threshold and leaves margin for costs the model under-counts (depth-limited
fills, the 33-min data-api indexing lag on the entry reference price).

## Sample-size guard

The verdict is only valid with **≥ 5 qualifying wallets** in the top-decile
cohort and **≥ 150 total simulated copies** in the window. Below that, the
result is "insufficient data — re-run when more history has accrued," not a
kill. Widen `--days` before concluding.

## What a clean "kill" means

A pre-registered kill that retires copy-entry is a **success**, not a failure:
it frees the harness, the latency lane, and the calibration loop to concentrate
on the one engine that works, and it stops the system polishing a −EV lane.

---

## First run — 2026-07-07 (60d window)

Executed immediately after registering the rule above.

- **7,332 copies simulated across 445 wallets — 100% exited via mirror-SELL,
  0% held to resolution.** This is the load-bearing finding: the copyable
  wallets are *scalpers* (they sell before resolution), which is exactly why
  the hold-to-resolution `naive_copy_backtest` found nobody — it measured the
  wrong exit. The mirror frame is the right frame.
- **14 wallets qualify (fast-lane), 11 (poller)** — up from ZERO under
  hold-to-resolution.
- Poller top-decile net EV/trade: **+$6.82** (fast-lane +$7.27).

**Verdict: NO KILL — but NOT a KEEP-to-live either.** The pre-registered
sample guard fires: the qualified top-decile is only **1 wallet** (< 5 floor),
so the +$6.82 headline is not statistically trustworthy. And the number is
implausibly high (+136%/trade on a $5 stake) because the diagnostic fills at
the leader's exact entry AND exit prices with only a tiny latency slip — it
does **not** model exit fill-probability on thin books (the audit showed these
markets have collapsing near-resolution liquidity) or depth-limited fills. Both
inflate the result.

**Actions triggered by this run:**
1. Do NOT retire copy-entry (the naive-copy "kill" conclusion is overturned —
   the wallet graph is more than a feature source after all).
2. Re-run with a wider window (`--days 120`) to grow the qualified top-decile
   past the ≥5 / ≥150-copy guard before trusting the magnitude.
3. Add exit fill-probability + depth modeling to the diagnostic before any
   number is used to size real capital (a "did a taker cross the leader's sell
   price within our latency window" check).
4. If it survives (2)+(3), a **shadow-mode** mirror-copy pilot on the top-decile
   cohort — logged, not funded — gated by the standard promotion ladder
   (event-clustered n, FDR, net-of-cost). Never live directly off this diagnostic.

The rule did its job: it converted "should copy-trading exist?" from a vibe
into a measured, guard-railed decision, and the measurement said "keep
investigating, with named next steps" rather than either extreme.

---

## Amendment 1 — registered 2026-07-07, BEFORE the C1+C2 decision run

Recon of the shipped diagnostic found three defects that must be fixed and
re-registered before any number is trusted:

1. **Survivorship drop (invalidates the run-1 headline).** Rows whose leader
   never sold AND whose markets-join payout was unresolvable were silently
   skipped: 43,082 eligible (wallet, market) first-BUY pairs at 60d vs 7,332
   simulated — **83% of copies dropped**, all of them never-sold rows
   (disproportionately hold-to-resolution losers). The "100% mirror-exit /
   everyone is a scalper" finding was an artifact of the resolution join
   failing, not a market fact. Fix: payout falls back to the sign of the
   leader's enriched pnl (`pnl > 0 → 1.0 else 0.0`, valid because
   enrich_resolution sets pnl = size·(1−price) if token won else −size·price,
   and the fetch already filters pnl_reliable=1). **Zero copies are dropped
   in the amended diagnostic; run-1/run-2 numbers are NOT comparable to the
   amended run and the headline is expected to FALL.**
2. **Cross-token exits.** The sell-lookup matched wallet+market+side without
   matching the token — a leader selling the market's *other* token supplied
   an exit price in the wrong token space. Fixed with `s.asset = f.asset`.
3. **No fill-probability model.** Amended exit waterfall, per leg:
   - Evidence = the best taker print (market_ticks, 6.5M rows / ~90d) or any
     other wallet's BUY on the same token within
     [sell_ts + latency, sell_ts + latency + FILL_WINDOW_S (default 900s)].
   - `mirror_confirmed` — evidence ≥ leader's sell price − 0.01: exit at the
     latency-slipped leader price.
   - `mirror_degraded` — evidence exists but lower: exit at the evidence
     price (we'd have sold into what actually traded).
   - `unfilled` — market has tick coverage and NOTHING traded in the window:
     the mirror sell does not fill; the copy books resolution payout instead.
   - `mirror_assumed_nocoverage` — no tick coverage for the market at all:
     assume the fill under `--no-evidence-policy assume-fill` (default) or
     book resolution under `resolution`. Reported separately so the
     assumption's weight is visible.
4. **Costs ON by default** via cost_model.CostModel (spread + size-tier
   slippage; resolution exits exempt). MIRROR_COST_BPS env still overrides.

**Amended decision procedure (registered before the run):**
- Primary metric unchanged: poller-lane top-decile net EV/trade vs the
  +$0.10 floor, guard ≥5 top-decile wallets AND ≥150 copies.
- Per-cluster (category) verdicts with guard ≥3 top-decile wallets AND ≥75
  copies per cluster: **KILL only if the global metric is ≤ +$0.10 AND no
  measurable cluster clears +$0.10**. KEEP-investigate if global OR any
  measurable cluster clears the floor. INSUFFICIENT only if the global guard
  fires and no cluster is measurable.
- Decision run: `--days 120`. A `--perfect-fill` flag reproduces run-1
  behavior for comparability; it is diagnostic-only and cannot drive the
  verdict.

## Amendment 2 — registered 2026-07-07, before the 120d decision run

**Disclosed motivation:** the 60d perfect-fill sanity run (run after
Amendment 1) showed 42,665 copies / 223 wallets at min_n / **0 qualified
wallets** — under the letter of the rule this is "INSUFFICIENT" (no
top-decile to measure), which misreads overwhelming negative evidence as
absence of data. The guard was written to protect against trusting a
too-small top-decile mean, not to make an all-negative universe unfalsifiable.

**Amended clause (applies to the 120d decision run and after):** if the
poller lane has **≥ 10,000 total copies AND ≥ 100 wallets at min_n AND 0
qualified wallets**, the verdict is **KILL** (no copyable cohort exists in
the measured universe), not INSUFFICIENT. All other semantics unchanged.
A KILL under this clause carries the same consequences as §"The rule":
retire live copy-entry; the wallet graph survives as a feature source.
