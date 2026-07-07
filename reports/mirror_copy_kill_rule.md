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
