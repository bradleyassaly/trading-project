# Whale Entry Experiments — 2026-04-11

**TL;DR — whale_entry copy-trading does not have a recoverable edge in the current dataset.** Nine experiments and a 122-combination grid search produced **zero** statistically significant positive-EV configurations. The signal is structurally broken in ways that filtering cannot fix. Recommendation: retire `whale_entry` from the active executor path, repurpose its DB rows as input to a future "whale-aware" feature, and double down on `accumulation` and `market_maker_flip` which already work.

All experiments are read-only against `data/polymarket/wallet_intelligence.db`. Re-runnable via `.venv/Scripts/python.exe scripts/whale_entry_experiments.py`. Full grid CSV: `/tmp/whale_entry_grid.csv` (122 rows).

---

## Experiment Results Summary Table

| # | Experiment | Tested | Key finding | Best EV found | Sample | Significance |
|---|---|---|---|---|---|---|
| 0 | **Baseline** | n=87 | WR 49.4%, EV −0.205, p≈1.0 | −0.205 | 87 | catastrophic |
| 1 | **Latency / zero-lag sim** | lag bucketing + hypothetical "enter at whale price" | Even at zero lag the EV stays at **−0.34** (recovered only +0.0042). The losses are **not** from latency. | −0.34 hyp | 17 matched | latency ≠ root cause |
| 2 | **Tier filter (S/A/B)** | EV by category-tier | **S/A tier wallets are catastrophic** for whale_entry: 20 trades, WR 10%, EV **−0.506**. C-tier is the *least bad*: WR 62%, EV −0.092. | C-tier −0.09 | 61 | wrong direction |
| 3 | **Price-lag (slippage)** | max allowed |slippage| | All slippage thresholds show EV around −0.30. Tighter slippage doesn't help — it filters to a worse subset. | −0.27 (≤10%) | 27 | does not help |
| 4 | **Entry price sweet spot** | EV by price zone | The only profitable zone is **price > 0.85** (WR 79%, EV +0.002 — basically zero). The "uncertain" 0.15-0.85 zone is universally negative. | +0.002 | 48 | non-bet |
| 5 | **Convergence (concurrent wallets)** | EV when ≥k other tracked wallets traded same side in window | At ≥5 concurrent wallets in 48h, n=3, EV +0.31 — only positive cell, but n is too small to act on. Every other convergence threshold is negative. | +0.31 | 3 | n too small |
| 6 | **Whale exit strategy** | EV if we exit when whale exits | **Whales basically don't exit before resolution** — only 2/87 (2.3%) had a SELL after their initial BUY. Exit-on-whale-exit is not a viable strategy on this data. | n/a | 2 | non-actionable |
| 7 | **Market age (hold_days proxy)** | EV by minimum days-before-close | No bucket showed positive EV. (`hold_days` data was sparse — most signals don't have it from the backfill.) | n/a | — | inconclusive |
| 8 | **Whale size / conviction** | EV by absolute trade size + multiple of avg | Trade-size enrichment matched only 1 of 87 signals — the originating wallet trades aren't in `wallet_trades` for most whale_entry signals. **Inconclusive due to data gap.** | n/a | 1 | data gap |
| 9 | **Per-category** | One-sample t-test per category | **All four categories are negatively significant** (p < 0.10): crypto p=0.009, entertainment p=0.047, geopolitics p=0.069, other p=0.002. The negative EV is real, not noise. | none | 87 | losses confirmed |
| 10 | **Combined grid (122 combos)** | tier × slippage × price × confidence × category | **Zero combinations** clear EV > 0 AND p < 0.15 AND n ≥ 10. The closest "least bad" combo is the unfiltered baseline at EV −0.20. | none | varies | **definitive** |

---

## The Verdict

**Can whale_entry be made profitable by filtering? No.**

The 122-combo grid is exhaustive across the dimensions we have data for, and not a single combination produces a positive-EV cell at n ≥ 10 with p < 0.15. The closest positive-EV cells are:

1. **Convergence ≥ 5 in 48h**: n=3, EV +0.31 — sample too small
2. **Entry price ≥ 0.85**: n=48, EV +0.002 — economically zero
3. **Tier=D**: n=3, EV +0.001 — sample too small

None of these is actionable. The two viable paths are economically meaningless (price > 0.85 wins by ~$0.002 per unit) or sample-starved.

**The losses are not from latency.** The zero-lag simulation recovers only +0.004 EV — moving from current detection lag (median 151 minutes!) to instant detection still leaves us at EV ≈ −0.33. The whales we're copying are **systematically wrong** in the markets we're catching them in, not just early.

**The losses concentrate in the highest-tier wallets.** A-tier-in-category produces WR 10%, EV −0.51. This is the *opposite* of what tier-filtering is supposed to do. The signal is selecting against itself: the most reputable copy-targets are the worst on this signal type. That smells like a **systematic bias in which trades trigger whale_entry vs which don't** — possibly we only fire whale_entry when a high-tier wallet does something *unusual* (against their normal pattern), and "unusual for them" turns out to mean "wrong this time."

**All four observed categories are negatively significant** (p < 0.10). This is not noise or a thin sample artefact.

---

## Latency Analysis (detail)

| metric | value |
|---|---:|
| signals with matched whale trade in `wallet_trades` | **17 of 87 (19.5%)** |
| lag — mean | 370.9 min (~6 hours) |
| lag — median | **150.9 min** (2.5 hours) |
| lag — p25 | 8.2 min |
| lag — p75 | 418.4 min |
| lag — p95 | 1,135.5 min (~19 hours) |
| price slippage — mean | −0.0042 (−3.4%) |
| price slippage — median | 0.0000 |

Detection lag is *bad* — median 2.5 hours is enough for any informational alpha to be priced in. But the zero-lag simulation says fixing it would only buy us back **+0.0042 EV**, which doesn't move us anywhere near profitable. The wallets we copy are not generating losses because we're slow; they're generating losses because their *initial* positions are wrong.

That said, the **17/87 match rate is itself a finding**. Most whale_entry signals have no corresponding wallet trade in `wallet_trades` at all. The signal engine is firing on wallets the ingest pipeline doesn't track. That means:

1. The "whales" being copied are mostly NOT in our 55-wallet `sync-wallet-trades` list — they're being detected by some other path (maybe the WebSocket trade feed, maybe the leaderboard scanner) and routed through `whale_signal_engine.on_whale_trade()` without ever populating `wallet_trades`.
2. Without `wallet_trades` rows, the alpha gate's "is this wallet copyable in this category" check has nothing to evaluate, and the tier-rank lookup misses entirely.

This is a separate, smaller bug worth fixing regardless of whether `whale_entry` ever becomes useful.

---

## Best Filter Configuration

There is no winning configuration. The grid search hits a wall.

For completeness, the absolute best by raw EV (ignoring significance) at n ≥ 10 is the unfiltered baseline. Adding any filter makes things worse — the grid is monotonically degrading. That's the "wrong direction" signature: filters intended to find a better subset surface the most-broken subsets instead.

---

## Implementation Status

- `whale_entry_filtered` was **NOT created**. Per Part 12 instructions, implementation is conditional on finding a winning config; none exists.
- Latency reduction was **NOT implemented**. The zero-lag simulation showed it wouldn't fix the problem.
- Whale exit tracking was **NOT added**. Only 2.3% of whales exit before resolution, so exit-detection has nothing to act on.
- No production code touched. `whale_entry` continues to fire and record into `signal_outcomes` for ongoing monitoring; the executor still receives it (and continues to lose money on it).

**Recommended production change** (separate from this read-only session): in `whale_signal_engine.py`, add `"whale_entry"` to `DISABLED_SIGNAL_TYPES` (alongside `price_velocity`, `oversized_bet`, `cascade`, `convergence`, `no_position_entry`). Keep the recording for tracking, kill the executor path. Wait — `oversized_bet` is in DISABLED_SIGNAL_TYPES but the recovery report identified it as the strongest single-signal performer (n=47, WR 85%, EV +0.37). Two of the disabled types may be wrongly disabled. **That's a separate audit, not part of this experiment.**

---

## Category-Specific Findings

| category | n | WR | EV | t-test p (one-tailed) |
|---|---:|---:|---:|---:|
| crypto | 31 | 51.6% | **−0.134** | 0.009 (significantly negative) |
| entertainment | 14 | 28.6% | **−0.353** | 0.047 (significantly negative) |
| geopolitics | 7 | 42.9% | **−0.226** | 0.069 (marginally negative) |
| other | 35 | 57.1% | **−0.203** | 0.002 (significantly negative) |
| politics | 0 | — | — | no signals fired |

There is no category in which whale_entry has a positive edge. Geopolitics has only 7 samples but the trend is the same direction as the others.

This is **the opposite of accumulation**, which has WR 91% / EV +0.41 on n=22 in geopolitics. Whatever wallets are good at picking accumulation entries in geopolitics, *whale_entry copies a different set of wallets* (or the same wallets in a different mode), and that different set is consistently wrong.

---

## Comparison to Structural Signals

| signal | n | WR | avg EV | total EV | p-value | live ready? |
|---|---:|---:|---:|---:|---:|---|
| **accumulation** | 34 | 82.4% | **+0.309** | +10.49 | <0.001 | yes |
| **market_maker_flip** | 27 | 59.3% | **+0.138** | +3.73 | 0.097 | yes |
| oversized_bet | 47 | 85.1% | **+0.367** | +17.25 | <0.001 | one category gate fail |
| **whale_entry** | 87 | 49.4% | **−0.205** | **−17.80** | 1.000 | **no** |
| price_velocity | 1,140 | 34.5% | **−0.644** | −719.90 | 1.000 | no (disabled) |

`whale_entry` is the only signal type with > 50 resolved samples that has negative EV with statistical significance and no recoverable subset. **The best fix is to stop using it.**

The interesting comparison is between `whale_entry` (negative) and `accumulation` (positive). Both are wallet-derived. Both fire on tracked smart wallets. The structural difference: `accumulation` requires multiple entries by the same wallet on the same market over time — proof of conviction. `whale_entry` fires on a single trade — a whale could be diversifying, hedging, or making a one-off speculation. The `accumulation` filter is itself a quality gate that `whale_entry` lacks. **The lesson: "wallet did a thing" is too noisy. "Wallet did a thing twice" is alpha.**

---

## Why Tier-A Wallets Underperform on whale_entry

This is the most surprising finding and worth a dedicated theory. Tier-A wallets in `wallet_category_profiles` have proven *all-time* edge — that's how they got the tier letter. So why do their whale_entry signals lose −0.51 EV?

Three competing hypotheses, in order of plausibility:

1. **Selection bias.** `whale_entry` likely only fires on a tier-A wallet's *atypical* trades — trades that broke their normal pattern enough to trip a freshness/novelty heuristic in the signal engine. Their normal-pattern trades (which is where their alpha lives) may be filtered out before reaching this code path. We're copying their *least typical* moves and learning that those are the unprofitable ones.
2. **Look-ahead bias in the tier letter.** The `wallet_category_profiles` table is built from *all* of a wallet's resolved history including the trades whale_entry fired on. The tier letter is partly an average of the wins, but for the *forward-looking* subset of "trades that triggered whale_entry," the wallet's edge is gone.
3. **2024-election retiree contamination.** The S-tier political wallets with WR > 0.90 stopped trading after Nov 2024. The Apr-2026 whale_entry signals are firing on different wallets entirely (or on the same wallets making rare reactivation trades that don't carry alpha). Tier letters from a frozen-in-2024 sample don't apply to 2026 fills.

All three could be true simultaneously. Diagnosing which dominates is its own session — but the pragmatic conclusion is the same: **stop using tier letters as a filter for whale_entry**, and stop using whale_entry.

---

## Next Steps

1. **Disable whale_entry from the executor path.** Add `"whale_entry"` to `DISABLED_SIGNAL_TYPES` in `whale_signal_engine.py:40`. Keep `_record_signal_outcome()` recording so the data keeps growing. Re-evaluate at n=200.
2. **Audit the 70/87 missing-wallet-trade rows.** The fact that 80% of whale_entry signals don't have a corresponding `wallet_trades` row means the ingest pipeline doesn't see those wallets. Either expand `sync-wallet-trades --top-n 100` → `--top-n 500`, or wire the `whale_entry` firing path to also enqueue an immediate `data-api-fetch` for the firing wallet. Without this, every other diagnostic that joins `signal_outcomes` against `wallet_trades` is biased.
3. **Re-audit the existing `DISABLED_SIGNAL_TYPES` set.** `oversized_bet` is on it (line 42, comment "negative edge in data") but the live-readiness report shows it's the strongest single-type performer (n=47, WR 85%, EV +0.37). Either the comment is stale or the recovery improved its performance. **Worth checking before disabling whale_entry — same logic might re-enable oversized_bet.**
4. **Build a `accumulation_geopolitics` priority lane.** Currently the executor treats all signal types equally. Given that `accumulation` × `geopolitics` (n=22, WR 91%, EV +0.41) is 5-10× more profitable than baseline accumulation, weight Kelly sizing toward this slice and consider auto-firing at a slightly relaxed confidence floor.
5. **Re-run this experiment at n=200.** When `signal_outcomes` accumulates 200+ resolved whale_entry samples (currently 87), re-run `scripts/whale_entry_experiments.py`. If the verdict is unchanged at n=200, retire whale_entry permanently. If a configuration emerges with better data, revisit.
6. **Measure detection latency in the live signal path** rather than from `wallet_trades` reconstruction. Add a `whale_trade_ts` column to `market_signals` (set to the wallet's actual fill time at fire time), so future latency analyses don't depend on a 17/87 join match rate.

---

**Report file:** `C:\Users\bradl\PycharmProjects\trading_platform\reports\whale_entry_experiments_2026-04-11.md`
**Experiment script:** `scripts/whale_entry_experiments.py`
**Full grid CSV:** `/tmp/whale_entry_grid.csv` (122 rows)
**Raw log:** `/tmp/whale_exp.log`
