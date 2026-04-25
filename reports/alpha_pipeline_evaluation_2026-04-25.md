# Alpha Signal/Strategy Pipeline — Evaluation
*2026-04-25*

The infrastructure is in place. The data tells us where the pipeline leaks.

## Funnel volumes (24h)

| Stage | Volume | Notes |
|---|---:|---|
| Synthetic signals (NON_TRADEABLE) | 2,294 | price_velocity + order_flow_imbalance — informational only |
| Whale-derived signals fired | 480 | network_leader_entry 141, market_maker_flip 70, wallet_reversal 54, oversized_bet 33, accumulation 25, whale_entry_filtered 24, cascade 18, specialist_entry 9, hci 7, etc. |
| **Paper trades opened (24h)** | **2** | **0.4% conversion from tradable signals** |
| Paper trades resolved (24h) | 83 | older trades resolving |
| Live trade attempts (7d) | 0 | end-to-end live blocked since the 14-gate stack tightened |
| Hypotheses resolved (7d) | 247 | scorecard fresh |

## The binding constraint

**0.4% signal → paper conversion.** 480 tradable whale signals/day, 2 paper trades. If the system only ever places 2 trades a day, 7%/day Kelly cap × $356 bankroll × 2 trades ≈ $50/day max risk — there is no path to $10–20K/month from that flow rate. **This is the single highest-EV problem in the system.**

## Hypothesis accuracy (14d, n ≥ 5)

| Signal | n | Accuracy | Verdict |
|---|---:|---:|---|
| `whale_entry` (raw) | 10 | **70.0%** | Star — currently blocked from trade by `DISABLED_SIGNAL_TYPES` |
| `whale_entry_filtered` | 19 | 57.9% | Strong — workhorse |
| `specialist_entry` | 10 | 50.0% | Underused — only 9 fires/day despite +0.93 EV |
| `cascade` | 23 | 47.8% | Solid |
| `market_maker_flip` | 31 | 48.4% | SignalEvaluator marked `disabled` despite +0.33 EV — auto-disable too aggressive |
| `wallet_reversal` | 32 | 46.9% | Solid (but boosted YES side hits 73% in shorter window) |
| `oversized_bet` | 19 | 42.1% | Marginal |
| `network_leader_entry` | 20 | 30.0% | Drag — should drop from live consideration entirely |
| `accumulation` | 24 | 25.0% | Drag — SignalEvaluator already disabled |
| `news_reactor` | 10 | 20.0% | Excluded ✓ |
| `no_position_entry` | 6 | 16.7% | Excluded ✓ |
| `copyable_contrarian` | 8 | 0.0% | Excluded ✓ |

## SignalEvaluator state vs reality

- `market_maker_flip` is **DISABLED** by the evaluator (rolling-20 EV +0.075) but still fires 70/day in the engine. Disconnect: paper executor doesn't read evaluator status, only EXCLUDE_SIGNAL_TYPES. Either bring them in sync or relax the auto-disable rule.
- `wallet_reversal` is `building` with sample 32 — should graduate to `live` soon.
- `whale_entry` calibrated at WR 64%, EV **−0.03** — the wins are smaller than the losses. The 70% hypothesis-accuracy figure is correct but doesn't price the asymmetric payoff. This signal needs a sizing model that respects the asymmetry, not a stake boost.
- `price_velocity` row has `status=live` but `sample_size=0` — calibration bug; junk entry.

## Where the alpha actually lives (8d cohort)

| Slice | n | WR | PnL | Stake mult shipped |
|---|---:|---:|---:|---|
| Long-shot YES (entry <0.30) | 74 | **50%** | **+$67** | 1.25× |
| `wallet_reversal` YES | 11 | 72.7% | +$19.74 | 1.5× |
| `whale_entry` raw (currently disabled) | 10 | 70% | +$863 (30d) | — |
| `cascade` YES | 18 | 55.6% | +$13 | 1.3× |
| `whale_entry_filtered` NO | 6 | 66.7% | +$3.63 | 1.3× |

## Top improvements needed now

| # | Action | Why now | Effort |
|---|---|---|---|
| 1 | **Diagnose 0.4% conversion** — find every silent return between signal emit and paper executor | Binding constraint on everything. Diagnostic logging just shipped (print()-based to bypass logger weirdness in live-collect) | 0.5h once next whale signal lands |
| 2 | **Lift `whale_entry` raw out of DISABLED** | 70% acc, +$863 PnL on 10 resolved is the highest-EV signal the system has | 1h with safe re-introduction (paper-only first) |
| 3 | **Bring SignalEvaluator status into the executor gate** | Currently the evaluator can mark a signal `disabled` and the engine still fires it, with the paper executor blind to that decision | 1h |
| 4 | **Increase `specialist_entry` + `high_conviction_insider` flow** | Highest unit EV (+0.93 specialist) but only 9+7 fires/day. Either tighten wallet specialist tagging or relax confidence floor | 2-3h |
| 5 | **Reconsider auto-disable threshold** | `market_maker_flip` disabled despite +0.33 EV — likely missing real winners | 1h |
| 6 | **Side-aware accuracy split** for the bottom-half signal types | `network_leader_entry` 30% may be all on one side; the other side may be edge | 1h to query, 30m to gate |
| 7 | **Wire `whale_entry` payoff asymmetry into sizing** | 64% WR but EV −0.03 = small wins, big losses. Needs Kelly with explicit avg_win/avg_loss not the symmetric assumption | 2h |
| 8 | **Investigate why specialist boost showed negative lift** | 7d sample only had 16 specialist trades; need 30d re-baseline | 2h |

## What's NOT broken (skip)

- Reliability — pool patched, escalation alerts live, scheduler healthy, readiness endpoint reports READY.
- Stake concentration on proven slices — shipped 4 winning combos + long-shot boost.
- Excluded signal types — `copyable_contrarian`/`news_reactor`/`no_position_entry`/`consensus_follower` correctly gated.
- Hypothesis backfill — 247 resolved in 7d, no drift.
- Side gates — `cascade NO`, `wallet_reversal NO`, `accumulation NO` correctly blocked.

## Just-shipped this turn

- Converted `[DISPATCH]`/`[SPORTS_BLOCK]`/`[DISABLED]`/`[INFO_ONLY]` markers from `logger.info` to `print(flush=True)` — `live-collect` doesn't run `setup_logging()` so JSON-logger INFO records were getting dropped. Diagnosis was hidden behind the wrong stdout layer. Restarted with the fix.
- Monitor armed for 10 min watching for the first whale-signal `[DISPATCH]` event post-fix.

## What I expect to find when monitor fires

One of three patterns will emerge:
1. **Most signals hit `[SPORTS_BLOCK]`** → too aggressive sports filter; relax the regex.
2. **Most signals hit `[DISPATCH]` and disappear** → paper executor's silent returns (Kelly=$0, dup-check, exec gates, fusion floor) are the actual block; instrument those.
3. **Few `[DISPATCH]` events at all despite signal volume** → upstream filter (NON_TRADEABLE, DISABLED, INFO_ONLY) is matching unexpectedly broadly.

Once the binding gate is identified, **one fix unlocks the conversion rate from 0.4% to >5%**, which is the difference between "pipeline exists" and "pipeline produces consistent live trades."
