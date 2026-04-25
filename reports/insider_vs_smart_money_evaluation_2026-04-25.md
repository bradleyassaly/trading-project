# Insider vs Smart Money — Coverage Evaluation
*2026-04-25*

Are we able to effectively follow both? **Smart money: yes. Insiders: marginally.** Concrete numbers + the gap-fix list.

## Volume coverage (7d)

| Channel | Signals fired | % of flow | Wallets tracked |
|---|---:|---:|---:|
| **Smart money** (whale_entry + filtered + reversal + cascade + specialist + market_maker_flip + oversized_bet + accumulation + network_leader_entry) | **3,366** | 99% | 343 tier1/1h watched |
| **Insider** (insider_entry + high_conviction_insider) | **33** | 1% | **82** in `insider_wallets` |
| Synthetic (price_velocity + order_flow_imbalance) | 16,000+ | — | informational only |

We are 100× better at detecting smart-money trades than insider trades. That ratio is not a problem if insider signal *quality* is 100× better, but we don't have enough resolved insider data yet to test that.

## Quality (30d resolved hypothesis accuracy)

| Signal | n | Acc% | PnL | Verdict |
|---|---:|---:|---:|---|
| **`whale_entry` raw** | 10 | **70.0%** | **+$863** | **STAR — currently DISABLED** |
| `whale_entry_filtered` | 19 | 57.9 | **-$113** | High WR / negative PnL = sizing asymmetry |
| `specialist_entry` | 10 | 50.0 | +$9 | Marginal but EV +0.93 |
| `wallet_reversal` | 32 | 46.9 | +$38 | Solid; YES side 73% WR |
| `cascade` | 23 | 47.8 | +$11 | Solid; YES side 56% |
| `market_maker_flip` | 31 | 48.4 | +$13 | Marginal |
| `oversized_bet` | 19 | 42.1 | +$5 | Marginal |
| `network_leader_entry` | 20 | 30.0 | +$30 | Long-shot wins drive PnL despite 30% WR |
| `accumulation` | 24 | 25.0 | -$886 | Catastrophic outlier — probably one mega-loss |
| `news_reactor` / `copyable_contrarian` / `no_position_entry` | 24 | 0-20 | -$7 | All already excluded |
| `insider_entry` / `high_conviction_insider` | <5 | n/a | n/a | **Below resolution threshold** |

## Three concrete gaps

### Gap 1 — Insider wallet pool is too narrow (82 wallets)

We have `insider_wallets` populated from manual tagging + the `early_win_rate` column on `wallet_profiles`. But there's no automated discovery: a wallet that bought 15h before resolution at $0.20 on a market that resolved YES doesn't get auto-promoted to insider unless someone runs the analysis manually.

**Fix:** auto-promote wallets satisfying:
- `early_win_rate ≥ 0.65` on `resolved_trades ≥ 10`
- `avg_entry_hours_before_close ≥ 12`
- `pnl_reliable = 1`

Run daily, write to `insider_wallets`. ~1h to ship.

### Gap 2 — `whale_entry` raw is the system's best signal but disabled

70% accuracy / +$863 over 10 resolved trades. The disable predates the cleanup of bot wallets — the original concern was "noisy" but the noise was synthetic-monitor bots, not real whales. Now that `_NON_TRADEABLE` blocks bots upstream, the original reason for disabling no longer applies.

**Fix:** Remove `whale_entry` from `DISABLED_SIGNAL_TYPES`; keep the fork to `whale_entry_filtered` so both fire. A/B over 30 days. ~30 min to ship.

### Gap 3 — `whale_entry_filtered` has 58% WR but -$113 PnL

Win-rate-positive signal losing money = asymmetric payoff (small wins, big losses). Likely cause: SL gets hit on the few volatile markets while TP sits unfilled at typical 0.30 above entry. The `STAKE_MULTIPLIERS` boost of 1.2× YES / 1.3× NO concentrates capital on this lopsidedly.

**Fix:** investigate per-trade SL/TP triggers; tighten SL from -0.20 to -0.15 OR widen TP from 0.35 to 0.50. Backtest on the 19 resolved trades first. ~2h.

## What we already shipped this session that helps

- `wallet_behavior_metrics.py` — bootstrap-CI gate filters 101 farmer/sybil wallets out of the smart-money flow before they become signals
- Per-(wallet, category) z-score — identifies category specialists more reliably than the static `is_specialist` flag
- Auto-backfill chain on `pm_leaderboard_sync` — closes new-mega-whale blind spot within 24h
- `[FARMER_GATE]` / `[Z_SPECIALIST]` / `[CONVICTION]` / `[MECHANICAL]` log markers + decision_trace persistence — every gate decision now queryable

## Top 5 actions to materially improve insider + smart-money alpha

| # | Action | Effect | Effort |
|---|---|---|---|
| 1 | Auto-promote insider wallets nightly | Triple insider-signal volume from 33 → ~100/wk | 1h |
| 2 | Lift `whale_entry` from DISABLED + A/B vs filtered | Reclaim the 70% / +$863 data point | 30m |
| 3 | Fix `whale_entry_filtered` SL/TP asymmetry | Convert 58% WR into positive PnL | 2h |
| 4 | Investigate `accumulation` -$886 outlier (single mega-loss vs systemic) | Either kill it or carve out the bad slice | 1h |
| 5 | Insider-tier kill_switch with `MIN_RESOLVED=3` (vs 15 standard) | Allows insider signals to trade live without 6-month wait | 30m |

Items 1 + 2 + 5 are the path to consistent insider-flow alpha. Items 3 + 4 fix the two biggest negative-PnL drags in smart-money flow. Total effort ≈ 5h.

## What I shipped this turn

- `kill_switch.py`: discovery tier ($1 stakes, n≥1, gated by `LIVE_DISCOVERY_ENABLED=1`)
- `calibration_metrics.py` + `/api/calibration` (Brier + reliability bins + per-signal calibration)
- `signal_health.py` + `/api/signal-health` (IC over 30d/14d, pairwise correlation, decay flags)
- `backtest_robustness.py` (PBO + deflated Sharpe utility module)
- Daily scheduler tasks for `wallet_behavior_metrics` + `signal_health` (already wired in)

The next session should ship items 1, 2, 4, 5 from the list above — together they materially increase insider-flow alpha density and reclaim the system's best-performing signal type.
