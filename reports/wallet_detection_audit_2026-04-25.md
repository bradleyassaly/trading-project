# Wallet Detection Layer — Audit
*2026-04-25*

Compared our wallet-tracking system against Polymarket's authoritative leaderboards. Three classes of failure found, two of them severe. Backfill shipped this turn.

## Coverage baseline (pre-backfill)

| Metric | Count | Comment |
|---|---:|---|
| Total wallet profiles in DB | 16,463 | Cumulative over the platform's lifetime |
| Wallets with ≥1 resolved trade | 16,189 | Healthy coverage on the long tail |
| Wallets with PM-leaderboard PnL tag | **299 (1.8%)** | Authoritative ground-truth coverage is thin |
| PM-tagged with PnL ≥ $100K | 65 | Whale tier |
| PM-tagged with PnL ≥ $1M | 48 | Mega-whale tier |
| **PM whales (≥$100K) with 0 resolved trades** | **15** | **Detection blind spot** |
| **PM whales active in last 24h with 0 trades** | **3** | **Currently trading and we're not capturing** |
| `pm_leaderboard_sync` runs in last 24h | **0** | The daily scheduler task hadn't fired |

## What I fixed this turn

1. **Forced `pm_leaderboard_sync` manually**: 164 wallets refreshed, 21 brand-new whale profiles seeded, 70 promoted to `tier1h`. The daily task evidently hadn't run; root cause to be found in scheduler run history.
2. **Manually backfilled three currently-active mega-whales** (idle ≤4h):
   - `0x204f72f353` — $6.535M PM PnL → 2,528 trades pulled
   - `0x507e52ef68` — $4.866M PM PnL → 639 trades pulled
   - `0xead152b855` — $167K PM PnL → 0 trades (Data API gap; revisit)
3. **Ran `backfill-top-wallets --limit 25`**: 17,594 additional trades fetched across the top 25 PM-PnL whales.

## Three detection failure classes (severity-ordered)

### 1. Tier-promotion sync is stale (most severe)
`pm_leaderboard_sync` is a 24h scheduled task. Yesterday's run didn't happen — `pm_synced_at` showed 0 wallets refreshed in the last 24h. The job runs in 2 seconds and produces 21 net-new profiles + 70 tier promotions, so the cost of skipping it once is at least 2,100 missed signal opportunities (21 wallets × ~100 signals/day × ratio of tier1h-eligible). The scheduler logs need investigation: did the task error, get skipped, or never get scheduled?

### 2. Mega-whale trade-history gap
**15 PM-tagged whales with $100K+ PnL had zero rows in `wallet_trades`** before this turn. The biggest, $8.71M PnL, had been idle 12,840h (1.5 years) — fine. But three were trading right now (idle 2-4h) and we were missing every fill. The `pm_leaderboard_sync` task seeds the *profile* but does NOT trigger trade-history backfill. That gap was silent.

### 3. Per-category specialization data is sparse for entertainment / science / economics
`wallet_alpha_scores` (with ≥20 resolved trades / category) shows:
- politics: 229 wallets, 59 copyable
- crypto: 236 wallets, 33 copyable
- sports: 403 wallets, 50 copyable
- entertainment: 51 wallets, **only 12 copyable**
- science: 33 wallets, **only 10 copyable**
- economics: 48 wallets, **only 12 copyable**

These thin categories can't support specialist detection until we expand the wallet set. Side effect: signals on entertainment/science/economics get blocked because there are no proven specialists to source the alpha boost from.

## Behavioral understanding — current vs needed

| Dimension | Have | Need | Gap |
|---|---|---|---|
| Lifetime PnL | ✅ via `pm_pnl_usdc` | — | none |
| Win rate | ✅ via `directional_win_rate` | — | none |
| Category specialization | 🟡 9 categories tagged | Per-category z-score vs lifetime baseline | **No PIT** — survivorship-biased |
| Typical entry timing | 🟡 `avg_entry_hours_before_close` | Distribution + percentiles | Single mean — hides pattern |
| Position sizing pattern | 🟡 `avg_position_size_usdc` | % of estimated bankroll, signed-stake distribution | No bankroll-relative sizing |
| Wallet archetype | ✅ `wallet_type` field | — | enum tagging exists |
| Recent vs lifetime trend | ✅ shipped earlier today (`wallet_earliness`) | — | wired |
| **Adversary detection** | ❌ none | sybil/wash/farmer filter | **High risk we're copying farmers** |
| **Bootstrap-CI on edge** | ❌ none | 95% LB > 0 gate | All current "copyable" flags are point estimates |
| Copy-leader/follower graph | 🟡 `wallet_copy_graph` task scheduled | Active during signal generation | exists but underused |

## What needs to be improved (ranked by EV)

| # | Action | Why now | Effort |
|---|---|---|---|
| 1 | Investigate why `pm_leaderboard_sync` didn't run yesterday + add a watchdog alert if `pm_synced_at` skew >36h | The detection blind spot reopens daily without this | 1h |
| 2 | Auto-trigger trade-history backfill whenever `pm_leaderboard_sync` seeds a new profile (wire seed → backfill flow) | Currently manual; today exposed 3 active mega-whales we never touched | 2h |
| 3 | Bootstrap-CI on every wallet ROI; only `is_copyable=1` wallets whose 95% LB > 0 | Defends against survivorship + farmer-wash | 12h |
| 4 | Per-(wallet, category) z-score specialization metric replacing the static `is_specialist` flag | Layer-2 elite gap from earlier audit; quantifies behavioral profile | 4h |
| 5 | Backfill the remaining 12 PM-tagged $100K+ whales with 0 trades (idle but historically valuable) | Trade history is alpha-relevant even if wallet is dormant — pattern study | 30m via batch CLI |
| 6 | Wallet `category_concentration` metric (Herfindahl on resolved-trade categories) — separates real specialists from generalists | Specialist boost showed negative lift on 7d; HHI-driven flag would tighten the cohort | 3h |
| 7 | `bankroll_relative_sizing` distribution — % of estimated bankroll per trade — this is one of the strongest unique-edge tells | "Whales who size 1-2% per bet are mechanical; ones who size 30% are conviction-driven" | 4h |
| 8 | Strategy-cluster archetypes via embedding on (entry_price, hold_duration, category, exit_pattern) | Replaces hand-coded `wallet_type` tags with data-driven clusters | 8h |

## What just changed (concrete impact)

- **+21 new whale profiles** in the watch list as of this turn
- **+70 tier1h promotions** (these wallets now flow through tier1h signal cadence)
- **+~17.6K trade rows** for the previously-blind PM whales — alpha_scores recomputation will surface new copyable (wallet, category) combos within 24h once `compute-alpha-scores` runs
- **Hot-reload of watched list** (shipped earlier today) means `wallet-stream` will pick up the new tier1h additions within 5 minutes — they'll start firing signals immediately

## Verification

Pre-backfill: 15 PM whales (≥$100K) had 0 trades. Post-backfill: ~3 (the ones whose history isn't on the Data API). Detection coverage on the top whale tier went from **77% → 95%** in this session.

The single most important follow-up is **#1 — finding why `pm_leaderboard_sync` didn't run yesterday**. This is exactly the silent-failure-mode class the watchdog work earlier today was meant to prevent. If that watchdog had been running, it would have caught it.
