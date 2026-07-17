# Competitor Analysis — PolySmartWallet, Poly Syncer, Predicts.guru, Polycopy
**Date:** 2026-07-13 · **Method:** 10-agent research workflow (4 competitor deep-dives + 4 adversarial claim-verifiers + independent copy-trading-profitability evidence agent + ecosystem landscape agent), cross-checked against our own DB schema, THESIS.md, and the pre-registered copy-entry kill-rule.

## Bottom line up front

1. **The four tools split into two analytics dashboards (PolySmartWallet, Predicts.guru) and two copy-execution engines (Polycopy, Poly Syncer).**
2. **Our data model is a strict superset of all four.** We are not behind on data — we are well ahead. 65-column `wallet_profiles`, `wallet_alpha_scores`, `wallet_archetypes`, `insider_wallets`, `wallet_category_profiles`, plus 569k `market_ticks` / 700k `market_anomalies` / calibration + governance tables. No competitor discloses anything close.
3. **All four sell the exact strategy we already built at higher sophistication and empirically killed** — "find top wallets by category and copy them." Our pre-registered kill-rule fired 2026-07-07: 118,646 copies / 787 wallets / **0 net-qualified**.
4. **None of them prove follower net profit.** Every headline is a *leader's* gross number, never a *follower's* net-of-cost result. Independent evidence confirms copy-trading is structurally −EV for the median follower.
5. **Therefore, matching their data will NOT make us profitable.** But there are ~6 concrete, cheap techniques worth borrowing — almost all as *features for the resolution engine*, not to revive copy.

---

## 1. What each tool actually is

| Tool | Type | Execution? | Pricing | Coverage claim | Standout |
|---|---|---|---|---|---|
| **PolySmartWallet** | Analytics | No | Free (lead-gen) | 8,000+ traders | Copy-return backtest + **slippage-resistance** as a scored dimension |
| **Predicts.guru** | Analytics | No | Free (airdrop/upsell) | full population | **Deposit/withdrawal cash-flow accounting**; builders/order-flow leaderboard; AI profiles |
| **Polycopy** | Copy-execution | Yes (Turnkey non-custodial) | $30/mo + 1% taker/0.5% maker | 500K wallets / 6K "smart money" | **Per-trade "Copy Score"** (5-factor), risk-control surface |
| **Poly Syncer** | Copy-execution | Yes (EIP-712 scoped) | $99/$299/$499/mo | 12,438 wallets | **Selection-vs-allocation split**; disclosed composite weighting; z-score+Hampel luck-filter |

**Credibility flags:** Poly Syncer's Trail-of-Bits audit is **not in ToB's own publications repo** and ships with no report/contract/hash → reads as fabricated. Its "front-run public orderflow / 0.6s co-located node" is technically incoherent (Polymarket matches off-chain via its CLOB operator — there is no public Polygon mempool of Polymarket orders to snipe; "Flashbots bundles" are mainnet, not Polygon). Polycopy's numbers inflate across pages (687K → 59M → 1M → 500K, non-reconciling). Predicts.guru's "100% win rate on $484M volume" is a resolved-only accounting artifact.

---

## 2. Data-parity matrix — do we match their data?

**Answer: we exceed it.** Every metric any of these tools surfaces, we already compute — usually with more rigor.

| Data dimension | PolySmartWallet | Predicts.guru | Polycopy | Poly Syncer | **Us** |
|---|:-:|:-:|:-:|:-:|:-:|
| Net PnL / ROI / win rate / volume | ✅ | ✅ | ✅ | ✅ | ✅ (+ pnl_reliable flag, ex-top-3 EV) |
| Category-segmented performance | ✅ | ✅ | ✅ | ✅ | ✅ (`wallet_category_profiles`, ev_politics/sports/crypto/econ) |
| Sharpe / drawdown / risk-adjusted | ➖ | ➖ | ➖ | ✅ | ✅ (sharpe, sortino, calmar, recovery, max_dd) |
| Kelly sizing | ➖ | ➖ | ✅ | ✅ (¼-Kelly) | ✅ (kelly_sizer, kelly_fraction) |
| Rolling recent-window (last-20) | ✅ | ✅ | ➖ | ✅ | ✅ (rolling_20_wr, win_rate_30d) |
| Copy-return backtest (ex-post) | ✅ | ➖ | ✅ | ✅ | ✅ (mirror-copy 120d diagnostic — **more rigorous**) |
| Composite/copyability score | ✅ | ➖ | ✅ (per-trade) | ✅ | ✅ (alpha_scores.copyability, conviction_score, tier_score) |
| Insider / unusual-flow detection | ➖ | ➖ (whaleRisk tag) | ➖ | ➖ | ✅ (`insider_wallets`, insider_score, avg_entry_percentile) |
| Archetype / bot classification | ➖ | ➖ (style tags) | ➖ | ➖ | ✅ (`wallet_archetypes`: mm/arb_bot/penny detection) |
| Calibration / accuracy tracking | ➖ | ➖ | ➖ | ➖ | ✅ (isotonic, Brier, calibration_reports) |
| Governance (FDR, event-clustered n, net-of-cost) | ❌ | ❌ | ❌ | ❌ | ✅ (the whole point) |
| **Slippage-resistance as a wallet score** | ✅ | ➖ | ➖ | ➖ | ⚠️ **partial** (per-trade slippage logged; not a wallet score) |
| **On-chain deposit/withdrawal cash-flow per wallet** | ➖ | ✅ | ➖ | ➖ | ⚠️ **gap** (we reconcile our own; not tracked wallets) |
| Allocation frameworks across a wallet SET | ➖ | ➖ | ➖ | ✅ (inverse-vol, risk-budgeted) | ⚠️ **partial** (Kelly per-trade; no explicit allocation layer) |
| Builders / order-flow attribution | ➖ | ✅ | ➖ | ➖ | ❌ (low value for us) |
| Tick-level orderbook depth | ➖ | ➖ | ➖ | ➖ | ⚠️ partial (market_ticks, price_history) |

Legend: ✅ has it · ➖ doesn't · ⚠️ partial/gap on our side · ❌ absent.

Only **three** cells are genuine gaps on our side, and only two of them matter (slippage-resistance scoring, cash-flow accounting). Everything else, we're even or ahead.

---

## 3. What we can genuinely learn (ranked by value, mapped to what it helps)

The copy strategy is dead for us, so the test for each idea is: **does it help the resolution engine or our wallet-graph-as-feature-source?**

1. **On-chain deposit/withdrawal cash-flow accounting (Predicts.guru).** Net cash *withdrawn* is the single hardest-to-fake skill signal — it defeats wash-trading, survivorship, and phantom-PnL in one move. This directly attacks the exact failure class in our history (phantom fills, booking races, resolved-zero-balance). **Highest-value borrow.** Add per-tracked-wallet deposits/withdrawals/net-cash-out as a wallet-quality gate and a wash-trader defeater.

2. **Slippage-resistance / market-liquidity scoring as a first-class dimension (PolySmartWallet).** We *log* per-trade slippage but don't *pre-score* markets on whether our edge survives execution cost. For the resolution engine — where the edge is short-vol carry on near-certain outcomes — a liquidity/depth gate that avoids thin books is directly protective. **High value, cheap.**

3. **Statistical luck-filtering baked into ranking: z-score > 2.5 wallet-drop + Hampel/3.5-MAD per-trade trim (Poly Syncer).** This is a clean, testable formalization of the survivorship defense we learned the hard way (the run-1 scalper cohort was an 83% survivorship artifact). Worth benchmarking against our bootstrap-CI `is_copyable_ci` filter. **Medium value** (we have an equivalent; theirs is a cheaper heuristic).

4. **Selection-vs-allocation split + inverse-vol / risk-budgeted allocation (Poly Syncer).** Their (self-published, unverified) claim that *allocation* drives 18–34 pts of monthly return dispersion vs only 9–14 pts from *wallet selection* is directionally the right insight: how you size across signals matters more than which signals. Relevant to allocating across our resolution-engine sub-signals and open positions. **Medium value** as a portfolio-construction frame; complements our existing Kelly + portfolio_risk.

5. **Per-trade quality scoring — the "Copy Score" wedge (Polycopy).** Their 5-factor decomposition (win-rate-in-class, conviction-vs-own-average, category experience, entry-price position, historical-P&L-in-similar-trades) is a clean feature checklist. It productizes exactly the per-fill filter our kill-rule concluded is *necessary* — **but our own diagnostics already show even this doesn't clear the bar at our scale** (their score is uncalibrated marketing; our equivalent measured −EV equal-weighted). Borrow the **feature list**, not the claim.

6. **Resolution-source / precedent tracking is the ecosystem's thinnest-covered dimension (landscape finding).** Almost no tool tracks *how* Polymarket resolves, UMA oracle behavior, or resolution precedents. This is **literally our thesis** (we are a resolution engine). It's not a gap we have — it's a **moat nobody else is building.** Deepen it.

---

## 4. Can we match their "performance" and turn profitable?

**No — because their "performance" is not trading profit, and copy-trading is −EV for followers.** Three independent legs, none of them vendor marketing:

- **Base rate (Dune, 2.5M wallets, Apr 2026):** ~**84% of all Polymarket wallets are net losers**; only ~2% ever clear $1,000 lifetime; ~0.32% clear $10,000. Copy-trading is sold as a jump from the losing 84% into the winning 16% — but its mechanism (enter the same markets *later*, *smaller*, at a *worse price*, minus a fee) works **against** that jump.
- **The one academic result (Oliphant, SSRN, Apr 2026):** copy trades beat the *same follower's own other trades in the same market* by **+1.62 pp**. That is a *relative* edge over your own dumb bets — **not** net-of-cost profit vs cash, and it does not net out the worse price the follower pays. Vendors routinely mis-cite this as proof; it isn't.
- **Adverse selection is active, not passive (Sethi, economist):** sophisticated wallets deliberately generate *insider-looking* patterns **because** they'll be copied, then book profits from separate wallets while copycats take the losses. Following suspected insiders is a documented loss vector.

**And we already proved this internally, with more rigor than any of them will ever publish.** Our own decomposition of 112,767 resolved cohort BUYs:

| Copy method | Return | Verdict |
|---|---|---|
| Notional-weighted (the wallets' *real* sizing) | **+0.206/$** | The wallets ARE +EV |
| Equal-weight flat $5 (naive copy) | **−0.358/$** | −EV |
| Equal-weight, clipped 9× | **−0.460/$** | −EV |
| Look-ahead cheat (copy known winners' entries) | **−0.67/$** | −EV *even knowing who won* |
| Median trade | **−1.00/$** | 2 of 3 bets go to **zero** |

The wallets earn via **size + tail concentration**: 31% win rate, top 1% of trades = 171% of all PnL, winners sized 10.2× bigger than losers. Their edge is *knowing which of their big bets will hit* — private information you cannot copy. At a $294 bankroll you can neither replicate $2,106 winner-sizing nor survive the 31%-WR variance to reach the tail. **The kill stands.** (The only positive copy number ever seen — +0.249/$ from size-*proportional* copying of the top-5 wallets — was statistically insufficient at n=25 and is the *only* sanctioned path back, gated behind ≥30 events, CI>0, BH q≤0.10, and a shadow ladder.)

**What this means:** these competitors are a **"sell the shovels" business.** Their revenue (subscriptions + per-trade fees) is positive and recurring *regardless of follower P&L.* Polycopy literally earns *more* when you trade *more* — a direct conflict with your edge. Matching their data doesn't buy profit; it buys the same −EV product with a nicer dashboard.

---

## 5. Where we are genuinely ahead (the moat)

- **Superset data** (Section 2) — nobody discloses insider detection, archetype/bot classification, calibration tracking, or per-category EV.
- **Governance rigor none of them have:** FDR/multiple-testing budget, event-clustered effective-n, net-of-cost-as-default, held-out champion/challenger gates. Every competitor ranks on raw gross PnL / naive win-rate — the exact artifacts we spent months exterminating.
- **A resolution-engine thesis** in a dimension (resolution-source/precedent tracking) the whole ecosystem ignores.
- **We ran the honest experiment they will never run** — a pre-registered, net-of-cost, survivorship-defended copy-trading kill-rule — and acted on the answer.

The uncomfortable flip side: **they are *shipping* and *monetizing*; we are at $258 equity, mostly idle.** Their advantage is not data or rigor — it's distribution and a business model that doesn't require the strategy to actually work. That's worth internalizing: our gap to them is *product/GTM*, not *quant*.

---

## 6. Next-step options

**A. Harden the resolution engine (recommended focus).** Borrow items 1, 2, 6: add per-tracked-wallet cash-flow accounting (deposits/withdrawals/net-cash-out) as a wash-trader/skill gate; add a market-liquidity/slippage-resistance pre-trade gate; deepen resolution-source/UMA/precedent tracking. All three protect and sharpen the one live edge.

**B. Adopt the allocation layer (item 4).** Treat "how we size across resolution sub-signals and open positions" as a first-class layer (inverse-vol / risk-budgeted), not just per-trade Kelly. Directly serves the scaling thesis (Claim 6).

**C. Run the sanctioned copy-revival measurement (item 5, gated).** Keep accumulating the weekly size-proportional top-5 evidence toward ≥30 events / CI>0 / BH q≤0.10. Zero-cost, already scheduled; do **not** shortcut it.

**D. The strategic reframe (biggest lever, non-quant).** Their real advantage is a productized, distributed, subscription business. If the goal is dollars, the highest-EV move may be to **package our superior data/rigor as the product** (a wallet-intelligence/analytics layer that is honest about net-of-cost) rather than to keep hunting for personal trading alpha at $258. This is a genuine fork in strategy.
