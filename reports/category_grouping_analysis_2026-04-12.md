# Category Grouping Analysis — 2026-04-12

**Should politics and geopolitics be merged? No — keep separate for signal filtering. Merge only for wallet tier computation.**

## Signal Performance: Politics vs Geopolitics vs Combined

### accumulation (the live signal)

| Slice | N | WR | EV | p |
|---|---:|---:|---:|---:|
| Overall | 59 | 79.7% | +0.280 | <0.001 |
| **Geopolitics** | **46** | **95.7%** | **+0.456** | **<0.001** |
| Politics | 0 | — | — | — |
| Pol+Geo merged | 46 | 95.7% | +0.456 | <0.001 |
| Other categories | 13 | 23.1% | −0.345 | >0.99 |

**Merging is a no-op for accumulation** — politics has zero resolved signals. All 46 resolved accumulation signals are geopolitics. The remaining 13 (crypto, other, sports) are deeply negative. The optimal strategy is **geopolitics only**.

### whale_entry (monitoring)

| Slice | N | WR | EV | p |
|---|---:|---:|---:|---:|
| Geopolitics | 19 | 78.9% | **+0.199** | **0.047** |
| Politics | 9 | 33.3% | −0.007 | 0.510 |
| Pol+Geo merged | 28 | 64.3% | +0.133 | 0.135 |

**Merging DILUTES** whale_entry from +0.199 (geo) to +0.133 (merged). Politics contributes 9 signals at break-even which water down the geopolitics edge. Keep separate.

### market_maker_flip (disabled, monitoring)

| Slice | N | WR | EV | p |
|---|---:|---:|---:|---:|
| Geopolitics | 11 | 72.7% | **+0.315** | **0.006** |
| Politics | 2 | 0.0% | −0.500 | — |
| Pol+Geo merged | 13 | 61.5% | +0.190 | 0.071 |

**Merging DILUTES** from +0.315 to +0.190. Politics has only 2 signals, both losses. Keep separate.

### Conclusion for signal filtering: **keep categories separate, boost geopolitics only**.

The +0.15 confidence boost currently applied to geopolitics for specialist/conviction wallets is correct. The +0.05 politics boost is not justified — politics has either zero data (accumulation) or break-even/negative EV (whale_entry, market_maker_flip) across every signal type.

## Wallet Overlap

| Group | Wallets |
|---|---:|
| Politics only | 4 |
| Geopolitics only | 24 |
| **Both** | **135** |
| Overlap rate | **82.8%** |

135 of 163 wallets trade both categories. The overlap is extremely high — these are largely the same wallets making the same kinds of bets across both categories.

**Win rate correlation (politics vs geopolitics): r=0.542, p<0.001.** A wallet's politics win rate is a statistically significant predictor of their geopolitics win rate. Expertise transfers across the boundary.

**Implication for wallet tiers**: a wallet that's proven in geopolitics should be trusted in politics (and vice versa). Merging for TIER COMPUTATION is justified even though merging for SIGNAL FILTERING is not.

## Boundary Markets

258 of 2,012 politics markets (12.8%) contain geopolitics keywords (iran, israel, ukraine, russia, nato, sanctions, etc.). These are markets like "Will Trump impose tariffs on China?" or "Will Biden sanction Iran?" that straddle both categories.

Overall miscategorization rate: **7.1%** — below 10%, so the categories are reasonably clean. The categorizer's boundary is imperfect but not noisy enough to argue for merging.

## Historical Simulation (accumulation only)

| Strategy | N | EV | PnL @ $25/trade |
|---|---:|---:|---:|
| All categories | 59 | +0.280 | **+$413** |
| **Geopolitics only** | **46** | **+0.456** | **+$525** |
| Pol+Geo merged | 46 | +0.456 | +$525 (same as geo) |
| Exclude crypto | 52 | +0.385 | +$500 |

**Geopolitics-only is the highest-PnL strategy** — $525 vs $413 for all-categories. The 13 non-geopolitics signals have negative EV and drag down the aggregate by $112.

**Excluding crypto** is the second-best strategy — removes 7 accumulation signals that all lost (crypto accumulation: N=7, WR=0%, EV=−0.50).

## All Categories EV (all signal types combined)

| Category | N | WR | EV | p | Verdict |
|---|---:|---:|---:|---:|---|
| entertainment | 156 | 96.2% | +0.081 | <0.001 | POS (penny collection) |
| sports | 1,029 | 97.8% | +0.016 | <0.001 | POS (penny collection) |
| science | 59 | 100.0% | +0.004 | <0.001 | POS (trivially) |
| economics | 147 | 97.3% | −0.002 | 0.393 | neutral |
| geopolitics | 467 | 81.4% | −0.052 | 0.004 | neg (dominated by price_velocity) |
| politics | 210 | 76.7% | −0.192 | <0.001 | neg |
| other | 133 | 50.4% | −0.274 | <0.001 | neg |
| crypto | 117 | 52.1% | −0.294 | <0.001 | neg |

**Warning**: these all-signal-type numbers are misleading because price_velocity (disabled, negative EV everywhere) dominates the sample. The signal-type-specific breakdowns in Part 2 are the numbers that matter for live trading decisions.

## Recommendation

### Keep categories separate for signal filtering and confidence boosting

The evidence is unambiguous: geopolitics carries the alpha, politics doesn't. Merging dilutes every signal type. The current confidence boost configuration should be:

| Category | Boost | Rationale |
|---|---:|---|
| geopolitics | **+0.15** | Strongest EV cell in every signal type |
| politics | **+0.00** | No positive EV on any signal type; remove the +0.05 boost |
| all other | **+0.00** | No boost (crypto should arguably get a penalty) |

### Consider pol+geo as a unified tier for wallet computation

82.8% wallet overlap + r=0.54 skill correlation justifies treating them as a single pool when computing tier letters. A wallet that's B-tier in geopolitics with no politics trades should still be considered "politically competent" for whale_entry_filtered. Implementation: add a `pol_geo` virtual category to `build_wallet_category_profiles.py` that unions politics + geopolitics trades.

### Exclude crypto from accumulation

Crypto accumulation: N=7, WR=0%, EV=−0.50. Every single crypto accumulation signal lost. Until this improves, consider adding `crypto` to an accumulation-specific exclusion list in the signal engine (not globally — crypto might work for other signal types eventually).

### No other category changes needed

The miscategorization rate is 7.1% (under 10%), the categorizer boundary is clean enough, and the remaining categories don't have actionable edge to worry about grouping.

---

**Report file:** `C:\Users\bradl\PycharmProjects\trading_platform\reports\category_grouping_analysis_2026-04-12.md`
**Script:** `scripts/category_grouping.py`
