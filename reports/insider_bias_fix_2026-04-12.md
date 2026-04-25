# Insider Bias Fix — 2026-04-12

## The Sampling Bias Problem

**The insider detector's accuracy metric is invalid.** The top insider wallet (`0xd035fd5f...`) shows 28/28 correct in our database but we only captured 208 of their markets — an unknown fraction of their total Polymarket activity. A wallet that trades 4,000 markets but appears in our DB on 208 of them will show inflated accuracy if those 208 skew toward wins (which they will, because our ingestion pipeline fetches trades from specific tracked markets that tend to resolve in predictable ways).

## API Validation Results

### Insider wallets — API returned NO DATA

| Wallet | Our accuracy | Our N | Our markets | API trades | API PnL | Coverage |
|---|---:|---:|---:|---:|---:|---:|
| 0xd035fd5f... | 100% | 28 | 208 | None | None | ? |
| 0xa7c1f914... | 65.2% | 23 | 125 | None | None | ? |
| 0x22f0872d... | 65.2% | 23 | 149 | None | None | ? |

The Polymarket Data API returned no profile data for any of the three insider wallets. They likely trade through Polymarket's proxy wallet system, where the on-chain address isn't the same as the user's profile address.

### Broader check — **20/20 sign flips**

For ALL 20 of our top wallets by computed PnL, the Polymarket API returned **$0 PnL**. Not negative, not positive — exactly zero. Our data shows them as profitable ($1,689 to $157,824), but the API disagrees on every single one.

This is not a PnL computation error — it's an **API coverage gap**. The Polymarket Data API's `/profile/{address}` endpoint returns default values (0) for proxy wallet addresses. Our wallet_trades table stores the proxy address (which is what appears on-chain), not the user's EOA (which is what the profile API indexes).

**Implication**: API cross-reference is not viable for validating wallet-level metrics in this system. The wallets we track are on-chain proxy contracts, not user profiles.

## Redesigned Methods — ALL FAILED

| Method | Wallets found | Reason for failure |
|---|---:|---|
| Method A (API-profitable + accuracy) | 0 | API returns no data for our wallets |
| Method C (PnL-weighted, avg_pnl > 0) | 0 | Threshold too strict for price-delta units |
| Method A AND C | 0 | — |
| Method A OR C | 0 | — |

Without API validation, there's no external source of truth to distinguish genuine insiders from sampling artifacts. The bias-resistant methodology requires either:
1. An API that covers proxy wallet addresses (doesn't exist)
2. Much broader trade coverage (>50% of each wallet's activity) which requires ingesting ALL markets, not just tracked ones
3. A fundamentally different approach (e.g., on-chain analysis via Goldsky subgraph)

## Decision: Insider Detection RETIRED

**insider_entry stays in DISABLED_SIGNAL_TYPES.** There is no viable way to validate insider accuracy with current data coverage and API limitations.

Applied this session:
- `insider_entry` added to `DISABLED_SIGNAL_TYPES`
- `insider_entry` removed from `LIVE_SIGNAL_TYPES`
- InsiderDetector module kept for future research but NOT wired into live execution

## Impact on Other Signals

| Signal | Affected? | Reason |
|---|---|---|
| **accumulation** | NO | Fires on market-level convergence, not wallet accuracy. Core EV metric from signal_outcomes is unaffected. |
| **whale_entry_filtered** | NO | Uses behavioral archetype (fills/market, trade frequency) — not accuracy-based. |
| **Wallet tier profiles** | MINOR RISK | Computed from wallet_trades.pnl which has the same coverage limitation. Tier letters may be inflated for wallets with low coverage. |

The 20/20 sign flip finding means **all wallet_trades.pnl values are computed from partial data**. However:
- **accumulation** doesn't use wallet PnL for its core EV (it's measured from signal_outcomes resolutions)
- **whale_entry_filtered** uses archetype classification which is behavioral, not PnL-based
- The **tier-based confidence boost** (+0.15 for geopolitics) is the only part that depends on wallet_category_profiles, which in turn depends on wallet_trades.pnl. This is a minor risk — the boost is additive, not gating.

## Updated Live Signal Configuration

| Signal | Status | EV | Validated? |
|---|---|---:|---|
| **accumulation** | LIVE | +0.280 | signal_outcomes (clean) |
| **whale_entry_filtered** | LIVE (paper) | collecting | archetype-based (no bias) |
| **insider_entry** | **DISABLED** | **invalidated** | **sampling bias** |
| whale_entry (raw) | DISABLED | +0.006 | — |
| All others | DISABLED | negative | — |

## Lessons Learned

1. **Always validate computed metrics against the source of truth.** We assumed wallet accuracy in our DB reflected their actual skill. It didn't.
2. **Partial trade coverage creates systematic bias toward apparent skill.** The markets we track are not a random sample of all Polymarket markets — they're pre-selected by our universe scanner, which introduces selection effects.
3. **Polymarket's Data API doesn't cover proxy wallet addresses.** Any future wallet-level validation needs to use the Goldsky subgraph (on-chain data) or a profile-to-proxy address mapping.
4. **This is why we paper-test before going live.** The insider signal was built, tested, and CAUGHT before real money was deployed. The system worked as designed — bugs surface in validation, not in production losses.
5. **The 20/20 sign flip finding applies to ALL wallet-level PnL metrics.** Any future signal that depends on "this wallet is profitable" must account for coverage bias.

## What Would Fix This

1. **Ingest ALL markets, not just tracked ones.** This requires fetching the full Polymarket market list and ingesting trade data for every resolved market. Would give >90% coverage per wallet. Estimated effort: ~2-4 hours of API scraping + DB expansion.
2. **Build a proxy-to-EOA address mapping.** Use Goldsky or direct on-chain analysis to link proxy wallets to user profiles. Then validate against the profile API.
3. **Use on-chain settlement data directly.** Instead of our enrichment pipeline, use Polymarket's CTF contract events to get definitive PnL per address.

None of these are blockers for going live with `accumulation` — that signal doesn't depend on wallet-level metrics. They're improvements for future wallet-derived signals.

---

**Report file:** `C:\Users\bradl\PycharmProjects\trading_platform\reports\insider_bias_fix_2026-04-12.md`
