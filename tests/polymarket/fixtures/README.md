# Chain-decoder fixtures

Real Polygon on-chain logs pinning `decode_order_filled` and `decode_ctf_event`
in `src/trading_platform/polymarket/wallet_stream.py`. Consumed by
`tests/polymarket/test_chain_decoders.py`.

These exist because the byte-level decode used to be inline, untested code inside
async handlers. On 2026-07-21 that hid a layout bug that silently dropped **100%**
of `PayoutRedemption` events (3,610 / 10 min flowing on-chain) — `PayoutRedemption`
indexes the collateral token and carries the `conditionId` in data, the *opposite*
of `PositionSplit`/`PositionsMerge`. A wrong-layout regression must now fail loudly.

## Provenance

Captured via `wss://polygon-bor-rpc.publicnode.com` (HTTP is 403/429 for our IP;
drpc `getLogs` returns empty):

| fixture | event | topic0 (short) | how captured |
|---|---|---|---|
| `order_filled_v2_buy_maker.json` | OrderFilled V2 | `0xd543adfd…` | `eth_getLogs`, real |
| `order_filled_v2_sell_maker.json` | OrderFilled V2 | `0xd543adfd…` | **derived** (see below) |
| `order_filled_v2_token_to_token.json` | OrderFilled V2 | `0xd543adfd…` | `eth_getLogs`, real |
| `position_split.json` | PositionSplit | `0x2e6bb91f…` | `eth_getTransactionReceipt`, real |
| `positions_merge.json` | PositionsMerge | `0x6f13ca62…` | `eth_getTransactionReceipt`, real |
| `payout_redemption.json` | PayoutRedemption | `0x2682012a…` | `eth_getTransactionReceipt`, real |
| `condition_resolution.json` | ConditionResolution | `0xb44d84d3…` | `eth_getLogs`, real |

CTF tx hashes were drawn from our own `wallet_ctf_events` rows; OrderFilled logs
from a small recent block range on the two V2 exchanges
(`0xe111…996b`, `0xe222…0f59`). The ConditionResolution log comes from the
canonical ConditionalTokens deployment `0x4d97dcd9…` (~1–2k reports/day, so a
recent range always has one).

## On-chain layouts (what the fixtures pin)

```
OrderFilled (V1/V2):
  topics = [sig, orderHash, maker, taker]
  data   = [makerAssetId, takerAssetId, makerAmountFilled, takerAmountFilled, fee, …]
  asset_id 0 == USDC collateral; the non-zero leg is the outcome token.

PositionSplit / PositionsMerge:
  topics = [sig, stakeholder,        parentCollectionId, conditionId]
  data   = [collateralToken, partition, amount]

PayoutRedemption   (⚠ OPPOSITE of split/merge):
  topics = [sig, redeemer, collateralToken, parentCollectionId]
  data   = [conditionId, indexSets, payout]

ConditionResolution (the oracle report itself):
  topics = [sig, conditionId, oracle, questionId]
  data   = [outcomeSlotCount, arrayOffset(0x40), arrayLen, numerators…]
  payoutNumerators is per OUTCOME SLOT; slot i ↔ clobTokenIds[i], so
  numerators[0] is the YES payout ([1,0] = YES wins, [0,1] = NO wins,
  [1,1] = 50/50 void). The decoder honors the ABI offset word rather than
  assuming 0x40, and rejects a log whose array length disagrees with
  outcomeSlotCount.
```

## The derived `sell_maker` fixture

The live V2 exchange **never emits a maker-SELL** `OrderFilled` — 0 of 8,761 logs
sampled 2026-07-23. It always encodes the USDC leg on the maker side, so a "sell"
surfaces as the **taker** side of a maker-BUY log (the handler inverts direction by
role). `order_filled_v2_sell_maker.json` is therefore the real buy-maker capture
with data words `[0]↔[1]` and `[2]↔[3]` swapped: it decodes to an *identical*
token_id / usdc / shares / price but exercises the decoder's `taker_asset_id == 0`
SELL branch so a regression there fails. Its `source` field says `DERIVED` and it
carries a `_derivation` note — it is the only non-raw log here.

## Regenerating

`scripts/capture_chain_decoder_fixtures.py` (also archived in the session
scratchpad). Requires DB access (for CTF tx hashes) + the publicnode WS endpoint.
Fixtures are immutable snapshots — regenerate only if a layout genuinely changes.
`--only condition_resolution` (comma-separated names accepted) captures a single
fixture and leaves the rest untouched.

Note: publicnode prunes log history after roughly two days, so a capture must
target a recent block range; older ranges return `-32701 history has been
pruned`.
