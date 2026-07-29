"""Regenerate the chain-decoder test fixtures from real Polygon on-chain logs.

Writes tests/polymarket/fixtures/*.json — real captured logs pinning
decode_order_filled / decode_ctf_event (see that fixtures dir's README).

OrderFilled fixtures come from eth_getLogs over a small recent block range on
publicnode (which serves both the buy-maker and the token<->token orientations
that actually occur on V2). CTF split/merge/redeem come from
eth_getTransactionReceipt on tx hashes drawn from our own wallet_ctf_events rows
(drpc getLogs returns empty; HTTP is 403/429 for our IP). Every captured log is
validated by decoding it with the production pure decoders before it is written.

Provenance note baked into the fixtures: the live V2 exchange NEVER emits a
maker-SELL OrderFilled (the USDC leg is always on the maker side, so SELLs
surface via the taker role). The sell-maker fixture is a transparent leg-swap of
the real buy-maker capture, existing solely to pin the decoder's SELL branch.

Usage:
    python scripts/capture_chain_decoder_fixtures.py
    python scripts/capture_chain_decoder_fixtures.py --only condition_resolution

Requires DB access (for the CTF tx hashes) and the publicnode WS endpoint.
Fixtures are immutable snapshots — only regenerate if a layout genuinely
changes; --only limits capture to the named fixture(s) so the others stay
untouched.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys

import websockets

from trading_platform.polymarket.db_connection import get_connection
from trading_platform.polymarket.wallet_stream import (
    ORDER_FILLED_TOPIC_V2, CTF_SPLIT_TOPIC, CTF_MERGE_TOPIC, CTF_REDEEM_TOPIC,
    CONDITION_RESOLUTION_TOPIC, CONDITIONAL_TOKENS,
    _CTF_COLLATERAL, decode_ctf_event, decode_order_filled,
    decode_condition_resolution, payout_yes_from_numerators,
)

WS_URL = "wss://polygon-bor-rpc.publicnode.com"
EXCH_V2 = ["0xe111180000d2663c0091e4f400237545b87b996b",
           "0xe2222d279d744050d28e00520010520000310f59"]
FIX_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                       "tests", "polymarket", "fixtures")

_LOG_FIELDS = ("address", "topics", "data", "blockNumber",
               "transactionHash", "logIndex")


def _slim(log: dict) -> dict:
    return {k: log.get(k) for k in _LOG_FIELDS}


def _ctf_candidates() -> dict:
    conn = get_connection()
    out: dict = {}
    try:
        for et in ("split", "merge", "redeem"):
            rows = conn.execute(
                "SELECT transaction_hash FROM wallet_ctf_events "
                "WHERE event_type=%s AND transaction_hash LIKE '0x%%' "
                "AND length(transaction_hash)=66 ORDER BY timestamp DESC LIMIT 40",
                (et,)).fetchall()
            seen, txs = set(), []
            for r in rows:
                if r[0] not in seen:
                    seen.add(r[0]); txs.append(r[0])
            out[et] = txs
    finally:
        conn.close()
    return out


async def _call(ws, method, params, rid):
    await ws.send(json.dumps({"jsonrpc": "2.0", "id": rid,
                              "method": method, "params": params}))
    for _ in range(6):
        msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=30))
        if msg.get("id") == rid:
            return msg
    return {}


async def _receipt_log(ws, tx, topic0, rid):
    rec = (await _call(ws, "eth_getTransactionReceipt", [tx], rid)).get("result")
    if not rec:
        return None
    for lg in rec.get("logs") or []:
        t = lg.get("topics") or []
        if t and t[0].lower() == topic0:
            return _slim(lg)
    return None


async def _capture(only: set[str] | None = None) -> dict:
    def want(*keys: str) -> bool:
        return only is None or any(k in only for k in keys)

    got: dict = {}
    ctf = _ctf_candidates() if want("position_split", "positions_merge",
                                    "payout_redemption") else {}
    async with websockets.connect(WS_URL, max_size=32 * 1024 * 1024,
                                  ping_interval=20, ping_timeout=20) as ws:
        rid = 1000
        bn = int((await _call(ws, "eth_blockNumber", [], rid))["result"], 16)

        if want("order_filled_v2_buy_maker", "order_filled_v2_sell_maker",
                "order_filled_v2_token_to_token"):
            buy_maker = tok2tok = None
            start = bn - 30
            while (buy_maker is None or tok2tok is None) and start < bn:
                rid += 1
                msg = await _call(ws, "eth_getLogs", [{
                    "fromBlock": hex(start), "toBlock": hex(min(start + 5, bn)),
                    "address": EXCH_V2, "topics": [[ORDER_FILLED_TOPIC_V2]]}], rid)
                for lg in msg.get("result") or []:
                    d = decode_order_filled(lg.get("topics"), lg.get("data"))
                    if d is None and buy_maker is not None and tok2tok is None:
                        tok2tok = _slim(lg)
                    elif d and d["maker_side"] == "BUY" and buy_maker is None:
                        buy_maker = _slim(lg)
                start += 6
            got["order_filled_v2_buy_maker"] = buy_maker
            got["order_filled_v2_token_to_token"] = tok2tok

            if buy_maker:
                dh = (buy_maker["data"] or "0x").lower().replace("0x", "")
                w = [dh[i * 64:(i + 1) * 64] for i in range(len(dh) // 64)]
                w[0], w[1] = w[1], w[0]      # makerAssetId <-> takerAssetId
                w[2], w[3] = w[3], w[2]      # makerAmount  <-> takerAmount
                got["order_filled_v2_sell_maker"] = {**buy_maker, "data": "0x" + "".join(w)}

        for et, key, topic in (("split", "position_split", CTF_SPLIT_TOPIC),
                               ("merge", "positions_merge", CTF_MERGE_TOPIC),
                               ("redeem", "payout_redemption", CTF_REDEEM_TOPIC)):
            if not want(key):
                continue
            for tx in ctf.get(et, []):
                rid += 1
                lg = await _receipt_log(ws, tx, topic.lower(), rid)
                if not lg:
                    continue
                d = decode_ctf_event(et, lg.get("topics"), lg.get("data"))
                if d and d["collateral"] in _CTF_COLLATERAL and d["amount"] > 0:
                    got[key] = lg
                    break

        if want("condition_resolution"):
            # Oracle reports land every few minutes; scan back in small
            # chunks (publicnode rejects getLogs ranges over ~2.5k blocks)
            # until a clean BINARY report decodes. No DB needed.
            hi = bn
            while "condition_resolution" not in got and hi > bn - 60_000:
                rid += 1
                msg = await _call(ws, "eth_getLogs", [{
                    "fromBlock": hex(hi - 2000), "toBlock": hex(hi),
                    "address": CONDITIONAL_TOKENS,
                    "topics": [[CONDITION_RESOLUTION_TOPIC]]}], rid)
                for lg in msg.get("result") or []:
                    d = decode_condition_resolution(lg.get("topics"), lg.get("data"))
                    if d and payout_yes_from_numerators(
                            d["payout_numerators"]) in (0.0, 1.0):
                        got["condition_resolution"] = _slim(lg)
                        break
                hi -= 2001
    return got


_META = {
    "order_filled_v2_buy_maker": {
        "event": "OrderFilled (V2)", "topic0": ORDER_FILLED_TOPIC_V2,
        "orientation": "maker BUY — makerAssetId=0 (USDC), takerAssetId=token",
        "source": "eth_getLogs on wss://polygon-bor-rpc.publicnode.com (REAL)"},
    "order_filled_v2_sell_maker": {
        "event": "OrderFilled (V2)", "topic0": ORDER_FILLED_TOPIC_V2,
        "orientation": "maker SELL — makerAssetId=token, takerAssetId=0 (USDC)",
        "source": "DERIVED (leg-swap of the real buy-maker capture)",
        "_derivation": "The live V2 exchange never emits a maker-SELL OrderFilled "
                       "(the USDC leg is always on the maker side; SELLs surface via "
                       "the taker role). This is the real buy-maker capture with data "
                       "words [0]<->[1] and [2]<->[3] swapped, so it decodes to an "
                       "identical token_id/usdc/shares/price but exercises the "
                       "decoder's SELL branch. It exists only to guard that branch."},
    "order_filled_v2_token_to_token": {
        "event": "OrderFilled (V2)", "topic0": ORDER_FILLED_TOPIC_V2,
        "orientation": "token<->token — neither leg is USDC (asset_id 0); decoder "
                       "returns None (no priceable USDC leg)",
        "source": "eth_getLogs on wss://polygon-bor-rpc.publicnode.com (REAL)"},
    "position_split": {
        "event": "PositionSplit", "topic0": CTF_SPLIT_TOPIC,
        "layout": "topics=[sig,stakeholder,parentCollectionId,conditionId]; "
                  "data=[collateralToken,partition,amount]",
        "source": "eth_getTransactionReceipt on wss://polygon-bor-rpc.publicnode.com (REAL)"},
    "positions_merge": {
        "event": "PositionsMerge", "topic0": CTF_MERGE_TOPIC,
        "layout": "topics=[sig,stakeholder,parentCollectionId,conditionId]; "
                  "data=[collateralToken,partition,amount]",
        "source": "eth_getTransactionReceipt on wss://polygon-bor-rpc.publicnode.com (REAL)"},
    "payout_redemption": {
        "event": "PayoutRedemption", "topic0": CTF_REDEEM_TOPIC,
        "layout": "topics=[sig,redeemer,collateralToken,parentCollectionId]; "
                  "data=[conditionId,indexSets,payout] — OPPOSITE of split/merge",
        "source": "eth_getTransactionReceipt on wss://polygon-bor-rpc.publicnode.com (REAL)"},
    "condition_resolution": {
        "event": "ConditionResolution", "topic0": CONDITION_RESOLUTION_TOPIC,
        "layout": "topics=[sig,conditionId,oracle,questionId]; "
                  "data=[outcomeSlotCount,offset(0x40),len,numerators...] — "
                  "payoutNumerators IS the oracle report; slot i ↔ clobTokenIds[i]",
        "source": "eth_getLogs on wss://polygon-bor-rpc.publicnode.com (REAL), "
                  "canonical ConditionalTokens 0x4d97dcd9…"},
}


def main() -> int:
    only: set[str] | None = None
    if "--only" in sys.argv:
        only = {s.strip() for s in
                sys.argv[sys.argv.index("--only") + 1].split(",") if s.strip()}
        unknown = only - set(_META)
        if unknown:
            print(f"unknown fixture name(s): {sorted(unknown)}")
            return 2
    os.makedirs(FIX_DIR, exist_ok=True)
    got = asyncio.run(_capture(only))
    wanted = only or set(_META)
    missing = [k for k in _META if k in wanted and not got.get(k)]
    for key, meta in _META.items():
        if key not in wanted:
            continue
        log = got.get(key)
        if not log:
            print(f"!! MISSING {key}")
            continue
        doc = {"_comment": "Fixture for tests/polymarket/test_chain_decoders.py. "
                           "See the fixtures README. Do not hand-edit.",
               **meta, "log": {k: log[k] for k in _LOG_FIELDS}}
        with open(os.path.join(FIX_DIR, key + ".json"), "w", encoding="utf-8") as fh:
            json.dump(doc, fh, indent=2)
        print(f"wrote {key}: tx={log['transactionHash'][:14]}…")
    return 1 if missing else 0


if __name__ == "__main__":
    raise SystemExit(main())
