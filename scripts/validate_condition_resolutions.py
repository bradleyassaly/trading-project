"""Validate (and optionally backfill) chain ConditionResolution truth.

The wallet-stream now decodes the ConditionalTokens ConditionResolution
event — the oracle report itself — into market_resolutions rows
(source='chain_condition_resolution', rank 85). Its ONE mapping assumption
is that Gamma's clobTokenIds order matches CTF outcome-slot order, i.e.
markets.yes_token_id (= clobTokenIds[0]) is outcome slot 0, so
payoutNumerators[0] is the YES payout. This script is the empirical gate
for that assumption:

  --validate (default, NO WRITES): scan recent ConditionResolution logs
      off Polygon (publicnode WS, small getLogs chunks — the provider
      rejects ranges over ~2.5k blocks), decode with the PRODUCTION pure
      decoder, join against market_resolutions rows that already carry
      authoritative truth (source_rank >= 70, resolves_yes NOT NULL), and
      report agreement under the YES=slot-0 hypothesis. The first samples
      are printed with question text for eyeball confirmation.

  --apply: after --validate has been accepted, write rank-85 rows for
      scanned conditions that LACK authoritative truth (no row, NULL
      verdict, or source_rank < 70), including [1,1] voids (payout_yes=0.5,
      NULL verdict) — same semantics as the live handler. record_resolution
      is monotonic write-once-wins, so re-runs are idempotent; agreeing
      high-rank rows are deliberately NOT rewritten (no churn).

Acceptance run 2026-07-29: 259 comparisons, ZERO disagreements — 59/59
against existing high-rank rows (47 clob_winner + 12 data_api_positions)
and 200/200 against live CLOB winner flags on an independent random
sample, with 0 yes-token identity mismatches. YES = outcome slot 0
confirmed; the live source is trusted on that basis.

Note: publicnode prunes log history after ~2 days, so a scan reaches back
about 88k blocks regardless of --blocks (it stops cleanly at the prune
boundary). Older gaps need an archive node.

Usage:
    python scripts/validate_condition_resolutions.py                # validate
    python scripts/validate_condition_resolutions.py --blocks 250000
    python scripts/validate_condition_resolutions.py --apply
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import time

import websockets

from trading_platform.polymarket.db_connection import get_connection
from trading_platform.polymarket.wallet_stream import (
    CONDITION_RESOLUTION_TOPIC,
    CONDITIONAL_TOKENS,
    decode_condition_resolution,
    payout_yes_from_numerators,
)

logger = logging.getLogger(__name__)

WS_URL = "wss://polygon-bor-rpc.publicnode.com"
CHUNK = 2000          # publicnode rejects ranges over ~2.5k blocks
MIN_CHUNK = 250


async def _call(ws, method, params, rid):
    await ws.send(json.dumps({"jsonrpc": "2.0", "id": rid,
                              "method": method, "params": params}))
    for _ in range(8):
        msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=30))
        if msg.get("id") == rid:
            return msg
    return {}


async def scan_resolutions(max_blocks: int, stop_check=None) -> dict[str, dict]:
    """cid -> {numerators, block} for every ConditionResolution in the last
    max_blocks blocks (newest first). stop_check(found) may end the scan
    early once the caller has enough matched conditions."""
    found: dict[str, dict] = {}
    async with websockets.connect(WS_URL, max_size=32 * 1024 * 1024,
                                  ping_interval=20, ping_timeout=20) as ws:
        rid = 500
        head = int((await _call(ws, "eth_blockNumber", [], rid))["result"], 16)
        floor = head - max_blocks
        hi = head
        chunk = CHUNK
        while hi > floor:
            lo = max(floor, hi - chunk)
            rid += 1
            msg = await _call(ws, "eth_getLogs", [{
                "fromBlock": hex(lo), "toBlock": hex(hi),
                "address": CONDITIONAL_TOKENS,
                "topics": [[CONDITION_RESOLUTION_TOPIC]]}], rid)
            if "error" in msg:
                err = msg["error"] or {}
                # publicnode prunes log history after ~2 days (-32701) —
                # that's the end of scannable history, not a failure.
                if err.get("code") == -32701 or "pruned" in str(err).lower():
                    logger.info("history pruned at block %d — stopping scan "
                                "(%d blocks covered)", hi, head - hi)
                    break
                if chunk <= MIN_CHUNK:
                    raise RuntimeError(f"getLogs failing at min chunk: {err}")
                chunk = max(MIN_CHUNK, chunk // 2)
                continue
            for lg in msg.get("result") or []:
                d = decode_condition_resolution(lg.get("topics"), lg.get("data"))
                if d is None:
                    continue
                found.setdefault(d["condition_id"], {
                    "numerators": d["payout_numerators"],
                    "block": int(lg["blockNumber"], 16),
                })
            hi = lo - 1
            if stop_check and await stop_check(found):
                break
            await asyncio.sleep(0.1)
    return found


async def _block_timestamps(blocks: list[int]) -> dict[int, int]:
    """Exact block timestamps for resolved_at, pipelined.

    A backfill pass can touch thousands of distinct blocks; one
    request-response round trip each would take half an hour. Batched
    request ids let the provider answer a whole window at once — the
    responses are matched by id, so out-of-order replies are fine."""
    out: dict[int, int] = {}
    if not blocks:
        return out
    async with websockets.connect(WS_URL, max_size=32 * 1024 * 1024,
                                  ping_interval=20, ping_timeout=20) as ws:
        for i in range(0, len(blocks), 50):
            window = blocks[i:i + 50]
            ids = {}
            for n, b in enumerate(window):
                rid = 9000 + i + n
                ids[rid] = b
                await ws.send(json.dumps({
                    "jsonrpc": "2.0", "id": rid,
                    "method": "eth_getBlockByNumber", "params": [hex(b), False]}))
            pending = set(ids)
            while pending:
                try:
                    msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=30))
                except asyncio.TimeoutError:
                    break
                rid = msg.get("id")
                if rid not in pending:
                    continue
                pending.discard(rid)
                ts = (msg.get("result") or {}).get("timestamp")
                if ts:
                    out[ids[rid]] = int(ts, 16)
            if i and i % 1000 == 0:
                logger.info("block timestamps: %d/%d", len(out), len(blocks))
    return out


def _db_rows(cids: list[str]) -> dict[str, dict]:
    """cid -> {yes_tok, no_tok, question, source, rank, resolves_yes}."""
    out: dict[str, dict] = {}
    if not cids:
        return out
    conn = get_connection()
    try:
        for i in range(0, len(cids), 500):
            chunk = cids[i:i + 500]
            ph = ",".join("?" for _ in chunk)
            for r in conn.execute(
                    f"""SELECT m.condition_id, m.yes_token_id, m.no_token_id,
                               m.question, mr.source, mr.source_rank,
                               mr.resolves_yes
                          FROM markets m
                          LEFT JOIN market_resolutions mr
                                 ON mr.condition_id = m.condition_id
                         WHERE m.condition_id IN ({ph})""", chunk).fetchall():
                out[str(r[0]).lower()] = {
                    "yes_tok": r[1], "no_tok": r[2], "question": r[3] or "",
                    "source": r[4], "rank": int(r[5] or 0),
                    "resolves_yes": r[6],
                }
    finally:
        try: conn.close()
        except Exception: pass
    return out


def _classify(found: dict[str, dict], rows: dict[str, dict]) -> dict:
    """Join chain payouts against DB truth under the YES=slot-0 hypothesis."""
    matched, disagreements, samples, ties, gaps = [], [], [], [], []
    for cid, info in found.items():
        payout = payout_yes_from_numerators(info["numerators"])
        if payout is None:
            continue                      # non-binary
        row = rows.get(cid)
        if not row or not row.get("yes_tok") or not row.get("no_tok"):
            continue                      # not in our indexed universe
        if payout not in (0.0, 1.0):
            # A [1,1] void pays both sides $0.50 — real resolution truth with
            # no YES/NO winner, so it can't be scored for agreement, but it
            # IS worth writing (a heuristic row claiming a full $1 winner
            # here is simply wrong). Falls through to the gap check below.
            ties.append(cid)
            if row["rank"] < 70 or row["resolves_yes"] is None:
                gaps.append(cid)
            continue
        chain_yes = int(payout)
        if row["rank"] >= 70 and row["resolves_yes"] is not None:
            agree = int(row["resolves_yes"]) == chain_yes
            matched.append((cid, agree, row["source"]))
            if len(samples) < 5:
                samples.append({
                    "cid": cid, "question": row["question"][:70],
                    "numerators": info["numerators"], "chain_yes": chain_yes,
                    "db_yes": int(row["resolves_yes"]), "db_source": row["source"],
                    "agree": agree,
                })
            if not agree:
                disagreements.append({
                    "cid": cid, "question": row["question"][:70],
                    "numerators": info["numerators"], "chain_yes": chain_yes,
                    "db_yes": int(row["resolves_yes"]), "db_source": row["source"],
                })
        elif row["rank"] < 70 or row["resolves_yes"] is None:
            gaps.append(cid)
    by_source: dict[str, list[int]] = {}
    for _, agree, src in matched:
        a = by_source.setdefault(src, [0, 0])
        a[0] += int(agree)
        a[1] += 1
    return {
        "matched": len(matched),
        "agree": sum(1 for _, a, _ in matched if a),
        "disagree": len(disagreements),
        "by_source": {s: f"{a}/{n}" for s, (a, n) in by_source.items()},
        "ties_50_50": len(ties),
        "gap_candidates": gaps,
        "samples": samples,
        "disagreements": disagreements[:20],
    }


def clob_probe(found: dict[str, dict], rows: dict[str, dict], n: int,
               sleep_s: float = 0.1) -> dict:
    """Direct chain-vs-CLOB check on the SAME conditions, independent of
    what's already in market_resolutions: fetch /markets/{cid} winner flags
    live for a random sample of scanned conditions and compare against the
    chain payout under YES=slot-0. Also cross-checks token identity
    (markets.yes_token_id vs the CLOB token labeled Yes). Only works for
    literal Yes/No-labeled markets (parse_clob_resolution requirement);
    team-name binary markets are counted as skipped — the slot-order
    mapping being validated is label-agnostic, so Yes/No coverage
    generalizes."""
    import random

    from trading_platform.polymarket.clob_resolution_backfill import (
        fetch_clob_market,
        parse_clob_resolution,
    )
    cands = [c for c in found
             if rows.get(c, {}).get("yes_tok") and rows.get(c, {}).get("no_tok")
             and payout_yes_from_numerators(found[c]["numerators"]) in (0.0, 1.0)]
    random.Random(42).shuffle(cands)
    agree = disagree = skipped = token_mismatch = 0
    disagreements = []
    for cid in cands:
        if agree + disagree >= n:
            break
        parsed = parse_clob_resolution(fetch_clob_market(cid))
        time.sleep(sleep_s)
        if not parsed:
            skipped += 1
            continue
        chain_yes = int(payout_yes_from_numerators(found[cid]["numerators"]))
        if (parsed.get("yes_token_id")
                and str(rows[cid]["yes_tok"]) != str(parsed["yes_token_id"])):
            token_mismatch += 1
        if int(parsed["resolves_yes"]) == chain_yes:
            agree += 1
        else:
            disagree += 1
            disagreements.append({
                "cid": cid, "question": (parsed.get("question") or "")[:70],
                "numerators": found[cid]["numerators"],
                "chain_yes": chain_yes, "clob_yes": int(parsed["resolves_yes"]),
            })
        if (agree + disagree) % 50 == 0:
            logger.info("clob probe: %d/%d compared, %d agree",
                        agree + disagree, n, agree)
    return {"compared": agree + disagree, "agree": agree,
            "disagree": disagree, "skipped_non_yesno_or_unsettled": skipped,
            "yes_token_identity_mismatches": token_mismatch,
            "disagreements": disagreements[:20]}


def run(max_blocks: int, apply: bool, min_matches: int,
        apply_limit: int, clob_probe_n: int = 0) -> dict:
    t0 = time.time()

    async def _main():
        checked_at = {"n": 0}

        async def stop_check(found):
            # Re-join the DB every ~20k blocks of scan; stop once we have
            # enough matched high-rank conditions to call the verdict.
            if len(found) - checked_at["n"] < 300:
                return False
            checked_at["n"] = len(found)
            rows = _db_rows(list(found.keys()))
            c = _classify(found, rows)
            logger.info("scan progress: %d cids, %d matched (%d agree)",
                        len(found), c["matched"], c["agree"])
            return c["matched"] >= min_matches

        found = await scan_resolutions(max_blocks, stop_check)
        rows = _db_rows(list(found.keys()))
        result = _classify(found, rows)
        result.update({
            "scanned_cids": len(found),
            "in_markets_with_tokens": sum(
                1 for c in found
                if rows.get(c, {}).get("yes_tok") and rows.get(c, {}).get("no_tok")),
        })
        if clob_probe_n:
            result["clob_probe"] = await asyncio.to_thread(
                clob_probe, found, rows, clob_probe_n)
        result["elapsed_seconds"] = round(time.time() - t0, 1)

        if apply:
            gaps = result["gap_candidates"][:apply_limit]
            blocks = sorted({found[c]["block"] for c in gaps})
            ts_map = await _block_timestamps(blocks)
            from trading_platform.polymarket.resolutions import record_resolution
            counts: dict[str, int] = {}
            for cid in gaps:
                info = found[cid]
                row = rows[cid]
                payout = payout_yes_from_numerators(info["numerators"])
                num = info["numerators"]
                resolves_yes = (1 if num[0] == sum(num)
                                else 0 if num[0] == 0 else None)
                status = record_resolution(
                    cid,
                    source="chain_condition_resolution",
                    resolves_yes=resolves_yes,
                    payout_yes=payout,
                    winning_outcome=("Yes" if resolves_yes == 1 else
                                     "No" if resolves_yes == 0 else None),
                    yes_token_id=str(row["yes_tok"]),
                    no_token_id=str(row["no_tok"]),
                    resolved_at=ts_map.get(info["block"]),
                    question=row["question"] or None,
                    details={"numerators": num, "block": info["block"],
                             "src": "validate_condition_resolutions --apply"},
                )
                counts[status] = counts.get(status, 0) + 1
            result["apply_counts"] = counts
        result["gap_candidates"] = len(result["gap_candidates"])
        return result

    return asyncio.run(_main())


def main() -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--blocks", type=int, default=130_000,
                    help="how far back to scan (~43k blocks/day)")
    ap.add_argument("--min-matches", type=int, default=200,
                    help="stop scanning once this many high-rank matches found")
    ap.add_argument("--apply", action="store_true",
                    help="write rank-85 rows for conditions lacking truth")
    ap.add_argument("--apply-limit", type=int, default=2000)
    ap.add_argument("--clob-probe", type=int, default=0,
                    help="also live-compare N scanned conditions against "
                         "CLOB winner flags (independent of DB rows)")
    args = ap.parse_args()
    out = run(args.blocks, args.apply, args.min_matches, args.apply_limit,
              clob_probe_n=args.clob_probe)
    print(json.dumps(out, indent=1, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
