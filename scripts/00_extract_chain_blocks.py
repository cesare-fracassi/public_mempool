# scripts/00_extract_chain_blocks.py
"""
Extract, from an Ethereum node, the two things the mempool build needs from the chain: which block mined each
transaction hash, and the timestamp of each block. Nothing else is kept.

  data/raw/chain/blocks/blocks_<first>_<last>.parquet                block_number (Int64), block_timestamp (Int64, unix s)
  data/raw/chain/transactions/transactions_<first>_<last>.parquet    transaction_hash (str, 0x + 64 lower-case hex), block_number (Int64)

The layout and column names are the ones `06_local_bn_dumpster_combine.py` reads (its `--chain-dir`, default
data/raw/chain). One file pair per block range; a range is complete only when both files exist (the blocks file is
written last). Files are written with zstd compression.

How it works: one JSON-RPC call `eth_getBlockByNumber(<n>, false)` per block, sent in batches over a few connections.
The reply holds the block timestamp and the list of transaction hashes. Any Ethereum execution client answers it
(designed and tested with Erigon on a local machine; a hosted RPC endpoint works but is slower and rate-limited).

Block range: by default from the block after the last one already in --out-dir (block 0 for an empty folder) to the
latest FINALIZED block, so a chain reorganisation can never change what is stored. Other ends: --last-block N, or
--through-month YYYY-MM (the last block of that month, UTC; the script stops with an error if that block is not
finalized yet). Re-running with the same arguments continues where it stopped. The script refuses to leave a gap.

Usage (from the repository root):
    python scripts/00_extract_chain_blocks.py --benchmark 2000                # speed test, writes nothing
    python scripts/00_extract_chain_blocks.py                                 # everything up to the finalized block
    python scripts/00_extract_chain_blocks.py --through-month 2026-10         # up to the end of October 2026
    python scripts/00_extract_chain_blocks.py --first-block 20000000 --last-block 20000099 \
        --chunk-blocks 100 --out-dir /tmp/chain_test                          # small test

The node URL comes from --rpc-url or the environment variable ERIGON_RPC_URL (default http://127.0.0.1:8545).
Full chain (about 26 million blocks, about 3.8 billion transaction hashes): on the order of 100 to 150 GB of output.
"""
import argparse
import calendar
import datetime as dt
import http.client
import json
import os
import re
import sys
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import polars as pl

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")

HASH_RE = re.compile(r"^0x[0-9a-f]{64}$")
_local = threading.local()


class Rpc:
    """Minimal JSON-RPC client over one persistent HTTP connection, one instance per thread."""

    def __init__(self, url: str, timeout: int = 180):
        u = urllib.parse.urlparse(url)
        self.https = u.scheme == "https"
        self.host = u.hostname
        self.port = u.port or (443 if self.https else 80)
        self.path = u.path or "/"
        self.timeout = timeout
        self.conn = None

    def call(self, payload):
        last = None
        for attempt in range(1, 6):
            try:
                if self.conn is None:
                    cls = http.client.HTTPSConnection if self.https else http.client.HTTPConnection
                    self.conn = cls(self.host, self.port, timeout=self.timeout)
                self.conn.request("POST", self.path, body=json.dumps(payload),
                                  headers={"Content-Type": "application/json"})
                resp = self.conn.getresponse()
                data = resp.read()
                if resp.status != 200:
                    raise RuntimeError(f"HTTP {resp.status}: {data[:200]!r}")
                return json.loads(data)
            except Exception as e:  # noqa: BLE001
                last = e
                self.conn = None
                time.sleep(min(2 ** attempt, 30))
        raise RuntimeError(f"RPC call failed after 5 attempts: {last}")


def rpc(url: str) -> Rpc:
    if getattr(_local, "rpc", None) is None:
        _local.rpc = Rpc(url)
    return _local.rpc


def get_block(url: str, tag) -> dict:
    """One block header (no transactions) by number or tag ('finalized', 'latest')."""
    p = hex(tag) if isinstance(tag, int) else tag
    r = rpc(url).call({"jsonrpc": "2.0", "id": 1, "method": "eth_getBlockByNumber", "params": [p, False]})
    if r.get("error") or not r.get("result"):
        sys.exit(f"node returned no block for {p}: {r.get('error')}")
    return r["result"]


def fetch_batch(url: str, first: int, last: int):
    """Blocks first..last (inclusive): list of (block_number, timestamp, [tx hashes]) in block order."""
    payload = [{"jsonrpc": "2.0", "id": n, "method": "eth_getBlockByNumber", "params": [hex(n), False]}
               for n in range(first, last + 1)]
    replies = rpc(url).call(payload)
    if not isinstance(replies, list):
        raise RuntimeError(f"batch reply is not a list: {str(replies)[:200]}")
    by_id = {}
    for r in replies:
        if r.get("error") or not r.get("result"):
            raise RuntimeError(f"block {r.get('id')}: {r.get('error') or 'null result'}")
        by_id[r["id"]] = r["result"]
    out = []
    for n in range(first, last + 1):
        b = by_id.get(n)
        if b is None:
            raise RuntimeError(f"block {n} missing from the batch reply")
        if int(b["number"], 16) != n:
            raise RuntimeError(f"block {n}: reply carries number {b['number']}")
        txs = b["transactions"]
        if txs and not isinstance(txs[0], str):
            raise RuntimeError(f"block {n}: transactions are objects, expected hashes")
        out.append((n, int(b["timestamp"], 16), txs))
    return out


def fetch_range(url: str, first: int, last: int, batch: int, pool: ThreadPoolExecutor):
    spans = [(s, min(s + batch - 1, last)) for s in range(first, last + 1, batch)]
    parts = list(pool.map(lambda sp: fetch_batch(url, sp[0], sp[1]), spans))
    return [blk for part in parts for blk in part]


def existing_ranges(out_dir: Path):
    """Complete ranges in out_dir (both files present): sorted list of (first, last). Exits on a gap or overlap."""
    bl = {m.group(1, 2): p for p in (out_dir / "blocks").glob("blocks_*.parquet")
          if (m := re.match(r"blocks_(\d+)_(\d+)\.parquet$", p.name))}
    tx = {m.group(1, 2) for p in (out_dir / "transactions").glob("transactions_*.parquet")
          if (m := re.match(r"transactions_(\d+)_(\d+)\.parquet$", p.name))}
    rng = sorted((int(a), int(b)) for (a, b) in bl if (a, b) in tx)
    for (a0, b0), (a1, b1) in zip(rng, rng[1:]):
        if a1 != b0 + 1:
            sys.exit(f"gap or overlap in {out_dir}: range {a0}-{b0} is followed by {a1}-{b1}")
    return rng


def month_last_block(url: str, month: str) -> int:
    """Last block whose timestamp is before the first second of the month after `month` (YYYY-MM, UTC)."""
    y, m = (int(x) for x in month.split("-"))
    ny, nm = (y + 1, 1) if m == 12 else (y, m + 1)
    limit = calendar.timegm(dt.datetime(ny, nm, 1).timetuple())
    lo, hi = 0, int(get_block(url, "latest")["number"], 16)
    if int(get_block(url, hi)["timestamp"], 16) < limit:
        sys.exit(f"the chain has not reached the end of {month} (latest block is earlier than {ny}-{nm:02d}-01)")
    while lo < hi:  # first block with timestamp >= limit
        mid = (lo + hi) // 2
        if int(get_block(url, mid)["timestamp"], 16) >= limit:
            hi = mid
        else:
            lo = mid + 1
    return lo - 1


def write_chunk(out_dir: Path, first: int, last: int, blocks):
    """Check one range and write its two files (transactions first, blocks last)."""
    if len(blocks) != last - first + 1:
        sys.exit(f"range {first}-{last}: got {len(blocks)} blocks")
    nums = [b[0] for b in blocks]
    stamps = [b[1] for b in blocks]
    if any(stamps[i + 1] < stamps[i] for i in range(len(stamps) - 1)):
        sys.exit(f"range {first}-{last}: timestamps are not non-decreasing")
    th, tb = [], []
    for n, _, txs in blocks:
        th.extend(txs)
        tb.extend([n] * len(txs))
    bad = next((h for h in th if not HASH_RE.match(h)), None)
    if bad is not None:
        sys.exit(f"range {first}-{last}: malformed transaction hash {bad!r}")
    name = f"{first:010d}_{last:010d}.parquet"
    (out_dir / "transactions").mkdir(parents=True, exist_ok=True)
    (out_dir / "blocks").mkdir(parents=True, exist_ok=True)
    t_final = out_dir / "transactions" / f"transactions_{name}"
    b_final = out_dir / "blocks" / f"blocks_{name}"
    t_tmp, b_tmp = t_final.with_suffix(".parquet.tmp"), b_final.with_suffix(".parquet.tmp")
    pl.DataFrame({"transaction_hash": th, "block_number": tb},
                 schema={"transaction_hash": pl.Utf8, "block_number": pl.Int64}).write_parquet(t_tmp, compression="zstd")
    os.replace(t_tmp, t_final)
    pl.DataFrame({"block_number": nums, "block_timestamp": stamps},
                 schema={"block_number": pl.Int64, "block_timestamp": pl.Int64}).write_parquet(b_tmp, compression="zstd")
    os.replace(b_tmp, b_final)
    return len(th)


def main():
    ap = argparse.ArgumentParser(description="Extract block timestamps and transaction hashes from an Ethereum node.")
    ap.add_argument("--rpc-url", default=os.environ.get("ERIGON_RPC_URL", "http://127.0.0.1:8545"))
    ap.add_argument("--out-dir", default="data/raw/chain", type=Path)
    ap.add_argument("--first-block", type=int, default=None, help="default: the block after the last one in --out-dir (0 if empty)")
    end = ap.add_mutually_exclusive_group()
    end.add_argument("--last-block", type=int, default=None, help="default: the latest finalized block")
    end.add_argument("--through-month", default=None, metavar="YYYY-MM", help="last block of this month (UTC)")
    ap.add_argument("--allow-unfinalized", action="store_true", help="allow an end beyond the finalized block (reorgs can then change the data)")
    ap.add_argument("--chunk-blocks", type=int, default=20000, help="blocks per output file pair")
    ap.add_argument("--batch", type=int, default=50, help="blocks per JSON-RPC batch request")
    ap.add_argument("--workers", type=int, default=8, help="parallel connections")
    ap.add_argument("--benchmark", type=int, default=0, metavar="N", help="fetch N blocks, print the speed, write nothing")
    a = ap.parse_args()

    url = a.rpc_url
    fin = int(get_block(url, "finalized")["number"], 16)
    print(f"node {url}: finalized block {fin:,}", flush=True)

    if a.benchmark:
        first = a.first_block if a.first_block is not None else max(0, fin - a.benchmark - 1)
        t0 = time.time()
        with ThreadPoolExecutor(a.workers) as pool:
            blocks = fetch_range(url, first, first + a.benchmark - 1, a.batch, pool)
        dtime = time.time() - t0
        ntx = sum(len(b[2]) for b in blocks)
        print(f"benchmark: {len(blocks):,} blocks, {ntx:,} transactions in {dtime:.1f}s = "
              f"{len(blocks)/dtime:,.0f} blocks/s, {ntx/dtime:,.0f} tx/s with {a.workers} workers, batch {a.batch}")
        return

    out = a.out_dir
    rng = existing_ranges(out)
    first = a.first_block if a.first_block is not None else (rng[-1][1] + 1 if rng else 0)
    if a.through_month:
        last = month_last_block(url, a.through_month)
        print(f"last block of {a.through_month}: {last:,}", flush=True)
    elif a.last_block is not None:
        last = a.last_block
    else:
        last = fin
    if last > fin and not a.allow_unfinalized:
        sys.exit(f"block {last:,} is not finalized yet (finalized: {fin:,}); wait, or pass --allow-unfinalized")
    if rng and first != rng[-1][1] + 1:
        sys.exit(f"--first-block {first} does not continue the stored data, which ends at block {rng[-1][1]}")
    if first > last:
        print(f"nothing to do: stored up to block {first - 1:,}, requested end {last:,}")
        return

    total = last - first + 1
    print(f"extracting blocks {first:,}..{last:,} ({total:,}) into {out}, chunks of {a.chunk_blocks:,}", flush=True)
    t_start = time.time()
    done_blocks = done_tx = 0
    with ThreadPoolExecutor(a.workers) as pool:
        s = first
        while s <= last:
            e = min(s + a.chunk_blocks - 1, last)
            blocks = fetch_range(url, s, e, a.batch, pool)
            n = write_chunk(out, s, e, blocks)
            done_blocks += e - s + 1
            done_tx += n
            el = time.time() - t_start
            eta = el / done_blocks * (total - done_blocks)
            print(f"blocks {s:,}..{e:,}: {n:,} tx | {done_blocks:,}/{total:,} blocks, {done_tx:,} tx, "
                  f"{el/60:.1f} min elapsed, ETA {eta/60:.0f} min", flush=True)
            s = e + 1
    print(f"CHAIN EXTRACT DONE: blocks {first:,}..{last:,}, {done_tx:,} transactions, {(time.time()-t_start)/60:.1f} min", flush=True)


if __name__ == "__main__":
    main()
