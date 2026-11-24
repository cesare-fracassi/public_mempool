# scripts/05_xatu_mempool_all_days.py
"""
Xatu public mempool (ethPandaOps `mempool_transaction`), every day, reduced to one row per hash.

Source (public, CC BY 4.0; NON-padded month and day in the path):
    https://data.ethpandaops.io/xatu/mainnet/databases/default/mempool_transaction/YYYY/M/D.parquet
One file per UTC day (by the sighting time event_date_time). Each row is one sighting of a transaction by a
sentry, so a hash appears many times in a file, and a hash seen on several days is in each of those files.

STAGE 1 (default): one reduced file per day.
  Only the two columns `hash` and `event_date_time` are fetched, with HTTP range requests (the other columns are about
  half of the file's bytes and are never downloaded), so no raw Xatu file is ever written to disk and there is nothing
  to delete afterwards. Per day the sightings are collapsed to one row per hash:
      daily/YYYY-MM-DD.parquet      hash (String, lowercase 0x + 64 hex), first_ts, last_ts (Datetime ms, UTC)
  Checks per day (also written to manifest.csv): rows read equal the parquet footer row count, every hash has the
  0x + 64 lowercase hex format (the invalid rows are all-zero placeholder values, about 0.02% of rows; they are dropped
  and counted), all sighting times fall inside the file's UTC day,
  sightings per UTC hour (24 numbers, for judging collection gaps). A day whose daily file exists is skipped.

STAGE 2 (--merge): one row per hash over ALL days, in 256 shard files by the first two hex characters of the hash:
      merged/shard_XX.parquet       hash, first_ts = earliest first_ts, last_ts = latest last_ts   (sorted by hash)
  Daily files are routed in batches of days into routed/<shard>/b<k>.parquet, then each shard is deduplicated.
  `--verify N` recomputes N random merged hashes from the daily files and checks the totals.

Use in the pipeline: scripts/00_run_all_mempool.py (stage xatu) runs both stages with a frozen --end date, and
scripts/06_local_bn_dumpster_combine.py reads merged/shard_XX.parquet (first_ts becomes first_seen). last_ts is the
last mempool sighting, not the inclusion time; it is not used downstream (the block time comes from Erigon).
Do not extend a finished merge by adding days: stage 2 marks batches by index, so a new day changes the last
batch without invalidating its mark. Rebuild the merge (new out-root) for a new end date.

Pacing (global rule 21): one request at a time, PAUSE_RANGE seconds between range requests, PAUSE_DAY between days,
stop on HTTP 403 or 429, three tries then stop on other errors. No parallel requests to the host.

Usage (repo root; all data goes under --out-root, default data/interim/xatu_full):
    python scripts/05_xatu_mempool_all_days.py --days 2023-04-05 2025-09-19      # test days
    python scripts/05_xatu_mempool_all_days.py                                   # 2023-03-03 to yesterday
    python scripts/05_xatu_mempool_all_days.py --merge --verify 300
"""
import argparse
import csv
import datetime as dt
import io
import json
import os
import random
import sys
import time
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import requests

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")

BASE = "https://data.ethpandaops.io/xatu/mainnet/databases/default/mempool_transaction"
HEADERS = {"User-Agent": "academic-research-downloader"}
FIRST_DAY = dt.date(2023, 3, 3)
PAUSE_RANGE = 0.3
PAUSE_DAY = 2.0
MAX_TRIES = 3
TAIL_BYTES = 2 * 1024 * 1024
HASH_RE = r"^0x[0-9a-f]{64}$"
COLLAPSE_ROWS = 8_000_000
MANIFEST_COLS = ["day", "status", "rows", "hashes", "invalid_hash_rows", "min_ts", "max_ts", "outside_day_rows",
                 "net_mb", "requests", "file_mb", "seconds", "hourly_rows"]


class Blocked(Exception):
    pass


class Missing(Exception):
    pass


class RangeFile(io.RawIOBase):
    """Read-only file object over an HTTP URL; reads are served from prefetched ranges, else by one range request."""

    def __init__(self, url):
        self.url = url
        self.pos = 0
        self.cache = []
        self.n_requests = 0
        self.net_bytes = 0
        r = requests.head(url, headers=HEADERS, timeout=(30, 60), allow_redirects=True)
        if r.status_code == 404:
            raise Missing(url)
        if r.status_code in (403, 429):
            raise Blocked(f"HTTP {r.status_code} on HEAD {url}")
        r.raise_for_status()
        if r.headers.get("Accept-Ranges") != "bytes" or "Content-Length" not in r.headers:
            raise IOError(f"server does not support range requests for {url}")
        self.size = int(r.headers["Content-Length"])

    def seekable(self):
        return True

    def readable(self):
        return True

    def tell(self):
        return self.pos

    def seek(self, off, whence=0):
        self.pos = off if whence == 0 else (self.pos + off if whence == 1 else self.size + off)
        return self.pos

    def _get(self, start, end):
        for attempt in range(1, MAX_TRIES + 1):
            try:
                with requests.get(self.url, headers={**HEADERS, "Range": f"bytes={start}-{end}"}, timeout=(30, 300),
                                  stream=True) as r:
                    if r.status_code in (403, 429):
                        raise Blocked(f"HTTP {r.status_code} on range {start}-{end}")
                    if r.status_code != 206:
                        raise IOError(f"HTTP {r.status_code} (expected 206) on range {start}-{end}")
                    data = r.content
                if len(data) != end - start + 1:
                    raise IOError(f"short range read {len(data)} of {end - start + 1}")
                self.n_requests += 1
                self.net_bytes += len(data)
                return data
            except (requests.RequestException, IOError):
                if attempt == MAX_TRIES:
                    raise
                time.sleep(30)

    def prefetch(self, start, end):
        end = min(end, self.size - 1)
        self.cache.append((start, self._get(start, end)))

    def drop_cache(self):
        keep = self.size - TAIL_BYTES
        self.cache = [(s, b) for s, b in self.cache if s >= keep]

    def _from_cache(self, pos, n):
        """Bytes pos..pos+n-1 assembled from the prefetched ranges, or None when any part is not cached."""
        out, cur, end = [], pos, pos + n
        while cur < end:
            for s, b in self.cache:
                if s <= cur < s + len(b):
                    take = min(end, s + len(b))
                    out.append(b[cur - s:take - s])
                    cur = take
                    break
            else:
                nxt = min([s for s, b in self.cache if s > cur] + [end])   # fetch only the uncached gap
                if nxt - cur > 4 * 1024 * 1024:
                    return None
                out.append(self._get(cur, nxt - 1))
                cur = nxt
        return b"".join(out)

    def read(self, n=-1):
        if n < 0 or self.pos + n > self.size:
            n = self.size - self.pos
        if n <= 0:
            return b""
        out = self._from_cache(self.pos, n)       # pyarrow may ask for two adjacent column chunks in one read
        if out is not None:
            self.pos += n
            return out
        data = self._get(self.pos, self.pos + n - 1)
        self.pos += n
        return data

    def readinto(self, b):
        d = self.read(len(b))
        b[:len(d)] = d
        return len(d)


def chunk_range(cc):
    offs = [o for o in (cc.dictionary_page_offset, cc.data_page_offset) if o]
    start = min(offs)
    return start, start + cc.total_compressed_size - 1


def collapse(frames):
    return (pl.concat(frames).group_by("hash")
            .agg(pl.col("first_ts").min(), pl.col("last_ts").max()))


def reduce_day(day: dt.date, out_dir: Path):
    url = f"{BASE}/{day.year}/{day.month}/{day.day}.parquet"
    t0 = time.time()
    f = RangeFile(url)
    f.prefetch(f.size - min(f.size, TAIL_BYTES), f.size - 1)
    pf = pq.ParquetFile(f)
    md = pf.metadata
    names = [md.schema.column(i).name for i in range(md.num_columns)]
    ih, it = names.index("hash"), names.index("event_date_time")
    parts, acc_rows, nrows, invalid = [], 0, 0, 0
    tmin, tmax, outside = None, None, 0
    d0 = dt.datetime(day.year, day.month, day.day)
    d1 = d0 + dt.timedelta(days=1)
    hourly = [0] * 24
    for g in range(md.num_row_groups):
        for c in (ih, it):
            s, e = chunk_range(md.row_group(g).column(c))
            f.prefetch(s, e)
            time.sleep(PAUSE_RANGE)
        t = pl.from_arrow(pf.read_row_group(g, columns=["hash", "event_date_time"]))
        f.drop_cache()
        t = t.select(pl.col("hash").cast(pl.Utf8).str.to_lowercase().alias("hash"),
                     pl.col("event_date_time").dt.replace_time_zone(None).cast(pl.Datetime("ms")).alias("ts"))
        nrows += t.height
        bad = ~t["hash"].str.contains(HASH_RE)
        invalid += int(bad.sum())
        t = t.filter(~bad)
        if t.height:
            lo, hi = t["ts"].min(), t["ts"].max()
            tmin = lo if tmin is None or lo < tmin else tmin
            tmax = hi if tmax is None or hi > tmax else tmax
            outside += int(((t["ts"] < d0) | (t["ts"] >= d1)).sum())
            for h, n in t.group_by(pl.col("ts").dt.hour().alias("h")).len().iter_rows():
                hourly[h] += n
            p = t.group_by("hash").agg(pl.col("ts").min().alias("first_ts"), pl.col("ts").max().alias("last_ts"))
            parts.append(p)
            acc_rows += p.height
            if acc_rows > COLLAPSE_ROWS:
                parts = [collapse(parts)]
                acc_rows = parts[0].height
    if nrows != md.num_rows:
        raise IOError(f"{day}: read {nrows} rows, footer says {md.num_rows}")
    if parts:
        df = collapse(parts).sort("hash")
    else:                              # an empty source file (Xatu published 0 rows for 2023-07-17 to 2023-07-21)
        df = pl.DataFrame(schema={"hash": pl.Utf8, "first_ts": pl.Datetime("ms"), "last_ts": pl.Datetime("ms")})
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"{day.isoformat()}.parquet"
    tmp = out.with_suffix(".parquet.tmp")
    df.write_parquet(tmp, compression="zstd")
    os.replace(tmp, out)
    return {"day": day.isoformat(), "status": "ok" if df.height else "empty_source_file", "rows": nrows, "hashes": df.height, "invalid_hash_rows": invalid,
            "min_ts": tmin, "max_ts": tmax, "outside_day_rows": outside, "net_mb": round(f.net_bytes / 1e6, 1),
            "requests": f.n_requests, "file_mb": round(out.stat().st_size / 1e6, 1), "seconds": round(time.time() - t0),
            "hourly_rows": json.dumps(hourly)}


def append_manifest(path: Path, row: dict):
    new = not path.exists()
    with open(path, "a", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=MANIFEST_COLS)
        if new:
            w.writeheader()
        w.writerow({k: row.get(k, "") for k in MANIFEST_COLS})


def stage1(a, root: Path):
    daily = root / "daily"
    manifest = root / "manifest.csv"
    stop_file = root / "STOP"
    if a.days:
        days = [dt.date.fromisoformat(d) for d in a.days]
    else:
        start = dt.date.fromisoformat(a.start)
        end = dt.date.fromisoformat(a.end) if a.end else dt.date.today() - dt.timedelta(days=1)
        days = [start + dt.timedelta(days=i) for i in range((end - start).days + 1)]
    if a.part:                      # "i/n": this process takes every n-th day starting at i (n processes in parallel)
        pi, pn = (int(x) for x in a.part.split("/"))
        days = [d for k, d in enumerate(days) if k % pn == pi]
        manifest = root / f"manifest_p{pi}of{pn}.csv"
    if a.limit:
        days = days[:a.limit]
    print(f"=== Xatu reduce: {len(days)} days {days[0]}..{days[-1]} into {daily} ===", flush=True)
    missing_streak, done, t_start = 0, 0, time.time()
    for i, day in enumerate(days, 1):
        if stop_file.exists():
            print("STOP file present (another worker was blocked or the user asked to stop): stopping", flush=True)
            break
        if (daily / f"{day.isoformat()}.parquet").exists():
            missing_streak = 0
            continue
        try:
            row = reduce_day(day, daily)
        except Missing:
            row = {"day": day.isoformat(), "status": "missing"}
            missing_streak += 1
            print(f"[{i}/{len(days)}] {day}: not published (404)", flush=True)
            append_manifest(manifest, row)
            if missing_streak >= 5:
                print("5 missing days in a row: assuming the end of the archive, stopping", flush=True)
                break
            time.sleep(PAUSE_DAY)
            continue
        except Blocked as e:
            stop_file.write_text(f"{day}: {e}")
            sys.exit(f"{e}: possible rate-limit block; stopping (STOP file written, other workers stop too)")
        missing_streak = 0
        done += 1
        append_manifest(manifest, row)
        flag = ""
        if row["outside_day_rows"] or row["invalid_hash_rows"]:
            flag = f"  CHECK outside_day={row['outside_day_rows']} invalid_hash={row['invalid_hash_rows']}"
        if row["status"] != "ok":
            flag += f"  STATUS {row['status']}"
        print(f"[{i}/{len(days)}] {day}: {row['rows']:,} rows, {row['hashes']:,} hashes, {row['net_mb']:.0f} MB fetched in "
              f"{row['requests']} requests, file {row['file_mb']:.0f} MB, {row['seconds']} s, elapsed "
              f"{(time.time() - t_start) / 60:.0f} min{flag}", flush=True)
        time.sleep(PAUSE_DAY)
    print(f"STAGE 1 DONE: {done} days built this run", flush=True)


def shard_of():
    return pl.col("hash").str.slice(2, 2).alias("shard")


def stage2(a, root: Path):
    daily = sorted((root / "daily").glob("*.parquet"))
    routed, merged = root / "routed", root / "merged"
    (routed / "_done").mkdir(parents=True, exist_ok=True)
    merged.mkdir(parents=True, exist_ok=True)
    batches = [daily[i:i + a.batch_days] for i in range(0, len(daily), a.batch_days)]
    print(f"=== merge: {len(daily)} daily files, {len(batches)} batches of {a.batch_days} days ===", flush=True)
    t0 = time.time()
    rows_routed = 0
    for b, files in enumerate(batches):
        mark = routed / "_done" / f"b{b:04d}.json"
        if mark.exists():
            rows_routed += json.loads(mark.read_text())["rows"]
            continue
        df = collapse([pl.read_parquet(p) for p in files])
        for s, sub in df.with_columns(shard_of()).partition_by("shard", as_dict=True, maintain_order=False).items():
            s = s[0] if isinstance(s, tuple) else s
            d = routed / s
            d.mkdir(exist_ok=True)
            tmp = d / f"b{b:04d}.parquet.tmp"
            sub.drop("shard").write_parquet(tmp, compression="zstd")
            os.replace(tmp, d / f"b{b:04d}.parquet")
        mark.write_text(json.dumps({"batch": b, "files": [files[0].stem, files[-1].stem], "rows": df.height}))
        rows_routed += df.height
        print(f"[route {b + 1}/{len(batches)}] {files[0].stem}..{files[-1].stem}: {df.height:,} hashes, elapsed "
              f"{(time.time() - t0) / 60:.1f} min", flush=True)
    rows_out = 0
    shards = sorted(p.name for p in routed.iterdir() if p.is_dir() and not p.name.startswith("_"))
    for k, s in enumerate(shards, 1):
        out = merged / f"shard_{s}.parquet"
        stat = merged / f"_stats_{s}.json"
        if out.exists() and stat.exists():
            rows_out += json.loads(stat.read_text())["rows_out"]
            continue
        parts = [pl.read_parquet(p) for p in sorted((routed / s).glob("b*.parquet"))]
        n_in = sum(p.height for p in parts)
        df = collapse(parts).sort("hash")
        tmp = out.with_suffix(".parquet.tmp")
        df.write_parquet(tmp, compression="zstd")
        os.replace(tmp, out)
        stat.write_text(json.dumps({"shard": s, "rows_in": n_in, "rows_out": df.height}))
        rows_out += df.height
        if k % 16 == 0 or k == len(shards):
            print(f"[merge {k}/{len(shards)}] shard {s}: {n_in:,} -> {df.height:,}, elapsed {(time.time() - t0) / 60:.1f} min",
                  flush=True)
    print(f"STAGE 2 DONE: {rows_routed:,} batch-level hashes routed, {rows_out:,} unique hashes in {len(shards)} shards", flush=True)
    if a.verify:
        verify(root, a.verify, rows_routed)


def verify(root: Path, n: int, rows_routed: int):
    merged = root / "merged"
    stats = [json.loads(p.read_text()) for p in merged.glob("_stats_*.json")]
    rin, rout = sum(s["rows_in"] for s in stats), sum(s["rows_out"] for s in stats)
    ok1 = rin == rows_routed
    print(f"verify totals: rows routed {rows_routed:,}, rows read by shards {rin:,} {'OK' if ok1 else 'MISMATCH'}; unique {rout:,}", flush=True)
    files = sorted(merged.glob("shard_*.parquet"))
    rng = random.Random(0)
    sample = []
    for p in rng.sample(files, min(len(files), 30)):
        d = pl.read_parquet(p)
        sample.append(d.sample(min(d.height, max(1, n // 30)), seed=0))
    smp = pl.concat(sample)
    ref = (pl.scan_parquet(str(root / "daily" / "*.parquet")).filter(pl.col("hash").is_in(smp["hash"].implode()))
           .group_by("hash").agg(pl.col("first_ts").min().alias("f"), pl.col("last_ts").max().alias("l")).collect())
    j = smp.join(ref, on="hash", how="left")
    bad = j.filter((pl.col("first_ts") != pl.col("f")) | (pl.col("last_ts") != pl.col("l")) | pl.col("f").is_null()).height
    print(f"verify sample: {smp.height} hashes recomputed from the daily files, mismatches {bad}", flush=True)
    dup = sum(1 for p in files[:20] if pl.read_parquet(p, columns=["hash"])["hash"].is_duplicated().any())
    print(f"verify uniqueness: duplicate hashes in {dup} of 20 sampled shards", flush=True)
    print("VERIFY " + ("PASSED" if ok1 and bad == 0 and dup == 0 else "FAILED"), flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-root", default="data/interim/xatu_full")
    ap.add_argument("--start", default=FIRST_DAY.isoformat())
    ap.add_argument("--end", default=None)
    ap.add_argument("--days", nargs="*")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--part", default=None, help="i/n: process every n-th day starting at i (run n processes)")
    ap.add_argument("--merge", action="store_true")
    ap.add_argument("--batch-days", type=int, default=10)
    ap.add_argument("--verify", type=int, default=0)
    a = ap.parse_args()
    root = Path(a.out_root)
    root.mkdir(parents=True, exist_ok=True)
    if a.merge:
        stage2(a, root)
    else:
        stage1(a, root)


if __name__ == "__main__":
    main()
