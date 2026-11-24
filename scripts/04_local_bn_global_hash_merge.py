# scripts/04_local_bn_global_hash_merge.py
"""
Local third stage, GLOBAL version: exactly one record per hash across the whole
Blocknative archive. Supersedes local_bn_daily_crossday_merge.py (merges only
across one midnight) and local_bn_daily_lookahead_merge.py (7-day window): a
2026-11-04 cross-day test showed hashes re-appear days and weeks later (mostly
repeated rejected / replacement-underpriced submissions), so any windowed merge
leaves a second record for them.

Input : data/interim/blocknative_daily/YYYYMMDD.parquet
        (03_local_bn_daily_aggregate.py output: one row per hash per day;
         columns hash, ts_min, ts_max, timepending)
Output: data/interim/blocknative_global/shard_XX.parquet
        257 files (shard = first two hex characters after "0x", plus "zz" for any
        hash that does not start with 0x + two lowercase hex characters).
        Columns hash, ts_min, ts_max, timepending; one row per distinct hash.
        Merge rule (same as the aggregate stage): ts_min = min over all days,
        ts_max = max over all days, timepending = largest non-null (null only if
        every appearance was null).

Why shards: the archive has ~2 billion day-rows; instead of one giant group-by,
every copy of a hash is routed to the same shard, so each shard (~1/256 of the
data) is deduplicated independently in a few GB of memory.

Phase 1 (route): read the daily files in batches of --batch-days days, split by
  shard, write scratch/<shard>/b<batch>.parquet. Resumable: a finished batch
  leaves scratch/_done/b<batch>.json (input rows == routed rows is checked).
Phase 2 (merge): per shard, read all its batch files, group by hash, write
  out-dir/shard_<shard>.parquet via .tmp + os.replace. Resumable: shards whose
  output exists are skipped. Several processes can work on different
  --shard-start/--shard-end index ranges at once (indices 0..255 = hex 00..ff,
  256 = zz).
Verify: sample hashes from a few input days, recompute their true merged row
  directly from ALL input days, and compare with the shard output.

Safety: never writes to the input folder; the scratch folder is outside Box;
outputs are written to .tmp then renamed.

Usage (from the repo root):
    python scripts/04_local_bn_global_hash_merge.py --phase 1
    python scripts/04_local_bn_global_hash_merge.py --phase 2 --shard-start 0 --shard-end 85
    python scripts/04_local_bn_global_hash_merge.py --verify 300
    # small test on the first 5 days into other folders:
    python scripts/04_local_bn_global_hash_merge.py --first-days 5 --scratch-dir <dir> --out-dir <dir> --phase all
"""
import argparse
import json
import os
import random
import re
import sys
import time
from pathlib import Path

import polars as pl

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")

COLS = ["hash", "ts_min", "ts_max", "timepending"]
SHARDS = [f"{i:02x}" for i in range(256)] + ["zz"]
HEX2 = re.compile(r"^[0-9a-f]{2}$")


def shard_expr():
    key = pl.col("hash").str.slice(2, 2)
    ok = pl.col("hash").str.starts_with("0x") & key.str.contains(r"^[0-9a-f]{2}$")
    return pl.when(ok).then(key).otherwise(pl.lit("zz")).alias("shard")


def list_days(in_dir: Path, first_days: int):
    days = sorted(p.stem for p in in_dir.glob("*.parquet") if len(p.stem) == 8 and p.stem.isdigit())
    return days[:first_days] if first_days else days


def phase1(in_dir: Path, scratch: Path, batch_days: int, first_days: int):
    days = list_days(in_dir, first_days)
    batches = [days[i:i + batch_days] for i in range(0, len(days), batch_days)]
    (scratch / "_done").mkdir(parents=True, exist_ok=True)
    t_start = time.time()
    rows_total = 0
    print(f"=== phase 1: {len(days)} days in {len(batches)} batches of up to {batch_days}, scratch={scratch} ===", flush=True)
    for b, batch in enumerate(batches):
        done = scratch / "_done" / f"b{b:04d}.json"
        if done.exists():
            rows_total += json.loads(done.read_text())["rows"]
            continue
        t0 = time.time()
        df = pl.concat([
            pl.read_parquet(in_dir / f"{d}.parquet").select(COLS).with_columns(pl.col("timepending").cast(pl.Int64))
            for d in batch
        ]).with_columns(shard_expr())
        n_in = df.height
        n_out = 0
        for s in df["shard"].unique().to_list():
            sub = df.filter(pl.col("shard") == s).select(COLS)
            d = scratch / s
            d.mkdir(exist_ok=True)
            tmp = d / f"b{b:04d}.parquet.tmp"
            sub.write_parquet(tmp, compression="zstd")
            os.replace(tmp, d / f"b{b:04d}.parquet")
            n_out += sub.height
        if n_in != n_out:
            sys.exit(f"batch {b}: routed {n_out} rows but read {n_in} - stopping")
        done.write_text(json.dumps({"batch": b, "days": [batch[0], batch[-1]], "rows": n_in}))
        rows_total += n_in
        print(f"[batch {b+1}/{len(batches)}] {batch[0]}..{batch[-1]}: {n_in:,} rows routed, "
              f"{time.time()-t0:.0f}s, elapsed {(time.time()-t_start)/60:.1f} min", flush=True)
    print(f"PHASE 1 DONE: {len(days)} days, {rows_total:,} day-rows routed to {len(SHARDS)} shards, "
          f"{(time.time()-t_start)/60:.1f} min", flush=True)


def phase2(scratch: Path, out_dir: Path, s0: int, s1: int):
    out_dir.mkdir(parents=True, exist_ok=True)
    t_start = time.time()
    rows_in = rows_out = built = skipped = 0
    names = SHARDS[s0:s1 + 1]
    print(f"=== phase 2: shards {names[0]}..{names[-1]} ({len(names)}), out={out_dir} ===", flush=True)
    for s in names:
        out = out_dir / f"shard_{s}.parquet"
        if out.exists():
            skipped += 1
            continue
        files = sorted((scratch / s).glob("b*.parquet")) if (scratch / s).exists() else []
        if not files:
            print(f"shard {s}: no batch files (empty shard)", flush=True)
            continue
        t0 = time.time()
        df = pl.concat([pl.read_parquet(f) for f in files])
        merged = df.group_by("hash").agg([
            pl.col("ts_min").min().alias("ts_min"),
            pl.col("ts_max").max().alias("ts_max"),
            pl.col("timepending").max().alias("timepending"),
        ]).select(COLS)
        tmp = out_dir / f"shard_{s}.parquet.tmp"
        merged.write_parquet(tmp, compression="zstd")
        os.replace(tmp, out)
        rows_in += df.height
        rows_out += merged.height
        built += 1
        print(f"shard {s}: {df.height:,} day-rows -> {merged.height:,} unique hashes ({merged.height/df.height:.1%}), "
              f"{time.time()-t0:.0f}s", flush=True)
    print(f"PHASE 2 DONE (shards {names[0]}..{names[-1]}): {built} built, {skipped} already existed, "
          f"{rows_in:,} day-rows -> {rows_out:,} unique hashes, {(time.time()-t_start)/60:.1f} min", flush=True)


def verify(in_dir: Path, out_dir: Path, k: int, first_days: int, seed: int = 20261104):
    days = list_days(in_dir, first_days)
    rng = random.Random(seed)
    pick_days = rng.sample(days, min(3, len(days)))
    sample = []
    for d in pick_days:
        h = pl.read_parquet(in_dir / f"{d}.parquet", columns=["hash"])["hash"]
        sample += h.sample(min(k // len(pick_days), h.len()), seed=seed).to_list()
    sample = sorted(set(sample))
    print(f"verify: {len(sample)} sampled hashes from days {pick_days}; recomputing truth from {len(days)} input days", flush=True)
    # one day at a time (a single lazy scan over all days pulled ~60 GB into memory in testing)
    sample_df = pl.DataFrame({"hash": sample})
    parts = []
    for d in days:
        part = (pl.read_parquet(in_dir / f"{d}.parquet").select(COLS)
                  .with_columns(pl.col("timepending").cast(pl.Int64))
                  .join(sample_df, on="hash", how="semi"))
        if part.height:
            parts.append(part)
    truth = pl.concat(parts).group_by("hash").agg(
        [pl.col("ts_min").min(), pl.col("ts_max").max(), pl.col("timepending").max()])
    got_frames = []
    sdf = pl.DataFrame({"hash": sample}).with_columns(shard_expr())
    for s in sdf["shard"].unique().to_list():
        hs = sdf.filter(pl.col("shard") == s)["hash"].to_list()
        got_frames.append(pl.read_parquet(out_dir / f"shard_{s}.parquet").filter(pl.col("hash").is_in(hs)))
    got = pl.concat(got_frames)
    mism = truth.join(got, on="hash", how="full", suffix="_g", coalesce=True)
    bad = mism.filter(
        pl.col("ts_min").is_null() | pl.col("ts_min_g").is_null()
        | (pl.col("ts_min") != pl.col("ts_min_g")) | (pl.col("ts_max") != pl.col("ts_max_g"))
        | (pl.col("timepending").fill_null(-1) != pl.col("timepending_g").fill_null(-1))
    )
    multi = truth.height
    print(f"VERIFY: {len(sample)} sampled; truth rows {truth.height}, output rows {got.height}; mismatches: {bad.height}", flush=True)
    if bad.height:
        print(bad.head(10), flush=True)
    # uniqueness + totals from the shard files themselves
    tot = 0
    dup_shards = 0
    for p in sorted(out_dir.glob("shard_*.parquet")):
        h = pl.read_parquet(p, columns=["hash"])
        tot += h.height
        if h["hash"].n_unique() != h.height:
            dup_shards += 1
    print(f"VERIFY TOTALS: {tot:,} rows across {len(list(out_dir.glob('shard_*.parquet')))} shard files; shards with duplicate hashes: {dup_shards}", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-dir", default="data/interim/blocknative_daily")
    ap.add_argument("--scratch-dir", default="D:/pfof_scratch/bn_shards")
    ap.add_argument("--out-dir", default="data/interim/blocknative_global")
    ap.add_argument("--batch-days", type=int, default=12)
    ap.add_argument("--first-days", type=int, default=0, help="only use the first N input days (testing)")
    ap.add_argument("--phase", choices=["1", "2", "all"], default=None)
    ap.add_argument("--shard-start", type=int, default=0)
    ap.add_argument("--shard-end", type=int, default=len(SHARDS) - 1)
    ap.add_argument("--verify", type=int, default=0)
    a = ap.parse_args()
    in_dir, scratch, out_dir = Path(a.in_dir), Path(a.scratch_dir), Path(a.out_dir)
    if a.phase in ("1", "all"):
        scratch.mkdir(parents=True, exist_ok=True)
        phase1(in_dir, scratch, a.batch_days, a.first_days)
    if a.phase in ("2", "all"):
        phase2(scratch, out_dir, a.shard_start, a.shard_end)
    if a.verify:
        verify(in_dir, out_dir, a.verify, a.first_days)


if __name__ == "__main__":
    main()
