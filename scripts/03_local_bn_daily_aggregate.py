# scripts/03_local_bn_daily_aggregate.py
"""
Local (laptop) consolidation of the per-hour BN dedup output into one row per
hash per calendar day. Second stage of the three-stage local BN pipeline:

  1. 02_local_bn_hourly_dedup.py         raw CSV.gz -> hourly parquet
                                       (hash, timepending, ts_min, ts_max)
  2. 03_local_bn_daily_aggregate.py      hourly parquet -> daily parquet (this script)
  3. 04_local_bn_global_hash_merge.py    daily parquet -> one row per hash across
                                       the whole archive (replaced the archived
                                       local_bn_daily_crossday_merge.py)
  Run all stages with scripts/00_run_all_mempool.py.

For each calendar day, combines that day's hourly per-hash rows into one row
per hash:
  ts_min      = min of the hash's ts_min across its hours
  ts_max      = max of the hash's ts_max across its hours
  timepending = largest non-null timepending across its hours (Polars'
                group_by max() already ignores nulls; null only if every
                hour's value was null)

A day is only built once all 24 of its hourly parquet files actually exist on
disk - no exemption for an hour listed in
data/raw/corrupt_hourly_files_local_dedup.md. (An earlier
version of this script treated a confirmed-corrupt hour as "accounted for"
and built the day from its other 23 hours; changed 2026-11-03 per user
decision - every hour so far listed as corrupt turned out to be a transient
Box Drive sync false positive, not real corruption, so building ahead of a
real hydrated read risked locking in a day with a genuinely missing hour.
Stricter is simpler: a day waits until it has all 24 files, full stop.) A day
with any missing hour - corrupt-listed or just not yet attempted/retried - is
left "not ready yet" and will be picked up on a later run once that hour's
file actually appears.

This step does NOT handle hashes that span a day boundary (e.g. first seen
23:58 on day N, still pending 00:03 on day N+1) - 04_local_bn_global_hash_merge.py
resolves that from this script's output.

Resumable: skips any day whose output parquet already exists.

Usage:
    python scripts/03_local_bn_daily_aggregate.py
    python scripts/03_local_bn_daily_aggregate.py --force-list hours.txt   # also rebuild every day named in the list
"""
import argparse
import os
import sys
import time
from collections import defaultdict
from pathlib import Path

import polars as pl

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")

HOURLY_DIR = Path("data/interim/blocknative_hourly")
OUT_DIR = Path("data/interim/blocknative_daily")
LOG_PATH = Path("logs/local_bn_daily_aggregate.log")

OUT_DIR.mkdir(parents=True, exist_ok=True)


def merge_hash_groups(df: pl.DataFrame) -> pl.DataFrame:
    """Combine any number of partial (hash, timepending, ts_min, ts_max) rows
    for the same hash into one row: ts_min/ts_max span every appearance,
    timepending keeps the largest non-null value (null only if all are null).
    The same rule is applied across days by 04_local_bn_global_hash_merge.py."""
    return df.group_by("hash").agg([
        pl.col("ts_min").min().alias("ts_min"),
        pl.col("ts_max").max().alias("ts_max"),
        pl.col("timepending").max().alias("timepending"),
    ])


def group_hourly_files_by_day(hourly_dir: Path) -> dict:
    by_day = defaultdict(dict)  # day -> {hour_str: Path}
    for f in hourly_dir.glob("*.csv.parquet"):
        stem = f.name.replace(".csv.parquet", "")  # YYYYMMDD_HH
        day, hour = stem.split("_")
        by_day[day][hour] = f
    return by_day


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--part", help="i/n: process only every n-th day of the sorted day list starting at index i (parallel processes, own log)")
    ap.add_argument("--force-list", help="text file of hours (YYYYMMDD_HH) or days (YYYYMMDD); every day it names "
                    "is rebuilt even if its daily parquet exists (old file replaced only after the new one is written)")
    args = ap.parse_args()
    force_days = set()
    if args.force_list:
        force_days = {t[:8] for t in Path(args.force_list).read_text(encoding="utf-8").split()}
    by_day = group_hourly_files_by_day(HOURLY_DIR)
    all_days = sorted(by_day)
    global LOG_PATH
    if args.part:
        pi, pn = (int(x) for x in args.part.split("/"))
        assert 0 <= pi < pn
        all_days = all_days[pi::pn]
        LOG_PATH = LOG_PATH.with_name(LOG_PATH.stem + f"_p{pi}of{pn}" + LOG_PATH.suffix)
    expected_hours = {f"{h:02d}" for h in range(24)}

    total = len(all_days)
    done = skipped = not_ready = 0
    t_start = time.time()

    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_PATH, "a", encoding="utf-8", errors="replace") as log:
        log.write(f"\n=== Run started {time.strftime('%Y-%m-%d %H:%M:%S')} - {total} days seen ===\n")
        log.flush()

        for i, day in enumerate(all_days, 1):
            out_path = OUT_DIR / f"{day}.parquet"
            if out_path.exists() and day not in force_days:
                skipped += 1
                continue

            present_hours = set(by_day[day])
            missing = expected_hours - present_hours
            if missing:
                not_ready += 1
                continue  # still waiting on hours not yet attempted/retried/recovered

            files = [by_day[day][h] for h in sorted(present_hours)]
            t0 = time.time()
            df = pl.concat([pl.read_parquet(f) for f in files])
            result = merge_hash_groups(df)
            tmp_path = out_path.with_suffix(".parquet.tmp")
            result.write_parquet(tmp_path, compression="zstd")
            os.replace(tmp_path, out_path)
            elapsed = time.time() - t0
            done += 1

            msg = (f"[{i}/{total}] {day}: hours_used={len(files)}/24 "
                   f"rows_out={result.height:,} time={elapsed:.1f}s")
            print(msg, flush=True)
            log.write(msg + "\n")
            log.flush()

    total_elapsed = time.time() - t_start
    print(f"\nDONE: {done} days built, {skipped} already existed, {not_ready} not ready yet "
          f"(hours pending), {total_elapsed/60:.1f} min", flush=True)


if __name__ == "__main__":
    main()
