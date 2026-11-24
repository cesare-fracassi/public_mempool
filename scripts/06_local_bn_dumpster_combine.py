# scripts/06_local_bn_dumpster_combine.py
"""
Phase 1e: the wallet-agnostic PUBLIC mempool file, built in two stages so the
expensive source merge never has to be redone when block data changes.

STAGE A  (sources only; needs BN, Dumpster, Xatu; no block data)
  Combines the Blocknative global per-hash set (Phase 1d), Flashbots Mempool
  Dumpster and the full Xatu per-hash set into ONE ROW PER HASH with the earliest
  sighting. Output: <out-dir>/first_seen/first_seen_YYYY-MM.parquet
      hash        String
      first_seen  Datetime(ms, UTC)   earliest sighting across BN, Dumpster, Xatu
  One file per MONTH of first_seen, sorted by hash; each hash in exactly one file.
  A hash with no parseable first_seen goes to first_seen_unknown.parquet (should be
  empty or tiny).

STAGE B  (needs Erigon; runs across ALL blocks and transactions)
  Attaches the block time. Every hash of every monthly file is looked up in the whole
  Erigon transactions table (hash -> block_number) and the blocks table
  (block_number -> block_timestamp). No time window: a transaction mined years after
  it was first seen is found. Output: <out-dir>/public_mempool_YYYY-MM.parquet
      hash         String
      first_seen   Datetime(ms, UTC)
      included_ts  Datetime(ms, UTC)  block timestamp; NULL = not mined
  Because stage A is kept separately, swapping in the real Erigon block timestamps
  (replacing the BigQuery stand-in) only re-runs stage B.
  How stage B is organised: Erigon transactions are routed once into 257 hash shards
  (same scheme as Phase 1d); for each shard the transactions are loaded once and
  joined to that shard's rows of every monthly file. Result is identical to joining
  each monthly file against the whole chain, but reads the transactions once, not
  once per month.

Intermediates: <out-dir>/_parts_fs/<month>/shard_XX.parquet and _stats_fs/ (stage A),
  <out-dir>/_parts/<month>/shard_XX.parquet and _stats/ (stage B). A shard with a stats
  json is "done" for that stage.

Inputs
  BN     data/interim/blocknative_global/shard_XX.parquet   (Phase 1d)
           hash, ts_min, ts_max (ISO strings, UTC), timepending (Int64)
  DS     data/raw/dumpster/YYYY-MM-DD.parquet
           timestamp (Datetime ms), hash, includedAtBlockHeight, inclusionDelayMs
  XATU   (default, --xatu-source full) <xatu-dir>/shard_XX.parquet, 256 files (hash, first_ts, last_ts),
           one row per hash over every Xatu day (2023-03-03 onward), built by
           scripts/05_xatu_mempool_all_days.py (stage 1 daily files, then --merge). The shard key is
           hash[2:4], the same as BN and this script, so each file is routed as it is (no re-sharding).
           first_ts becomes first_seen; last_ts (last mempool sighting) is NOT used: the end of a
           transaction's life is included_ts, the block time attached in stage B.
         (--xatu-source gap) the old gap-day file only:
           data/interim/xatu_gap_days/xatu_gap_days_first_seen.parquet (hash, first_seen), the six days
           Dumpster never published (2024-10-27..30, 2025-09-19, 2025-12-02);
           the script that built it was removed on 2026-11-24; the option only reads an existing file with these columns.
  Chain  <chain-dir>/transactions/transactions_*.parquet : transaction_hash, block_number
         <blocks-dir>/blocks_*.parquet                   : block_number, block_timestamp
         (block_timestamp = unix seconds). blocks-dir defaults to <chain-dir>/blocks. Both tables are written by
         scripts/00_extract_chain_blocks.py (default folder data/raw/chain); any folder with the same layout and
         columns works. (--erigon-dir is the old name of --chain-dir and still accepted.)

Public rule (decided 2026-11-05, Xatu extended from the gap days to all days): a hash is in the
  output if Dumpster saw it, OR Xatu saw it (any day), OR BN saw it with timepending != 0 (null counts as public;
  only timepending == 0 is treated as private). first_seen is the earliest sighting
  across the sources. Basis, 10 random days Oct 2023 to Nov 2024: BN tp>0 hashes are in
  Dumpster 99.92% of the time, tp null 78.5%, tp==0 only 0.69%. A hash absent from the
  file is NOT public. Consumers derive "private" as "absent from this file", restricted
  to the era where a source has coverage (BN only before Aug 2023, both Aug 2023 to
  Feb 2025, Dumpster only after; Aug 2023 and Dec 2024 to Feb 2025 are unreliable,
  README section 9).

Known inherited limitations (no per-hash quality flag, by decision):
  * BN hours that fail the gzip checksum (54) and ~25 low-output hours leave those
    days possibly incomplete or holding a few bogus hashes.
  * BN keeps hashes whose only events were rejected/evicted/failed.
  * The ~21% of BN null-timepending rows that Dumpster lacks have unverified public status.
  * Xatu now covers every day from 2023-03-03 to the frozen end date of the Xatu build, so any Xatu
    sighting makes a hash public and can move first_seen earlier. Its agreement with Dumpster and BN
    on first-sighting times has NOT been measured yet (only BN vs Dumpster was: median 3.5 ms).
    Xatu published 0 rows for 2023-07-17 to 2023-07-21; BN is the only source for those days.

Phases (--phase)
  route            Dumpster daily files -> scratch/<shard>/d<batch>.parquet; Xatu -> scratch/<shard>/x0000.parquet
                   (full: one file per shard straight from <xatu-dir>, checked against its _stats_XX.json)
  combine          stage A per hash shard (resumable; several processes may take shard ranges)
  assemble_fs      stage A monthly files from _parts_fs
  route_tx         Erigon transactions -> scratch/<shard>/t<batch>.parquet (hash, block_number)
  attach           stage B per hash shard (resumable; several processes may take shard ranges)
  assemble         stage B monthly files public_mempool_YYYY-MM.parquet
  all              runs the six phases above in order (two stages)
  single_pass      stages A and B in ONE pass per shard, no first_seen intermediate files:
                   needs route and route_tx done, i.e. every big input reachable from one machine
  all_single       route, route_tx, single_pass, assemble
  --verify N       sample hashes; recompute first_seen from BN, Dumpster and Xatu directly,
                   check included_ts against Dumpster's own block time (first sighting +
                   inclusionDelayMs) where it knows the block, and against an optional
                   --xatu-check-file; also uniqueness and totals against _stats.

Safety: never writes to the inputs; scratch is outside Box; outputs go to .tmp then rename.

Usage (repo root; or run everything via scripts/00_run_all_mempool.py):
    python scripts/06_local_bn_dumpster_combine.py --phase route
    python scripts/06_local_bn_dumpster_combine.py --phase combine --shard-start 0 --shard-end 85
    python scripts/06_local_bn_dumpster_combine.py --phase assemble_fs
    python scripts/06_local_bn_dumpster_combine.py --phase route_tx
    python scripts/06_local_bn_dumpster_combine.py --phase attach --blocks-dir <dir>
    python scripts/06_local_bn_dumpster_combine.py --phase assemble
    python scripts/06_local_bn_dumpster_combine.py --verify 300
"""
import argparse
import json
import os
import random
import sys
import time
from pathlib import Path

import polars as pl

import importlib.util as _ilu  # the module name starts with a digit, so it cannot be imported with a plain import
from pathlib import Path as _P
_spec = _ilu.spec_from_file_location('local_bn_global_hash_merge', _P(__file__).with_name('04_local_bn_global_hash_merge.py'))
_merge = _ilu.module_from_spec(_spec)
_spec.loader.exec_module(_merge)
SHARDS, shard_expr = _merge.SHARDS, _merge.shard_expr  # same 257-shard scheme as Phase 1d

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")

DS_COLS = ["hash", "timestamp", "includedAtBlockHeight"]
BN_SCHEMA = {"hash": pl.String, "ts_min": pl.String, "ts_max": pl.String, "timepending": pl.Int64}
DS_EMPTY = {"hash": pl.String, "timestamp": pl.Datetime("ms"), "includedAtBlockHeight": pl.Int64}
TX_EMPTY = {"hash": pl.String, "block_number": pl.Int64}
XA_EMPTY = {"hash": pl.String, "first_seen": pl.Datetime("ms")}
FS_EMPTY = {"hash": pl.String, "first_seen": pl.Datetime("ms")}
BN_TS_FORMAT = "%Y-%m-%dT%H:%M:%S%.fZ"


def list_ds_days(ds_dir: Path, first_days: int):
    days = sorted(p.stem for p in ds_dir.glob("*.parquet") if len(p.stem) == 10)
    return days[:first_days] if first_days else days


def _route_frames(df: pl.DataFrame, scratch: Path, prefix: str, b: int, cols: list) -> int:
    """Write df split by shard to scratch/<shard>/<prefix><b>.parquet; return rows written."""
    df = df.with_columns(shard_expr())
    n_out = 0
    for s in df["shard"].unique().to_list():
        sub = df.filter(pl.col("shard") == s).select(cols)
        d = scratch / s
        d.mkdir(exist_ok=True)
        tmp = d / f"{prefix}{b:04d}.parquet.tmp"
        sub.write_parquet(tmp, compression="zstd")
        os.replace(tmp, d / f"{prefix}{b:04d}.parquet")
        n_out += sub.height
    return n_out


def route_dumpster(ds_dir: Path, scratch: Path, batch_days: int, first_days: int, b0: int = 0, b1=None):
    """b0..b1 (inclusive batch indices) let several processes route disjoint batch ranges in parallel;
    each batch writes its own d<batch>.parquet files and its own done marker."""
    days = list_ds_days(ds_dir, first_days)
    batches = [days[i:i + batch_days] for i in range(0, len(days), batch_days)]
    (scratch / "_done").mkdir(parents=True, exist_ok=True)
    t_start = time.time()
    rows_total = 0
    print(f"=== route Dumpster: {len(days)} days in {len(batches)} batches of up to {batch_days}, "
          f"batches {b0}..{b1 if b1 is not None else len(batches) - 1}, scratch={scratch} ===", flush=True)
    for b, batch in enumerate(batches):
        if b < b0 or (b1 is not None and b > b1):
            continue
        done = scratch / "_done" / f"d{b:04d}.json"
        if done.exists():
            rows_total += json.loads(done.read_text())["rows"]
            continue
        t0 = time.time()
        df = pl.concat([
            pl.read_parquet(ds_dir / f"{d}.parquet").select(DS_COLS)
              .with_columns(pl.col("hash").str.to_lowercase(), pl.col("includedAtBlockHeight").cast(pl.Int64))
            for d in batch
        ])
        n_in = df.height
        n_out = _route_frames(df, scratch, "d", b, DS_COLS)
        if n_in != n_out:
            sys.exit(f"Dumpster batch {b}: routed {n_out} rows but read {n_in} - stopping")
        done.write_text(json.dumps({"batch": b, "days": [batch[0], batch[-1]], "rows": n_in}))
        rows_total += n_in
        print(f"[Dumpster batch {b+1}/{len(batches)}] {batch[0]}..{batch[-1]}: {n_in:,} rows, "
              f"{time.time()-t0:.0f}s, elapsed {(time.time()-t_start)/60:.1f} min", flush=True)
    print(f"ROUTE DUMPSTER DONE: {len(days)} days, {rows_total:,} rows routed, {(time.time()-t_start)/60:.1f} min", flush=True)


def xatu_ts_col(path: Path) -> str:
    """Name of the first-sighting column in a Xatu parquet: first_ts (05_xatu_mempool_all_days.py) or first_seen."""
    names = pl.read_parquet_schema(path)
    for c in ("first_ts", "first_seen"):
        if c in names:
            return c
    sys.exit(f"{path}: no first_ts or first_seen column (found {list(names)})")


def read_xatu_shard(xatu_dir: Path, s: str) -> pl.DataFrame:
    """One Xatu shard as (hash, first_seen), lower case, ms. Empty frame when the shard file is absent."""
    p = xatu_dir / f"shard_{s}.parquet"
    if not p.exists():
        return pl.DataFrame(schema=XA_EMPTY)
    c = xatu_ts_col(p)
    return (pl.read_parquet(p, columns=["hash", c])
              .select(pl.col("hash").str.to_lowercase(), pl.col(c).cast(pl.Datetime("ms")).alias("first_seen")))


def route_xatu_full(xatu_dir: Path, scratch: Path, expected_shards: int = 256):
    """Route the full Xatu per-hash set into the hash shards as scratch/<shard>/x0000.parquet.
    The Xatu merge already shards by hash[2:4] (256 files, no 'zz'), so each file is copied through
    unchanged except for the rename first_ts -> first_seen (last_ts is dropped). Per shard it checks
    that every hash belongs to the shard, is unique, and that the row count equals the merge's own
    _stats_XX.json. Resumable per shard."""
    (scratch / "_done").mkdir(parents=True, exist_ok=True)
    done = scratch / "_done" / "x0000.json"
    if done.exists():
        info = json.loads(done.read_text())
        if info.get("source") != "full":
            sys.exit(f"{done} was written by the old gap-day routing; move the scratch x*.parquet files and "
                     f"{done.name} aside (or use a new --scratch-dir) before routing the full Xatu set")
        print(f"ROUTE XATU already done ({info['rows']:,} rows)", flush=True)
        return
    if not xatu_dir.is_dir():
        sys.exit(f"Xatu folder not found: {xatu_dir} (build it with scripts/05_xatu_mempool_all_days.py, then --merge)")
    files = sorted(xatu_dir.glob("shard_*.parquet"))
    if len(files) != expected_shards:   # 256 in production; the test passes a smaller number on a sample
        sys.exit(f"{xatu_dir}: expected {expected_shards} shard_XX.parquet files, found {len(files)}")
    t0 = time.time()
    total = 0
    for k, f in enumerate(files, 1):
        s = f.stem.split("_", 1)[1]
        mark = scratch / "_done" / f"xs_{s}.json"
        if mark.exists():
            total += json.loads(mark.read_text())["rows"]
            continue
        stats_f = xatu_dir / f"_stats_{s}.json"
        if not stats_f.exists():
            sys.exit(f"{stats_f} missing: the Xatu merge did not finish shard {s}")
        df = read_xatu_shard(xatu_dir, s)
        if df["hash"].n_unique() != df.height:
            sys.exit(f"Xatu shard {s} has duplicate hashes; expected one row per hash")
        if df.height != json.loads(stats_f.read_text())["rows_out"]:
            sys.exit(f"Xatu shard {s}: read {df.height} rows but the merge recorded {json.loads(stats_f.read_text())['rows_out']}")
        if df.height and not (df["hash"].str.slice(2, 2) == s).all():
            sys.exit(f"Xatu shard {s} holds hashes whose prefix is not {s}")
        d = scratch / s
        d.mkdir(exist_ok=True)
        tmp = d / "x0000.parquet.tmp"
        df.write_parquet(tmp, compression="zstd")
        os.replace(tmp, d / "x0000.parquet")
        mark.write_text(json.dumps({"shard": s, "rows": df.height}))
        total += df.height
        if k % 32 == 0 or k == len(files):
            print(f"[route Xatu {k}/{len(files)}] shard {s}, {total:,} rows so far, {(time.time()-t0)/60:.1f} min", flush=True)
    done.write_text(json.dumps({"source": "full", "dir": str(xatu_dir), "shards": len(files), "rows": total}))
    print(f"ROUTE XATU DONE: {total:,} rows routed from {len(files)} shards", flush=True)


def route_xatu(xatu_file: Path, scratch: Path):
    """Legacy (--xatu-source gap): route the small Xatu gap-day file into the hash shards as x0000."""
    (scratch / "_done").mkdir(parents=True, exist_ok=True)
    done = scratch / "_done" / "x0000.json"
    if done.exists():
        if json.loads(done.read_text()).get("source") == "full":
            sys.exit(f"{done} holds the full Xatu routing; use a different --scratch-dir for the gap-day variant")
        print(f"ROUTE XATU already done ({json.loads(done.read_text())['rows']:,} rows)", flush=True)
        return
    if not xatu_file.exists():
        sys.exit(f"Xatu file not found: {xatu_file}")
    df = (pl.read_parquet(xatu_file, columns=["hash", "first_seen"])
            .with_columns(pl.col("hash").str.to_lowercase(), pl.col("first_seen").cast(pl.Datetime("ms"))))
    if df["hash"].n_unique() != df.height:
        sys.exit("Xatu file has duplicate hashes; expected one row per hash")
    n_out = _route_frames(df, scratch, "x", 0, ["hash", "first_seen"])
    if n_out != df.height:
        sys.exit(f"Xatu: routed {n_out} rows but read {df.height} - stopping")
    done.write_text(json.dumps({"file": xatu_file.name, "rows": df.height}))
    print(f"ROUTE XATU DONE: {df.height:,} rows routed", flush=True)


def route_transactions(tx_dir: Path, scratch: Path, files_per_batch: int, b0: int = 0, b1=None):
    """b0..b1 (inclusive batch indices) let several processes route disjoint batch ranges in parallel."""
    files = sorted(tx_dir.glob("transactions_*.parquet"))
    if not files:
        sys.exit(f"no transactions_*.parquet files in {tx_dir}")
    batches = [files[i:i + files_per_batch] for i in range(0, len(files), files_per_batch)]
    (scratch / "_done").mkdir(parents=True, exist_ok=True)
    t_start = time.time()
    rows_total = 0
    print(f"=== route Erigon transactions: {len(files)} files in {len(batches)} batches of up to {files_per_batch}, "
          f"batches {b0}..{b1 if b1 is not None else len(batches) - 1}, source={tx_dir} ===", flush=True)
    for b, batch in enumerate(batches):
        if b < b0 or (b1 is not None and b > b1):
            continue
        done = scratch / "_done" / f"t{b:04d}.json"
        if done.exists():
            rows_total += json.loads(done.read_text())["rows"]
            continue
        t0 = time.time()
        df = pl.concat([
            pl.read_parquet(f, columns=["transaction_hash", "block_number"])
              .select(pl.col("transaction_hash").str.to_lowercase().alias("hash"), pl.col("block_number").cast(pl.Int64))
            for f in batch
        ])
        n_in = df.height
        n_out = _route_frames(df, scratch, "t", b, ["hash", "block_number"])
        if n_in != n_out:
            sys.exit(f"transactions batch {b}: routed {n_out} rows but read {n_in} - stopping")
        done.write_text(json.dumps({"batch": b, "files": [batch[0].name, batch[-1].name], "rows": n_in}))
        rows_total += n_in
        print(f"[tx batch {b+1}/{len(batches)}] {batch[0].name}..{batch[-1].name}: {n_in:,} rows, "
              f"{time.time()-t0:.0f}s, elapsed {(time.time()-t_start)/60:.1f} min", flush=True)
    print(f"ROUTE TRANSACTIONS DONE: {len(files)} files, {rows_total:,} rows routed, {(time.time()-t_start)/60:.1f} min", flush=True)


def load_blocks(blocks_dir: Path) -> pl.DataFrame:
    files = sorted(blocks_dir.glob("blocks_*.parquet"))
    if not files:
        sys.exit(f"no blocks_*.parquet files in {blocks_dir}")
    blocks = pl.concat([pl.read_parquet(f, columns=["block_number", "block_timestamp"]) for f in files])
    if blocks["block_number"].n_unique() != blocks.height:
        sys.exit(f"{blocks_dir}: duplicate block_number values across blocks files")
    return blocks.with_columns(pl.col("block_timestamp").cast(pl.Int64))


def combine_frames(bn: pl.DataFrame, ds_raw: pl.DataFrame, xatu=None) -> pl.DataFrame:
    """Stage A rule. bn: hash, ts_min, ts_max, timepending (one row per hash).
    ds_raw: Dumpster rows (hash, timestamp, includedAtBlockHeight), any number per hash.
    xatu: (hash, first_seen), one row per hash, or None (duplicates are collapsed to the earliest).
    Returns public hashes only: hash, first_seen, in_bn, in_dumpster, in_xatu."""
    bn = bn.select(
        "hash",
        pl.col("ts_min").str.to_datetime(BN_TS_FORMAT, strict=False).dt.cast_time_unit("ms").alias("bn_first"),
        pl.col("timepending").cast(pl.Int64).alias("bn_timepending"),
        pl.lit(True).alias("in_bn"),
    )
    ds = ds_raw.group_by("hash").agg(pl.col("timestamp").min().alias("ds_first")).with_columns(
        pl.lit(True).alias("in_dumpster"))
    m = bn.join(ds, on="hash", how="full", coalesce=True)
    if xatu is not None and xatu.height:
        xa = (xatu.group_by("hash").agg(pl.col("first_seen").min().alias("xa_first"))
                  .with_columns(pl.lit(True).alias("in_xatu")))
        m = m.join(xa, on="hash", how="full", coalesce=True)
    else:
        m = m.with_columns(pl.lit(None, dtype=pl.Datetime("ms")).alias("xa_first"), pl.lit(False).alias("in_xatu"))
    m = m.with_columns(pl.col("in_bn").fill_null(False), pl.col("in_dumpster").fill_null(False),
                       pl.col("in_xatu").fill_null(False))
    # public = Dumpster or Xatu saw it, or BN saw it with timepending != 0 (null counts as public)
    bn_public = pl.col("in_bn") & (pl.col("bn_timepending") != 0).fill_null(True)
    m = m.filter(bn_public | pl.col("in_dumpster") | pl.col("in_xatu")).with_columns(
        pl.min_horizontal("bn_first", "ds_first", "xa_first").alias("first_seen"))
    return m.select("hash", "first_seen", "in_bn", "in_dumpster", "in_xatu")


def attach_inclusion(part: pl.DataFrame, tx_ts: pl.DataFrame) -> pl.DataFrame:
    """Stage B join. part: hash, first_seen. tx_ts: hash, included_ts (one row per hash)."""
    return part.join(tx_ts, on="hash", how="left", coalesce=True).select("hash", "first_seen", "included_ts")


def read_bn_shard(bn_dir: Path, s: str) -> pl.DataFrame:
    p = bn_dir / f"shard_{s}.parquet"
    if not p.exists():
        return pl.DataFrame(schema=BN_SCHEMA)
    return pl.read_parquet(p).select(list(BN_SCHEMA))


def _read_scratch(scratch: Path, s: str, prefix: str, empty_schema: dict) -> pl.DataFrame:
    files = sorted((scratch / s).glob(f"{prefix}*.parquet")) if (scratch / s).exists() else []
    return pl.concat([pl.read_parquet(f) for f in files]) if files else pl.DataFrame(schema=empty_schema)


def _write_month_parts(df: pl.DataFrame, parts_dir: Path, s: str) -> int:
    """Split df (hash, first_seen, ...) by month of first_seen and write parts_dir/<month>/shard_<s>.parquet."""
    month = pl.col("first_seen").dt.strftime("%Y-%m").fill_null("unknown")
    parts = df.with_columns(month.alias("_month"))
    n = 0
    for mth in parts["_month"].unique().to_list():
        sub = parts.filter(pl.col("_month") == mth).drop("_month")
        d = parts_dir / mth
        d.mkdir(parents=True, exist_ok=True)
        tmp = d / f"shard_{s}.parquet.tmp"
        sub.write_parquet(tmp, compression="zstd")
        os.replace(tmp, d / f"shard_{s}.parquet")
        n += sub.height
    return n


def _source_stats(res: pl.DataFrame) -> dict:
    """Per shard: public hashes by which sources saw them (BN, Dumpster, Xatu)."""
    if not res.height:
        return {"ds_or_xatu_only": 0, "xatu_only": 0}
    return {"ds_or_xatu_only": int((~res["in_bn"]).sum()),
            "xatu_only": int((res["in_xatu"] & ~res["in_bn"] & ~res["in_dumpster"]).sum())}


def combine(bn_dir: Path, scratch: Path, out_dir: Path, s0: int, s1: int):
    """Stage A."""
    stats_dir = out_dir / "_stats_fs"
    stats_dir.mkdir(parents=True, exist_ok=True)
    names = SHARDS[s0:s1 + 1]
    t_start = time.time()
    built = skipped = 0
    tot_out = 0
    print(f"=== combine (stage A): shards {names[0]}..{names[-1]} ({len(names)}), out={out_dir} ===", flush=True)
    for s in names:
        if (stats_dir / f"shard_{s}.json").exists():
            skipped += 1
            continue
        t0 = time.time()
        bn = read_bn_shard(bn_dir, s)
        ds_raw = _read_scratch(scratch, s, "d", DS_EMPTY)
        xa = _read_scratch(scratch, s, "x", XA_EMPTY)
        res = combine_frames(bn, ds_raw, xa)
        n_written = _write_month_parts(res.select("hash", "first_seen"), out_dir / "_parts_fs", s)
        if n_written != res.height:
            sys.exit(f"shard {s}: wrote {n_written} rows but combined {res.height} - stopping")
        stats = {"shard": s, "bn_rows": bn.height, "ds_rows": ds_raw.height, "xatu_rows": xa.height,
                 "out_rows": res.height,
                 "first_seen_null": int(res["first_seen"].is_null().sum()) if res.height else 0,
                 **_source_stats(res)}
        (stats_dir / f"shard_{s}.json").write_text(json.dumps(stats))
        built += 1
        tot_out += res.height
        print(f"shard {s}: BN {bn.height:,} + Dumpster {ds_raw.height:,} + Xatu {xa.height:,} -> {res.height:,} public hashes, "
              f"{time.time()-t0:.0f}s", flush=True)
    print(f"COMBINE DONE (shards {names[0]}..{names[-1]}): {built} built, {skipped} already existed, "
          f"{tot_out:,} public hashes written, {(time.time()-t_start)/60:.1f} min", flush=True)


def attach(scratch: Path, out_dir: Path, blocks_dir: Path, s0: int, s1: int):
    """Stage B: for each shard load its transactions once, join to that shard's rows of every month."""
    (out_dir / "_stats").mkdir(parents=True, exist_ok=True)
    fs_parts = out_dir / "_parts_fs"
    if not fs_parts.exists():
        sys.exit("attach: stage A output (_parts_fs) not found; run combine first")
    months = sorted(d.name for d in fs_parts.iterdir() if d.is_dir())
    blocks = load_blocks(blocks_dir)
    print(f"blocks table: {blocks.height:,} blocks, {blocks['block_number'].min()}..{blocks['block_number'].max()} from {blocks_dir}", flush=True)
    names = SHARDS[s0:s1 + 1]
    t_start = time.time()
    built = skipped = 0
    print(f"=== attach (stage B): shards {names[0]}..{names[-1]} ({len(names)}), {len(months)} months ===", flush=True)
    for s in names:
        if (out_dir / "_stats" / f"shard_{s}.json").exists():
            skipped += 1
            continue
        t0 = time.time()
        tx = _read_scratch(scratch, s, "t", TX_EMPTY).unique(subset="hash")
        tx_ts = (tx.join(blocks, on="block_number", how="left", coalesce=True)
                   .select("hash", pl.from_epoch("block_timestamp", time_unit="s").dt.cast_time_unit("ms").alias("included_ts")))
        rows = mined = 0
        for mth in months:
            p = fs_parts / mth / f"shard_{s}.parquet"
            if not p.exists():
                continue
            res = attach_inclusion(pl.read_parquet(p), tx_ts)
            d = out_dir / "_parts" / mth
            d.mkdir(parents=True, exist_ok=True)
            tmp = d / f"shard_{s}.parquet.tmp"
            res.write_parquet(tmp, compression="zstd")
            os.replace(tmp, d / f"shard_{s}.parquet")
            rows += res.height
            mined += int(res["included_ts"].is_not_null().sum()) if res.height else 0
        (out_dir / "_stats" / f"shard_{s}.json").write_text(json.dumps(
            {"shard": s, "tx_rows": tx.height, "out_rows": rows, "mined": mined}))
        built += 1
        print(f"shard {s}: tx {tx.height:,} -> {rows:,} rows attached ({mined:,} mined), {time.time()-t0:.0f}s", flush=True)
    print(f"ATTACH DONE (shards {names[0]}..{names[-1]}): {built} built, {skipped} already existed, "
          f"{(time.time()-t_start)/60:.1f} min", flush=True)


def single_pass(bn_dir: Path, scratch: Path, out_dir: Path, blocks_dir: Path, s0: int, s1: int):
    """Stages A and B in one pass per hash shard, with no first_seen intermediate: combine the three
    sources in memory, attach the block time from that shard's transactions, write the final month
    parts. Needs BN, Dumpster, Xatu AND Erigon transactions routed into the same scratch folder
    (phases route and route_tx), i.e. all big inputs reachable from one machine."""
    (out_dir / "_stats").mkdir(parents=True, exist_ok=True)
    names = SHARDS[s0:s1 + 1]
    blocks = load_blocks(blocks_dir)
    print(f"blocks table: {blocks.height:,} blocks, {blocks['block_number'].min()}..{blocks['block_number'].max()} from {blocks_dir}", flush=True)
    t_start = time.time()
    built = skipped = 0
    print(f"=== single pass: shards {names[0]}..{names[-1]} ({len(names)}), out={out_dir} ===", flush=True)
    for s in names:
        if (out_dir / "_stats" / f"shard_{s}.json").exists():
            skipped += 1
            continue
        t0 = time.time()
        bn = read_bn_shard(bn_dir, s)
        ds_raw = _read_scratch(scratch, s, "d", DS_EMPTY)
        xa = _read_scratch(scratch, s, "x", XA_EMPTY)
        tx = _read_scratch(scratch, s, "t", TX_EMPTY).unique(subset="hash")
        tx_ts = (tx.join(blocks, on="block_number", how="left", coalesce=True)
                   .select("hash", pl.from_epoch("block_timestamp", time_unit="s").dt.cast_time_unit("ms").alias("included_ts")))
        comb = combine_frames(bn, ds_raw, xa)
        res = attach_inclusion(comb.select("hash", "first_seen"), tx_ts)
        n_written = _write_month_parts(res, out_dir / "_parts", s)
        if n_written != res.height:
            sys.exit(f"shard {s}: wrote {n_written} rows but combined {res.height} - stopping")
        mined = int(res["included_ts"].is_not_null().sum()) if res.height else 0
        (out_dir / "_stats" / f"shard_{s}.json").write_text(json.dumps(
            {"shard": s, "bn_rows": bn.height, "ds_rows": ds_raw.height, "xatu_rows": xa.height,
             "tx_rows": tx.height, "out_rows": res.height, "mined": mined, **_source_stats(comb)}))
        built += 1
        print(f"shard {s}: BN {bn.height:,} + Dumpster {ds_raw.height:,} + Xatu {xa.height:,} + tx {tx.height:,} -> "
              f"{res.height:,} public hashes ({mined:,} mined), {time.time()-t0:.0f}s", flush=True)
    print(f"SINGLE_PASS DONE (shards {names[0]}..{names[-1]}): {built} built, {skipped} already existed, "
          f"{(time.time()-t_start)/60:.1f} min", flush=True)


def _assemble(out_dir: Path, parts_name: str, stats_name: str, out_subdir: str, prefix: str, label: str,
              final_dir: Path = None):
    """Concatenate the per-shard monthly parts into one file per month. Working files (_parts, _stats)
    stay in out_dir; the finished monthly files go to final_dir when given (the public folder
    data/processed), else to out_dir/out_subdir."""
    n_stats = len(list((out_dir / stats_name).glob("shard_*.json")))
    if n_stats != len(SHARDS):
        sys.exit(f"{label}: only {n_stats} of {len(SHARDS)} shards have finished the previous phase")
    target = final_dir if final_dir else (out_dir / out_subdir if out_subdir else out_dir)
    target.mkdir(parents=True, exist_ok=True)
    months = sorted(d.name for d in (out_dir / parts_name).iterdir() if d.is_dir())
    t_start = time.time()
    built = skipped = 0
    print(f"=== {label}: {len(months)} months -> {target} ===", flush=True)
    for mth in months:
        out = target / f"{prefix}_{mth}.parquet"
        if out.exists():
            skipped += 1
            continue
        df = pl.concat([pl.read_parquet(f) for f in sorted((out_dir / parts_name / mth).glob("shard_*.parquet"))]).sort("hash")
        tmp = out.with_suffix(".parquet.tmp")
        df.write_parquet(tmp, compression="zstd")
        os.replace(tmp, out)
        built += 1
        extra = f" ({df['included_ts'].is_not_null().sum():,} mined)" if "included_ts" in df.columns else ""
        print(f"{mth}: {df.height:,} hashes{extra}", flush=True)
    stats_rows = sum(json.loads(p.read_text())["out_rows"] for p in (out_dir / stats_name).glob("shard_*.json"))
    files_rows = sum(pl.scan_parquet(p).select(pl.len()).collect().item() for p in target.glob(f"{prefix}_*.parquet"))
    print(f"{label.upper()} DONE: {built} built, {skipped} already existed, {files_rows:,} rows in monthly files vs "
          f"{stats_rows:,} per shard stats ({'MATCH' if files_rows == stats_rows else 'MISMATCH'}), "
          f"{(time.time()-t_start)/60:.1f} min", flush=True)


def assemble_first_seen(out_dir: Path):
    _assemble(out_dir, "_parts_fs", "_stats_fs", "first_seen", "first_seen", "assemble_first_seen")


def assemble(out_dir: Path, final_dir: Path):
    _assemble(out_dir, "_parts", "_stats", "", "public_mempool", "assemble", final_dir)


def verify(ds_dir: Path, bn_dir: Path, out_dir: Path, final_dir: Path, k: int, first_days: int, xatu_file: Path,
           xatu_check_file, xatu_source: str = "full", xatu_dir: Path = None, seed: int = 20261105):
    rng = random.Random(seed)
    days = list_ds_days(ds_dir, first_days)
    sample = []
    # a share from random Dumpster days, random BN shards and the Xatu file
    for d in rng.sample(days, min(3, len(days))):
        h = pl.read_parquet(ds_dir / f"{d}.parquet", columns=["hash"])["hash"].str.to_lowercase()
        sample += h.sample(min(k // 6, h.len()), seed=seed).to_list()
    present = [s for s in SHARDS if (bn_dir / f"shard_{s}.parquet").exists()]
    for s in rng.sample(present, min(3, len(present))):
        h = pl.read_parquet(bn_dir / f"shard_{s}.parquet", columns=["hash"])["hash"]
        sample += h.sample(min(k // 6, h.len()), seed=seed).to_list()
    if xatu_source == "full":
        # the full Xatu set is ~1.9 billion hashes: sample from a few shard files, recompute from shard files
        xa_present = [s for s in SHARDS if (xatu_dir / f"shard_{s}.parquet").exists()]
        for s in rng.sample(xa_present, min(3, len(xa_present))):
            h = read_xatu_shard(xatu_dir, s)["hash"]
            sample += h.sample(min(k // 6, h.len()), seed=seed).to_list()
        xa_all = None
    else:
        xa_all = pl.read_parquet(xatu_file, columns=["hash", "first_seen"]).with_columns(
            pl.col("hash").str.to_lowercase(), pl.col("first_seen").cast(pl.Datetime("ms")))
        sample += xa_all["hash"].sample(min(k // 6, xa_all.height), seed=seed).to_list()
    sample = sorted(set(sample))
    sdf = pl.DataFrame({"hash": sample}).with_columns(shard_expr())
    print(f"verify: {len(sample)} sampled hashes; recomputing from BN shards, Xatu and all {len(days)} Dumpster days", flush=True)
    bn_truth = pl.concat([
        read_bn_shard(bn_dir, s).filter(pl.col("hash").is_in(sdf.filter(pl.col("shard") == s)["hash"].to_list()))
        for s in sdf["shard"].unique().to_list()
    ])
    sample_df = pl.DataFrame({"hash": sample})
    ds_parts = []
    for d in days:
        part = (pl.read_parquet(ds_dir / f"{d}.parquet")
                  .select("hash", "timestamp", "includedAtBlockHeight", "inclusionDelayMs")
                  .with_columns(pl.col("hash").str.to_lowercase(), pl.col("includedAtBlockHeight").cast(pl.Int64))
                  .join(sample_df, on="hash", how="semi"))
        if part.height:
            ds_parts.append(part)
    ds_all = pl.concat(ds_parts) if ds_parts else pl.DataFrame(schema={**DS_EMPTY, "inclusionDelayMs": pl.Int64})
    if xatu_source == "full":
        xa_truth = pl.concat([
            read_xatu_shard(xatu_dir, s).filter(pl.col("hash").is_in(sdf.filter(pl.col("shard") == s)["hash"].to_list()))
            for s in sdf["shard"].unique().to_list()
        ])
    else:
        xa_truth = xa_all.join(sample_df, on="hash", how="semi")
    truth = combine_frames(bn_truth, ds_all.select(DS_COLS), xa_truth)  # reuses the rule; checks routing/sharding/joins, not the rule itself
    mfiles = sorted(final_dir.glob("public_mempool_*.parquet"))
    got = pl.scan_parquet([str(p) for p in mfiles]).filter(pl.col("hash").is_in(sample)).collect()
    mism = truth.select("hash", "first_seen", pl.lit(True).alias("t")).join(
        got.select("hash", "first_seen", pl.lit(True).alias("g")), on="hash", how="full", suffix="_g", coalesce=True)
    cond = (pl.col("t").is_null() | pl.col("g").is_null()
            | (pl.col("first_seen").is_null() != pl.col("first_seen_g").is_null())
            | (pl.col("first_seen") != pl.col("first_seen_g")).fill_null(False))
    bad = mism.filter(cond)
    print(f"VERIFY: {len(sample)} sampled; expected public rows {truth.height}, output rows {got.height}; mismatches: {bad.height}", flush=True)
    if bad.height:
        print(bad.head(10), flush=True)
    # independent check of included_ts: Dumpster's first sighting + inclusionDelayMs is the block timestamp
    ds_inc = (ds_all.filter(pl.col("includedAtBlockHeight") > 0)
              .group_by("hash").agg((pl.col("timestamp").dt.epoch("ms") + pl.col("inclusionDelayMs")).first().alias("bt_ms")))
    chk = got.join(ds_inc, on="hash", how="inner").with_columns(pl.col("included_ts").dt.epoch("ms").alias("got_ms"))
    n_bad_inc = chk.filter(pl.col("got_ms").is_null() | (pl.col("got_ms") != pl.col("bt_ms"))).height
    print(f"VERIFY INCLUDED_TS: {chk.height} sampled hashes with a Dumpster-known block; mismatches vs Erigon-derived included_ts: {n_bad_inc}", flush=True)
    if xatu_check_file and Path(xatu_check_file).exists():
        xc = (pl.read_parquet(xatu_check_file, columns=["hash", "included_ts"])
                .with_columns(pl.col("hash").str.to_lowercase(), pl.col("included_ts").cast(pl.Datetime("ms")))
                .filter(pl.col("included_ts").is_not_null()).join(sample_df, on="hash", how="semi"))
        xchk = got.join(xc.select("hash", pl.col("included_ts").alias("xa_inc")), on="hash", how="inner")
        n_bad_x = xchk.filter(pl.col("included_ts").is_null() | (pl.col("included_ts") != pl.col("xa_inc"))).height
        print(f"VERIFY XATU_INCLUDED_TS: {xchk.height} sampled Xatu hashes mined in the window; mismatches vs the Xatu intermediate: {n_bad_x}", flush=True)
    tot = dup = 0
    stats_rows = 0
    for p in mfiles:
        h = pl.read_parquet(p, columns=["hash"])["hash"]
        tot += h.len()
        dup += int(h.n_unique() != h.len())
    for p in (out_dir / "_stats").glob("shard_*.json"):
        stats_rows += json.loads(p.read_text())["out_rows"]
    print(f"VERIFY TOTALS: {tot:,} rows across {len(mfiles)} monthly files; "
          f"months with duplicate hashes: {dup}; rows per _stats: {stats_rows:,}", flush=True)


PHASES = ["route", "combine", "assemble_fs", "route_tx", "attach", "assemble"]
SINGLE_PASS_PHASES = ["route", "route_tx", "single_pass", "assemble"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ds-dir", default="data/raw/dumpster")
    ap.add_argument("--bn-dir", default="data/interim/blocknative_global")
    ap.add_argument("--chain-dir", "--erigon-dir", dest="erigon_dir", default="data/raw/chain",
                    help="folder holding transactions/ (and blocks/ unless --blocks-dir is given), written by 00_extract_chain_blocks.py")
    ap.add_argument("--blocks-dir", default=None,
                    help="folder with blocks_*.parquet (block_number, block_timestamp); default <chain-dir>/blocks")
    ap.add_argument("--xatu-source", choices=["full", "gap"], default="full",
                    help="full: all Xatu days from --xatu-dir (default); gap: only the six Dumpster gap days from --xatu-file")
    ap.add_argument("--xatu-dir", default="data/interim/xatu_full/merged",
                    help="folder with the 256 shard_XX.parquet files (hash, first_ts, last_ts) and _stats_XX.json written by "
                         "scripts/05_xatu_mempool_all_days.py --merge")
    ap.add_argument("--xatu-file", default="data/interim/xatu_gap_days/xatu_gap_days_first_seen.parquet",
                    help="Xatu gap-day intermediate (hash, first_seen); used with --xatu-source gap. The file is not kept "
                         "and the script that built it was removed")
    ap.add_argument("--xatu-check-file", default=None,
                    help="optional Xatu gap-day file with included_ts (hash, included_ts), used only by --verify as a cross-check; "
                         "no script builds it any more. Default: no cross-check")
    ap.add_argument("--scratch-dir", default="D:/pfof_scratch/public_mempool_shards")
    ap.add_argument("--out-dir", default="data/interim/public_mempool_work",
                    help="working folder: _parts, _stats (and first_seen/ for the two-stage variant)")
    ap.add_argument("--final-dir", default="data/processed",
                    help="finished monthly files public_mempool_YYYY-MM.parquet (the public data)")
    ap.add_argument("--batch-days", type=int, default=30)
    ap.add_argument("--tx-files-per-batch", type=int, default=40)
    ap.add_argument("--first-days", type=int, default=0, help="only use the first N Dumpster days (testing)")
    ap.add_argument("--phase", choices=PHASES + ["single_pass", "all", "all_single"], default=None)
    ap.add_argument("--batch-start", type=int, default=0, help="route phases: first batch index (parallel routing workers)")
    ap.add_argument("--batch-end", type=int, default=None, help="route phases: last batch index, inclusive")
    ap.add_argument("--shard-start", type=int, default=0)
    ap.add_argument("--shard-end", type=int, default=len(SHARDS) - 1)
    ap.add_argument("--verify", type=int, default=0)
    a = ap.parse_args()
    ds_dir, bn_dir, scratch, out_dir = Path(a.ds_dir), Path(a.bn_dir), Path(a.scratch_dir), Path(a.out_dir)
    final_dir = Path(a.final_dir)
    erigon = Path(a.erigon_dir)
    blocks_dir = Path(a.blocks_dir) if a.blocks_dir else erigon / "blocks"
    run = (PHASES if a.phase == "all" else SINGLE_PASS_PHASES if a.phase == "all_single"
           else ([a.phase] if a.phase else []))
    for ph in run:
        if ph in ("route", "route_tx"):
            scratch.mkdir(parents=True, exist_ok=True)
        if ph == "route":
            route_dumpster(ds_dir, scratch, a.batch_days, a.first_days, a.batch_start, a.batch_end)
            if a.xatu_source == "full":
                route_xatu_full(Path(a.xatu_dir), scratch)
            else:
                route_xatu(Path(a.xatu_file), scratch)
        elif ph == "combine":
            combine(bn_dir, scratch, out_dir, a.shard_start, a.shard_end)
        elif ph == "assemble_fs":
            assemble_first_seen(out_dir)
        elif ph == "route_tx":
            route_transactions(erigon / "transactions", scratch, a.tx_files_per_batch, a.batch_start, a.batch_end)
        elif ph == "attach":
            attach(scratch, out_dir, blocks_dir, a.shard_start, a.shard_end)
        elif ph == "single_pass":
            single_pass(bn_dir, scratch, out_dir, blocks_dir, a.shard_start, a.shard_end)
        elif ph == "assemble":
            assemble(out_dir, final_dir)
    if a.verify:
        verify(ds_dir, bn_dir, out_dir, final_dir, a.verify, a.first_days, Path(a.xatu_file), a.xatu_check_file,
               a.xatu_source, Path(a.xatu_dir))


if __name__ == "__main__":
    main()
