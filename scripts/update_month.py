# scripts/update_month.py
"""
Update of the public mempool dataset: adds every COMPLETE month after the last one in data/processed/, without
rebuilding anything. A month is complete when it has ended (UTC). Example: with 2026-10 the last month built, a run on
15 January 2027 adds 2026-11 and 2026-12 and stops there; a run on 2 February adds 2027-01 as well.
(--through YYYY-MM stops earlier; --month YYYY-MM builds one month.)

Steps 1 and 3 run once for the whole range of months; steps 2 and 4 to 7 run for each month in order:
  1. Downloads that month's Dumpster days (01_download_dumpster_days.py) and Xatu days (05_xatu_mempool_all_days.py,
     daily files only; the global Xatu merge is not used for updates). Blocknative ended in 2025-03 and is never read again.
  2. Checks the month is complete: it has ended (UTC) and every day has a Dumpster file OR a Xatu file (days covered
     by one source only are listed; a day with neither stops the run unless --allow-gap-days).
  3. Extends the chain data in data/raw/chain with 00_extract_chain_blocks.py (finalized blocks only, from a node).
  4. Builds the month: Dumpster + Xatu combined under the public rule (the same function as the full build),
     hashes already published in earlier months removed (hash index), block time attached from the chain data.
  5. Writes data/processed/public_mempool_YYYY-MM.parquet (hash, first_seen, included_ts; sorted by hash).
     A hash whose first sighting turns out to belong to an EARLIER month (a day-boundary straggler) is merged into that month's file.
  6. Refreshes included_ts: null values in the last --refresh-months published months (default 3) are filled from the
     blocks added since the previous run.
  7. Verifies, then commits: the hash index and data/state.json are updated LAST, so an interrupted run can simply be rerun.

Files:
  data/state.json                         last month built, last chain block seen by the previous run
  data/interim/hash_index/shard_XX.parquet  one row per published hash: key = 8 bytes of the hash (hex characters 5 to 20),
                                          sorted; XX = the hash's first byte (the 257-shard scheme of the full build).
                                          An 8-byte key makes a false match between two different hashes about 1e-3 per month
                                          over the whole history; such a match would drop one new hash.
  logs/update_log.jsonl                   one line of statistics per update

Commands (run from the repository root; needs a node for step 3 unless --no-chain):
    python scripts/update_month.py --init               # once: build the hash index and state.json from data/processed/
    python scripts/update_month.py --status             # show state, the months that would be built, what is missing
    python scripts/update_month.py --dry-run            # all checks for those months, writes nothing
    python scripts/update_month.py                      # add every complete month after the last one built
    python scripts/update_month.py --through 2026-12    # ... but stop after December 2026
    python scripts/update_month.py --month 2026-11 --no-download --no-chain   # one month, inputs already in place

Assumption (stated, not measured): a transaction first seen by a sentry is not mined more than one day before it is seen,
so block times are looked up from one day before the month starts. A hash mined earlier than that keeps a null included_ts.
Not safe to run two updates at once.
"""
import argparse
import calendar
import datetime as dt
import importlib.util as ilu
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import polars as pl

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
os.chdir(ROOT)
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")


def _load(name: str, file: str):
    spec = ilu.spec_from_file_location(name, HERE / file)
    mod = ilu.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


comb = _load("combine06", "06_local_bn_dumpster_combine.py")  # the full build's combine rule, reused
SHARDS = comb.SHARDS
shard_expr = comb.shard_expr

KEY_DT = pl.Binary
OUT_COLS = {"hash": pl.String, "first_seen": pl.Datetime("ms"), "included_ts": pl.Datetime("ms")}


# ----------------------------------------------------------------------------- dates and paths

def parse_month(m: str):
    y, mo = (int(x) for x in m.split("-"))
    return y, mo


def month_str(y: int, mo: int) -> str:
    return f"{y:04d}-{mo:02d}"


def next_month(m: str) -> str:
    y, mo = parse_month(m)
    return month_str(y + 1, 1) if mo == 12 else month_str(y, mo + 1)


def prev_month(m: str) -> str:
    y, mo = parse_month(m)
    return month_str(y - 1, 12) if mo == 1 else month_str(y, mo - 1)


def month_bounds(m: str):
    """(first day, last day, first second of the next month as unix time, first second of the month as unix time)."""
    y, mo = parse_month(m)
    first = dt.date(y, mo, 1)
    last = dt.date(y, mo, calendar.monthrange(y, mo)[1])
    nm = next_month(m)
    ny, nmo = parse_month(nm)
    end_epoch = calendar.timegm(dt.datetime(ny, nmo, 1).timetuple())
    start_epoch = calendar.timegm(dt.datetime(y, mo, 1).timetuple())
    return first, last, end_epoch, start_epoch


def days_between(a: dt.date, b: dt.date):
    return [(a + dt.timedelta(days=i)).isoformat() for i in range((b - a).days + 1)]


class Paths:
    def __init__(self, a):
        d = Path(a.data_dir)
        self.data = d
        self.processed = d / "processed"
        self.dumpster = d / "raw" / "dumpster"
        self.chain = Path(a.chain_dir) if a.chain_dir else d / "raw" / "chain"
        self.xatu_root = d / "interim" / "xatu_full"
        self.xatu_daily = self.xatu_root / "daily"
        self.index = d / "interim" / "hash_index"
        self.state = d / "state.json"
        self.log = Path("logs") / "update_log.jsonl"  # relative to the repository root, like the other scripts
        self.work = Path(a.work_dir)

    def month_file(self, m: str) -> Path:
        return self.processed / f"public_mempool_{m}.parquet"


def read_state(p: Paths):
    return json.loads(p.state.read_text()) if p.state.exists() else None


def write_state(p: Paths, st: dict):
    tmp = p.state.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(st, indent=1))
    os.replace(tmp, p.state)


def key_expr() -> pl.Expr:
    return pl.col("hash").str.slice(4, 16).str.decode("hex").alias("key")


def atomic_write(df: pl.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    df.write_parquet(tmp, compression="zstd")
    os.replace(tmp, path)


# ----------------------------------------------------------------------------- hash index

def read_index(p: Paths, s: str) -> pl.DataFrame:
    f = p.index / f"shard_{s}.parquet"
    return pl.read_parquet(f) if f.exists() else pl.DataFrame(schema={"key": KEY_DT})


def build_index(p: Paths):
    """One-time: index every hash already in data/processed/, routed by first byte, sorted by key."""
    files = sorted(p.processed.glob("public_mempool_*.parquet"))
    if not files:
        sys.exit(f"no monthly files in {p.processed}")
    if p.index.exists() and any(p.index.glob("shard_*.parquet")):
        sys.exit(f"{p.index} already holds an index; move it aside to rebuild")
    parts = p.work / "index_parts"
    (parts / "_done").mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    total = 0
    for f in files:
        mark = parts / "_done" / (f.stem + ".json")
        if mark.exists():
            total += json.loads(mark.read_text())["rows"]
            continue
        df = pl.read_parquet(f, columns=["hash"]).with_columns(shard_expr(), key_expr()).select("shard", "key")
        for key_tuple, sub in df.partition_by(["shard"], as_dict=True).items():
            s = key_tuple[0] if isinstance(key_tuple, tuple) else key_tuple
            d = parts / s
            d.mkdir(exist_ok=True)
            tmp = d / (f.stem + ".parquet.tmp")
            sub.select("key").write_parquet(tmp, compression="zstd")
            os.replace(tmp, d / (f.stem + ".parquet"))
        mark.write_text(json.dumps({"rows": df.height}))
        total += df.height
        print(f"[index] {f.name}: {df.height:,} hashes, {(time.time()-t0)/60:.1f} min", flush=True)
    expected = sum(pl.scan_parquet(f).select(pl.len()).collect().item() for f in files)
    if total != expected:
        sys.exit(f"index parts hold {total:,} hashes but the monthly files hold {expected:,}")
    n_written = 0
    for s in SHARDS:
        fs = sorted((parts / s).glob("*.parquet")) if (parts / s).exists() else []
        df = pl.concat([pl.read_parquet(f) for f in fs]).sort("key") if fs else pl.DataFrame(schema={"key": KEY_DT})
        atomic_write(df, p.index / f"shard_{s}.parquet")
        n_written += df.height
    if n_written != expected:
        sys.exit(f"index shards hold {n_written:,} keys, expected {expected:,}")
    print(f"INDEX BUILT: {n_written:,} hashes in {len(SHARDS)} shards, {(time.time()-t0)/60:.1f} min", flush=True)
    return n_written


# ----------------------------------------------------------------------------- chain data

def chain_blocks(p: Paths) -> pl.DataFrame:
    d = p.chain / "blocks"
    if not d.exists() or not list(d.glob("blocks_*.parquet")):
        sys.exit(f"no chain data in {p.chain}; run scripts/00_extract_chain_blocks.py")
    return comb.load_blocks(d)


def read_tx_window(p: Paths, read_from: int) -> pl.DataFrame:
    """(hash, block_number) for blocks >= read_from, from the chain transaction files that can hold them."""
    files = []
    for f in sorted((p.chain / "transactions").glob("transactions_*.parquet")):
        last = int(f.stem.split("_")[2])
        if last >= read_from:
            files.append(f)
    if not files:
        return pl.DataFrame(schema={"hash": pl.String, "block_number": pl.Int64})
    df = pl.concat([
        pl.read_parquet(f, columns=["transaction_hash", "block_number"])
          .select(pl.col("transaction_hash").str.to_lowercase().alias("hash"), pl.col("block_number").cast(pl.Int64))
        for f in files
    ])
    return df.filter(pl.col("block_number") >= read_from)


def with_included_ts(tx: pl.DataFrame, blocks: pl.DataFrame) -> pl.DataFrame:
    """tx (hash, block_number) to (hash, block_number, included_ts); one row per hash."""
    j = tx.join(blocks.select("block_number", "block_timestamp"), on="block_number", how="left")
    if j["block_timestamp"].null_count():
        sys.exit("transactions in blocks without a timestamp: the blocks and transactions chain files do not match")
    return (j.select("hash", "block_number", pl.from_epoch("block_timestamp", time_unit="s").dt.cast_time_unit("ms").alias("included_ts"))
             .unique(subset="hash", keep="first"))


# ----------------------------------------------------------------------------- inputs

def run_script(args: list):
    print("+ " + " ".join(args), flush=True)
    r = subprocess.run([sys.executable, *args], cwd=ROOT)
    if r.returncode != 0:
        sys.exit(f"{args[0]} failed with exit code {r.returncode}")


def day_sources(p: Paths, days: list):
    ds = {d: (p.dumpster / f"{d}.parquet").exists() for d in days}
    xa = {d: (p.xatu_daily / f"{d}.parquet").exists() for d in days}
    return ds, xa


def check_month_ended(month: str):
    _, last, _, _ = month_bounds(month)
    today = dt.datetime.now(dt.timezone.utc).date()
    if today <= last:
        sys.exit(f"{month} has not ended yet (today is {today} UTC); run after {last}")


def check_days(p: Paths, month: str, allow_gap: bool):
    first, last, _, _ = month_bounds(month)
    days = days_between(first, last)
    ds, xa = day_sources(p, days)
    neither = [d for d in days if not ds[d] and not xa[d]]
    only_ds = [d for d in days if ds[d] and not xa[d]]
    only_xa = [d for d in days if xa[d] and not ds[d]]
    print(f"{month}: {len(days)} days; both sources {len(days) - len(neither) - len(only_ds) - len(only_xa)}, "
          f"Dumpster only {len(only_ds)}, Xatu only {len(only_xa)}, neither {len(neither)}", flush=True)
    if only_ds:
        print(f"  Dumpster only: {', '.join(only_ds)}", flush=True)
    if only_xa:
        print(f"  Xatu only: {', '.join(only_xa)}", flush=True)
    if neither:
        msg = f"days with neither source: {', '.join(neither)}"
        if not allow_gap:
            sys.exit(msg + " (use --allow-gap-days to build the month anyway)")
        print("  WARNING " + msg, flush=True)
    return {"days": len(days), "dumpster_only": only_ds, "xatu_only": only_xa, "neither": neither}


def download_inputs(p: Paths, first_month: str, last_month: str):
    first, _, _, _ = month_bounds(first_month)
    _, last, _, _ = month_bounds(last_month)
    start = (first - dt.timedelta(days=1)).isoformat()   # one overlap day for sightings across the month boundary
    run_script(["scripts/01_download_dumpster_days.py", "--start", start, "--end", last.isoformat()])
    run_script(["scripts/05_xatu_mempool_all_days.py", "--out-root", str(p.xatu_root), "--start", start, "--end", last.isoformat()])


def extend_chain(p: Paths, a):
    cmd = ["scripts/00_extract_chain_blocks.py", "--out-dir", str(p.chain)]
    if a.chain_through_month:
        cmd += ["--through-month", a.chain_through_month]
    run_script(cmd)


# ----------------------------------------------------------------------------- the update

def route_days(p: Paths, scratch: Path, days: list):
    (scratch / "_done").mkdir(parents=True, exist_ok=True)
    for i, d in enumerate(days):
        mark = scratch / "_done" / f"day{i:03d}.json"
        if mark.exists() and json.loads(mark.read_text())["day"] == d:
            continue
        n_ds = n_xa = 0
        f = p.dumpster / f"{d}.parquet"
        if f.exists():
            df = (pl.read_parquet(f).select(comb.DS_COLS)
                    .with_columns(pl.col("hash").str.to_lowercase(), pl.col("includedAtBlockHeight").cast(pl.Int64),
                                  pl.col("timestamp").cast(pl.Datetime("ms"))))
            n_ds = df.height
            if comb._route_frames(df, scratch, "d", i, comb.DS_COLS) != n_ds:
                sys.exit(f"Dumpster {d}: routed rows differ from rows read")
        f = p.xatu_daily / f"{d}.parquet"
        if f.exists():
            df = (pl.read_parquet(f, columns=["hash", "first_ts"])
                    .select(pl.col("hash").str.to_lowercase(), pl.col("first_ts").cast(pl.Datetime("ms")).alias("first_seen")))
            n_xa = df.height
            if comb._route_frames(df, scratch, "x", i, ["hash", "first_seen"]) != n_xa:
                sys.exit(f"Xatu {d}: routed rows differ from rows read")
        mark.write_text(json.dumps({"day": d, "dumpster_rows": n_ds, "xatu_rows": n_xa}))
        print(f"[route {i+1}/{len(days)}] {d}: Dumpster {n_ds:,}, Xatu {n_xa:,}", flush=True)


def fill_nulls(df: pl.DataFrame, upd: pl.DataFrame):
    """df (hash, first_seen, included_ts); upd (hash, included_ts). Fill null included_ts from upd. Returns (df, n_filled)."""
    nulls = df.filter(pl.col("included_ts").is_null()).select("hash")
    if not nulls.height or not upd.height:
        return df, 0
    hit = nulls.join(upd.rename({"included_ts": "_new"}), on="hash", how="inner")
    if not hit.height:
        return df, 0
    out = (df.join(hit, on="hash", how="left", coalesce=True)
             .with_columns(pl.coalesce("included_ts", "_new").alias("included_ts")).drop("_new"))
    return out, hit.height


def update_month(a, p: Paths, month: str):
    st = read_state(p)
    if st is None:
        sys.exit("no state.json: run `python scripts/update_month.py --init` once first")
    if month != next_month(st["last_month"]) and not a.force:
        sys.exit(f"the next month is {next_month(st['last_month'])}, not {month} (months are added in order; --force overrides)")
    if p.month_file(month).exists() and not (p.work / month / "stats").exists() and not a.force:
        sys.exit(f"{p.month_file(month)} exists and there is no unfinished run of {month} to resume in {p.work / month}")
    first, last, end_epoch, start_epoch = month_bounds(month)
    t_start = time.time()
    check_month_ended(month)

    cov = check_days(p, month, a.allow_gap_days)
    blocks = chain_blocks(p)
    chain_last = int(blocks["block_number"].max())
    chain_last_ts = int(blocks.filter(pl.col("block_number") == chain_last)["block_timestamp"].item())
    if chain_last_ts < end_epoch - 24:   # the last block of the month is at most one slot (12 s) before its end
        sys.exit(f"chain data ends at block {chain_last:,} ({dt.datetime.utcfromtimestamp(chain_last_ts)} UTC), before the end of {month}")
    win_first = int(blocks.filter(pl.col("block_timestamp") >= start_epoch - 86400)["block_number"].min())
    read_from = min(win_first, int(st["chain_last_block"]) + 1)
    print(f"chain: blocks to {chain_last:,}; month window starts at block {win_first:,}; "
          f"previous run saw block {st['chain_last_block']:,}; reading transactions from block {read_from:,}", flush=True)
    if a.dry_run:
        print("DRY RUN: checks passed, nothing written")
        return

    tx = with_included_ts(read_tx_window(p, read_from), blocks)
    print(f"transactions read: {tx.height:,}", flush=True)
    tx_shards = {}
    for kt, sub in tx.with_columns(shard_expr()).partition_by(["shard"], as_dict=True).items():
        s = kt[0] if isinstance(kt, tuple) else kt
        tx_shards[s] = sub.select("hash", "included_ts")
    prev_block = int(st["chain_last_block"])
    new_ts = tx.filter(pl.col("block_number") > prev_block).select("hash", "included_ts")  # blocks added since the previous run

    work = p.work / month
    scratch = work / "scratch"
    parts = work / "parts"
    idx_new = work / "index_new"
    for d in (scratch, parts, idx_new, work / "stats"):
        d.mkdir(parents=True, exist_ok=True)
    days = days_between(first - dt.timedelta(days=1), last)
    route_days(p, scratch, days)

    empty_bn = pl.DataFrame(schema=comb.BN_SCHEMA)
    tot = {"combined": 0, "already_published": 0, "new": 0, "new_null_ts": 0}
    for k, s in enumerate(SHARDS, 1):
        sj = work / "stats" / f"shard_{s}.json"
        if sj.exists():
            t = json.loads(sj.read_text())
            for key in tot:
                tot[key] += t[key]
            continue
        ds = comb._read_scratch(scratch, s, "d", comb.DS_EMPTY)
        xa = comb._read_scratch(scratch, s, "x", comb.XA_EMPTY)
        res = comb.combine_frames(empty_bn, ds, xa).select("hash", "first_seen")
        idx = read_index(p, s)
        res = res.with_columns(key_expr())
        new = res.join(idx, on="key", how="anti").drop("key")
        txs = tx_shards.get(s)
        new = (new.join(txs, on="hash", how="left", coalesce=True) if txs is not None
               else new.with_columns(pl.lit(None, dtype=pl.Datetime("ms")).alias("included_ts")))
        new = new.select(list(OUT_COLS))
        n_parts = comb._write_month_parts(new, parts, s)
        if n_parts != new.height:
            sys.exit(f"shard {s}: wrote {n_parts} rows but built {new.height}")
        keys = new.select(key_expr()).get_column("key")
        merged = pl.concat([idx, pl.DataFrame({"key": keys})]).sort("key")
        atomic_write(merged, idx_new / f"shard_{s}.parquet")
        stats = {"shard": s, "combined": res.height, "already_published": res.height - new.height, "new": new.height,
                 "new_null_ts": int(new["included_ts"].null_count())}
        sj.write_text(json.dumps(stats))
        for key in tot:
            tot[key] += stats[key]
        if k % 32 == 0 or k == len(SHARDS):
            print(f"[shard {k}/{len(SHARDS)}] combined {tot['combined']:,}, already published {tot['already_published']:,}, "
                  f"new {tot['new']:,}, {(time.time()-t_start)/60:.1f} min", flush=True)
    if tot["combined"] != tot["already_published"] + tot["new"]:
        sys.exit("shard statistics do not add up")

    # ---- assemble month files from the parts (the month itself, plus any earlier month that received stragglers)
    written = {}
    for mdir in sorted(d for d in parts.iterdir() if d.is_dir()):
        m = mdir.name
        fs = sorted(mdir.glob("shard_*.parquet"))
        newdf = pl.concat([pl.read_parquet(f) for f in fs])
        if m == "unknown":
            sys.exit(f"{newdf.height} new hashes have no first_seen")
        if m == month:
            out = newdf
        elif m < month:
            old = pl.read_parquet(p.month_file(m)) if p.month_file(m).exists() else pl.DataFrame(schema=OUT_COLS)
            out = pl.concat([old, newdf]).unique(subset="hash", keep="first")
            print(f"  {newdf.height:,} straggler hashes belong to {m}; merged into that file", flush=True)
        else:
            sys.exit(f"{newdf.height} new hashes have first_seen in {m}, after {month}")
        out = out.sort("hash")
        atomic_write(out, p.month_file(m))
        written[m] = out.height
    if month not in written:
        sys.exit(f"no new hashes for {month}")

    # ---- verify the new month file
    mf = pl.read_parquet(p.month_file(month))
    if mf["hash"].n_unique() != mf.height or not mf["hash"].is_sorted():
        sys.exit(f"{p.month_file(month).name}: hashes are not unique and sorted")
    lo = dt.datetime(first.year, first.month, 1)
    hi = dt.datetime.utcfromtimestamp(end_epoch)
    if mf["first_seen"].min() < lo or mf["first_seen"].max() >= hi:
        sys.exit(f"{p.month_file(month).name}: first_seen outside {month}")
    print(f"VERIFIED {p.month_file(month).name}: {mf.height:,} hashes, unique and sorted, first_seen inside the month, "
          f"included_ts null for {mf['included_ts'].null_count():,}", flush=True)

    # ---- refresh included_ts of earlier months from the blocks added since the previous run
    refreshed = {}
    m = st["last_month"]
    for _ in range(a.refresh_months):
        f = p.month_file(m)
        if f.exists() and new_ts.height:
            df = pl.read_parquet(f)
            df2, n = fill_nulls(df, new_ts)
            if n:
                atomic_write(df2.sort("hash"), f)
            refreshed[m] = n
            print(f"refresh {m}: {n:,} null included_ts filled", flush=True)
        m = prev_month(m)

    # ---- commit: index first, state last
    for s in SHARDS:
        os.replace(idx_new / f"shard_{s}.parquet", p.index / f"shard_{s}.parquet")
    n_index = sum(pl.scan_parquet(p.index / f"shard_{s}.parquet").select(pl.len()).collect().item() for s in SHARDS)
    new_state = {"last_month": month, "chain_last_block": chain_last, "index_rows": n_index,
                 "updated_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")}
    write_state(p, new_state)
    entry = {**new_state, "month": month, "rows_new_month": mf.height, "totals": tot, "stragglers": {m: n for m, n in written.items() if m != month},
             "refreshed": refreshed, "coverage": cov, "minutes": round((time.time() - t_start) / 60, 1)}
    p.log.parent.mkdir(parents=True, exist_ok=True)
    with open(p.log, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry) + "\n")
    shutil.rmtree(work, ignore_errors=True) if a.clean_work else None
    print(f"UPDATE DONE: {month}, {mf.height:,} new hashes ({tot['already_published']:,} already published), "
          f"index {n_index:,}, {(time.time()-t_start)/60:.1f} min", flush=True)


def months_to_build(a, st: dict):
    """The months to add: --month if given, else every month after the last built that has already ended (UTC)."""
    if a.month:
        return [a.month]
    today = dt.datetime.now(dt.timezone.utc).date()
    out, m = [], next_month(st["last_month"])
    while month_bounds(m)[1] < today and not (a.through and m > a.through):
        out.append(m)
        m = next_month(m)
    return out


def run_update(a, p: Paths):
    st = read_state(p)
    if st is None:
        sys.exit("no state.json: run `python scripts/update_month.py --init` once first")
    months = months_to_build(a, st)
    if not months:
        nm = next_month(st["last_month"])
        print(f"nothing to do: the last month built is {st['last_month']} and {nm} ends on {month_bounds(nm)[1]} (UTC)")
        return
    print(f"months to add: {', '.join(months)}", flush=True)
    if not a.dry_run:
        if not a.no_download:
            download_inputs(p, months[0], months[-1])
        if not a.no_chain:
            extend_chain(p, a)
    for m in months:
        update_month(a, p, m)
    if a.dry_run:
        print(f"DRY RUN done for {len(months)} month(s): nothing written")


def init(a, p: Paths):
    if p.state.exists() and not a.force:
        sys.exit(f"{p.state} exists (--force overwrites it)")
    files = sorted(p.processed.glob("public_mempool_*.parquet"))
    if not files:
        sys.exit(f"no monthly files in {p.processed}")
    last_month = files[-1].stem.replace("public_mempool_", "")
    blocks = chain_blocks(p)
    n = build_index(p)
    st = {"last_month": last_month, "chain_last_block": int(blocks["block_number"].max()), "index_rows": n,
          "updated_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")}
    write_state(p, st)
    print(f"INIT DONE: {st}")


def status(a, p: Paths):
    st = read_state(p)
    print(f"state: {st}")
    files = sorted(p.processed.glob("public_mempool_*.parquet"))
    print(f"monthly files: {len(files)}" + (f", {files[0].stem[-7:]} to {files[-1].stem[-7:]}" if files else ""))
    if st:
        months = months_to_build(a, st)
        if not months:
            nm = next_month(st["last_month"])
            print(f"nothing to add: {nm} ends on {month_bounds(nm)[1]} (UTC)")
        for m in months:
            first, last, _, _ = month_bounds(m)
            ds, xa = day_sources(p, days_between(first, last))
            print(f"would add {m}: Dumpster days present {sum(ds.values())}, Xatu days present {sum(xa.values())} of {len(ds)}")


def main():
    ap = argparse.ArgumentParser(description="Add one month to the public mempool dataset.")
    ap.add_argument("--data-dir", default="data")
    ap.add_argument("--chain-dir", default=None, help="default <data-dir>/raw/chain")
    ap.add_argument("--work-dir", default="D:/pfof_scratch/update_work", help="scratch folder outside Box")
    ap.add_argument("--month", default=None, metavar="YYYY-MM", help="build only this month (default: every complete month after the last one built)")
    ap.add_argument("--through", default=None, metavar="YYYY-MM", help="stop after this month")
    ap.add_argument("--init", action="store_true", help="build the hash index and state.json from data/processed (once)")
    ap.add_argument("--status", action="store_true")
    ap.add_argument("--dry-run", action="store_true", help="run the checks for the month, write nothing")
    ap.add_argument("--no-download", action="store_true", help="do not download Dumpster and Xatu days")
    ap.add_argument("--no-chain", action="store_true", help="do not extend the chain data from the node")
    ap.add_argument("--chain-through-month", default=None, metavar="YYYY-MM", help="stop the chain extraction at the end of this month (default: finalized block)")
    ap.add_argument("--allow-gap-days", action="store_true", help="build the month although some days have neither source")
    ap.add_argument("--refresh-months", type=int, default=3)
    ap.add_argument("--clean-work", action="store_true", help="delete the month's scratch folder when done")
    ap.add_argument("--force", action="store_true")
    a = ap.parse_args()
    p = Paths(a)
    if a.status:
        status(a, p)
    elif a.init:
        init(a, p)
    else:
        run_update(a, p)


if __name__ == "__main__":
    main()
