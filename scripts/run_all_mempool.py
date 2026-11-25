# scripts/run_all_mempool.py
"""
Single entry point for the local Blocknative (BN) mempool construction, Phase 1d.

Runs the whole chain from the raw hourly archive to the final one-row-per-hash
dataset, with a check after every stage. Each underlying script is resumable, so
this driver is too: rerunning it skips finished work and picks up where it
stopped. Stages:

  hourly   raw CSV.gz -> hourly parquet (hash, timepending, ts_min, ts_max)
           scripts/02_local_bn_hourly_dedup.py
           in : data/raw/blocknative/*.csv.gz
           out: data/interim/blocknative_hourly/
  daily    hourly -> one row per hash per day
           scripts/03_local_bn_daily_aggregate.py
           out: data/interim/blocknative_daily/
  route    daily rows -> 257 hash shards in a scratch folder outside Box
           scripts/04_local_bn_global_hash_merge.py --phase 1
           out: <scratch-dir>/<shard>/b*.parquet   (~124 GB)
  merge    each shard deduplicated to one row per hash (parallel processes)
           scripts/04_local_bn_global_hash_merge.py --phase 2
           out: data/interim/blocknative_global/shard_XX.parquet
  verify   sampled recomputation from all daily files + uniqueness + totals
           scripts/04_local_bn_global_hash_merge.py --verify
  xatu     Xatu mempool_transaction, every day from 2023-03-03 to a FROZEN end date (--xatu-end), reduced to one
           row per hash (daily files, then a 256-shard merge) -> <xatu-root>/daily and <xatu-root>/merged
           scripts/05_xatu_mempool_all_days.py. Paced downloads, resumable; skipped when the data is complete and
           stage 2 already logged VERIFY PASSED. The end date is frozen on purpose: the merge is not safe to
           extend incrementally, so a new end date means rebuilding the merge.
  public_route    Phase 1e: Dumpster days + the full Xatu shards -> hash shards (scratch, outside Box)
  public_combine  Phase 1e stage A: BN + Dumpster + Xatu -> one row per hash, earliest sighting
           out: data/interim/public_mempool_work/first_seen/first_seen_YYYY-MM.parquet (hash, first_seen)
  public_attach   Phase 1e stage B: Erigon transactions routed once; block time attached to every
           monthly file across ALL blocks -> data/processed/public_mempool_YYYY-MM.parquet
           (hash, first_seen, included_ts; one file per month of first_seen; wallet-agnostic)
  public_verify   Phase 1e: sampled recomputation + uniqueness + totals
           (all of these: scripts/06_local_bn_dumpster_combine.py; see its docstring)

Usage (from anywhere; the driver changes into the repo root itself):
    python scripts/run_all_mempool.py                    # all stages
    python scripts/run_all_mempool.py --status           # read-only progress report, runs nothing
    python scripts/run_all_mempool.py --from-stage merge # start at a later stage
    python scripts/run_all_mempool.py --only verify      # one stage only
    python scripts/run_all_mempool.py --scratch-dir E:/scratch/bn_shards --merge-procs 2

Requirements and limits:
  * Runs on the laptop/desktop with Box Drive (an approved override of the "never
    process data locally" rule for this task: the hourly stage is bound by the
    Box fetch). Needs ~124 GB free on the scratch drive and a machine with
    roughly 96 GB RAM for three parallel merge processes (use --merge-procs 1
    on a smaller machine).
  * Not everything in the delivered dataset is reproducible from this script
    alone. Three raw hours were replaced from Box version history, 54 raw hours
    fail the gzip checksum (51 with no clean copy), and part of the hourly stage
    was originally run newest-to-oldest on a second machine. See README section
    4, Phase 1d, "Replication caveat".
  * Not covered: Phase 1a (the old EC2 production pipeline of the MetaMask project, not part of this build; it
    read the raw Blocknative files with default quote handling and loses hashes). The Dumpster and Xatu combine
    (the stages public_route, public_combine, public_attach, public_verify) is written and part of this chain.
  * Do not run two writers over the same hours (Box creates conflict copies).

The driver stops at the first failed check and prints PIPELINE FAILED.
"""
import argparse
import datetime as dt
import glob
import json
import math
import os
import re
import subprocess
import sys
import time
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")

REPO = Path(__file__).resolve().parent.parent
PY = sys.executable
SCRIPTS = REPO / "scripts"
LOGS = REPO / "logs"  # every log file goes here (not versioned)
RAW_DIR = REPO / "data/raw/blocknative"
HOURLY_DIR = REPO / "data/interim/blocknative_hourly"
DAILY_DIR = REPO / "data/interim/blocknative_daily"
GLOBAL_DIR = REPO / "data/interim/blocknative_global"
N_SHARDS = 257  # 256 hex prefixes + "zz" (malformed hashes)
STAGES = ["hourly", "daily", "route", "merge", "verify", "xatu",
          "public_route", "public_combine", "public_attach", "public_verify"]
XATU_FIRST_DAY = dt.date(2023, 3, 3)                       # first Xatu day (FIRST_DAY in 05_xatu_mempool_all_days.py)
PUBLIC_DIR = REPO / "data/interim/public_mempool_work"   # working files: _parts, _stats, first_seen/
FINAL_DIR = REPO / "data/processed"                       # finished monthly files (the public data)
DUMPSTER_DIR = REPO / "data/raw/dumpster"

results = []


def check(name, ok, detail=""):
    results.append(bool(ok))
    print(f"CHECK {'PASS' if ok else 'FAIL'}: {name} {detail}".rstrip(), flush=True)
    return bool(ok)


def fail(msg):
    print(f"PIPELINE FAILED: {msg}", flush=True)
    sys.exit(1)


def run_script(args, log_name=None):
    """Run a pipeline script from the repo root, streaming its output."""
    cmd = [PY, *args]
    print(f"\n$ {' '.join(cmd)}", flush=True)
    r = subprocess.run(cmd, cwd=REPO)
    if r.returncode != 0:
        fail(f"{' '.join(args)} exited with code {r.returncode}")


def raw_files():
    return sorted(p.name for p in RAW_DIR.glob("*.csv.gz"))


def hourly_days():
    return {p.name.split("_")[0] for p in HOURLY_DIR.glob("*.csv.parquet")}


def expected_batches(n_days, batch_days):
    return math.ceil(n_days / batch_days) if n_days else 0


def status(scratch, batch_days, xatu_root=None):
    raw = len(raw_files())
    hourly = len(list(HOURLY_DIR.glob("*.csv.parquet")))
    days_h = len(hourly_days())
    daily = len(list(DAILY_DIR.glob("*.parquet")))
    done = glob.glob(str(scratch / "_done" / "b*.json"))
    shards_out = len(list(GLOBAL_DIR.glob("shard_*.parquet")))
    print("STATUS (read-only):")
    print(f"  hourly : {hourly:,} of {raw:,} raw files have an hourly parquet")
    print(f"  daily  : {daily:,} daily files for {days_h:,} days present in the hourly folder")
    print(f"  route  : {len(done)} of {expected_batches(daily, batch_days)} batches routed (scratch {scratch})")
    print(f"  merge  : {shards_out} of {N_SHARDS} shard files in {GLOBAL_DIR.relative_to(REPO)}")
    if xatu_root is not None:
        xd = Path(xatu_root) / "daily"
        n_x = len(list(xd.glob("????-??-??.parquet"))) if xd.exists() else 0
        n_s = len(list((Path(xatu_root) / "merged").glob("shard_*.parquet"))) if (Path(xatu_root) / "merged").exists() else 0
        print(f"  xatu   : {n_x:,} daily files, {n_s} of 256 merged shard files in {xatu_root}")


def stage_hourly(a):
    run_script(["scripts/02_local_bn_hourly_dedup.py"])
    raw = set(raw_files())
    out = {p.name.replace(".csv.parquet", ".csv.gz") for p in HOURLY_DIR.glob("*.csv.parquet")}
    missing = sorted(raw - out)
    ok = check("every raw hour has an hourly parquet", not missing,
               f"({len(out):,} outputs for {len(raw):,} raw files; {len(missing)} missing"
               + (f", first: {', '.join(missing[:5])}" if missing else "") + ")")
    check("no leftover .tmp files in the hourly folder", not list(HOURLY_DIR.glob("*.tmp")))
    check("no conflict-named hourly files (Box rename)", not [p for p in HOURLY_DIR.glob("*.parquet") if "(" in p.name])
    if not ok and not a.allow_incomplete:
        fail("hourly stage incomplete (see lists beside the raw folder: corrupt/timeout/recovered). "
             "Fix or rerun, or pass --allow-incomplete to continue with the days that are complete.")


def stage_daily(a):
    run_script(["scripts/03_local_bn_daily_aggregate.py"])
    days = hourly_days()
    built = {p.stem for p in DAILY_DIR.glob("*.parquet")}
    missing = sorted(days - built)
    ok = check("every day with hourly files has a daily file", not missing,
               f"({len(built):,} daily files for {len(days):,} days; {len(missing)} not built"
               + (f", first: {', '.join(missing[:5])}" if missing else "") + ")")
    check("no leftover .tmp files in the daily folder", not list(DAILY_DIR.glob("*.tmp")))
    if not ok and not a.allow_incomplete:
        fail("daily stage incomplete: a day is built only when all 24 of its hours exist. "
             "Pass --allow-incomplete to continue without those days.")


def stage_route(a):
    run_script(["scripts/04_local_bn_global_hash_merge.py", "--phase", "1",
                "--scratch-dir", str(a.scratch_dir), "--batch-days", str(a.batch_days)])
    n_days = len(list(DAILY_DIR.glob("*.parquet")))
    done = [json.load(open(f)) for f in glob.glob(str(a.scratch_dir / "_done" / "b*.json"))]
    check("every batch has a completion record", len(done) == expected_batches(n_days, a.batch_days),
          f"({len(done)} records for {n_days:,} daily files)")
    check("no leftover .tmp files in scratch", not glob.glob(str(a.scratch_dir / "*" / "*.tmp")))
    total = sum(d["rows"] for d in done)
    print(f"  routed {total:,} day-rows in total", flush=True)


def stage_merge(a):
    n = max(1, a.merge_procs)
    size = math.ceil(N_SHARDS / n)
    ranges = [(i * size, min((i + 1) * size, N_SHARDS) - 1) for i in range(n) if i * size < N_SHARDS]
    procs = []
    for lo, hi in ranges:
        LOGS.mkdir(parents=True, exist_ok=True)
        logp = LOGS / f"run_all_merge_{lo}_{hi}.log"
        log = open(logp, "w", encoding="utf-8")
        p = subprocess.Popen([PY, "scripts/04_local_bn_global_hash_merge.py", "--phase", "2",
                              "--scratch-dir", str(a.scratch_dir),
                              "--shard-start", str(lo), "--shard-end", str(hi)],
                             cwd=REPO, stdout=log, stderr=subprocess.STDOUT)
        procs.append((lo, hi, p, log, logp))
    print(f"\nmerge started: {len(procs)} process(es) on shard index ranges {ranges}", flush=True)
    t0 = time.time()
    while any(p.poll() is None for _, _, p, _, _ in procs):
        time.sleep(60)
        n_out = len(list(GLOBAL_DIR.glob("shard_*.parquet")))
        print(f"  merge progress: {n_out} shard files written, {(time.time() - t0) / 60:.0f} min", flush=True)
    for *_, log, _ in procs:
        log.close()
    rows_in = rows_out = 0
    for lo, hi, p, _, logp in procs:
        txt = logp.read_text(encoding="utf-8", errors="replace")
        m = re.search(r"PHASE 2 DONE .*?: (\d+) built, (\d+) already existed, ([\d,]+) day-rows -> ([\d,]+) unique hashes", txt)
        check(f"merge process {lo}-{hi} finished cleanly", p.returncode == 0 and bool(m) and "Traceback" not in txt,
              f"(exit {p.returncode}, log {logp.name})")
        if m:
            rows_in += int(m.group(3).replace(",", ""))
            rows_out += int(m.group(4).replace(",", ""))
    shard_dirs = [d for d in os.listdir(a.scratch_dir) if d != "_done" and (a.scratch_dir / d).is_dir()]
    outs = list(GLOBAL_DIR.glob("shard_*.parquet"))
    check("one output file per non-empty scratch shard", len(outs) == len(shard_dirs),
          f"({len(outs)} outputs vs {len(shard_dirs)} shards)")
    check("no leftover .tmp files in the output folder", not list(GLOBAL_DIR.glob("*.tmp")))
    done = [json.load(open(f)) for f in glob.glob(str(a.scratch_dir / "_done" / "b*.json"))]
    p1_rows = sum(d["rows"] for d in done)
    if rows_in:  # zero when every shard already existed from an earlier run
        check("merge consumed exactly the rows routing wrote", rows_in == p1_rows, f"({rows_in:,} vs {p1_rows:,})")
        print(f"RESULT: {rows_in:,} day-rows -> {rows_out:,} unique hashes "
              f"({rows_out / rows_in:.1%} kept; {rows_in - rows_out:,} repeated appearances folded)", flush=True)


def stage_verify(a):
    cmd = [PY, "scripts/04_local_bn_global_hash_merge.py", "--verify", str(a.verify_sample)]
    print(f"\n$ {' '.join(cmd)}", flush=True)
    v = subprocess.run(cmd, cwd=REPO, capture_output=True, text=True)
    out = v.stdout + v.stderr
    print("\n".join(out.strip().splitlines()[-6:]) if out.strip() else "(no output)", flush=True)
    m = re.search(r"VERIFY: (\d+) sampled; truth rows (\d+), output rows (\d+); mismatches: (\d+)", out)
    check("sampled hashes match a direct recomputation from all days", bool(m) and m.group(4) == "0",
          f"({m.group(0) if m else 'no result line'})")
    m2 = re.search(r"VERIFY TOTALS: ([\d,]+) rows across (\d+) shard files; shards with duplicate hashes: (\d+)", out)
    check("no duplicate hashes inside any shard", bool(m2) and m2.group(3) == "0",
          f"({m2.group(0) if m2 else 'no totals line'})")
    if m2:
        print(f"  total unique hashes: {m2.group(1)} in {m2.group(2)} shard files", flush=True)


def xatu_merge_complete(root: Path):
    m = root / "merged"
    return (len(list(m.glob("shard_*.parquet"))) == 256 and len(list(m.glob("_stats_*.json"))) == 256
            and not list(m.glob("*.tmp")))


def stage_xatu(a):
    """Build (or confirm) the full Xatu per-hash set: daily files for every day up to the frozen end date,
    then the 256-shard merge with its own verification."""
    if a.xatu_source != "full":
        print("xatu stage skipped: --xatu-source gap uses the six gap-day file only", flush=True)
        return
    root = Path(a.xatu_root)
    end = dt.date.fromisoformat(a.xatu_end)
    n_days = (end - XATU_FIRST_DAY).days + 1
    daily = root / "daily"

    def n_daily():
        return len(list(daily.glob("????-??-??.parquet"))) if daily.exists() else 0

    if n_daily() < n_days:
        run_script(["scripts/05_xatu_mempool_all_days.py", "--out-root", str(root), "--end", a.xatu_end])
    check("one Xatu daily file per day, first day to the frozen end date", n_daily() == n_days,
          f"({n_daily():,} files for {n_days:,} days {XATU_FIRST_DAY}..{end}; a day Xatu has not published stays missing)")
    check("no leftover .tmp files in the Xatu daily folder", not (daily.exists() and list(daily.glob("*.tmp"))))
    check("no STOP file (no worker was blocked)", not (root / "STOP").exists())
    log = LOGS / "stage2.log"
    if not xatu_merge_complete(root):
        log.parent.mkdir(parents=True, exist_ok=True)
        cmd = [PY, "scripts/05_xatu_mempool_all_days.py", "--out-root", str(root), "--merge", "--verify", str(a.verify_sample)]
        print(f"\n$ {' '.join(cmd)}  (output also appended to {log})", flush=True)
        with open(log, "a", encoding="utf-8") as fh:
            r = subprocess.run(cmd, cwd=REPO, stdout=fh, stderr=subprocess.STDOUT)
        print("\n".join(log.read_text(encoding="utf-8", errors="replace").strip().splitlines()[-6:]), flush=True)
        if r.returncode != 0:
            fail(f"Xatu merge exited with code {r.returncode}")
    check("Xatu merge complete: 256 shards, 256 stats files, no .tmp", xatu_merge_complete(root))
    txt = log.read_text(encoding="utf-8", errors="replace") if log.exists() else ""
    check("Xatu merge logged VERIFY PASSED", "VERIFY PASSED" in txt and "VERIFY FAILED" not in txt.split("VERIFY PASSED")[-1],
          f"(log {log})")


def _combine_script_args(a, *extra):
    blocks_args = ["--blocks-dir", str(a.blocks_dir)] if a.blocks_dir else []
    return ["scripts/06_local_bn_dumpster_combine.py", "--scratch-dir", str(a.public_scratch_dir),
            "--erigon-dir", str(a.erigon_dir), *blocks_args,
            "--xatu-source", a.xatu_source, "--xatu-dir", str(Path(a.xatu_root) / "merged"), *extra]


def _run_shard_phase(a, phase, done_dir, done_word):
    """Run `06_local_bn_dumpster_combine.py --phase <phase>` in parallel over shard ranges."""
    n = max(1, a.merge_procs)
    size = math.ceil(N_SHARDS / n)
    ranges = [(i * size, min((i + 1) * size, N_SHARDS) - 1) for i in range(n) if i * size < N_SHARDS]
    procs = []
    for lo, hi in ranges:
        LOGS.mkdir(parents=True, exist_ok=True)
        logp = LOGS / f"run_all_{phase}_{lo}_{hi}.log"
        log = open(logp, "w", encoding="utf-8")
        p = subprocess.Popen([PY, *_combine_script_args(a, "--phase", phase, "--shard-start", str(lo), "--shard-end", str(hi))],
                             cwd=REPO, stdout=log, stderr=subprocess.STDOUT)
        procs.append((lo, hi, p, log, logp))
    print(f"\n{phase} started: {len(procs)} process(es) on shard index ranges {ranges}", flush=True)
    t0 = time.time()
    while any(p.poll() is None for _, _, p, _, _ in procs):
        time.sleep(60)
        print(f"  {phase} progress: {len(list((PUBLIC_DIR / done_dir).glob('shard_*.json')))} shards done, "
              f"{(time.time() - t0) / 60:.0f} min", flush=True)
    for *_, log, _ in procs:
        log.close()
    for lo, hi, p, _, logp in procs:
        txt = logp.read_text(encoding="utf-8", errors="replace")
        check(f"{phase} process {lo}-{hi} finished cleanly",
              p.returncode == 0 and done_word in txt and "Traceback" not in txt,
              f"(exit {p.returncode}, log {logp.name})")
    n_done = len(list((PUBLIC_DIR / done_dir).glob("shard_*.json")))
    check(f"every hash shard finished {phase}", n_done == N_SHARDS, f"({n_done} of {N_SHARDS})")
    return n_done == N_SHARDS


def stage_public_route(a):
    """Stage A inputs: Dumpster days and the Xatu file into hash shards."""
    run_script(_combine_script_args(a, "--phase", "route"))
    n_days = len(list(DUMPSTER_DIR.glob("*.parquet")))
    done = glob.glob(str(a.public_scratch_dir / "_done" / "d*.json"))
    check("every Dumpster batch has a completion record", len(done) == expected_batches(n_days, 30),
          f"({len(done)} records for {n_days:,} Dumpster day files)")
    xdone = a.public_scratch_dir / "_done" / "x0000.json"
    check("Xatu routed", xdone.exists())
    if a.xatu_source == "full" and xdone.exists():
        info = json.loads(xdone.read_text())
        n_marks = len(glob.glob(str(a.public_scratch_dir / "_done" / "xs_*.json")))
        check("every one of the 256 Xatu shards was routed from the full set",
              info.get("source") == "full" and n_marks == 256 and info.get("shards") == 256,
              f"(source {info.get('source')}, {n_marks} shard records, {info.get('rows', 0):,} rows)")
    check("no leftover .tmp files in scratch", not glob.glob(str(a.public_scratch_dir / "*" / "*.tmp")))


def stage_public_combine(a):
    """Stage A: one row per hash with the earliest sighting across BN, Dumpster, Xatu; monthly files."""
    if _run_shard_phase(a, "combine", "_stats_fs", "COMBINE DONE"):
        run_script(_combine_script_args(a, "--phase", "assemble_fs"))
        check("no leftover .tmp files in the output folder", not glob.glob(str(PUBLIC_DIR / "**" / "*.tmp"), recursive=True))
        check("first_seen monthly files exist", bool(list((PUBLIC_DIR / "first_seen").glob("first_seen_*.parquet"))))


def stage_public_attach(a):
    """Stage B: route Erigon transactions once, then attach the block time to every monthly file."""
    run_script(_combine_script_args(a, "--phase", "route_tx"))
    n_tx = len(list((a.erigon_dir / "transactions").glob("transactions_*.parquet")))
    done_t = glob.glob(str(a.public_scratch_dir / "_done" / "t*.json"))
    check("every Erigon transactions batch has a completion record", len(done_t) == expected_batches(n_tx, 40),
          f"({len(done_t)} records for {n_tx:,} transactions files)")
    if _run_shard_phase(a, "attach", "_stats", "ATTACH DONE"):
        run_script(_combine_script_args(a, "--phase", "assemble"))
        check("no leftover .tmp files in the output folder", not glob.glob(str(PUBLIC_DIR / "**" / "*.tmp"), recursive=True))
        check("monthly files with block time exist", bool(list(FINAL_DIR.glob("public_mempool_*.parquet"))))


def stage_public_single_pass(a):
    """Alternative to public_combine + public_attach: no first_seen intermediate. Needs the Dumpster,
    Xatu, BN and Erigon inputs all reachable from this machine."""
    run_script(_combine_script_args(a, "--phase", "route_tx"))
    n_tx = len(list((a.erigon_dir / "transactions").glob("transactions_*.parquet")))
    done_t = glob.glob(str(a.public_scratch_dir / "_done" / "t*.json"))
    check("every Erigon transactions batch has a completion record", len(done_t) == expected_batches(n_tx, 40),
          f"({len(done_t)} records for {n_tx:,} transactions files)")
    if _run_shard_phase(a, "single_pass", "_stats", "SINGLE_PASS DONE"):
        run_script(_combine_script_args(a, "--phase", "assemble"))
        check("no leftover .tmp files in the output folder", not glob.glob(str(PUBLIC_DIR / "**" / "*.tmp"), recursive=True))
        check("monthly files with block time exist", bool(list(FINAL_DIR.glob("public_mempool_*.parquet"))))


def stage_public_verify(a):
    cmd = [PY, *_combine_script_args(a, "--verify", str(a.verify_sample))]
    print(f"\n$ {' '.join(cmd)}", flush=True)
    v = subprocess.run(cmd, cwd=REPO, capture_output=True, text=True)
    out = v.stdout + v.stderr
    print("\n".join(out.strip().splitlines()[-6:]) if out.strip() else "(no output)", flush=True)
    m = re.search(r"VERIFY: (\d+) sampled; expected public rows (\d+), output rows (\d+); mismatches: (\d+)", out)
    check("sampled hashes match a recomputation from BN shards, Xatu and all Dumpster days",
          bool(m) and m.group(4) == "0", f"({m.group(0) if m else 'no result line'})")
    m2 = re.search(r"VERIFY TOTALS: ([\d,]+) rows across (\d+) monthly files; months with duplicate hashes: (\d+); rows per _stats: ([\d,]+)", out)
    m3 = re.search(r"VERIFY INCLUDED_TS: (\d+) sampled hashes with a Dumpster-known block; mismatches vs Erigon-derived included_ts: (\d+)", out)
    check("included_ts matches Dumpster's own block time for sampled hashes", bool(m3) and m3.group(2) == "0",
          f"({m3.group(0) if m3 else 'no included_ts line'})")
    m4 = re.search(r"VERIFY XATU_INCLUDED_TS: (\d+) sampled Xatu hashes mined in the window; mismatches vs the Xatu intermediate: (\d+)", out)
    check("included_ts matches the Xatu intermediate for sampled gap-day hashes", bool(m4) and m4.group(2) == "0",
          f"({m4.group(0) if m4 else 'no Xatu included_ts line'})")
    check("no duplicate hashes inside any monthly file", bool(m2) and m2.group(3) == "0",
          f"({m2.group(0) if m2 else 'no totals line'})")
    if m2:
        check("file row total equals the per-shard stats total", m2.group(1) == m2.group(4), f"({m2.group(1)} vs {m2.group(4)})")


def main():
    ap = argparse.ArgumentParser(description="Run the whole local BN mempool construction (Phase 1d) and the public mempool file (Phase 1e).")
    ap.add_argument("--public-scratch-dir", default="D:/pfof_scratch/public_mempool_shards", type=Path,
                    help="Phase 1e shard scratch folder (Dumpster + Erigon transactions), outside Box")
    ap.add_argument("--chain-dir", "--erigon-dir", dest="erigon_dir", default=Path("data/raw/chain"), type=Path,
                    help="folder holding transactions/ and blocks/, written by 00_extract_chain_blocks.py")
    ap.add_argument("--blocks-dir", default=None, type=Path,
                    help="folder with blocks_*.parquet carrying block_timestamp; default <chain-dir>/blocks")
    ap.add_argument("--xatu-root", default="data/interim/xatu_full", type=Path,
                    help="folder holding the Xatu daily/ and merged/ outputs (default data/interim/xatu_full)")
    ap.add_argument("--xatu-end", default="2026-11-16",
                    help="last Xatu day, frozen. Do not move it without rebuilding the Xatu merge (see the xatu stage)")
    ap.add_argument("--xatu-source", choices=["full", "gap"], default="full",
                    help="full: every Xatu day (default); gap: only the six Dumpster gap days (reproduces the earlier build)")
    ap.add_argument("--scratch-dir", default="D:/pfof_scratch/bn_shards", type=Path,
                    help="shard scratch folder, outside Box, ~124 GB")
    ap.add_argument("--batch-days", type=int, default=12)
    ap.add_argument("--merge-procs", type=int, default=3, help="parallel merge processes (3 needs ~96 GB RAM)")
    ap.add_argument("--verify-sample", type=int, default=300)
    ap.add_argument("--from-stage", choices=STAGES, default="hourly")
    ap.add_argument("--only", choices=STAGES, default=None)
    ap.add_argument("--single-pass", action="store_true",
                    help="Phase 1e in one pass per shard (no first_seen intermediate) instead of stages A and B")
    ap.add_argument("--allow-incomplete", action="store_true",
                    help="continue even if some hours or days are missing (they stay out of the result)")
    ap.add_argument("--status", action="store_true", help="print progress per stage and exit")
    a = ap.parse_args()

    os.chdir(REPO)
    if a.status:
        status(a.scratch_dir, a.batch_days, a.xatu_root)
        return
    stages = [a.only] if a.only else STAGES[STAGES.index(a.from_stage):]
    if a.single_pass and not a.only:
        # one pass per shard, no first_seen intermediate: replaces the two-stage pair
        stages = [s for s in stages if s not in ("public_combine", "public_attach")]
        if "public_verify" in stages:
            stages.insert(stages.index("public_verify"), "public_single_pass")
        else:
            stages.append("public_single_pass")
    if not RAW_DIR.exists() and "hourly" in stages:
        fail(f"raw folder not found: {RAW_DIR}")
    t0 = time.time()
    print(f"run_all_mempool started {time.strftime('%Y-%m-%d %H:%M:%S')}; stages: {', '.join(stages)}", flush=True)
    funcs = {"hourly": stage_hourly, "daily": stage_daily, "route": stage_route,
             "merge": stage_merge, "verify": stage_verify, "xatu": stage_xatu,
             "public_route": stage_public_route, "public_combine": stage_public_combine,
             "public_attach": stage_public_attach, "public_single_pass": stage_public_single_pass,
             "public_verify": stage_public_verify}
    for s in stages:
        print(f"\n=== stage: {s} ===", flush=True)
        funcs[s](a)
        if not all(results):
            fail(f"checks failed in stage '{s}'; later stages NOT started")
    print(f"\nPIPELINE {'PASSED' if all(results) else 'HAS FAILED CHECKS'}: "
          f"{sum(results)}/{len(results)} checks passed, {(time.time() - t0) / 60:.0f} min total", flush=True)


if __name__ == "__main__":
    main()
