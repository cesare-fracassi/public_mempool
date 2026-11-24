# scripts/02_local_bn_hourly_dedup.py
"""
Local (laptop) processing of the raw BN mempool archive, run with explicit
user approval overriding the standing "never process data locally" rule for
this specific task (bandwidth-bound fetch from Box, not EC2-appropriate CPU
work).

For each raw hourly CSV.gz file:
  1. Read hash, detecttime, timepending.
  2. Add ts_min / ts_max per hash (min/max detecttime across all of that
     hash's rows in this file), broadcast onto every row of that hash.
  3. Deduplicate to one row per hash: keep the row with the largest non-null
     timepending; if no row for that hash has a non-null timepending, keep
     the row with the latest detecttime (timepending stays null).
  4. Keep only hash, timepending, ts_min, ts_max.
  5. Save as parquet (same base name as the source file, same naming
     convention as the existing public_mempool_BN/temp folder:
     "<date>_<hour>.csv.parquet"), zstd-compressed, into
     data/interim/blocknative_hourly/.

Resumable: skips any file whose output parquet already exists, so an
interruption (planned or not) loses at most the file in progress.

Each file is processed in a child process with a hard wall-clock timeout
(FILE_TIMEOUT_SECONDS). This exists because Polars' native gzip reader can
hang indefinitely on some files instead of raising an exception (confirmed
directly on 20191114_10.csv.gz via a 60s timeout that never returned) - a
plain try/except around the read can never catch that, and without an
external timeout one bad file freezes the whole batch forever. A file that
times out twice is recorded in timeout_hourly_files_local_dedup.md and
skipped automatically on later runs (not silently - logged every time) so
the batch doesn't re-spend the timeout on a file known to hang; delete its
line there to force a retry once the underlying issue (corrupt data, or a
stuck Box Drive sync) is believed fixed.

Row validity (step 1b, added 2026-11-05): rows with a malformed hash or an impossible detecttime are dropped
before step 2 (see HASH_RE / TS_RE below); per-hour counts go to invalid_rows_hourly_local_dedup.md.

Parsing and recovery rules that the delivered data depend on (all added 2026-11; a rerun without them gives different data):
  * quote_char=None. With polars' default quote handling a field containing a quote character swallows lines and
    the hour silently returns only a fraction of its rows (the first run kept 32.6% of the distinct hashes of 20230110_08).
  * An empty timepending that is the literal text "" (common in 2019 to 2022 hours) is mapped to null before validation
    (_validate). Without this every pending row of such an hour fails TP_RE and is dropped (all 504,419 rows of 20200529_14).
  * Damaged gzip streams: if polars raises, or if its native read leaves fewer than half the rows valid (it returns rows of
    garbage without raising on 9 hours, e.g. 20230313_15), the file is re-read through Python's gzip module, which decodes
    everything up to the bad CRC check (_read_csv_with_fallback); valid rows are kept, invalid rows are dropped and counted
    in invalid_rows_hourly_local_dedup*.md, fallback hours go to recovered_hourly_files_local_dedup*.md.
  * Row validity (HASH_RE, TS_RE, date within one day of the file name, TP_RE) as described above.

Usage:
    python scripts/02_local_bn_hourly_dedup.py
    python scripts/02_local_bn_hourly_dedup.py --force-list hours.txt   # rebuild listed hours even if output exists
"""
import argparse
import gzip
import io
import multiprocessing as mp
import os
import queue
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl

# Windows console defaults to cp1252; a corrupt source file can produce an
# exception message containing bytes cp1252 can't encode, which would
# otherwise crash print() and kill the whole batch over one bad file.
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")

RAW_DIR = Path("data/raw/blocknative")
# The exclusion lists and the log get a suffix _p<i>of<n> when the script runs as --part i/n, so parallel parts never write the same list file.
OUT_DIR = Path("data/interim/blocknative_hourly")
LOG_PATH = Path("scripts/_local_bn_hourly_dedup.log")
CORRUPT_LIST_PATH = RAW_DIR.parent / "corrupt_hourly_files_local_dedup.md"
RECOVERED_LIST_PATH = RAW_DIR.parent / "recovered_hourly_files_local_dedup.md"
TIMEOUT_LIST_PATH = RAW_DIR.parent / "timeout_hourly_files_local_dedup.md"
INVALID_LIST_PATH = RAW_DIR.parent / "invalid_rows_hourly_local_dedup.md"
# Row validity (added 2026-11-05). Some raw hours are damaged inside the gzip stream (checksum failures):
# lines are spliced together or have altered bytes, e.g. hash "0x02b93", detecttime
# "2022-01-12T17:16:359:21.411Z" or "2054-07-20T19:07:23.669Z" in a file for 2022-07-20. A row is kept only if
# its hash is 0x + 64 lowercase hex characters AND its detecttime matches the ISO pattern AND its date is within
# one day of the date in the file name. Invalid rows are dropped BEFORE ts_min/ts_max are computed, so a damaged
# row can never set a valid hash's first or last sighting. This cannot catch a damaged hash that still looks
# like a valid 66-character hash.
HASH_RE = r"^0x[0-9a-f]{64}$"
TS_RE = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?Z$"
TP_RE = r"^\d{1,15}$"
FILE_TIMEOUT_SECONDS = 300
TIMEOUT_STRIKES_BEFORE_SKIP = 2
CSV_KWARGS = dict(
    separator="\t",
    columns=["hash", "detecttime", "timepending"],
    dtypes={"hash": pl.Utf8, "detecttime": pl.Utf8, "timepending": pl.Utf8},
    ignore_errors=True, truncate_ragged_lines=True,
    # quote_char=None (added 2026-11-10): with the default quote handling Polars swallows lines when a field contains a
    # quote character, and silently returns only a fraction of the rows (e.g. 32.6% of the distinct hashes of 20230110_08 and
    # 75.3% of 20220902_19). With quote_char=None the distinct hashes equal the plain-text count (100% in both hours).
    quote_char=None,
)

OUT_DIR.mkdir(parents=True, exist_ok=True)

# Box Drive sync errors are transient (file is fine, just temporarily
# unreachable) and must not be recorded as corrupt - they're retried
# automatically on the next resume since no output parquet gets written.
# Observed wordings vary ("cloud sync provider failed...", "cloud operation
# was not completed before the time-out period expired...", different "os
# error" codes), so match on "cloud" rather than an exact-string/code list.


def is_transient(e: Exception) -> bool:
    return "cloud" in str(e).lower()


def load_known_corrupt() -> set:
    if not CORRUPT_LIST_PATH.exists():
        return set()
    names = set()
    for line in CORRUPT_LIST_PATH.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if line.startswith("- `"):
            names.add(line[3:].split("`", 1)[0])
    return names


def append_corrupt(fname: str, reason: str):
    if fname in load_known_corrupt():
        return
    header_needed = not CORRUPT_LIST_PATH.exists()
    with open(CORRUPT_LIST_PATH, "a", encoding="utf-8") as f:
        if header_needed:
            f.write("# Corrupt Blocknative hourly CSV.gz files\n\n")
            f.write("Found by `scripts/02_local_bn_hourly_dedup.py` (gzip/CRC/decompression errors "
                     "- transient Box-sync network failures are excluded, since those are retried "
                     "automatically on the next resume). These have no corresponding output parquet "
                     "in `data/interim/blocknative_hourly/` and are excluded from the pipeline.\n\n")
        f.write(f"- `{fname}` — {reason}\n")


def remove_from_corrupt(fname: str):
    """A file previously logged as corrupt can later succeed (e.g. the
    CRC-tolerant fallback wasn't implemented yet when it first failed) -
    drop its stale entry so the list only ever reflects files with no
    output parquet."""
    if not CORRUPT_LIST_PATH.exists():
        return
    lines = CORRUPT_LIST_PATH.read_text(encoding="utf-8", errors="replace").splitlines(keepends=True)
    kept = [l for l in lines if not l.strip().startswith(f"- `{fname}`")]
    if len(kept) != len(lines):
        CORRUPT_LIST_PATH.write_text("".join(kept), encoding="utf-8")


def load_known_recovered() -> set:
    if not RECOVERED_LIST_PATH.exists():
        return set()
    names = set()
    for line in RECOVERED_LIST_PATH.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if line.startswith("- `"):
            names.add(line[3:].split("`", 1)[0])
    return names


def append_recovered(fname: str, note: str):
    if fname in load_known_recovered():
        return
    header_needed = not RECOVERED_LIST_PATH.exists()
    with open(RECOVERED_LIST_PATH, "a", encoding="utf-8") as f:
        if header_needed:
            f.write("# Hourly files recovered via CRC-tolerant fallback\n\n")
            f.write("Polars' native gzip read failed on these (CRC mismatch / truncated gzip "
                     "stream). Recovered by decompressing with Python's `gzip` module, which "
                     "returns everything it decoded before the trailing checksum check fails, "
                     "then dropping the final incomplete line before parsing. Output parquet "
                     "for these files may be missing a small number of trailing rows from the "
                     "affected hour (see the row counts logged at the time).\n\n")
        f.write(f"- `{fname}` — {note}\n")


def record_invalid_rows(fname: str, rows_in: int, rows_dropped: int):
    """Keep one line per hour in INVALID_LIST_PATH (replaced on a rebuild, removed if the hour is now clean)."""
    lines = []
    if INVALID_LIST_PATH.exists():
        lines = [l for l in INVALID_LIST_PATH.read_text(encoding="utf-8", errors="replace").splitlines(keepends=True)
                 if not l.strip().startswith(f"- `{fname}`")]
    elif rows_dropped == 0:
        return
    if rows_dropped:
        if not lines:
            lines = ["# Hours where rows were dropped as invalid\n\n",
                     "Written by `scripts/02_local_bn_hourly_dedup.py`. Rows dropped before the per-hash dedup because "
                     "the hash was not 0x + 64 hex characters, or detecttime was not an ISO timestamp within one day "
                     "of the file's date. These come from damaged raw gzip hours (see the HASH_RE / TS_RE note in "
                     "the script). Damaged hashes that still look valid are not caught.\n\n"]
        lines.append(f"- `{fname}` — dropped {rows_dropped:,} of {rows_in:,} rows\n")
    INVALID_LIST_PATH.write_text("".join(lines), encoding="utf-8")


def load_timeout_counts() -> dict:
    if not TIMEOUT_LIST_PATH.exists():
        return {}
    counts = {}
    for line in TIMEOUT_LIST_PATH.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line.startswith("- `"):
            continue
        fname, _, tail = line[3:].partition("`")
        m = re.search(r"count=(\d+)", tail)
        if m:
            counts[fname] = int(m.group(1))
    return counts


def record_timeout(fname: str, counts: dict) -> int:
    """Increment and persist this file's timeout count; returns the new count."""
    counts[fname] = counts.get(fname, 0) + 1
    lines = [
        "# Hourly files whose read timed out\n\n",
        f"Produced by `scripts/02_local_bn_hourly_dedup.py` when a file's read exceeds "
        f"{FILE_TIMEOUT_SECONDS}s and is killed - a genuine stall, not a thrown exception "
        f"(confirmed directly: Polars' native reader can hang indefinitely on some files "
        f"without raising, so a plain try/except never catches it). After "
        f"{TIMEOUT_STRIKES_BEFORE_SKIP} timeouts the file is skipped automatically on later "
        f"runs instead of re-spending the timeout on it every time (still logged, not silently "
        f"dropped). A timeout does not necessarily mean the file is corrupt - it may be a stuck "
        f"Box Drive sync; delete the file's line here to force a retry.\n\n",
    ]
    for fn, c in sorted(counts.items()):
        lines.append(f"- `{fn}` — count={c}, last={time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    TIMEOUT_LIST_PATH.write_text("".join(lines), encoding="utf-8")
    return counts[fname]


def _read_csv_with_fallback(f: Path, force_fallback: bool = False) -> tuple[pl.DataFrame, bool]:
    """Try Polars' native gzip read first. If that fails (e.g. a truncated/
    CRC-mismatched file that Polars' decoder doesn't handle gracefully), fall
    back to Python's gzip module, which tolerates the bad trailing checksum
    and returns everything decoded up to that point; the final (possibly
    incomplete) line is dropped before parsing."""
    if not force_fallback:
        try:
            return pl.read_csv(f, **CSV_KWARGS), False
        except Exception:
            pass

    buf = io.BytesIO()
    with gzip.open(f, "rb") as gz:
        while True:
            try:
                chunk = gz.read(1024 * 1024)
            except Exception:
                break
            if not chunk:
                break
            buf.write(chunk)
    data = buf.getvalue()
    last_nl = data.rfind(b"\n")
    if last_nl == -1:
        raise ValueError("gzip fallback recovered no complete line")
    data = data[:last_nl + 1]
    return pl.read_csv(io.BytesIO(data), **CSV_KWARGS), True


def _validate(f: Path, df: pl.DataFrame):
    """Lowercase hashes, map the text "" in timepending to null, keep only valid rows. Returns (valid_df, rows_read)."""
    # Fix 2026-11-13: in many hours (e.g. 20200529_14, 20210424_02) an empty timepending is the literal two characters "";
    # with quote_char=None that stays text and failed TP_RE, so the whole pending row was dropped as invalid (all 504,419 rows of
    # 20200529_14). The default quote handling used to turn it into null; do the same explicitly.
    df = df.with_columns(pl.col("hash").str.to_lowercase(),
                         pl.when(pl.col("timepending") == '""').then(None).otherwise(pl.col("timepending")).alias("timepending"))
    rows_read = df.height
    file_date = datetime.strptime(f.name[:8], "%Y%m%d")
    allowed_dates = [(file_date + timedelta(days=k)).strftime("%Y-%m-%d") for k in (-1, 0, 1)]
    # timepending is read as text so a garbled value is dropped instead of silently becoming null (= "public").
    # Valid: null, or 1-15 digits (a clean hour contains only nulls and digit strings; negatives are invalid).
    valid = (pl.col("hash").str.contains(HASH_RE)
             & pl.col("detecttime").str.contains(TS_RE)
             & pl.col("detecttime").str.slice(0, 10).is_in(allowed_dates)
             & (pl.col("timepending").is_null() | pl.col("timepending").str.contains(TP_RE))).fill_null(False)
    return df.filter(valid).with_columns(pl.col("timepending").cast(pl.Int64)), rows_read


def process_file(f: Path) -> dict:
    df, recovered = _read_csv_with_fallback(f)
    df, rows_read = _validate(f, df)
    # Fix 2026-11-13: on some damaged hours (20230313_15, 20230301_00, 20211211_20 and 6 more) polars' native read does not
    # raise, it returns rows of garbage, so the CRC-tolerant fallback never ran and the hour came out empty although 400k to
    # 900k valid rows are recoverable. If the native read leaves less than half the rows valid, redo the read through the
    # Python gzip fallback (decodes up to the bad CRC check) and validate again; keep it if it yields more valid rows.
    if not recovered and rows_read > 0 and df.height < 0.5 * rows_read:
        try:
            df2, _ = _read_csv_with_fallback(f, force_fallback=True)
            df2, rows_read2 = _validate(f, df2)
            if df2.height > df.height:
                df, rows_read, recovered = df2, rows_read2, True
        except Exception:
            pass
    rows_dropped = rows_read - df.height

    df = df.with_columns([
        pl.col("detecttime").min().over("hash").alias("ts_min"),
        pl.col("detecttime").max().over("hash").alias("ts_max"),
        pl.col("timepending").is_not_null().alias("_has_tp"),
    ])

    df_sorted = df.sort(
        by=["hash", "_has_tp", "timepending", "detecttime"],
        descending=[False, True, True, True],
        nulls_last=True,
    )
    dedup = df_sorted.unique(subset=["hash"], keep="first", maintain_order=True)
    result = dedup.select(["hash", "timepending", "ts_min", "ts_max"])

    return {"rows_in": rows_read, "rows_out": result.height, "result": result, "recovered": recovered,
            "rows_dropped": rows_dropped}


def _process_file_worker(f_str: str, out_path_str: str, q: "mp.Queue"):
    """Runs in a child process so the parent can enforce a hard wall-clock
    timeout via terminate() - a plain try/except in-process can't catch a
    read that hangs instead of raising. Writes to a .tmp path and renames
    only on success, so a terminate() mid-write never leaves a partial
    parquet file that a later run would mistake for a completed output."""
    try:
        f = Path(f_str)
        out_path = Path(out_path_str)
        stats = process_file(f)
        tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
        stats["result"].write_parquet(tmp_path, compression="zstd")
        os.replace(tmp_path, out_path)
        q.put(("ok", stats["rows_in"], stats["rows_out"], stats["recovered"], stats["rows_dropped"]))
    except Exception as e:
        q.put(("error", str(e)))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--force-list", help="text file with one hour per line (YYYYMMDD_HH or YYYYMMDD_HH.csv.gz); "
                    "these hours are rebuilt even if their output parquet already exists (the old parquet is "
                    "replaced only after the new one is written)")
    ap.add_argument("--part", help="i/n: process only every n-th raw file starting at index i (0-based) of the sorted list, e.g. "
                    "--part 0/6 ... --part 5/6 for six parallel processes. Each part has its own log and exclusion lists.")
    ap.add_argument("--release-raw", action="store_true",
                    help="Windows + Box Drive: after an hour's parquet is written, return its raw .csv.gz to 'online only' (attrib +U -P) so the "
                    "downloaded copy does not pile up on C: (the raw folder is 4.47 TiB). The Box copy is untouched. Also pins the output folder "
                    "(attrib +P -U) so the hourly parquet files stay on local disk for the daily stage.")
    ap.add_argument("--skip-over-mb", type=float, default=None,
                    help="skip raw files larger than this many MB (they are run separately with fewer workers, memory per file is 5-7x its size)")
    ap.add_argument("--only-over-mb", type=float, default=None, help="process only raw files larger than this many MB")
    ap.add_argument("--timeout", type=int, default=None, help="per-file wall-clock timeout in seconds (default 300; raise it when many streams share Box Drive bandwidth)")
    args = ap.parse_args()
    global FILE_TIMEOUT_SECONDS
    if args.timeout:
        FILE_TIMEOUT_SECONDS = args.timeout
    if args.release_raw:
        import subprocess as _sp
        _sp.run(["attrib", "+P", "-U", str(OUT_DIR)], capture_output=True)
    part_i, part_n = 0, 1
    if args.part:
        part_i, part_n = (int(x) for x in args.part.split("/"))
        assert 0 <= part_i < part_n
        global LOG_PATH, CORRUPT_LIST_PATH, RECOVERED_LIST_PATH, TIMEOUT_LIST_PATH, INVALID_LIST_PATH
        sfx = f"_p{part_i}of{part_n}"
        LOG_PATH = LOG_PATH.with_name(LOG_PATH.stem + sfx + LOG_PATH.suffix)
        CORRUPT_LIST_PATH = CORRUPT_LIST_PATH.with_name(CORRUPT_LIST_PATH.stem + sfx + CORRUPT_LIST_PATH.suffix)
        RECOVERED_LIST_PATH = RECOVERED_LIST_PATH.with_name(RECOVERED_LIST_PATH.stem + sfx + RECOVERED_LIST_PATH.suffix)
        TIMEOUT_LIST_PATH = TIMEOUT_LIST_PATH.with_name(TIMEOUT_LIST_PATH.stem + sfx + TIMEOUT_LIST_PATH.suffix)
        INVALID_LIST_PATH = INVALID_LIST_PATH.with_name(INVALID_LIST_PATH.stem + sfx + INVALID_LIST_PATH.suffix)
    force = set()
    if args.force_list:
        for ln in Path(args.force_list).read_text(encoding="utf-8").split():
            force.add(ln.strip().split(".")[0] + ".csv.gz")
    files = sorted(RAW_DIR.glob("*.csv.gz"))[part_i::part_n]
    if args.skip_over_mb is not None:
        files = [f for f in files if f.stat().st_size <= args.skip_over_mb * 1e6]
    if args.only_over_mb is not None:
        files = [f for f in files if f.stat().st_size > args.only_over_mb * 1e6]
    total = len(files)
    done = 0
    skipped = 0
    failed = 0
    timeout_counts = load_timeout_counts()
    t_start = time.time()

    with open(LOG_PATH, "a", encoding="utf-8", errors="replace") as log:
        log.write(f"\n=== Run started {time.strftime('%Y-%m-%d %H:%M:%S')} - {total} files total "
                   f"(per-file timeout {FILE_TIMEOUT_SECONDS}s) ===\n")
        log.flush()

        def maybe_print_progress(i):
            if (done + skipped + failed) % 50 == 0:
                total_elapsed = time.time() - t_start
                progress_msg = (f"--- PROGRESS: {done} done, {skipped} skipped, {failed} failed, "
                                 f"{total - i} remaining, elapsed {total_elapsed/3600:.2f}h ---")
                print(progress_msg, flush=True)
                log.write(progress_msg + "\n")

        for i, f in enumerate(files, 1):
            out_name = f.name.replace(".csv.gz", ".csv.parquet")
            out_path = OUT_DIR / out_name

            if out_path.exists() and f.name not in force:
                skipped += 1
                continue

            if timeout_counts.get(f.name, 0) >= TIMEOUT_STRIKES_BEFORE_SKIP:
                failed += 1
                msg = (f"[{i}/{total}] {f.name}: SKIPPED (timed out "
                       f"{timeout_counts[f.name]}x previously - see {TIMEOUT_LIST_PATH.name})")
                print(msg, flush=True)
                log.write(msg + "\n")
                continue

            t0 = time.time()
            q: mp.Queue = mp.Queue()
            p = mp.Process(target=_process_file_worker, args=(str(f), str(out_path), q))
            p.start()
            p.join(FILE_TIMEOUT_SECONDS)

            if p.is_alive():
                p.terminate()
                p.join(10)
                failed += 1
                n = record_timeout(f.name, timeout_counts)
                msg = (f"[{i}/{total}] {f.name}: FAILED (timeout - read hung past "
                       f"{FILE_TIMEOUT_SECONDS}s, strike {n}/{TIMEOUT_STRIKES_BEFORE_SKIP})")
                print(msg, flush=True)
                log.write(msg + "\n")
                log.flush()
                maybe_print_progress(i)
                continue

            try:
                result = q.get(timeout=5)
            except queue.Empty:
                result = None

            elapsed = time.time() - t0
            if result is None:
                failed += 1
                msg = (f"[{i}/{total}] {f.name}: FAILED (worker exited with no result, "
                       f"exitcode={p.exitcode})")
                print(msg, flush=True)
                log.write(msg + "\n")
            elif result[0] == "ok":
                _, rows_in, rows_out, recovered, rows_dropped = result
                done += 1
                tag = " [RECOVERED-FALLBACK]" if recovered else ""
                if rows_dropped:
                    tag += f" [DROPPED-INVALID {rows_dropped:,}]"
                record_invalid_rows(f.name, rows_in, rows_dropped)
                msg = (f"[{i}/{total}] {f.name}: rows_in={rows_in:,} "
                       f"rows_out={rows_out:,} time={elapsed:.1f}s{tag}")
                print(msg, flush=True)
                log.write(msg + "\n")
                if recovered:
                    append_recovered(f.name, f"rows_in={rows_in:,} rows_out={rows_out:,}")
                remove_from_corrupt(f.name)
                if args.release_raw:      # the parquet is on disk (written through .tmp + os.replace in the worker), so the raw copy can go
                    _sp.run(["attrib", "+U", "-P", str(f)], capture_output=True)
            else:
                _, emsg = result
                failed += 1
                if is_transient(Exception(emsg)):
                    msg = f"[{i}/{total}] {f.name}: FAILED (transient network) - {emsg}"
                else:
                    msg = f"[{i}/{total}] {f.name}: FAILED (corrupt) - {emsg}"
                    append_corrupt(f.name, emsg.splitlines()[0][:200])
                print(msg, flush=True)
                log.write(msg + "\n")

            maybe_print_progress(i)
            log.flush()

    # Reconciliation: every raw file must end up in exactly one of three
    # buckets - has an output parquet, confirmed corrupt (unrecoverable even
    # with the fallback), or still unaccounted (a transient failure this run,
    # guaranteed to be retried on the next resume since no output exists for it).
    out_names = {p.name.replace(".csv.parquet", ".csv.gz") for p in OUT_DIR.glob("*.parquet")}
    corrupt_names = load_known_corrupt()
    all_names = {f.name for f in files}
    missing = sorted(all_names - out_names - corrupt_names)
    summary = (f"\nDONE: {done} processed, {skipped} skipped (already existed), {failed} failed this run\n"
               f"RECONCILIATION: {len(all_names)} total raw files | {len(out_names)} have output parquet | "
               f"{len(corrupt_names)} confirmed corrupt (unrecoverable) | "
               f"{len(missing)} unaccounted (will retry on next run)")
    if missing:
        summary += "\nUnaccounted files (first 20): " + ", ".join(missing[:20])
    print(summary, flush=True)


if __name__ == "__main__":
    main()
