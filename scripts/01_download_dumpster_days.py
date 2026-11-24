# scripts/01_download_dumpster_days.py
"""
Fill gaps in, and extend, the local Flashbots Mempool Dumpster archive
(data/raw/dumpster/YYYY-MM-DD.parquet).

Every day from START (default 2023-08-08, the first local file) through the last
complete UTC day (yesterday) whose file is missing locally is fetched from
    https://mempool-dumpster.flashbots.net/ethereum/mainnet/YYYY-MM/YYYY-MM-DD.parquet
One file at a time, with a pause between files (global rule 21: pace external
hosts; no parallel requests). The upstream file carries many columns including
rawTx and can be several GB, so each file is streamed to a temp folder OUTSIDE Box,
only the four project columns are kept, and the result is written zstd-compressed
into the Box folder via .tmp + os.replace. The upstream row count is checked
against the written row count.

Columns kept (same as the existing local files; see README section 5):
    timestamp, hash, includedAtBlockHeight, inclusionDelayMs

Stops immediately on HTTP 403/429 (a rate-limit or WAF block; do not hammer).
A 404 is recorded as "not published upstream" and skipped. Resumable: existing
files are never touched.

Usage (repo root):
    python scripts/01_download_dumpster_days.py --list          # show what would be fetched, no download
    python scripts/01_download_dumpster_days.py                 # download everything missing
    python scripts/01_download_dumpster_days.py --start 2024-10-25 --end 2024-10-30
"""
import argparse
import datetime as dt
import os
import sys
import time
from pathlib import Path

import pyarrow.parquet as pq
import requests

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")

BASE = "https://mempool-dumpster.flashbots.net/ethereum/mainnet"
OUT_DIR = Path("data/raw/dumpster")
KEEP = ["timestamp", "hash", "includedAtBlockHeight", "inclusionDelayMs"]
HEADERS = {"User-Agent": "academic-research-downloader"}
PAUSE_SECONDS = 3
MAX_TRIES = 2


def missing_days(start: dt.date, end: dt.date):
    have = {p.stem for p in OUT_DIR.glob("*.parquet")}
    d = start
    while d <= end:
        if d.isoformat() not in have:
            yield d
        d += dt.timedelta(days=1)


def fetch_one(day: dt.date, tmp_dir: Path):
    """Returns 'ok', 'missing' (404) or raises on error. Exits the program on 403/429."""
    ds = day.isoformat()
    url = f"{BASE}/{ds[:7]}/{ds}.parquet"
    tmp_dl = tmp_dir / f"{ds}.download"
    out = OUT_DIR / f"{ds}.parquet"
    with requests.get(url, headers=HEADERS, stream=True, timeout=(30, 300)) as r:
        if r.status_code == 404:
            return "missing", 0, 0
        if r.status_code in (403, 429):
            sys.exit(f"HTTP {r.status_code} from the host on {ds}: possible rate-limit/WAF block. Stopping; do not retry soon.")
        r.raise_for_status()
        expected = int(r.headers.get("Content-Length", 0))
        got = 0
        with open(tmp_dl, "wb") as f:
            for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                f.write(chunk)
                got += len(chunk)
    if expected and got != expected:
        tmp_dl.unlink(missing_ok=True)
        raise IOError(f"incomplete download: {got} of {expected} bytes")
    # Explicit file handles: on Windows a lingering ParquetFile handle blocks the unlink below.
    with open(tmp_dl, "rb") as fh:
        pf = pq.ParquetFile(fh)
        n_up = pf.metadata.num_rows
        cols = [c for c in KEEP if c in pf.schema_arrow.names]
        if set(cols) != set(KEEP):
            raise ValueError(f"upstream file lacks columns {set(KEEP) - set(cols)}")
        table = pf.read(columns=KEEP)
    tmp_out = out.with_suffix(".parquet.tmp")
    pq.write_table(table, tmp_out, compression="zstd")
    with open(tmp_out, "rb") as fh:
        n_out = pq.ParquetFile(fh).metadata.num_rows
    if n_out != n_up:
        tmp_out.unlink(missing_ok=True)
        raise ValueError(f"row count mismatch: upstream {n_up}, written {n_out}")
    os.replace(tmp_out, out)
    tmp_dl.unlink(missing_ok=True)
    return "ok", n_out, got


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2023-08-08")
    ap.add_argument("--end", default=None, help="default: yesterday (UTC)")
    ap.add_argument("--tmp-dir", default="D:/pfof_scratch/dumpster_dl", help="outside Box")
    ap.add_argument("--list", action="store_true")
    a = ap.parse_args()
    start = dt.date.fromisoformat(a.start)
    end = dt.date.fromisoformat(a.end) if a.end else dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(days=1)
    todo = list(missing_days(start, end))
    print(f"{len(todo)} missing days between {start} and {end}: "
          f"first {todo[0] if todo else None}, last {todo[-1] if todo else None}", flush=True)
    if a.list:
        print(", ".join(d.isoformat() for d in todo[:40]) + (" ..." if len(todo) > 40 else ""))
        return
    tmp_dir = Path(a.tmp_dir)
    tmp_dir.mkdir(parents=True, exist_ok=True)
    ok = miss = fail = 0
    unavailable, failed = [], []
    t0 = time.time()
    for i, day in enumerate(todo, 1):
        for attempt in range(1, MAX_TRIES + 1):
            try:
                status, n, size = fetch_one(day, tmp_dir)
                break
            except Exception as e:
                print(f"[{i}/{len(todo)}] {day}: error (try {attempt}/{MAX_TRIES}): {e}", flush=True)
                status = "error"
                time.sleep(10)
        if status == "ok":
            ok += 1
            print(f"[{i}/{len(todo)}] {day}: {n:,} rows, downloaded {size/1e6:,.0f} MB, "
                  f"elapsed {(time.time()-t0)/60:.1f} min", flush=True)
        elif status == "missing":
            miss += 1
            unavailable.append(day.isoformat())
            print(f"[{i}/{len(todo)}] {day}: 404 not published upstream", flush=True)
        else:
            fail += 1
            failed.append(day.isoformat())
        time.sleep(PAUSE_SECONDS)
    print(f"\nDONE: {ok} downloaded, {miss} not published upstream {unavailable}, "
          f"{fail} failed {failed}, {(time.time()-t0)/60:.1f} min", flush=True)


if __name__ == "__main__":
    main()
