# Public Ethereum mempool: first sighting and inclusion time per transaction hash

A chain-wide table of every Ethereum transaction that was seen in the public mempool, with the time it was first seen and the time of the block that included it. It does not depend on any project's transaction list: join it by transaction hash to ask, for example, whether a transaction reached the public mempool before it was mined (public) or not (private).

The months covered are the files present in `data/processed/`.

## The data

`data/processed/public_mempool_YYYY-MM.parquet`, one file per month of first sighting, sorted by hash. A hash appears in exactly one file. Only public hashes are included; a hash that was never publicly seen is absent.

| Column | Type | Meaning |
|---|---|---|
| `hash` | string | Transaction hash, lower case, `0x` plus 64 hex characters |
| `first_seen` | datetime (ms, UTC) | Earliest public sighting across the sources below |
| `included_ts` | datetime (ms, UTC) | Timestamp of the block that included the transaction; null if it was not mined in the chain data |

## Sources

| Source | Period | License | Role |
|---|---|---|---|
| Blocknative mempool archive (hourly `.csv.gz`, 46,752 hours) | 2019-11-01 to 2025-03-01 | Not verified; the service is shut down. Do not redistribute the raw files until the terms are checked | Main source before August 2023 |
| [Flashbots Mempool Dumpster](https://mempool-dumpster.flashbots.net) (daily parquet) | 2023-08-08 onward | CC-0 | Main source from August 2023 |
| [ethPandaOps Xatu](https://ethpandaops.io) `mempool_transaction` | 2023-03-03 onward, every day | CC BY 4.0 (attribution required) | Third source on every day; the only source for the days Dumpster lacks |
| Ethereum execution node: block timestamps and transaction hashes, extracted by `scripts/00_extract_chain_blocks.py` | whole chain | chain data | Hash to block number to block timestamp, which gives `included_ts` |

Dumpster has no file for 2024-10-27 to 2024-10-30, 2025-09-19 and 2025-12-02; Xatu fills those days. Xatu published no rows for 2023-07-17 to 2023-07-21, so Blocknative is the only source on those five days.

## The public rule

A hash is public if any of these holds:
- Dumpster has it;
- Xatu has it;
- Blocknative has it with `timepending != 0`. A null `timepending` counts as public. Only `timepending == 0` (included without having been pending) is private.

For the Blocknative part: on 10 random days (October 2023 to November 2024), Blocknative hashes with `timepending > 0` are in Dumpster 99.92% of the time, those with a null value 78.5%, and those with `timepending == 0` 0.69%.

## What "absent" means

A hash that is not in the files was seen by none of the sources that covered its date. Which sources covered a date decides how much that says:

| Period | Sources | Absent means |
|---|---|---|
| 2019-11 to 2023-02 | Blocknative | Private, or missed by Blocknative |
| 2023-03 to 2023-07 | Blocknative and Xatu (Blocknative only on 2023-07-17 to 07-21) | Private, or missed by the sources present |
| 2023-08 | Blocknative, Dumpster (ramping up), Xatu | Dumpster coverage is incomplete this month |
| 2023-09 to 2025-02 | Blocknative, Dumpster, Xatu (Blocknative degrades from 2024-12) | Private |
| 2025-03 onward | Dumpster, Xatu | Private |

## How to rebuild

Run every command from this folder; the scripts use paths relative to it. Scripts 00 to 06 are numbered in run order. `run_all_mempool.py` runs stages 02 to 06 in sequence, checks each stage before starting the next, and can be resumed: finished work is skipped. `python scripts/run_all_mempool.py --status` shows progress and runs nothing.

Inputs to put in place first:
1. Raw Blocknative hours in `data/raw/blocknative/`. They cannot be downloaded again, so keep them safe.
2. Raw Dumpster days: `python scripts/01_download_dumpster_days.py` (paced, one request at a time; never run it in parallel against the host).
3. The chain data: `python scripts/00_extract_chain_blocks.py` (needs an Ethereum execution node; give its address with `--rpc-url` or the environment variable `ERIGON_RPC_URL`, default `http://127.0.0.1:8545`). It keeps only the block number, block timestamp and transaction hash, writes `data/raw/chain/blocks/` and `data/raw/chain/transactions/`, goes up to the latest finalized block (or `--through-month YYYY-MM`), and continues where it stopped if rerun. The full chain is about 26 million blocks and 3.8 billion hashes, roughly 100 to 150 GB (an estimate). Speed on a local node: about 1,000 to 3,000 blocks per second measured on a 2,000-block sample, so a few hours.

Then:

```
python scripts/run_all_mempool.py --single-pass
```

| Stage | Script | What it does |
|---|---|---|
| hourly | `02_local_bn_hourly_dedup.py` | Raw hour to one row per hash (`timepending`, `ts_min`, `ts_max`); malformed rows are dropped and counted, never guessed |
| daily | `03_local_bn_daily_aggregate.py` | Hourly files to one row per hash per day |
| route, merge, verify | `04_local_bn_global_hash_merge.py` | Route days into 257 hash-prefix shards, deduplicate each shard to one row per hash over the whole archive, recompute a sample to check |
| xatu | `05_xatu_mempool_all_days.py` | Every Xatu day to one row per hash per day, then 256 hash shards (paced downloads, resumable) |
| public_* | `06_local_bn_dumpster_combine.py` | Combine the three sources under the public rule, attach the block time, write the monthly files, verify a sample |

`--from-stage` and `--only` select stages. Without `--single-pass` the last step runs in two stages (first-seen file, then block time), which lets the block times be replaced later by rerunning only the second one.

Requirements and limits:
- The Xatu end date is frozen (`--end` in stage xatu). The shard merge cannot be extended by adding days; a new end date means a rebuild into a new folder. The monthly update does not use that merge.
- Scratch space of about 124 GB outside the repository for the Blocknative routing step (`--scratch-dir`), plus space for the last stage. The merge needs about 96 GB of RAM with three parallel processes; use `--merge-procs 1` on a smaller machine.
- The Blocknative stages read about 4.9 TB of raw files and are limited by read bandwidth. The full build took about 30 hours.
- Do not run two writers over the same hours or days.
- A rebuild will not reproduce the delivered files exactly: three raw hours were replaced from older versions of the archive, and Xatu serves the data as it is now.

## Monthly update

After a full build, one command adds every complete month since the last one in `data/processed/`, without rebuilding anything. A month is complete once it has ended (UTC): run on 15 January, it adds everything up to 31 December; run on 2 February, it adds January too. Blocknative ended in 2025-03, so only Dumpster, Xatu and the chain data grow.

```
python scripts/update_month.py --init     # once, after a full build: builds the hash index and data/state.json
python scripts/update_month.py            # whenever you like: adds every complete month after the last one built
```

`--status` shows the state and the months that would be added; `--dry-run` runs every check for them and writes nothing; `--through YYYY-MM` stops after a month; `--month YYYY-MM` builds one month.

A run first downloads the Dumpster and Xatu daily files for all the months to add (scripts 01 and 05, plus the last day before the first month for sightings across the boundary) and extends the chain data from the node up to the latest finalized block (script 00; `--chain-through-month` stops earlier). Then, for each month in order:
1. Checks every day of the month has a Dumpster file or a Xatu file. It lists the days covered by one source only, and stops if a day has neither (`--allow-gap-days` overrides).
2. Combines Dumpster and Xatu under the public rule, drops hashes already published in earlier months (`data/interim/hash_index/`), and attaches the block time.
3. Writes `data/processed/public_mempool_YYYY-MM.parquet`. A hash whose first sighting belongs to an earlier month (a straggler at a day boundary) is merged into that month's file.
4. Fills null `included_ts` in the last 3 months (`--refresh-months`) from the blocks added since the previous run. A hash first seen at the end of a month is usually mined in the next one.
5. Verifies the new file (unique, sorted, `first_seen` inside the month), then updates the index and `data/state.json` last, so an interrupted run can be rerun. One line of statistics per run is appended to `data/update_log.jsonl`.

Needs the node for the chain extension (`--no-chain` if the chain data is already in place); `--no-download` if the days are already in place. Do not run two updates at once.

Limits of the update:
- A hash is matched against the index by 8 bytes of the hash. A false match between two different hashes has a probability of about 1e-3 per month over the whole history and would drop one new hash.
- Block times are looked up from one day before the month starts. This assumes a transaction is not mined more than a day before a sentry first sees it. A hash mined earlier keeps a null `included_ts`.
- The monthly update has been tested on a small synthetic dataset only, not yet on a real month.

## Folder layout

```
data/raw/blocknative/       hourly Blocknative CSV.gz
data/raw/dumpster/          daily Dumpster parquet
data/raw/chain/             block timestamps and transaction hashes from the node (blocks/, transactions/)
data/interim/blocknative_hourly/   one parquet per hour (hash, timepending, ts_min, ts_max)
data/interim/blocknative_daily/    one row per hash per day
data/interim/blocknative_global/   257 shards, one row per hash over the archive
data/interim/xatu_full/            daily/ (hash, first_ts, last_ts) and merged/ (256 shards)
data/interim/public_mempool_work/  working files of the last stage; safe to delete after a build
data/interim/hash_index/           one sorted key per published hash, 257 shards (monthly update)
data/processed/                    the monthly files
data/state.json                    last month built and last chain block
logs/                              every log file the scripts write (update_log.jsonl: one line per monthly update); not versioned
scripts/                           the build; bn_wrong_hour_files.csv lists the 51 wrong-hour files
```

## Known limitations

- **Damaged Blocknative hours.** 54 raw hours fail the gzip checksum and 51 have no clean copy. They are read through a fallback that decodes the stream up to the bad checksum, and invalid rows are dropped. Some stretches cannot be decoded (the largest: 509,977 of 920,836 rows in `20221208_20`). A damaged line that still looks like a valid hash cannot be detected, so a few bogus hashes may remain.
- **Wrong-hour Blocknative files.** In 51 raw files the rows belong to another hour: the file named `_01` holds the rows of hour 10 and the file named `_02` those of hour 20 (December 2020 to February 2024; list in `scripts/bn_wrong_hour_files.csv`). In those hours, and for 300 seconds after them, absence means unknown, not private, wherever Blocknative is the only source (before 2023-03-03). The monthly files do not mark these hours; consumers must apply the list.
- **Rejected transactions.** The hourly step keeps a hash whatever its raw `status` (including `rejected`, `evicted`, `failed`). Whether a hash that was only ever rejected should count as publicly seen is undecided.
- **Xatu.** Any Xatu sighting makes a hash public and can move `first_seen` earlier. Its agreement with Dumpster and Blocknative on first-sighting times has not been measured. From 2025-04-22 Xatu records about 800,000 more hashes a day that are almost never mined, so the share of public hashes with an `included_ts` falls from about 85 to 94% to about 50% a day from that date. Anyone counting public hashes per day should count Xatu-only hashes separately.
- **Spam bursts.** Dumpster itself records days with millions of mostly unmined hashes (for example 2025-06-05, 2025-07-08 to 07-15, 2025-10-14 to 16, 2025-12-20 and 21).
- **Inclusion times.** A transaction mined after the last block in `data/raw/chain/` has a null `included_ts` until a later build covers its block.

## Attribution

Contains data from the Flashbots Mempool Dumpster (CC-0) and from ethPandaOps Xatu (CC BY 4.0, https://ethpandaops.io). Blocknative data is used for the derived first-sighting times only; check the license before any release of raw or row-level Blocknative content.
