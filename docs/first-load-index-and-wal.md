# First load: index deferral and WAL settings

Bead `gm-discogs-sql-loader-ymm9`, 2026-11-04. A decision, with measurements. No production default or
database-schema change follows from it.

## Decision

- **Do not defer secondary indexes during the first load, on this evidence.** The runs show about a quarter less
  WAL and no time difference larger than the spread between identical runs. Every row below is a single run
  (n=1), so "no time gain" means *not shown*, not *shown absent*.
- **Do not change WAL or checkpoint settings** (`max_wal_size`, `checkpoint_timeout`, `wal_compression`).
- **`full_page_writes=off` and `synchronous_commit=off` stay rejected**: they trade crash safety for at most a
  few percent here, and the loader acks a delivery after commit.
- The 7qar audit found the first load CPU-bound in the schema's generated `role_category` column (its item 1),
  which no index or WAL setting touches. This run collected no CPU data and does not re-check that finding.

## Method

`just benchmark-first-load` (`scripts/benchmark-first-load.{sh,py}`): 60 000 synthetic releases (the
`benchmark-write-path` generator), batch 500, two workers, through the real batch writer on the repo's pinned
`postgres:18-alpine`, a fresh container per run with `--rm`, on a random loopback port. 60 000 releases is a
488 MB database, kept under the 1 GB local cap. `--defer-indexes` drops all 94 secondary indexes (the pinned database-schema 0.6.1 declared 94; main now pins 0.6.3, which has 98, and the table below was measured before that bump) in `public`
and `graph` (primary keys and unique indexes stay, because `ON CONFLICT` needs them), loads, then recreates them
(`maintenance_work_mem` 256 MB, two parallel workers). `BENCHMARK_PG_ARGS` passes server settings. Each row is
one run (n=1); this machine varies by about 10% between identical runs and up to about 40% for some cells in
the `pf94` sweep (see `write-path-benchmark.md`).

| Variant | Load s | Drop + rebuild s | Total s | WAL MB | DB MB |
| --- | ---: | ---: | ---: | ---: | ---: |
| Baseline | 130.4 | 0 | 130.4 | 969 | 488 |
| Indexes deferred (94) | 137.8 | 6.1 | 143.9 | 727 | 461 |
| `max_wal_size=8GB`, `checkpoint_timeout=30min` | 140.2 | 0 | 140.2 | 908 | 488 |
| `wal_compression=lz4` | 139.5 | 0 | 139.5 | 936 | 488 |
| `full_page_writes=off` (unsafe bound) | 124.7 | 0 | 124.7 | 909 | 490 |
| `shared_buffers=16MB`, baseline | 132.5 | 0 | 132.5 | 962 | 489 |
| `shared_buffers=16MB`, indexes deferred | 121.5 | 5.8 | 127.3 | 803 | 460 |
| Larger WAL settings + indexes deferred | 118.2 | 5.7 | 123.9 | 694 | 461 |

Records per second by 10 000-release chunk. Baseline: 452, 445, 444, 471, 466, 537. Deferred: 448, 444, 328,
439, 520, 557. The baseline shows no slowdown across the six chunks, though its last chunk is faster than its
first. The deferred run's third chunk (328) is about 26% below its neighbours; one run cannot say whether
that is noise or an effect. Neither run shows the rate falling as the database grows to 488 MB.

## Reading it

- Every total is between 124 s and 144 s, a 15% range, which is about the spread between identical runs here, so
  no time difference is resolved either way. The only consistent effect is WAL:
  deferral writes 17-28% less (727, 803 and 694 MB against 969 MB), because the index entries are not logged
  per row. That is real but is not time.
- **Scale caveat, and what is not measured in either direction.** At 60 000 releases the whole database fits
  in memory, even with `shared_buffers` at 16 MB (the operating system's cache holds it), so none of the
  following can show up here. The full load is about 300 times larger.
  - *Cost of deferring (extrapolated):* rebuilding the 94 indexes took about 6 s here; scaled linearly that is
    about 30 minutes at full size. That estimate is **optimistic**: with `maintenance_work_mem` at 256 MB the
    large builds will spill to disk and sort in several passes, and a linear scale ignores that.
  - *Benefits of deferring (unmeasured):* about 25% less WAL is about 71 GB saved if the full load writes about
    284 GB (this run's 16 KB per release would give about 317 GB, so about 79 GB); and index inserts slow once
    the indexes outgrow memory, which deferral avoids entirely. Either could be worth far more than the 30
    minutes. Neither is measured, so the numbers above argue neither for nor against deferral at full size.
- **Why not defer anyway.** Deferring means dropping 94 indexes the schema declares and recreating them after
  the load: a window with no indexes for readers, a crash mid-load that leaves them missing, and a rebuild that
  must be sequenced against `extraction_complete` and the derived refresh. At 60 000 releases the measured gain is no
  resolved time and a quarter less WAL; the cost is correctness risk in the one step that cannot be redone cheaply. Revisit only
  if a measurement on lux-sized data (operator-approved) shows the rate falling as the load grows, or if WAL volume
  becomes the constraint. The chunk rates above are the number to watch; repeat runs (n>1) would be needed to
  trust any difference.

## What it would need from database-schema

Nothing today. If the decision is revisited, the schema would need to publish which indexes are safe to
defer (all non-unique ones, by the rule above) and a function that recreates them from the contract, so the
loader does not carry index definitions of its own (`gm-database-schema-wclt` is the place to say so).

No change is filed: no measured gain justifies one.
