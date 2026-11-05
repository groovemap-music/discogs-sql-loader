# Write-path benchmark

One command reproduces the loader's batch write throughput on a disposable PostgreSQL:

```sh
just benchmark-write-path                                  # 5000 releases, batch 500, one worker
just benchmark-write-path --batch-size 2000 --workers 2   # a variant
```

It prints one JSON line:

```json
{"releases": 5000, "batch_size": 500, "workers": 1, "seed": 1, "seconds": 13.59, "releases_per_second": 368.0, "wal_mb": 73.2}
```

Arguments: `--releases` (the record count, whatever the type), `--batch-size`, `--workers`, `--seed`
(default 5000, 500, 1, 1), `--data-type releases|artists|masters` (default `releases`), and `--statements N`,
which (loopback or Unix-socket databases only, even with `--allow-remote-host`, because it runs `CREATE EXTENSION` and
`pg_stat_statements_reset()`) also prints the N costliest statements from `pg_stat_statements` (calls, rows, server milliseconds, WAL).

## What it does

`scripts/benchmark-write-path.sh` starts the repo's pinned `postgres:18-alpine` with `--rm` on a random
loopback port, and removes the container and its anonymous volume on exit (`docker rm -fv`).
`scripts/benchmark-write-path.py` applies the pinned `groovemap-database-schema`, builds deterministic
Discogs-shaped releases (artists, labels, genres, styles, 0-14 credits, 0-4 companies, 4-16 tracks with
credits on about 30%, Zipf-distributed entity ids so vertices conflict as they do in a real dump), and writes
them through the real `PostgreSQLBatchWriter.process_batch`, `AsyncPostgreSQLPool` and `WriteAdmission`.
Identifier aliases are not generated; the 7qar audit measured them at about 1% of server time.

Stopping the wrapper with SIGTERM or Ctrl-C stops the benchmark first, then removes the container.

It is not part of `just check` or the default test lane, and it needs Docker. To use your own throwaway
database instead, run `TEST_DATABASE_URL=... uv run python scripts/benchmark-write-path.py`; it applies the
schema and writes rows, so never point it at a real database. Before any DDL or write, the script connects (one connection, no statement) and checks the socket libpq
really opened: a Unix socket or a loopback peer passes, anything else is refused unless you also pass
`--allow-remote-host`. The check looks at the connection, not the DSN text, so `hostaddr`, a `service` entry, and
`PGHOST` or `PGHOSTADDR` in the environment cannot route it elsewhere. A host that cannot be reached is refused
too. (An SSH tunnel to a remote server looks like loopback; do not tunnel this at a real database.)

## Reading the numbers

Rates are synthetic and machine-specific, so compare only runs from the same machine with the same arguments
and `--seed`. Same seed gives the same data: WAL size is identical between runs (73.2 MB above), and
throughput repeated within 1% (368.0 and 364.7 releases/s on 2026-11-04, Docker on a Mac). Treat differences
under about 15% as noise on a busy machine, and run each variant more than once.

## Artist and master write paths (gm-discogs-sql-loader-2njb)

**Scope: these numbers cover the insert path only.** Every record written is new, so the hash gate never finds an
existing row. The changed-row path (hash differs: upsert conflict update, `DELETE` then re-insert of derived edges)
and the unchanged-row path (`updated_at` refresh, backfills) are not measured here.

Measured 2026-11-04 on the same machine and a disposable PostgreSQL 18, v0.4.3 (`c74843c`) with the
harness above: 20 000 records, batch 500, one worker, seed 1.

| Type | Records/s | WAL | Server time | Costliest statements (server ms, 20 000 records) |
| --- | ---: | ---: | ---: | --- |
| `artists` | 3 370, 6 444 (two runs) | 57.6 MB | about 2.3 s | `member_of` 913, `artists` upsert 502, `provider_aliases` 403, `alias_of` 293 |
| `masters` | 4 391, 4 707 (two runs) | 63.7 MB | about 2.1 s | `master_in_style` 595, `master_by_artist` 420, `masters` upsert 399, `master_in_genre` 333, `provider_aliases` 300 |
| `releases` (5 000, for scale) | 368, 365 | 73.2 MB | | see the 7qar audit for the per-statement breakdown |

Every statement is one `INSERT ... SELECT FROM unnest(...)` per relation per batch (40 calls for 40
batches). The hash-gate `SELECT`, `resolve_aliases` lookup, the `catalog_items` mint, and the
document-scoped `DELETE`s each cost under 80 ms in total.

**Findings.**

- Both types are already set-based; nothing in either path is a per-row statement or a per-row function call
  like the `credit_role_category` cost on releases.
- An artist or master costs about 1/12 of a release in time (about 4 500 against 365 records/s) and about
  1/5 in WAL (about 3 KB per artist or master against 14.6 KB per release). At the measured rates one million
  artists or masters take about 3-5 minutes against about 45 minutes for one million releases, so these
  two phases are not where a full load spends its time.
- Server time is only about half of wall time (2.3 s of 3.1-5.9 s); the rest is client-side derivation,
  round trips and Jsonb encoding. Run-to-run spread was large (artists 3 370 vs 6 444 records/s), so the
  first run after a container start is slower; discard it.
- Batch 2000 and two workers did not give a consistent gain for either type (artists 4 765 and 5 135,
  masters 3 917 and 6 363 records/s, against 3 370-6 444 and 4 391-4 707 at the baseline): inside the
  noise on this machine. No change to batch size or worker count follows from these two types; the
  sweep for the whole load is gm-discogs-sql-loader-pf94.
- The largest relations, `member_of` for artists and `master_in_style` for masters, take about 30% of
  server time each. Both are insert-only (`ON CONFLICT DO NOTHING`) and scale with the number of edges;
  there is no redundant work to remove.

**Fixes filed:** none. No statement in either path is out of proportion to the rows it writes.

## Batch size and worker count (gm-discogs-sql-loader-pf94)

Measured 2026-11-04 on v0.4.3 with the pinned schema (`groovemap-database-schema` 0.6.1), real PostgreSQL 18 in
a fresh container per cell, 12 000 releases, seed 1, two repetitions per cell. Insert path only (see above).
Records per second, both repetitions, then the mean:

| Batch size | 1 worker | 2 workers | 4 workers |
| ---: | --- | --- | --- |
| 100 | 228, 308 (268) | 389, 263 (326) | 436, 314 (375) |
| 500 | 406, 389 (397) | 388, 373 (380) | 423, 364 (393) |
| 1000 | 393, 426 (409) | 432, 368 (400) | 446, 311 (379) |
| 2000 | 404, 400 (402) | 428, 222 (325) | 451, 307 (379) |
| 4000 | 400, 349 (374) | 440, 349 (394) | 434, 295 (364) |

WAL was 177-180 MB in every cell, so the work done is identical. Repetitions of the same cell differ by up to
about 40% on this machine (swap pressure, other developers' containers), so only large effects are readable:

- **Batch 100 is clearly worse** than 500 and up (about 270 against about 400 records/s with one worker).
- **Batch 500 to 4000 is flat** within the noise. The 7qar audit's 26.3 s to 15.2 s (1.7x) for 500 to 2000 does
  not reproduce on the current code: it was measured before the set-based writes landed, and with them the
  per-batch round-trip overhead it removed is already gone.
- **Workers 1 to 4 add at most about 10%** and not consistently. The cost is server-side CPU in the
  schema's generated `role_category` column (audit item 1), which more connections do not shorten.

**Recommendation: keep batch size 500 and the current writer admission; change neither for the first load.**
A larger batch buys nothing measurable and costs more per poison isolation (below) and more unacked
deliveries held (prefetch is `max(200, 2 x batch)` per consumer). If any deployment still runs the code
default of 100, moving it to 500 is the one change the data supports. The lever that moves the full-load time
is the schema's `credit_role_category` cost, which is a database-schema change.

### Poison isolation and the hold budget

`just benchmark-poison` puts one record in a batch that PostgreSQL rejects (a NUL character in a JSON string,
SQLSTATE 22P05) and drains it through the real batch processor. Results, poison at the middle of the batch:

| Batch size | Acked | Dead-lettered | Healthy dead-lettered | Writer calls | Seconds |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | 99 | 1 | 0 | 13 | 2.3 |
| 500 | 499 | 1 | 0 | 17 | 5.3 |
| 1000 | 999 | 1 | 0 | 19 | 9.9 |
| 2000 | 1999 | 1 | 0 | 21 | 13.7 |
| 4000 | 3999 | 1 | 0 | 23 | 24.3 |

Position 0 gave the same counts at 500 (4.1 s) and 2000 (11.6 s). The engine bisects to exactly the poison
record: no healthy record is dead-lettered at any size, and the writer calls grow with log2(batch size). Time
grows about linearly with batch size (a bisection rewrites each half), from 2 s to 24 s per poison record
between 100 and 4000.

The hold budget (`max_hold_s` 1500 s, plus the 5 s flush interval and 30 s maximum backoff, must stay under the
1800 s broker `consumer_timeout`) bounds time spent in transient backoff. One poison isolation at 4000 takes
24 s, so batch size does not threaten it, but a dump with frequent poison records would pay that per record:
isolation cost is the reason not to raise the batch size without a measured gain.
