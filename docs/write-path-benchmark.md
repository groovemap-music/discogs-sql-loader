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
which also prints the N costliest statements from `pg_stat_statements` (calls, rows, server milliseconds, WAL).

## What it does

`scripts/benchmark-write-path.sh` starts the repo's pinned `postgres:18-alpine` with `--rm` on a random
loopback port, and removes the container and its anonymous volume on exit (`docker rm -fv`).
`scripts/benchmark-write-path.py` applies the pinned `groovemap-database-schema`, builds deterministic
Discogs-shaped releases (artists, labels, genres, styles, 0-14 credits, 0-4 companies, 4-16 tracks with
credits on about 30%, Zipf-distributed entity ids so vertices conflict as they do in a real dump), and writes
them through the real `PostgreSQLBatchWriter.process_batch`, `AsyncPostgreSQLPool` and `WriteAdmission`.
Identifier aliases are not generated; the 7qar audit measured them at about 1% of server time.

It is not part of `just check` or the default test lane, and it needs Docker. To use your own throwaway
database instead, run `TEST_DATABASE_URL=... uv run python scripts/benchmark-write-path.py`; it applies the
schema and writes rows, so never point it at a real database.

## Reading the numbers

Rates are synthetic and machine-specific, so compare only runs from the same machine with the same arguments
and `--seed`. Same seed gives the same data: WAL size is identical between runs (73.2 MB above), and
throughput repeated within 1% (368.0 and 364.7 releases/s on 2026-11-04, Docker on a Mac). Treat differences
under about 15% as noise on a busy machine, and run each variant more than once.

## Artist and master write paths (gm-discogs-sql-loader-2njb)

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
