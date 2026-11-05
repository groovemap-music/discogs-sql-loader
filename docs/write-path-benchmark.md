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

Arguments: `--releases`, `--batch-size`, `--workers`, `--seed` (default 5000, 500, 1, 1).

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
schema and writes rows, so never point it at a real database. The script refuses a `TEST_DATABASE_URL` whose host is not loopback or a
Unix socket unless you also pass `--allow-remote-host`.

## Reading the numbers

Rates are synthetic and machine-specific, so compare only runs from the same machine with the same arguments
and `--seed`. Same seed gives the same data: WAL size is identical between runs (73.2 MB above), and
throughput repeated within 1% (368.0 and 364.7 releases/s on 2026-11-04, Docker on a Mac). Treat differences
under about 15% as noise on a busy machine, and run each variant more than once.
