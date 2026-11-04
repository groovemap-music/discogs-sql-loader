# SQL write-path throughput audit

Bead `gm-discogs-sql-loader-7qar`, 2026-11-04. Read-only audit: no production code changed,
nothing was written on lux. Audited commit `b4fbf55` (v0.4.1 plus the shared classifier).

## Summary

On lux the loader writes about 2 M releases/hour, so the 19.6 M-release load needs about
**10 hours** of SQL time. Measured on a disposable PostgreSQL, three changes explain
almost all of the avoidable time:

| # | Change | Where | Measured effect (10 k releases, one worker) |
| --- | --- | --- | --- |
| 1 | `graph.credit_role_category()` is evaluated for every `credited_on` / `track_credited_on` row and costs about 0.14 ms per call | `database-schema` | 85.6 s → 43.6 s (**1.96x**) |
| 2 | One statement execution per graph row (`executemany`), about 23 per release | this repo | 85.6 s → 33.1 s (**2.6x**); 43.6 s → 26.3 s (**1.66x**) on top of #1 |
| 3 | Batch size 500 → 2000, after #1 and #2 | config + this repo | 26.3 s → 15.2 s (**1.7x**) |

Together #1 + #2 are **3.3x** and #1 + #2 + #3 are **5.6x** over the baseline at the same
worker count. Smaller items follow in the ranked list.

**Partial data.** Postgres on lux holds 3.97 M of 19.6 M releases, so every number here
is a lower bound for the full load. My runs start from an empty schema and cover 0 to 10 k
releases. See [Scaling to the full row counts](#scaling-to-the-full-row-counts).

## Method

- PostgreSQL 18 (the repo's pinned `postgres:18-alpine` digest) in a throwaway container,
  on an ephemeral port, started with `--rm` and `pg_stat_statements` enabled. Schema from
  the pinned `groovemap_schema.postgres.create_postgres_schema` (database-schema v0.6.1 contract
  commit `707b241`). Every variant started from a `CREATE DATABASE ... TEMPLATE` copy of the
  same empty schema. The container and its volume were removed afterwards.
- Load generator: 10 000 synthetic Discogs-shaped releases (500 per batch, 20 batches) with
  the real `PostgreSQLBatchWriter.process_batch`, a real `AsyncPostgreSQLPool`, and
  `WriteAdmission`. Each release has 1-3 artists, 1-2 labels, 1-2 genres, 1-4 styles, 0-14
  credits, 0-4 companies, 4-16 tracks with credits on about 30%, and 1-3 identifier aliases
  (about 5% drawn from a shared catalogue-number pool, to produce alias conflicts). Entity
  popularity is Zipf-distributed so vertex conflicts occur.
  The data is synthetic, so **absolute rates are not comparable to lux**; only ratios
  between variants are meaningful.
- Per-statement times come from `pg_stat_statements` (server execution time) and
  `pg_current_wal_lsn()` deltas. Wall time is client-side per run.
- Each variant was run **once**. The Docker-on-Mac VM is noisy; treat differences under
  about 15% as noise.
- Disk: the largest database was 138 MB, far below the 3 GB cap.
- Incident: my first container start failed on a port bind, and my schema-apply step ran against
  another developer's container (`ct7c-pg`) on that port. The pinned initializer reported zero
  failures and nothing else was written there. I told the dispatcher at once. The rest of the audit used a port taken
  from `docker port` of my own container, checked before every run.

## Per-statement breakdown (baseline, one worker, 20 batches of 500)

Wall time was 85.6 s for 10 000 releases (117 releases/s); WAL written was 243 MB.
Server-side statement time totalled about 63 s. The other 22 s (26%) is client-side:
Python derivation is negligible (`derive_document` 28 ms per 500 records), so this is
round trips and per-row bind/execute overhead of about 230 000 row executions.

| Statement | Rows | Server time | Per row | WAL |
| --- | ---: | ---: | ---: | ---: |
| `INSERT graph.credited_on` | 60 658 | **33.2 s** | 0.55 ms | 32 MB |
| `INSERT graph.track_credited_on` | 30 143 | **16.5 s** | 0.55 ms | 19 MB |
| `INSERT graph.person` | 57 656 | 3.7 s | 0.06 ms | 42 MB |
| `INSERT graph.same_as` | 70 208 | 3.0 s | 0.04 ms | 24 MB |
| `INSERT releases` (upsert) | 10 000 | 2.0 s | 0.20 ms | 52 MB |
| `INSERT graph.credited_to` | 17 143 | 0.8 s | 0.05 ms | 8 MB |
| `INSERT graph.company` | 7 583 | 0.7 s | 0.09 ms | 3 MB |
| `INSERT provider_aliases` | 24 539 | 0.7 s | 0.03 ms | 12 MB |
| every other edge/vertex insert, resolve, hash-gate `SELECT`, `DELETE`s | | under 0.3 s each | | |

`credited_on` + `track_credited_on` are **79% of server statement time**.

**Why they are slow.** Both tables carry `role_category text GENERATED ALWAYS AS
(graph.credit_role_category(role)) STORED`. The function is a SQL `CASE` of about 74
`strpos(normalized.role, '...') > 0` branches. I timed it at **0.138 ms per call** over
100 000 varied roles (the statement-level cost above is that plus three index inserts).
A stored generated column is computed for every row offered to `INSERT`, including rows
that then hit `ON CONFLICT DO NOTHING`, and a few thousand distinct role strings account
for nearly all credits, so the same answer is recomputed millions of times. At lux
scale this is the largest single cost.

Alias and identity cost, as asked in the bead: `resolve_aliases` is three statements per batch and
`attach_aliases` one or two. `provider_aliases` took **0.7 s of 63 s (about 1%)** in the baseline, and the alias-conflict
path is one extra `SELECT` per batch. Alias writes are not a bottleneck. Identity
backfill is not exercised on a first load (no hash-unchanged rows).

## Ranked improvements

Gains are from this audit's runs unless marked *estimate*. All of them preserve the
hash gate, `ON CONFLICT DO NOTHING` convergence, vertices-before-edges ordering,
document-scoped delete-then-insert, `source = 'discogs'` scoping, and the single
transaction per batch that poison isolation depends on.

1. **Make `credit_role_category` cheap without changing its answers** (`database-schema`).
   *Measured upper bound:* replacing the function with a constant took 85.6 s to 43.6 s
   and `credited_on` from 33.2 s to 4.0 s server time. That stub is only a bound; it is
   **not** a proposed fix, because the column has to keep returning the vocabulary's
   categories. Real options: an exact-match lookup (a `VALUES` or `jsonb` map, or a small
   `graph.credit_role` table) tried first with the substring scan only on a miss;
   or a rewrite that keeps the substring rules but short-circuits. Acceptance is
   byte-identical output over every role in the vocabulary plus the full set of roles seen
   in the dump. Needs a database-schema release and a contract bump here.
2. **Set-based graph writes** (this repo). One `INSERT ... SELECT FROM unnest(arrays)
   ON CONFLICT DO NOTHING` per relation per batch instead of one execution per row.
   *Measured with a prototype in the harness only:* 85.6 s to 33.1 s alone; 43.6 s to
   26.3 s on top of #1. The same change for the entity upsert (`executemany` of 10 000
   row executions, about 2 s server time plus round trips) is not prototyped:
   *estimate* a further 3-5%. Sort rows by key before insert so concurrent batches take
   locks in the same order (see #5).
3. **Larger batches for the first load** (config, with a check on the hold budget).
   After #1 and #2, 2000 per batch took 26.3 s to 15.2 s (one run). Constraints to
   verify first: the batch hold must stay inside `consumer_timeout` (the v0.4.1
   1500 s hold budget), prefetch must stay at least 2x the batch size, and a poison
   batch of 2000 costs more to isolate than one of 500. Worth a measured sweep (500,
   1000, 2000, 4000) rather than taking my single point.
4. **Concurrency only pays after #1 and #2.** With both applied, two workers took 26.3 s
   to 13.2 s (2.0x). Without them, two workers took 95 s against 85.6 s for one (no gain),
   so the extra connections are being spent on lock and CPU contention. The code's
   writer admission is `pool_max - 3` (9 with the default pool of 12); `docs/performance-guide.md`
   says "at most two". Confirm the value on lux before changing anything.
5. **Deadlock on shared vertex rows.** The first two-worker baseline run failed with
   `deadlock detected` on `graph.genre`: two concurrent batches insert the same vertex
   rows in different orders. Sorting each relation's rows before insert (also part of #2)
   fixes the cause; the retry classifier only hides it. This is a correctness-adjacent
   throughput risk, since every deadlock rolls back a 500-record transaction.
6. **First-load index deferral** (coordinate with the database-schema audit).
   `releases` has 12 secondary indexes, among them five GINs. Dropping all of them
   cut the `releases` upsert from 2.15 s to 0.93 s server time, which is only 1.2 s of
   44 s at 10 k rows. It will matter more at millions of rows because GIN and hash
   maintenance grow with table size, but **I did not measure that**. The graph side has
   the same shape: `person` carries a btree on `sha256(name)`, a hash index and a
   trigram GIN, and writes 0.7 KB of WAL per inserted row. *Estimate:* modest
   at first, growing with table size; decide with a measurement at 1 M+ rows.
7. **WAL volume.** 243 MB per 10 k releases is 24 KB per release, or *estimate* about
   470 GB over 19.6 M releases. The figure is inflated by full-page images on a fresh
   database and did not move with #1-#3 (230-243 MB in every variant), so only fewer
   or cheaper indexes (#6) reduce it. On lux, check `max_wal_size` and checkpoint
   frequency during the load.
8. **`synchronous_commit = off`: 85.6 s to 72.7 s (1.18x, within noise).** Not
   recommended. RabbitMQ acks after commit returns, so an unflushed commit can be lost
   after the broker has already dropped the message.

Not an improvement: alias writes (about 1%), `derive_document` (about 0.06 ms per
record), hash-gate `SELECT` and the three-statement resolve (under 1% combined).

## Target for the full load

Today: about 2 M releases/hour on lux, so about **9.8 h** for 19.6 M releases, plus artists.

- **Target: 4 h or less for the release load**, a 2.5x speed-up. That needs #1 and #2
  (3.3x measured at 0-10 k rows; I discount it to 2.5x because the gap narrows once
  index maintenance on large tables dominates) plus #5 so that concurrency stays safe.
  Nothing in this tier changes the consumer's delivery behaviour.
- **Stretch: about 2.5 h** (about 4x), adding #3 and, if a 1 M+ row measurement supports
  it, #6.

Artists (10.3 M) and masters are not profiled here. Artist documents write far fewer
graph rows and carry no `role_category`, so #1 does not help them. #2 and the entity
upsert change should. Treat any artist estimate as unmeasured.

## Scaling to the full row counts

Because lux is partly loaded, these runs only cover the early part of the curve.

| Cost | Behaviour as tables grow |
| --- | --- |
| `credit_role_category` | **Constant per row**, so it scales linearly with credit rows. Its share of time stays at least as large as measured. |
| Round trips / per-row execution | Constant per row. Linear. |
| Index maintenance (`releases` GINs and hash, `person` trigram, unique `text_key` btrees) | Grows with table and index size once they stop fitting in cache. **Per-batch time rises over the load**, so these timings understate the late-load cost. Not measured. |
| WAL and checkpoints | Linear in rows, with full-page-image bursts after each checkpoint. Not measured. |
| Load-end derived refresh and identity ledger | Not re-measured. `docs/derived-refresh-ack-budget.md` (1 M-release synthetic, 2026-09) put the derived transaction at about 121 s and found a full dump cannot be certified inline under the 1800 s ack timeout. That is why durable refresh is the default; its cost still grows with full row counts. |

## Proposed follow-up beads

Not filed here, as instructed.

1. `database-schema`: speed up `graph.credit_role_category` with identical output
   (#1). Acceptance: identical categories over the full vocabulary and observed role set;
   `credited_on` insert time per row falls to the level of the other edge tables.
2. `discogs-sql-loader`: set-based graph and entity writes with sorted rows (#2, #5).
   Acceptance: parity and integration suites unchanged; hash-gate, quarantine and
   source-scoped deletes tested; a two-worker run shows no deadlocks.
3. `discogs-sql-loader`: commit the batch-write benchmark harness (used here, not
   committed) as a reproducible script, with a 1 M-row scaling mode.
4. `discogs-sql-loader` + `deployment`: batch-size sweep for the first load and the
   writer admission value on lux (#3, #4), within the hold budget.
5. `database-schema` + `discogs-sql-loader`: measured decision on deferring secondary
   indexes and `person` trigram indexing during first load (#6, #7); include
   `max_wal_size` guidance.
6. `discogs-sql-loader`: re-measure the full-dump derived refresh and identity-ledger
   work at the real 19.6 M / 10.3 M row counts once the Postgres load completes.

## Caveats

- Single runs on a Docker-on-Mac VM; ratios, not rates.
- Synthetic data. Real credit counts per release and role distributions may shift the
  `credited_on` share, probably upward given 25 M aliases and long credit lists.
- The prototype and stub lived only in the throwaway harness. No production code was changed.
- `main` has since moved (a P0 in `consumers_lost_at_broker` is being fixed elsewhere).
  Nothing here touches the consumer path.
