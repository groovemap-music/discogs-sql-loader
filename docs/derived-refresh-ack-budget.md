# Derived-relation refresh and RabbitMQ acknowledgement budget

Bead `gm-discogs-sql-loader-vd0`, measured 2026-09-20/21. Verdict: **do not certify the
current inline refresh for a full Discogs dump**. Preserve the existing
commit-before-ack behavior until a durable scheduled handoff is implemented; never
replace it with an untracked in-process task.

## Question and method

The last of four `extraction_complete` deliveries currently holds its RabbitMQ ack
while it flushes/purges, reconciles `member_of` and `same_as`, rebuilds seven counters,
refreshes `artist_member_of`, refreshes `vertex_degree`, and commits the latch stamp.
RabbitMQ's consumer acknowledgement timeout is **1,800 seconds**. The timings below
cover only the derived-relation transaction invoked with `latch=None` (the same work,
minus one small final stamp). Flush, purge, signal upsert, latch stamp, pool wait,
broker delivery handling, and contention spend additional time from that same budget.

The deterministic generator at `database-schema`'s
`docs/spikes/gm-database-schema-9c8.1/generate.py` (seed **20260917**, source revision
`274a84758c53c61e4905c242bdd038f1e3cc9f06`, this loader's immutable dev pin)
streamed a synthetic catalog into the exact promoted schema. It has 1,000,000 releases,
120,000 artists, 20,000 labels, 250,000 masters, 60,000 MusicBrainz artists, and
254,925 MusicBrainz relationships (300,000 generated, with Discogs-origin relationships
excluded from the MusicBrainz table). The materialized graph contained 1,920,257
`by_artist`, 1,329,094 `on_label`, 1,644,556 `in_genre`, 2,265,063 `in_style`,
1,599,397 `credited_on`, and 1,000,000 `issued_on` rows. The source generator has
power-law artist/label popularity and deterministic cardinalities, rather than uniform
relationships. It does **not** assert Discogs `member_of` or `same_as` edges or a
cross-provenance MusicBrainz MEMBER_OF union, so those two output cardinalities and
their refresh costs are understated; the million-release document scan in `same_as`
is nevertheless exercised.

Hardware was a local Apple M1 Pro host (10 logical CPUs) running a **2-vCPU, 7.74-GiB
Docker VM**. PostgreSQL was `19beta3-alpine` at immutable digest
`sha256:b1692e50613a21e61c424859f943b9e193ae73e5a8c68abd5382dfb235bf15fc`,
with 1,536-MB shared buffers, 128-MB work memory, two parallel workers, 1-GiB shared
memory, `random_page_cost=1.1`, `track_io_timing=on`, and JIT off. No IBM Cloud or
production resources were used.

Reproduce on a disposable database, never one serving the product:

1. Start the digest-pinned container with the settings above; apply this loader's
   pinned `groovemap_schema.initializer._apply_postgres_schema` (the existing
   `gm-database-schema-gkt.1/run.sh` demonstrates the same startup settings).
2. For each of `artists`, `labels`, `masters`, `releases`, `mb_artists`, and
   `mb_relationships`, pipe `python3 generate.py --target postgres --scale synthetic
   --table <name>` into PostgreSQL `\copy` of, respectively,
   `public.artists(data_id,hash,data)`, `public.labels(data_id,hash,data)`,
   `public.masters(data_id,hash,data)`, `public.releases(data_id,hash,data,media)`,
   `musicbrainz.artists(mbid,name,sort_name,type,gender,begin_date,end_date,ended,area,begin_area,end_area,disambiguation,discogs_artist_id)`,
   and `musicbrainz.relationships(source_mbid,target_mbid,source_entity_type,target_entity_type,relationship_type,begin_date,end_date,ended,attributes)`.
   The existing `gm-database-schema-9c8.1/load-postgres.sh` shows those exact commands;
   omit its `materialize.sql` step because it builds a separate spike namespace.
3. `VACUUM ANALYZE` the source tables. Set `TEST_DATABASE_URL` to this disposable DB,
   then run `uv run python scripts/prepare-derived-refresh.py` to fill the promoted
   graph's vertices and edges from the pinned definitions, committing each relation.
4. Run `uv run python scripts/measure-derived-stages.py --timeout-seconds 600` for
   independent stage diagnostics, then
   `uv run python scripts/measure-derived-refresh.py --runs 2 --timeout-seconds 1800`
   for the real all-or-nothing loader transaction and unchanged-data retry. Both
   profilers print JSON without logging the connection string. Repeat the whole-pass
   command for a third observation.
5. Stop and remove **only** the named disposable container and volume created for
   this run. Do not run a broad Docker prune. The local run used `gmvd0-pg` and
   `gmvd0-pgdata`.

`graph.bootstrap_fill()` is **not** the timed loader path. As a diagnostic, running
that one-off fill immediately after bulk loading, in one transaction without graph
relation statistics, was canceled after **2,284.985 s** while executing
`style_stats` (all preceding vertices/edges and `genre_stats` had run, but no later
counter or path refresh had). Its transaction rolled back. This demonstrates a
dangerous cold-statistics shape, not the steady-state loader duration; the source
tables survived and the promoted graph was instead prepared in committed stages.
`credited_on` preparation itself took 450.044 s and lies **outside** the ack window.

## Evidence

Three complete executions of the actual `refresh_derived_relations` function, on the
committed promoted graph, produced these durations in seconds:

| Stage | Initial | Unchanged-data retry | Third pass |
| --- | ---: | ---: | ---: |
| Reconcile `member_of` | 0.045 | 0.045 | 0.045 |
| Reconcile `same_as` | 24.442 | 24.087 | 24.354 |
| Counter `genre_stats` | 17.539 | 17.932 | 18.083 |
| Counter `style_stats` | 29.918 | 31.120 | 31.156 |
| Counter `label_stats` | 8.507 | 10.204 | 13.476 |
| Counter `artist_degree` | 0.610 | 0.698 | 0.693 |
| Counter `release_degree_base` | 4.821 | 5.009 | 5.671 |
| Counter `artist_genre` | 9.622 | 9.836 | 10.921 |
| Counter `label_genre` | 4.882 | 5.127 | 5.526 |
| `refresh_artist_member_of()` | 0.661 | 0.687 | 0.775 |
| `refresh_vertex_degree()` | 8.717 | 8.853 | 10.225 |
| **Actual total, including transaction/commit** | **109.806** | **113.624** | **120.954** |

The unchanged-data retry returned exactly the same seven counter row counts as the
first pass (15, 440, 20,000, 119,995, 1,000,000, 1,263,775, 298,309 in the table
order above), with unchanged source and output relation cardinalities, including
1,390,450 `vertex_degree` rows. The integration suite independently checks
downward convergence, repeat-pass idempotence, rollback on failure, once-per-version
latch stamping, restart between signals, and superseded stragglers. A crash during the
current inline pass rolls back the relation changes and latch stamp, and RabbitMQ
redelivers the unacked signal; that is the durability property any redesign must keep.

For the measured tier, an explicit **300-second safety margin** added to the worst
120.954-second pass gives **420.954 s**, below 1,800 s. But the real full dump has
roughly **17 million releases** (the source-code comment calls out the ~17M releases
that credit nobody), about **17×** this synthetic tier. A *linear-only illustration*,
not a measured full-dump prediction, gives 120.954 × 17 = **2,056.218 s**, or
**2,356.218 s including the same 300-second margin**, already over the broker limit.
The retained `same_as` document scan, edge aggregation, nonempty membership, cold
statistics, slower hardware, concurrent reads, and stale-row purge can worsen it.
Conversely, a stronger machine might improve it; this local result cannot establish
full-scale safety in either direction. The readiness rule is that the worst observed
full-scale end-to-end delivery **plus 300 s** must be under 1,800 s on the actual
deployment profile, repeatedly, before anyone chooses inline semantics.

## Durable handoff required before removing inline work

This design is implemented by `tableinator/durable_refresh.py` against immutable
`database-schema` pin `96da0291ccb30a86af94d0ee8dcf959bd47ada6b`. Production now
defaults to `DERIVED_REFRESH_MODE=durable`; `DERIVED_REFRESH_MODE=inline` is an explicit
rollback to the old commit-before-ack path while retaining the additive job relations.
If the durable contract probe or legacy-latch reconciliation fails, the service reports
degraded health and does not subscribe to the queues until recovery. If a terminal
signal/job commit fails after subscription, it cancels every consumer with broker
confirmation before nacking that delivery. The broker keeps the message queued;
a periodic recovery probe retries the failed signal's database commit without
consuming another delivery attempt, and only then resubscribes. The in-memory copy
is a recovery hint, never the sole obligation or a reason to ack. This prevents a
database outage from rapidly exhausting the quorum queue's delivery limit of 20.

The durable producer accepts only real `YYYYMMDD` dump versions. The extractor's
`started_at` can identify an attempt but cannot order source dumps; later arrival at
this consumer is not evidence of a newer extraction. The first accepted signal locks
the loader cursor and assigns a generation. Startup reconciles complete legacy
NULL-generation latches into jobs without requiring a fresh RabbitMQ delivery, while
an older legacy latch is fenced by a newer accepted source version. A fourth signal
and pending job commit before ack. Duplicate redelivery keeps the same job. The
worker scans at startup and every 15 seconds, claims under the cursor lock, waits on
a per-loader transaction advisory lock for the full pass, heartbeats its token/epoch
lease, and rechecks both cursor and lease under lock just before the atomic commit.
Expired leases enter bounded retry with a health-visible failure and an alert log.

The `/health` payload's `durable_derived_refresh` object exposes current and last
completed versions, phase/duration, pending age, attempts, retry due, lease expiry,
sanitized failure, and superseded count. A stale pending, retrying, or expired leased
job degrades readiness. Low-cardinality OpenTelemetry measurements mirror worker
transitions, duration, pending age, attempts, and superseded count. Versionless or
unordered terminal deliveries are refused to the dead-letter exchange and recorded
in `derived_relation_refresh_last_refused`; they are never marked completed.

The original design requirements follow for audit:

`public.loader_extraction_latch` already durably records the four signals and stamps
`refreshed_at` only in the refresh transaction. The handoff should build a persistent
job state on that schema-owned latch (or a schema-owned job relation keyed by
`(loader, version)`) with a monotonic extraction generation, pending/leased/retry/
completed/superseded state, attempt count, next-attempt time, lease owner/expiry,
last sanitized failure, and timestamps. No loader issues DDL. Required invariants:

1. After flush and guarded purge, commit the fourth signal **and the pending job** in
   PostgreSQL before acking. If that commit fails, nack/requeue; if ack fails after
   commit, redelivery upserts the same job. A missing or incompatible latch must fail
   closed for the terminal signal, not silently ack a memory-only obligation.
2. A worker scans pending/expired jobs at startup and periodically, not merely when a
   notification or in-process task survives. Claim a job transactionally, serialize
   refreshes per loader, and fence stale claimants. The work may take longer than
   RabbitMQ's timeout because the trigger is already durably scheduled.
3. Reconcile, seven counters, both path refreshes, latch `refreshed_at`, and completed
   job state commit atomically. A process/DB crash before commit leaves the old graph
   and an eligible durable job; a crash after commit is done exactly once per version.
   An expired lease is requeued with bounded backoff and an alert, not silently
   abandoned after the broker's delivery limit.
4. A newer extraction supersedes an older *pending* job; a straggler cannot schedule
   an old one. A running job rechecks the monotonic generation at commit under a
   short writer/commit fence: if superseded, roll back instead of stamping the old
   version. The newer complete extraction must eventually refresh. Validate behavior
   when a new dump begins during an old pass rather than assuming queue order.
5. Expose pending age, current version, phase/duration, attempt count, retry due,
   last failure, superseded count, and newest completed version in health/metrics.
   An old or failing job must make readiness degraded even if RabbitMQ deliveries
   have been acked. Refuse or dead-letter versionless terminal messages with an
   explicit alert; do not report them completed.

The implementation's tests inject failure at every boundary (pre-commit, post-commit /
pre-ack, mid-refresh, post-refresh/pre-stamp, lease expiry), duplicate delivery,
two workers, a later extraction, restart with no new messages, schema absence,
and a simulated slow refresh over 1,800 s. For rollback, the current inline,
commit-before-ack behavior remains available by explicit mode selection, but a real
full dump still cannot be certified under the broker timeout in that mode.
