# Changelog

All notable changes to this repository will be recorded here by Commitizen from
Conventional Commits.

## v0.4.5 (2026-11-05)

### Operator notes

- **Behaviour change: an outage past about 25.4 hours dead-letters held records.** While PostgreSQL is
  down the loader holds each delivery and returns it to RabbitMQ. A delivery now goes back at most
  `POSTGRES_BATCH_MAX_REQUEUES` times (default 60, so 61 holds, about 25.4 hours at the 1500 s hold),
  counted from the broker's `x-acquired-count`, and is then dead-lettered instead of requeued. Records
  held past that are in the DLQ, not lost, and a dead-lettered record vetoes that run's stale-row
  purge. Restarts and channel closes spend the same count. Before this, a held record could return
  forever. Raise `POSTGRES_BATCH_MAX_REQUEUES` (at least 1) to survive a longer outage.
- **Person names.** A credit name is stripped of surrounding whitespace, control and format characters
  (for example U+200C, U+200F, U+FEFF) before it keys a Person, and a name that is only padding is
  skipped and counted. Release together with discogs-graph-enricher v0.6.1: stripping in one store
  alone splits the Person keys. `groovemap-runtime` is pinned to `0687736`.

### Fix

- **person-names**: strip padding from a credit name before it keys a Person
- **redelivery**: clear the purge veto only after the ack succeeds
- **redelivery**: keep the purge veto until the terminal signal commits
- **redelivery**: dead-letter a record that keeps failing at a stated bound

## v0.4.4 (2026-11-05)

### Operator notes

- **Batch engine fix.** `groovemap-runtime` is pinned to `ecde48b`, which stops one busy queue starving
  every other queue's periodic flush and the memory growth that followed from it. The pin also carries
  the other runtime commits since the previous pin, including PostgreSQL pool and health-probe changes.
- **Pending counts.** The batch snapshot's `pending` now includes handlers still inside `submit`, not
  only queued records, so it can read higher than before for the same load.
- **Shutdown.** `flush_all` can take up to one `POSTGRES_BATCH_FLUSH_INTERVAL` longer, because a flush
  now waits for those handlers before it reports nothing pending.
- **Lux memory cap.** Lux's 30 GiB cap stays in place until memory is proven under a real backlog.
- **Set-based `first_year`.** `database-schema` is pinned to `0f02656`, which computes genre and style
  `first_year` in one pass over releases.

### Fix

- **perf**: refuse multi-host DSNs, pin the verified address, and refuse databases that are not the benchmark's own
- **perf**: use the connected-socket guard in the poison benchmark
- **perf**: never let --statements run against a non-loopback database
- **perf**: refuse --statements on a non-loopback database and scope the profile to inserts
- **perf**: check the socket the benchmark really opened, not the DSN text
- **perf**: refuse non-loopback hosts in the benchmark and stop it on SIGTERM

### Perf

- **counters**: compute genre and style first_year in one pass over releases

## v0.4.3 (2026-11-04)

### Perf

- **postgres**: write batches set-based and in sorted lock order

## v0.4.2 (2026-11-04)

### Fix

- **tableinator**: detect lost consumers from real frames and always reattach
- **postgres**: default batch hold to 1500 s and budget it under consumer_timeout
- **postgres**: adopt shared failure classifier and bound held batch deliveries

## v0.4.1 (2026-11-04)

### Fix

- **tableinator**: dead-letter permanent PostgreSQL errors and recover cancelled consumers

## v0.4.0 (2026-10-31)

### Operator notes

- **Extraction latch.** On `extraction_complete` the loader records a `discogs` row in
  `public.loader_extraction_latch` (keyed `(loader, version)`); the counter, MEMBER_OF and degree
  refreshes run once all four entity signals have arrived. The deployment's Discogs import gate reads
  this table. Requires `groovemap-database-schema` v0.5.0 or later, which declares the latch, the
  derived-refresh cursor and job tables, and `graph.refresh_artist_member_of()` /
  `graph.refresh_vertex_degree()`. The loader never creates these objects; with the latch relation
  absent it runs degraded and logs a warning.
- **Durable derived refresh (default).** `DERIVED_REFRESH_MODE=durable` schedules the refresh in
  `public.loader_derived_refresh_job` before acknowledging the terminal signal, and a worker completes
  it outside the acknowledgement window. If the durable schema is missing or incompatible the loader
  stays up, pauses consumers, requeues terminal deliveries and reports `degraded` on the health
  endpoint. Set `DERIVED_REFRESH_MODE=inline` to roll back to commit-before-ack refresh.
- **Database connections.** Durable mode reserves 3 pool connections (refresh transaction, lease
  heartbeat, health sampler) and needs `POSTGRES_POOL_MAX_SIZE` of at least 4 (default 12).
- **Health port.** The health endpoint stays on `8002` by default; `HEALTH_PORT` now overrides it and
  the image `HEALTHCHECK` follows. An invalid value fails startup instead of falling back.
- **Exit on startup failure.** A startup that gives up (configuration error, PostgreSQL pool or AMQP
  broker unreachable) now exits with status 1 so Docker's restart policy applies.

### Feat

- **consumers**: bound database work across per-consumer qos
- **health**: read tableinator health port from HEALTH_PORT
- **graph**: derive per-track credits and track performer relations
- **refresh**: schedule derived maintenance durably before ack
- **graph**: refresh pathfinder relations on extraction latch
- **health**: surface the last extraction_complete refused for naming no extraction
- **graph**: refresh the counter and degree relations on extraction_complete
- **graph**: derive and write the graph vertex and edge rows per document

### Fix

- **runtime**: pin groovemap-runtime with the log-file fallback
- **ingestion**: share pool admission and drain Discogs callbacks (#17)
- **consumers**: retain admission until callbacks drain
- **refresh**: reserve database headroom in durable mode
- **ingestion**: drain callbacks before releasing pool budget
- **ingestion**: share write admission across SQL sinks
- **build**: install the staged runtime wheel without source overrides
- **deps**: upgrade msgpack past CVE-2026-114613
- **ingestion**: bound Discogs writers with shared admission
- **tableinator**: exit 1 when startup gives up
- **tableinator**: exit 1 when startup gives up
- **tableinator**: exit 1 when startup gives up
- **deps**: upgrade filelock to 4.2.1 past CVE-2026-112940
- **deps**: update certifi to 2026.10.19 and opentelemetry-api to 1.45.1
- **deps**: update multidict to 6.9.1 and urllib3 to 2.8.0 (#10)
- **consumers**: retain transport state after failed recovery cleanup
- **consumers**: confirm bounded cancellations and reset run state
- **refresh**: reset broker on every uncertain cancel
- **refresh**: fence recovery on confirmed broker reset
- **refresh**: pause consumers before retrying durable terminals
- **counters**: sync graphinator parity bodies
- **graph**: refuse an extraction_complete that names no extraction
- **graph**: probe for the extraction latch relation instead of creating it
- **graph**: key the derived-relation latch on the extraction and persist it
- **graph**: commit a document and its graph rows in one transaction

### Refactor

- **latch**: one declared relation, a required discriminator, a checked key

## v0.3.0 (2026-09-15)

### Feat

- **loader**: attach identifier aliases per batch and per record
- **loader**: resolve native catalog ids per batch and write gm_item_id
- **telemetry**: join the extractor's trace and span every batch flush

### Fix

- **ci**: accept commitizen's no-eligible-commits bump-preview state
- **deps**: bump python to 3.14.7-slim and dockerfile frontend to 1.27 (#1)
- **upsert**: backfill media in the non-batch path too
- **batch**: backfill media on hash-unchanged releases rows
- **media**: require all formats entries be mappings before routing structured
- **media**: preserve format qty in legacy media fallback

### Refactor

- **delivery**: adopt shared SQL runtime contracts
- **sql**: isolate batch writes from coordination
- **sql**: separate consumer lifecycle from persistence

## v0.2.0 (2026-09-04)

### Feat

- **loader**: write the canonical media column on release upsert
- **telemetry**: adopt common.telemetry and record pipeline metrics

### Fix

- **contracts**: add a local dead-letter-name adapter for the split binding
- **image**: install the otel extra in the runtime image
- **ci**: use public python libraries

## v0.1.1 (2026-08-31)

### Fix

- **ci**: accept release-boundary bump states and use Commitizen's supported files-only option

## v0.1.0 (2026-08-31)

The v0.1.0 workflow failed before publishing artifacts or images. The tag is
retained as an immutable record of that release attempt.
