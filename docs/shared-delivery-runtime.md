# Shared delivery runtime attestation

Discogs SQL delivery lifecycle behavior is pinned to `groovemap-runtime` source revision
`fb4577ecef1090d29ad3ea9f6d2a5e9e8edf90d7`, whose Git tree is
`fdd4b7f51f75e41391a98976eb2ac370bfd3154d`. Both `pyproject.toml` and `uv.lock` resolve that
immutable revision; the container wheel preparation guard requires the same revision.

`common.delivery.run_delivery` is the single settlement authority for the non-batch persistence
attempt. `common.batch.AsyncBatchEngine` owns batch capacity, per-entity serialization,
cross-entity concurrency, adaptive sizing, retry state, periodic flush, bounded drain,
cancellation restoration, and exactly-once terminal settlement.

Each RabbitMQ consumer uses per-consumer QoS. One `common.WriteAdmission` instance is created
after pool initialization and shared by direct entity writes, batch SQL writes, and stale-row
purges. It is sized to the configured pool maximum and remains alive until all handlers and
flushes have drained before pool shutdown. Batch collection and pending-queue backpressure do
not acquire a database permit.

The owner boundary remains in `tableinator`: normalization, PostgreSQL statements and result
sets, transient/deterministic classification, metrics and traces, completion control, stale-row
purging, the dead-letter purge veto, and batch/non-batch RabbitMQ QoS. A lifecycle defect shared
by consumers belongs in `common.delivery` or `common.batch`; this repository should then update
its immutable runtime pin instead of growing another queue or settlement implementation.

Verification commands:

```bash
just check
just test-integration
just test-admission-integration
just image
```

`test-admission-integration` starts its own PostgreSQL and RabbitMQ containers, disables
RabbitMQ channel-global QoS in the broker configuration, and exercises all four entity queues
under concurrent load plus consumer cancel/re-subscribe. It is separate from the default,
service-free check.

The PostgreSQL lane exercises the shared-engine batch path and the `run_delivery` non-batch path
against a disposable PostgreSQL 18 service, including outcome sets, transient recovery, poison
isolation, shutdown drain, purge veto, and settlement counts.
