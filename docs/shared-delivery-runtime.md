# Shared delivery runtime attestation

Discogs SQL delivery lifecycle behavior is pinned to `groovemap-runtime` source revision
`24704f5fd48d3ef4fff29398585e9924e225b0c5`, whose Git tree is
`c5b96bdeab082057480a26784ad6065497aaae9a`. Both `pyproject.toml` and `uv.lock` resolve that
immutable revision; the container wheel preparation guard requires the same revision.

`common.delivery.run_delivery` is the single settlement authority for the non-batch persistence
attempt. `common.batch.AsyncBatchEngine` owns batch capacity, per-entity serialization,
cross-entity concurrency, adaptive sizing, retry state, periodic flush, bounded drain,
cancellation restoration, and exactly-once terminal settlement.

The owner boundary remains in `tableinator`: normalization, PostgreSQL statements and result
sets, transient/deterministic classification, metrics and traces, completion control, stale-row
purging, the dead-letter purge veto, and batch/non-batch RabbitMQ QoS. A lifecycle defect shared
by consumers belongs in `common.delivery` or `common.batch`; this repository should then update
its immutable runtime pin instead of growing another queue or settlement implementation.

Verification commands:

```bash
just check
just test-integration
just image
```

The PostgreSQL lane exercises the shared-engine batch path and the `run_delivery` non-batch path
against a disposable PostgreSQL 18 service, including outcome sets, transient recovery, poison
isolation, shutdown drain, purge veto, and settlement counts.
