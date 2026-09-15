# Shared delivery runtime attestation

Discogs SQL delivery lifecycle behavior is pinned to `groovemap-runtime` source revision
`e372b6a7598ae31ee6578fdff39bc920bedd7136`, whose Git tree is
`5dfee55fd070deb3463e5eb54944561dc969ec40`. Both `pyproject.toml` and `uv.lock` resolve that
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
