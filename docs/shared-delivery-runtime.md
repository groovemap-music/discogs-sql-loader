# Shared delivery runtime attestation

Discogs SQL delivery lifecycle behavior is pinned to `groovemap-runtime` source revision
`264e1d84c7165525844d3b1210179cfbf72a61f2`, whose Git tree is
`8fdb428da1857dd064d947d38ed61aa185a68ebd`. Both `pyproject.toml` and `uv.lock` resolve that
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

Non-batch persistence and stale-row purges share one `common.delivery.DeliveryAdmission`
budget at the configured PostgreSQL pool maximum. Admission happens before connection
acquisition and returns its slot on cancellation or failure. Broker reconnects retain the
same budget while old writers drain. Startup and recovery use per-consumer QoS; batch-fill
prefetch and the batch engine's own concurrency policy remain unchanged.
