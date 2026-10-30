# Shared delivery runtime attestation

Discogs SQL delivery lifecycle behavior is pinned to `groovemap-runtime` source revision
`6d0425f908e01a446b040e2993045e2afe30460e`, whose Git tree is
`abec17ef8df697bdb3b0495ac6186315d69f6b56`. Both `pyproject.toml` and `uv.lock` resolve that
immutable revision; the container wheel preparation guard requires the same revision.

| Consumer-owned reference | Previous published pin | Reconciled canonical pin | Evidence |
| --- | --- | --- | --- |
| Runtime API in `pyproject.toml`, `uv.lock`, wheel guard, and this attestation | `264e1d84c7165525844d3b1210179cfbf72a61f2` exported `common.delivery.DeliveryAdmission`; producer PR #21 and reviewed loader lineage `gm-discogs-sql-loader-h1t3` renamed the public type to `common.WriteAdmission` at `40d799c6ae0cd9b11b524f7477642a5c14f5f8f2`, tree `d65554a74551c653b1a8e0943fedbfa903b19015` | `6d0425f908e01a446b040e2993045e2afe30460e`, tree `abec17ef8df697bdb3b0495ac6186315d69f6b56` | Reviewed runtime handler-lifetime and bounded-drain extension; same `WriteAdmission` class, no alias or second admission implementation |

The schema producer's immutable source remains `244cb2ba40d8dd80717bcbd5315049896662c99f`;
its generated bindings and producer-owned persistence contracts are unchanged. The schema
producer retains its historical runtime reference; this loader's uv override resolves that
consumer dependency against the reviewed `WriteAdmission` runtime above.

`common.delivery.run_delivery` is the single settlement authority for the non-batch persistence
attempt. `common.batch.AsyncBatchEngine` owns batch capacity, per-entity serialization,
cross-entity concurrency, adaptive sizing, retry state, periodic flush, bounded drain,
cancellation restoration, and exactly-once terminal settlement.

Each RabbitMQ consumer uses per-consumer QoS. One `common.WriteAdmission` instance is created
after pool initialization and shared by direct entity writes, batch SQL writes, stale-row
purges, and extraction completion writes. Inline refresh mode budgets the configured pool
maximum. Durable refresh mode reserves three connections for its refresh transaction, lease
heartbeat, and health sampler, then budgets the remaining pool connections for writers. Startup
fails closed unless at least one writer connection remains. `handler()` tracks each callback
without using a SQL permit; `slot()` bounds actual SQL and tracks waiters. After stopping
consumers and background producers and confirming broker closure, shutdown calls `drain()` with
a finite timeout before pool shutdown. A timed-out drain retains the pool and budget. Batch
collection and pending-queue backpressure do not acquire a database permit.

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
