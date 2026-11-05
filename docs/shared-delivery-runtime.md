# Shared delivery runtime attestation

Discogs SQL delivery lifecycle behavior is pinned to `groovemap-runtime` source revision
`87c0d75ca457bd38b14e6ce6f713098a2b4c5e26`, whose Git tree is
`bf2a2016a8d893e03788d05e1672dcdce915e3da`. It adds `BatchPolicy.max_requeues`, and a delivery that keeps failing now
dead-letters at a stated bound, which `x-delivery-limit` does not give a nacked message. It builds on
`ecde48bdc30784ee0370166463f1d1a466544f35` (tree `7ec3ac9338b25797b460bd6140c69ac188f510cd`), which
superseded `5459343ccaf46da5f71db11cda40d9338ce61d8d`
(tree `722fabc9ceab39fcfb0497b1c8a54ade84616a21`), whose batch engine let one queue's backlog starve every other queue's periodic flush
and retained each handler that crossed the batch threshold, which leaked memory until the container
restarted. A flush now returns under constant backlog, and returns `True` only when nothing is
pending, handlers still waiting in `submit` included, so `flush_queue` still gates `purge_stale_rows`
safely. `snapshot()["pending"]` counts those handlers too, so batches can be slightly smaller when
capacity is exhausted, and a shutdown `flush_all` can take one tick longer.

That revision superseded `6d0425f908e01a446b040e2993045e2afe30460e`
(tree `abec17ef8df697bdb3b0495ac6186315d69f6b56`), whose loader-local classifier and unbounded batch
holds it replaces: `PostgreSQLFailureClassifier` now comes from `common.postgres_resilient`, where only
SQLSTATE class 54 is a permanent `OperationalError`, and `BatchPolicy.max_hold_s` is set from
`POSTGRES_BATCH_MAX_HOLD_S` (default 900 s). Both `pyproject.toml` and `uv.lock` resolve that
immutable revision; the container wheel preparation guard requires the same revision.

| Consumer-owned reference | Previous published pin | Reconciled canonical pin | Evidence |
| --- | --- | --- | --- |
| Runtime API in `pyproject.toml`, `uv.lock`, wheel guard, and this attestation | `264e1d84c7165525844d3b1210179cfbf72a61f2` exported `common.delivery.DeliveryAdmission`; producer PR #21 and reviewed loader lineage `gm-discogs-sql-loader-h1t3` renamed the public type to `common.WriteAdmission` at `40d799c6ae0cd9b11b524f7477642a5c14f5f8f2`, tree `d65554a74551c653b1a8e0943fedbfa903b19015` | `6d0425f908e01a446b040e2993045e2afe30460e`, tree `abec17ef8df697bdb3b0495ac6186315d69f6b56` | Reviewed runtime handler-lifetime and bounded-drain extension; same `WriteAdmission` class, no alias or second admission implementation |

The schema producer's immutable source remains `244cb2ba40d8dd80717bcbd5315049896662c99f`;
its generated bindings and producer-owned persistence contracts are unchanged. The schema
producer retains its historical runtime reference; this loader's uv override resolves that
consumer dependency against the reviewed `WriteAdmission` runtime above.

## Held deliveries and the outage window

During PostgreSQL transient backoff, a batch delivery is held unacknowledged. Once it has been
held `POSTGRES_BATCH_MAX_HOLD_S` seconds (default 1500), the shared engine returns it with
`nack(requeue=True)`, so RabbitMQ redelivers it later. Nothing is acked or dropped by this path.

- The bound is checked at each engine flush entry, and `run_periodic` flushes every
  `POSTGRES_BATCH_FLUSH_INTERVAL` seconds. `BatchConfig` rejects any setting where
  `max_hold_s + flush_interval + backoff_max` (30 s default) reaches the 1800 s lux `consumer_timeout`.
  A write that is still blocked when the check is due can delay it further; the engine has no write timeout.
- RabbitMQ does not bound this. On 4.3.5 and 4.4.0 `nack(requeue=True)` and a consumer timeout leave
  `x-delivery-count` unset and never dead-letter. A timeout cancels the consumer, requeues the delivery
  and leaves the channel open. Only `basic.reject` and channel or connection closes spend
  `x-delivery-limit` 20, and those dead-letter after the 21st delivery.
- So the loader counts. `POSTGRES_BATCH_MAX_REQUEUES` (default 60) is read against the broker's
  `x-acquired-count`, which rises on every return, and a delivery past it is logged and dead-lettered
  instead of requeued, which also vetoes that run's stale-row purge. That is 61 holds, about **25.4
  hours** of continuous outage at 1500 s, and up to a flush interval and a backoff more per hold.
  Restarts and channel closes spend the same count, so they shorten it. The veto clears only once the
  terminal signal has committed, so a failed commit cannot redeliver it into a purge.
- Non-batch mode has its own bound: `run_delivery` dead-letters after 500 requeues, about 8.3 hours at
  `OutageBackoff`'s 60 s cap.
- `requeue_callback`, `headers` and the budget work together: without the headers `max_requeues` is
  never reached, and the shared engine warns once.
- `file_complete` and `extraction_complete` are not bounded this way. A marker that cannot drain goes
  back with `nack(requeue=True)` after `flush_queue`'s retries, and it keeps going until PostgreSQL
  answers.
- Dead-lettered records go to the DLQ, which is capped at **50,000 messages with drop-head**. Once the cap
  is reached the oldest dead letters are discarded. Under a long outage, check DLQ depth against this
  cap, because discarded dead letters cannot be replayed.

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
just test-consumer-recovery
just test-admission-integration
just image
```

`test-consumer-recovery` runs the real checker, recovery and handlers on the homelab smoke's
RabbitMQ image with a 2 s `consumer_timeout`. `test-integration` includes it, so CI runs it.

`test-admission-integration` starts its own PostgreSQL and RabbitMQ containers, disables
RabbitMQ channel-global QoS in the broker configuration, and exercises all four entity queues
under concurrent load plus consumer cancel/re-subscribe. It is separate from the default,
service-free check.

The PostgreSQL lane exercises the shared-engine batch path and the `run_delivery` non-batch path
against a disposable PostgreSQL 18 service, including outcome sets, transient recovery, poison
isolation, shutdown drain, purge veto, and settlement counts.
