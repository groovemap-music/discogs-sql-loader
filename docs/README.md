# Discogs SQL loader documentation

Start with these repository-specific guides:

- [Operations](operations.md) — inputs, PostgreSQL output, configuration, health,
  restart and completion behavior, validation, and troubleshooting.
- [Compatibility identifiers](compatibility.md) — names retained to preserve Python
  imports, durable AMQP queues, and regression provenance.
- [Consumer cancellation and draining](consumer-cancellation.md) — how terminal
  messages stop deliveries without losing accepted work.
- [File completion tracking](file-completion-tracking.md) — file and extraction
  completion semantics.
- [Database resilience](database-resilience.md) — outage and recovery behavior.
- [Shared delivery runtime](shared-delivery-runtime.md) — immutable runtime revision,
  owner/runtime boundary, and migration attestation.
- [Performance](performance-guide.md) — batching and PostgreSQL tuning within this
  consumer.
- [PostgreSQL pool protection](postgres-pool-exhaustion-analysis.md) — how batch and
  non-batch modes bound connection demand.
- [PostgreSQL persistence boundary](database-schema.md) — tables and write invariants
  this consumer relies on, with links to the owning schema repository.
- [Cross-store parity](store-parity.md) — the opt-in lane that holds this loader's graph
  to the graph enricher's, and the differences on record between them.
- [Query performance ownership](query-performance-optimizations.md) — pointers to the
  API, graph, schema, and deployment owners.

Additional repository governance:

- [Release compliance](release-compliance.md)
- [History rewrite approval gate](history-rewrite-gate.md)

Private planning records are preserved exclusively in the private `planning-archive`
repository. They are not active service documentation and must not be copied here.
