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
- [Performance](performance-guide.md) — batching and PostgreSQL tuning guidance.
- [PostgreSQL pool exhaustion analysis](postgres-pool-exhaustion-analysis.md) — why
  non-batch concurrency is bounded by the pool.

Additional migrated reference material:

- [Database schema](database-schema.md)
- [Query performance optimizations](query-performance-optimizations.md)
- [Release compliance](release-compliance.md)
- [History rewrite approval gate](history-rewrite-gate.md)

Private planning records are preserved exclusively in the private `planning-archive`
repository. They are not active service documentation and must not be copied here.
