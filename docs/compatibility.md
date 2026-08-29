# Compatibility identifiers

The repository's active identity is `discogs-sql-loader`. The executable, container
image, health payload, startup banner, and log component all use that name.

The following pre-split identifiers remain intentionally stable:

| Identifier | Why it remains |
| --- | --- |
| Python package `tableinator` | Preserves existing imports while the public command remains `discogs-sql-loader`. |
| `TableinatorConfig` | Preserves the internal configuration API used by the package and tests. |
| AMQP consumer key `tableinator` | Preserves durable queue, dead-letter exchange, and dead-letter queue names so an upgrade does not strand deliveries. |
| Historical `discogsography-*` issue IDs | Retained only in regression comments, regression test descriptions, and source-history provenance so the reason for data-safety fixes remains traceable. |

These names are compatibility and provenance boundaries, not product branding. New
runtime messages, documentation, images, and public interfaces should use GrooveMap and
`discogs-sql-loader`.
