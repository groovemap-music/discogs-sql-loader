# Query performance ownership

`discogs-sql-loader` does not serve catalog queries and does not write Neo4j. The
historical endpoint and Cypher optimization report previously kept here described
other services and has been removed from this repository's active guidance.

Use the current owning documentation instead:

- [`catalog-api` query performance decisions](https://github.com/groovemap-music/catalog-api/blob/main/docs/query-performance-optimizations.md)
  for SQL and Cypher executed by HTTP endpoints.
- [`database-schema` PostgreSQL and Neo4j definitions](https://github.com/groovemap-music/database-schema)
  for indexes and constraints.
- [`discogs-graph-enricher`](https://github.com/groovemap-music/discogs-graph-enricher)
  for Discogs graph projection and write behavior.
- [`deployment` observability](https://github.com/groovemap-music/deployment/blob/main/docs/observability.md)
  for measuring the released stack.

The only query behavior owned here is the bounded PostgreSQL read/write work needed
for idempotent upserts, media backfills, and guarded stale-row cleanup. See
[Discogs SQL loader performance](performance-guide.md) and
[PostgreSQL persistence boundary](database-schema.md).
