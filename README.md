# GrooveMap Discogs SQL loader

Consumes versioned Discogs catalog events and builds PostgreSQL tables for structured
queries, analysis, and cross-catalog enrichment.

## Development

This service consumes the private `groovemap-runtime` package at immutable commit
`28fa329702bc76896cc54ab8d05ec5b1bd3d929e`. Local setup requires read access to
`groovemap-music/python-libraries` through the operator's normal Git credential helper.

```bash
mise install
just setup
just check
just image
```

`just check` is credential-free and uses mocked PostgreSQL/RabbitMQ boundaries. Live
integration, load, and deployment checks remain separate. See
[tableinator/README.md](tableinator/README.md) for configuration and behavior.

The source-check workflow can run with the repository-scoped GitHub token. Full dependency
installation and tests remain operator-local until a narrowly installed GitHub App can mint
a short-lived token that reads the private Python libraries repository; no cross-repository
PAT is accepted.

## Contracts

- Catalog-event contract: v1, promoted byte-for-byte from immutable
  `catalog-ingestion` commit `e7038d1492da54e91444bfa990598e8963972ce2`.
- Persistence compatibility: v1, promoted from immutable `database-schema` commit
  `4622bfeb4cd9c9553cbf640bb96c1e80b2cba710`.

`just source-check` verifies both promoted files and the generated Python binding by SHA-256.
There are no cross-repository relative imports or generated writes.

## Release and license

This repository versions one service wheel and container image. Commitizen reads the PEP 621
version and uses annotated `v$version` tags. Dry runs do not tag, push, publish, or release.

The current tree is MIT licensed. Historical revisions retain their then-applicable license.
