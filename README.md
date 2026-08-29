# GrooveMap Discogs SQL loader

Consumes versioned Discogs catalog events and builds PostgreSQL tables for structured
queries, analysis, and cross-catalog enrichment.

## Development

This service consumes the private `groovemap-runtime` package. Local setup requires read
access to `groovemap-music/python-libraries`; the lockfile records the reviewed revision.

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

- Catalog-event contract: v1, promoted byte-for-byte from `catalog-ingestion`.
- Persistence compatibility: v1, promoted from `database-schema`.

`just source-check` verifies both promoted files and the generated Python binding by SHA-256.
There are no cross-repository relative imports or generated writes.

## Release and license

This repository versions one service wheel and container image. Commitizen reads the PEP 621
version and uses annotated `v$version` tags. Dry runs do not tag, push, publish, or release.

The current tree is MIT licensed. Historical revisions retain their then-applicable license.

## Documentation

See the [documentation index](docs/README.md) and the
[tableinator reference](tableinator/README.md).
