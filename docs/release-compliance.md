# Release compliance

`just check` validates formatting, linting, types, tests and coverage, promoted contracts,
immutable automation, package construction and installation, MIT metadata, secret scans, and
version consistency. `just audit` performs the network-backed Python vulnerability audit.

`just image` builds `discogs-sql-loader:local`, verifies its installed service import, and checks
its numeric non-root runtime identity. `just release-dry-run` produces local checksums, an SBOM,
third-party notices, and provenance without creating a tag, upload, release, or repository
setting.

Ordinary and Dependabot-authored pull requests run the same required CI job graph. Releases are
tag-only, and no Renovate workflow is active. Publication additionally requires sanitized
reachable history, a reviewed green commit, successful hosted CI, and explicit operator
approval. Visibility, tags, packages, images, and releases remain separate gates.
