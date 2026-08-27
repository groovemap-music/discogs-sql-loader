# History-preserving extraction

The source was migration branch `wt/bead/issue/discogsography-2kpm.15` at
`e3e461be` in the unchanged monorepo. A disposable `--no-local` clone retained
`tableinator/`, `tests/tableinator/`, applicable PostgreSQL, resilience, completion,
performance, and query-optimization documents, and `LICENSE`; owned tests were promoted
to `tests/`.

The exact `git filter-repo` arguments were:

```text
--path tableinator/
--path tests/tableinator/
--path LICENSE
--path docs/consumer-cancellation.md
--path docs/database-resilience.md
--path docs/database-schema.md
--path docs/file-completion-tracking.md
--path docs/performance-guide.md
--path docs/postgres-pool-exhaustion-analysis.md
--path docs/query-performance-optimizations.md
--path docs/superpowers/plans/2026-03-21-query-perf-opt-v5.md
--path-rename tests/tableinator/:tests/
```

The destination `main` branch retains 198 relevant source commits and no tags. The
current tree is MIT licensed by owner decision; earlier license revisions remain in
history. The original monorepo and its refs were not rewritten or deleted.
