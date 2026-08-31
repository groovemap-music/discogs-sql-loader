# History rewrite approval gate

History sanitation runs only in separate backup and sanitized clones. It removes `.planning/`,
`docs/superpowers/`, `docs/specs/`, and extraction-only migration records from every reachable
commit. The private `planning-archive` repository at commit
`daf82a149aaa382b3cebbd4b43d3c82e53d4128e` preserves the source records, including the
canonical readable planning trees.

```mermaid
flowchart TD
    Candidate[Reviewed private candidate] --> Backup[Bundle and mirror backup]
    Backup --> Filter[Independent sanitized mirror]
    Filter --> Map[Commit, ref, and removed-object maps]
    Map --> Scan[Object-graph and secret scans]
    Scan --> Validate[Fresh sanitized checkout validation]
    Validate --> Gate{Explicit operator approval?}
    Gate -- No --> Stop[Private remote remains unchanged]
    Gate -- Yes --> Cutover[Force-with-lease private-remote cutover]
    Cutover --> Verify[Fresh-clone verification]
```

The operator must approve the exact old-to-new map, expected remote values, rollback backup,
and cutover before any remote rewrite. Remote drift invalidates the rehearsal. Repository
visibility, tags, releases, packages, and container publication are separate approvals.
