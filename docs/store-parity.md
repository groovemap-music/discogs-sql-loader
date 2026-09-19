# Cross-store parity with the graph enricher

Phase 4 of the property-graph migration retires `discogs-graph-enricher` once this loader
writes the same graph into PostgreSQL that the enricher writes into Neo4j. That is a claim
about two stores, and it is demonstrated rather than assumed by one opt-in lane:

```bash
just test-parity
```

The lane starts a disposable PostgreSQL 18 and a disposable Neo4j 2026 community, both
pinned by digest to the images `database-schema`, `discogs-graph-enricher`, and
`catalog-api` pin, plays one sequence of fixture events through **both** services, and then
compares the two graphs relation by relation. It lives in
`tests/integration/test_store_parity.py` and `scripts/test-parity.sh`.

It is outside `just check` and outside `just test-integration`. It is the only lane in this
repository that needs a second engine, so it carries its own `parity` marker and
`just test-integration` deselects it.

## How both sides are driven

`scripts/test-parity.sh` installs the enricher at a pinned revision and passes that
revision to the suite, which holds the installed distribution to it — so a parity run states
exactly which enricher it proved the loader equal to, and a stale dev environment fails
instead of quietly proving the loader equal to something else. Both sides go through their
batch write path, on the same documents, in the same order, with the same `sha256` on each:

| side | entry point |
| --- | --- |
| this loader | `PostgreSQLBatchWriter.process_batch` |
| the enricher | `Neo4jBatchProcessor.flush_queue` |

Neither side sees a broker and neither re-normalizes the payload, so the same `dict` content
reaches both stores and a divergence can only come from the projections themselves.

A container would have needed RabbitMQ and an image build to deliver the same events, and
would still have been the same Python projecting the same documents. Importing the enricher
keeps the fixture the single source of both stores' input.

The enricher is installed by the lane rather than declared in `pyproject.toml`, and with
`--no-deps`. It pins its own `groovemap-runtime` revision, which is a hard URL conflict with
the one the persistence contract pins here, and resolving that conflict in the project's own
dependency graph would reach every other lane — including `install-check`, which installs
the locally built runtime wheel and must not be redirected anywhere. Installing the enricher
alone leaves this repository's runtime in place, which is the answer parity wants: both
stores have to derive media and credit roles from one `common.media` and one
`common.credit_roles`, or the comparison would be measuring a runtime skew instead of the
loader. Every other dependency the enricher has is already in the dev environment, and
`uv sync` removes the package again, so nothing leaks into another lane.

Because the enricher is not a declared dependency, every other lane collects the suite
without it and skips at import with a message naming `just test-parity`.

## What is compared

`RELATION_MAPPING` in the suite is ADR 0012's label mapping made executable: each Neo4j
relationship type paired with the `graph` relation it becomes, the endpoint labels that
split one Neo4j type into two relations, and the properties that travel with the edge.
Seventeen edge labels are covered — `by_artist` and `master_by_artist` from `BY`, `in_genre`
/ `in_style` / `master_in_genre` / `master_in_style` from `IS`, `on_label`, `derived_from`,
`member_of`, `alias_of`, `same_as`, `sublabel_of`, `part_of`, `in_family`, `issued_on`,
`credited_to`, and `credited_on`, whose Neo4j `category` property is the GENERATED
`role_category` column here. `part_of`, `in_family`, and `sublabel_of` are views at the
pinned schema revision, which changes nothing about what they must contain.

`VERTEX_MAPPING` covers the six vertices each store mints from document content: genre,
style, and person names; media family names; medium ids with their family and label; and
company ids under the casefold rule, with their name.

The four entity vertices — artist, label, master, release — are deliberately outside the
comparison. Neo4j MERGEs a stub node for every id a document *references*, so the enricher
holds an `Artist` for a band member whose own document has not arrived yet, while
`graph.artist` holds only documents the loader actually loaded. That is a difference in when
a vertex appears, not in what the graph asserts, and every edge naming such an id is
compared in full regardless.

## The fixture

Two rounds of events over one small catalog, small enough to reason about by hand:

- a band stated from both ends, so the reciprocal membership pair has to collapse to one
  edge in both stores;
- a release naming three artists and two genres, which is also the shape that asserts no
  `part_of` because nothing in the document says which genre a style sits under;
- a master with one genre and two styles, which is the shape that does;
- a label stating its hierarchy from both ends;
- a person credited twice under two roles, once by id so `same_as` is minted;
- a company with a Discogs id beside one without, so both identity rules are exercised;
- a canonical media block naming a medium the vendored taxonomy does not carry, beside a
  pre-cutover release whose media are derived from raw `formats`;
- the Discogs "no entity" sentinel id `0`, stated as element text.

Round two re-states the band and one release with corrected content — an artist and a genre
dropped, a credit removed, a company renamed, a medium moved to another family, an alias
withdrawn. Pruning behaviour cannot be compared any other way: a store that never prunes
looks identical to one that does until something is withdrawn.

## The expected-differences registry

`EXPECTED_DIFFERENCES` started empty. An entry is added only when a divergence actually
materialises on this fixture *and* has a reason that is a decision rather than a bug. It is
a plain mapping keyed by relation, and the suite holds it in both directions: a relation
outside the registry must have identical sets, and a relation inside it must diverge exactly
as declared, on the same side, with the same tuples. A declared difference that stops
materialising fails the run rather than sitting there absolving a relation nobody is
checking any more.

Six entries are on record. Each reason is stated in full in the registry itself.

| relation | shape of the difference | cause |
| --- | --- | --- |
| `by_artist` | Neo4j holds an edge to an `Artist` with id `0` | The loader drops the Discogs "no entity" sentinel, because that is what `_usable_id` and the phase 0 views assert. The enricher tests raw truthiness, which drops a numeric `0` but keeps the string `"0"` the dump states. |
| `alias_of` | Neo4j keeps a withdrawn alias | The loader replaces `alias_of` per asserting document. `process_artist` has no `ALIAS_OF` prune. |
| `credited_on` | Neo4j keeps two superseded credits | The loader replaces `credited_on` per release. The enricher MERGEs `CREDITED_ON` with no matching prune. |
| `in_family` | Neo4j keeps an edge to the family the medium left | The view is a projection of `graph.medium.family`, so a medium has exactly one family. `IN_FAMILY` is MERGEd and never pruned. |
| `medium` | the family property differs | Every vertex INSERT is `ON CONFLICT DO NOTHING`; `MERGE_MEDIA_CYPHER` has `ON MATCH SET m.family`. |
| `company` | the name property differs | The same, against `MERGE_COMPANY_CYPHER`'s `SET co.name`. |

The last two are property drift on vertex tables that are co-owned with
`musicbrainz-sql-loader`, where a blind `ON CONFLICT DO UPDATE` would have each provider
overwrite the other's answer. Whether the fix is a scoped `DO UPDATE` or a backfill is not
decided here; the identity columns are derived from the same rule on both sides and cannot
drift, and every edge naming those vertices is identical.

Two divergences the edge-writer review predicted did **not** materialise, and so have no
entry: `member_of` and `same_as` are additive in *both* stores, because neither has a column
naming one asserting document, so the membership the fixture withdraws and the credit it
removes survive on both sides. The suite asserts that agreement explicitly rather than
leaving it to the absence of an entry.
