"""Write one document's `graph` vertex and edge rows on the transaction that wrote it.

`tableinator.graph_derivation` decides WHICH rows a Discogs document asserts; this module
decides how they reach PostgreSQL. It is the only place in the loader that names the
`graph` schema, and it holds the three orderings the schema reviews require:

1. **Vertices before edges.** `graph.part_of` and `graph.in_family` are views that inner
   join the vertex tables, so an edge written before its endpoints is silently absent from
   them. Every vertex INSERT runs before any edge INSERT of the same batch.
2. **Document-scoped delete, then insert.** A Discogs document is mutable: a release
   corrected from Vinyl to CD passes the hash gate, writes the CD row, and would keep the
   vinyl one forever. Each relation `DocumentGraph.replaced` names has its rows for these
   documents deleted first, which is the SQL reading of the enricher's `_prune_stale_edges`,
   `PRUNE_ISSUED_ON_CYPHER`, and `PRUNE_CREDITED_TO_CYPHER`.
3. **Source-scoped where the relation is shared.** `graph.issued_on` and
   `graph.credited_to` carry `source` in their key because `musicbrainz-sql-loader` writes
   its own rows over the same `graph.medium` and `graph.company` vertices. Every delete this
   module issues against those two is scoped to `source = 'discogs'`, so it can only ever
   reach rows this loader wrote.

Every vertex INSERT is `ON CONFLICT DO NOTHING`: `graph.medium` and `graph.media_family` are
shared with `musicbrainz-sql-loader`, and the name-keyed vertices are a vocabulary every
document re-asserts rather than a row any one document owns. Every edge INSERT is too, so a
relation written additively (`member_of`, `same_as`) converges instead of raising, and so a
batch in which two documents assert the same row writes it once.

One consequence is worth stating before the parity harness finds it. The enricher UPDATES a
vertex it matches — `SET co.name = c.name` in `MERGE_COMPANY_CYPHER`, `ON MATCH SET m.family
… m.label` in `MERGE_MEDIA_CYPHER` — where DO NOTHING keeps whatever the first write put
there. A company renamed upstream, or a medium relabelled by a newer taxonomy version, keeps
its first spelling in `graph.company.name` and in `graph.medium.family` and `.label` until
something rewrites it, while the Neo4j node moves. Only the property differs; the identity
columns are derived from the same rule on both sides and cannot drift. The cross-store parity
test in gm-discogs-sql-loader-2eg.4 will see this, and whether it becomes an ON CONFLICT DO
UPDATE or a backfill is that bead's call, not this one's — a blind DO UPDATE here would have
each provider overwrite the other's answer on the two shared vertex tables.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final

from psycopg import sql

from tableinator.graph_derivation import DISCOGS_SOURCE, EDGE_COLUMNS, VERTEX_COLUMNS, derive_document


if TYPE_CHECKING:
    from collections.abc import Sequence


__all__ = ["GRAPH_SCHEMA", "purge_document_graph", "write_document_graph"]

GRAPH_SCHEMA: Final = "graph"

# The column that names the asserting document for each relation written as a replace, and
# whether the relation is shared with another provider and therefore source-scoped. A
# relation absent here is never deleted document-scoped; `graph_derivation` explains which
# two those are and why neither can be.
_DOCUMENT_SCOPE: Final[dict[str, tuple[str, bool]]] = {
    "by_artist": ("release_id", False),
    "on_label": ("release_id", False),
    "derived_from": ("release_id", False),
    "in_genre": ("release_id", False),
    "in_style": ("release_id", False),
    "credited_on": ("release_id", False),
    "credited_to": ("release_id", True),
    "issued_on": ("release_id", True),
    "master_by_artist": ("master_id", False),
    "master_in_genre": ("master_id", False),
    "master_in_style": ("master_id", False),
    "alias_of": ("artist_id", False),
    "track_credited_on": ("release_id", False),
    "track_by_artist": ("release_id", False),
}

# The relations each entity table's purge sweeps, and the columns that reach them. A purge
# is a document-scoped delete with no insert after it, so it sweeps exactly the relations
# `_DOCUMENT_SCOPE` lets that document replace — never a relation another document asserts.
# A purged artist therefore does not take `graph.by_artist` rows with it: those belong to
# the releases that name the artist, which still name it, and the phase 0 `by_artist` body
# derives them from `public.releases` alone.
#
# `member_of` and `same_as` are swept by nothing, for the reason `graph_derivation` gives
# for never replacing them: neither has a column that names one asserting document. That
# leaves a purged artist's membership rows standing, which is what Neo4j does too — the
# enricher never deletes a MEMBER_OF relationship either.
#
# The name-keyed vertex tables are not purged at all. They are a shared vocabulary,
# `graph.medium` and `graph.media_family` are co-owned with `musicbrainz-sql-loader`, and a
# genre surviving the last release that named it costs one row.
_PURGE_SCOPE: Final[dict[str, tuple[str, ...]]] = {
    "releases": (
        "by_artist",
        "on_label",
        "derived_from",
        "in_genre",
        "in_style",
        "credited_on",
        "credited_to",
        "issued_on",
        "track_credited_on",
        "track_by_artist",
    ),
    "masters": ("master_by_artist", "master_in_genre", "master_in_style"),
    "artists": ("alias_of",),
    "labels": (),
}


def _insert(relation: str, columns: tuple[str, ...]) -> sql.Composed:
    """Return the conflict-tolerant INSERT for one graph relation."""
    return sql.SQL("INSERT INTO {relation} ({columns}) VALUES ({values}) ON CONFLICT DO NOTHING").format(
        relation=sql.Identifier(GRAPH_SCHEMA, relation),
        columns=sql.SQL(", ").join(sql.Identifier(column) for column in columns),
        values=sql.SQL(", ").join([sql.Placeholder()] * len(columns)),
    )


def _scoped_delete(relation: str, column: str, source_scoped: bool) -> sql.Composed:
    """Return the delete removing one relation's rows for a set of documents."""
    predicate = sql.SQL("{column} = ANY(%s)").format(column=sql.Identifier(column))
    if source_scoped:
        predicate = sql.SQL("{predicate} AND {source} = %s").format(predicate=predicate, source=sql.Identifier("source"))
    return sql.SQL("DELETE FROM {relation} WHERE {predicate}").format(
        relation=sql.Identifier(GRAPH_SCHEMA, relation),
        predicate=predicate,
    )


def _purge_delete(relation: str, column: str, entity_table: str, source_scoped: bool) -> sql.Composed:
    """Return the set-based delete removing one relation's rows for every stale document.

    The stale set is named by the same `updated_at` predicate the entity purge itself runs,
    joined server-side, so no deleted id is ever streamed back and buffered — a purge of a
    shrunk dump stays O(1) in the client, which is the property `discogsography-6u1o` asked
    for and the reason this is not `DELETE ... RETURNING data_id`.
    """
    predicate = sql.SQL("edge.{column} = stale.data_id").format(column=sql.Identifier(column))
    if source_scoped:
        predicate = sql.SQL("{predicate} AND edge.{source} = %s").format(predicate=predicate, source=sql.Identifier("source"))
    return sql.SQL("DELETE FROM {relation} AS edge USING {table} AS stale WHERE stale.updated_at < %s AND {predicate}").format(
        relation=sql.Identifier(GRAPH_SCHEMA, relation),
        table=sql.Identifier(entity_table),
        predicate=predicate,
    )


async def write_document_graph(
    cursor: Any,
    data_type: str,
    documents: Sequence[tuple[str, dict[str, Any]]],
) -> None:
    """Write the graph rows of DOCUMENTS on the caller's open transaction.

    The caller passes only the documents whose content hash changed, so an unchanged
    document is skipped here exactly as it is skipped in the entity table — the gate is one
    decision made once, not two that can disagree.

    Args:
        cursor: An open async cursor on the transaction that wrote the entity rows.
        data_type: One of the four contract entity tables.
        documents: `(data_id, data)` for every document whose rows are to be rewritten.
    """
    if not documents:
        return

    derived = [derive_document(data_type, data_id, data) for data_id, data in documents]

    vertex_rows: dict[str, dict[tuple[Any, ...], None]] = {}
    edge_rows: dict[str, dict[tuple[Any, ...], None]] = {}
    replaced_ids: dict[str, list[str]] = {}
    for (data_id, _data), document in zip(documents, derived, strict=True):
        for relation, rows in document.vertices.items():
            vertex_rows.setdefault(relation, {}).update(dict.fromkeys(rows))
        for relation, rows in document.edges.items():
            edge_rows.setdefault(relation, {}).update(dict.fromkeys(rows))
        for relation in document.replaced:
            replaced_ids.setdefault(relation, []).append(data_id)

    # 1. Vertices, so the views that inner join them resolve every edge written below.
    for relation in VERTEX_COLUMNS:
        pending = vertex_rows.get(relation)
        if pending:
            await cursor.executemany(_insert(relation, VERTEX_COLUMNS[relation]), list(pending))

    # 2. The document-scoped delete, before the insert that re-creates the current set.
    for relation, data_ids in replaced_ids.items():
        column, source_scoped = _DOCUMENT_SCOPE[relation]
        parameters: tuple[Any, ...] = (data_ids, DISCOGS_SOURCE) if source_scoped else (data_ids,)
        await cursor.execute(_scoped_delete(relation, column, source_scoped), parameters)

    # 3. The edges themselves.
    for relation in EDGE_COLUMNS:
        pending = edge_rows.get(relation)
        if pending:
            await cursor.executemany(_insert(relation, EDGE_COLUMNS[relation]), list(pending))


async def purge_document_graph(cursor: Any, data_type: str, started_at: Any) -> None:
    """Delete the graph rows of every document the stale-row purge is about to remove.

    Called from inside `PostgreSQLRecordPersistence.purge_stale_rows`, on its transaction and
    BEFORE the entity rows go, so the join that names the stale documents still has them to
    join against.

    Args:
        cursor: An open async cursor on the purge's transaction.
        data_type: One of the four contract entity tables.
        started_at: The extraction start the purge compares `updated_at` against.
    """
    for relation in _PURGE_SCOPE.get(data_type, ()):
        column, source_scoped = _DOCUMENT_SCOPE[relation]
        parameters: tuple[Any, ...] = (started_at, DISCOGS_SOURCE) if source_scoped else (started_at,)
        await cursor.execute(_purge_delete(relation, column, data_type, source_scoped), parameters)
