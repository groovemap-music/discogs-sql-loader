"""Recompute the derived `graph` relations on the `extraction_complete` latch.

`graph_writer` writes what ONE document asserts, on the transaction that wrote it. This
module writes what the WHOLE catalog asserts, once, after all four extraction_complete
signals — the same latch `graphinator` waits on before it starts its own post-import pass
(`graphinator.handle_extraction_complete`, which defers until
`extraction_complete_signals.issuperset(DATA_TYPES)` because the four fanout queues drain
at very different rates and releases finishes last).

Two things run here, in this order, and the order is load-bearing.

**1. The additive-edge reconciliation.** `member_of` and `same_as` are the two relations
`graph_writer` can only write additively: neither has a column naming one asserting
document, so no document-scoped delete can tell two asserters apart, and the enricher never
prunes MEMBER_OF or SAME_AS either. `graph_derivation.derive_artist` and `derive_release`
both record the consequence — a membership Discogs withdraws from BOTH documents survives
in the table and in Neo4j until something sweeps it, which is the one way either relation
can drift from the schema's phase 0 body. This is that sweep. It re-derives both relations
from the documents currently in `public.artists` and `public.releases` with the same
`graph_derivation` the per-document path uses, then deletes every row no current document
asserts. It runs FIRST because `artist_degree` sums both relations.

**2. The counter refresh.** Seven relations `catalog-api` reads as if they were free, each
recomputed with the body `groovemap_schema.postgres._COUNTER_BOOTSTRAP` states for it.
Those bodies are copied verbatim below rather than imported, because `database-schema` is a
DEV dependency of this loader and runtime code cannot reach it;
`tests/test_graph_counters.py` holds the copy byte-identical to the pinned original, so a
schema revision that changes a definition fails `just check` rather than drifting silently.

Where each counter comes from, and the function it mirrors:

| relation | reference |
| --- | --- |
| `genre_stats` | `graphinator.compute_genre_style_stats`, its `genre_cypher` |
| `style_stats` | `graphinator.compute_genre_style_stats`, its `style_cypher` |
| `label_stats` | `graphinator.compute_genre_style_stats`, its `label_cypher` |
| `artist_degree` | no graphinator pass writes it; it is `COUNT { (a)--() }`, `rarity_queries._ARTIST_DEGREE_QUERY`, counted once instead of per request |
| `release_degree_base` | the catalog half of `COUNT { (r)--() }`, `rarity_queries._DEGREE_QUERY` |
| `artist_genre`, `label_genre` | no Neo4j counterpart at all; both are pair aggregates the schema adds |

Three properties the schema reviews require, and where each one lives:

- **Non-null zeros.** Every count column is `bigint NOT NULL DEFAULT 0` and every body
  produces a zero rather than a NULL where graphinator writes an explicit zero: the
  `genre_stats`/`style_stats` correlated subqueries are `count(...)`, which is 0 over no
  rows, and `label_stats` LEFT JOINs so a label with no artists counts 0. A Genre no
  release names still gets a row, with `release_count` 0, because the body drives off
  `graph.genre`. `first_year` is the one nullable column and stays NULL when unknown —
  `min(...)` over no qualifying release — matching `min(r.year)` over an empty match.
- **`release_degree_base` is the loader's half only.** `COUNT { (r)--() }` in Neo4j counts
  COLLECTED and WANTS, which `catalog-api` writes and this loader never sees. The body here
  unions only the eight catalog edge tables; `graph.release_degree` adds the live
  collection and wantlist counts on read.
- **`label_stats.release_count` does not fan out.** It is `count(DISTINCT
  on_label.release_id)` over a LEFT JOIN to `by_artist` and `in_genre`, so a release with
  three artists and two genres counts once, as `count(DISTINCT r)` does in `label_cypher`.

Two costs are stated rather than hidden. `TRUNCATE` takes ACCESS EXCLUSIVE, so a reader of
one of these seven tables waits for the whole transaction; that is the schema's own choice
in `graph.bootstrap_fill` and for the same reason — `ON CONFLICT DO NOTHING` converges
upward only, so a row the documents no longer justify would survive every re-run. And the
whole pass is inline on the `extraction_complete` delivery, beside the stale-row purge it
already runs, so a dump-scale pass shares that delivery's ack budget; `graphinator` acks
first and detaches for exactly this reason (discogsography-zjja), and doing the same here
is a decision for whoever measures a full dump.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Final, LiteralString

from psycopg import sql

from tableinator.graph_derivation import EDGE_COLUMNS, derive_document


if TYPE_CHECKING:
    from collections.abc import Sequence


__all__ = [
    "ADDITIVE_RELATIONS",
    "COUNTER_BODIES",
    "COUNTER_COLUMNS",
    "GRAPH_SCHEMA",
    "REFRESH_ORDER",
    "reconcile_additive_edges",
    "refresh_counter_relations",
    "refresh_derived_relations",
]

GRAPH_SCHEMA: Final = "graph"

# How many documents one reconciliation round trip reads. The whole point of paging is that
# the asserted set is accumulated in PostgreSQL rather than in this process, so this bounds
# memory at one page of documents regardless of catalog size.
DOCUMENT_PAGE_SIZE: Final = 2000

# The two relations `graph_writer` writes additively, the entity table whose documents
# assert them, and the predicate selecting the documents that could assert a row at all. A
# document the predicate rejects derives nothing for the relation, so narrowing here is
# exact rather than an approximation — and it is what keeps the sweep off the ~17M releases
# that credit nobody.
ADDITIVE_RELATIONS: Final[dict[str, tuple[str, LiteralString]]] = {
    "member_of": (
        "artists",
        "(jsonb_typeof(data -> 'members') = 'array' AND jsonb_array_length(data -> 'members') > 0)"
        " OR (jsonb_typeof(data -> 'groups') = 'array' AND jsonb_array_length(data -> 'groups') > 0)",
    ),
    "same_as": (
        "releases",
        "jsonb_typeof(data -> 'extraartists') = 'array' AND jsonb_array_length(data -> 'extraartists') > 0",
    ),
}

COUNTER_BODIES: Final[dict[str, LiteralString]] = {
    "genre_stats": """
SELECT genre.name AS name,
       (SELECT count(*) FROM graph.in_genre AS edge WHERE edge.genre_name = genre.name) AS release_count,
       (SELECT count(DISTINCT by_artist.artist_id)
          FROM graph.in_genre AS edge
          JOIN graph.by_artist AS by_artist ON by_artist.release_id = edge.release_id
         WHERE edge.genre_name = genre.name) AS artist_count,
       (SELECT count(DISTINCT on_label.label_id)
          FROM graph.in_genre AS edge
          JOIN graph.on_label AS on_label ON on_label.release_id = edge.release_id
         WHERE edge.genre_name = genre.name) AS label_count,
       (SELECT count(*) FROM graph.part_of AS part WHERE part.genre_name = genre.name) AS style_count,
       (SELECT min(NULLIF(btrim(release.year), '')::integer)
          FROM graph.in_genre AS edge
          JOIN graph.release AS release ON release.release_id = edge.release_id
         WHERE edge.genre_name = genre.name
           AND btrim(release.year) ~ '^[0-9]{4}$') AS first_year
FROM graph.genre AS genre
""",
    "style_stats": """
SELECT style.name AS name,
       (SELECT count(*) FROM graph.in_style AS edge WHERE edge.style_name = style.name) AS release_count,
       (SELECT count(DISTINCT by_artist.artist_id)
          FROM graph.in_style AS edge
          JOIN graph.by_artist AS by_artist ON by_artist.release_id = edge.release_id
         WHERE edge.style_name = style.name) AS artist_count,
       (SELECT count(DISTINCT on_label.label_id)
          FROM graph.in_style AS edge
          JOIN graph.on_label AS on_label ON on_label.release_id = edge.release_id
         WHERE edge.style_name = style.name) AS label_count,
       (SELECT count(*) FROM graph.part_of AS part WHERE part.style_name = style.name) AS genre_count,
       (SELECT min(NULLIF(btrim(release.year), '')::integer)
          FROM graph.in_style AS edge
          JOIN graph.release AS release ON release.release_id = edge.release_id
         WHERE edge.style_name = style.name
           AND btrim(release.year) ~ '^[0-9]{4}$') AS first_year
FROM graph.style AS style
""",
    "label_stats": """
SELECT on_label.label_id AS label_id,
       count(DISTINCT on_label.release_id) AS release_count,
       count(DISTINCT by_artist.artist_id) AS artist_count,
       count(DISTINCT in_genre.genre_name) AS genre_count
FROM graph.on_label AS on_label
LEFT JOIN graph.by_artist AS by_artist ON by_artist.release_id = on_label.release_id
LEFT JOIN graph.in_genre AS in_genre ON in_genre.release_id = on_label.release_id
GROUP BY on_label.label_id
""",
    "artist_degree": """
SELECT endpoint.artist_id AS artist_id, count(*) AS degree
FROM (
    SELECT artist_id FROM graph.by_artist
    UNION ALL SELECT artist_id FROM graph.master_by_artist
    UNION ALL SELECT artist_id FROM graph.same_as
    UNION ALL SELECT member_artist_id AS artist_id FROM graph.member_of
    UNION ALL SELECT group_artist_id AS artist_id FROM graph.member_of
    UNION ALL SELECT alias_artist_id AS artist_id FROM graph.alias_of
    UNION ALL SELECT artist_id FROM graph.alias_of
) AS endpoint
GROUP BY endpoint.artist_id
""",
    "release_degree_base": """
SELECT endpoint.release_id AS release_id, count(*) AS degree
FROM (
    SELECT release_id FROM graph.by_artist
    UNION ALL SELECT release_id FROM graph.on_label
    UNION ALL SELECT release_id FROM graph.in_genre
    UNION ALL SELECT release_id FROM graph.in_style
    UNION ALL SELECT release_id FROM graph.derived_from
    UNION ALL SELECT release_id FROM graph.credited_on
    UNION ALL SELECT release_id FROM graph.credited_to
    UNION ALL SELECT release_id FROM graph.issued_on
) AS endpoint
GROUP BY endpoint.release_id
""",
    "artist_genre": """
SELECT by_artist.artist_id AS artist_id,
       in_genre.genre_name AS genre_name,
       count(DISTINCT by_artist.release_id) AS release_count
FROM graph.by_artist AS by_artist
JOIN graph.in_genre AS in_genre ON in_genre.release_id = by_artist.release_id
GROUP BY by_artist.artist_id, in_genre.genre_name
""",
    "label_genre": """
SELECT on_label.label_id AS label_id,
       in_genre.genre_name AS genre_name,
       count(DISTINCT on_label.release_id) AS release_count
FROM graph.on_label AS on_label
JOIN graph.in_genre AS in_genre ON in_genre.release_id = on_label.release_id
GROUP BY on_label.label_id, in_genre.genre_name
""",
}

COUNTER_COLUMNS: Final[dict[str, tuple[str, ...]]] = {
    "genre_stats": ("name", "release_count", "artist_count", "label_count", "style_count", "first_year"),
    "style_stats": ("name", "release_count", "artist_count", "label_count", "genre_count", "first_year"),
    "label_stats": ("label_id", "release_count", "artist_count", "genre_count"),
    "artist_degree": ("artist_id", "degree"),
    "release_degree_base": ("release_id", "degree"),
    "artist_genre": ("artist_id", "genre_name", "release_count"),
    "label_genre": ("label_id", "genre_name", "release_count"),
}

# Fill order, straight off `COUNTER_BODIES`, which is the schema's own `_COUNTER_BOOTSTRAP`
# order. Every body is a sum over the edge tables, so all seven necessarily run after the
# edges are written and after the reconciliation above; among themselves the order is free,
# and keeping the schema's makes the two texts diffable.
REFRESH_ORDER: Final[tuple[str, ...]] = tuple(COUNTER_BODIES)


def _relation(relation: str) -> sql.Identifier:
    """Return one graph relation, schema-qualified."""
    return sql.Identifier(GRAPH_SCHEMA, relation)


def _asserted_table(relation: str) -> sql.Identifier:
    """Return the temporary table holding the rows the current documents assert."""
    return sql.Identifier(f"asserted_{relation}")


def _column_list(columns: tuple[str, ...]) -> sql.Composed:
    """Return COLUMNS as a comma-separated identifier list."""
    return sql.SQL(", ").join(sql.Identifier(column) for column in columns)


def _truncate(relation: str) -> sql.Composed:
    """Return the statement emptying one counter relation."""
    return sql.SQL("TRUNCATE {relation}").format(relation=_relation(relation))


def _refill(relation: str) -> sql.Composed:
    """Return the statement refilling one counter relation from its schema body."""
    columns = _column_list(COUNTER_COLUMNS[relation])
    return sql.SQL("INSERT INTO {relation} ({columns}) SELECT {columns} FROM ({body}) AS refreshed").format(
        relation=_relation(relation),
        columns=columns,
        body=sql.SQL(COUNTER_BODIES[relation]),
    )


def _create_asserted_table(relation: str) -> sql.Composed:
    """Return the temporary table one relation's asserted rows are accumulated in.

    `ON COMMIT DROP`, so the set lives exactly as long as the transaction that computes it
    and a rollback leaves nothing behind for the next borrower of this pooled connection.
    """
    definitions = sql.SQL(", ").join(sql.SQL("{column} text NOT NULL").format(column=sql.Identifier(column)) for column in EDGE_COLUMNS[relation])
    return sql.SQL("CREATE TEMPORARY TABLE {asserted} ({definitions}) ON COMMIT DROP").format(
        asserted=_asserted_table(relation),
        definitions=definitions,
    )


def _copy_asserted(relation: str) -> sql.Composed:
    """Return the COPY that streams one page of derived rows into the temporary table."""
    return sql.SQL("COPY {asserted} ({columns}) FROM STDIN").format(
        asserted=_asserted_table(relation),
        columns=_column_list(EDGE_COLUMNS[relation]),
    )


def _document_page(table: str, predicate: LiteralString) -> sql.Composed:
    """Return the keyset page reading the documents that could assert a row.

    Keyset rather than OFFSET, and rather than a server-side cursor: the loop below
    interleaves reads with COPY on the same cursor, and a page that names its own last key
    needs nothing held open between round trips.
    """
    return sql.SQL("SELECT data_id, data FROM {table} WHERE ({predicate}) AND data_id > %s ORDER BY data_id LIMIT %s").format(
        table=sql.Identifier(table),
        predicate=sql.SQL(predicate),
    )


def _insert_asserted(relation: str) -> sql.Composed:
    """Return the insert adding every asserted row the relation is missing."""
    columns = _column_list(EDGE_COLUMNS[relation])
    return sql.SQL("INSERT INTO {relation} ({columns}) SELECT DISTINCT {columns} FROM {asserted} ON CONFLICT DO NOTHING").format(
        relation=_relation(relation),
        columns=columns,
        asserted=_asserted_table(relation),
    )


def _delete_unasserted(relation: str) -> sql.Composed:
    """Return the anti-join deleting every row no current document asserts.

    Set difference in the database rather than in this process: the rows that survive are
    never streamed back, so the sweep costs one page of documents in memory no matter how
    many edges the relation holds.
    """
    predicate = sql.SQL(" AND ").join(
        sql.SQL("asserted.{column} = edge.{column}").format(column=sql.Identifier(column)) for column in EDGE_COLUMNS[relation]
    )
    return sql.SQL("DELETE FROM {relation} AS edge WHERE NOT EXISTS (SELECT 1 FROM {asserted} AS asserted WHERE {predicate})").format(
        relation=_relation(relation),
        asserted=_asserted_table(relation),
        predicate=predicate,
    )


def _asserted_rows(relation: str, data_type: str, documents: Sequence[tuple[str, Any]]) -> list[tuple[Any, ...]]:
    """Return the rows one page of documents asserts for RELATION, deduplicated.

    `derive_document` is the same entry point the per-document write path calls, which is
    the whole point: the sweep cannot decide a row is unasserted by a rule the writer does
    not also apply.
    """
    rows: dict[tuple[Any, ...], None] = {}
    for data_id, data in documents:
        if not isinstance(data, dict):
            continue
        rows.update(dict.fromkeys(derive_document(data_type, data_id, data).edges.get(relation, ())))
    return list(rows)


async def reconcile_additive_edges(cursor: Any, logger: Any) -> dict[str, tuple[int, int]]:
    """Recompute `member_of` and `same_as` from the current documents on the caller's transaction.

    Both are written additively by `graph_writer` because neither has a column naming one
    asserting document — see `graph_derivation.derive_artist` — so a membership Discogs
    withdraws from BOTH documents has no per-document delete that could remove it. This
    recomputes each relation's full asserted set from the documents present now, adds what
    is missing, and deletes what nothing asserts.

    A source table with no rows is skipped rather than treated as asserting nothing: the
    pass runs after all four extraction_complete signals, so an empty entity table is a
    failed load rather than an empty catalog, and emptying the relation on it would be the
    same mistake `purge_stale_rows` refuses to make.

    Args:
        cursor: An open async cursor on the refresh transaction.
        logger: The loader's structured logger.

    Returns:
        `(inserted, deleted)` per relation.
    """
    results: dict[str, tuple[int, int]] = {}

    for relation, (table, predicate) in ADDITIVE_RELATIONS.items():
        started = time.perf_counter()

        await cursor.execute(sql.SQL("SELECT count(*) FROM {table}").format(table=sql.Identifier(table)))
        total = await cursor.fetchone()
        if not total or not total[0]:
            logger.warning(
                f"⚠️ Skipping the {relation} reconciliation — {table} is empty",
                relation=relation,
                table=table,
            )
            results[relation] = (0, 0)
            continue

        await cursor.execute(_create_asserted_table(relation))

        page_query = _document_page(table, predicate)
        copy_statement = _copy_asserted(relation)
        asserted = 0
        last_id = ""
        while True:
            await cursor.execute(page_query, (last_id, DOCUMENT_PAGE_SIZE))
            page: list[tuple[Any, ...]] = await cursor.fetchall()
            if not page:
                break
            rows = _asserted_rows(relation, table, [(str(data_id), data) for data_id, data in page])
            if rows:
                async with cursor.copy(copy_statement) as copy:
                    for row in rows:
                        await copy.write_row(row)
                asserted += len(rows)
            last_id = str(page[-1][0])
            if len(page) < DOCUMENT_PAGE_SIZE:
                break

        # Stats before the anti-join: an unanalyzed temporary table defaults to a handful of
        # rows and the planner picks a nested loop over the whole edge table.
        await cursor.execute(sql.SQL("ANALYZE {asserted}").format(asserted=_asserted_table(relation)))

        await cursor.execute(_insert_asserted(relation))
        inserted = max(cursor.rowcount, 0)
        await cursor.execute(_delete_unasserted(relation))
        deleted = max(cursor.rowcount, 0)

        results[relation] = (inserted, deleted)
        logger.info(
            f"🔗 Reconciled graph.{relation} against the current {table}",
            relation=f"{GRAPH_SCHEMA}.{relation}",
            asserted=asserted,
            inserted=inserted,
            deleted=deleted,
            duration_seconds=round(time.perf_counter() - started, 3),
        )

    return results


async def refresh_counter_relations(cursor: Any, logger: Any) -> dict[str, int]:
    """Recompute every counter relation from the edge tables on the caller's transaction.

    Empty-then-refill rather than upsert, for the reason `graph.bootstrap_fill` gives:
    `ON CONFLICT DO NOTHING` converges upward only, so a genre whose last release was
    corrected away would keep its old count forever. Emptying first makes each relation
    exactly the projection of the edges present, which is what idempotent has to mean here.

    Args:
        cursor: An open async cursor on the refresh transaction.
        logger: The loader's structured logger.

    Returns:
        The row count written per relation.
    """
    counts: dict[str, int] = {}

    for relation in REFRESH_ORDER:
        started = time.perf_counter()
        await cursor.execute(_truncate(relation))
        await cursor.execute(_refill(relation))
        rows = max(cursor.rowcount, 0)
        counts[relation] = rows
        logger.info(
            f"📊 Refreshed graph.{relation}",
            relation=f"{GRAPH_SCHEMA}.{relation}",
            rows=rows,
            duration_seconds=round(time.perf_counter() - started, 3),
        )

    return counts


async def refresh_derived_relations(connection_pool: Any, logger: Any) -> dict[str, int]:
    """Reconcile the additive edges and recompute every counter, in one transaction.

    One transaction for both steps, because `artist_degree` sums `member_of` and `same_as`:
    a reconciliation that committed separately would leave a window in which the degrees
    count edges the sweep has already decided are gone. A failure anywhere leaves every
    relation exactly as it was, and the caller retries the whole pass.

    Args:
        connection_pool: The loader's `AsyncPostgreSQLPool`.
        logger: The loader's structured logger.

    Returns:
        The row count written per counter relation.
    """
    started = time.perf_counter()

    async with connection_pool.connection() as conn:
        # The pool hands out an AUTOCOMMIT connection, so without this every TRUNCATE below
        # would commit on its own and a failure mid-pass would leave counter tables empty.
        await conn.set_autocommit(False)
        async with conn.transaction(), conn.cursor() as cursor:
            reconciled = await reconcile_additive_edges(cursor, logger)
            counts = await refresh_counter_relations(cursor, logger)

    logger.info(
        "✅ Refreshed the derived graph relations",
        duration_seconds=round(time.perf_counter() - started, 3),
        rows=counts,
        total_rows=sum(counts.values()),
        reconciled={relation: {"inserted": inserted, "deleted": deleted} for relation, (inserted, deleted) in reconciled.items()},
    )
    return counts
