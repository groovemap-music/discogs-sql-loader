"""Recompute the derived `graph` relations on the `extraction_complete` latch.

`graph_writer` writes what ONE document asserts, on the transaction that wrote it. This
module writes what the WHOLE catalog asserts, once, after all four extraction_complete
signals — the same latch `graphinator` waits on before it starts its own post-import pass
(`graphinator.handle_extraction_complete`, which defers until
`extraction_complete_signals.issuperset(DATA_TYPES)` because the four fanout queues drain
at very different rates and releases finishes last).

Four things run here, in this order, and the order is load-bearing.

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

**3. The cross-provenance MEMBER_OF refresh.** `graph.refresh_artist_member_of()` rebuilds
the path functions' artist-to-artist relation from the reconciled Discogs membership and
the MusicBrainz relationship projection. It follows the counters because the schema's
documented refresh contract assigns both path relations to this completed-extraction pass.

**4. The path degree refresh.** `graph.refresh_vertex_degree()` must run after the MEMBER_OF
refresh because that union is one of the ten relations it sums. The extraction latch is
stamped only after both functions succeed, on the same transaction as the reconciliation
and counters, so any failure rolls the entire pass back. Inline mode nacks the delivery;
the durable worker instead retains a retryable job.

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
  rows, and `label_stats` now drives from `graph.label` with the same shape, so labels with
  no releases also get explicit zero rows. `first_year` is the one nullable column and
  stays NULL when unknown — `min(...)` over no qualifying positive decimal year — matching
  `min(r.year)` over an empty match.
- **`release_degree_base` is the loader's half only.** `COUNT { (r)--() }` in Neo4j counts
  COLLECTED and WANTS, which `catalog-api` writes and this loader never sees. The body here
  unions only the eight catalog edge tables; `graph.release_degree` adds the live
  collection and wantlist counts on read.
- **`label_stats.release_count` does not fan out.** Each aggregate is a correlated
  `count(DISTINCT ...)` over one label's releases, so a release with three artists and two
  genres counts once, as `count(DISTINCT r)` does in `label_cypher`.
- **Genre/style counters use release co-occurrence, not taxonomy.** `style_count` and
  `genre_count` join the normalized `in_genre` and `in_style` edges by release, matching
  graphinator even when a release has several genres. `graph.part_of` deliberately keeps
  its stricter single-genre taxonomy rule; the counter fix does not broaden that relation.
- **Years mirror the Cypher predicate.** Any decimal value greater than zero participates
  in `first_year`, including positive three-digit years; `0000` is rejected. Plausibility
  bounds remain the importer's normalization responsibility rather than a counter rule.

Two costs are stated rather than hidden. `TRUNCATE` takes ACCESS EXCLUSIVE, so a reader of
one of these seven tables waits for the whole transaction; that is the schema's own choice
in `graph.bootstrap_fill` and for the same reason — `ON CONFLICT DO NOTHING` converges
upward only, so a row the documents no longer justify would survive every re-run.

Historically the whole pass ran inline on the still-unacked `extraction_complete`
delivery, beside the stale-row purge, so a dump-scale pass spent that delivery's
ack budget. The default durable path now schedules it transactionally before ack
and supplies the commit fence through `before_refresh`/`before_commit` below;
`DERIVED_REFRESH_MODE=inline` retains the old rollback behavior.
`graphinator` acks the trigger BEFORE its own post-import maintenance starts and runs it
detached with its own retry, because holding the delivery across unbounded work trips
RabbitMQ's 30-minute consumer ack timeout, which closes the SHARED channel with
PRECONDITION_FAILED: all four consumers die, the signal is redelivered, the sweep restarts
from scratch, and after `x-delivery-limit=20` redeliveries the trigger is dead-lettered and
maintenance never completes at all (discogsography-zjja). The latch and schema-owned
job in `tableinator.durable_refresh` make the worker restart-safe; the measurement
and cutover gates are in `docs/derived-refresh-ack-budget.md`.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Final, LiteralString

from psycopg import sql

from tableinator.extraction_latch import LatchRelation, mark_extraction_refreshed
from tableinator.graph_derivation import EDGE_COLUMNS, derive_document


if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence


__all__ = [
    "ADDITIVE_RELATIONS",
    "COUNTER_BODIES",
    "COUNTER_COLUMNS",
    "GRAPH_SCHEMA",
    "PATH_REFRESH_FUNCTIONS",
    "PATH_REFRESH_RELATIONS",
    "REFRESH_ORDER",
    "reconcile_additive_edges",
    "refresh_counter_relations",
    "refresh_derived_relations",
]

GRAPH_SCHEMA: Final = "graph"

# The schema-owned path refresh functions, in their required dependency order. The degree
# relation sums artist_member_of, so reversing these would publish a degree snapshot of the
# previous union. Both calls remain inside refresh_derived_relations' transaction.
PATH_REFRESH_FUNCTIONS: Final = ("refresh_artist_member_of", "refresh_vertex_degree")
PATH_REFRESH_RELATIONS: Final = ("artist_member_of", "vertex_degree")

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
       (SELECT count(DISTINCT in_style.style_name)
          FROM graph.in_genre AS in_genre
          JOIN graph.in_style AS in_style ON in_style.release_id = in_genre.release_id
         WHERE in_genre.genre_name = genre.name) AS style_count,
       (SELECT min(btrim(release.year)::integer)
          FROM graph.in_genre AS edge
          JOIN graph.release AS release ON release.release_id = edge.release_id
         WHERE edge.genre_name = genre.name
           AND btrim(release.year) ~ '^[0-9]+$'
           AND btrim(release.year)::numeric > 0) AS first_year
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
       (SELECT count(DISTINCT in_genre.genre_name)
          FROM graph.in_style AS in_style
          JOIN graph.in_genre AS in_genre ON in_genre.release_id = in_style.release_id
         WHERE in_style.style_name = style.name) AS genre_count,
       (SELECT min(btrim(release.year)::integer)
          FROM graph.in_style AS edge
          JOIN graph.release AS release ON release.release_id = edge.release_id
         WHERE edge.style_name = style.name
           AND btrim(release.year) ~ '^[0-9]+$'
           AND btrim(release.year)::numeric > 0) AS first_year
FROM graph.style AS style
""",
    "label_stats": """
SELECT label.label_id AS label_id,
       (SELECT count(DISTINCT on_label.release_id)
          FROM graph.on_label AS on_label
         WHERE on_label.label_id = label.label_id) AS release_count,
       (SELECT count(DISTINCT by_artist.artist_id)
          FROM graph.on_label AS on_label
          JOIN graph.by_artist AS by_artist ON by_artist.release_id = on_label.release_id
         WHERE on_label.label_id = label.label_id) AS artist_count,
       (SELECT count(DISTINCT in_genre.genre_name)
          FROM graph.on_label AS on_label
          JOIN graph.in_genre AS in_genre ON in_genre.release_id = on_label.release_id
         WHERE on_label.label_id = label.label_id) AS genre_count
FROM graph.label AS label
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

# Fill order, straight off `COUNTER_BODIES`, preserving the order of these seven entries in
# the schema's `_COUNTER_BOOTSTRAP`. Its eighth entry is vertex_degree, refreshed through the
# schema-owned function below rather than copied here. Every body is a sum over edge tables,
# so all seven necessarily run after the reconciliation; among themselves the order is free.
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


async def refresh_derived_relations(
    connection_pool: Any,
    logger: Any,
    version: str,
    latch: LatchRelation | None = None,
    *,
    before_refresh: Callable[[Any], Awaitable[None]] | None = None,
    before_commit: Callable[[Any], Awaitable[None]] | None = None,
) -> dict[str, int]:
    """Reconcile edges and recompute counters and path relations, in one transaction.

    One transaction for every step, because `artist_degree` sums `member_of` and
    `same_as`: a reconciliation that committed separately would leave a window in which the
    degrees count edges the sweep has already decided are gone. The path MEMBER_OF union is
    rebuilt after the counters, then vertex_degree after that union because it sums it. The
    extraction is stamped refreshed on the same transaction, so a pass that rolls back leaves
    it unstamped and the next delivery of any of its four signals runs it again. A failure
    anywhere leaves every relation exactly as it was, and the caller retries the whole pass.

    Args:
        connection_pool: The loader's `AsyncPostgreSQLPool`.
        logger: The loader's structured logger.
        version: The extraction this pass is running for, from `extraction_latch_key`.
        latch: The declared latch relation to stamp, or None for a caller driving the pass
            directly rather than off a signal.
        before_refresh: Optional transaction-scoped serialization and early fence for a
            durable worker. Runs before any relation is changed.
        before_commit: Optional final fence and atomic job/latch completion. Runs after
            every relation refresh but before this transaction commits.

    Returns:
        The row count written per counter relation.
    """
    started = time.perf_counter()

    async with connection_pool.connection() as conn:
        # The pool hands out an AUTOCOMMIT connection, so without this every TRUNCATE below
        # would commit on its own and a failure mid-pass would leave counter tables empty.
        await conn.set_autocommit(False)
        async with conn.transaction(), conn.cursor() as cursor:
            if before_refresh is not None:
                await before_refresh(cursor)
            reconciled = await reconcile_additive_edges(cursor, logger)
            counts = await refresh_counter_relations(cursor, logger)
            for function in PATH_REFRESH_FUNCTIONS:
                function_started = time.perf_counter()
                await cursor.execute(f"SELECT * FROM {GRAPH_SCHEMA}.{function}()")  # noqa: S608
                logger.info(
                    f"🧭 Refreshed graph.{function}",
                    function=f"{GRAPH_SCHEMA}.{function}",
                    duration_seconds=round(time.perf_counter() - function_started, 3),
                )
            if before_commit is not None:
                await before_commit(cursor)
            if latch is not None:
                await mark_extraction_refreshed(cursor, latch, version)

    logger.info(
        "✅ Refreshed the derived graph relations",
        version=version,
        duration_seconds=round(time.perf_counter() - started, 3),
        rows=counts,
        total_rows=sum(counts.values()),
        reconciled={relation: {"inserted": inserted, "deleted": deleted} for relation, (inserted, deleted) in reconciled.items()},
    )
    return counts
