"""The graph rows a run of fixture events leaves in a real PostgreSQL.

`tests/test_graph_derivation.py` holds each rule to the enricher function it mirrors
without a database. This suite is the other half: it plays a small but complete catalog
through both write paths against the promoted schema's own tables and asserts the row
count of every relation the loader owns, so a rule that derives the right tuples but
writes them to the wrong relation — or writes an edge the property graph cannot resolve
because its vertex is missing — fails here rather than in production.
"""

import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import psycopg
import pytest
import pytest_asyncio

from tableinator.batch_writer import PostgreSQLBatchWriter
from tableinator.graph_derivation import EDGE_COLUMNS, VERTEX_COLUMNS
from tableinator.media import media_for_release
from tableinator.record_persistence import PostgreSQLRecordPersistence


if TYPE_CHECKING:
    from collections.abc import AsyncIterator


pytestmark = pytest.mark.integration

ENTITY_TABLES = ("artists", "labels", "masters", "releases")


@dataclass(frozen=True)
class BatchRecord:
    """One flushed message, in the shape `PostgreSQLBatchWriter` reads."""

    data_id: str
    data: dict[str, Any]
    sha256: str


class SingleConnectionPool:
    """Expose one test connection through the production pool protocol.

    `common.AsyncPostgreSQLPool` hands out an AUTOCOMMIT connection and restores autocommit
    when the caller gives it back, precisely so a caller that opened its own transaction
    cannot poison the next borrower. Both write paths and the stale-row purge rely on that,
    so the double has to do it too or a test would see a connection production never hands
    out.
    """

    def __init__(self, connection: psycopg.AsyncConnection[Any]) -> None:
        self._connection = connection

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[psycopg.AsyncConnection[Any]]:
        try:
            yield self._connection
        finally:
            if not self._connection.autocommit:
                await self._connection.set_autocommit(True)


# ── The fixture catalog ──────────────────────────────────────────────────────
# Small enough to count by hand, wide enough to reach every rule: a band stated from both
# ends, a release naming three artists and two genres, one label named twice under two
# catalogue numbers, a person credited twice under two roles, a company with a Discogs id
# beside one without, and two releases on different media.

ARTISTS = [
    ("gw-a1", {"id": "gw-a1", "name": "The Band", "members": [{"id": "gw-a2"}, {"id": "gw-a3"}], "aliases": [{"id": "gw-a4"}]}),
    # Reciprocal: `gw-a1` already named this artist a member, so the pair is one edge.
    ("gw-a2", {"id": "gw-a2", "name": "First Member", "groups": [{"id": "gw-a1"}]}),
    ("gw-a3", {"id": "gw-a3", "name": "Second Member", "groups": [{"id": "gw-a1"}]}),
]

# A label states its hierarchy from both ends, and `graph.sublabel_of` is a VIEW at the
# pinned schema revision, so this document must leave no table row anywhere.
LABELS = [("gw-l1", {"id": "gw-l1", "name": "A Label", "parentLabel": {"id": "gw-l2"}, "sublabels": [{"id": "gw-l3"}]})]

MASTERS = [
    (
        "gw-m1",
        {
            "id": "gw-m1",
            "title": "A Master",
            "artists": [{"id": "gw-a1"}],
            "genres": ["Rock"],
            "styles": ["Pop Rock", "Psychedelic Rock"],
        },
    )
]

RELEASES = [
    (
        "gw-r1",
        {
            "id": "gw-r1",
            "title": "A Release",
            "master_id": "gw-m1",
            "artists": [{"id": "gw-a1"}, {"id": "gw-a2"}, {"id": "gw-a3"}],
            # One label, named once per catalogue number: one `on_label` row.
            "labels": [{"id": "gw-l1", "catno": "AL-001"}, {"id": "gw-l1", "catno": "AL-002"}],
            "genres": ["Rock", "Pop"],
            "styles": ["Pop Rock"],
            "extraartists": [
                {"id": "gw-a2", "name": "First Member", "role": "Producer"},
                {"name": "A Producer", "role": "Producer"},
                {"name": "A Producer", "role": "Mastered By"},
                {"name": "No Role Here"},
            ],
            "companies": {
                "items": [
                    {"discogs_id": "4321", "name": "A Pressing Plant", "role": "Pressed By", "role_category": "manufacture"},
                    {"name": "A Cutting Room", "role": "Lacquer Cut By"},
                ]
            },
            "formats": [{"name": "Vinyl", "qty": "2", "descriptions": {"description": ["LP", "Album"]}}],
            # `discogs-ingestion` never recurses into `tracklist`, so a single track and its
            # single sub-track credit keep the raw xmltodict wrapper: a bare object under
            # `track`/`artist`, not a one-element array.
            "tracklist": {
                "track": {
                    "position": "A1",
                    "extraartists": {"artist": {"id": "gw-a4", "name": "Track Engineer", "role": "Engineer"}},
                    "artists": {"artist": {"id": "gw-a5"}},
                    "sub_tracks": {"track": [{"position": "A1a", "extraartists": {"artist": {"name": "Medley Producer", "role": "Producer"}}}]},
                }
            },
        },
    ),
    (
        "gw-r2",
        {
            "id": "gw-r2",
            "title": "Another Release",
            "artists": [{"id": "gw-a1"}],
            "labels": [{"id": "gw-l1", "catno": "AL-003"}],
            "genres": ["Rock"],
            # No canonical `companies` block: a pre-cutover record is silent about company
            # credits rather than asserting it has none, so it writes no `credited_to` row.
            "formats": [{"name": "CD", "qty": "1", "descriptions": {"description": ["Album"]}}],
            # The other wrapper shape: several tracks make `track` a real array, and so does
            # a track's own `extraartists`/`artists`.
            "tracklist": {
                "track": [
                    {
                        "position": "B1",
                        "extraartists": {"artist": [{"id": "gw-a6", "name": "Another Engineer", "role": "Engineer"}]},
                        "artists": {"artist": [{"id": "gw-a1"}]},
                    }
                ]
            },
        },
    ),
]

# What the catalog above asserts, relation by relation.
EXPECTED_EDGES = {
    "by_artist": 4,  # three artists on gw-r1, one on gw-r2
    "on_label": 2,  # the repeated catalogue number is one edge
    "derived_from": 1,
    "in_genre": 3,  # Rock and Pop on gw-r1, Rock on gw-r2
    "in_style": 1,
    "master_by_artist": 1,
    "master_in_genre": 1,
    "master_in_style": 2,
    "member_of": 2,  # the reciprocal pair collapsed
    "alias_of": 1,
    "credited_on": 3,  # one person under two roles is two edges; the roleless credit none
    "same_as": 3,  # First Member/gw-a2, Track Engineer/gw-a4, Another Engineer/gw-a6
    "credited_to": 2,
    "issued_on": 2,
    "track_credited_on": 3,  # gw-r1's track credit and sub-track credit, gw-r2's track credit
    "track_by_artist": 2,  # one track performer per release
}

EXPECTED_VERTICES = {
    "genre": 2,
    "style": 2,
    "person": 5,  # First Member, A Producer, Track Engineer, Medley Producer, Another Engineer
    "media_family": 2,  # vinyl and optical
    "medium": 2,
    "company": 2,
}


@pytest_asyncio.fixture
async def graph_connection() -> AsyncIterator[psycopg.AsyncConnection[Any]]:
    """A connection on the promoted schema, with every table this suite counts emptied."""
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("TEST_DATABASE_URL is required for PostgreSQL integration tests")

    connection = await psycopg.AsyncConnection.connect(database_url)
    await connection.set_autocommit(True)
    try:
        await _truncate(connection)
        yield connection
    finally:
        if not connection.autocommit:
            await connection.rollback()
            await connection.set_autocommit(True)
        await _truncate(connection)
        await connection.close()


async def _truncate(connection: psycopg.AsyncConnection[Any]) -> None:
    relations = ", ".join(f"graph.{relation}" for relation in (*EDGE_COLUMNS, *VERTEX_COLUMNS))
    # Every relation name here is one of this module's own constants, never input.
    await connection.execute(f"TRUNCATE {relations}")
    await connection.execute(f"TRUNCATE {', '.join(ENTITY_TABLES)}")


async def _count(connection: psycopg.AsyncConnection[Any], relation: str) -> int:
    row = await (await connection.execute(f"SELECT count(*) FROM graph.{relation}")).fetchone()  # noqa: S608
    assert row is not None
    return int(row[0])


async def _counts(connection: psycopg.AsyncConnection[Any], relations: Any) -> dict[str, int]:
    return {relation: await _count(connection, relation) for relation in relations}


async def _write_batch(connection: psycopg.AsyncConnection[Any], data_type: str, documents: Any, suffix: str = "v1") -> Any:
    """Send one batch of documents through the batch write path."""
    writer = PostgreSQLBatchWriter(SingleConnectionPool(connection), MagicMock(), media_for_release)
    records = [BatchRecord(data_id, data, f"{data_id}-{suffix}") for data_id, data in documents]
    return await writer.process_batch(data_type, records)


async def _persist(connection: psycopg.AsyncConnection[Any], data_type: str, data_id: str, data: dict[str, Any]) -> None:
    """Send one document through the single-record write path."""
    persistence = PostgreSQLRecordPersistence(SingleConnectionPool(connection), MagicMock(), 0.9, media_for_release)
    await persistence.persist_record(data_type, data_id, data)


def _break_one_edge_insert(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make the first edge INSERT fail, after the document-scoped DELETE has run.

    `write_document_graph` writes vertices, then deletes each replaced relation's rows for
    this document, then inserts the edges. Naming a column `graph.by_artist` does not have
    turns that third step into an `UndefinedColumn` raised by the server, which is the
    shape every real mid-write failure has: the delete is already done and the edges are
    not back yet.
    """
    monkeypatch.setitem(EDGE_COLUMNS, "by_artist", ("release_id", "column_that_does_not_exist"))


async def _load_catalog(connection: psycopg.AsyncConnection[Any], suffix: str = "v1") -> None:
    await _write_batch(connection, "artists", ARTISTS, suffix)
    await _write_batch(connection, "labels", LABELS, suffix)
    await _write_batch(connection, "masters", MASTERS, suffix)
    await _write_batch(connection, "releases", RELEASES, suffix)


@pytest.mark.asyncio
async def test_the_batch_path_writes_every_relation_the_loader_owns(graph_connection: psycopg.AsyncConnection[Any]) -> None:
    """One pass over the fixture catalog fills all sixteen edges and all six vertices."""
    await _load_catalog(graph_connection)

    assert await _counts(graph_connection, EXPECTED_EDGES) == EXPECTED_EDGES
    assert await _counts(graph_connection, EXPECTED_VERTICES) == EXPECTED_VERTICES


@pytest.mark.asyncio
async def test_a_label_document_writes_no_edge_row(graph_connection: psycopg.AsyncConnection[Any]) -> None:
    """`graph.sublabel_of` is a view at the pinned revision, so a label asserts no row."""
    await _write_batch(graph_connection, "labels", LABELS)

    assert await _counts(graph_connection, EXPECTED_EDGES) == dict.fromkeys(EXPECTED_EDGES, 0)


@pytest.mark.asyncio
async def test_the_rows_resolve_through_the_views_that_join_their_vertices(graph_connection: psycopg.AsyncConnection[Any]) -> None:
    """`graph.part_of` and `graph.in_family` inner join the vertex tables.

    Both are views the loader never writes, so they are the check that the vertex rows
    went in — and went in first. `part_of` is asserted only by a document carrying exactly
    one genre, which here is the master and `gw-r2`, never the two-genre `gw-r1`.
    """
    await _load_catalog(graph_connection)

    part_of = await (await graph_connection.execute("SELECT style_name, genre_name FROM graph.part_of ORDER BY style_name")).fetchall()
    in_family = await (await graph_connection.execute("SELECT medium_id, family_name FROM graph.in_family ORDER BY medium_id")).fetchall()

    assert part_of == [("Pop Rock", "Rock"), ("Psychedelic Rock", "Rock")]
    assert in_family == [("optical_cd", "optical"), ("vinyl_12", "vinyl")]


@pytest.mark.asyncio
async def test_the_shared_relations_carry_this_loaders_own_source(graph_connection: psycopg.AsyncConnection[Any]) -> None:
    """`musicbrainz-sql-loader` writes `source = 'musicbrainz'` over the same vertices."""
    await _load_catalog(graph_connection)

    issued_on = await (
        await graph_connection.execute("SELECT release_id, medium_id, source, qty FROM graph.issued_on ORDER BY release_id")
    ).fetchall()
    credited_to = await (
        await graph_connection.execute("SELECT company_id, role, role_category, source FROM graph.credited_to ORDER BY company_id")
    ).fetchall()

    assert issued_on == [("gw-r1", "vinyl_12", "discogs", 2), ("gw-r2", "optical_cd", "discogs", 1)]
    assert credited_to == [
        ("4321", "Pressed By", "manufacture", "discogs"),
        ("name:a cutting room", "Lacquer Cut By", "other", "discogs"),
    ]


@pytest.mark.asyncio
async def test_the_generated_role_category_is_filled_by_the_schema(graph_connection: psycopg.AsyncConnection[Any]) -> None:
    """`graph.credited_on.role_category` is GENERATED, so the loader never writes it."""
    await _load_catalog(graph_connection)

    rows = await (
        await graph_connection.execute("SELECT person_name, role, role_category FROM graph.credited_on ORDER BY person_name, role")
    ).fetchall()

    assert rows == [
        ("A Producer", "Mastered By", "mastering"),
        ("A Producer", "Producer", "production"),
        ("First Member", "Producer", "production"),
    ]


@pytest.mark.asyncio
async def test_an_unchanged_document_is_skipped_and_writes_nothing(graph_connection: psycopg.AsyncConnection[Any]) -> None:
    """The content-hash gate covers the graph rows exactly as it covers the entity row."""
    await _load_catalog(graph_connection)
    before = await _counts(graph_connection, EXPECTED_EDGES)

    result = await _write_batch(graph_connection, "releases", RELEASES)

    assert result.unchanged_ids == {"gw-r1", "gw-r2"}
    assert await _counts(graph_connection, EXPECTED_EDGES) == before


@pytest.mark.asyncio
async def test_a_corrected_document_replaces_its_own_rows_and_only_its_own(graph_connection: psycopg.AsyncConnection[Any]) -> None:
    """A release that loses an artist and a genre must not keep either edge.

    The other release's rows are the control: a document-scoped delete that reached them
    would be as wrong as one that reached nothing.
    """
    await _load_catalog(graph_connection)

    corrected = dict(RELEASES[0][1])
    corrected["artists"] = [{"id": "gw-a1"}]
    corrected["genres"] = ["Rock"]
    await _write_batch(graph_connection, "releases", [("gw-r1", corrected)], suffix="v2")

    by_artist = await (await graph_connection.execute("SELECT release_id, artist_id FROM graph.by_artist ORDER BY release_id, artist_id")).fetchall()
    in_genre = await (await graph_connection.execute("SELECT release_id, genre_name FROM graph.in_genre ORDER BY release_id, genre_name")).fetchall()

    assert by_artist == [("gw-r1", "gw-a1"), ("gw-r2", "gw-a1")]
    assert in_genre == [("gw-r1", "Rock"), ("gw-r2", "Rock")]
    # The vertex vocabulary is shared and never withdrawn: `Pop` outlives the only release
    # that named it, which costs one row and keeps a concurrent writer's edge resolvable.
    assert await _count(graph_connection, "genre") == 2


@pytest.mark.asyncio
async def test_the_stale_row_purge_removes_the_edges_of_the_documents_it_deletes(
    graph_connection: psycopg.AsyncConnection[Any],
) -> None:
    """A release that left the dump takes its edges with it, and leaves the others alone."""
    await _load_catalog(graph_connection)
    await graph_connection.execute("UPDATE releases SET updated_at = '2000-01-01T00:00:00+00:00' WHERE data_id = 'gw-r2'")

    persistence = PostgreSQLRecordPersistence(SingleConnectionPool(graph_connection), MagicMock(), 0.9, media_for_release)
    await persistence.purge_stale_rows("releases", "2001-01-01T00:00:00+00:00", record_count=1)

    remaining = await (await graph_connection.execute("SELECT data_id FROM releases")).fetchall()
    assert remaining == [("gw-r1",)]
    assert await _counts(graph_connection, EXPECTED_EDGES) == {
        **EXPECTED_EDGES,
        "by_artist": 3,
        "on_label": 1,
        "in_genre": 2,
        "issued_on": 1,
        "track_credited_on": 2,  # gw-r2's track credit purged with the rest of its document
        "track_by_artist": 1,
    }


@pytest.mark.asyncio
async def test_a_failed_graph_write_rolls_back_the_document_and_its_edges(
    graph_connection: psycopg.AsyncConnection[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The single-record path must commit the document, its hash, and its edges together.

    The pool hands out an AUTOCOMMIT connection, so without an explicit transaction the
    document-scoped DELETE would commit on its own ahead of the edge INSERTs. A failure in
    between would leave the entity row and its NEW hash durable with the edges gone — and
    the next delivery of the same event would read that hash, find it unchanged, and skip
    the re-derivation that is the only thing that would have put them back. This test is
    the one that fails if the transaction goes away, because the fixture connection runs in
    autocommit exactly as production does.
    """
    release = {**RELEASES[0][1], "sha256": "gw-r1-v1"}
    await _persist(graph_connection, "releases", "gw-r1", release)
    assert await _count(graph_connection, "by_artist") == 3

    _break_one_edge_insert(monkeypatch)
    corrected = {**release, "artists": [{"id": "gw-a1"}], "genres": ["Rock"], "sha256": "gw-r1-v2"}
    with pytest.raises(psycopg.errors.UndefinedColumn):
        await _persist(graph_connection, "releases", "gw-r1", corrected)

    stored = await (await graph_connection.execute("SELECT hash FROM releases WHERE data_id = 'gw-r1'")).fetchone()
    assert stored == ("gw-r1-v1",), "the entity row's content hash must not survive a failed graph write"
    assert await _count(graph_connection, "by_artist") == 3
    assert await _count(graph_connection, "in_genre") == 2


@pytest.mark.asyncio
async def test_a_failed_graph_write_rolls_back_the_whole_batch(
    graph_connection: psycopg.AsyncConnection[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The batch path's own transaction has to cover its graph writes for the same reason."""
    await _write_batch(graph_connection, "releases", RELEASES)
    assert await _count(graph_connection, "by_artist") == 4

    _break_one_edge_insert(monkeypatch)
    corrected = [("gw-r1", {**RELEASES[0][1], "artists": [{"id": "gw-a1"}]})]
    with pytest.raises(psycopg.errors.UndefinedColumn):
        await _write_batch(graph_connection, "releases", corrected, suffix="v2")

    stored = await (await graph_connection.execute("SELECT hash FROM releases WHERE data_id = 'gw-r1'")).fetchone()
    assert stored == ("gw-r1-v1",)
    assert await _count(graph_connection, "by_artist") == 4


@pytest.mark.asyncio
async def test_the_single_record_path_writes_the_same_rows_as_the_batch_path(
    graph_connection: psycopg.AsyncConnection[Any],
) -> None:
    """`POSTGRES_BATCH_MODE=false` selects the other path; it must not derive differently."""
    await _load_catalog(graph_connection)
    batched = await _counts(graph_connection, {**EXPECTED_EDGES, **EXPECTED_VERTICES})

    await _truncate(graph_connection)
    for data_type, documents in (("artists", ARTISTS), ("labels", LABELS), ("masters", MASTERS), ("releases", RELEASES)):
        for data_id, data in documents:
            await _persist(graph_connection, data_type, data_id, {**data, "sha256": f"{data_id}-v1"})

    assert await _counts(graph_connection, {**EXPECTED_EDGES, **EXPECTED_VERTICES}) == batched


@pytest.mark.asyncio
async def test_the_single_record_path_also_skips_an_unchanged_document(
    graph_connection: psycopg.AsyncConnection[Any],
) -> None:
    """Its hash gate is the `prior` CTE its own upsert carries, not a second SELECT."""
    release = {**RELEASES[0][1], "sha256": "gw-r1-v1"}

    await _persist(graph_connection, "releases", "gw-r1", release)
    await graph_connection.execute("DELETE FROM graph.by_artist")
    await _persist(graph_connection, "releases", "gw-r1", release)

    # Unchanged, so nothing was rewritten — the deleted rows stay deleted until the
    # document's content actually changes.
    assert await _count(graph_connection, "by_artist") == 0

    await _persist(graph_connection, "releases", "gw-r1", {**release, "sha256": "gw-r1-v2"})
    assert await _count(graph_connection, "by_artist") == 3
