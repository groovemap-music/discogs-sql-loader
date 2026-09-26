"""The loader's track relations, held against the schema's own bootstrap projection.

`tests/test_graph_derivation.py` holds `_track_credits`/`_track_performers` to the schema's
`_TRACK_CREDIT_SOURCE`/`_TRACK_PERFORMER_SOURCE` by reading both implementations, without a
database. That is still a claim about what one function returns, not about what ends up in
PostgreSQL, and `track_credited_on`/`track_by_artist` are outside `tests/integration/
test_store_parity.py`'s Neo4j comparison — there is no enricher counterpart for either
relation to read back (see `NO_ENRICHER_COUNTERPART` there). This suite is the missing half
for these two: it plays one fixture through the loader's own derive-and-write path, reads
`graph.track_credited_on`, `graph.track_by_artist`, `graph.person`, and `graph.same_as` back,
then runs `graph.bootstrap_fill()` — the schema's own SQL projection of the same documents,
declared in `_phase0_relation_bodies()` and shipped as a function precisely so a promoted
environment and a loader can be compared against one text rather than two — and asserts the
two are identical.

`graph.bootstrap_fill()` TRUNCATEs every relation it fills before refilling it from
`public.releases`/`masters`/`artists`, so it cannot be run before the loader's rows are read:
running it clobbers what the loader wrote and replaces it with its own projection of the same
underlying documents. Reading the loader's rows first and the bootstrap's second, off the same
committed entity tables, is what makes the comparison possible in one connection.
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
from tableinator.graph_counters import PATH_REFRESH_RELATIONS, REFRESH_ORDER
from tableinator.graph_derivation import EDGE_COLUMNS, VERTEX_COLUMNS
from tableinator.media import media_for_release


if TYPE_CHECKING:
    from collections.abc import AsyncIterator


pytestmark = pytest.mark.integration

ENTITY_TABLES = ("artists", "labels", "masters", "releases")

# The relations this suite compares. `person` and `same_as` are shared with the release-level
# credit source, so the fixture also states a release-level credit to prove the two sources'
# rows still converge on the schema's side exactly as `derive_release` converges them on the
# loader's.
COMPARED_RELATIONS = ("track_credited_on", "track_by_artist", "person", "same_as")


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
    cannot poison the next borrower. The batch write path relies on that, so the double has
    to do it too.
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
# Real xmltodict wrapper shapes throughout, both variants at every level `_xmltodict_array`
# unwraps: a bare object for one child, a real array for several. `bp-r1` is the single-object
# side (one track, its own credit and performer, an array of two sub-tracks); `bp-r2` is the
# array side (two tracks, one bare and one with array extraartists/artists, one with no track
# content at all); `bp-r3` carries no `tracklist` key at all, the pre-track-relation shape.
RELEASES: list[tuple[str, dict[str, Any]]] = [
    (
        "bp-r1",
        {
            "id": "bp-r1",
            "title": "Single-Object Wrapper Release",
            "artists": [{"id": "bp-a1"}],
            "genres": ["Electronic"],
            # A release-level credit sharing an id with a track-level one below, so `person`
            # and `same_as` have to converge on one vertex and one same_as row from two
            # sources, on both the loader's side and the schema's.
            "extraartists": [{"id": "bp-a2", "name": "Shared Credit", "role": "Producer"}],
            "tracklist": {
                "track": {
                    "position": "A1",
                    "extraartists": {"artist": {"id": "bp-a2", "name": "Shared Credit", "role": "Engineer"}},
                    "artists": {"artist": {"id": "bp-a3"}},
                    "sub_tracks": {
                        "track": [
                            {"position": "A1a", "extraartists": {"artist": {"name": "Medley Producer", "role": "Producer"}}},
                            {"position": "A1b", "extraartists": {"artist": {"id": "0", "name": "No Such Artist", "role": "Mixed By"}}},
                        ]
                    },
                }
            },
        },
    ),
    (
        "bp-r2",
        {
            "id": "bp-r2",
            "title": "Array Wrapper Release",
            "artists": [{"id": "bp-a1"}],
            "genres": ["Electronic"],
            "tracklist": {
                "track": [
                    {
                        "position": "B1",
                        "extraartists": {"artist": [{"id": "bp-a4", "name": "Another Engineer", "role": "Engineer"}]},
                        "artists": {"artist": [{"id": "bp-a1"}, {"id": ""}, {"id": "0"}]},
                    },
                    # No credit or performer content at all: both sides must derive nothing
                    # for this track ordinal without erroring on the missing keys.
                    {"position": "B2"},
                ]
            },
        },
    ),
    (
        "bp-r3",
        {
            "id": "bp-r3",
            "title": "No Tracklist At All",
            "artists": [{"id": "bp-a1"}],
            "genres": ["Electronic"],
            "extraartists": [{"id": "bp-a5", "name": "Release Only Credit", "role": "Mastered By"}],
        },
    ),
]


@pytest_asyncio.fixture
async def bootstrap_connection() -> AsyncIterator[psycopg.AsyncConnection[Any]]:
    """A connection on the promoted schema, with every entity and graph table emptied.

    `graph.bootstrap_fill()` truncates every relation it fills, so leftovers from another
    test would be silently absorbed into "what the schema projects" here; both the setup and
    the teardown truncate every relation this suite could touch, including ones outside
    `COMPARED_RELATIONS`, since a stray row in an uncompared relation could still make
    `graph.bootstrap_fill()` itself fail on a uniqueness violation.
    """
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


# Every relation `graph.bootstrap_fill()` fills: the loader's own vertex and edge tables,
# plus the counter and path relations `tableinator.graph_counters` owns. A stray row left in
# any of them — not only the four `COMPARED_RELATIONS` — could make the fill itself raise on
# a uniqueness violation, so the truncate before and after covers all of them.
ALL_GRAPH_RELATIONS: tuple[str, ...] = (*VERTEX_COLUMNS, *EDGE_COLUMNS, *REFRESH_ORDER, *PATH_REFRESH_RELATIONS)


async def _truncate(connection: psycopg.AsyncConnection[Any]) -> None:
    relations = ", ".join(f"graph.{relation}" for relation in ALL_GRAPH_RELATIONS)
    await connection.execute(f"TRUNCATE {relations}")
    await connection.execute(f"TRUNCATE {', '.join(ENTITY_TABLES)}")


async def _write_via_loader(connection: psycopg.AsyncConnection[Any]) -> None:
    """Write the fixture catalog through the loader's own batch derive-and-write path."""
    writer = PostgreSQLBatchWriter(SingleConnectionPool(connection), MagicMock(), media_for_release)
    records = [BatchRecord(data_id, data, f"{data_id}-v1") for data_id, data in RELEASES]
    await writer.process_batch("releases", records)


async def _read_relation(connection: psycopg.AsyncConnection[Any], relation: str, columns: tuple[str, ...]) -> frozenset[tuple[Any, ...]]:
    projection = ", ".join(columns)
    rows = await (await connection.execute(f"SELECT {projection} FROM graph.{relation}")).fetchall()  # noqa: S608
    return frozenset(tuple(row) for row in rows)


_COMPARED_COLUMNS: dict[str, tuple[str, ...]] = {**EDGE_COLUMNS, **VERTEX_COLUMNS}


async def _read_compared(connection: psycopg.AsyncConnection[Any]) -> dict[str, frozenset[tuple[Any, ...]]]:
    return {relation: await _read_relation(connection, relation, _COMPARED_COLUMNS[relation]) for relation in COMPARED_RELATIONS}


@pytest.mark.asyncio
async def test_the_loader_and_the_bootstrap_projection_agree_on_every_compared_relation(
    bootstrap_connection: psycopg.AsyncConnection[Any],
) -> None:
    """`graph.bootstrap_fill()` re-derives the same documents in SQL; the two must match.

    Both sides read the same committed `public.releases` rows: the loader wrote them as part
    of its own batch path, and the bootstrap fill reads them straight back out with no
    knowledge the loader ran at all. A real disagreement here means the Python and SQL
    projections have drifted, not that a fixture failed to exercise something.
    """
    await _write_via_loader(bootstrap_connection)
    loader_rows = await _read_compared(bootstrap_connection)

    empty_before_bootstrap = [relation for relation, rows in loader_rows.items() if not rows]
    assert empty_before_bootstrap == [], f"the loader wrote nothing for {empty_before_bootstrap}"

    await bootstrap_connection.execute("SELECT * FROM graph.bootstrap_fill()")
    bootstrap_rows = await _read_compared(bootstrap_connection)

    mismatched = {
        relation: (loader_rows[relation] - bootstrap_rows[relation], bootstrap_rows[relation] - loader_rows[relation])
        for relation in COMPARED_RELATIONS
        if loader_rows[relation] != bootstrap_rows[relation]
    }
    assert not mismatched, "the loader and graph.bootstrap_fill() disagree:" + "".join(
        f"\n  {relation}\n    only from the loader: {sorted(only_loader)}\n    only from bootstrap_fill: {sorted(only_bootstrap)}"
        for relation, (only_loader, only_bootstrap) in sorted(mismatched.items())
    )


@pytest.mark.asyncio
async def test_track_credited_on_carries_no_artist_id_or_generated_column_on_either_side(
    bootstrap_connection: psycopg.AsyncConnection[Any],
) -> None:
    """Neither the loader nor `graph.bootstrap_fill()` names `role_category`; both resolve
    an id-bearing track credit through `same_as` instead of a column on `track_credited_on`.
    """
    await _write_via_loader(bootstrap_connection)

    loader_shared_credit = await (
        await bootstrap_connection.execute(
            "SELECT track_ordinal, sub_track_ordinal, role FROM graph.track_credited_on "
            "WHERE release_id = 'bp-r1' AND person_name = 'Shared Credit' ORDER BY track_ordinal"
        )
    ).fetchall()
    assert loader_shared_credit == [(1, 0, "Engineer")]

    same_as = await (await bootstrap_connection.execute("SELECT artist_id FROM graph.same_as WHERE person_name = 'Shared Credit'")).fetchall()
    assert same_as == [("bp-a2",)]

    await bootstrap_connection.execute("SELECT * FROM graph.bootstrap_fill()")

    bootstrap_shared_credit = await (
        await bootstrap_connection.execute(
            "SELECT track_ordinal, sub_track_ordinal, role FROM graph.track_credited_on "
            "WHERE release_id = 'bp-r1' AND person_name = 'Shared Credit' ORDER BY track_ordinal"
        )
    ).fetchall()
    assert bootstrap_shared_credit == loader_shared_credit
