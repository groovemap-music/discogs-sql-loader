"""The counter values a run of the refresh leaves in a real PostgreSQL.

`tests/test_graph_counters.py` holds each body byte-identical to the schema's own and each
statement to its order, without a database. This suite is the other half: it loads a
catalog small enough to count by hand through the real write path, runs the
`extraction_complete` pass against the promoted schema, and asserts every counter row
against a value computed in the comments below rather than by re-running the same SQL.

The catalog is chosen to reach every rule the schema reviews named. The first release
carries three artists, two genres, and a style, so a label counting its releases has six
joined rows to fan out over; the third carries no year, so `first_year` has a release it
must ignore; one membership is stated from both ends, and one release credits a person by
id, so the two additive relations have a row that outlives the document asserting it.

The last section is the extraction latch that decides WHEN the pass runs, against the same
real PostgreSQL: a second dump collecting its own four signals, a restart losing none of
them, and a straggler from a superseded dump firing nothing.
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
from tableinator.extraction_latch import (
    KEY_COLUMNS,
    LATCH_RELATION,
    LOADER_DISCRIMINATOR,
    extraction_latch_key,
    mark_extraction_refreshed,
    probe_latch_relation,
    record_extraction_signal,
)
from tableinator.graph_counters import PATH_REFRESH_RELATIONS, REFRESH_ORDER, refresh_derived_relations
from tableinator.graph_derivation import EDGE_COLUMNS, VERTEX_COLUMNS
from tableinator.media import media_for_release


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
    """Expose one test connection through the production pool protocol."""

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
# Discogs ids are numeric, and `graph.release_degree` casts the release key to bigint
# before it counts collection rows, so the fixture uses numeric ids rather than readable
# ones. The comments name them by their short suffix.

A1, A2, A3, A4 = "9000001", "9000002", "9000003", "9000004"  # Alpha, Beta, Gamma, Delta
L1, L2 = "9100001", "9100002"
M1 = "9200001"
R1, R2, R3 = "9300001", "9300002", "9300003"

ARTISTS = [
    # The membership is stated from both ends, so the reciprocal pair is one edge.
    (A1, {"id": A1, "name": "Alpha", "members": [{"id": A2}]}),
    (A2, {"id": A2, "name": "Beta", "groups": [{"id": A1}]}),
    (A3, {"id": A3, "name": "Gamma", "aliases": [{"id": A1}]}),
    (A4, {"id": A4, "name": "Delta"}),
]

LABELS = [(L1, {"id": L1, "name": "First Label"}), (L2, {"id": L2, "name": "Second Label"})]

MASTERS = [(M1, {"id": M1, "title": "A Master", "artists": [{"id": A1}], "genres": ["Rock"], "styles": ["Pop Rock"]})]

RELEASES = [
    (
        R1,
        {
            "id": R1,
            "title": "First Release",
            "year": "1970",
            "master_id": M1,
            # Three artists and two genres: the label join yields six rows for this one
            # release, so only a DISTINCT keeps its label's release_count at one for it.
            "artists": [{"id": A1}, {"id": A2}, {"id": A4}],
            "labels": [{"id": L1, "catno": "FL-001"}],
            # Two genres, so this release asserts no `part_of` row.
            "genres": ["Rock", "Jazz"],
            "styles": ["Pop Rock"],
            "extraartists": [{"id": A3, "name": "Person One", "role": "Producer"}],
            "companies": {"items": [{"discogs_id": "4321", "name": "A Pressing Plant", "role": "Pressed By", "role_category": "manufacture"}]},
            "formats": [{"name": "Vinyl", "qty": "2", "descriptions": {"description": ["LP", "Album"]}}],
        },
    ),
    (
        R2,
        {
            "id": R2,
            "title": "Second Release",
            "year": "1965",
            "artists": [{"id": A1}],
            "labels": [{"id": L1, "catno": "FL-002"}],
            "genres": ["Rock"],
            "styles": ["Fusion"],
        },
    ),
    (
        R3,
        {
            "id": R3,
            "title": "Third Release",
            # No year at all: `first_year` must ignore this release rather than sort it first.
            "artists": [{"id": A3}],
            "labels": [{"id": L2, "catno": "SL-001"}],
            "genres": ["Jazz"],
        },
    ),
]

# ── What the catalog above asserts, counted by hand ──────────────────────────
#
# Edges, which every counter sums:
#   by_artist   (R1,A1) (R1,A2) (R1,A4) (R2,A1) (R3,A3)
#   on_label    (R1,L1) (R2,L1) (R3,L2)
#   in_genre    (R1,Rock) (R1,Jazz) (R2,Rock) (R3,Jazz)
#   in_style    (R1,Pop Rock) (R2,Fusion)
#   derived_from (R1,M1)   credited_on (Person One,R1,Producer)
#   credited_to (R1,4321)  issued_on (R1,vinyl_12)
#   member_of   (A2,A1)    alias_of (A1,A3)    same_as (Person One,A3)
#   master_by_artist (M1,A1)  master_in_genre (M1,Rock)  master_in_style (M1,Pop Rock)
#
# `graph.part_of` is asserted only by a document carrying exactly one genre: R2 gives
# (Fusion, Rock) and the master gives (Pop Rock, Rock). R1 has two genres and R3 has no
# style, so neither contributes. Counter parity is intentionally independent: genre/style
# counts use release co-occurrence while `part_of` preserves its stricter taxonomy meaning.

# (name, release_count, artist_count, label_count, style_count, first_year)
#   Jazz  : R1 and R3 → 2 releases; artists {A1,A2,A4} plus {A3} = 4; labels {L1,L2} = 2;
#           Pop Rock co-occurs with Jazz on R1 → 1;
#           years 1970 and (none) → 1970.
#   Rock  : R1 and R2 → 2 releases; artists {A1,A2,A4} plus {A1} = 3; labels {L1} = 1;
#           Fusion and Pop Rock sit under Rock → 2; years 1970 and 1965 → 1965.
EXPECTED_GENRE_STATS = [("Jazz", 2, 4, 2, 1, 1970), ("Rock", 2, 3, 1, 2, 1965)]

# (name, release_count, artist_count, label_count, genre_count, first_year)
#   Fusion   : R2 → 1 release; artists {A1}; labels {L1}; one genre above it; 1965.
#   Pop Rock : R1 → 1 release; artists {A1,A2,A4} = 3; labels {L1};
#              Rock and Jazz co-occur with it on R1; 1970.
EXPECTED_STYLE_STATS = [("Fusion", 1, 1, 1, 1, 1965), ("Pop Rock", 1, 3, 1, 2, 1970)]

# (label_id, release_count, artist_count, genre_count)
#   L1 : R1 and R2. R1 alone joins three artists and two genres, so the join yields six
#        rows for it — `count(DISTINCT release_id)` is what keeps release_count at 2.
#        Artists {A1,A2,A4} plus {A1} = 3; genres {Rock,Jazz} plus {Rock} = 2.
#   L2 : R3 only.
EXPECTED_LABEL_STATS = [(L1, 2, 3, 2), (L2, 1, 1, 1)]

# (artist_id, degree), every edge an `:Artist` carries counted undirected:
#   A1 : by_artist x2, master_by_artist x1, member_of as the group x1, alias_of as the
#        alias x1 → 5
#   A2 : by_artist x1, member_of as the member x1 → 2
#   A3 : by_artist x1, same_as x1, alias_of as the target x1 → 3
#   A4 : by_artist x1 → 1
EXPECTED_ARTIST_DEGREE = [(A1, 5), (A2, 2), (A3, 3), (A4, 1)]

# (release_id, degree), the catalog half only:
#   R1 : 3 by_artist + 1 on_label + 2 in_genre + 1 in_style + 1 derived_from
#        + 1 credited_on + 1 credited_to + 1 issued_on = 11
#   R2 : 1 + 1 + 1 + 1 = 4
#   R3 : 1 + 1 + 1 = 3
EXPECTED_RELEASE_DEGREE_BASE = [(R1, 11), (R2, 4), (R3, 3)]

# (artist_id, genre_name, release_count): every genre of every release the artist is on.
EXPECTED_ARTIST_GENRE = [
    (A1, "Jazz", 1),
    (A1, "Rock", 2),
    (A2, "Jazz", 1),
    (A2, "Rock", 1),
    (A3, "Jazz", 1),
    (A4, "Jazz", 1),
    (A4, "Rock", 1),
]

# (label_id, genre_name, release_count)
EXPECTED_LABEL_GENRE = [(L1, "Jazz", 1), (L1, "Rock", 2), (L2, "Jazz", 1)]


@pytest_asyncio.fixture
async def counter_connection() -> AsyncIterator[psycopg.AsyncConnection[Any]]:
    """A connection on the promoted schema, with every relation this suite counts emptied."""
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
    # Every relation name here is one of this module's own constants, never input.
    relations = ", ".join(f"graph.{relation}" for relation in (*EDGE_COLUMNS, *VERTEX_COLUMNS, *REFRESH_ORDER, *PATH_REFRESH_RELATIONS))
    await connection.execute(f"TRUNCATE {relations}")
    await connection.execute(f"TRUNCATE {', '.join(ENTITY_TABLES)}")
    # Emptied, never dropped: the latch relation belongs to the promoted schema now, and the
    # session applies that schema once. Dropping it here would take it away from every test
    # that ran after this one.
    await connection.execute("TRUNCATE {}.{}".format(*LATCH_RELATION))


async def _write_batch(connection: psycopg.AsyncConnection[Any], data_type: str, documents: Any, suffix: str = "v1") -> None:
    writer = PostgreSQLBatchWriter(SingleConnectionPool(connection), MagicMock(), media_for_release)
    await writer.process_batch(data_type, [BatchRecord(data_id, data, f"{data_id}-{suffix}") for data_id, data in documents])


async def _load_catalog(connection: psycopg.AsyncConnection[Any]) -> None:
    await _write_batch(connection, "artists", ARTISTS)
    await _write_batch(connection, "labels", LABELS)
    await _write_batch(connection, "masters", MASTERS)
    await _write_batch(connection, "releases", RELEASES)


async def _refresh(connection: psycopg.AsyncConnection[Any], version: str = "20260101") -> dict[str, int]:
    return await refresh_derived_relations(SingleConnectionPool(connection), MagicMock(), version)


async def _rows(connection: psycopg.AsyncConnection[Any], query: str, parameters: Any = None) -> list[tuple[Any, ...]]:
    return list(await (await connection.execute(query, parameters)).fetchall())


@pytest_asyncio.fixture
async def refreshed(counter_connection: psycopg.AsyncConnection[Any]) -> psycopg.AsyncConnection[Any]:
    """The fixture catalog loaded and the `extraction_complete` pass run once over it."""
    await _load_catalog(counter_connection)
    await _refresh(counter_connection)
    return counter_connection


# ── The counters ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_genre_stats_matches_the_hand_computed_counts(refreshed: psycopg.AsyncConnection[Any]) -> None:
    """A genre's four counts and its first year, as `genre_cypher` computes them in Neo4j."""
    query = "SELECT name, release_count, artist_count, label_count, style_count, first_year FROM graph.genre_stats ORDER BY name"

    assert await _rows(refreshed, query) == EXPECTED_GENRE_STATS


@pytest.mark.asyncio
async def test_style_stats_matches_the_hand_computed_counts(refreshed: psycopg.AsyncConnection[Any]) -> None:
    query = "SELECT name, release_count, artist_count, label_count, genre_count, first_year FROM graph.style_stats ORDER BY name"

    assert await _rows(refreshed, query) == EXPECTED_STYLE_STATS


@pytest.mark.asyncio
async def test_label_stats_counts_a_release_once_however_it_fans_out(refreshed: psycopg.AsyncConnection[Any]) -> None:
    """The first release joins three artists and two genres; its label still counts it once."""
    query = "SELECT label_id, release_count, artist_count, genre_count FROM graph.label_stats ORDER BY label_id"

    assert await _rows(refreshed, query) == EXPECTED_LABEL_STATS


@pytest.mark.asyncio
async def test_artist_degree_counts_every_edge_the_artist_carries(refreshed: psycopg.AsyncConnection[Any]) -> None:
    """`COUNT { (a)--() }`, summed over the seven edge columns that reach an artist."""
    query = "SELECT artist_id, degree FROM graph.artist_degree ORDER BY artist_id"

    assert await _rows(refreshed, query) == EXPECTED_ARTIST_DEGREE


@pytest.mark.asyncio
async def test_release_degree_base_is_the_catalog_half_only(refreshed: psycopg.AsyncConnection[Any]) -> None:
    """The loader writes the base; `graph.release_degree` adds collection and wantlist live.

    With no collection or wantlist row for these releases the view's live half is zero, so
    the two must agree exactly — which is also what proves the view reads this table.
    """
    base = await _rows(refreshed, "SELECT release_id, degree FROM graph.release_degree_base ORDER BY release_id")
    live = await _rows(refreshed, "SELECT release_id, degree FROM graph.release_degree ORDER BY release_id")

    assert base == EXPECTED_RELEASE_DEGREE_BASE
    assert live == EXPECTED_RELEASE_DEGREE_BASE


@pytest.mark.asyncio
async def test_the_pair_aggregates_match_the_hand_computed_counts(refreshed: psycopg.AsyncConnection[Any]) -> None:
    artist_genre = await _rows(refreshed, "SELECT artist_id, genre_name, release_count FROM graph.artist_genre ORDER BY artist_id, genre_name")
    label_genre = await _rows(refreshed, "SELECT label_id, genre_name, release_count FROM graph.label_genre ORDER BY label_id, genre_name")

    assert artist_genre == EXPECTED_ARTIST_GENRE
    assert label_genre == EXPECTED_LABEL_GENRE


@pytest.mark.asyncio
async def test_a_genre_no_release_names_gets_a_non_null_zero(counter_connection: psycopg.AsyncConnection[Any]) -> None:
    """graphinator writes an explicit zero onto every `:Genre`; a NULL would break the reads."""
    await _load_catalog(counter_connection)
    await counter_connection.execute("INSERT INTO graph.genre (name) VALUES ('Unnamed By Anything')")

    await _refresh(counter_connection)

    row = await _rows(
        counter_connection,
        "SELECT release_count, artist_count, label_count, style_count, first_year FROM graph.genre_stats WHERE name = 'Unnamed By Anything'",
    )
    assert row == [(0, 0, 0, 0, None)]


@pytest.mark.asyncio
async def test_counter_parity_covers_empty_labels_positive_years_zero_year_and_cooccurrence(
    counter_connection: psycopg.AsyncConnection[Any],
) -> None:
    """The refresh mirrors graphinator without broadening `part_of` taxonomy.

    These deliberately bypass the importer normalizer to exercise the counter contract on
    legacy/raw stored data. The normalizer's plausibility bounds are asserted separately.
    """
    empty_label_id = "9400001"
    positive_year_release_id = "9400002"
    zero_year_release_id = "9400003"
    await _write_batch(
        counter_connection,
        "labels",
        [(empty_label_id, {"id": empty_label_id, "name": "No Releases Label"})],
    )
    await _write_batch(
        counter_connection,
        "releases",
        [
            (
                positive_year_release_id,
                {
                    "id": positive_year_release_id,
                    "title": "Positive Three Digit Year",
                    "year": "197",
                    "genres": ["Counter Genre One", "Counter Genre Two"],
                    "styles": ["Counter Style"],
                },
            ),
            (
                zero_year_release_id,
                {
                    "id": zero_year_release_id,
                    "title": "Zero Year",
                    "year": "0000",
                    "genres": ["Zero Year Genre"],
                },
            ),
        ],
    )

    await _refresh(counter_connection)

    assert await _rows(
        counter_connection,
        "SELECT release_count, artist_count, genre_count FROM graph.label_stats WHERE label_id = %s",
        (empty_label_id,),
    ) == [(0, 0, 0)]
    assert await _rows(
        counter_connection,
        "SELECT name, style_count, first_year FROM graph.genre_stats WHERE name IN ('Counter Genre One', 'Counter Genre Two') ORDER BY name",
    ) == [("Counter Genre One", 1, 197), ("Counter Genre Two", 1, 197)]
    assert await _rows(
        counter_connection,
        "SELECT genre_count, first_year FROM graph.style_stats WHERE name = 'Counter Style'",
    ) == [(2, 197)]
    assert await _rows(
        counter_connection,
        "SELECT first_year FROM graph.genre_stats WHERE name = 'Zero Year Genre'",
    ) == [(None,)]
    assert (
        await _rows(
            counter_connection,
            "SELECT genre_name FROM graph.part_of WHERE style_name = 'Counter Style'",
        )
        == []
    )


@pytest.mark.asyncio
async def test_the_refresh_reports_the_rows_it_wrote(counter_connection: psycopg.AsyncConnection[Any]) -> None:
    await _load_catalog(counter_connection)

    counts = await _refresh(counter_connection)

    assert counts == {
        "genre_stats": len(EXPECTED_GENRE_STATS),
        "style_stats": len(EXPECTED_STYLE_STATS),
        "label_stats": len(EXPECTED_LABEL_STATS),
        "artist_degree": len(EXPECTED_ARTIST_DEGREE),
        "release_degree_base": len(EXPECTED_RELEASE_DEGREE_BASE),
        "artist_genre": len(EXPECTED_ARTIST_GENRE),
        "label_genre": len(EXPECTED_LABEL_GENRE),
    }


@pytest.mark.asyncio
async def test_the_promoted_path_refreshes_build_the_member_union_then_vertex_degrees(
    counter_connection: psycopg.AsyncConnection[Any],
) -> None:
    """The real promoted functions populate both loader-owned pathfinder relations."""
    await _load_catalog(counter_connection)

    await _refresh(counter_connection)

    member_of = await _rows(
        counter_connection,
        "SELECT member_artist_id, group_artist_id, source FROM graph.artist_member_of ORDER BY 1, 2, 3",
    )
    artist_degrees = await _rows(
        counter_connection,
        "SELECT key, degree FROM graph.vertex_degree WHERE kind = 'a' ORDER BY key",
    )
    assert member_of == [(A2, A1, "discogs")]
    # Pathfinder degree excludes SAME_AS: its ten-relation surface gives A3 only
    # BY plus ALIAS_OF, while the other three artist counts match artist_degree.
    assert artist_degrees == [(A1, 5), (A2, 2), (A3, 2), (A4, 1)]


@pytest.mark.asyncio
async def test_the_refresh_converges_downward_as_well_as_up(counter_connection: psycopg.AsyncConnection[Any]) -> None:
    """A count the documents no longer justify has to fall, which an upsert could not do."""
    await _load_catalog(counter_connection)
    await _refresh(counter_connection)

    # The second release drops Rock, so Rock keeps only the first release — and its first
    # year moves from 1965 back up to 1970.
    reduced = [(R2, {**RELEASES[1][1], "genres": ["Jazz"]})]
    await _write_batch(counter_connection, "releases", reduced, suffix="v2")
    await _refresh(counter_connection)

    rows = await _rows(counter_connection, "SELECT release_count, first_year FROM graph.genre_stats WHERE name = 'Rock'")
    assert rows == [(1, 1970)]


@pytest.mark.asyncio
async def test_running_the_pass_twice_changes_nothing(counter_connection: psycopg.AsyncConnection[Any]) -> None:
    """The latch can be redelivered, so a second pass over the same catalog is a no-op."""
    await _load_catalog(counter_connection)
    first = await _refresh(counter_connection)
    genre_stats = await _rows(counter_connection, "SELECT * FROM graph.genre_stats ORDER BY name")

    second = await _refresh(counter_connection)

    assert second == first
    assert await _rows(counter_connection, "SELECT * FROM graph.genre_stats ORDER BY name") == genre_stats


# ── The additive-edge reconciliation ─────────────────────────────────────────


@pytest.mark.asyncio
async def test_a_membership_removed_from_both_documents_survives_until_the_latch(
    counter_connection: psycopg.AsyncConnection[Any],
) -> None:
    """`member_of` is written additively, so no per-document delete can reach this row.

    `graph_derivation.derive_artist` records why: the row names two artists and either
    document can assert it, so a delete scoped to one of them would drop a row the other
    end still asserts. The sweep on the `extraction_complete` latch is what removes it once
    NEITHER end asserts it any more.
    """
    await _load_catalog(counter_connection)
    assert await _rows(counter_connection, "SELECT member_artist_id, group_artist_id FROM graph.member_of") == [(A2, A1)]

    withdrawn = [
        (A1, {"id": A1, "name": "Alpha"}),
        (A2, {"id": A2, "name": "Beta"}),
    ]
    await _write_batch(counter_connection, "artists", withdrawn, suffix="v2")
    assert await _rows(counter_connection, "SELECT member_artist_id, group_artist_id FROM graph.member_of") == [(A2, A1)]

    await _refresh(counter_connection)

    assert await _rows(counter_connection, "SELECT member_artist_id, group_artist_id FROM graph.member_of") == []


@pytest.mark.asyncio
async def test_a_credit_removed_from_its_release_leaves_same_as_after_the_latch(
    counter_connection: psycopg.AsyncConnection[Any],
) -> None:
    """`same_as` has no release column, so it too outlives the document that asserted it."""
    await _load_catalog(counter_connection)
    assert await _rows(counter_connection, "SELECT person_name, artist_id FROM graph.same_as") == [("Person One", A3)]

    anonymised = [(R1, {**RELEASES[0][1], "extraartists": [{"name": "Person One", "role": "Producer"}]})]
    await _write_batch(counter_connection, "releases", anonymised, suffix="v2")
    assert await _rows(counter_connection, "SELECT person_name, artist_id FROM graph.same_as") == [("Person One", A3)]

    await _refresh(counter_connection)

    assert await _rows(counter_connection, "SELECT person_name, artist_id FROM graph.same_as") == []


@pytest.mark.asyncio
async def test_the_sweep_keeps_every_membership_a_document_still_asserts(refreshed: psycopg.AsyncConnection[Any]) -> None:
    """A relation both documents still assert survives the sweep, and `alias_of` is untouched."""
    assert await _rows(refreshed, "SELECT member_artist_id, group_artist_id FROM graph.member_of") == [(A2, A1)]
    # A3's document names A1 as ITS alias, so A1 is the alias and A3 the target.
    assert await _rows(refreshed, "SELECT alias_artist_id, artist_id FROM graph.alias_of") == [(A1, A3)]
    assert await _rows(refreshed, "SELECT person_name, artist_id FROM graph.same_as") == [("Person One", A3)]


@pytest.mark.asyncio
async def test_the_sweep_reinstates_a_row_the_documents_assert_and_the_table_lost(
    counter_connection: psycopg.AsyncConnection[Any],
) -> None:
    """The recompute converges in both directions, so a row deleted out of band comes back."""
    await _load_catalog(counter_connection)
    await counter_connection.execute("DELETE FROM graph.member_of")

    await _refresh(counter_connection)

    assert await _rows(counter_connection, "SELECT member_artist_id, group_artist_id FROM graph.member_of") == [(A2, A1)]


@pytest.mark.asyncio
async def test_the_degrees_see_the_sweep_rather_than_the_rows_it_removed(
    counter_connection: psycopg.AsyncConnection[Any],
) -> None:
    """`artist_degree` sums `member_of`, so the sweep has to run first inside the same pass."""
    await _load_catalog(counter_connection)
    withdrawn = [(A1, {"id": A1, "name": "Alpha"}), (A2, {"id": A2, "name": "Beta"})]
    await _write_batch(counter_connection, "artists", withdrawn, suffix="v2")

    await _refresh(counter_connection)

    rows = await _rows(counter_connection, "SELECT artist_id, degree FROM graph.artist_degree ORDER BY artist_id")
    # A1 loses the membership it was the group of, 5 down to 4, and A2 loses its only
    # membership, 2 down to 1. A3 and A4 are untouched.
    assert rows == [(A1, 4), (A2, 1), (A3, 3), (A4, 1)]


@pytest.mark.asyncio
async def test_an_empty_entity_table_does_not_empty_the_relation(counter_connection: psycopg.AsyncConnection[Any]) -> None:
    """A failed artists load must not be read as a catalog in which nobody is in a band."""
    await _load_catalog(counter_connection)
    await counter_connection.execute("TRUNCATE artists")

    await _refresh(counter_connection)

    assert await _rows(counter_connection, "SELECT member_artist_id, group_artist_id FROM graph.member_of") == [(A2, A1)]


# ── Counter parity without weakening taxonomy ────────────────────────────────


@pytest.mark.asyncio
async def test_style_and_genre_counts_use_cooccurrence_while_part_of_keeps_taxonomy(
    refreshed: psycopg.AsyncConnection[Any],
) -> None:
    """Counters mirror Neo4j while `part_of` retains the single-genre assertion rule."""
    part_of = await _rows(refreshed, "SELECT style_name, genre_name FROM graph.part_of ORDER BY style_name")
    jazz = await _rows(refreshed, "SELECT style_count FROM graph.genre_stats WHERE name = 'Jazz'")
    pop_rock = await _rows(refreshed, "SELECT genre_count FROM graph.style_stats WHERE name = 'Pop Rock'")

    # Only the master and the second release carry exactly one genre.
    assert part_of == [("Fusion", "Rock"), ("Pop Rock", "Rock")]
    assert jazz == [(1,)]
    assert pop_rock == [(2,)]


# ── The extraction latch ─────────────────────────────────────────────────────
# Which signal fires the pass, and what survives a restart. Every call below goes through a
# separate `SingleConnectionPool`, which is what a restarted process would do: nothing is
# carried between them but the rows.
#
# Nothing here creates the relation. The promoted `groovemap-database-schema` declares
# `public.loader_extraction_latch`, the session fixture in `conftest.py` applies that
# initializer once, and every test below resolves the relation the way the running loader
# does: by probing for it. A stand-in `CREATE TABLE` used to stand where this comment is,
# and it is gone on purpose — a fixture that builds the table it then asserts against can
# only ever prove the fixture right.

FIRST_DUMP = "20260101"
SECOND_DUMP = "20260201"

# The same catalog read the probe makes, asked here by name so a schema revision that keeps
# the columns and drops the constraint fails this suite rather than every signal at runtime.
_KEY_CONSTRAINTS = """
SELECT conname, array_agg(attribute.attname::text ORDER BY attribute.attname::text)
FROM pg_constraint AS constraint_
JOIN pg_class AS relation ON relation.oid = constraint_.conrelid
JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
JOIN LATERAL unnest(constraint_.conkey) AS member(attnum) ON true
JOIN pg_attribute AS attribute ON attribute.attrelid = constraint_.conrelid AND attribute.attnum = member.attnum
WHERE namespace.nspname = %s AND relation.relname = %s AND constraint_.contype IN ('p', 'u')
GROUP BY constraint_.oid, conname
"""


@pytest_asyncio.fixture
async def declared_latch(counter_connection: psycopg.AsyncConnection[Any]) -> Any:
    """The relation as the startup probe resolves it out of the promoted schema."""
    latch = await probe_latch_relation(SingleConnectionPool(counter_connection), MagicMock())
    assert latch is not None, "the promoted schema must declare the extraction latch relation"
    return latch


async def _signal(connection: psycopg.AsyncConnection[Any], latch: Any, version: str, data_type: str) -> Any:
    return await record_extraction_signal(SingleConnectionPool(connection), latch, version, data_type)


async def _mark_refreshed(connection: psycopg.AsyncConnection[Any], latch: Any, version: str) -> None:
    async with connection.cursor() as cursor:
        await mark_extraction_refreshed(cursor, latch, version)


def test_the_latch_key_prefers_the_version_then_the_start_then_nothing() -> None:
    """Two dumps that both omit a version must not share one latch row.

    And a message carrying neither has no key at all, rather than a sentinel — which is
    what the third case asserts, and what the old name of this test got wrong.
    """
    assert extraction_latch_key({"version": "20260101", "started_at": "2026-01-01T00:00:00Z"}) == "20260101"
    assert extraction_latch_key({"started_at": "2026-01-01T00:00:00Z"}) == "2026-01-01T00:00:00Z"
    # And a message naming neither has no key, so nothing is recorded for it at all.
    assert extraction_latch_key({}) is None


@pytest.mark.asyncio
async def test_the_probe_finds_the_relation_the_promoted_schema_declares(counter_connection: psycopg.AsyncConnection[Any]) -> None:
    """The whole point of the repin, asserted against the real initializer rather than a fixture.

    Before it, this suite built the table itself and the promoted schema had none — so the
    probe's answer said something about the fixture and nothing about what a deployed loader
    would find. Here the only thing that has run is `create_postgres_schema`.
    """
    relation = await probe_latch_relation(SingleConnectionPool(counter_connection), MagicMock())

    assert relation is not None
    assert (relation.schema, relation.table) == LATCH_RELATION
    assert relation.keyed_on_loader is True
    assert relation.key_columns == KEY_COLUMNS


@pytest.mark.asyncio
async def test_the_promoted_schema_declares_the_key_the_upsert_conflicts_on(counter_connection: psycopg.AsyncConnection[Any]) -> None:
    """`ON CONFLICT (loader, version)` needs a real key, and the probe now insists on one.

    Asserted here against the catalog the probe reads, so a later schema revision that keeps
    the columns and loses the constraint fails this suite rather than every signal at runtime.
    """
    keys = await _rows(counter_connection, _KEY_CONSTRAINTS, LATCH_RELATION)

    assert keys == [("loader_extraction_latch_pkey", sorted(KEY_COLUMNS))]


@pytest.mark.asyncio
async def test_four_signals_fire_the_pass_once_and_write_only_this_loaders_rows(
    counter_connection: psycopg.AsyncConnection[Any], declared_latch: Any
) -> None:
    """One extraction end to end over the promoted relation: four signals, one pass, one row.

    The relation is shared with `musicbrainz-sql-loader`, so `loader` is asserted on every
    row this loader left behind — a row written without the discriminator would be one the
    other loader's probe could read as its own.
    """
    await _load_catalog(counter_connection)

    fired = []
    for data_type in ENTITY_TABLES:
        latch = await _signal(counter_connection, declared_latch, FIRST_DUMP, data_type)
        fired.append(latch.should_refresh(ENTITY_TABLES))
    assert fired == [False, False, False, True], "the pass fires on the fourth signal and no earlier"

    await refresh_derived_relations(SingleConnectionPool(counter_connection), MagicMock(), FIRST_DUMP, declared_latch)

    schema, table = LATCH_RELATION
    rows = await _rows(
        counter_connection,
        f"SELECT loader, version, signals, refreshed_at IS NOT NULL FROM {schema}.{table}",  # noqa: S608
    )
    assert rows == [(LOADER_DISCRIMINATOR, FIRST_DUMP, sorted(ENTITY_TABLES), True)]

    # And a fifth delivery of any of the four is now a cheap no-op rather than a second sweep.
    redelivered = await _signal(counter_connection, declared_latch, FIRST_DUMP, "releases")
    assert redelivered.should_refresh(ENTITY_TABLES) is False


@pytest.mark.asyncio
async def test_only_the_fourth_signal_of_an_extraction_fires_the_pass(counter_connection: psycopg.AsyncConnection[Any], declared_latch: Any) -> None:
    fired = []
    for data_type in ENTITY_TABLES:
        latch = await _signal(counter_connection, declared_latch, FIRST_DUMP, data_type)
        fired.append(latch.should_refresh(ENTITY_TABLES))

    assert fired == [False, False, False, True]


@pytest.mark.asyncio
async def test_a_restart_between_signals_loses_none_of_them(counter_connection: psycopg.AsyncConnection[Any], declared_latch: Any) -> None:
    """An in-memory set would lose the first two, and four would never be reached.

    Each call opens its own pool over the connection, so nothing but the rows carries from
    one to the next — which is exactly what the process has after a restart.
    """
    await _signal(counter_connection, declared_latch, FIRST_DUMP, "artists")
    await _signal(counter_connection, declared_latch, FIRST_DUMP, "labels")

    # ... the loader restarts here ...
    third = await _signal(counter_connection, declared_latch, FIRST_DUMP, "masters")
    fourth = await _signal(counter_connection, declared_latch, FIRST_DUMP, "releases")

    assert third.signals == frozenset({"artists", "labels", "masters"})
    assert third.should_refresh(ENTITY_TABLES) is False
    assert fourth.signals == frozenset(ENTITY_TABLES)
    assert fourth.should_refresh(ENTITY_TABLES) is True


@pytest.mark.asyncio
async def test_a_second_extraction_collects_its_own_four_signals(counter_connection: psycopg.AsyncConnection[Any], declared_latch: Any) -> None:
    """An unkeyed latch would fire on the FIRST signal of the second dump, and then thrice more."""
    for data_type in ENTITY_TABLES:
        await _signal(counter_connection, declared_latch, FIRST_DUMP, data_type)
    await _mark_refreshed(counter_connection, declared_latch, FIRST_DUMP)

    fired = []
    for data_type in ENTITY_TABLES:
        latch = await _signal(counter_connection, declared_latch, SECOND_DUMP, data_type)
        fired.append(latch.should_refresh(ENTITY_TABLES))

    assert fired == [False, False, False, True]


@pytest.mark.asyncio
async def test_a_straggler_from_a_superseded_extraction_fires_nothing(counter_connection: psycopg.AsyncConnection[Any], declared_latch: Any) -> None:
    """The next dump has started, so the late fourth signal of the last one must not sweep."""
    for data_type in ("artists", "labels", "masters"):
        await _signal(counter_connection, declared_latch, FIRST_DUMP, data_type)
    await _signal(counter_connection, declared_latch, SECOND_DUMP, "artists")

    straggler = await _signal(counter_connection, declared_latch, FIRST_DUMP, "releases")

    assert straggler.signals == frozenset(ENTITY_TABLES)
    assert straggler.superseded is True
    assert straggler.should_refresh(ENTITY_TABLES) is False


@pytest.mark.asyncio
async def test_a_redelivered_signal_after_a_successful_pass_fires_nothing(
    counter_connection: psycopg.AsyncConnection[Any], declared_latch: Any
) -> None:
    for data_type in ENTITY_TABLES:
        await _signal(counter_connection, declared_latch, FIRST_DUMP, data_type)
    await _mark_refreshed(counter_connection, declared_latch, FIRST_DUMP)

    redelivered = await _signal(counter_connection, declared_latch, FIRST_DUMP, "releases")

    assert redelivered.already_signalled is True
    assert redelivered.already_refreshed is True
    assert redelivered.should_refresh(ENTITY_TABLES) is False


@pytest.mark.asyncio
async def test_a_failed_pass_leaves_the_extraction_unstamped_so_the_retry_runs(
    counter_connection: psycopg.AsyncConnection[Any], declared_latch: Any
) -> None:
    """The pass nacks its delivery on failure, and `refreshed_at` is stamped on its own transaction."""
    for data_type in ENTITY_TABLES:
        await _signal(counter_connection, declared_latch, FIRST_DUMP, data_type)

    retry = await _signal(counter_connection, declared_latch, FIRST_DUMP, "releases")

    assert retry.already_signalled is True
    assert retry.already_refreshed is False
    assert retry.should_refresh(ENTITY_TABLES) is True


@pytest.mark.asyncio
async def test_a_shared_relation_keeps_the_two_loaders_apart(counter_connection: psycopg.AsyncConnection[Any], declared_latch: Any) -> None:
    """musicbrainz-sql-loader's rows must neither satisfy nor supersede this loader's."""
    schema, table = LATCH_RELATION
    await counter_connection.execute(
        f"INSERT INTO {schema}.{table} (loader, version, signals) VALUES ('musicbrainz', %s, %s)",  # noqa: S608
        (FIRST_DUMP, list(ENTITY_TABLES)),
    )

    ours = await _signal(counter_connection, declared_latch, FIRST_DUMP, "artists")

    assert ours.signals == frozenset({"artists"})
    assert ours.superseded is False
    rows = await _rows(counter_connection, f"SELECT loader, version FROM {schema}.{table} ORDER BY loader")  # noqa: S608
    assert rows == [(LOADER_DISCRIMINATOR, FIRST_DUMP), ("musicbrainz", FIRST_DUMP)]


@pytest.mark.asyncio
async def test_the_pass_stamps_the_extraction_on_its_own_transaction(counter_connection: psycopg.AsyncConnection[Any], declared_latch: Any) -> None:
    """A committed pass marks the extraction done, which is what makes a redelivery cheap."""
    await _load_catalog(counter_connection)
    for data_type in ENTITY_TABLES:
        await _signal(counter_connection, declared_latch, FIRST_DUMP, data_type)

    await refresh_derived_relations(SingleConnectionPool(counter_connection), MagicMock(), FIRST_DUMP, declared_latch)

    schema, table = LATCH_RELATION
    stamped = await _rows(counter_connection, f"SELECT refreshed_at IS NOT NULL FROM {schema}.{table}")  # noqa: S608
    assert stamped == [(True,)]
