"""The statements `tableinator.graph_writer` issues, and the order it issues them in."""

from typing import Any

import pytest

from tableinator.graph_writer import purge_document_graph, write_document_graph


class RecordingCursor:
    """Record every statement a write makes, in order, with its parameters."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, Any]] = []

    async def execute(self, statement: Any, parameters: Any = None) -> None:
        self.calls.append(("execute", statement.as_string(None), parameters))

    async def executemany(self, statement: Any, rows: Any) -> None:
        self.calls.append(("executemany", statement.as_string(None), list(rows)))

    def statements(self) -> list[str]:
        return [statement for _kind, statement, _parameters in self.calls]

    def index_of(self, fragment: str) -> int:
        """Return the position of the first statement containing FRAGMENT."""
        for position, statement in enumerate(self.statements()):
            if fragment in statement:
                return position
        raise AssertionError(f"no statement contained {fragment!r}: {self.statements()}")

    def rows_for(self, fragment: str) -> Any:
        for _kind, statement, parameters in self.calls:
            if fragment in statement:
                return parameters
        raise AssertionError(f"no statement contained {fragment!r}")


_RELEASE = {
    "artists": [{"id": "1"}],
    "genres": ["Rock"],
    "styles": ["Indie Rock"],
    "extraartists": [{"id": "1", "name": "Nigel Godrich", "role": "Producer"}],
    "companies": {"items": [{"discogs_id": "9", "name": "Sony DADC", "role": "Pressed By", "role_category": "manufacture"}]},
    "media": {"items": [{"medium": "vinyl_12", "family": "vinyl", "qty": 2}]},
}


@pytest.mark.asyncio
async def test_nothing_is_executed_for_an_empty_batch() -> None:
    cursor = RecordingCursor()

    await write_document_graph(cursor, "releases", [])

    assert cursor.calls == []


@pytest.mark.asyncio
async def test_every_vertex_is_written_before_every_edge() -> None:
    """`graph.part_of` and `graph.in_family` inner join the vertex tables.

    An edge written before its endpoints is silently absent from both views, so the
    ordering is the correctness argument rather than an optimisation.
    """
    cursor = RecordingCursor()

    await write_document_graph(cursor, "releases", [("r1", _RELEASE)])

    last_vertex = max(cursor.index_of(f'INTO "graph"."{relation}"') for relation in ("genre", "style", "person", "media_family", "medium", "company"))
    first_edge = min(
        cursor.index_of(f'INTO "graph"."{relation}"') for relation in ("by_artist", "in_genre", "credited_on", "credited_to", "issued_on")
    )

    assert last_vertex < first_edge


@pytest.mark.asyncio
async def test_the_document_scoped_delete_precedes_the_insert_it_re_creates() -> None:
    """A release corrected from Vinyl to CD must not keep the vinyl row forever."""
    cursor = RecordingCursor()

    await write_document_graph(cursor, "releases", [("r1", _RELEASE)])

    assert cursor.index_of('DELETE FROM "graph"."issued_on"') < cursor.index_of('INTO "graph"."issued_on"')


@pytest.mark.asyncio
async def test_the_delete_is_scoped_to_the_documents_being_written() -> None:
    cursor = RecordingCursor()

    await write_document_graph(cursor, "releases", [("r1", _RELEASE), ("r2", _RELEASE)])

    statement = next(s for s in cursor.statements() if 'DELETE FROM "graph"."by_artist"' in s)
    assert '"release_id" = ANY(%s)' in statement
    assert cursor.rows_for('DELETE FROM "graph"."by_artist"') == (["r1", "r2"],)


@pytest.mark.asyncio
async def test_a_shared_relation_is_deleted_only_at_this_loaders_own_source() -> None:
    """`musicbrainz-sql-loader` writes its own rows over the same release."""
    cursor = RecordingCursor()

    await write_document_graph(cursor, "releases", [("r1", _RELEASE)])

    for fragment in ('DELETE FROM "graph"."issued_on"', 'DELETE FROM "graph"."credited_to"'):
        statement = next(s for s in cursor.statements() if fragment in s)
        assert '"source" = %s' in statement
        assert cursor.rows_for(fragment) == (["r1"], "discogs")


@pytest.mark.asyncio
async def test_no_relation_outside_the_document_scope_is_ever_deleted() -> None:
    """`member_of` and `same_as` have no column naming one asserting document."""
    cursor = RecordingCursor()

    await write_document_graph(cursor, "artists", [("a1", {"members": [{"id": "2"}], "aliases": [{"id": "3"}]})])
    await write_document_graph(cursor, "releases", [("r1", _RELEASE)])

    deletes = [statement for statement in cursor.statements() if statement.startswith("DELETE FROM")]
    assert not any('"member_of"' in statement for statement in deletes)
    assert not any('"same_as"' in statement for statement in deletes)


@pytest.mark.asyncio
async def test_every_insert_tolerates_a_conflict() -> None:
    """Shared vertices, and the two relations written additively, must converge not raise."""
    cursor = RecordingCursor()

    await write_document_graph(cursor, "releases", [("r1", _RELEASE)])

    inserts = [statement for statement in cursor.statements() if statement.startswith("INSERT INTO")]
    assert inserts
    assert all(statement.endswith("ON CONFLICT DO NOTHING") for statement in inserts)


@pytest.mark.asyncio
async def test_the_generated_role_category_is_never_named_in_an_insert() -> None:
    """`graph.credited_on.role_category` is GENERATED; naming it is an error."""
    cursor = RecordingCursor()

    await write_document_graph(cursor, "releases", [("r1", _RELEASE)])

    statement = next(s for s in cursor.statements() if 'INTO "graph"."credited_on"' in s)
    assert '"role_category"' not in statement
    assert cursor.rows_for('INTO "graph"."credited_on"') == [("Nigel Godrich", "r1", "Producer")]


@pytest.mark.asyncio
async def test_the_plain_role_category_of_credited_to_is_written() -> None:
    cursor = RecordingCursor()

    await write_document_graph(cursor, "releases", [("r1", _RELEASE)])

    assert cursor.rows_for('INTO "graph"."credited_to"') == [("r1", "9", "Pressed By", "manufacture", "discogs")]


@pytest.mark.asyncio
async def test_one_row_is_written_once_when_two_documents_assert_it() -> None:
    """Two artists naming each other assert the same `member_of` row."""
    cursor = RecordingCursor()

    await write_document_graph(
        cursor,
        "artists",
        [("a1", {"groups": [{"id": "a2"}]}), ("a2", {"members": [{"id": "a1"}]})],
    )

    assert cursor.rows_for('INTO "graph"."member_of"') == [("a1", "a2")]


@pytest.mark.asyncio
async def test_a_label_document_executes_nothing() -> None:
    cursor = RecordingCursor()

    await write_document_graph(cursor, "labels", [("l1", {"sublabels": [{"id": "2"}]})])

    assert cursor.calls == []


class TestPurge:
    """The sweep that removes the edges of the documents a stale-row purge deletes."""

    @pytest.mark.asyncio
    async def test_a_release_purge_reaches_every_relation_a_release_owns(self) -> None:
        cursor = RecordingCursor()

        await purge_document_graph(cursor, "releases", "2026-07-20T00:00:00+00:00")

        assert [statement.split('"')[3] for statement in cursor.statements()] == [
            "by_artist",
            "on_label",
            "derived_from",
            "in_genre",
            "in_style",
            "credited_on",
            "credited_to",
            "issued_on",
        ]

    @pytest.mark.asyncio
    async def test_the_sweep_joins_the_stale_documents_rather_than_listing_them(self) -> None:
        """No deleted id is streamed back, so a large purge stays O(1) in the client."""
        cursor = RecordingCursor()

        await purge_document_graph(cursor, "masters", "2026-07-20T00:00:00+00:00")

        for statement in cursor.statements():
            assert "RETURNING" not in statement
            assert 'USING "masters" AS stale' in statement
            assert "stale.updated_at < %s" in statement

    @pytest.mark.asyncio
    async def test_a_shared_relation_is_swept_only_at_this_loaders_own_source(self) -> None:
        cursor = RecordingCursor()

        await purge_document_graph(cursor, "releases", "2026-07-20T00:00:00+00:00")

        assert cursor.rows_for('"graph"."issued_on"') == ("2026-07-20T00:00:00+00:00", "discogs")
        assert cursor.rows_for('"graph"."by_artist"') == ("2026-07-20T00:00:00+00:00",)

    @pytest.mark.asyncio
    async def test_a_purged_artist_takes_only_the_relations_it_asserted(self) -> None:
        """`graph.by_artist` belongs to the releases that name the artist, which still do."""
        cursor = RecordingCursor()

        await purge_document_graph(cursor, "artists", "2026-07-20T00:00:00+00:00")

        assert [statement.split('"')[3] for statement in cursor.statements()] == ["alias_of"]

    @pytest.mark.asyncio
    async def test_a_label_purge_sweeps_nothing(self) -> None:
        """`sublabel_of` is a view, so a label document never wrote a row to remove."""
        cursor = RecordingCursor()

        await purge_document_graph(cursor, "labels", "2026-07-20T00:00:00+00:00")

        assert cursor.calls == []
