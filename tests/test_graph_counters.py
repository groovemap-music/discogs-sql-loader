"""The derived-relation refresh: its definitions, its order, and its reconciliation.

Two halves, split by what can be checked without a database.

`tableinator.graph_counters` carries a COPY of the seven counter bodies rather than an
import, because `groovemap-database-schema` is a dev dependency and runtime code cannot
reach it. That copy is the thing most able to drift silently, so the first half of this
suite holds it byte-identical to the pinned original — a schema revision that changes a
definition fails here rather than in production. The counters' hand-computed values are
checked where the definitions can actually be executed, against a real PostgreSQL, in
`tests/integration/test_graph_counters.py`.

The second half is the `member_of` / `same_as` reconciliation, which IS checkable without a
database: what it asserts is `graph_derivation`'s answer for a set of documents, and what
it deletes is the complement. Both are hand-computed from the fixture below.
"""

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

import pytest
from groovemap_schema.postgres import _COUNTER_BOOTSTRAP, _COUNTER_COLUMNS

from tableinator.graph_counters import (
    ADDITIVE_RELATIONS,
    COUNTER_BODIES,
    COUNTER_COLUMNS,
    PATH_REFRESH_FUNCTIONS,
    REFRESH_ORDER,
    reconcile_additive_edges,
    refresh_counter_relations,
    refresh_derived_relations,
)


if TYPE_CHECKING:
    from collections.abc import AsyncIterator


# ── The doubles ──────────────────────────────────────────────────────────────


class RecordingCopy:
    """One COPY, recording the rows written into it."""

    def __init__(self, cursor: RecordingCursor, statement: str) -> None:
        self.cursor = cursor
        self.statement = statement

    async def __aenter__(self) -> RecordingCopy:
        return self

    async def __aexit__(self, *_exc: Any) -> bool:
        return False

    async def write_row(self, row: Any) -> None:
        self.cursor.copied.setdefault(self.statement, []).append(tuple(row))


class RecordingCursor:
    """Record every statement, and answer the reads the refresh makes.

    `counts` answers `SELECT count(*) FROM <entity table>`; `pages` hands out successive
    keyset pages of documents per entity table, so a relation's document stream is scripted
    exactly as PostgreSQL would deliver it.
    """

    def __init__(
        self,
        counts: dict[str, int] | None = None,
        pages: dict[str, list[list[Any]]] | None = None,
        fail_once_on: str | None = None,
    ) -> None:
        self.calls: list[tuple[str, Any]] = []
        self.copied: dict[str, list[tuple[Any, ...]]] = {}
        self.rowcount = 0
        self._counts = counts or {}
        self._pages = {table: list(pages_) for table, pages_ in (pages or {}).items()}
        self._one: tuple[Any, ...] | None = None
        self._all: list[Any] = []
        self._fail_once_on = fail_once_on

    async def execute(self, statement: Any, parameters: Any = None) -> None:
        text = statement if isinstance(statement, str) else statement.as_string(None)
        self.calls.append((text, parameters))
        if self._fail_once_on is not None and self._fail_once_on in text:
            self._fail_once_on = None
            raise RuntimeError(f"scripted failure on {text}")
        self.rowcount = 0
        if text.startswith("SELECT count(*) FROM"):
            self._one = (self._counts.get(_first_identifier(text), 0),)
        elif text.startswith("SELECT data_id, data FROM"):
            remaining = self._pages.get(_first_identifier(text), [])
            self._all = remaining.pop(0) if remaining else []

    async def fetchone(self) -> tuple[Any, ...] | None:
        return self._one

    async def fetchall(self) -> list[Any]:
        return self._all

    def copy(self, statement: Any) -> RecordingCopy:
        return RecordingCopy(self, statement.as_string(None))

    def statements(self) -> list[str]:
        return [statement for statement, _parameters in self.calls]

    def index_of(self, fragment: str) -> int:
        for position, statement in enumerate(self.statements()):
            if fragment in statement:
                return position
        raise AssertionError(f"no statement contained {fragment!r}: {self.statements()}")

    def indexes_of(self, fragment: str) -> list[int]:
        return [position for position, statement in enumerate(self.statements()) if fragment in statement]


def _first_identifier(statement: str) -> str:
    """Return the first double-quoted identifier of a composed statement."""
    return statement.split('"')[1]


class RecordingLogger:
    """Collect the structured lines the refresh emits."""

    def __init__(self) -> None:
        self.lines: list[tuple[str, str, dict[str, Any]]] = []

    def info(self, event: str, **fields: Any) -> None:
        self.lines.append(("info", event, fields))

    def warning(self, event: str, **fields: Any) -> None:
        self.lines.append(("warning", event, fields))

    def error(self, event: str, **fields: Any) -> None:
        self.lines.append(("error", event, fields))

    def events(self, level: str) -> list[str]:
        return [event for line_level, event, _fields in self.lines if line_level == level]

    def fields_for(self, fragment: str) -> dict[str, Any]:
        for _level, event, fields in self.lines:
            if fragment in event:
                return fields
        raise AssertionError(f"no line contained {fragment!r}: {[event for _l, event, _f in self.lines]}")


class FakeConnection:
    """One pooled connection whose transaction and cursor are the recording double."""

    def __init__(self, cursor: RecordingCursor) -> None:
        self._cursor = cursor
        self.autocommit = True
        self.commits = 0
        self.rollbacks = 0

    async def set_autocommit(self, value: bool) -> None:
        self.autocommit = value

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        try:
            yield
        except BaseException:
            self.rollbacks += 1
            raise
        else:
            self.commits += 1

    @asynccontextmanager
    async def cursor(self) -> AsyncIterator[RecordingCursor]:
        yield self._cursor


class FakePool:
    def __init__(self, cursor: RecordingCursor) -> None:
        self.connection_double = FakeConnection(cursor)

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[FakeConnection]:
        yield self.connection_double


# ── The definitions ──────────────────────────────────────────────────────────


def test_every_counter_body_is_the_pinned_schemas_own() -> None:
    """The seven runtime copies match their pinned schema bodies character for character.

    The promoted schema also carries vertex_degree in this map, but the loader invokes its
    schema-owned refresh function instead of copying that eighth body into runtime code.
    """
    assert {relation: _COUNTER_BOOTSTRAP[relation] for relation in REFRESH_ORDER} == COUNTER_BODIES
    assert set(_COUNTER_BOOTSTRAP) - set(COUNTER_BODIES) == {"vertex_degree"}


def test_every_counter_column_list_is_the_pinned_schemas_own() -> None:
    assert {relation: _COUNTER_COLUMNS[relation] for relation in REFRESH_ORDER} == COUNTER_COLUMNS


def test_the_refresh_covers_exactly_the_seven_counter_relations() -> None:
    assert REFRESH_ORDER == (
        "genre_stats",
        "style_stats",
        "label_stats",
        "artist_degree",
        "release_degree_base",
        "artist_genre",
        "label_genre",
    )
    assert set(REFRESH_ORDER) == set(COUNTER_COLUMNS)


def test_release_degree_base_is_the_loaders_half_only() -> None:
    """`COUNT { (r)--() }` counts COLLECTED and WANTS; `graph.release_degree` adds them live.

    The base body must never reach `user_collections` or `user_wantlists` — those rows are
    `catalog-api`'s, arrive between dumps, and a counter that included them would be stale
    the moment a user saved a release.
    """
    body = COUNTER_BODIES["release_degree_base"]
    assert "user_collections" not in body
    assert "user_wantlists" not in body
    assert body.count("UNION ALL") == 7


def test_label_stats_counts_a_release_once_however_it_fans_out() -> None:
    """A release with three artists and two genres is one release for its label.

    `label_cypher` counts `count(DISTINCT r)`; the join to `by_artist` and `in_genre` here
    multiplies rows, so only a DISTINCT keeps the two readings equal.
    """
    assert "count(DISTINCT on_label.release_id) AS release_count" in COUNTER_BODIES["label_stats"]
    assert "LEFT JOIN graph.by_artist" in COUNTER_BODIES["label_stats"]
    assert "LEFT JOIN graph.in_genre" in COUNTER_BODIES["label_stats"]


@pytest.mark.parametrize(("relation", "vertex"), [("genre_stats", "graph.genre"), ("style_stats", "graph.style")])
def test_a_vocabulary_row_no_release_names_still_gets_a_row(relation: str, vertex: str) -> None:
    """graphinator writes explicit zeros onto every `:Genre`, including one with no releases.

    Driving the body off the vertex table rather than off the edge table is what produces
    the zero row; the counts themselves are `count(...)`, which is 0 rather than NULL.
    """
    body = COUNTER_BODIES[relation]
    assert body.rstrip().endswith(f"FROM {vertex} AS {vertex.split('.')[1]}")
    assert "count(*)" in body


@pytest.mark.parametrize("relation", ["genre_stats", "style_stats"])
def test_first_year_stays_null_when_no_release_states_one(relation: str) -> None:
    """`min(r.year)` over an empty match is NULL in Cypher and `min(...)` is NULL here."""
    assert "min(NULLIF(btrim(release.year), '')::integer)" in COUNTER_BODIES[relation]
    assert "COALESCE" not in COUNTER_BODIES[relation]


# ── The statements ───────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_each_counter_is_emptied_before_it_is_refilled() -> None:
    """`ON CONFLICT DO NOTHING` converges upward only, so the relation is emptied first."""
    cursor = RecordingCursor()

    await refresh_counter_relations(cursor, RecordingLogger())

    for relation in REFRESH_ORDER:
        truncate = cursor.index_of(f'TRUNCATE "graph"."{relation}"')
        refill = cursor.index_of(f'INSERT INTO "graph"."{relation}"')
        assert truncate < refill, f"{relation} was refilled before it was emptied"


@pytest.mark.asyncio
async def test_each_counter_is_refilled_with_its_own_schema_body() -> None:
    cursor = RecordingCursor()

    await refresh_counter_relations(cursor, RecordingLogger())

    for relation in REFRESH_ORDER:
        statement = cursor.statements()[cursor.index_of(f'INSERT INTO "graph"."{relation}"')]
        assert COUNTER_BODIES[relation].strip() in statement
        projection = ", ".join(f'"{column}"' for column in COUNTER_COLUMNS[relation])
        assert f"({projection}) SELECT {projection} FROM" in statement


@pytest.mark.asyncio
async def test_every_relation_reports_its_row_count_and_duration() -> None:
    logger = RecordingLogger()

    await refresh_counter_relations(RecordingCursor(), logger)

    for relation in REFRESH_ORDER:
        fields = logger.fields_for(f"graph.{relation}")
        assert fields["relation"] == f"graph.{relation}"
        assert "rows" in fields
        assert fields["duration_seconds"] >= 0


# ── The reconciliation ───────────────────────────────────────────────────────
# Hand-computed from these documents. `gw-a1` names two members and `gw-a2` names the same
# band back, so the reciprocal pair is one edge; `gw-a3` no longer names a group and is no
# longer named as a member, so the membership it once asserted is asserted by nothing.

ARTIST_DOCUMENTS = [
    ("gw-a1", {"id": "gw-a1", "members": [{"id": "gw-a2"}]}),
    ("gw-a2", {"id": "gw-a2", "groups": [{"id": "gw-a1"}]}),
]

# `(member_artist_id, group_artist_id)`: both documents state the same membership.
ASSERTED_MEMBER_OF = [("gw-a2", "gw-a1")]

RELEASE_DOCUMENTS = [
    (
        "gw-r1",
        {
            "id": "gw-r1",
            "extraartists": [
                {"id": "gw-a9", "name": "Nigel Godrich", "role": "Producer"},
                {"id": "gw-a9", "name": "Nigel Godrich", "role": "Mixed By"},
                {"name": "No Id At All", "role": "Design"},
            ],
        },
    ),
]

# `(person_name, artist_id)`: two roles for one identified person are one `same_as` row,
# and the credit with no id asserts none.
ASSERTED_SAME_AS = [("Nigel Godrich", "gw-a9")]


@pytest.mark.asyncio
async def test_the_reconciliation_asserts_exactly_what_the_documents_derive() -> None:
    cursor = RecordingCursor(
        counts={"artists": 2, "releases": 1},
        pages={"artists": [ARTIST_DOCUMENTS], "releases": [RELEASE_DOCUMENTS]},
    )

    await reconcile_additive_edges(cursor, RecordingLogger())

    member_of = cursor.copied['COPY "asserted_member_of" ("member_artist_id", "group_artist_id") FROM STDIN']
    same_as = cursor.copied['COPY "asserted_same_as" ("person_name", "artist_id") FROM STDIN']
    assert member_of == ASSERTED_MEMBER_OF
    assert same_as == ASSERTED_SAME_AS


@pytest.mark.asyncio
async def test_the_reconciliation_deletes_every_row_no_document_asserts() -> None:
    """The set difference is an anti-join in PostgreSQL, so no surviving row is streamed back."""
    cursor = RecordingCursor(
        counts={"artists": 2, "releases": 1},
        pages={"artists": [ARTIST_DOCUMENTS], "releases": [RELEASE_DOCUMENTS]},
    )

    await reconcile_additive_edges(cursor, RecordingLogger())

    assert cursor.statements()[cursor.index_of('DELETE FROM "graph"."member_of"')] == (
        'DELETE FROM "graph"."member_of" AS edge WHERE NOT EXISTS '
        '(SELECT 1 FROM "asserted_member_of" AS asserted WHERE asserted."member_artist_id" = edge."member_artist_id" '
        'AND asserted."group_artist_id" = edge."group_artist_id")'
    )
    assert cursor.statements()[cursor.index_of('DELETE FROM "graph"."same_as"')] == (
        'DELETE FROM "graph"."same_as" AS edge WHERE NOT EXISTS '
        '(SELECT 1 FROM "asserted_same_as" AS asserted WHERE asserted."person_name" = edge."person_name" '
        'AND asserted."artist_id" = edge."artist_id")'
    )


@pytest.mark.asyncio
async def test_the_reconciliation_adds_back_what_the_documents_assert_and_the_table_lacks() -> None:
    cursor = RecordingCursor(counts={"artists": 2, "releases": 1}, pages={"artists": [ARTIST_DOCUMENTS]})

    await reconcile_additive_edges(cursor, RecordingLogger())

    for relation in ADDITIVE_RELATIONS:
        insert = cursor.index_of(f'INSERT INTO "graph"."{relation}"')
        delete = cursor.index_of(f'DELETE FROM "graph"."{relation}"')  # noqa: S608
        assert "ON CONFLICT DO NOTHING" in cursor.statements()[insert]
        assert "SELECT DISTINCT" in cursor.statements()[insert]
        assert insert < delete


@pytest.mark.asyncio
async def test_the_asserted_set_is_accumulated_in_postgresql_not_in_this_process() -> None:
    """One temporary table per relation, dropped on commit, filled a page at a time."""
    cursor = RecordingCursor(counts={"artists": 2, "releases": 1}, pages={"artists": [ARTIST_DOCUMENTS, ARTIST_DOCUMENTS]})

    await reconcile_additive_edges(cursor, RecordingLogger())

    for relation in ADDITIVE_RELATIONS:
        create = cursor.statements()[cursor.index_of(f'CREATE TEMPORARY TABLE "asserted_{relation}"')]
        assert create.endswith("ON COMMIT DROP")
        assert cursor.index_of(f'ANALYZE "asserted_{relation}"') < cursor.index_of(f'INSERT INTO "graph"."{relation}"')


@pytest.mark.asyncio
async def test_the_document_scan_pages_on_the_key_and_stops_on_a_short_page() -> None:
    cursor = RecordingCursor(counts={"artists": 2}, pages={"artists": [ARTIST_DOCUMENTS]})

    await reconcile_additive_edges(cursor, RecordingLogger())

    pages = [cursor.calls[position] for position in cursor.indexes_of('SELECT data_id, data FROM "artists"')]
    assert len(pages) == 1, "a page shorter than the page size must end the scan"
    assert pages[0][1][0] == "", "the first page starts before every key"


@pytest.mark.asyncio
async def test_an_empty_entity_table_is_a_failed_load_rather_than_an_empty_catalog() -> None:
    """Emptying `member_of` because `artists` is empty is the mistake `purge_stale_rows` refuses."""
    cursor = RecordingCursor(counts={"artists": 0, "releases": 0})
    logger = RecordingLogger()

    results = await reconcile_additive_edges(cursor, logger)

    assert results == {"member_of": (0, 0), "same_as": (0, 0)}
    assert not cursor.indexes_of("DELETE FROM")
    assert len(logger.events("warning")) == 2


@pytest.mark.asyncio
async def test_each_relation_reports_what_it_inserted_and_deleted() -> None:
    logger = RecordingLogger()
    cursor = RecordingCursor(counts={"artists": 2, "releases": 1}, pages={"artists": [ARTIST_DOCUMENTS]})

    await reconcile_additive_edges(cursor, logger)

    fields = logger.fields_for("graph.member_of")
    assert fields["asserted"] == len(ASSERTED_MEMBER_OF)
    assert fields["inserted"] >= 0
    assert fields["deleted"] >= 0
    assert fields["duration_seconds"] >= 0


# ── The pass as a whole ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_the_additive_edges_are_reconciled_before_the_degrees_sum_them() -> None:
    """`artist_degree` unions `member_of` and `same_as`, so the sweep has to precede it."""
    cursor = RecordingCursor(counts={"artists": 2, "releases": 1}, pages={"artists": [ARTIST_DOCUMENTS]})

    await refresh_derived_relations(FakePool(cursor), RecordingLogger(), "20260101")

    assert cursor.index_of('DELETE FROM "graph"."member_of"') < cursor.index_of('TRUNCATE "graph"."artist_degree"')
    assert cursor.index_of('DELETE FROM "graph"."same_as"') < cursor.index_of('TRUNCATE "graph"."artist_degree"')


@pytest.mark.asyncio
async def test_the_path_relations_refresh_once_in_dependency_order_before_the_latch_stamp() -> None:
    """The degree refresh sums the union, and the stamp certifies both completed exactly once."""
    from tableinator.extraction_latch import LatchRelation

    cursor = RecordingCursor(counts={"artists": 2, "releases": 1}, pages={"artists": [ARTIST_DOCUMENTS]})
    latch = LatchRelation(schema="public", table="loader_extraction_latch")

    await refresh_derived_relations(FakePool(cursor), RecordingLogger(), "20260101", latch)

    counter = cursor.index_of('TRUNCATE "graph"."label_genre"')
    member_of = cursor.index_of("SELECT * FROM graph.refresh_artist_member_of()")
    degree = cursor.index_of("SELECT * FROM graph.refresh_vertex_degree()")
    stamp = cursor.index_of("SET refreshed_at = NOW()")
    assert counter < member_of < degree < stamp
    for function in PATH_REFRESH_FUNCTIONS:
        assert len(cursor.indexes_of(f"SELECT * FROM graph.{function}()")) == 1  # noqa: S608 -- fixed internal names


@pytest.mark.asyncio
async def test_a_path_refresh_failure_rolls_back_without_a_stamp_and_the_retry_repeats_the_pass() -> None:
    """The latch remains eligible and the retry re-runs both ordered schema refreshes."""
    from tableinator.extraction_latch import LatchRelation

    cursor = RecordingCursor(
        counts={"artists": 2, "releases": 1},
        pages={"artists": [ARTIST_DOCUMENTS]},
        fail_once_on="graph.refresh_vertex_degree()",
    )
    pool = FakePool(cursor)
    latch = LatchRelation(schema="public", table="loader_extraction_latch")

    with pytest.raises(RuntimeError, match="refresh_vertex_degree"):
        await refresh_derived_relations(pool, RecordingLogger(), "20260101", latch)

    assert pool.connection_double.rollbacks == 1
    assert not cursor.indexes_of("SET refreshed_at = NOW()")

    await refresh_derived_relations(pool, RecordingLogger(), "20260101", latch)

    assert pool.connection_double.commits == 1
    assert len(cursor.indexes_of("SELECT * FROM graph.refresh_artist_member_of()")) == 2
    assert len(cursor.indexes_of("SELECT * FROM graph.refresh_vertex_degree()")) == 2
    assert len(cursor.indexes_of("SET refreshed_at = NOW()")) == 1


@pytest.mark.asyncio
async def test_the_whole_pass_runs_on_one_non_autocommit_transaction() -> None:
    """The pool hands out an AUTOCOMMIT connection; without this every TRUNCATE commits alone."""
    cursor = RecordingCursor(counts={"artists": 2, "releases": 1}, pages={"artists": [ARTIST_DOCUMENTS]})
    pool = FakePool(cursor)

    await refresh_derived_relations(pool, RecordingLogger(), "20260101")

    assert pool.connection_double.autocommit is False


@pytest.mark.asyncio
async def test_the_pass_reports_its_row_counts_and_duration() -> None:
    logger = RecordingLogger()
    cursor = RecordingCursor(counts={"artists": 2, "releases": 1}, pages={"artists": [ARTIST_DOCUMENTS]})

    counts = await refresh_derived_relations(FakePool(cursor), logger, "20260101")

    assert set(counts) == set(REFRESH_ORDER)
    fields = logger.fields_for("Refreshed the derived graph relations")
    assert set(fields["rows"]) == set(REFRESH_ORDER)
    assert set(fields["reconciled"]) == set(ADDITIVE_RELATIONS)
    assert fields["duration_seconds"] >= 0


@pytest.mark.asyncio
async def test_the_extraction_is_stamped_on_the_passs_own_transaction() -> None:
    """A pass that rolls back must leave the extraction unstamped, so the retry re-runs it."""
    from tableinator.extraction_latch import LOADER_DISCRIMINATOR, LatchRelation

    latch = LatchRelation(schema="public", table="loader_extraction_latch")
    cursor = RecordingCursor(counts={"artists": 2, "releases": 1}, pages={"artists": [ARTIST_DOCUMENTS]})

    await refresh_derived_relations(FakePool(cursor), RecordingLogger(), "20260101", latch)

    stamp = cursor.index_of("SET refreshed_at = NOW()")
    assert cursor.calls[stamp][1] == {"version": "20260101", "loader": LOADER_DISCRIMINATOR}
    assert stamp > cursor.index_of("SELECT * FROM graph.refresh_vertex_degree()"), "the stamp must follow the last relation it certifies"


@pytest.mark.asyncio
async def test_a_pass_driven_without_a_latch_stamps_nothing() -> None:
    """A caller driving the counters directly has no extraction to certify."""
    cursor = RecordingCursor(counts={"artists": 2, "releases": 1}, pages={"artists": [ARTIST_DOCUMENTS]})

    await refresh_derived_relations(FakePool(cursor), RecordingLogger(), "20260101")

    assert not cursor.indexes_of("SET refreshed_at = NOW()")
