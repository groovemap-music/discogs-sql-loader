"""The loader's edge sets, held against the graph enricher's, on one catalog of events.

Phase 4 of the property-graph migration retires `discogs-graph-enricher` once this loader
writes the same graph. That is a claim about two stores, so it cannot be checked inside
either one: `tests/test_graph_derivation.py` holds each rule to the enricher function it
mirrors by reading both implementations, and `tests/integration/test_graph_writes.py`
counts what reaches PostgreSQL. Neither would notice the two agreeing about the wrong
thing. This suite plays ONE sequence of fixture events through BOTH services — the loader
into a real PostgreSQL, the enricher into a real Neo4j — and compares, per edge label, the
set of `(source, target, properties)` tuples the two stores ended up holding.

It is opt-in. `just test-parity` starts both containers and selects the `parity` marker;
`just test-integration` deselects it, and `just check` never reaches it, because it is the
only lane in this repository that needs a second engine.

## How the two sides are driven

Both sides are driven through their BATCH write path, on the same documents, in the same
order, with the same `sha256` on every document. `PostgreSQLBatchWriter.process_batch` is
the loader's; `Neo4jBatchProcessor.flush_queue` is the enricher's, reached through
`_engine.submit` exactly as the enricher's own `tests/integration/test_real_neo4j_writes.py`
reaches it. Neither side sees a broker, and neither re-normalizes: the same `dict` object's
content goes to both, so a divergence can only come from the projections themselves.

The enricher is imported rather than run as a container. A container would need RabbitMQ
and an image build to deliver the same events, and would still be the same Python projecting
the same documents; importing it keeps the fixture the single source of both stores' input.
`scripts/test-parity.sh` installs it at a pinned revision with `--no-deps`, because the
enricher pins a `groovemap-runtime` revision this repository's persistence contract does
not; that keeps THIS repository's runtime in place, so both halves share one `common.media`
and one `common.credit_roles` and the comparison measures the loader rather than a runtime
skew. `test_the_pinned_enricher_is_the_one_under_comparison` holds the installed revision to
the one the script pinned, so a stale environment cannot quietly make a run meaningless.

## What is compared, and what is not

`RELATION_MAPPING` is ADR 0012's label mapping made executable: every Neo4j relationship
type paired with the `graph` relation it becomes, the endpoint labels that disambiguate the
two `IS` splits and the two `BY` splits, and the properties that travel with the edge —
including `CREDITED_ON.category`, which is `credited_on.role_category` here. `part_of`,
`in_family`, and `sublabel_of` are views rather than tables at the pinned schema revision,
which changes nothing about what they must contain.

`VERTEX_MAPPING` covers the six derived vertices, which are the ones each store MINTS from
document content: genre, style, and person names; medium ids; company ids under the casefold
rule; media family names. The four entity vertices — artist, label, master, release — are
deliberately absent. Neo4j MERGEs a stub node for every id a document REFERENCES, so the
enricher holds an `Artist` for a member whose own document has not arrived, while the
loader's `graph.artist` view holds only documents it actually loaded. That is a difference
in when a vertex appears, not in what the graph asserts, and every edge naming such an id is
compared in full regardless.

## The expected-differences registry

`EXPECTED_DIFFERENCES` starts empty and gains an entry only when a divergence actually
materialises on this fixture and has a reason that is a decision rather than a bug. It is a
plain mapping keyed by relation, and it is held to both directions at once:

- a relation NOT in the registry must have identical sets, or the test fails naming it;
- a relation IN the registry must diverge exactly as declared — the same tuples, on the same
  side — so a declaration that stops being true fails rather than silently absolving.

Each entry below is a divergence the edge-writer review predicted and this fixture provokes.
Two predicted divergences did NOT materialise and so have no entry: `member_of` and
`same_as` are additive in BOTH stores, so the membership the fixture withdraws and the
credit it removes survive on both sides. That is the registry doing its job — the prediction
was that they would agree, and they do.
"""

from __future__ import annotations

import importlib.metadata
import json
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Final
from unittest.mock import AsyncMock, MagicMock

import psycopg
import pytest
import pytest_asyncio
from neo4j import AsyncGraphDatabase

from tableinator.batch_writer import PostgreSQLBatchWriter
from tableinator.graph_derivation import EDGE_COLUMNS, VERTEX_COLUMNS
from tableinator.media import media_for_release


if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from neo4j import AsyncDriver


pytestmark = [pytest.mark.integration, pytest.mark.parity]

ENRICHER_DISTRIBUTION: Final = "groovemap-discogs-graph-enricher"

# The enricher is installed by `scripts/test-parity.sh`, not declared in `pyproject.toml`,
# so every other lane collects this module without it and skips here rather than erroring.
_enricher = pytest.importorskip(
    "graphinator.batch_processor",
    reason=f"{ENRICHER_DISTRIBUTION} is installed by the parity lane; run `just test-parity`",
)
Neo4jBatchProcessor = _enricher.Neo4jBatchProcessor
PendingMessage = _enricher.PendingMessage

ENTITY_TABLES: Final = ("artists", "labels", "masters", "releases")

# The three relations that are views at the pinned schema revision. The loader writes no row
# for any of them; `part_of` and `in_family` are projections of the vertex tables it does
# write, and `sublabel_of` reads `public.labels` directly.
VIEW_RELATIONS: Final = frozenset({"part_of", "in_family", "sublabel_of"})

# `track_credited_on` and `track_by_artist` (gm-database-schema-ug3v, gm-discogs-sql-
# loader-b2a) were declared for the FastRP embedding pipeline's chw.2 spike after this
# enricher last moved, so there is no Neo4j relationship to read them back from and no
# cross-store parity claim to make for them here. `tests/test_graph_derivation.py` holds
# them to the schema's own `_TRACK_CREDIT_SOURCE` / `_TRACK_PERFORMER_SOURCE` instead.
NO_ENRICHER_COUNTERPART: Final = frozenset({"track_credited_on", "track_by_artist"})


# ── ADR 0012's label mapping, made executable ────────────────────────────────


@dataclass(frozen=True)
class EdgeMapping:
    """One Neo4j relationship type paired with the `graph` relation it becomes.

    `source`/`target` are the PostgreSQL columns; `source_label`/`target_label` are the Neo4j
    node labels, which are what splits one relationship type into two relations where ADR
    0012 says it does. `properties` pairs a PostgreSQL column with the Neo4j edge property
    holding the same value, in the order both sides are read.
    """

    relation: str
    relationship: str
    source: str
    source_label: str
    target: str
    target_label: str
    source_key: str = "id"
    target_key: str = "id"
    properties: tuple[tuple[str, str], ...] = ()

    @property
    def statement(self) -> str:
        """Return the SELECT reading this relation out of PostgreSQL."""
        columns = [f"{self.source}::text", f"{self.target}::text", *(column for column, _property in self.properties)]
        return f"SELECT {', '.join(columns)} FROM graph.{self.relation}"  # noqa: S608

    @property
    def cypher(self) -> str:
        """Return the MATCH reading the same relation out of Neo4j."""
        returned = [f"source.{self.source_key}", f"target.{self.target_key}", *(f"edge.{name}" for _column, name in self.properties)]
        pattern = f"(source:{self.source_label})-[edge:{self.relationship}]->(target:{self.target_label})"
        return f"MATCH {pattern} RETURN {', '.join(f'{expression} AS c{index}' for index, expression in enumerate(returned))}"


@dataclass(frozen=True)
class VertexMapping:
    """One Neo4j node label paired with the `graph` vertex table it becomes."""

    relation: str
    label: str
    key: str
    key_property: str = "name"
    properties: tuple[tuple[str, str], ...] = ()

    @property
    def statement(self) -> str:
        """Return the SELECT reading this vertex out of PostgreSQL."""
        columns = [f"{self.key}::text", *(column for column, _property in self.properties)]
        return f"SELECT {', '.join(columns)} FROM graph.{self.relation}"  # noqa: S608

    @property
    def cypher(self) -> str:
        """Return the MATCH reading the same vertex out of Neo4j."""
        returned = [f"vertex.{self.key_property}", *(f"vertex.{name}" for _column, name in self.properties)]
        return f"MATCH (vertex:{self.label}) RETURN {', '.join(f'{expression} AS c{index}' for index, expression in enumerate(returned))}"


RELATION_MAPPING: Final[tuple[EdgeMapping, ...]] = (
    # `BY` is reserved in SQL and covers two endpoints, so it splits by source label.
    EdgeMapping("by_artist", "BY", "release_id", "Release", "artist_id", "Artist"),
    EdgeMapping("master_by_artist", "BY", "master_id", "Master", "artist_id", "Artist"),
    # `ON` is reserved.
    EdgeMapping("on_label", "ON", "release_id", "Release", "label_id", "Label"),
    # `IS` is reserved and is one type over two target labels, split by endpoint, times two
    # source labels.
    EdgeMapping("in_genre", "IS", "release_id", "Release", "genre_name", "Genre", target_key="name"),
    EdgeMapping("in_style", "IS", "release_id", "Release", "style_name", "Style", target_key="name"),
    EdgeMapping("master_in_genre", "IS", "master_id", "Master", "genre_name", "Genre", target_key="name"),
    EdgeMapping("master_in_style", "IS", "master_id", "Master", "style_name", "Style", target_key="name"),
    EdgeMapping("derived_from", "DERIVED_FROM", "release_id", "Release", "master_id", "Master"),
    EdgeMapping("member_of", "MEMBER_OF", "member_artist_id", "Artist", "group_artist_id", "Artist"),
    EdgeMapping("alias_of", "ALIAS_OF", "alias_artist_id", "Artist", "artist_id", "Artist"),
    EdgeMapping("sublabel_of", "SUBLABEL_OF", "sublabel_id", "Label", "parent_label_id", "Label"),
    EdgeMapping("part_of", "PART_OF", "style_name", "Style", "genre_name", "Genre", source_key="name", target_key="name"),
    EdgeMapping("in_family", "IN_FAMILY", "medium_id", "Medium", "family_name", "MediaFamily", target_key="name"),
    EdgeMapping("issued_on", "ISSUED_ON", "release_id", "Release", "medium_id", "Medium", properties=(("qty", "qty"), ("source", "source"))),
    EdgeMapping(
        "credited_to",
        "CREDITED_TO",
        "release_id",
        "Release",
        "company_id",
        "Company",
        properties=(("role", "role"), ("role_category", "role_category"), ("source", "source")),
    ),
    # The one property that is spelled differently on the two sides: ADR 0012 keeps the
    # Neo4j edge's `category` and names the column `role_category`, where it is GENERATED.
    EdgeMapping(
        "credited_on",
        "CREDITED_ON",
        "person_name",
        "Person",
        "release_id",
        "Release",
        source_key="name",
        properties=(("role", "role"), ("role_category", "category")),
    ),
    EdgeMapping("same_as", "SAME_AS", "person_name", "Person", "artist_id", "Artist", source_key="name"),
)

VERTEX_MAPPING: Final[tuple[VertexMapping, ...]] = (
    VertexMapping("genre", "Genre", "name"),
    VertexMapping("style", "Style", "name"),
    VertexMapping("person", "Person", "name"),
    VertexMapping("media_family", "MediaFamily", "name"),
    # `graph.company.discogs_label_id` has no Neo4j counterpart: it is derived from the
    # company id by the same rule on both sides, so comparing it would restate the key.
    VertexMapping("company", "Company", "company_id", key_property="id", properties=(("name", "name"),)),
    VertexMapping("medium", "Medium", "medium_id", key_property="id", properties=(("family", "family"), ("label", "label"))),
)


# ── The fixture catalog ──────────────────────────────────────────────────────
# Two rounds of events over one small catalog. Round one establishes every shape the edge
# model has; round two re-states three documents with corrected content, which is the only
# way the two stores' pruning rules can be compared at all.

ARTISTS_V1: Final = [
    (
        "pp-a1",
        {
            "id": "pp-a1",
            "name": "The Band",
            "sha256": "pp-a1-v1",
            # `pp-a2` and `pp-a3` state the same membership from their own side; `pp-a6`
            # is named only here, so it is the membership round two can withdraw outright.
            "members": [{"id": "pp-a2"}, {"id": "pp-a3"}, {"id": "pp-a6"}],
            "aliases": [{"id": "pp-a4"}, {"id": "pp-a5"}],
        },
    ),
    ("pp-a2", {"id": "pp-a2", "name": "First Member", "sha256": "pp-a2-v1", "groups": [{"id": "pp-a1"}]}),
    ("pp-a3", {"id": "pp-a3", "name": "Second Member", "sha256": "pp-a3-v1", "groups": [{"id": "pp-a1"}]}),
]

# A label states its hierarchy from both ends. `graph.sublabel_of` is a view over
# `public.labels`, so the loader derives no row and the view has to produce both anyway.
LABELS_V1: Final = [
    ("pp-l1", {"id": "pp-l1", "name": "A Label", "sha256": "pp-l1-v1", "parentLabel": {"id": "pp-l2"}, "sublabels": [{"id": "pp-l3"}]})
]

MASTERS_V1: Final = [
    (
        "pp-m1",
        {
            "id": "pp-m1",
            "title": "A Master",
            "sha256": "pp-m1-v1",
            "artists": [{"id": "pp-a1"}],
            # One genre and two styles: the only shape that asserts PART_OF unambiguously.
            "genres": ["Rock"],
            "styles": ["Pop Rock", "Psychedelic Rock"],
        },
    )
]

RELEASES_V1: Final = [
    (
        "pp-r1",
        {
            "id": "pp-r1",
            "title": "A Release",
            "sha256": "pp-r1-v1",
            "master_id": "pp-m1",
            "artists": [{"id": "pp-a1"}, {"id": "pp-a2"}, {"id": "pp-a3"}],
            "labels": [{"id": "pp-l1", "catno": "AL-001"}, {"id": "pp-l1", "catno": "AL-002"}],
            "genres": ["Rock", "Pop"],
            "styles": ["Pop Rock"],
            "extraartists": [
                {"id": "pp-a2", "name": "First Member", "role": "Producer"},
                {"name": "A Producer", "role": "Producer"},
                {"name": "A Producer", "role": "Mastered By"},
            ],
            "companies": {
                "items": [
                    {"discogs_id": "4321", "name": "A Pressing Plant", "role": "Pressed By", "role_category": "manufacture"},
                    {"name": "A Cutting Room", "role": "Lacquer Cut By"},
                ]
            },
            # A canonical block naming a medium the vendored taxonomy does not carry, so both
            # sides fall back to labelling it with its own id. Round two moves it to another
            # family, which is the "producer upgraded its taxonomy" case.
            "media": {"families": ["vinyl"], "items": [{"medium": "parity_wax", "family": "vinyl", "qty": 2}]},
        },
    ),
    (
        "pp-r2",
        {
            "id": "pp-r2",
            "title": "Another Release",
            "sha256": "pp-r2-v1",
            # `"0"` is the Discogs "no entity" sentinel stated as element text.
            "artists": [{"id": "pp-a1"}, {"id": "0"}],
            "labels": [{"id": "pp-l1", "catno": "AL-003"}],
            "genres": ["Rock"],
            # No canonical `companies` block: a pre-cutover record is silent about company
            # credits rather than asserting it has none, so neither store prunes them.
            "formats": [{"name": "CD", "qty": "1", "descriptions": {"description": ["Album"]}}],
        },
    ),
]

ARTISTS_V2: Final = [
    (
        "pp-a1",
        {
            "id": "pp-a1",
            "name": "The Band",
            "sha256": "pp-a1-v2",
            "members": [{"id": "pp-a2"}, {"id": "pp-a3"}],
            "aliases": [{"id": "pp-a4"}],
        },
    )
]

RELEASES_V2: Final = [
    (
        "pp-r1",
        {
            "id": "pp-r1",
            "title": "A Release",
            "sha256": "pp-r1-v2",
            "master_id": "pp-m1",
            "artists": [{"id": "pp-a1"}],
            "labels": [{"id": "pp-l1", "catno": "AL-001"}],
            "genres": ["Rock"],
            "styles": ["Pop Rock"],
            "extraartists": [{"name": "A Producer", "role": "Producer"}],
            "companies": {
                "items": [
                    {"discogs_id": "4321", "name": "A Renamed Pressing Plant", "role": "Pressed By", "role_category": "manufacture"},
                    {"name": "A Cutting Room", "role": "Lacquer Cut By"},
                ]
            },
            "media": {"families": ["optical"], "items": [{"medium": "parity_wax", "family": "optical", "qty": 2}]},
        },
    )
]

EVENTS: Final[tuple[tuple[str, list[tuple[str, dict[str, Any]]]], ...]] = (
    ("artists", ARTISTS_V1),
    ("labels", LABELS_V1),
    ("masters", MASTERS_V1),
    ("releases", RELEASES_V1),
    ("artists", ARTISTS_V2),
    ("releases", RELEASES_V2),
)


# ── The expected-differences registry ────────────────────────────────────────


@dataclass(frozen=True)
class ExpectedDifference:
    """One divergence this fixture provokes, with the decision that causes it."""

    reason: str
    only_in_postgres: frozenset[tuple[Any, ...]] = frozenset()
    only_in_neo4j: frozenset[tuple[Any, ...]] = frozenset()


EXPECTED_DIFFERENCES: Final[dict[str, ExpectedDifference]] = {
    "by_artist": ExpectedDifference(
        reason=(
            "The loader drops the Discogs 'no entity' sentinel id `0`, because that is what "
            "`_usable_id` in `groovemap_schema.postgres` does and therefore what "
            "`graph.bootstrap_fill()` and the phase 0 views assert. The enricher tests raw "
            'truthiness, which drops a numeric 0 but keeps the STRING "0" the dump states, '
            "so it MERGEs an Artist node for a reference that names nothing."
        ),
        only_in_neo4j=frozenset({("pp-r2", "0")}),
    ),
    "alias_of": ExpectedDifference(
        reason=(
            "An `alias_of` row names its asserting document in `artist_id`, so the loader "
            "replaces the relation per document and an alias withdrawn upstream goes. The "
            "enricher's `process_artist` has no ALIAS_OF prune at all, so the withdrawn alias "
            "outlives the document that asserted it."
        ),
        only_in_neo4j=frozenset({("pp-a5", "pp-a1")}),
    ),
    "credited_on": ExpectedDifference(
        reason=(
            "`credited_on` is release-scoped, so the loader deletes the release's rows before "
            "re-inserting the current set and a credit removed upstream goes. The enricher "
            "MERGEs CREDITED_ON without a matching prune, so every superseded credit stays. "
            "The two rows here are the artist-linked credit and the second role of the person "
            "the corrected release still credits once."
        ),
        only_in_neo4j=frozenset({("First Member", "pp-r1", "Producer", "production"), ("A Producer", "pp-r1", "Mastered By", "mastering")}),
    ),
    "in_family": ExpectedDifference(
        reason=(
            "`graph.in_family` is a view over `graph.medium.family`, so a medium belongs to "
            "exactly one family and re-stating it moves the projection. The enricher MERGEs "
            "IN_FAMILY and never prunes it, so a medium whose family changes keeps an edge to "
            "the family it has left. Same cause as the `medium` entry below."
        ),
        only_in_neo4j=frozenset({("parity_wax", "optical")}),
    ),
    "medium": ExpectedDifference(
        reason=(
            "Every vertex INSERT the loader issues is ON CONFLICT DO NOTHING, because "
            "`graph.medium` is co-owned with `musicbrainz-sql-loader` and a blind DO UPDATE "
            "would have each provider overwrite the other's answer. `MERGE_MEDIA_CYPHER` has "
            "`ON MATCH SET m.family`, so the Neo4j node moves and the row keeps its first "
            "spelling. Only the property differs; the identity is the same rule on both sides. "
            "Whether this becomes a DO UPDATE or a backfill is not decided here."
        ),
        only_in_postgres=frozenset({("parity_wax", "vinyl", "parity_wax")}),
        only_in_neo4j=frozenset({("parity_wax", "optical", "parity_wax")}),
    ),
    "company": ExpectedDifference(
        reason=(
            "The same ON CONFLICT DO NOTHING against `MERGE_COMPANY_CYPHER`'s `SET co.name`: a "
            "company renamed upstream keeps its first spelling in `graph.company.name` while "
            "the node moves. The id is identical — both sides derive it from "
            "`company_identity`, so the casefold rule agrees — and every `credited_to` edge "
            "naming it is identical too."
        ),
        only_in_postgres=frozenset({("4321", "A Pressing Plant")}),
        only_in_neo4j=frozenset({("4321", "A Renamed Pressing Plant")}),
    ),
}


# ── Driving both stores ──────────────────────────────────────────────────────


@dataclass(frozen=True)
class BatchRecord:
    """One flushed message, in the shape `PostgreSQLBatchWriter` reads."""

    data_id: str
    data: dict[str, Any]
    sha256: str


@dataclass(frozen=True)
class Divergence:
    """The tuples one store holds and the other does not, for one relation."""

    only_in_postgres: frozenset[tuple[Any, ...]] = frozenset()
    only_in_neo4j: frozenset[tuple[Any, ...]] = frozenset()

    def __bool__(self) -> bool:
        return bool(self.only_in_postgres or self.only_in_neo4j)


@dataclass(frozen=True)
class ParityRun:
    """One finished run: what each store holds, and where the two differ."""

    postgres: dict[str, frozenset[tuple[Any, ...]]] = field(default_factory=dict)
    neo4j: dict[str, frozenset[tuple[Any, ...]]] = field(default_factory=dict)
    divergences: dict[str, Divergence] = field(default_factory=dict)


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


@pytest_asyncio.fixture
async def postgres_connection() -> AsyncIterator[psycopg.AsyncConnection[Any]]:
    """A connection on the promoted schema, emptied of every relation this suite reads."""
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("the parity lane needs TEST_DATABASE_URL; run `just test-parity`")

    connection = await psycopg.AsyncConnection.connect(database_url)
    await connection.set_autocommit(True)
    try:
        await _empty_postgres(connection)
        yield connection
    finally:
        if not connection.autocommit:
            await connection.rollback()
            await connection.set_autocommit(True)
        await _empty_postgres(connection)
        await connection.close()


@pytest_asyncio.fixture
async def neo4j_driver() -> AsyncIterator[AsyncDriver]:
    """A driver on the disposable Neo4j, emptied before and after the run."""
    uri = os.environ.get("NEO4J_URI")
    password = os.environ.get("NEO4J_INTEGRATION_PASSWORD")
    if not uri or not password:
        pytest.skip("the parity lane needs NEO4J_URI and NEO4J_INTEGRATION_PASSWORD; run `just test-parity`")

    driver = AsyncGraphDatabase.driver(uri, auth=(os.environ.get("NEO4J_INTEGRATION_USER", "neo4j"), password))
    await driver.verify_connectivity()
    try:
        await _empty_neo4j(driver)
        yield driver
    finally:
        await _empty_neo4j(driver)
        await driver.close()


@pytest_asyncio.fixture
async def parity(postgres_connection: psycopg.AsyncConnection[Any], neo4j_driver: AsyncDriver) -> ParityRun:
    """Play the fixture events through both services and read both stores back."""
    await _load(postgres_connection, neo4j_driver)
    return await _compare(postgres_connection, neo4j_driver)


async def _empty_postgres(connection: psycopg.AsyncConnection[Any]) -> None:
    relations = ", ".join(f"graph.{relation}" for relation in (*EDGE_COLUMNS, *VERTEX_COLUMNS))
    # Every relation name here is one of the loader's own constants, never input.
    await connection.execute(f"TRUNCATE {relations}")
    await connection.execute(f"TRUNCATE {', '.join(ENTITY_TABLES)}")


async def _empty_neo4j(driver: AsyncDriver) -> None:
    async with driver.session(database="neo4j") as session:
        await session.run("MATCH (node) DETACH DELETE node")


async def _load(connection: psycopg.AsyncConnection[Any], driver: AsyncDriver) -> None:
    """Send every fixture event to both services, in one order, through both batch paths."""
    writer = PostgreSQLBatchWriter(SingleConnectionPool(connection), MagicMock(), media_for_release)
    processor = Neo4jBatchProcessor(driver)

    for data_type, documents in EVENTS:
        await writer.process_batch(data_type, [BatchRecord(data_id, data, data["sha256"]) for data_id, data in documents])

        for _data_id, data in documents:
            message = PendingMessage(data_type, data, AsyncMock(), AsyncMock())
            # The enricher's own integration suite submits through the engine the same way;
            # `add_message` would re-normalize and settle against a broker that is not here.
            await processor._engine.submit(data_type, message, message)
        assert await processor.flush_queue(data_type) is True, f"the enricher failed to flush its {data_type} batch"


async def _read_postgres(connection: psycopg.AsyncConnection[Any], statement: str) -> frozenset[tuple[Any, ...]]:
    return frozenset(await (await connection.execute(statement)).fetchall())


async def _read_neo4j(driver: AsyncDriver, cypher: str) -> frozenset[tuple[Any, ...]]:
    async with driver.session(database="neo4j") as session:
        result = await session.run(cypher)
        return frozenset([tuple(record.values()) async for record in result])


async def _compare(connection: psycopg.AsyncConnection[Any], driver: AsyncDriver) -> ParityRun:
    """Read every mapped relation out of both stores and diff the two sets."""
    run = ParityRun()
    for mapping in (*RELATION_MAPPING, *VERTEX_MAPPING):
        postgres = await _read_postgres(connection, mapping.statement)
        neo4j = await _read_neo4j(driver, mapping.cypher)
        run.postgres[mapping.relation] = postgres
        run.neo4j[mapping.relation] = neo4j
        run.divergences[mapping.relation] = Divergence(postgres - neo4j, neo4j - postgres)
    return run


def _describe(relation: str, divergence: Divergence) -> str:
    """Return a failure line naming the mapping and both sides of the disagreement."""
    mapping = _mapping_for(relation)
    return (
        f"\n  {relation} (mapped from {mapping})"
        f"\n    only in PostgreSQL: {sorted(divergence.only_in_postgres)}"
        f"\n    only in Neo4j:      {sorted(divergence.only_in_neo4j)}"
    )


def _mapping_for(relation: str) -> str:
    """Return the Neo4j side of the mapping entry a relation came from."""
    for edge in RELATION_MAPPING:
        if edge.relation == relation:
            return f"({edge.source_label})-[:{edge.relationship}]->({edge.target_label})"
    for vertex in VERTEX_MAPPING:
        if vertex.relation == relation:
            return f"(:{vertex.label})"
    return "no mapping"


# ── The assertions ───────────────────────────────────────────────────────────


def test_the_mapping_covers_every_relation_the_loader_writes() -> None:
    """A relation the loader gains without a mapping entry would go uncompared.

    `NO_ENRICHER_COUNTERPART` is the one deliberate exception: a relation the loader owns
    that the pinned enricher has no relationship for at all, so it is excluded by name
    rather than silently passing because nothing reads it back on the Neo4j side.
    """
    mapped = {edge.relation for edge in RELATION_MAPPING}
    unmapped = set(EDGE_COLUMNS) - mapped - NO_ENRICHER_COUNTERPART
    assert not unmapped, f"unmapped edge relations: {sorted(unmapped)}"
    assert mapped - set(EDGE_COLUMNS) == VIEW_RELATIONS
    assert {vertex.relation for vertex in VERTEX_MAPPING} == set(VERTEX_COLUMNS)


def test_the_pinned_enricher_is_the_one_under_comparison() -> None:
    """A parity run means nothing if the reference implementation is not the pinned one.

    `scripts/test-parity.sh` installs the enricher from a revision and passes that revision
    on, so a dev environment left over from another branch fails here instead of quietly
    proving the loader equal to something else.
    """
    expected = os.environ.get("PARITY_ENRICHER_REVISION")
    if not expected:
        pytest.skip("PARITY_ENRICHER_REVISION is set by `just test-parity`")

    direct_url = importlib.metadata.distribution(ENRICHER_DISTRIBUTION).read_text("direct_url.json")
    assert direct_url is not None, f"{ENRICHER_DISTRIBUTION} was not installed from a pinned revision"
    installed = json.loads(direct_url)["vcs_info"]["commit_id"]
    assert installed == expected, f"the parity lane pinned {expected} but {ENRICHER_DISTRIBUTION} {installed} is installed"


def test_the_registry_names_only_relations_the_mapping_reaches() -> None:
    """An entry keyed on a relation nobody reads would never be held to anything."""
    mapped = {edge.relation for edge in RELATION_MAPPING} | {vertex.relation for vertex in VERTEX_MAPPING}
    assert set(EXPECTED_DIFFERENCES) <= mapped, f"registry keys outside the mapping: {sorted(set(EXPECTED_DIFFERENCES) - mapped)}"


@pytest.mark.asyncio
async def test_both_stores_hold_the_catalog_the_fixture_states(parity: ParityRun) -> None:
    """A comparison of two empty stores would agree about nothing at all.

    Every relation the fixture asserts has to be non-empty on both sides before any
    equality below means anything, and the two entity-keyed vertex sets prove both services
    actually consumed all six event batches rather than silently dropping one.
    """
    empty_in_postgres = sorted(relation for relation, rows in parity.postgres.items() if not rows)
    empty_in_neo4j = sorted(relation for relation, rows in parity.neo4j.items() if not rows)

    assert empty_in_postgres == [], f"the loader wrote nothing for {empty_in_postgres}"
    assert empty_in_neo4j == [], f"the enricher wrote nothing for {empty_in_neo4j}"


@pytest.mark.asyncio
async def test_every_relation_outside_the_registry_is_identical_in_both_stores(parity: ParityRun) -> None:
    """The claim Phase 4 rests on: same events in, same graph out.

    A divergence that is not in `EXPECTED_DIFFERENCES` fails here naming the relation, the
    Neo4j pattern it was mapped from, and the tuples each store holds alone — which is
    enough to tell a loader bug from a decision that needs recording.
    """
    undeclared = {relation: divergence for relation, divergence in parity.divergences.items() if divergence and relation not in EXPECTED_DIFFERENCES}

    assert not undeclared, "the two stores disagree about relations no expected difference covers:" + "".join(
        _describe(relation, divergence) for relation, divergence in sorted(undeclared.items())
    )


@pytest.mark.asyncio
async def test_every_declared_difference_materialises_exactly_as_declared(parity: ParityRun) -> None:
    """The registry is held to the run in both directions.

    A declared difference that stops materialising means the divergence was fixed, or the
    fixture stopped provoking it, and either way the entry has to go rather than sit there
    absolving a relation nobody is checking any more. A declared difference whose tuples
    have moved is a different divergence wearing the old one's reason.
    """
    stale = sorted(relation for relation in EXPECTED_DIFFERENCES if not parity.divergences[relation])
    assert stale == [], f"declared differences that did not materialise on this fixture: {stale}"

    for relation, expected in sorted(EXPECTED_DIFFERENCES.items()):
        divergence = parity.divergences[relation]
        assert divergence.only_in_postgres == expected.only_in_postgres, (
            f"{relation}: PostgreSQL-only tuples moved{_describe(relation, divergence)}\n    reason on record: {expected.reason}"
        )
        assert divergence.only_in_neo4j == expected.only_in_neo4j, (
            f"{relation}: Neo4j-only tuples moved{_describe(relation, divergence)}\n    reason on record: {expected.reason}"
        )


@pytest.mark.asyncio
async def test_the_relations_predicted_to_agree_are_the_ones_that_do(parity: ParityRun) -> None:
    """`member_of` and `same_as` are additive in BOTH stores, so a withdrawal survives twice.

    These are the two relations neither side can prune, for the same reason: neither has a
    column naming one asserting document. The fixture withdraws a membership only `pp-a1`
    ever stated and removes the credit that minted the `same_as` row, and both stores keep
    both — which is why neither relation is in the registry.
    """
    assert not parity.divergences["member_of"]
    assert not parity.divergences["same_as"]
    assert ("pp-a6", "pp-a1") in parity.postgres["member_of"]
    assert ("First Member", "pp-a2") in parity.postgres["same_as"]
