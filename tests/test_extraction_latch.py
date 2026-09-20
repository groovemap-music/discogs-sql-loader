"""The extraction latch: what keys it, what fires it, and what it records.

The rows themselves are exercised against a real PostgreSQL in
`tests/integration/test_graph_counters.py`, because the union upsert and the superseded
check are SQL. What lives here is the decision the loader makes from a latch it has read,
and the statement it issues to record one.
"""

import pathlib
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

import pytest

from tableinator.extraction_latch import (
    EXTRACTION_LATCH_UNKNOWN_VERSION,
    LATCH_CANDIDATES,
    LOADER_DISCRIMINATOR,
    ExtractionLatch,
    LatchRelation,
    extraction_latch_key,
    mark_extraction_refreshed,
    probe_latch_relation,
    record_extraction_signal,
)


if TYPE_CHECKING:
    from collections.abc import AsyncIterator


DATA_TYPES = ("artists", "labels", "masters", "releases")

# The two shapes the pending `database-schema` chore may land: one relation per loader, or
# one shared by both with a `loader` discriminator.
UNSHARED = LatchRelation(schema="public", table="loader_extraction_latch", keyed_on_loader=False)
SHARED = LatchRelation(schema="public", table="loader_extraction_latch", keyed_on_loader=True)

TIMESTAMP = "timestamp with time zone"
DECLARED_COLUMNS = [
    ("version", "text", "text"),
    ("signals", "ARRAY", "_text"),
    ("created_at", TIMESTAMP, "timestamptz"),
    ("updated_at", TIMESTAMP, "timestamptz"),
    ("refreshed_at", TIMESTAMP, "timestamptz"),
]


def _latch(signals: Any, **overrides: Any) -> ExtractionLatch:
    fields: dict[str, Any] = {
        "version": "20260101",
        "signals": frozenset(signals),
        "already_signalled": False,
        "already_refreshed": False,
        "superseded": False,
    }
    fields.update(overrides)
    return ExtractionLatch(**fields)


# ── What keys a latch ────────────────────────────────────────────────────────


def test_the_version_keys_the_latch_when_the_message_carries_one() -> None:
    assert extraction_latch_key({"version": "20260101", "started_at": "2026-01-01T00:00:00Z"}) == "20260101"


def test_the_start_keys_it_when_the_version_is_missing_or_blank() -> None:
    """Two dumps that both omit a version would otherwise share one row, and the second
    would inherit the first's four signals and fire on its very first message."""
    assert extraction_latch_key({"started_at": "2026-02-01T00:00:00Z"}) == "2026-02-01T00:00:00Z"
    assert extraction_latch_key({"version": "   ", "started_at": "2026-02-01T00:00:00Z"}) == "2026-02-01T00:00:00Z"


def test_a_message_naming_no_extraction_falls_back_to_unknown() -> None:
    assert extraction_latch_key({}) == EXTRACTION_LATCH_UNKNOWN_VERSION
    assert extraction_latch_key({"version": None, "started_at": ""}) == EXTRACTION_LATCH_UNKNOWN_VERSION


# ── What fires the pass ──────────────────────────────────────────────────────


def test_an_incomplete_extraction_does_not_fire() -> None:
    latch = _latch({"artists", "labels", "masters"})

    assert latch.should_refresh(DATA_TYPES) is False
    assert latch.pending(DATA_TYPES) == ["releases"]


def test_the_signal_that_completes_an_extraction_fires() -> None:
    latch = _latch(DATA_TYPES)

    assert latch.should_refresh(DATA_TYPES) is True
    assert latch.pending(DATA_TYPES) == []


def test_an_extraction_already_refreshed_does_not_fire_again() -> None:
    """The sweep takes ACCESS EXCLUSIVE on seven tables; a redelivery must not repeat it."""
    assert _latch(DATA_TYPES, already_refreshed=True).should_refresh(DATA_TYPES) is False


def test_a_straggler_from_a_superseded_extraction_does_not_fire() -> None:
    """A later dump has started, so this catalog is no longer the one the signal describes."""
    assert _latch(DATA_TYPES, superseded=True).should_refresh(DATA_TYPES) is False


def test_a_redelivery_after_a_FAILED_pass_does_fire() -> None:
    """`already_signalled` is deliberately not a veto: the failed pass nacked, and owes a retry."""
    assert _latch(DATA_TYPES, already_signalled=True).should_refresh(DATA_TYPES) is True


# ── What it records ──────────────────────────────────────────────────────────


class RecordingCursor:
    def __init__(self, row: Any = None, probe_rows: Any = None) -> None:
        self.statements: list[tuple[str, Any]] = []
        self._row = row
        self._probe_rows = dict(probe_rows or {})
        self._rows: list[Any] = []

    async def execute(self, statement: Any, parameters: Any = None) -> None:
        text = statement if isinstance(statement, str) else statement.as_string(None)
        self.statements.append((text, parameters))
        if "information_schema.columns" in text:
            self._rows = self._probe_rows.get(tuple(parameters or ()), [])

    async def fetchone(self) -> Any:
        return self._row

    async def fetchall(self) -> Any:
        return self._rows


class FakeConnection:
    def __init__(self, cursor: RecordingCursor) -> None:
        self._cursor = cursor
        self.autocommit = True

    async def set_autocommit(self, value: bool) -> None:
        self.autocommit = value

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        yield

    @asynccontextmanager
    async def cursor(self) -> AsyncIterator[RecordingCursor]:
        yield self._cursor


class ExplodingPool:
    """A pool whose connection cannot be had, as during an outage."""

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[Any]:
        raise RuntimeError("no connection")
        yield  # pragma: no cover


class RecordingLogger:
    """Collect the structured lines the probe emits."""

    def __init__(self) -> None:
        self.lines: list[tuple[str, str]] = []

    def info(self, event: str, **_fields: Any) -> None:
        self.lines.append(("info", event))

    def warning(self, event: str, **_fields: Any) -> None:
        self.lines.append(("warning", event))

    def error(self, event: str, **_fields: Any) -> None:
        self.lines.append(("error", event))

    def events(self, level: str) -> list[str]:
        return [event for line_level, event in self.lines if line_level == level]


class FakePool:
    def __init__(self, cursor: RecordingCursor) -> None:
        self.connection_double = FakeConnection(cursor)

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[FakeConnection]:
        yield self.connection_double


@pytest.mark.asyncio
async def test_the_signal_is_recorded_before_the_delivery_is_acked() -> None:
    """The table is created if absent and the signal recorded in one transaction.

    graphinator writes its latch to Neo4j before acking for the same reason: the ack
    destroys the queued message, which was otherwise the only durable copy of this
    coordination state (discogsography-tk7v).
    """
    cursor = RecordingCursor((["artists", "labels"], False, False, False))
    pool = FakePool(cursor)

    latch = await record_extraction_signal(pool, UNSHARED, "20260101", "labels")

    assert pool.connection_double.autocommit is False
    assert cursor.statements[0][1] == {"version": "20260101", "data_type": "labels"}
    assert latch.signals == frozenset({"artists", "labels"})
    assert latch.version == "20260101"


@pytest.mark.asyncio
async def test_the_recorded_flags_come_back_on_the_latch() -> None:
    cursor = RecordingCursor((["artists", "labels", "masters", "releases"], True, True, True))

    latch = await record_extraction_signal(FakePool(cursor), UNSHARED, "20260101", "releases")

    assert latch.already_signalled is True
    assert latch.already_refreshed is True
    assert latch.superseded is True


@pytest.mark.asyncio
async def test_the_stamp_names_the_extraction_it_certifies() -> None:
    cursor = RecordingCursor(None)

    await mark_extraction_refreshed(cursor, UNSHARED, "20260101")

    statement, parameters = cursor.statements[0]
    assert "SET refreshed_at = NOW()" in statement
    assert parameters == {"version": "20260101"}


# ── The probe, and the degraded mode a miss leaves behind ────────────────────
# `docs/database-schema.md` says this service does not create or migrate database objects,
# and `tests/test_service_contract.py` guards that sentence. So the relation is found, or it
# is not there and nothing is written — never created here.


def test_no_module_statement_creates_a_database_object() -> None:
    """The one rule this module was bounced on, asserted against its own source."""
    import tableinator.extraction_latch as module

    source = pathlib.Path(module.__file__ or "").read_text()
    body = source.split('"""', 2)[2]
    for forbidden in ("CREATE TABLE", "CREATE INDEX", "ALTER TABLE", "CREATE SCHEMA"):
        assert forbidden not in body, f"{forbidden} has no business in a loader"


@pytest.mark.asyncio
async def test_the_probe_finds_the_declared_relation() -> None:
    cursor = RecordingCursor(probe_rows={LATCH_CANDIDATES[0]: DECLARED_COLUMNS})

    relation = await probe_latch_relation(FakePool(cursor), RecordingLogger())

    assert relation == UNSHARED
    assert "information_schema.columns" in cursor.statements[0][0]
    assert cursor.statements[0][1] == LATCH_CANDIDATES[0]


@pytest.mark.asyncio
async def test_the_probe_takes_the_second_candidate_when_the_first_is_absent() -> None:
    """The chore declaring the relation has not settled its name; the repin cuts this list."""
    cursor = RecordingCursor(probe_rows={LATCH_CANDIDATES[1]: DECLARED_COLUMNS})

    relation = await probe_latch_relation(FakePool(cursor), RecordingLogger())

    assert relation is not None
    assert (relation.schema, relation.table) == LATCH_CANDIDATES[1]


@pytest.mark.asyncio
async def test_the_probe_keys_on_the_loader_column_when_the_relation_carries_one() -> None:
    """The relation is named for the loader family, so musicbrainz-sql-loader can share it."""
    columns = [*DECLARED_COLUMNS, ("loader", "text", "text")]
    cursor = RecordingCursor(probe_rows={LATCH_CANDIDATES[0]: columns})

    relation = await probe_latch_relation(FakePool(cursor), RecordingLogger())

    assert relation == SHARED


@pytest.mark.asyncio
async def test_an_absent_relation_is_a_degraded_mode_not_an_error() -> None:
    logger = RecordingLogger()

    relation = await probe_latch_relation(FakePool(RecordingCursor()), logger)

    assert relation is None
    assert any("No extraction latch relation is declared" in event for event in logger.events("warning"))
    assert logger.events("error") == []


@pytest.mark.asyncio
async def test_a_relation_whose_columns_are_wrong_is_declined() -> None:
    """Sharing a name is not being the relation; writing into it would be worse than degrading."""
    wrong = [("version", "text", "text"), ("signals", "text", "text")]
    cursor = RecordingCursor(probe_rows={LATCH_CANDIDATES[0]: wrong})
    logger = RecordingLogger()

    relation = await probe_latch_relation(FakePool(cursor), logger)

    assert relation is None
    assert any("does not have its columns" in event for event in logger.events("warning"))


@pytest.mark.asyncio
async def test_a_probe_that_cannot_reach_postgresql_degrades_rather_than_raising() -> None:
    """Startup must not fail over this; the loader still upserts documents without a refresh."""
    logger = RecordingLogger()

    relation = await probe_latch_relation(ExplodingPool(), logger)

    assert relation is None
    assert any("Could not probe" in event for event in logger.events("error"))


# ── The statements each shape produces ───────────────────────────────────────


def test_the_unshared_shape_keys_on_the_version_alone() -> None:
    statement = UNSHARED.record_statement().as_string(None)

    assert '"public"."loader_extraction_latch"' in statement
    assert 'ON CONFLICT ("version")' in statement
    assert '"loader"' not in statement
    assert UNSHARED.parameters("20260101", "releases") == {"version": "20260101", "data_type": "releases"}


def test_the_shared_shape_keys_on_the_loader_too() -> None:
    """Two loaders in one relation must not read each other's signals, nor supersede each other."""
    statement = SHARED.record_statement().as_string(None)

    assert 'ON CONFLICT ("loader", "version")' in statement
    assert '"newer"."loader" = %(loader)s' in statement
    assert SHARED.parameters("20260101", "releases") == {
        "version": "20260101",
        "loader": LOADER_DISCRIMINATOR,
        "data_type": "releases",
    }


def test_the_stamp_is_scoped_the_same_way_as_the_record() -> None:
    assert '"version" = %(version)s AND "loader" = %(loader)s' in SHARED.stamp_statement().as_string(None)
    assert SHARED.parameters("20260101") == {"version": "20260101", "loader": LOADER_DISCRIMINATOR}
