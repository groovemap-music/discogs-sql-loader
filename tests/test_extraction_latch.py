"""The extraction latch: what keys it, what fires it, and what it records.

The rows themselves are exercised against a real PostgreSQL in
`tests/integration/test_graph_counters.py`, because the union upsert and the superseded
check are SQL. What lives here is the decision the loader makes from a latch it has read,
and the statement it issues to record one.
"""

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

import pytest

from tableinator.extraction_latch import (
    EXTRACTION_LATCH_TABLE,
    EXTRACTION_LATCH_UNKNOWN_VERSION,
    ExtractionLatch,
    extraction_latch_key,
    mark_extraction_refreshed,
    record_extraction_signal,
)


if TYPE_CHECKING:
    from collections.abc import AsyncIterator


DATA_TYPES = ("artists", "labels", "masters", "releases")


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
    def __init__(self, row: Any) -> None:
        self.statements: list[tuple[str, Any]] = []
        self._row = row

    async def execute(self, statement: Any, parameters: Any = None) -> None:
        self.statements.append((str(statement), parameters))

    async def fetchone(self) -> Any:
        return self._row


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

    latch = await record_extraction_signal(pool, "20260101", "labels")

    assert pool.connection_double.autocommit is False
    assert f"CREATE TABLE IF NOT EXISTS {EXTRACTION_LATCH_TABLE}" in cursor.statements[0][0]
    assert cursor.statements[1][1] == {"version": "20260101", "data_type": "labels"}
    assert latch.signals == frozenset({"artists", "labels"})
    assert latch.version == "20260101"


@pytest.mark.asyncio
async def test_the_recorded_flags_come_back_on_the_latch() -> None:
    cursor = RecordingCursor((["artists", "labels", "masters", "releases"], True, True, True))

    latch = await record_extraction_signal(FakePool(cursor), "20260101", "releases")

    assert latch.already_signalled is True
    assert latch.already_refreshed is True
    assert latch.superseded is True


@pytest.mark.asyncio
async def test_the_stamp_names_the_extraction_it_certifies() -> None:
    cursor = RecordingCursor(None)

    await mark_extraction_refreshed(cursor, "20260101")

    assert f"CREATE TABLE IF NOT EXISTS {EXTRACTION_LATCH_TABLE}" in cursor.statements[0][0]
    statement, parameters = cursor.statements[1]
    assert "SET refreshed_at = NOW()" in statement
    assert parameters == ("20260101",)
