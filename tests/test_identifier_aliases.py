"""Owner-boundary tests for the identifier aliases both write paths mint (ADR 0011).

The aliases are what makes a release reachable by its printed barcode, catalogue number,
or matrix runout, and the loader is the only place they are minted from a Discogs event.
These lanes run offline: ``common.identity.attach_aliases`` is patched at the seam the
loader calls it through, so what is asserted is the exact call the loader makes — which
refs, keyed to which native id, on which connection, and at which point in the batch.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch

import pytest
from common.identifiers import IdentifierValidationError
from common.identity import AliasRef

from tableinator.batch_writer import PostgreSQLBatchWriter
from tableinator.record_persistence import PostgreSQLRecordPersistence
from tests.conftest import native_id_for


if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping, Sequence


CONTRACT = Path("contracts/catalog-events/v1/contract.json")

BARCODE = AliasRef("barcode", "release", "5012394144777")
MATRIX = AliasRef("matrix", "release", "PB 41447-A2 UTOPIA MS")
CATALOG_NUMBER = AliasRef("catalog_number", "release", "PB 41447")

EMPTY_BLOCK: dict[str, Any] = {
    "aliases": [],
    "identifiers_version": "1",
    "items": [],
    "types": [],
    "unmapped": {"types": []},
}


def contract_identifiers_block() -> dict[str, Any]:
    """Return the identifiers block the promoted producer contract publishes.

    Read from the promoted contract rather than transcribed, so a producer that changes
    the block's shape changes what these tests mint instead of leaving them agreeing with
    a stale copy.
    """
    fixture: dict[str, Any] = json.loads(CONTRACT.read_text())["fixture_payloads"]["discogs"]["releases"]["identifiers"]
    return fixture


def release(data_id: str, *, identifiers: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return a normalized release payload, with the identifiers block when given."""
    data: dict[str, Any] = {"id": data_id, "sha256": f"hash-{data_id}", "title": f"Release {data_id}"}
    if identifiers is not None:
        data["identifiers"] = identifiers
    return data


@dataclass
class Record:
    """The batch-record shape ``process_batch`` reads, without the delivery machinery."""

    data_id: str
    data: dict[str, Any]
    sha256: str = field(default="")

    def __post_init__(self) -> None:
        if not self.sha256:
            self.sha256 = f"hash-{self.data_id}"


def native_id(data_type: str, data_id: str) -> uuid.UUID:
    """Return the native id the offline resolver stub answers for one Discogs row."""
    from tableinator.identity import alias_ref

    return native_id_for(alias_ref(data_type, data_id))


class AttachRecorder:
    """Stand in for ``common.identity.attach_aliases`` and record every call it receives."""

    def __init__(self, returns: Mapping[AliasRef, uuid.UUID] | None = None) -> None:
        self.returns = dict(returns or {})
        self.calls: list[tuple[Any, dict[AliasRef, uuid.UUID], dict[str, Any]]] = []
        self.upserts_before_call: list[int] = []
        self.probe: Callable[[], int] = lambda: 0

    async def __call__(self, conn: Any, mapping: Mapping[AliasRef, uuid.UUID], **options: Any) -> dict[AliasRef, uuid.UUID]:
        self.calls.append((conn, dict(mapping), options))
        self.upserts_before_call.append(self.probe())
        return {ref: self.returns.get(ref, supplied) for ref, supplied in mapping.items()}

    @property
    def mapping(self) -> dict[AliasRef, uuid.UUID]:
        """Return the single mapping the loader attached, asserting there was one call."""
        assert len(self.calls) == 1, f"expected exactly one attach, saw {len(self.calls)}"
        return self.calls[0][1]


@pytest.fixture
def attach() -> Iterator[AttachRecorder]:
    """Patch the attach seam both write paths reach ``common.identity`` through."""
    recorder = AttachRecorder()
    with patch("tableinator.identity.attach_aliases", recorder):
        yield recorder


@pytest.fixture
def batch_writer(
    mock_postgres_connection: Any,
    mock_async_pool: Any,
    attach: AttachRecorder,
) -> tuple[PostgreSQLBatchWriter, Any, MagicMock]:
    """Return a batch writer over a mocked pool, with its connection and logger."""
    logger = MagicMock()
    cursor = mock_postgres_connection.cursor.return_value
    attach.probe = lambda: cursor.executemany.await_count
    writer = PostgreSQLBatchWriter(mock_async_pool(mock_postgres_connection), logger, lambda data: {"source": data["id"]})
    return writer, mock_postgres_connection, logger


@pytest.fixture
def record_persistence(
    mock_postgres_connection: Any,
    mock_async_pool: Any,
) -> tuple[PostgreSQLRecordPersistence, Any, MagicMock]:
    """Return the non-batch persistence over the same mocked pool."""
    logger = MagicMock()
    persistence = PostgreSQLRecordPersistence(
        mock_async_pool(mock_postgres_connection),
        logger,
        0.9,
        lambda data: {"source": data["id"]},
    )
    return persistence, mock_postgres_connection, logger


def existing(rows: Sequence[tuple[Any, ...]], connection: Any) -> None:
    """Make the batch writer's existing-rows SELECT answer with these rows."""
    connection.cursor.return_value.fetchall.return_value = list(rows)


class TestBatchPath:
    @pytest.mark.asyncio
    async def test_every_alias_bearing_identifier_type_attaches_to_the_release_native_id(
        self,
        batch_writer: tuple[PostgreSQLBatchWriter, Any, MagicMock],
        attach: AttachRecorder,
    ) -> None:
        """A mixed block mints one ref per alias namespace, all on the release's own id."""
        writer, connection, _logger = batch_writer
        messages = [
            Record("r1", release("r1", identifiers=contract_identifiers_block())),
            Record("r2", release("r2", identifiers=EMPTY_BLOCK)),
        ]

        await writer.process_batch("releases", messages)

        assert attach.mapping == {
            BARCODE: native_id("releases", "r1"),
            MATRIX: native_id("releases", "r1"),
            CATALOG_NUMBER: native_id("releases", "r1"),
        }
        assert attach.calls[0][0] is connection
        assert attach.calls[0][2] == {}

    @pytest.mark.asyncio
    async def test_aliases_attach_after_the_upsert_on_the_batch_transaction(
        self,
        batch_writer: tuple[PostgreSQLBatchWriter, Any, MagicMock],
        attach: AttachRecorder,
    ) -> None:
        """The attach runs once, after the rows it points at were written."""
        writer, connection, _logger = batch_writer

        await writer.process_batch("releases", [Record("r1", release("r1", identifiers=contract_identifiers_block()))])

        assert attach.upserts_before_call == [1]
        connection.set_autocommit.assert_awaited_once_with(False)
        connection.transaction.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_hash_unchanged_rows_still_attach_their_aliases(
        self,
        batch_writer: tuple[PostgreSQLBatchWriter, Any, MagicMock],
        attach: AttachRecorder,
    ) -> None:
        """The existing-rows SELECT cannot see aliases, so a skipped row is attached anyway."""
        writer, connection, _logger = batch_writer
        existing([("r1", "hash-r1", False, False)], connection)

        result = await writer.process_batch("releases", [Record("r1", release("r1", identifiers=contract_identifiers_block()))])

        assert result.unchanged_ids == {"r1"}
        assert connection.cursor.return_value.executemany.await_count == 0
        assert set(attach.mapping) == {BARCODE, MATRIX, CATALOG_NUMBER}
        assert attach.upserts_before_call == [0]

    @pytest.mark.asyncio
    async def test_an_empty_identifiers_block_attaches_nothing(
        self,
        batch_writer: tuple[PostgreSQLBatchWriter, Any, MagicMock],
        attach: AttachRecorder,
    ) -> None:
        """A block carrying no alias-bearing item makes no call at all."""
        writer, _connection, _logger = batch_writer

        await writer.process_batch("releases", [Record("r1", release("r1", identifiers=EMPTY_BLOCK))])

        assert attach.calls == []

    @pytest.mark.asyncio
    async def test_a_legacy_event_without_the_block_attaches_nothing(
        self,
        batch_writer: tuple[PostgreSQLBatchWriter, Any, MagicMock],
        attach: AttachRecorder,
    ) -> None:
        """The block is additive within v1, so an event produced before it still loads."""
        writer, _connection, _logger = batch_writer

        result = await writer.process_batch("releases", [Record("r1", release("r1"))])

        assert result.unchanged_ids == set()
        assert attach.calls == []

    @pytest.mark.asyncio
    async def test_a_non_release_table_attaches_nothing(
        self,
        batch_writer: tuple[PostgreSQLBatchWriter, Any, MagicMock],
        attach: AttachRecorder,
    ) -> None:
        """Only releases carry an identifiers block, so no other table mints aliases."""
        writer, _connection, _logger = batch_writer

        await writer.process_batch("artists", [Record("a1", {"id": "a1", "sha256": "hash-a1"})])

        assert attach.calls == []

    @pytest.mark.asyncio
    async def test_an_alias_owned_by_another_item_is_counted_and_logged_not_raised(
        self,
        batch_writer: tuple[PostgreSQLBatchWriter, Any, MagicMock],
        attach: AttachRecorder,
    ) -> None:
        """The existing alias wins; the batch reports the collision and still commits."""
        writer, _connection, logger = batch_writer
        incumbent = uuid.UUID("11111111-2222-3333-4444-555555555555")
        attach.returns = {BARCODE: incumbent}

        result = await writer.process_batch("releases", [Record("r1", release("r1", identifiers=contract_identifiers_block()))])

        assert result.unchanged_ids == set()
        assert logger.warning.call_count == 1
        assert logger.warning.call_args.kwargs == {"data_type": "releases", "aliases": 3, "alias_conflicts": 1}

    @pytest.mark.asyncio
    async def test_two_releases_printing_one_value_attach_it_once(
        self,
        batch_writer: tuple[PostgreSQLBatchWriter, Any, MagicMock],
        attach: AttachRecorder,
    ) -> None:
        """One alias holds one native id, so the first release in batch order takes it."""
        writer, _connection, _logger = batch_writer
        block = contract_identifiers_block()
        messages = [Record("r1", release("r1", identifiers=block)), Record("r2", release("r2", identifiers=block))]

        await writer.process_batch("releases", messages)

        assert attach.mapping == dict.fromkeys([BARCODE, MATRIX, CATALOG_NUMBER], native_id("releases", "r1"))

    @pytest.mark.asyncio
    async def test_a_malformed_block_fails_the_batch_deterministically(
        self,
        batch_writer: tuple[PostgreSQLBatchWriter, Any, MagicMock],
        attach: AttachRecorder,
    ) -> None:
        """A block that is not the promoted contract is a producer defect, not an outage."""
        writer, _connection, _logger = batch_writer
        malformed = {**EMPTY_BLOCK, "identifiers_version": "2"}

        with pytest.raises(IdentifierValidationError):
            await writer.process_batch("releases", [Record("r1", release("r1", identifiers=malformed))])

        assert attach.calls == []


class TestNonBatchPath:
    @pytest.mark.asyncio
    async def test_persist_record_attaches_the_same_aliases_for_one_release(
        self,
        record_persistence: tuple[PostgreSQLRecordPersistence, Any, MagicMock],
        attach: AttachRecorder,
    ) -> None:
        """The single-record path mints exactly what the batch path mints."""
        persistence, connection, _logger = record_persistence

        outcome = await persistence.persist_record("releases", "r1", release("r1", identifiers=contract_identifiers_block()))

        assert outcome == "processed"
        assert attach.mapping == dict.fromkeys([BARCODE, MATRIX, CATALOG_NUMBER], native_id("releases", "r1"))
        assert attach.calls[0][0] is connection

    @pytest.mark.asyncio
    async def test_persist_record_without_the_block_attaches_nothing(
        self,
        record_persistence: tuple[PostgreSQLRecordPersistence, Any, MagicMock],
        attach: AttachRecorder,
    ) -> None:
        """A legacy release event still persists on the non-batch path."""
        persistence, _connection, _logger = record_persistence

        assert await persistence.persist_record("releases", "r1", release("r1")) == "processed"
        assert attach.calls == []

    @pytest.mark.asyncio
    async def test_persist_record_counts_an_alias_owned_by_another_item(
        self,
        record_persistence: tuple[PostgreSQLRecordPersistence, Any, MagicMock],
        attach: AttachRecorder,
    ) -> None:
        """A collision is logged here too, and the record is still persisted."""
        persistence, _connection, logger = record_persistence
        attach.returns = {CATALOG_NUMBER: uuid.UUID("11111111-2222-3333-4444-555555555555")}

        outcome = await persistence.persist_record("releases", "r1", release("r1", identifiers=contract_identifiers_block()))

        assert outcome == "processed"
        assert logger.warning.call_args.kwargs == {"data_type": "releases", "aliases": 3, "alias_conflicts": 1}

    @pytest.mark.asyncio
    async def test_persist_record_attaches_nothing_for_a_non_release_table(
        self,
        record_persistence: tuple[PostgreSQLRecordPersistence, Any, MagicMock],
        attach: AttachRecorder,
    ) -> None:
        """Artists, labels, and masters carry no identifiers block."""
        persistence, _connection, _logger = record_persistence

        assert await persistence.persist_record("artists", "a1", {"id": "a1", "sha256": "hash-a1"}) == "processed"
        assert attach.calls == []
