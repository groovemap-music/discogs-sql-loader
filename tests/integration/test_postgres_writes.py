"""Real-PostgreSQL regressions for the loader's persistence boundary."""

import os
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

import psycopg
import pytest
import pytest_asyncio
from orjson import dumps
from psycopg import sql
from psycopg.errors import OperationalError
from psycopg.types.json import Jsonb

from tableinator.batch_processor import BatchConfig, PendingMessage, PostgreSQLBatchProcessor
from tableinator.batch_writer import BatchWriteResult, PostgreSQLBatchWriter
from tableinator.record_persistence import PostgreSQLRecordPersistence
from tableinator.tableinator import on_data_message


if TYPE_CHECKING:
    from collections.abc import AsyncIterator


pytestmark = pytest.mark.integration


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


@dataclass(frozen=True)
class BatchRecord:
    data_id: str
    data: dict[str, Any]
    sha256: str


@pytest_asyncio.fixture
async def postgres_connection() -> AsyncIterator[psycopg.AsyncConnection[Any]]:
    """Create isolated synthetic loader tables in a unique PostgreSQL schema."""
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("TEST_DATABASE_URL is required for PostgreSQL integration tests")

    connection = await psycopg.AsyncConnection.connect(database_url)
    await connection.set_autocommit(True)
    schema_name = f"discogs_sql_loader_test_{uuid.uuid4().hex}"
    schema = sql.Identifier(schema_name)

    try:
        await connection.execute(sql.SQL("CREATE SCHEMA {}").format(schema))
        await connection.execute(sql.SQL("SET search_path TO {}").format(schema))
        for table in ("artists", "labels", "masters"):
            await connection.execute(
                sql.SQL(
                    "CREATE TABLE {} ("
                    "hash text NOT NULL, data_id text PRIMARY KEY, data jsonb NOT NULL, "
                    "gm_item_id uuid, updated_at timestamptz NOT NULL DEFAULT NOW())"
                ).format(sql.Identifier(table))
            )
        await connection.execute(
            "CREATE TABLE releases ("
            "hash text NOT NULL, data_id text PRIMARY KEY, data jsonb NOT NULL, "
            "media jsonb, gm_item_id uuid, updated_at timestamptz NOT NULL DEFAULT NOW())"
        )
        # The native identity tables `common.identity.resolve_aliases` reads and mints
        # into, shaped as database-schema declares them (ADR 0009). The partial unique
        # index is what makes the lookup-or-create converge, so it is not optional here.
        await connection.execute(
            "CREATE TABLE catalog_items ("
            "id uuid PRIMARY KEY DEFAULT uuidv7(), "
            "kind text NOT NULL CHECK (kind IN ('release', 'master', 'artist', 'label')), "
            "created_at timestamptz NOT NULL DEFAULT NOW())"
        )
        await connection.execute(
            "CREATE TABLE provider_aliases ("
            "id uuid PRIMARY KEY DEFAULT uuidv7(), "
            "provider text NOT NULL, entity_kind text NOT NULL, external_id text NOT NULL, "
            "native_id uuid NOT NULL, valid_from timestamptz NOT NULL DEFAULT NOW(), "
            "valid_to timestamptz, confidence real NOT NULL DEFAULT 1.0, "
            "source text NOT NULL DEFAULT 'catalog', asserted_at timestamptz NOT NULL DEFAULT NOW())"
        )
        await connection.execute(
            "CREATE UNIQUE INDEX idx_provider_aliases_lookup ON provider_aliases (provider, entity_kind, external_id) WHERE valid_to IS NULL"
        )
        yield connection
    finally:
        if not connection.autocommit:
            await connection.rollback()
            await connection.set_autocommit(True)
        await connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(schema))
        await connection.close()


@pytest.mark.asyncio
async def test_single_record_upsert_round_trips_jsonb_and_preserves_hash_gate(
    postgres_connection: psycopg.AsyncConnection[Any],
) -> None:
    """Changed hashes update JSONB; unchanged hashes do not rewrite payloads."""
    pool = SingleConnectionPool(postgres_connection)
    persistence = PostgreSQLRecordPersistence(pool, MagicMock(), 0.9, lambda data: {"source": data["id"]})
    original = {"id": "42", "name": "First", "sha256": "hash-1", "nested": {"year": 1977}}

    await persistence.persist_record("artists", "42", original)
    row = await (await postgres_connection.execute("SELECT hash, data FROM artists WHERE data_id = '42'")).fetchone()
    assert row == ("hash-1", original)

    changed = {**original, "name": "Changed", "sha256": "hash-2"}
    await persistence.persist_record("artists", "42", changed)
    row = await (await postgres_connection.execute("SELECT hash, data FROM artists WHERE data_id = '42'")).fetchone()
    assert row == ("hash-2", changed)

    same_hash_new_payload = {**changed, "name": "Must not replace stored data"}
    await persistence.persist_record("artists", "42", same_hash_new_payload)
    row = await (await postgres_connection.execute("SELECT hash, data FROM artists WHERE data_id = '42'")).fetchone()
    assert row == ("hash-2", changed)


@pytest.mark.asyncio
async def test_release_upsert_backfills_media_on_an_unchanged_hash(
    postgres_connection: psycopg.AsyncConnection[Any],
) -> None:
    """The real CTE reports and fills legacy NULL media without rewriting data."""
    pool = SingleConnectionPool(postgres_connection)
    canonical_media = {"kind": "vinyl", "format": "LP"}
    persistence = PostgreSQLRecordPersistence(pool, MagicMock(), 0.9, lambda _data: canonical_media)
    stored = {"id": "7", "title": "Legacy", "sha256": "same-hash"}
    await postgres_connection.execute(
        "INSERT INTO releases (hash, data_id, data, media) VALUES (%s, %s, %s, NULL)",
        ("same-hash", "7", Jsonb(stored)),
    )

    outcome = await persistence.persist_record("releases", "7", {**stored, "title": "Ignored because hash is unchanged"})
    row = await (await postgres_connection.execute("SELECT hash, data, media FROM releases WHERE data_id = '7'")).fetchone()

    assert outcome == "media_backfilled"
    assert row == ("same-hash", stored, canonical_media)


@pytest.mark.asyncio
async def test_batch_writer_updates_conflicts_and_round_trips_jsonb(
    postgres_connection: psycopg.AsyncConnection[Any],
) -> None:
    """The batch executemany path performs its advertised atomic conflict update."""
    pool = SingleConnectionPool(postgres_connection)
    writer = PostgreSQLBatchWriter(pool, MagicMock(), lambda data: {"source": data["id"]})
    first = BatchRecord("99", {"id": "99", "name": "First"}, "batch-hash-1")
    changed = BatchRecord("99", {"id": "99", "name": "Changed", "tags": ["a", "b"]}, "batch-hash-2")

    assert await writer.process_batch("artists", [first]) == (set(), set(), set())
    assert await writer.process_batch("artists", [changed]) == (set(), set(), set())
    await postgres_connection.commit()
    row = await (await postgres_connection.execute("SELECT hash, data, gm_item_id FROM artists WHERE data_id = '99'")).fetchone()

    assert row is not None
    assert (row[0], row[1]) == ("batch-hash-2", changed.data)
    # The same Discogs id resolves to the same native item across both writes (ADR 0009).
    alias = await (
        await postgres_connection.execute(
            "SELECT native_id FROM provider_aliases WHERE provider = 'discogs' AND entity_kind = 'artist' AND external_id = '99'"
        )
    ).fetchone()
    assert alias is not None
    assert row[2] == alias[0]
    kind = await (await postgres_connection.execute("SELECT kind FROM catalog_items WHERE id = %s", (row[2],))).fetchone()
    assert kind == ("artist",)


@pytest.mark.asyncio
async def test_batch_writer_backfills_a_row_written_before_minting_existed(
    postgres_connection: psycopg.AsyncConnection[Any],
) -> None:
    """A hash-unchanged row with a NULL `gm_item_id` is minted in place, payload untouched."""
    pool = SingleConnectionPool(postgres_connection)
    writer = PostgreSQLBatchWriter(pool, MagicMock(), lambda data: {"source": data["id"]})
    legacy = {"id": "77", "name": "Written before ADR 0009"}
    await postgres_connection.execute(
        "INSERT INTO artists (hash, data_id, data, gm_item_id) VALUES (%s, %s, %s, NULL)",
        ("legacy-hash", "77", Jsonb(legacy)),
    )

    result = await writer.process_batch("artists", [BatchRecord("77", legacy, "legacy-hash")])
    await postgres_connection.commit()

    assert result == ({"77"}, set(), {"77"})
    row = await (await postgres_connection.execute("SELECT hash, data, gm_item_id FROM artists WHERE data_id = '77'")).fetchone()
    assert row is not None
    assert (row[0], row[1]) == ("legacy-hash", legacy)
    assert row[2] is not None


@pytest.mark.asyncio
async def test_persist_record_writes_the_native_id_on_the_non_batch_path(
    postgres_connection: psycopg.AsyncConnection[Any],
) -> None:
    """The single-record path mints and writes `gm_item_id` in its own transaction."""
    pool = SingleConnectionPool(postgres_connection)
    persistence = PostgreSQLRecordPersistence(pool, MagicMock(), 0.9, lambda data: {"source": data["id"]})

    await persistence.persist_record("labels", "5", {"id": "5", "name": "Label", "sha256": "hash-1"})

    row = await (await postgres_connection.execute("SELECT gm_item_id FROM labels WHERE data_id = '5'")).fetchone()
    assert row is not None
    assert row[0] is not None
    kind = await (await postgres_connection.execute("SELECT kind FROM catalog_items WHERE id = %s", (row[0],))).fetchone()
    assert kind == ("label",)


def engine_config(**overrides: Any) -> BatchConfig:
    values: dict[str, Any] = {
        "batch_size": 10,
        "flush_interval": 0.01,
        "max_pending": 20,
        "max_concurrent_flushes": 2,
        "min_batch_size": 1,
        "backoff_initial": 0.001,
        "backoff_max": 0.002,
        "backoff_multiplier": 2.0,
        "max_flush_retries": 3,
        "max_poison_retries": 2,
    }
    values.update(overrides)
    return BatchConfig(**values)


@pytest.mark.asyncio
async def test_shared_batch_engine_writes_real_postgres_and_settles_each_delivery_once(
    postgres_connection: psycopg.AsyncConnection[Any],
) -> None:
    pool = SingleConnectionPool(postgres_connection)
    processor = PostgreSQLBatchProcessor(pool, engine_config(batch_size=2))
    acks = [AsyncMock(), AsyncMock()]
    nacks = [AsyncMock(), AsyncMock()]

    await processor.add_message("artists", {"id": "201", "name": "First", "sha256": "h-201"}, acks[0], nacks[0])
    await processor.add_message("artists", {"id": "202", "name": "Second", "sha256": "h-202"}, acks[1], nacks[1])

    rows = await (await postgres_connection.execute("SELECT data_id, data->>'name' FROM artists ORDER BY data_id")).fetchall()
    assert rows == [("201", "First"), ("202", "Second")]
    assert all(callback.await_count == 1 for callback in acks)
    assert all(callback.await_count == 0 for callback in nacks)
    assert processor.get_stats()["processed"]["artists"] == 2


@pytest.mark.asyncio
async def test_shared_batch_engine_preserves_real_unchanged_media_and_identity_results(
    postgres_connection: psycopg.AsyncConnection[Any],
) -> None:
    pool = SingleConnectionPool(postgres_connection)
    processor = PostgreSQLBatchProcessor(pool, engine_config(batch_size=3))
    writer = PostgreSQLBatchWriter(pool, MagicMock(), lambda data: {"source": data["id"]})

    await writer.process_batch("artists", [BatchRecord("301", {"id": "301", "name": "Stable"}, "same")])
    await postgres_connection.execute(
        "INSERT INTO artists (hash, data_id, data, gm_item_id) VALUES (%s, %s, %s, NULL)",
        ("legacy", "302", Jsonb({"id": "302", "name": "Legacy"})),
    )
    await postgres_connection.execute(
        "INSERT INTO releases (hash, data_id, data, media, gm_item_id) VALUES (%s, %s, %s, NULL, NULL)",
        ("release", "303", Jsonb({"id": "303", "title": "Legacy release"})),
    )
    await postgres_connection.commit()

    artist_acks = [AsyncMock(), AsyncMock()]
    await processor.add_message("artists", {"id": "301", "name": "Ignored", "sha256": "same"}, artist_acks[0], AsyncMock())
    await processor.add_message("artists", {"id": "302", "name": "Legacy", "sha256": "legacy"}, artist_acks[1], AsyncMock())
    await processor.flush_queue("artists")
    release_ack = AsyncMock()
    await processor.add_message(
        "releases",
        {"id": "303", "title": "Legacy release", "sha256": "release", "formats": [{"name": "Vinyl", "qty": "1"}]},
        release_ack,
        AsyncMock(),
    )
    await processor.flush_queue("releases")

    assert all(callback.await_count == 1 for callback in artist_acks)
    release_ack.assert_awaited_once()
    stats = processor.get_stats()
    assert stats["identity_backfilled"]["artists"] == 1
    assert stats["media_backfilled"]["releases"] == 1
    rows = await (
        await postgres_connection.execute(
            "SELECT (SELECT gm_item_id IS NOT NULL FROM artists WHERE data_id = '302'), "
            "(SELECT media IS NOT NULL FROM releases WHERE data_id = '303')"
        )
    ).fetchone()
    assert rows == (True, True)


@pytest.mark.asyncio
async def test_shared_engine_retries_a_transient_before_real_postgres_commit(
    postgres_connection: psycopg.AsyncConnection[Any],
) -> None:
    processor = PostgreSQLBatchProcessor(SingleConnectionPool(postgres_connection), engine_config(batch_size=1))
    real_write = processor._process_batch
    attempts = 0

    async def fail_once(data_type: str, messages: list[PendingMessage]) -> BatchWriteResult:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OperationalError("temporary outage")
        return await real_write(data_type, messages)

    processor._process_batch = fail_once  # type: ignore[method-assign]
    ack = AsyncMock()
    nack = AsyncMock()
    await processor.add_message("labels", {"id": "401", "name": "Recovered", "sha256": "h"}, ack, nack)
    assert await processor.flush_queue("labels")

    assert attempts == 2
    ack.assert_awaited_once()
    nack.assert_not_awaited()
    row = await (await postgres_connection.execute("SELECT data->>'name' FROM labels WHERE data_id = '401'")).fetchone()
    assert row == ("Recovered",)


@pytest.mark.asyncio
async def test_shared_engine_isolates_poison_vetoes_purge_and_commits_healthy_tail(
    postgres_connection: psycopg.AsyncConnection[Any],
) -> None:
    processor = PostgreSQLBatchProcessor(
        SingleConnectionPool(postgres_connection),
        engine_config(batch_size=2, max_poison_retries=2),
    )
    real_write = processor._process_batch

    async def reject_poison(data_type: str, messages: list[PendingMessage]) -> BatchWriteResult:
        if any(message.data_id == "poison" for message in messages):
            raise ValueError("deterministic poison")
        return await real_write(data_type, messages)

    processor._process_batch = reject_poison  # type: ignore[method-assign]
    poison_ack = AsyncMock()
    poison_nack = AsyncMock()
    healthy_ack = AsyncMock()
    healthy_nack = AsyncMock()
    await processor.add_message("masters", {"id": "poison", "title": "Bad", "sha256": "bad"}, poison_ack, poison_nack)
    await processor.add_message("masters", {"id": "healthy", "title": "Good", "sha256": "good"}, healthy_ack, healthy_nack)

    poison_ack.assert_not_awaited()
    poison_nack.assert_awaited_once()
    healthy_ack.assert_awaited_once()
    healthy_nack.assert_not_awaited()
    assert processor.had_dlq_nacks("masters")
    row = await (await postgres_connection.execute("SELECT data->>'title' FROM masters WHERE data_id = 'healthy'")).fetchone()
    assert row == ("Good",)


@pytest.mark.asyncio
async def test_shutdown_drain_commits_pending_shared_engine_work(
    postgres_connection: psycopg.AsyncConnection[Any],
) -> None:
    processor = PostgreSQLBatchProcessor(SingleConnectionPool(postgres_connection), engine_config(batch_size=10))
    acks = [AsyncMock(), AsyncMock()]
    await processor.add_message("labels", {"id": "501", "name": "One", "sha256": "one"}, acks[0], AsyncMock())
    await processor.add_message("labels", {"id": "502", "name": "Two", "sha256": "two"}, acks[1], AsyncMock())
    processor.shutdown()

    assert await processor.flush_all()
    assert all(callback.await_count == 1 for callback in acks)
    count = await (await postgres_connection.execute("SELECT count(*) FROM labels WHERE data_id IN ('501', '502')")).fetchone()
    assert count == (2,)


@pytest.mark.asyncio
async def test_non_batch_handler_uses_shared_delivery_runner_against_real_postgres(
    postgres_connection: psycopg.AsyncConnection[Any],
) -> None:
    incoming = AsyncMock()
    incoming.body = dumps({"id": "601", "name": "Direct", "sha256": "direct"})
    incoming.headers = None

    with (
        patch("tableinator.tableinator.BATCH_MODE", False),
        patch("tableinator.tableinator.shutdown_requested", False),
        patch("tableinator.tableinator.connection_pool", SingleConnectionPool(postgres_connection)),
    ):
        await on_data_message(incoming, "artists")

    incoming.ack.assert_awaited_once()
    incoming.nack.assert_not_awaited()
    row = await (await postgres_connection.execute("SELECT data->>'name' FROM artists WHERE data_id = '601'")).fetchone()
    assert row == ("Direct",)
