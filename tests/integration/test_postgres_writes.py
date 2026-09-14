"""Real-PostgreSQL regressions for the loader's persistence boundary."""

import os
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import psycopg
import pytest
import pytest_asyncio
from psycopg import sql
from psycopg.types.json import Jsonb

from tableinator.batch_writer import PostgreSQLBatchWriter
from tableinator.record_persistence import PostgreSQLRecordPersistence


if TYPE_CHECKING:
    from collections.abc import AsyncIterator


pytestmark = pytest.mark.integration


class SingleConnectionPool:
    """Expose one test connection through the production pool protocol."""

    def __init__(self, connection: psycopg.AsyncConnection[Any]) -> None:
        self._connection = connection

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[psycopg.AsyncConnection[Any]]:
        yield self._connection


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
                    "updated_at timestamptz NOT NULL DEFAULT NOW())"
                ).format(sql.Identifier(table))
            )
        await connection.execute(
            "CREATE TABLE releases ("
            "hash text NOT NULL, data_id text PRIMARY KEY, data jsonb NOT NULL, "
            "media jsonb, updated_at timestamptz NOT NULL DEFAULT NOW())"
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

    assert await writer.process_batch("artists", [first]) == (set(), set())
    assert await writer.process_batch("artists", [changed]) == (set(), set())
    await postgres_connection.commit()
    row = await (await postgres_connection.execute("SELECT hash, data FROM artists WHERE data_id = '99'")).fetchone()

    assert row == ("batch-hash-2", changed.data)
