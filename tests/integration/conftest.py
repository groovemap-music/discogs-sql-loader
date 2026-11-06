"""Integration-suite setup: the promoted schema every PostgreSQL test writes against.

The loader's graph writes are schema-qualified to `graph`, so the `graph` schema and its
declared tables have to exist before any integration test runs — including the ones
that build their own synthetic entity tables in a throwaway schema, because a
schema-qualified write ignores the `search_path` they set. Applying the pinned
`groovemap-database-schema` initializer once per session is what puts them there, and it
is the same initializer `test_graph_schema.py` asserts against.
"""

import asyncio
import os
import uuid
from typing import TYPE_CHECKING, Any

import psycopg
import pytest
import pytest_asyncio
from common import AsyncPostgreSQLPool
from groovemap_schema.postgres import create_postgres_schema
from psycopg import sql


if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator


@pytest.fixture(scope="session", autouse=True)
def promoted_schema() -> Iterator[None]:
    """Apply the pinned database-schema initializer once, before any test connects."""
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        yield
        return

    async def apply() -> int:
        pool = AsyncPostgreSQLPool(
            connection_params={"conninfo": database_url},
            min_connections=1,
            max_connections=2,
            max_retries=1,
            health_check_interval=3600,
        )
        await pool.initialize()
        try:
            return await create_postgres_schema(pool)
        finally:
            await pool.close()

    failures = asyncio.run(apply())
    assert failures == 0, f"{failures} schema statements failed while applying the promoted schema"
    yield


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
