"""Integration-suite setup: the promoted schema every PostgreSQL test writes against.

The loader's graph writes are schema-qualified to `graph`, so the `graph` schema and its
twenty-seven tables have to exist before any integration test runs — including the ones
that build their own synthetic entity tables in a throwaway schema, because a
schema-qualified write ignores the `search_path` they set. Applying the pinned
`groovemap-database-schema` initializer once per session is what puts them there, and it
is the same initializer `test_graph_schema.py` asserts against.
"""

import asyncio
import os
from typing import TYPE_CHECKING

import pytest
from common import AsyncPostgreSQLPool
from groovemap_schema.postgres import create_postgres_schema


if TYPE_CHECKING:
    from collections.abc import Iterator


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
