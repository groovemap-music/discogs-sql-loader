"""The promoted database-schema initializer must create every graph table.

`groovemap-database-schema` is pinned as a dev dependency at the revision recorded in
`contracts/persistence/v1/source.json`. Applying its initializer is what proves this
loader builds against a schema that actually creates the `graph` schema's tables — the
relations `contracts/persistence/v1/compatibility.json` declares with
`graph_schema.relations[*].shape == "table"` under `graph_schema.schema` (`graph`) — rather
than merely vendoring a contract copy nothing exercises.
"""

import json
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
import pytest_asyncio
from common import AsyncPostgreSQLPool
from groovemap_schema.postgres import create_postgres_schema


if TYPE_CHECKING:
    from collections.abc import AsyncIterator


pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]
COMPATIBILITY_PATH = ROOT / "contracts/persistence/v1/compatibility.json"


def _expected_graph_tables() -> tuple[str, ...]:
    """Return every relation the promoted persistence contract declares as a table."""
    compatibility = json.loads(COMPATIBILITY_PATH.read_text())
    graph_schema = compatibility["graph_schema"]
    relations = graph_schema["relations"]
    tables = tuple(sorted(name for name, relation in relations.items() if relation["shape"] == "table"))
    assert len(tables) == graph_schema["tables"], "contract's declared table count drifted from its relations map"
    return tables


@pytest_asyncio.fixture
async def schema_pool() -> AsyncIterator[AsyncPostgreSQLPool]:
    """A pool against the integration PostgreSQL, ready for `create_postgres_schema`."""
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("TEST_DATABASE_URL is required for PostgreSQL integration tests")

    pool = AsyncPostgreSQLPool(
        connection_params={"conninfo": database_url},
        min_connections=1,
        max_connections=2,
        max_retries=1,
        health_check_interval=3600,
    )
    await pool.initialize()
    try:
        yield pool
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_promoted_initializer_creates_every_graph_table(schema_pool: AsyncPostgreSQLPool) -> None:
    """Applying the pinned database-schema initializer creates every declared graph table."""
    expected_tables = _expected_graph_tables()

    failures = await create_postgres_schema(schema_pool)
    assert failures == 0, f"{failures} schema statements failed while creating the PostgreSQL schema"

    async with schema_pool.connection() as conn, conn.cursor() as cursor:
        await cursor.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname = %s",
            ("graph",),
        )
        rows: list[tuple[Any, ...]] = await cursor.fetchall()
    observed_tables = {row[0] for row in rows}

    missing = sorted(set(expected_tables) - observed_tables)
    assert not missing, f"database-schema at the promoted revision did not create: {missing}"
