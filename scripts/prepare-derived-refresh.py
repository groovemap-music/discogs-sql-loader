"""Fill promoted graph vertices and edges before timing the loader-owned refresh.

Use only on a disposable DB already loaded with the deterministic spike catalog. Each
relation commits separately, unlike graph.bootstrap_fill(), so an over-budget counter
does not roll back the entire fixture. Definitions come from the exact dev-pinned schema.
"""

import asyncio
import json
import os
import time

import psycopg
from groovemap_schema.postgres import _BOOTSTRAP_FILL_ORDER, _bootstrap_fill_source
from psycopg import sql


async def main() -> None:
    """Materialize only the source relations, leaving counters to the timed pass."""
    dsn = os.environ.get("TEST_DATABASE_URL")
    if not dsn:
        raise SystemExit("TEST_DATABASE_URL is required (a disposable populated database)")

    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as connection:
        for relation in _BOOTSTRAP_FILL_ORDER:
            if relation == "genre_stats":
                break
            columns, body = _bootstrap_fill_source(relation)
            projection = sql.SQL(", ").join(sql.Identifier(column) for column in columns)
            started = time.perf_counter()
            async with connection.transaction(), connection.cursor() as cursor:
                await cursor.execute(sql.SQL("TRUNCATE graph.{relation}").format(relation=sql.Identifier(relation)))
                await cursor.execute(
                    sql.SQL("INSERT INTO graph.{relation} ({columns}) SELECT {columns} FROM ({body}) AS bootstrap").format(
                        relation=sql.Identifier(relation), columns=projection, body=sql.SQL(body)
                    )
                )
                rows = cursor.rowcount
            # The production writer commits per document; a one-off bulk preparation
            # must supply planner statistics before the timed aggregate pass instead
            # of measuring its own unanalyzed-load artifact.
            await connection.execute(sql.SQL("ANALYZE graph.{relation}").format(relation=sql.Identifier(relation)))
            print(json.dumps({"relation": f"graph.{relation}", "rows": rows, "seconds": round(time.perf_counter() - started, 3)}), flush=True)


if __name__ == "__main__":
    asyncio.run(main())
