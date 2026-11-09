"""Reload seen lists on real PostgreSQL, through the loader's own write paths and purge.

These run only against a database whose schema declares `loader_seen` (database-schema's
`reload_seen_lists`, gm-database-schema-uw6d); against an older schema they skip, as the loader
itself falls back to touch mode. They write the real `public` entity tables, because the
`loader_seen` functions name them, and each test empties the table it uses first.

What they prove, for gm-discogs-sql-loader-g77f:

- shadow: the touch purge and the seen purge name exactly the same rows on a reload that has
  unchanged, changed, new, backfilled, single-record and gone-upstream rows;
- seen: an unchanged reload rewrites no entity row, and the purge deletes exactly the gone rows
  and their graph rows;
- a list lost to a real crash, a partial list below the guard, and a redelivered signal purge
  nothing, and no live row is deleted in any of them;
- WAL and rewritten tuples of an unchanged reload, touch against seen.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock

import psycopg
import pytest
import pytest_asyncio
import structlog
from common import AsyncPostgreSQLPool, WriteAdmission
from orjson import dumps
from psycopg import sql

import tableinator.tableinator as service
from tableinator.batch_writer import PostgreSQLBatchWriter
from tableinator.extraction_latch import probe_latch_relation, purge_was_vetoed, record_purge_veto
from tableinator.media import media_for_release
from tableinator.record_persistence import PostgreSQLRecordPersistence
from tableinator.seen_list import SeenMode, probe_seen_lists, read_shadow


if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence
    from datetime import datetime


pytestmark = [pytest.mark.asyncio, pytest.mark.integration, pytest.mark.timeout(120)]

TABLE = "releases"
CAP = 0.90


@dataclass(frozen=True)
class Record:
    data_id: str
    data: dict[str, Any]
    sha256: str


def _ids(first: int, last: int) -> list[str]:
    return [f"g77f-{n}" for n in range(first, last + 1)]


def _release(data_id: str, version: int = 1, padding: int = 0) -> Record:
    """A release asserting graph rows (artist, label, genre), with an optional body padding."""
    data: dict[str, Any] = {
        "id": data_id,
        "title": f"Release {data_id} v{version}",
        "artists": [{"id": f"{data_id}-artist", "name": "Artist"}],
        "labels": [{"id": f"{data_id}-label", "name": "Label", "catno": "CAT-1"}],
        "genres": ["Rock"],
        "styles": ["Indie Rock"],
        "notes": "x" * padding,
    }
    sha256 = hashlib.sha256(f"{data_id}:{version}:{padding}".encode()).hexdigest()
    data["sha256"] = sha256
    return Record(data_id, data, sha256)


async def _value(conn: psycopg.AsyncConnection[Any], query: str, params: Sequence[Any] | None = None) -> Any:
    cursor = await conn.execute(query, params)  # type: ignore[arg-type]
    row = await cursor.fetchone()
    return row[0] if row else None


@pytest_asyncio.fixture
async def dsn() -> AsyncIterator[str]:
    """The disposable database, emptied of this suite's table; skips without `loader_seen`."""
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL is required for PostgreSQL integration tests")
    async with await psycopg.AsyncConnection.connect(url, autocommit=True) as conn:
        if await _value(conn, "SELECT to_regclass('loader_seen.lists') IS NULL"):
            pytest.skip("the schema under test does not declare loader_seen (database-schema reload_seen_lists)")
        await conn.execute(sql.SQL("TRUNCATE {} CASCADE").format(sql.Identifier(TABLE)))
        await conn.execute("SELECT loader_seen.start('discogs', %s)", (TABLE,))
    yield url


async def _pool(url: str, max_connections: int = 2) -> AsyncPostgreSQLPool:
    pool = AsyncPostgreSQLPool(
        connection_params={"conninfo": url},
        min_connections=1,
        max_connections=max_connections,
        max_retries=1,
        health_check_interval=3600,
    )
    await pool.initialize()
    return pool


@pytest_asyncio.fixture
async def pool(dsn: str) -> AsyncIterator[AsyncPostgreSQLPool]:
    pool = await _pool(dsn)
    try:
        yield pool
    finally:
        await pool.close()


async def _write(pool: Any, mode: SeenMode, records: Sequence[Record], batch_size: int = 50) -> None:
    """Write RECORDS through the real batch writer, in the loader's batch transactions."""
    writer = PostgreSQLBatchWriter(pool, structlog.get_logger(), media_for_release, WriteAdmission(2), mode)
    for first in range(0, len(records), batch_size):
        await writer.process_batch(TABLE, records[first : first + batch_size])


async def _begin_extraction(dsn: str) -> datetime:
    """Age every row and the list to last month, then return a started_at of a minute ago.

    The list counts as started two hours ago, well before the clock margin, which is what a list
    restarted when the previous extraction settled looks like.
    """
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
        await conn.execute(sql.SQL("UPDATE {} SET updated_at = now() - interval '30 days'").format(sql.Identifier(TABLE)))
        await conn.execute("SELECT loader_seen.start('discogs', %s)", (TABLE,))
        await conn.execute(
            "UPDATE loader_seen.lists SET opened_at = now() - interval '2 hours' WHERE loader = 'discogs' AND data_type = %s", (TABLE,)
        )
        return await _value(conn, "SELECT now() - interval '1 minute'")  # type: ignore[no-any-return]


async def _rows(dsn: str) -> set[str]:
    async with await psycopg.AsyncConnection.connect(dsn) as conn:
        cursor = await conn.execute(sql.SQL("SELECT data_id FROM {}").format(sql.Identifier(TABLE)))
        return {str(row[0]) for row in await cursor.fetchall()}


async def _graph_rows(dsn: str, data_ids: Sequence[str]) -> int:
    async with await psycopg.AsyncConnection.connect(dsn) as conn:
        return int(await _value(conn, "SELECT count(*) FROM graph.in_genre WHERE release_id = ANY(%s)", (list(data_ids),)))


async def _list_state(dsn: str) -> tuple[Any, int]:
    async with await psycopg.AsyncConnection.connect(dsn) as conn:
        opened_at = await _value(conn, "SELECT opened_at FROM loader_seen.lists WHERE loader = 'discogs' AND data_type = %s", (TABLE,))
        keys = await _value(conn, sql.SQL("SELECT count(*) FROM loader_seen.{}").format(sql.Identifier(f"discogs_{TABLE}")).as_string())
        return opened_at, int(keys)


def _persistence(pool: Any, mode: SeenMode, logger: Any = None) -> PostgreSQLRecordPersistence:
    return PostgreSQLRecordPersistence(pool, logger or MagicMock(), CAP, media_for_release, mode)


async def _reload_with_every_path(dsn: str, pool: Any, mode: SeenMode) -> tuple[datetime, set[str]]:
    """Load 120 releases, then reload them the way a real dump arrives. Returns (started_at, gone).

    The reload carries 1-90 unchanged (1-5 of them with a NULL media to backfill), 91-100
    changed, 101 unchanged through the single-record path, 121-130 new; 102-120 are gone.
    """
    await _write(pool, SeenMode.TOUCH, [_release(data_id) for data_id in _ids(1, 120)])
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
        await conn.execute(sql.SQL("UPDATE {} SET media = NULL WHERE data_id = ANY(%s)").format(sql.Identifier(TABLE)), (_ids(1, 5),))
    started_at = await _begin_extraction(dsn)

    reload = [_release(data_id) for data_id in _ids(1, 90)]
    reload += [_release(data_id, version=2) for data_id in _ids(91, 100)]
    reload += [_release(data_id) for data_id in _ids(121, 130)]
    await _write(pool, mode, reload)
    single = _release(_ids(101, 101)[0])
    await _persistence(pool, mode).persist_record(TABLE, single.data_id, single.data)
    return started_at, set(_ids(102, 120))


async def test_the_probe_finds_the_declared_schema(pool: AsyncPostgreSQLPool) -> None:
    assert await probe_seen_lists(pool, MagicMock()) is True


async def test_a_shadow_reload_names_the_same_purge_set_both_ways(dsn: str, pool: AsyncPostgreSQLPool) -> None:
    started_at, gone = await _reload_with_every_path(dsn, pool, SeenMode.SHADOW)
    live = await _rows(dsn) - gone

    async with pool.connection() as conn, conn.cursor() as cursor:
        shadow = await read_shadow(cursor, TABLE, started_at)
    assert shadow.intact
    assert shadow.total_rows == 130
    assert shadow.touch_purge == shadow.seen_purge == len(gone)
    assert shadow.refreshed_not_seen == 0
    assert shadow.seen_not_refreshed == 0
    assert shadow.matches

    logger = MagicMock()
    veto = await _persistence(pool, SeenMode.SHADOW, logger).purge_stale_rows(TABLE, started_at.isoformat(), 111)
    assert veto is None
    # The comparison is logged before the touch purge, with every column and the guard's view.
    logged = next(call for call in logger.info.call_args_list if "discogs_releases_shadow" in call.args[0])
    assert logged.kwargs["matches"] is True
    assert logged.kwargs["touch_purge"] == logged.kwargs["seen_purge"] == len(gone)
    assert logged.kwargs["seen_keys"] == 111
    assert logged.kwargs["seen_ratio"] == 1.0
    assert logged.kwargs["guard"] is None
    # The touch purge is still the real one, and the list restarts for the next extraction.
    assert await _rows(dsn) == live
    assert await _graph_rows(dsn, sorted(gone)) == 0
    assert await _graph_rows(dsn, sorted(live)) == len(live)
    opened_at, keys = await _list_state(dsn)
    assert opened_at > started_at
    assert keys == 0


async def test_a_seen_reload_rewrites_no_unchanged_row_and_purges_the_same_set(dsn: str, pool: AsyncPostgreSQLPool) -> None:
    started_at, gone = await _reload_with_every_path(dsn, pool, SeenMode.SEEN)
    live = await _rows(dsn) - gone

    async with await psycopg.AsyncConnection.connect(dsn) as conn:
        untouched = await _value(
            conn,
            sql.SQL("SELECT count(*) FROM {} WHERE updated_at < %s").format(sql.Identifier(TABLE)),
            (started_at,),
        )
        backfilled = await _value(
            conn,
            sql.SQL("SELECT count(*) FROM {} WHERE data_id = ANY(%s) AND media IS NOT NULL").format(sql.Identifier(TABLE)),
            (_ids(1, 5),),
        )
    # 6-90 were not touched (1-5 were backfilled, 101 went through the single-record upsert, which
    # still rewrites), plus the 19 gone rows.
    assert untouched == 85 + len(gone)
    assert backfilled == 5

    veto = await _persistence(pool, SeenMode.SEEN).purge_stale_rows(TABLE, started_at.isoformat(), 111)
    assert veto is None
    assert await _rows(dsn) == live
    assert live == set(_ids(1, 101)) | set(_ids(121, 130))
    assert await _graph_rows(dsn, sorted(gone)) == 0
    assert await _graph_rows(dsn, sorted(live)) == len(live)
    opened_at, keys = await _list_state(dsn)
    assert opened_at > started_at
    assert keys == 0


async def test_a_redelivered_signal_after_a_seen_purge_deletes_and_restarts_nothing(dsn: str, pool: AsyncPostgreSQLPool) -> None:
    started_at, gone = await _reload_with_every_path(dsn, pool, SeenMode.SEEN)
    persistence = _persistence(pool, SeenMode.SEEN)
    assert await persistence.purge_stale_rows(TABLE, started_at.isoformat(), 111) is None
    rows = await _rows(dsn)
    opened_at, _keys = await _list_state(dsn)

    assert await persistence.purge_stale_rows(TABLE, started_at.isoformat(), 111) is None
    assert await _rows(dsn) == rows
    assert not rows & gone
    assert (await _list_state(dsn))[0] == opened_at


async def test_a_partial_list_below_the_guard_purges_nothing(dsn: str, pool: AsyncPostgreSQLPool) -> None:
    await _write(pool, SeenMode.TOUCH, [_release(data_id) for data_id in _ids(1, 120)])
    started_at = await _begin_extraction(dsn)
    # The extractor sent 100 records, but only 90 reached the list (below 0.95 x 100).
    await _write(pool, SeenMode.SEEN, [_release(data_id) for data_id in _ids(1, 90)])
    before = await _rows(dsn)
    opened_before, _keys = await _list_state(dsn)

    veto = await _persistence(pool, SeenMode.SEEN).purge_stale_rows(TABLE, started_at.isoformat(), 100)
    assert veto is not None
    assert "90 ids, below 0.95 of the 100 records" in veto
    assert await _rows(dsn) == before
    # Restarting is the caller's, after it has recorded the veto.
    assert (await _list_state(dsn))[0] == opened_before

    # No producer count: nothing to hold the list against, so nothing is purged either.
    assert await _persistence(pool, SeenMode.SEEN).purge_stale_rows(TABLE, started_at.isoformat(), None) is not None
    assert await _rows(dsn) == before


async def test_the_delete_fraction_cap_still_refuses_a_seen_purge(dsn: str, pool: AsyncPostgreSQLPool) -> None:
    await _write(pool, SeenMode.TOUCH, [_release(data_id) for data_id in _ids(1, 120)])
    started_at = await _begin_extraction(dsn)
    await _write(pool, SeenMode.SEEN, [_release(data_id) for data_id in _ids(1, 10)])
    before = await _rows(dsn)

    logger = MagicMock()
    assert await _persistence(pool, SeenMode.SEEN, logger).purge_stale_rows(TABLE, started_at.isoformat(), 10) is None
    assert await _rows(dsn) == before
    assert "Refusing to purge 110/120" in logger.error.call_args.args[0]
    opened_at, _keys = await _list_state(dsn)
    assert opened_at > started_at


async def test_the_handler_records_a_short_list_as_a_purge_veto(dsn: str, pool: AsyncPostgreSQLPool, monkeypatch: pytest.MonkeyPatch) -> None:
    """Through the real terminal-signal handler: the veto lands in purge_vetoed, then the list restarts."""
    await _write(pool, SeenMode.TOUCH, [_release(data_id) for data_id in _ids(1, 120)])
    started_at = await _begin_extraction(dsn)
    await _write(pool, SeenMode.SEEN, [_release(data_id) for data_id in _ids(1, 50)])
    before = await _rows(dsn)
    version = f"g77f{started_at.strftime('%H%M%S%f')}"

    latch = await probe_latch_relation(pool, MagicMock())
    assert latch is not None
    monkeypatch.setattr(service, "record_purge_veto", record_purge_veto)
    monkeypatch.setattr(service, "purge_was_vetoed", purge_was_vetoed)
    monkeypatch.setattr(service, "connection_pool", pool)
    monkeypatch.setattr(service, "write_admission", WriteAdmission(2))
    monkeypatch.setattr(service, "extraction_latch", latch)
    monkeypatch.setattr(service, "seen_mode", SeenMode.SEEN)
    monkeypatch.setattr(service, "refresh_derived_relations", AsyncMock())

    message = MagicMock()
    message.body = dumps(
        {
            "type": "extraction_complete",
            "version": version,
            "started_at": started_at.isoformat(),
            "record_counts": {TABLE: 100},
        }
    )
    message.headers = {}
    message.ack = AsyncMock()
    message.nack = AsyncMock()
    await service.on_data_message(message, TABLE)

    message.ack.assert_awaited_once()
    message.nack.assert_not_awaited()
    assert await _rows(dsn) == before
    assert await purge_was_vetoed(pool, latch, version, TABLE) is True
    opened_at, keys = await _list_state(dsn)
    assert opened_at > started_at
    assert keys == 0


async def _wal(conn: psycopg.AsyncConnection[Any]) -> Any:
    return await _value(conn, "SELECT pg_current_wal_lsn()")


async def _hint_fpi_bytes(conn: psycopg.AsyncConnection[Any], start: Any, end: Any) -> int | None:
    """Return the bytes of hint-bit full page images between START and END, if pg_walinspect is there."""
    try:
        async with conn.transaction():
            await conn.execute("CREATE EXTENSION IF NOT EXISTS pg_walinspect")
    except psycopg.Error:
        return None
    return int(
        await _value(
            conn,
            'SELECT coalesce(sum(combined_size), 0) FROM pg_get_wal_stats(%s, %s, true) WHERE "resource_manager/record_type" = %s',
            (start, end, "XLOG/FPI_FOR_HINT"),
        )
    )


async def _measure_reload(dsn: str, mode: SeenMode, records: Sequence[Record]) -> dict[str, Any]:
    """Run one unchanged reload after a CHECKPOINT and return its WAL and rewritten tuples."""
    await _begin_extraction(dsn)
    pool = await _pool(dsn, max_connections=1)
    try:
        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
            ctids = dict(await (await conn.execute(sql.SQL("SELECT data_id, ctid::text FROM {}").format(sql.Identifier(TABLE)))).fetchall())
            # Rows a month old have long been vacuumed on lux. An autoanalyze still running from
            # the load holds the xmin horizon back and keeps VACUUM from finishing; wait for it.
            for _attempt in range(40):
                if not await _value(conn, "SELECT count(*) FROM pg_stat_activity WHERE backend_xmin IS NOT NULL AND pid <> pg_backend_pid()"):
                    break
                await asyncio.sleep(0.25)
            await conn.execute(sql.SQL("VACUUM {}").format(sql.Identifier(TABLE)))
            await conn.execute("CHECKPOINT")
            await conn.execute("SELECT pg_stat_reset_single_table_counters(%s::regclass)", (TABLE,))
            start = await _wal(conn)
            await _write(pool, mode, records, batch_size=100)
            end = await _wal(conn)
            wal = int(await _value(conn, "SELECT pg_wal_lsn_diff(%s, %s)", (end, start)))
            hint_fpi = await _hint_fpi_bytes(conn, start, end)
            after = dict(await (await conn.execute(sql.SQL("SELECT data_id, ctid::text FROM {}").format(sql.Identifier(TABLE)))).fetchall())
            async with pool.connection() as writer:
                await writer.execute("SELECT pg_stat_force_next_flush()")
            updates = dead = 0
            for _attempt in range(20):
                await conn.execute("SELECT pg_stat_clear_snapshot()")
                row = await (
                    await conn.execute("SELECT n_tup_upd, n_dead_tup FROM pg_stat_user_tables WHERE relid = %s::regclass", (TABLE,))
                ).fetchone()
                assert row is not None
                updates, dead = int(row[0]), int(row[1])
                if mode is SeenMode.SEEN or updates >= len(records):
                    break
                await asyncio.sleep(0.25)
    finally:
        await pool.close()
    rewritten = sum(1 for data_id, ctid in after.items() if ctids.get(data_id) != ctid)
    return {
        "mode": mode.value,
        "wal_range": [str(start), str(end)],
        "rows": len(records),
        "wal_bytes": wal,
        "wal_per_row": round(wal / len(records), 1),
        # Full page images logged only to set a hint bit (data checksums are on by default in
        # PostgreSQL 18). They follow the pages' hint state, not the write path, and land on
        # either mode alike; on lux month-old pages were hinted long ago. None if unmeasured.
        "hint_fpi_bytes": hint_fpi,
        "wal_without_hint_fpi_per_row": round((wal - (hint_fpi or 0)) / len(records), 1),
        "rewritten_rows": rewritten,
        "n_tup_upd": updates,
        "n_dead_tup": dead,
    }


async def test_an_unchanged_reload_writes_almost_nothing_after_the_switch(dsn: str, pool: AsyncPostgreSQLPool) -> None:
    """WAL and dead tuples of the same unchanged reload, touch (before) against seen (after)."""
    records = [_release(data_id, padding=1500) for data_id in _ids(1, 2000)]
    await _write(pool, SeenMode.TOUCH, records, batch_size=100)

    touch = await _measure_reload(dsn, SeenMode.TOUCH, records)
    seen = await _measure_reload(dsn, SeenMode.SEEN, records)
    print(json.dumps({"seen_list_wal": [touch, seen]}))

    assert touch["rewritten_rows"] == len(records)
    assert seen["rewritten_rows"] == 0
    assert seen["n_tup_upd"] == 0
    if seen["hint_fpi_bytes"] is None:
        pytest.skip("pg_walinspect is unavailable, so hint-bit page images cannot be told apart")
    assert seen["wal_without_hint_fpi_per_row"] < touch["wal_without_hint_fpi_per_row"] / 100


# Last in the file, after the WAL measurement: crash recovery leaves this cluster logging full
# page images for hint bits on the next reload's pages for a while, which would charge the seen
# reload WAL a long-running lux does not pay. pytest runs a file's tests in order.
async def test_a_list_lost_to_a_real_crash_purges_nothing(dsn: str) -> None:
    pool = await _pool(dsn)
    try:
        async with pool.connection() as conn:
            if not await _value(conn, "SELECT rolsuper FROM pg_roles WHERE rolname = current_user"):
                pytest.skip("crashing the server needs a superuser")
        started_at, _gone = await _reload_with_every_path(dsn, pool, SeenMode.SEEN)
        before = await _rows(dsn)
    finally:
        await pool.close()

    # SIGKILL one backend: the postmaster treats it as a crash and runs crash recovery, which
    # resets every unlogged relation, the lists and their registry, to empty.
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as killer:
        victim = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
        pid = await _value(victim, "SELECT pg_backend_pid()")
        with contextlib.suppress(psycopg.Error):
            await killer.execute(sql.SQL("COPY (SELECT 1) TO PROGRAM {}").format(sql.Literal(f"kill -9 {pid}")))
    with contextlib.suppress(psycopg.Error):
        await victim.close()
    for _attempt in range(60):
        try:
            async with await psycopg.AsyncConnection.connect(dsn) as conn:
                await conn.execute("SELECT 1")
            break
        except psycopg.OperationalError:
            await asyncio.sleep(0.5)

    assert await _rows(dsn) == before
    assert await _list_state(dsn) == (None, 0)

    pool = await _pool(dsn)
    try:
        persistence = _persistence(pool, SeenMode.SEEN)
        veto = await persistence.purge_stale_rows(TABLE, started_at.isoformat(), 111)
        assert veto is not None
        assert "not intact" in veto
        # Every row survives: the 19 gone ones included, because nothing proves they are gone.
        assert await _rows(dsn) == before
        # The caller restarts the list after recording the veto, re-arming the next extraction.
        await persistence.start_seen_list(TABLE, started_at.isoformat())
        async with pool.connection() as conn:
            assert await _value(conn, "SELECT loader_seen.intact('discogs', %s, %s)", (TABLE, started_at)) is False
            assert await _value(conn, "SELECT loader_seen.intact('discogs', %s, now() + interval '16 minutes')", (TABLE,)) is True
    finally:
        await pool.close()
