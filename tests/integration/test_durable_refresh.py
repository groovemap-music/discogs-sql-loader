"""Real PostgreSQL crash and concurrency gates for the durable refresh handoff."""

import asyncio
import os
from typing import Any
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
import pytest_asyncio
from common import AsyncPostgreSQLPool

import tableinator.durable_refresh as durable


pytestmark = pytest.mark.integration
TYPES = {"artists", "labels", "masters", "releases"}


@pytest_asyncio.fixture
async def durable_pool(monkeypatch: pytest.MonkeyPatch) -> Any:
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("TEST_DATABASE_URL is required for PostgreSQL integration tests")
    discriminator = f"discogs-test-{uuid4().hex}"
    monkeypatch.setattr(durable, "LOADER_DISCRIMINATOR", discriminator)
    pool = AsyncPostgreSQLPool(
        connection_params={"conninfo": database_url},
        min_connections=1,
        max_connections=4,
        max_retries=1,
        health_check_interval=3600,
    )
    await pool.initialize()
    try:
        yield pool
    finally:
        async with pool.connection() as conn:
            await conn.set_autocommit(False)
            async with conn.transaction(), conn.cursor() as cursor:
                await cursor.execute("DELETE FROM public.loader_derived_refresh_job WHERE loader = %s", (discriminator,))
                await cursor.execute("DELETE FROM public.loader_extraction_latch WHERE loader = %s", (discriminator,))
                await cursor.execute("DELETE FROM public.loader_derived_refresh_cursor WHERE loader = %s", (discriminator,))
        await pool.close()


async def _rows(pool: Any, statement: str, parameters: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
    async with pool.connection() as conn, conn.cursor() as cursor:
        await cursor.execute(statement, parameters)
        return list(await cursor.fetchall())


async def _complete_signals(pool: Any, version: str) -> None:
    for data_type in sorted(TYPES):
        await durable.record_durable_signal(pool, version, data_type, TYPES)


@pytest.mark.asyncio
async def test_promoted_schema_and_commit_before_ack_replay_and_source_order(durable_pool: Any) -> None:
    logger = MagicMock()
    assert await durable.probe_durable_schema(durable_pool, logger), logger.error.call_args_list
    for index, data_type in enumerate(sorted(TYPES)):
        result = await durable.record_durable_signal(durable_pool, "20990102", data_type, TYPES)
        if index == 0:
            assert (await durable.read_refresh_health(durable_pool))["phase"] == "waiting_for_signals"
    assert result.scheduled and result.generation == 1
    # A crash after PostgreSQL commit but before RabbitMQ ack redelivers the
    # terminal signal. It neither creates a second job nor advances generation.
    replay = await durable.record_durable_signal(durable_pool, "20990102", "releases", TYPES)
    assert replay.scheduled and replay.generation == 1
    # The older producer dump arrives later at this consumer; arrival order is
    # expressly not the source extraction order.
    old = await durable.record_durable_signal(durable_pool, "20990101", "artists", TYPES)
    assert old.superseded and old.generation is None
    assert await _rows(
        durable_pool,
        "SELECT generation, version FROM public.loader_derived_refresh_cursor WHERE loader = %s",
        (durable.LOADER_DISCRIMINATOR,),
    ) == [(1, "20990102")]
    assert await _rows(
        durable_pool,
        "SELECT version, state FROM public.loader_derived_refresh_job WHERE loader = %s",
        (durable.LOADER_DISCRIMINATOR,),
    ) == [("20990102", "pending")]


@pytest.mark.asyncio
async def test_startup_reconciles_complete_legacy_latch_without_new_message(durable_pool: Any) -> None:
    async with durable_pool.connection() as conn:
        await conn.set_autocommit(False)
        async with conn.transaction(), conn.cursor() as cursor:
            await cursor.execute(
                "INSERT INTO public.loader_extraction_latch (loader, version, signals) VALUES (%s, %s, %s)",
                (durable.LOADER_DISCRIMINATOR, "20990201", sorted(TYPES)),
            )
    await durable.reconcile_legacy_latches(durable_pool, TYPES)
    assert await _rows(
        durable_pool,
        "SELECT generation FROM public.loader_extraction_latch WHERE loader = %s AND version = '20990201'",
        (durable.LOADER_DISCRIMINATOR,),
    ) == [(1,)]
    assert await _rows(
        durable_pool,
        "SELECT state FROM public.loader_derived_refresh_job WHERE loader = %s",
        (durable.LOADER_DISCRIMINATOR,),
    ) == [("pending",)]
    assert await durable.claim_due_job(durable_pool, "restart-worker") is not None


@pytest.mark.asyncio
async def test_precommit_failure_rolls_back_fourth_signal_and_job(durable_pool: Any) -> None:
    for data_type in ("artists", "labels", "masters"):
        await durable.record_durable_signal(durable_pool, "20990202", data_type, TYPES)
    with patch.object(durable, "_upsert_job", side_effect=RuntimeError("injected precommit crash")), pytest.raises(RuntimeError, match="precommit"):
        await durable.record_durable_signal(durable_pool, "20990202", "releases", TYPES)
    assert await _rows(
        durable_pool,
        "SELECT signals FROM public.loader_extraction_latch WHERE loader = %s AND version = '20990202'",
        (durable.LOADER_DISCRIMINATOR,),
    ) == [(["artists", "labels", "masters"],)]
    assert (
        await _rows(
            durable_pool,
            "SELECT version FROM public.loader_derived_refresh_job WHERE loader = %s",
            (durable.LOADER_DISCRIMINATOR,),
        )
        == []
    )
    # RabbitMQ redelivery after nack is idempotent and completes the obligation.
    await durable.record_durable_signal(durable_pool, "20990202", "releases", TYPES)
    assert await durable.claim_due_job(durable_pool, "recovered-worker") is not None


@pytest.mark.asyncio
async def test_legacy_fallback_key_is_ignored_only_when_already_refreshed(durable_pool: Any) -> None:
    fallback = "2026-01-01T00:00:00Z"
    async with durable_pool.connection() as conn:
        await conn.set_autocommit(False)
        async with conn.transaction(), conn.cursor() as cursor:
            await cursor.execute(
                "INSERT INTO public.loader_extraction_latch (loader, version, signals, refreshed_at) VALUES (%s, %s, %s, NOW())",
                (durable.LOADER_DISCRIMINATOR, fallback, sorted(TYPES)),
            )
    await durable.reconcile_legacy_latches(durable_pool, TYPES)
    assert (
        await _rows(
            durable_pool,
            "SELECT version FROM public.loader_derived_refresh_job WHERE loader = %s",
            (durable.LOADER_DISCRIMINATOR,),
        )
        == []
    )
    async with durable_pool.connection() as conn:
        await conn.set_autocommit(False)
        async with conn.transaction(), conn.cursor() as cursor:
            await cursor.execute(
                "UPDATE public.loader_extraction_latch SET refreshed_at = NULL WHERE loader = %s AND version = %s",
                (durable.LOADER_DISCRIMINATOR, fallback),
            )
    with pytest.raises(ValueError, match="YYYYMMDD"):
        await durable.reconcile_legacy_latches(durable_pool, TYPES)


@pytest.mark.asyncio
async def test_two_workers_and_expired_lease_fence_the_old_claimant(durable_pool: Any) -> None:
    await _complete_signals(durable_pool, "20990301")
    claims = await asyncio.gather(
        durable.claim_due_job(durable_pool, "worker-a"),
        durable.claim_due_job(durable_pool, "worker-b"),
    )
    first = next(claim for claim in claims if claim is not None)
    assert sum(claim is not None for claim in claims) == 1
    async with durable_pool.connection() as conn:
        await conn.set_autocommit(False)
        async with conn.transaction(), conn.cursor() as cursor:
            await cursor.execute(
                "UPDATE public.loader_derived_refresh_job SET lease_expires_at = NOW() - INTERVAL '1 second' "
                "WHERE loader = %s AND version = '20990301'",
                (durable.LOADER_DISCRIMINATOR,),
            )
    logger = MagicMock()
    assert await durable.claim_due_job(durable_pool, "worker-replacement", logger) is None
    logger.error.assert_called_once()
    assert await _rows(
        durable_pool,
        "SELECT state, last_error FROM public.loader_derived_refresh_job WHERE loader = %s",
        (durable.LOADER_DISCRIMINATOR,),
    ) == [("retry", "lease_expired")]
    async with durable_pool.connection() as conn:
        await conn.set_autocommit(False)
        async with conn.transaction(), conn.cursor() as cursor:
            await cursor.execute(
                "UPDATE public.loader_derived_refresh_job SET next_attempt_at = NOW() - INTERVAL '1 second' "
                "WHERE loader = %s AND version = '20990301'",
                (durable.LOADER_DISCRIMINATOR,),
            )
    replacement = await durable.claim_due_job(durable_pool, "worker-replacement")
    assert replacement is not None
    assert replacement.epoch == first.epoch + 1 and replacement.token != first.token
    async with durable_pool.connection() as conn:
        await conn.set_autocommit(False)
        with pytest.raises(durable.SupersededJob):
            async with conn.transaction(), conn.cursor() as cursor:
                await durable._before_commit(cursor, first)


@pytest.mark.asyncio
async def test_new_extraction_during_old_refresh_rolls_back_old_stamp(durable_pool: Any) -> None:
    await _complete_signals(durable_pool, "20990401")
    claim = await durable.claim_due_job(durable_pool, "slow-worker")
    assert claim is not None

    async def advance_then_fence(cursor: Any) -> None:
        await durable.record_durable_signal(durable_pool, "20990402", "artists", TYPES)
        await durable._before_commit(cursor, claim)

    with pytest.raises(durable.SupersededJob):
        await durable.refresh_derived_relations(
            durable_pool,
            MagicMock(),
            claim.version,
            before_refresh=lambda cursor: durable._before_refresh(cursor, claim),
            before_commit=advance_then_fence,
        )
    assert await _rows(
        durable_pool,
        "SELECT refreshed_at FROM public.loader_extraction_latch WHERE loader = %s AND version = '20990401'",
        (durable.LOADER_DISCRIMINATOR,),
    ) == [(None,)]
    assert await _rows(
        durable_pool,
        "SELECT state FROM public.loader_derived_refresh_job WHERE loader = %s AND version = '20990401'",
        (durable.LOADER_DISCRIMINATOR,),
    ) == [("superseded",)]


@pytest.mark.asyncio
async def test_worker_completes_from_scanned_job_without_delivery(durable_pool: Any) -> None:
    await _complete_signals(durable_pool, "20990501")
    assert await durable.run_worker_once(durable_pool, MagicMock(), "worker-after-restart")
    assert await _rows(
        durable_pool,
        "SELECT job.state, latch.refreshed_at IS NOT NULL FROM public.loader_derived_refresh_job AS job "
        "JOIN public.loader_extraction_latch AS latch USING (loader, version, generation) "
        "WHERE job.loader = %s",
        (durable.LOADER_DISCRIMINATOR,),
    ) == [("completed", True)]
    health = await durable.read_refresh_health(durable_pool)
    assert health["newest_completed_version"] == "20990501"
    assert health["phase"] is None and health["status"] == "enabled"


@pytest.mark.asyncio
async def test_midrefresh_and_postrefresh_prestamp_crashes_keep_job_recoverable(durable_pool: Any) -> None:
    await _complete_signals(durable_pool, "20990601")
    claim = await durable.claim_due_job(durable_pool, "crashing-worker")
    assert claim is not None

    async def crash_mid(_cursor: Any, _logger: Any) -> Any:
        raise RuntimeError("injected midrefresh crash")

    with patch("tableinator.graph_counters.refresh_counter_relations", new=crash_mid), pytest.raises(RuntimeError, match="midrefresh"):
        await durable.refresh_derived_relations(
            durable_pool,
            MagicMock(),
            claim.version,
            before_refresh=lambda cursor: durable._before_refresh(cursor, claim),
            before_commit=lambda cursor: durable._before_commit(cursor, claim),
        )

    async def crash_after_relations(_cursor: Any) -> None:
        raise RuntimeError("injected postrefresh prestamp crash")

    with pytest.raises(RuntimeError, match="postrefresh"):
        await durable.refresh_derived_relations(
            durable_pool,
            MagicMock(),
            claim.version,
            before_refresh=lambda cursor: durable._before_refresh(cursor, claim),
            before_commit=crash_after_relations,
        )
    assert await _rows(
        durable_pool,
        "SELECT latch.refreshed_at, job.state FROM public.loader_extraction_latch AS latch "
        "JOIN public.loader_derived_refresh_job AS job USING (loader, version, generation) "
        "WHERE latch.loader = %s",
        (durable.LOADER_DISCRIMINATOR,),
    ) == [(None, "leased")]
    await durable._mark_after_failure(durable_pool, claim, error="injected_crash")
    health = await durable.read_refresh_health(durable_pool)
    assert health["status"] == "degraded" and health["last_sanitized_failure"] == "injected_crash"


@pytest.mark.asyncio
async def test_simulated_31_minute_worker_pass_renews_lease_outside_ack_budget(durable_pool: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    await _complete_signals(durable_pool, "20990701")
    monkeypatch.setattr(durable, "_HEARTBEAT_SECONDS", 0.001)
    simulated_elapsed = 0

    async def accelerated_pass(_pool: Any, _logger: Any, _version: str, **hooks: Any) -> dict[str, int]:
        nonlocal simulated_elapsed
        async with durable_pool.connection() as conn:
            await conn.set_autocommit(False)
            async with conn.transaction(), conn.cursor() as cursor:
                await hooks["before_refresh"](cursor)
                initial_expiry = await _rows(
                    durable_pool,
                    "SELECT lease_expires_at FROM public.loader_derived_refresh_job WHERE loader = %s",
                    (durable.LOADER_DISCRIMINATOR,),
                )
                for _ in range(31):
                    simulated_elapsed += 60
                    await asyncio.sleep(0.003)
                renewed_expiry = await _rows(
                    durable_pool,
                    "SELECT lease_expires_at FROM public.loader_derived_refresh_job WHERE loader = %s",
                    (durable.LOADER_DISCRIMINATOR,),
                )
                assert renewed_expiry[0][0] > initial_expiry[0][0]
                await hooks["before_commit"](cursor)
        return {}

    with patch.object(durable, "refresh_derived_relations", new=accelerated_pass):
        assert await durable.run_worker_once(durable_pool, MagicMock(), "long-worker")
    assert simulated_elapsed == 1860  # greater than RabbitMQ's 1,800-second ack limit
    assert await _rows(
        durable_pool,
        "SELECT state, attempt_count FROM public.loader_derived_refresh_job WHERE loader = %s",
        (durable.LOADER_DISCRIMINATOR,),
    ) == [("completed", 1)]
