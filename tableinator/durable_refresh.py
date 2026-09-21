"""Durable, fenced scheduling for the Discogs derived-relation refresh.

The database-schema pin owns every object used here. A terminal delivery is safe to
acknowledge only after its signal and (when complete) job commit together. A process
restart therefore loses no refresh obligation. The scanner and worker deliberately do
not depend on a notification from the delivery handler.
"""

from __future__ import annotations

import asyncio
import contextlib
import re
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any, Final
from uuid import UUID, uuid4

from tableinator import telemetry
from tableinator.extraction_latch import LOADER_DISCRIMINATOR
from tableinator.graph_counters import refresh_derived_relations


if TYPE_CHECKING:
    from collections.abc import Collection


CURSOR: Final = "public.loader_derived_refresh_cursor"
JOB: Final = "public.loader_derived_refresh_job"
LATCH: Final = "public.loader_extraction_latch"
_VERSION_RE: Final = re.compile(r"^[0-9]{8}$")
_LEASE_SECONDS: Final = 120
_HEARTBEAT_SECONDS: Final = 30
_SCAN_SECONDS: Final = 15
_STALE_SECONDS: Final = 300
_STALE_SIGNALS_SECONDS: Final = 86_400
_MAX_BACKOFF_SECONDS: Final = 300
_ADVISORY_KEY: Final = "groovemap:derived-refresh:discogs"


class DurableSchemaUnavailable(RuntimeError):
    """The configured durable path cannot safely acknowledge a delivery."""


class SupersededJob(RuntimeError):
    """A newer source extraction or claimant fenced this worker."""


@dataclass(frozen=True)
class DurableSignal:
    version: str
    generation: int | None
    signals: frozenset[str]
    scheduled: bool
    superseded: bool
    already_refreshed: bool


@dataclass(frozen=True)
class ClaimedJob:
    version: str
    generation: int
    token: UUID
    epoch: int
    attempt_count: int


def ordered_version(value: Any) -> str:
    """Require the producer's ordered YYYYMMDD dump version, never arrival order.

    `started_at` denotes an ingestion attempt, not the ordering of the source dumps.
    Its former fallback could distinguish versionless latch rows but cannot safely
    advance a monotonic extraction cursor, so the durable path refuses it.
    """
    version = str(value or "").strip()
    if not _VERSION_RE.fullmatch(version):
        raise ValueError("durable extraction_complete requires an eight-digit YYYYMMDD dump version")
    try:
        date.fromisoformat(f"{version[:4]}-{version[4:6]}-{version[6:]}")
    except ValueError as exc:
        raise ValueError("durable extraction_complete has an invalid YYYYMMDD dump version") from exc
    return version


async def probe_durable_schema(connection_pool: Any, logger: Any) -> bool:
    """Fail closed unless every job/lease/fence column and key is declared."""
    required = {
        "loader_extraction_latch": {
            "loader": "text",
            "version": "text",
            "signals": "ARRAY",
            "refreshed_at": "timestamp with time zone",
            "generation": "bigint",
        },
        "loader_derived_refresh_cursor": {"loader": "text", "generation": "bigint", "version": "text"},
        "loader_derived_refresh_job": {
            "loader": "text",
            "version": "text",
            "generation": "bigint",
            "state": "text",
            "attempt_count": "integer",
            "next_attempt_at": "timestamp with time zone",
            "lease_owner": "text",
            "lease_token": "uuid",
            "lease_epoch": "bigint",
            "lease_expires_at": "timestamp with time zone",
            "last_error": "text",
            "created_at": "timestamp with time zone",
            "updated_at": "timestamp with time zone",
            "started_at": "timestamp with time zone",
            "completed_at": "timestamp with time zone",
            "superseded_at": "timestamp with time zone",
        },
    }
    try:
        async with connection_pool.connection() as conn, conn.cursor() as cursor:
            await cursor.execute(
                "SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = ANY(%s)",
                (list(required),),
            )
            found: dict[str, dict[str, str]] = {name: {} for name in required}
            for table, column, data_type in await cursor.fetchall():
                found[str(table)][str(column)] = str(data_type)
            missing = {
                table: sorted(name for name, expected_type in columns.items() if found[table].get(name) != expected_type)
                for table, columns in required.items()
            }
            missing = {table: columns for table, columns in missing.items() if columns}
            if missing:
                logger.error("❌ Durable refresh schema is missing or incompatible", missing=missing)
                return False
            await cursor.execute(
                "SELECT ns.nspname || '.' || rel.relname, con.contype, "
                "array_agg(att.attname::text ORDER BY att.attname::text) "
                "FROM pg_constraint AS con "
                "JOIN pg_class AS rel ON rel.oid = con.conrelid "
                "JOIN pg_namespace AS ns ON ns.oid = rel.relnamespace "
                "JOIN LATERAL unnest(con.conkey) AS key(attnum) ON true "
                "JOIN pg_attribute AS att ON att.attrelid = con.conrelid AND att.attnum = key.attnum "
                "WHERE con.conrelid IN ('public.loader_extraction_latch'::regclass, "
                "'public.loader_derived_refresh_cursor'::regclass, 'public.loader_derived_refresh_job'::regclass) "
                "AND con.contype IN ('p', 'u', 'f') "
                "GROUP BY con.oid, ns.nspname, rel.relname, con.contype"
            )
            constraints = {(str(table), str(kind), tuple(columns)) for table, kind, columns in await cursor.fetchall()}
            expected = {
                (LATCH, "p", ("loader", "version")),
                (CURSOR, "p", ("loader",)),
                (JOB, "p", ("loader", "version")),
                (JOB, "u", ("generation", "loader")),
                (JOB, "f", ("generation", "loader", "version")),
            }
            if not expected.issubset(constraints):
                logger.error("❌ Durable refresh schema has incompatible keys", missing=sorted(expected - constraints))
                return False
            await cursor.execute("SELECT indexname FROM pg_indexes WHERE schemaname = 'public' AND tablename = 'loader_derived_refresh_job'")
            indexes = {str(row[0]) for row in await cursor.fetchall()}
            expected_indexes = {"idx_loader_derived_refresh_job_due", "idx_loader_derived_refresh_job_lease_expiry"}
            if not expected_indexes.issubset(indexes):
                logger.error("❌ Durable refresh schema is missing scheduler indexes", missing=sorted(expected_indexes - indexes))
                return False
            return True
    except Exception as exc:
        logger.error("❌ Could not verify durable refresh schema", error=type(exc).__name__)
        return False


async def _lock_cursor(cursor: Any) -> tuple[int, str | None]:
    await cursor.execute(
        f"INSERT INTO {CURSOR} (loader) VALUES (%s) ON CONFLICT (loader) DO NOTHING",  # noqa: S608
        (LOADER_DISCRIMINATOR,),
    )
    await cursor.execute(
        f"SELECT generation, version FROM {CURSOR} WHERE loader = %s FOR UPDATE",  # noqa: S608
        (LOADER_DISCRIMINATOR,),
    )
    row = await cursor.fetchone()
    if row is None:
        raise DurableSchemaUnavailable("durable cursor row could not be locked")
    return int(row[0]), str(row[1]) if row[1] is not None else None


async def _advance_cursor(cursor: Any, generation: int, version: str) -> None:
    await cursor.execute(
        f"UPDATE {CURSOR} SET generation = %s, version = %s, updated_at = NOW() WHERE loader = %s",  # noqa: S608
        (generation, version, LOADER_DISCRIMINATOR),
    )
    await cursor.execute(
        f"UPDATE {JOB} SET state = 'superseded', next_attempt_at = NULL, lease_owner = NULL, "  # noqa: S608
        "lease_token = NULL, lease_expires_at = NULL, superseded_at = NOW(), updated_at = NOW() "
        "WHERE loader = %s AND generation < %s AND state IN ('pending', 'retry', 'leased')",
        (LOADER_DISCRIMINATOR, generation),
    )


async def _upsert_job(cursor: Any, version: str, generation: int) -> bool:
    await cursor.execute(
        f"INSERT INTO {JOB} (loader, version, generation) VALUES (%s, %s, %s) "  # noqa: S608
        "ON CONFLICT (loader, version) DO NOTHING RETURNING version",
        (LOADER_DISCRIMINATOR, version, generation),
    )
    return await cursor.fetchone() is not None


async def _reconcile_legacy(cursor: Any, required_types: Collection[str]) -> tuple[int, str | None]:
    """Make pre-cutover latches durable before any new-protocol terminal ack.

    Older already-superseded legacy rows cannot publish. Complete current/newer rows
    become jobs; partial rows receive generations so their later signals can finish.
    A malformed legacy version makes ordering unknowable and fails the transaction.
    """
    generation, current = await _lock_cursor(cursor)
    await cursor.execute(
        f"SELECT version, signals, refreshed_at FROM {LATCH} "  # noqa: S608
        "WHERE loader = %s AND generation IS NULL ORDER BY version FOR UPDATE",
        (LOADER_DISCRIMINATOR,),
    )
    rows = await cursor.fetchall()
    for raw_version, raw_signals, refreshed_at in rows:
        # The historical inline writer accepted started_at as a fallback key.
        # A completed row owes no work and may keep that non-orderable legacy
        # key; an unfinished one cannot safely be placed after a source dump.
        if refreshed_at is not None and not _VERSION_RE.fullmatch(str(raw_version)):
            continue
        version = ordered_version(raw_version)
        if current is not None and version < current:
            # A newer accepted extraction already fences this old latch. It cannot
            # be assigned a new generation without reversing source order.
            continue
        if current is None or version > current:
            generation += 1
            current = version
            await _advance_cursor(cursor, generation, version)
        await cursor.execute(
            f"UPDATE {LATCH} SET generation = %s, updated_at = NOW() "  # noqa: S608
            "WHERE loader = %s AND version = %s AND generation IS NULL",
            (generation, LOADER_DISCRIMINATOR, version),
        )
        if refreshed_at is None and set(raw_signals or ()).issuperset(required_types):
            await _upsert_job(cursor, version, generation)
    return generation, current


async def reconcile_legacy_latches(connection_pool: Any, required_types: Collection[str]) -> None:
    """Startup recovery also runs without any incoming RabbitMQ message."""
    async with connection_pool.connection() as conn:
        await conn.set_autocommit(False)
        async with conn.transaction(), conn.cursor() as cursor:
            await _reconcile_legacy(cursor, required_types)


async def record_durable_signal(connection_pool: Any, version_value: Any, data_type: str, required_types: Collection[str]) -> DurableSignal:
    """Commit signal plus pending job before the caller can ack the delivery."""
    version = ordered_version(version_value)
    if data_type not in required_types:
        raise ValueError(f"unrecognized extraction type: {data_type}")
    async with connection_pool.connection() as conn:
        await conn.set_autocommit(False)
        async with conn.transaction(), conn.cursor() as cursor:
            generation, current = await _reconcile_legacy(cursor, required_types)
            superseded = current is not None and version < current
            if current is None or version > current:
                generation += 1
                await _advance_cursor(cursor, generation, version)
            accepted_generation = None if superseded else generation
            await cursor.execute(
                f"INSERT INTO {LATCH} AS latch (loader, version, generation, signals) "  # noqa: S608
                "VALUES (%s, %s, %s, ARRAY[%s]::text[]) "
                "ON CONFLICT (loader, version) DO UPDATE SET "
                "generation = COALESCE(latch.generation, EXCLUDED.generation), "
                "signals = (SELECT array_agg(DISTINCT signal ORDER BY signal) "
                "FROM unnest(latch.signals || EXCLUDED.signals) AS signal), updated_at = NOW() "
                "RETURNING signals, refreshed_at, generation",
                (LOADER_DISCRIMINATOR, version, accepted_generation, data_type),
            )
            row = await cursor.fetchone()
            if row is None:
                raise DurableSchemaUnavailable("durable latch upsert returned no row")
            signals = frozenset(str(signal) for signal in row[0] or ())
            already_refreshed = row[1] is not None
            row_generation = int(row[2]) if row[2] is not None else None
            scheduled = False
            if not superseded and not already_refreshed and signals.issuperset(required_types):
                if row_generation != generation:
                    raise DurableSchemaUnavailable("latch generation disagrees with the cursor")
                await _upsert_job(cursor, version, generation)
                scheduled = True  # Existing pending/leased/retry job is also durable.
    return DurableSignal(version, row_generation, signals, scheduled, superseded, already_refreshed)


async def claim_due_job(connection_pool: Any, owner: str, logger: Any | None = None) -> ClaimedJob | None:
    """Serialize claims per loader, including reclaim of an expired lease."""
    async with connection_pool.connection() as conn:
        await conn.set_autocommit(False)
        async with conn.transaction(), conn.cursor() as cursor:
            current_generation, _ = await _lock_cursor(cursor)
            await cursor.execute(
                f"SELECT EXISTS (SELECT 1 FROM {JOB} WHERE loader = %s AND state = 'leased' "  # noqa: S608
                "AND lease_expires_at > NOW())",
                (LOADER_DISCRIMINATOR,),
            )
            active = await cursor.fetchone()
            if active and active[0]:
                return None
            await cursor.execute(
                f"SELECT version, generation, lease_epoch, attempt_count, state FROM {JOB} "  # noqa: S608
                "WHERE loader = %s AND ((state IN ('pending', 'retry') AND next_attempt_at <= NOW()) "
                "OR (state = 'leased' AND lease_expires_at <= NOW())) "
                "ORDER BY generation DESC, next_attempt_at NULLS LAST FOR UPDATE SKIP LOCKED LIMIT 1",
                (LOADER_DISCRIMINATOR,),
            )
            row = await cursor.fetchone()
            if row is None:
                return None
            version, generation, epoch, attempts, state = str(row[0]), int(row[1]), int(row[2]), int(row[3]), str(row[4])
            if generation != current_generation:
                await cursor.execute(
                    f"UPDATE {JOB} SET state = 'superseded', next_attempt_at = NULL, "  # noqa: S608
                    "lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL, "
                    "superseded_at = NOW(), updated_at = NOW() WHERE loader = %s AND version = %s",
                    (LOADER_DISCRIMINATOR, version),
                )
                return None
            if state == "leased":
                # An expired claimant may still be alive. Fence it by clearing
                # its token, then put a bounded delay between attempts instead
                # of hot-looping through a broker-independent failure.
                delay = min(_MAX_BACKOFF_SECONDS, 2 ** min(attempts, 8))
                await cursor.execute(
                    f"UPDATE {JOB} SET state = 'retry', next_attempt_at = NOW() + (%s * INTERVAL '1 second'), "  # noqa: S608
                    "lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL, "
                    "last_error = 'lease_expired', updated_at = NOW() "
                    "WHERE loader = %s AND version = %s",
                    (delay, LOADER_DISCRIMINATOR, version),
                )
                if logger is not None:
                    logger.error("❌ Derived refresh lease expired; bounded retry scheduled", version=version, delay_seconds=delay)
                return None
            token = uuid4()
            await cursor.execute(
                f"UPDATE {JOB} SET state = 'leased', next_attempt_at = NULL, "  # noqa: S608
                "lease_owner = %s, lease_token = %s, lease_epoch = lease_epoch + 1, "
                "lease_expires_at = NOW() + (%s * INTERVAL '1 second'), "
                "attempt_count = attempt_count + 1, started_at = COALESCE(started_at, NOW()), "
                "updated_at = NOW() WHERE loader = %s AND version = %s",
                (owner, token, _LEASE_SECONDS, LOADER_DISCRIMINATOR, version),
            )
            return ClaimedJob(version, generation, token, epoch + 1, attempts + 1)


async def _owns_lease(cursor: Any, job: ClaimedJob, *, lock: bool) -> bool:
    await cursor.execute(
        f"SELECT state, lease_token, lease_epoch, lease_expires_at FROM {JOB} "  # noqa: S608
        f"WHERE loader = %s AND version = %s{' FOR UPDATE' if lock else ''}",
        (LOADER_DISCRIMINATOR, job.version),
    )
    row = await cursor.fetchone()
    return bool(
        row is not None
        and row[0] == "leased"
        and row[1] == job.token
        and int(row[2]) == job.epoch
        and row[3] is not None
        and row[3] > datetime.now(UTC)
    )


async def _before_refresh(cursor: Any, job: ClaimedJob) -> None:
    # Transaction-level lock spans the entire expensive pass, not merely claim.
    # A second process can reclaim an expired lease but cannot run a second pass
    # concurrently; its final token is checked again before any commit.
    await cursor.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (_ADVISORY_KEY,))
    await cursor.execute(
        f"SELECT generation, version FROM {CURSOR} WHERE loader = %s",  # noqa: S608
        (LOADER_DISCRIMINATOR,),
    )
    current = await cursor.fetchone()
    if current != (job.generation, job.version) or not await _owns_lease(cursor, job, lock=False):
        raise SupersededJob(f"job {job.version} lost generation or lease before refresh")


async def _before_commit(cursor: Any, job: ClaimedJob) -> None:
    # Lock in the same cursor→job order used by the producer. A producer can
    # advance generation during the long pass, but not between this comparison
    # and the commit. A stolen/expired lease also cannot publish.
    await cursor.execute(
        f"SELECT generation, version FROM {CURSOR} WHERE loader = %s FOR UPDATE",  # noqa: S608
        (LOADER_DISCRIMINATOR,),
    )
    current = await cursor.fetchone()
    if current != (job.generation, job.version) or not await _owns_lease(cursor, job, lock=True):
        raise SupersededJob(f"job {job.version} lost generation or lease at commit")
    await cursor.execute(
        f"UPDATE {LATCH} SET refreshed_at = NOW(), updated_at = NOW() "  # noqa: S608
        "WHERE loader = %s AND version = %s AND generation = %s AND refreshed_at IS NULL",
        (LOADER_DISCRIMINATOR, job.version, job.generation),
    )
    if cursor.rowcount != 1:
        raise SupersededJob(f"job {job.version} has no unstamped latch at commit")
    await cursor.execute(
        f"UPDATE {JOB} SET state = 'completed', lease_owner = NULL, lease_token = NULL, "  # noqa: S608
        "lease_expires_at = NULL, completed_at = NOW(), updated_at = NOW() "
        "WHERE loader = %s AND version = %s AND generation = %s AND lease_token = %s AND lease_epoch = %s",
        (LOADER_DISCRIMINATOR, job.version, job.generation, job.token, job.epoch),
    )
    if cursor.rowcount != 1:
        raise SupersededJob(f"job {job.version} lost its claim at completion")


async def _heartbeat(connection_pool: Any, job: ClaimedJob, lost: asyncio.Event) -> None:
    """Renew a real database lease while a full-dump pass exceeds 1,800 seconds."""
    while True:
        await asyncio.sleep(_HEARTBEAT_SECONDS)
        try:
            async with connection_pool.connection() as conn:
                await conn.set_autocommit(False)
                async with conn.transaction(), conn.cursor() as cursor:
                    await cursor.execute(
                        f"UPDATE {JOB} SET lease_expires_at = NOW() + (%s * INTERVAL '1 second'), "  # noqa: S608
                        "updated_at = NOW() WHERE loader = %s AND version = %s "
                        "AND state = 'leased' AND lease_token = %s AND lease_epoch = %s "
                        "AND lease_expires_at > NOW()",
                        (_LEASE_SECONDS, LOADER_DISCRIMINATOR, job.version, job.token, job.epoch),
                    )
                    if cursor.rowcount != 1:
                        lost.set()
                        return
        except Exception:
            # An outage is allowed to expire the lease; the commit fence rejects
            # it and the next scanner reclaims it. Never claim healthy ownership.
            lost.set()
            return


async def _mark_after_failure(connection_pool: Any, job: ClaimedJob, *, error: str) -> None:
    async with connection_pool.connection() as conn:
        await conn.set_autocommit(False)
        async with conn.transaction(), conn.cursor() as cursor:
            await cursor.execute(
                f"SELECT generation, version FROM {CURSOR} WHERE loader = %s FOR UPDATE",  # noqa: S608
                (LOADER_DISCRIMINATOR,),
            )
            current = await cursor.fetchone()
            obsolete = current != (job.generation, job.version)
            if obsolete:
                await cursor.execute(
                    f"UPDATE {JOB} SET state = 'superseded', next_attempt_at = NULL, "  # noqa: S608
                    "lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL, "
                    "superseded_at = NOW(), updated_at = NOW() "
                    "WHERE loader = %s AND version = %s AND state = 'leased' "
                    "AND lease_token = %s AND lease_epoch = %s",
                    (LOADER_DISCRIMINATOR, job.version, job.token, job.epoch),
                )
            else:
                delay = min(_MAX_BACKOFF_SECONDS, 2 ** min(job.attempt_count, 8))
                await cursor.execute(
                    f"UPDATE {JOB} SET state = 'retry', "  # noqa: S608
                    "next_attempt_at = NOW() + (%s * INTERVAL '1 second'), "
                    "lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL, "
                    "last_error = %s, updated_at = NOW() "
                    "WHERE loader = %s AND version = %s AND state = 'leased' "
                    "AND lease_token = %s AND lease_epoch = %s",
                    (delay, error[:1024], LOADER_DISCRIMINATOR, job.version, job.token, job.epoch),
                )


async def run_worker_once(connection_pool: Any, logger: Any, owner: str) -> bool:
    """Scan, claim, and finish one durable obligation; return whether one was claimed."""
    job = await claim_due_job(connection_pool, owner, logger)
    if job is None:
        return False
    started = time.perf_counter()
    lost = asyncio.Event()
    heartbeat = asyncio.create_task(_heartbeat(connection_pool, job, lost))

    async def start(cursor: Any) -> None:
        if lost.is_set():
            raise SupersededJob("lease heartbeat stopped")
        await _before_refresh(cursor, job)

    async def finish(cursor: Any) -> None:
        if lost.is_set():
            raise SupersededJob("lease heartbeat stopped")
        await _before_commit(cursor, job)

    try:
        await refresh_derived_relations(connection_pool, logger, job.version, before_refresh=start, before_commit=finish)
    except SupersededJob:
        await _mark_after_failure(connection_pool, job, error="lease_lost_or_superseded")
        telemetry.record_derived_refresh_transition("fenced", time.perf_counter() - started)
        logger.warning("⏭️ Derived refresh fenced as superseded", version=job.version)
    except Exception as exc:
        # Exception text can contain SQL literals or credentials. Persist the class
        # only; full diagnostic detail belongs in protected traces, not health.
        await _mark_after_failure(connection_pool, job, error=type(exc).__name__)
        telemetry.record_derived_refresh_transition("retry", time.perf_counter() - started)
        logger.error("❌ Durable derived refresh failed; retry scheduled", version=job.version, error_type=type(exc).__name__)
    else:
        telemetry.record_derived_refresh_transition("completed", time.perf_counter() - started)
    finally:
        heartbeat.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await heartbeat
    return True


async def read_refresh_health(connection_pool: Any) -> dict[str, Any]:
    """Database-backed health; an acked but stuck job never looks successful."""
    async with connection_pool.connection() as conn, conn.cursor() as cursor:
        await cursor.execute(
            f"SELECT generation, version, updated_at FROM {CURSOR} WHERE loader = %s",  # noqa: S608
            (LOADER_DISCRIMINATOR,),
        )
        current = await cursor.fetchone()
        await cursor.execute(
            f"SELECT version, state, attempt_count, next_attempt_at, lease_expires_at, "  # noqa: S608
            f"last_error, created_at, started_at, completed_at FROM {JOB} "
            "WHERE loader = %s ORDER BY generation DESC",
            (LOADER_DISCRIMINATOR,),
        )
        rows = await cursor.fetchall()
        legacy_superseded = 0
        if current is not None and current[1] is not None:
            await cursor.execute(
                f"SELECT count(*) FROM {LATCH} WHERE loader = %s AND generation IS NULL "  # noqa: S608
                "AND refreshed_at IS NULL AND version < %s",
                (LOADER_DISCRIMINATOR, current[1]),
            )
            count_row = await cursor.fetchone()
            legacy_superseded = int(count_row[0]) if count_row is not None else 0
    now = datetime.now(UTC)
    newest_completed = next((str(row[0]) for row in rows if row[1] == "completed"), None)
    active = next((row for row in rows if row[1] in ("pending", "leased", "retry")), None)
    waiting = active is None and current is not None and current[1] is not None and newest_completed != str(current[1])
    pending_age = (
        max(0.0, (now - active[6]).total_seconds()) if active is not None else max(0.0, (now - current[2]).total_seconds()) if waiting else None
    )
    active_state = str(active[1]) if active is not None else "waiting_for_signals" if waiting else None
    lease_expiry = active[4] if active is not None else None
    degraded = bool(
        (active_state == "retry")
        or (active_state == "pending" and pending_age is not None and pending_age > _STALE_SECONDS)
        or (active_state == "waiting_for_signals" and pending_age is not None and pending_age > _STALE_SIGNALS_SECONDS)
        or (active_state == "leased" and lease_expiry is not None and lease_expiry <= now)
    )
    return {
        "status": "degraded" if degraded else "enabled",
        "current_generation": int(current[0]) if current is not None else 0,
        "current_version": str(current[1]) if current is not None and current[1] is not None else None,
        "newest_completed_version": newest_completed,
        "active_version": str(active[0]) if active is not None else None,
        "phase": active_state,
        "duration_seconds": max(0.0, (now - active[7]).total_seconds()) if active is not None and active[7] is not None else None,
        "pending_age_seconds": pending_age,
        "attempt_count": int(active[2]) if active is not None else 0,
        "retry_due": active[3].isoformat() if active is not None and active[3] is not None else None,
        "lease_expiry": lease_expiry.isoformat() if lease_expiry is not None else None,
        "last_sanitized_failure": str(active[5]) if active is not None and active[5] is not None else None,
        "superseded_count": legacy_superseded + sum(1 for row in rows if row[1] == "superseded"),
    }


async def _sample_health_loop(connection_pool: Any, logger: Any, health: dict[str, Any]) -> None:
    """Observe a multi-hour pass while the worker is inside its transaction."""
    while True:
        try:
            snapshot = await read_refresh_health(connection_pool)
            scanner_error = health.get("scanner_error")
            if scanner_error is not None:
                snapshot["status"] = "degraded"
                snapshot["scanner_error"] = scanner_error
            health.clear()
            health.update(snapshot)
            telemetry.record_derived_refresh_health(health)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            health.clear()
            health.update({"status": "degraded", "last_sanitized_failure": type(exc).__name__})
            logger.error("❌ Durable refresh health sampler failed", error_type=type(exc).__name__)
        await asyncio.sleep(_SCAN_SECONDS)


async def run_worker_loop(connection_pool: Any, logger: Any, owner: str, health: dict[str, Any]) -> None:
    """Recover at startup and periodically even when RabbitMQ sends nothing."""
    sampler = asyncio.create_task(_sample_health_loop(connection_pool, logger, health))
    try:
        while True:
            try:
                worked = await run_worker_once(connection_pool, logger, owner)
                health.pop("scanner_error", None)
                if not worked:
                    await asyncio.sleep(_SCAN_SECONDS)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                health.clear()
                health.update({"status": "degraded", "scanner_error": type(exc).__name__})
                logger.error("❌ Durable refresh scanner failed; retrying", error_type=type(exc).__name__)
                await asyncio.sleep(_SCAN_SECONDS)
    finally:
        sampler.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await sampler
