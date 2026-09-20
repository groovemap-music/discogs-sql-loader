"""The durable, version-keyed latch that says when a whole extraction has finished.

The loader handles `extraction_complete` once per data type, and the stale-row purge it
already ran needs nothing more than that: a purge is scoped to the one table whose signal
arrived. The derived-relation refresh is not. It sums whole edge tables, so it must not run
until every type of THIS extraction has signalled — the four fanout queues drain at very
different rates and releases finishes last, so a per-type refresh would publish counts over
a half-loaded catalog. This is therefore a new latch rather than a reuse of anything the
purge has.

It mirrors `graphinator`'s, which is the reference implementation, and copies both of the
properties that make that one correct.

**Keyed on the extraction.** `graphinator._sync_extraction_signals` rebuilds its cache
whenever the version changes, precisely so signals from a previous dump cannot satisfy the
all-four check for the next one. A latch that only counted to four would fire on the FIRST
signal of the second monthly dump, over a catalog that is one type loaded, and then fire
again on each of the remaining three. Here every signal is recorded against the version the
message carries, so each dump collects its own four.

**Durable.** `graphinator` writes its latch to Neo4j BEFORE acking the trigger, because the
ack destroys the queued message, which was otherwise the only durable copy of the
coordination state (discogsography-tk7v). An in-memory set has the same hole: a restart
between the second and third signal loses the two already collected, the remaining two can
never reach four, and the refresh silently never runs for that dump. The rows here outlive
the process.

The table is small, loader-private, and not declared by `groovemap-database-schema`, which
is pinned and owns only relations more than one service reads. It is created with `CREATE
TABLE IF NOT EXISTS` on the transaction that first writes to it: four DDL statements per
dump, idempotent, and no startup ordering to get wrong. `public.extraction_history` is not
usable for this — it is keyed on a UUID and a `users` row the loader has neither of — and
`public.app_config` holds the encrypted Discogs consumer key and secret, which is not a
table to put coordination state in.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Final


__all__ = [
    "EXTRACTION_LATCH_TABLE",
    "EXTRACTION_LATCH_UNKNOWN_VERSION",
    "ExtractionLatch",
    "ensure_latch_table",
    "extraction_latch_key",
    "mark_extraction_refreshed",
    "record_extraction_signal",
]

# `graphinator.EXTRACTION_LATCH_UNKNOWN_VERSION`, for a signal that names no extraction.
EXTRACTION_LATCH_UNKNOWN_VERSION: Final = "unknown"

EXTRACTION_LATCH_TABLE: Final = "discogs_loader_extraction_latch"

# One row per extraction, holding the types that have signalled it. `refreshed_at` records
# that the derived-relation pass completed for that extraction, which is what lets a
# redelivered signal be answered without re-running a sweep that already succeeded.
_CREATE_LATCH_TABLE: Final = f"""
CREATE TABLE IF NOT EXISTS {EXTRACTION_LATCH_TABLE} (
    version      text PRIMARY KEY,
    signals      text[] NOT NULL DEFAULT '{{}}',
    created_at   timestamptz NOT NULL DEFAULT NOW(),
    updated_at   timestamptz NOT NULL DEFAULT NOW(),
    refreshed_at timestamptz
)
"""

# Record one signal and report the state the caller decides on, in a single statement so
# two consumers signalling at once cannot read a set neither of them wrote. `prior` is
# evaluated on the pre-statement snapshot, so it sees the row as it was before the upsert.
_RECORD_SIGNAL: Final = f"""
WITH prior AS (
    SELECT signals AS signals FROM {EXTRACTION_LATCH_TABLE} WHERE version = %(version)s
), upserted AS (
    INSERT INTO {EXTRACTION_LATCH_TABLE} AS latch (version, signals)
    VALUES (%(version)s, ARRAY[%(data_type)s]::text[])
    ON CONFLICT (version) DO UPDATE
       SET signals = (SELECT array_agg(DISTINCT signal ORDER BY signal)
                        FROM unnest(latch.signals || EXCLUDED.signals) AS signal),
           updated_at = NOW()
    RETURNING latch.signals AS signals, latch.refreshed_at AS refreshed_at, latch.created_at AS created_at
)
SELECT upserted.signals AS signals,
       COALESCE(%(data_type)s = ANY(prior.signals), false) AS already_signalled,
       upserted.refreshed_at IS NOT NULL AS already_refreshed,
       EXISTS (
           SELECT 1 FROM {EXTRACTION_LATCH_TABLE} AS newer WHERE newer.created_at > upserted.created_at
       ) AS superseded
FROM upserted LEFT JOIN prior ON true
"""  # noqa: S608

_MARK_REFRESHED: Final = f"UPDATE {EXTRACTION_LATCH_TABLE} SET refreshed_at = NOW(), updated_at = NOW() WHERE version = %s"  # noqa: S608


@dataclass(frozen=True)
class ExtractionLatch:
    """One extraction's latch, as it stands after recording a signal.

    Attributes:
        version: The extraction this signal named.
        signals: Every data type that has signalled this extraction.
        already_signalled: This type had already signalled, so the delivery is a redelivery.
        already_refreshed: The derived-relation pass has already completed for this extraction.
        superseded: A later extraction has started, so this signal is a straggler.
    """

    version: str
    signals: frozenset[str]
    already_signalled: bool
    already_refreshed: bool
    superseded: bool

    def should_refresh(self, data_types: Any) -> bool:
        """Whether this signal is the one that should run the derived-relation pass.

        Complete, not already done, and not a straggler from an extraction a later one has
        replaced. `already_signalled` is deliberately NOT a reason to decline: a pass that
        failed leaves `refreshed_at` unset and nacks its delivery, and the retry has to be
        allowed to run.
        """
        return self.signals.issuperset(data_types) and not self.already_refreshed and not self.superseded

    def pending(self, data_types: Any) -> list[str]:
        """The data types this extraction is still waiting on."""
        return sorted(set(data_types) - self.signals)


def extraction_latch_key(data: dict[str, Any]) -> str:
    """Return the extraction one `extraction_complete` message belongs to.

    `version` is what the extractor stamps and what `graphinator` keys on. `started_at` is
    the fallback rather than `unknown`, because two dumps that both omit a version would
    otherwise share one latch row and the second would inherit the first's four signals.
    """
    version = str(data.get("version") or "").strip()
    if version:
        return version
    started_at = str(data.get("started_at") or "").strip()
    return started_at or EXTRACTION_LATCH_UNKNOWN_VERSION


async def ensure_latch_table(cursor: Any) -> None:
    """Create the latch table if it is absent, on the caller's transaction.

    Idempotent and cheap, and called by every writer rather than once at startup: there is
    then no ordering between the loader connecting and the first signal arriving, and no
    statement that fails because a fresh database has never seen a dump.
    """
    await cursor.execute(_CREATE_LATCH_TABLE)


async def record_extraction_signal(connection_pool: Any, version: str, data_type: str) -> ExtractionLatch:
    """Record that DATA_TYPE has signalled VERSION, and return the latch as it now stands.

    Written before the delivery is acked, and idempotent under redelivery: the signal set
    is a union, so the same signal recorded twice leaves the same row.

    Args:
        connection_pool: The loader's `AsyncPostgreSQLPool`.
        version: The extraction the signal named, from `extraction_latch_key`.
        data_type: One of the four contract entity tables.

    Returns:
        This extraction's latch after the signal.

    Raises:
        Exception: Whatever PostgreSQL raised. The caller must requeue the signal rather
            than assume nothing has been recorded, which would lose the coordination state
            the ack is about to destroy.
    """
    async with connection_pool.connection() as conn:
        await conn.set_autocommit(False)
        async with conn.transaction(), conn.cursor() as cursor:
            await ensure_latch_table(cursor)
            await cursor.execute(_RECORD_SIGNAL, {"version": version, "data_type": data_type})
            row = await cursor.fetchone()

    if row is None:  # pragma: no cover - the upsert always returns its row
        raise RuntimeError(f"extraction latch returned no row for {version}")

    signals, already_signalled, already_refreshed, superseded = row
    return ExtractionLatch(
        version=version,
        signals=frozenset(signals or ()),
        already_signalled=bool(already_signalled),
        already_refreshed=bool(already_refreshed),
        superseded=bool(superseded),
    )


async def mark_extraction_refreshed(cursor: Any, version: str) -> None:
    """Stamp VERSION as refreshed, on the caller's transaction.

    On the pass's own transaction rather than after it, so a pass that rolls back leaves the
    extraction unstamped and the next delivery of any of its four signals runs it again.

    The table is ensured here too, because a pass can be driven directly — by the parity
    harness, or by a test — without a signal having been recorded first.
    """
    await ensure_latch_table(cursor)
    await cursor.execute(_MARK_REFRESHED, (version,))
