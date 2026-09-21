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
never reach four, and the refresh silently never runs for that dump.

**This module issues no DDL.** `docs/database-schema.md` states that this service does not
create or migrate database objects, `tests/test_service_contract.py` guards that sentence,
and `database-schema` owns every executable definition. The latch relation is declared
there; what happens here is a read of `information_schema` and `pg_constraint` at startup to
find it and check its shape. The retained `DERIVED_REFRESH_MODE=inline` rollback path
runs as described above; if absent, that path logs and reports degraded health rather
than creating DDL. The default durable path separately probes the job contract and
requeues terminal deliveries when it is missing or incompatible: no durable refresh
obligation may disappear behind an ack.

`public.extraction_history` cannot serve as that relation: it is keyed on a UUID and a
`users` row the loader has neither of. `public.app_config` holds the encrypted Discogs
consumer key and secret, which is not a table to put coordination state in.

**A message that names no extraction is refused rather than guessed at.** See
`extraction_latch_key`: there is no key that tells two versionless dumps apart, so lumping
them under one sentinel row would let the first stamp `refreshed_at` and every later one
read as already refreshed and never fire. That is the silent skip this latch exists to
prevent, so the signal is logged at ERROR and dropped instead.

**One declared name, and one declared key.** `database-schema` at the pinned revision
declares `public.loader_extraction_latch` with a `loader` discriminator and a composite
primary key over `(loader, version)`, so the probe looks for that relation and nothing else,
and the discriminator is required rather than tolerated. The relation is named for the
loader family, not for one loader, so `musicbrainz-sql-loader` writes its own rows into the
same table; every statement here is scoped to `LOADER_DISCRIMINATOR` so the two loaders can
neither read nor supersede each other's signals.

The probe checks that key as well as the columns. `ON CONFLICT (loader, version)` needs a
unique or primary-key constraint over exactly those columns, and a relation carrying the
right columns without one raises `InvalidColumnReference` on the FIRST signal — a failure
that nacks the delivery and is redelivered until the trigger is dead-lettered, long after
whoever deployed the schema has stopped watching. Asked once at startup, that same mismatch
is one log line and the degraded mode instead.

The completed pass also refreshes the two pathfinder relations the promoted schema assigns
to this loader. `graph.refresh_artist_member_of()` runs after the existing counters, then
`graph.refresh_vertex_degree()` runs because it sums that union. Only after both succeed does
the same transaction stamp `refreshed_at`; a failure therefore leaves the durable latch
eligible for redelivery and restores every relation to its pre-pass state.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Final

from psycopg import sql


__all__ = [
    "LATCH_RELATION",
    "LOADER_DISCRIMINATOR",
    "ExtractionLatch",
    "LatchRelation",
    "extraction_latch_key",
    "mark_extraction_refreshed",
    "probe_latch_relation",
    "record_extraction_signal",
]

# This loader's value for the `loader` column. `musicbrainz-sql-loader` writes 'musicbrainz'
# into the same relation; the contract records both under `extraction_latch.loader_values`.
LOADER_DISCRIMINATOR: Final = "discogs"

# The one relation `database-schema` declares, at the pinned revision.
LATCH_RELATION: Final[tuple[str, str]] = ("public", "loader_extraction_latch")

# The discriminator that keys this loader's rows apart from the other loader's. Declared
# NOT NULL, so it is required, not tolerated.
LOADER_COLUMN: Final = "loader"

# The columns this module reads and writes, with the `information_schema` type each must
# have. A relation that is missing one, or spells one differently, is not this relation and
# the probe declines it rather than writing into something that merely shares a name.
REQUIRED_COLUMNS: Final[dict[str, tuple[str, str | None]]] = {
    LOADER_COLUMN: ("text", None),
    "version": ("text", None),
    "signals": ("ARRAY", "_text"),
    "created_at": ("timestamp with time zone", None),
    "updated_at": ("timestamp with time zone", None),
    "refreshed_at": ("timestamp with time zone", None),
}

# The columns every statement keys on, and the ones `ON CONFLICT` names.
KEY_COLUMNS: Final[tuple[str, ...]] = (LOADER_COLUMN, "version")

_PROBE_COLUMNS = """
SELECT column_name, data_type, udt_name
FROM information_schema.columns
WHERE table_schema = %s AND table_name = %s
"""

# Every PRIMARY KEY and UNIQUE constraint on the relation, as its column set. `pg_constraint`
# rather than `information_schema.table_constraints` because `contype` answers the question
# directly and the catalog is readable without owning the relation. See the module docstring
# for why a missing key has to fail at startup instead of on the first signal.
_PROBE_KEYS = """
SELECT array_agg(attribute.attname::text ORDER BY attribute.attname::text) AS columns
FROM pg_constraint AS constraint_
JOIN pg_class AS relation ON relation.oid = constraint_.conrelid
JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
JOIN LATERAL unnest(constraint_.conkey) AS member(attnum) ON true
JOIN pg_attribute AS attribute ON attribute.attrelid = constraint_.conrelid AND attribute.attnum = member.attnum
WHERE namespace.nspname = %s AND relation.relname = %s AND constraint_.contype IN ('p', 'u')
GROUP BY constraint_.oid
"""


@dataclass(frozen=True)
class LatchRelation:
    """The declared latch relation this loader found at startup.

    Attributes:
        schema: The schema it lives in.
        table: Its name.
    """

    schema: str
    table: str

    @property
    def qualified(self) -> str:
        """The relation as it reads in a log line."""
        return f"{self.schema}.{self.table}"

    @property
    def keyed_on_loader(self) -> bool:
        """Always true: `loader` is a declared NOT NULL column and half of the primary key.

        Kept as a named property rather than dropped, because it is what the startup log
        line reports and what a reader checks to know this loader's statements are scoped to
        its own rows and cannot see `musicbrainz-sql-loader`'s.
        """
        return True

    @property
    def key_columns(self) -> tuple[str, ...]:
        """The columns every statement keys on, and the ones `ON CONFLICT` names."""
        return KEY_COLUMNS

    def _relation(self) -> sql.Identifier:
        return sql.Identifier(self.schema, self.table)

    def _key_predicate(self, alias: str | None = None) -> sql.Composed:
        def column(name: str) -> sql.Composable:
            return sql.SQL("{alias}.{column}").format(alias=sql.Identifier(alias), column=sql.Identifier(name)) if alias else sql.Identifier(name)

        return sql.SQL(" AND ").join(
            sql.SQL("{column} = {value}").format(column=column(name), value=sql.Placeholder(name)) for name in ("version", LOADER_COLUMN)
        )

    def record_statement(self) -> sql.Composed:
        """Return the one statement that records a signal and reports the latch.

        One statement, so two consumers signalling at once cannot read a set neither of them
        wrote. `prior` is evaluated on the pre-statement snapshot, so it sees the row as it
        was before the upsert.
        """
        columns = sql.SQL(", ").join(sql.Identifier(name) for name in self.key_columns)
        values = sql.SQL(", ").join(sql.Placeholder(name) for name in self.key_columns)
        newer_scope = sql.SQL(" AND {column} = {value}").format(column=sql.Identifier("newer", LOADER_COLUMN), value=sql.Placeholder(LOADER_COLUMN))
        return sql.SQL(
            "WITH prior AS ("
            "SELECT signals AS signals FROM {relation} WHERE {key}"
            "), upserted AS ("
            "INSERT INTO {relation} AS latch ({columns}, signals) VALUES ({values}, ARRAY[{data_type}]::text[]) "
            "ON CONFLICT ({columns}) DO UPDATE "
            "SET signals = (SELECT array_agg(DISTINCT signal ORDER BY signal) FROM unnest(latch.signals || EXCLUDED.signals) AS signal), "
            "updated_at = NOW() "
            "RETURNING latch.signals AS signals, latch.refreshed_at AS refreshed_at, latch.created_at AS created_at"
            ") "
            "SELECT upserted.signals AS signals, "
            "COALESCE({data_type} = ANY(prior.signals), false) AS already_signalled, "
            "upserted.refreshed_at IS NOT NULL AS already_refreshed, "
            "EXISTS (SELECT 1 FROM {relation} AS newer WHERE newer.created_at > upserted.created_at{newer_scope}) AS superseded "
            "FROM upserted LEFT JOIN prior ON true"
        ).format(
            relation=self._relation(),
            key=self._key_predicate(),
            columns=columns,
            values=values,
            data_type=sql.Placeholder("data_type"),
            newer_scope=newer_scope,
        )

    def stamp_statement(self) -> sql.Composed:
        """Return the statement marking one extraction's pass as complete."""
        return sql.SQL("UPDATE {relation} SET refreshed_at = NOW(), updated_at = NOW() WHERE {key}").format(
            relation=self._relation(),
            key=self._key_predicate(),
        )

    def parameters(self, version: str, data_type: str | None = None) -> dict[str, Any]:
        """Return the bound parameters for one statement against this relation."""
        values: dict[str, Any] = {"version": version, LOADER_COLUMN: LOADER_DISCRIMINATOR}
        if data_type is not None:
            values["data_type"] = data_type
        return values


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


def extraction_latch_key(data: dict[str, Any]) -> str | None:
    """Return the extraction one `extraction_complete` message belongs to, or None.

    `version` is what the extractor stamps and what `graphinator` keys on. `started_at` is
    the fallback, because two dumps that both omit a version would otherwise share one latch
    row and the second would inherit the first's four signals.

    A message carrying NEITHER names no extraction, and this returns None rather than a
    sentinel. `graphinator` files those under the literal version `"unknown"`, and that is
    the same bug one level down: every versionless dump lands on one row, the first one to
    complete stamps `refreshed_at`, and every dump after it reads as already refreshed and
    silently never fires. There is no key that can tell those dumps apart, so the honest
    answer is to refuse — the caller records nothing, refreshes nothing, and says so loudly.
    """
    version = str(data.get("version") or "").strip()
    if version:
        return version
    return str(data.get("started_at") or "").strip() or None


def _has_declared_columns(columns: dict[str, tuple[str, str]]) -> bool:
    """Whether these `information_schema` rows are the declared relation's columns."""
    for name, (expected_type, expected_udt) in REQUIRED_COLUMNS.items():
        found = columns.get(name)
        if found is None or found[0] != expected_type:
            return False
        if expected_udt is not None and found[1] != expected_udt:
            return False
    return True


async def probe_latch_relation(connection_pool: Any, logger: Any) -> LatchRelation | None:
    """Return the declared latch relation, or None when the schema does not carry one.

    Read-only, and run once at startup. A miss is not an error here: it is the degraded mode
    the module docstring describes, and the caller reports it rather than creating anything.

    Three things have to hold, and each failure has its own log line so the deployment that
    caused it can be read off the startup output: the relation exists, it carries every
    column in `REQUIRED_COLUMNS` at the declared type, and a primary or unique key covers
    exactly `KEY_COLUMNS`. The last is the one that cannot be deferred to first use — without
    it the upsert's `ON CONFLICT` raises, and the signal nacks and redelivers forever.

    Args:
        connection_pool: The loader's `AsyncPostgreSQLPool`.
        logger: The loader's structured logger.

    Returns:
        The relation to use, or None to run degraded.
    """
    schema, table = LATCH_RELATION
    relation = LatchRelation(schema=schema, table=table)
    try:
        async with connection_pool.connection() as conn, conn.cursor() as cursor:
            await cursor.execute(_PROBE_COLUMNS, (schema, table))
            rows = await cursor.fetchall()
            if not rows:
                logger.warning(
                    "⚠️ No extraction latch relation is declared — the counter, degree, and genre-aggregate "
                    "relations will NOT be refreshed on extraction_complete. Apply a database-schema revision "
                    "that declares it; this service never creates database objects.",
                    relation=relation.qualified,
                )
                return None

            columns = {str(name): (str(data_type), str(udt)) for name, data_type, udt in rows}
            if not _has_declared_columns(columns):
                logger.warning(
                    "⚠️ A relation with the latch's name does not have its columns — ignoring it",
                    relation=relation.qualified,
                    columns=sorted(columns),
                    required=sorted(REQUIRED_COLUMNS),
                )
                return None

            await cursor.execute(_PROBE_KEYS, (schema, table))
            declared_keys = {tuple(sorted(row[0] or ())) for row in await cursor.fetchall()}
            required_key = tuple(sorted(relation.key_columns))
            if required_key not in declared_keys:
                logger.warning(
                    "⚠️ The latch relation has no primary or unique key on the columns the upsert "
                    "conflicts on — ignoring it rather than nacking every signal forever",
                    relation=relation.qualified,
                    required=list(required_key),
                    declared=[list(key) for key in sorted(declared_keys)],
                )
                return None

            logger.info(
                "🔒 Extraction latch relation found — the derived-relation refresh is enabled",
                relation=relation.qualified,
                keyed_on_loader=relation.keyed_on_loader,
                key_columns=list(relation.key_columns),
            )
            return relation
    except Exception as exc:
        logger.error(
            "❌ Could not probe for the extraction latch relation — running without the derived-relation refresh",
            error=str(exc),
        )
        return None


async def record_extraction_signal(connection_pool: Any, latch: LatchRelation, version: str, data_type: str) -> ExtractionLatch:
    """Record that DATA_TYPE has signalled VERSION, and return the latch as it now stands.

    Written before the delivery is acked, and idempotent under redelivery: the signal set is
    a union, so the same signal recorded twice leaves the same row.

    Args:
        connection_pool: The loader's `AsyncPostgreSQLPool`.
        latch: The declared relation the startup probe found.
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
            await cursor.execute(latch.record_statement(), latch.parameters(version, data_type))
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


async def mark_extraction_refreshed(cursor: Any, latch: LatchRelation, version: str) -> None:
    """Stamp VERSION as refreshed, on the caller's transaction.

    On the pass's own transaction rather than after it, so a pass that rolls back leaves the
    extraction unstamped and the next delivery of any of its four signals runs it again.
    """
    await cursor.execute(latch.stamp_statement(), latch.parameters(version))
