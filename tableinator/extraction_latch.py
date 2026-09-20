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
there; what happens here is a read of `information_schema` at startup to find it. When it is
present the loader runs as described above. When it is absent the loader runs in a degraded
mode that records no signal and fires no refresh, says so in the log and in the health
payload, and never tries to make the relation itself. The counters simply stay as the last
successful pass left them, which is the failure that can be seen and fixed rather than the
one that writes a table nobody declared.

`public.extraction_history` cannot serve as that relation: it is keyed on a UUID and a
`users` row the loader has neither of. `public.app_config` holds the encrypted Discogs
consumer key and secret, which is not a table to put coordination state in.

**A message that names no extraction is refused rather than guessed at.** See
`extraction_latch_key`: there is no key that tells two versionless dumps apart, so lumping
them under one sentinel row would let the first stamp `refreshed_at` and every later one
read as already refreshed and never fire. That is the silent skip this latch exists to
prevent, so the signal is logged at ERROR and dropped instead.

**Until the pin moves**, the relation is being declared by a `database-schema` chore and its
name is not yet settled, so `LATCH_CANDIDATES` names both proposals and the probe takes the
first that matches. The follow-up that repins to the revision declaring it should cut this
list to the one that landed. The `loader` discriminator is optional for the same reason: the
relation is named for the loader family so `musicbrainz-sql-loader` can share it, and if it
carries that column this module keys on it so the two loaders cannot read each other's
signals.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Final

from psycopg import sql


__all__ = [
    "LATCH_CANDIDATES",
    "LOADER_DISCRIMINATOR",
    "ExtractionLatch",
    "LatchRelation",
    "extraction_latch_key",
    "mark_extraction_refreshed",
    "probe_latch_relation",
    "record_extraction_signal",
]

# This loader's value for the `loader` column, when the declared relation carries one.
LOADER_DISCRIMINATOR: Final = "discogs"

# Where the declared relation may live, most specific first. Both names are the ones the
# `database-schema` chore proposes; the repin cuts this to whichever landed.
LATCH_CANDIDATES: Final[tuple[tuple[str, str], ...]] = (
    ("public", "loader_extraction_latch"),
    ("graph", "extraction_latch"),
)

# The columns this module reads and writes, with the `information_schema` type each must
# have. A relation that is missing one, or spells one differently, is not this relation and
# the probe declines it rather than writing into something that merely shares a name.
REQUIRED_COLUMNS: Final[dict[str, tuple[str, str | None]]] = {
    "version": ("text", None),
    "signals": ("ARRAY", "_text"),
    "created_at": ("timestamp with time zone", None),
    "updated_at": ("timestamp with time zone", None),
    "refreshed_at": ("timestamp with time zone", None),
}

# Optional, and part of the key when present.
LOADER_COLUMN: Final = "loader"
LOADER_COLUMN_TYPE: Final = "text"

_PROBE = """
SELECT column_name, data_type, udt_name
FROM information_schema.columns
WHERE table_schema = %s AND table_name = %s
"""


@dataclass(frozen=True)
class LatchRelation:
    """The declared latch relation this loader found at startup.

    Attributes:
        schema: The schema it lives in.
        table: Its name.
        keyed_on_loader: It carries a `loader` column, so both loaders share it and every
            statement this module issues is scoped to `LOADER_DISCRIMINATOR`.
    """

    schema: str
    table: str
    keyed_on_loader: bool

    @property
    def qualified(self) -> str:
        """The relation as it reads in a log line."""
        return f"{self.schema}.{self.table}"

    def _relation(self) -> sql.Identifier:
        return sql.Identifier(self.schema, self.table)

    def _key_columns(self) -> tuple[str, ...]:
        return (LOADER_COLUMN, "version") if self.keyed_on_loader else ("version",)

    def _key_predicate(self, alias: str | None = None) -> sql.Composed:
        def column(name: str) -> sql.Composable:
            return sql.SQL("{alias}.{column}").format(alias=sql.Identifier(alias), column=sql.Identifier(name)) if alias else sql.Identifier(name)

        clauses = [sql.SQL("{column} = {value}").format(column=column("version"), value=sql.Placeholder("version"))]
        if self.keyed_on_loader:
            clauses.append(sql.SQL("{column} = {value}").format(column=column(LOADER_COLUMN), value=sql.Placeholder(LOADER_COLUMN)))
        return sql.SQL(" AND ").join(clauses)

    def record_statement(self) -> sql.Composed:
        """Return the one statement that records a signal and reports the latch.

        One statement, so two consumers signalling at once cannot read a set neither of them
        wrote. `prior` is evaluated on the pre-statement snapshot, so it sees the row as it
        was before the upsert.
        """
        key_columns = self._key_columns()
        columns = sql.SQL(", ").join(sql.Identifier(name) for name in key_columns)
        values = sql.SQL(", ").join(sql.Placeholder(name) for name in key_columns)
        newer_scope = (
            sql.SQL(" AND {column} = {value}").format(column=sql.Identifier("newer", LOADER_COLUMN), value=sql.Placeholder(LOADER_COLUMN))
            if self.keyed_on_loader
            else sql.SQL("")
        )
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
        values: dict[str, Any] = {"version": version}
        if self.keyed_on_loader:
            values[LOADER_COLUMN] = LOADER_DISCRIMINATOR
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


def _match(columns: dict[str, tuple[str, str]], schema: str, table: str) -> LatchRelation | None:
    """Return the relation these `information_schema` rows describe, or None."""
    for name, (expected_type, expected_udt) in REQUIRED_COLUMNS.items():
        found = columns.get(name)
        if found is None or found[0] != expected_type:
            return None
        if expected_udt is not None and found[1] != expected_udt:
            return None
    loader = columns.get(LOADER_COLUMN)
    return LatchRelation(schema=schema, table=table, keyed_on_loader=loader is not None and loader[0] == LOADER_COLUMN_TYPE)


async def probe_latch_relation(connection_pool: Any, logger: Any) -> LatchRelation | None:
    """Return the declared latch relation, or None when the schema does not carry one.

    Read-only, and run once at startup. A miss is not an error here: it is the degraded mode
    the module docstring describes, and the caller reports it rather than creating anything.

    Args:
        connection_pool: The loader's `AsyncPostgreSQLPool`.
        logger: The loader's structured logger.

    Returns:
        The relation to use, or None to run degraded.
    """
    try:
        async with connection_pool.connection() as conn, conn.cursor() as cursor:
            for schema, table in LATCH_CANDIDATES:
                await cursor.execute(_PROBE, (schema, table))
                rows = await cursor.fetchall()
                if not rows:
                    continue
                columns = {str(name): (str(data_type), str(udt)) for name, data_type, udt in rows}
                relation = _match(columns, schema, table)
                if relation is not None:
                    logger.info(
                        "🔒 Extraction latch relation found — the derived-relation refresh is enabled",
                        relation=relation.qualified,
                        keyed_on_loader=relation.keyed_on_loader,
                    )
                    return relation
                logger.warning(
                    "⚠️ A relation with the latch's name does not have its columns — ignoring it",
                    relation=f"{schema}.{table}",
                    columns=sorted(columns),
                )
    except Exception as exc:
        logger.error(
            "❌ Could not probe for the extraction latch relation — running without the derived-relation refresh",
            error=str(exc),
        )
        return None

    logger.warning(
        "⚠️ No extraction latch relation is declared — the counter, degree, and genre-aggregate "
        "relations will NOT be refreshed on extraction_complete. Apply a database-schema revision "
        "that declares it; this service never creates database objects.",
        candidates=[f"{schema}.{table}" for schema, table in LATCH_CANDIDATES],
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
