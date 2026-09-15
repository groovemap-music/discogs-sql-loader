from __future__ import annotations

from typing import TYPE_CHECKING, Any, NamedTuple, Protocol

from common.identity import resolve_aliases
from psycopg import sql
from psycopg.types.json import Jsonb

from tableinator.identity import alias_ref, alias_targets, attach_alias_targets


if TYPE_CHECKING:
    import uuid
    from collections.abc import Callable, Sequence


class BatchRecord(Protocol):
    data_id: str
    data: dict[str, Any]
    sha256: str


class BatchWriteResult(NamedTuple):
    """Records skipped by hash and the subsets receiving a backfill."""

    unchanged_ids: set[str]
    media_backfilled_ids: set[str]
    identity_backfilled_ids: set[str]


class PostgreSQLBatchWriter:
    """Own entity-specific statements and parameters for one atomic batch write."""

    def __init__(
        self,
        connection_pool: Any,
        logger: Any,
        media_resolver: Callable[[dict[str, Any]], dict[str, Any]],
    ) -> None:
        self.connection_pool = connection_pool
        self.logger = logger
        self.media_resolver = media_resolver

    async def process_batch(
        self,
        data_type: str,
        messages: Sequence[BatchRecord],
    ) -> BatchWriteResult:
        """Write a batch in one transaction and report unchanged records."""
        async with self.connection_pool.connection() as conn:
            await conn.set_autocommit(False)
            async with conn.transaction(), conn.cursor() as cursor:
                data_ids = [msg.data_id for msg in messages]
                hash_query = (
                    "SELECT data_id, hash, media IS NULL, gm_item_id IS NULL FROM {table} WHERE data_id = ANY(%s)"
                    if data_type == "releases"
                    else "SELECT data_id, hash, gm_item_id IS NULL FROM {table} WHERE data_id = ANY(%s)"
                )
                await cursor.execute(  # nosemgrep: python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
                    sql.SQL(hash_query).format(table=sql.Identifier(data_type)),
                    (data_ids,),
                )
                existing_rows = await cursor.fetchall()
                existing_hashes = {row[0]: row[1] for row in existing_rows}
                media_is_null_ids = {row[0] for row in existing_rows if row[2]} if data_type == "releases" else set()
                # `gm_item_id IS NULL` is the last column either query selects, so its index
                # follows the release-only `media IS NULL` column when that one is present.
                identity_column = 3 if data_type == "releases" else 2
                identity_is_null_ids = {row[0] for row in existing_rows if row[identity_column]}

                # Native identity (ADR 0009): one resolve for the whole batch, on this
                # transaction's own connection and before any write, so the batch is either
                # fully identified or rolled back whole. `resolve_aliases` mints the misses
                # and opens no SAVEPOINT, so a failure surfaces to this transaction.
                refs = {msg.data_id: alias_ref(data_type, msg.data_id) for msg in messages}
                native_ids = await resolve_aliases(conn, list(refs.values()))
                unresolved = sorted(data_id for data_id, ref in refs.items() if ref not in native_ids)
                if unresolved:
                    # Every Discogs entity kind is a catalog kind, so a miss here is a broken
                    # assumption rather than a retryable outage: fail the batch as poison.
                    raise RuntimeError(f"resolve_aliases returned no native id for {data_type}: {unresolved}")

                # Identifier aliases (ADR 0011): every release in the batch contributes, not
                # only the ones whose hash changed. The existing-rows SELECT reads the entity
                # table and cannot see whether a row's aliases were ever attached, so a row
                # skipped by hash is exactly the row whose aliases are most likely missing.
                # Attaching is idempotent, so re-attaching a present alias costs one conflicted
                # INSERT row and nothing else.
                alias_attachments = alias_targets(data_type, [(msg.data, native_ids[refs[msg.data_id]]) for msg in messages])

                records_to_upsert: list[tuple[Any, ...]] = []
                unchanged_ids: list[str] = []
                media_backfills: list[tuple[Jsonb, str]] = []
                identity_backfills: list[tuple[uuid.UUID, str]] = []
                for msg in messages:
                    native_id = native_ids[refs[msg.data_id]]
                    existing_hash = existing_hashes.get(msg.data_id)
                    if existing_hash == msg.sha256:
                        unchanged_ids.append(msg.data_id)
                        if msg.data_id in media_is_null_ids:
                            media_backfills.append((Jsonb(self.media_resolver(msg.data)), msg.data_id))
                        if msg.data_id in identity_is_null_ids:
                            identity_backfills.append((native_id, msg.data_id))
                        continue
                    if data_type == "releases":
                        records_to_upsert.append(
                            (
                                msg.sha256,
                                msg.data_id,
                                Jsonb(msg.data),
                                Jsonb(self.media_resolver(msg.data)),
                                native_id,
                            )
                        )
                    else:
                        records_to_upsert.append((msg.sha256, msg.data_id, Jsonb(msg.data), native_id))

                media_backfilled_ids = {data_id for _media, data_id in media_backfills}
                identity_backfilled_ids = {data_id for _native_id, data_id in identity_backfills}

                if unchanged_ids:
                    self.logger.debug(
                        "🔄 Skipped unchanged records",
                        data_type=data_type,
                        skipped=len(unchanged_ids) - len(media_backfills),
                        media_backfilled=len(media_backfills),
                        identity_backfilled=len(identity_backfills),
                    )
                    # A backfill UPDATE carries its own NOW(), so the rows it touches are
                    # excluded from the plain updated_at refresh rather than written twice.
                    backfilled_ids = media_backfilled_ids | identity_backfilled_ids
                    refresh_ids = [data_id for data_id in unchanged_ids if data_id not in backfilled_ids]
                    if refresh_ids:
                        await cursor.execute(  # nosemgrep: python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
                            sql.SQL("UPDATE {table} SET updated_at = NOW() WHERE data_id = ANY(%s)").format(table=sql.Identifier(data_type)),
                            (refresh_ids,),
                        )

                if media_backfills:
                    self.logger.info(
                        "🎚️ Backfilled media on hash-unchanged rows",
                        data_type=data_type,
                        media_backfilled=len(media_backfills),
                    )
                    await cursor.executemany(
                        sql.SQL("UPDATE {table} SET media = %s, updated_at = NOW() WHERE data_id = %s").format(table=sql.Identifier(data_type)),
                        media_backfills,
                    )

                if identity_backfills:
                    self.logger.info(
                        "🪪 Backfilled gm_item_id on hash-unchanged rows",
                        data_type=data_type,
                        identity_backfilled=len(identity_backfills),
                    )
                    await cursor.executemany(
                        sql.SQL("UPDATE {table} SET gm_item_id = %s, updated_at = NOW() WHERE data_id = %s").format(table=sql.Identifier(data_type)),
                        identity_backfills,
                    )

                if records_to_upsert:
                    if data_type == "releases":
                        await cursor.executemany(
                            sql.SQL(
                                "INSERT INTO {table} (hash, data_id, data, media, gm_item_id, updated_at) "
                                "VALUES (%s, %s, %s, %s, %s, NOW()) "
                                "ON CONFLICT (data_id) DO UPDATE "
                                "SET hash = EXCLUDED.hash, data = EXCLUDED.data, media = EXCLUDED.media, "
                                "gm_item_id = EXCLUDED.gm_item_id, updated_at = NOW()"
                            ).format(table=sql.Identifier(data_type)),
                            records_to_upsert,
                        )
                    else:
                        await cursor.executemany(
                            sql.SQL(
                                "INSERT INTO {table} (hash, data_id, data, gm_item_id, updated_at) "
                                "VALUES (%s, %s, %s, %s, NOW()) "
                                "ON CONFLICT (data_id) DO UPDATE "
                                "SET hash = EXCLUDED.hash, data = EXCLUDED.data, "
                                "gm_item_id = EXCLUDED.gm_item_id, updated_at = NOW()"
                            ).format(table=sql.Identifier(data_type)),
                            records_to_upsert,
                        )

                    self.logger.debug(
                        "🐘 Batch upserted records",
                        data_type=data_type,
                        upserted=len(records_to_upsert),
                        skipped=len(unchanged_ids) - len(media_backfills),
                        media_backfilled=len(media_backfills),
                        identity_backfilled=len(identity_backfills),
                    )

                # One attach per batch, after the upsert and on this transaction's own
                # connection, so the aliases and the rows they point at commit or roll back
                # together.
                await attach_alias_targets(conn, alias_attachments, self.logger, data_type)

                return BatchWriteResult(set(unchanged_ids), media_backfilled_ids, identity_backfilled_ids)
