from __future__ import annotations

from typing import TYPE_CHECKING, Any, NamedTuple, Protocol

from psycopg import sql
from psycopg.types.json import Jsonb


if TYPE_CHECKING:
    from collections.abc import Callable, Sequence


class BatchRecord(Protocol):
    data_id: str
    data: dict[str, Any]
    sha256: str


class BatchWriteResult(NamedTuple):
    """Records skipped by hash and the subset receiving a media backfill."""

    unchanged_ids: set[str]
    media_backfilled_ids: set[str]


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
                    "SELECT data_id, hash, media IS NULL FROM {table} WHERE data_id = ANY(%s)"
                    if data_type == "releases"
                    else "SELECT data_id, hash FROM {table} WHERE data_id = ANY(%s)"
                )
                await cursor.execute(  # nosemgrep: python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
                    sql.SQL(hash_query).format(table=sql.Identifier(data_type)),
                    (data_ids,),
                )
                existing_rows = await cursor.fetchall()
                existing_hashes = {row[0]: row[1] for row in existing_rows}
                media_is_null_ids = {row[0] for row in existing_rows if row[2]} if data_type == "releases" else set()

                records_to_upsert: list[tuple[Any, ...]] = []
                unchanged_ids: list[str] = []
                media_backfills: list[tuple[Jsonb, str]] = []
                for msg in messages:
                    existing_hash = existing_hashes.get(msg.data_id)
                    if existing_hash == msg.sha256:
                        unchanged_ids.append(msg.data_id)
                        if msg.data_id in media_is_null_ids:
                            media_backfills.append((Jsonb(self.media_resolver(msg.data)), msg.data_id))
                        continue
                    if data_type == "releases":
                        records_to_upsert.append(
                            (
                                msg.sha256,
                                msg.data_id,
                                Jsonb(msg.data),
                                Jsonb(self.media_resolver(msg.data)),
                            )
                        )
                    else:
                        records_to_upsert.append((msg.sha256, msg.data_id, Jsonb(msg.data)))

                media_backfilled_ids = {data_id for _media, data_id in media_backfills}

                if unchanged_ids:
                    self.logger.debug(
                        "🔄 Skipped unchanged records",
                        data_type=data_type,
                        skipped=len(unchanged_ids) - len(media_backfills),
                        media_backfilled=len(media_backfills),
                    )
                    refresh_ids = [data_id for data_id in unchanged_ids if data_id not in media_backfilled_ids]
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

                if not records_to_upsert:
                    return BatchWriteResult(set(unchanged_ids), media_backfilled_ids)

                if data_type == "releases":
                    await cursor.executemany(
                        sql.SQL(
                            "INSERT INTO {table} (hash, data_id, data, media, updated_at) "
                            "VALUES (%s, %s, %s, %s, NOW()) "
                            "ON CONFLICT (data_id) DO UPDATE "
                            "SET hash = EXCLUDED.hash, data = EXCLUDED.data, media = EXCLUDED.media, updated_at = NOW()"
                        ).format(table=sql.Identifier(data_type)),
                        records_to_upsert,
                    )
                else:
                    await cursor.executemany(
                        sql.SQL(
                            "INSERT INTO {table} (hash, data_id, data, updated_at) "
                            "VALUES (%s, %s, %s, NOW()) "
                            "ON CONFLICT (data_id) DO UPDATE "
                            "SET hash = EXCLUDED.hash, data = EXCLUDED.data, updated_at = NOW()"
                        ).format(table=sql.Identifier(data_type)),
                        records_to_upsert,
                    )

                self.logger.debug(
                    "🐘 Batch upserted records",
                    data_type=data_type,
                    upserted=len(records_to_upsert),
                    skipped=len(unchanged_ids) - len(media_backfills),
                    media_backfilled=len(media_backfills),
                )

                return BatchWriteResult(set(unchanged_ids), media_backfilled_ids)
