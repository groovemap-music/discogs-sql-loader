"""Contract tests for the PostgreSQL doubles shared by unit tests."""

import inspect
from typing import Any

import pytest


@pytest.mark.asyncio
async def test_connection_fixture_matches_async_psycopg_protocol(mock_postgres_connection: Any) -> None:
    """The shared connection exposes real async cursor and transaction shapes."""
    unknown_method = "invented_database_method"
    with pytest.raises(AttributeError):
        getattr(mock_postgres_connection, unknown_method)

    cursor = mock_postgres_connection.cursor()
    assert not inspect.isawaitable(cursor)
    async with cursor as entered_cursor:
        assert entered_cursor is cursor
        await entered_cursor.execute("SELECT 1")
        assert await entered_cursor.fetchone() is None

    transaction = mock_postgres_connection.transaction()
    assert not inspect.isawaitable(transaction)
    async with transaction as entered_transaction:
        assert entered_transaction is transaction


@pytest.mark.asyncio
async def test_pool_fixture_connection_is_an_async_context_not_a_coroutine(
    mock_postgres_connection: Any,
    mock_async_pool: Any,
) -> None:
    """The pool follows ``async with pool.connection()`` exactly."""
    pool = mock_async_pool(mock_postgres_connection)
    unknown_method = "invented_pool_method"
    with pytest.raises(AttributeError):
        getattr(pool, unknown_method)

    connection_context = pool.connection()
    assert not inspect.isawaitable(connection_context)
    async with connection_context as entered_connection:
        assert entered_connection is mock_postgres_connection

    pool.connection.assert_called_once_with()
