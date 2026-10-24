"""cli() exit status: a failed start exits 1 (Docker restart: on-failure), a clean stop exits 0."""

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import tableinator.tableinator as tableinator_module
from tableinator.tableinator import cli


REQUIRED_ENV = {
    "POSTGRES_HOST": "localhost",
    "POSTGRES_USERNAME": "user",
    "POSTGRES_PASSWORD": "pw",
    "POSTGRES_DATABASE": "db",
    "RABBITMQ_USERNAME": "u",
    "RABBITMQ_PASSWORD": "p",
    "STARTUP_DELAY": "0",
}


def _run_cli(env: dict[str, str], *patches: Any) -> tuple[Any, MagicMock]:
    """Run cli() under env with startup side effects stubbed; return (exit status, shutdown_telemetry mock)."""
    with (
        patch.dict("os.environ", env, clear=True),
        patch("tableinator.tableinator.setup_logging"),
        patch("tableinator.tableinator.setup_telemetry"),
        patch("tableinator.tableinator.start_event_loop_monitor"),
        patch("tableinator.tableinator.HealthServer"),
        patch("tableinator.tableinator.shutdown_telemetry") as shutdown,
        patch("tableinator.tableinator.shutdown_requested", False),
    ):
        for p in patches:
            p.start()
        try:
            with pytest.raises(SystemExit) as exc:
                cli()
        finally:
            patch.stopall()
        return exc.value.code, shutdown


def _expect_exit_1(env: dict[str, str], *patches: Any) -> None:
    code, shutdown = _run_cli(env, *patches)
    assert code == 1
    shutdown.assert_called_once()


@pytest.mark.parametrize("missing", ["POSTGRES_HOST", "POSTGRES_USERNAME", "POSTGRES_PASSWORD", "POSTGRES_DATABASE"])
def test_missing_required_env_exits_1(missing: str) -> None:
    _expect_exit_1({k: v for k, v in REQUIRED_ENV.items() if k != missing})


def test_invalid_health_port_exits_1() -> None:
    _expect_exit_1({**REQUIRED_ENV, "HEALTH_PORT": "abc"})


def test_small_postgres_pool_exits_1() -> None:
    _expect_exit_1({**REQUIRED_ENV, "POSTGRES_POOL_MAX_SIZE": "3", "POSTGRES_POOL_MIN_SIZE": "1"})


def test_connection_pool_failure_exits_1() -> None:
    pool = MagicMock()
    pool.initialize = AsyncMock(side_effect=OSError("db down"))
    _expect_exit_1(REQUIRED_ENV, patch("tableinator.tableinator.AsyncPostgreSQLPool", return_value=pool))


def _pool_ok() -> Any:
    pool = MagicMock()
    pool.initialize = AsyncMock()
    return patch("tableinator.tableinator.AsyncPostgreSQLPool", return_value=pool)


def _probes() -> list[Any]:
    return [
        _pool_ok(),
        patch("tableinator.tableinator.probe_latch_relation", AsyncMock(return_value=None)),
        patch("tableinator.tableinator.probe_durable_schema", AsyncMock(return_value=False)),
        patch("tableinator.tableinator.asyncio.sleep", AsyncMock()),
    ]


def test_amqp_unreachable_exits_1() -> None:
    manager = MagicMock()
    manager.connect = AsyncMock(side_effect=OSError("broker down"))
    _expect_exit_1(
        REQUIRED_ENV,
        *_probes(),
        patch("tableinator.tableinator.AsyncResilientRabbitMQ", return_value=manager),
    )


def test_no_amqp_connection_exits_1() -> None:
    manager = MagicMock()
    manager.connect = AsyncMock(return_value=None)
    _expect_exit_1(
        REQUIRED_ENV,
        *_probes(),
        patch("tableinator.tableinator.AsyncResilientRabbitMQ", return_value=manager),
    )


def test_unhandled_error_exits_1() -> None:
    _expect_exit_1(REQUIRED_ENV, patch("tableinator.tableinator.TableinatorConfig.from_env", side_effect=RuntimeError("boom")))


def test_clean_shutdown_exits_0() -> None:
    with patch("tableinator.tableinator.main", AsyncMock(return_value=None)):
        cli()  # returns normally: exit status 0


def test_signal_before_amqp_connected_exits_0() -> None:
    def request_shutdown(*_: Any) -> None:
        tableinator_module.shutdown_requested = True

    manager = MagicMock()
    manager.connect = AsyncMock()
    try:
        with (
            patch.dict("os.environ", REQUIRED_ENV, clear=True),
            patch("tableinator.tableinator.setup_logging"),
            patch("tableinator.tableinator.setup_telemetry"),
            patch("tableinator.tableinator.start_event_loop_monitor"),
            patch("tableinator.tableinator.HealthServer"),
            patch("tableinator.tableinator.shutdown_telemetry"),
            patch("tableinator.tableinator.AsyncResilientRabbitMQ", return_value=manager),
            patch("tableinator.tableinator.probe_latch_relation", AsyncMock(return_value=None)),
            patch("tableinator.tableinator.probe_durable_schema", AsyncMock(return_value=False)),
            patch("tableinator.tableinator.signal.signal", side_effect=request_shutdown),
            _pool_ok(),
        ):
            cli()  # a stop requested during startup is not a failed start
    finally:
        tableinator_module.shutdown_requested = False
