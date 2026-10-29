"""HEALTH_PORT: the SQL-loader role port (8002) and its validated override."""

import re
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from tableinator.config import DEFAULT_HEALTH_PORT, TableinatorConfig
from tableinator.tableinator import StartupFailure, main


DOCKERFILE = (Path(__file__).parent.parent / "Dockerfile").read_text()

REQUIRED_ENV = {
    "POSTGRES_HOST": "localhost",
    "POSTGRES_USERNAME": "user",
    "POSTGRES_PASSWORD": "pw",
    "POSTGRES_DATABASE": "db",
    "RABBITMQ_USERNAME": "u",
    "RABBITMQ_PASSWORD": "p",
}


def _load(extra: dict[str, str]) -> TableinatorConfig:
    with patch.dict("os.environ", {**REQUIRED_ENV, **extra}, clear=True):
        return TableinatorConfig.from_env()


def test_default_health_port_is_role_port() -> None:
    assert DEFAULT_HEALTH_PORT == 8002
    assert _load({}).health_port == 8002


def test_health_port_override() -> None:
    assert _load({"HEALTH_PORT": "9102"}).health_port == 9102


@pytest.mark.parametrize("value", ["abc", "", " ", "0", "-1", "65536", "80.5"])
def test_invalid_health_port_is_rejected(value: str) -> None:
    with pytest.raises(ValueError, match="HEALTH_PORT"):
        _load({"HEALTH_PORT": value})


@pytest.mark.asyncio
async def test_main_starts_health_server_on_configured_port() -> None:
    config = MagicMock(health_port=9102, postgres_pool_max_size=12)
    with (
        patch.dict("os.environ", {"STARTUP_DELAY": "0"}),
        patch("tableinator.tableinator.setup_logging"),
        patch("tableinator.tableinator.HealthServer") as health_server,
        patch("tableinator.tableinator.TableinatorConfig.from_env", return_value=config),
        patch("tableinator.tableinator.parse_postgres_host_port", side_effect=RuntimeError("stop")),
        pytest.raises(RuntimeError, match="stop"),
    ):
        await main()
    assert health_server.call_args.args[0] == 9102


@pytest.mark.asyncio
async def test_main_does_not_start_health_server_on_invalid_port() -> None:
    with (
        patch.dict("os.environ", {**REQUIRED_ENV, "STARTUP_DELAY": "0", "HEALTH_PORT": "nope"}),
        patch("tableinator.tableinator.setup_logging"),
        patch("tableinator.tableinator.HealthServer") as health_server,
        patch("tableinator.tableinator.logger") as logger,
        pytest.raises(StartupFailure),
    ):
        await main()
    health_server.assert_not_called()
    assert "HEALTH_PORT" in str(logger.error.call_args)


def test_dockerfile_exposes_role_port_and_healthcheck_honours_override() -> None:
    assert re.search(r"^EXPOSE 8002$", DOCKERFILE, re.MULTILINE)
    healthcheck = next(line for line in DOCKERFILE.splitlines() if "urlopen(" in line)
    assert "os.environ.get('HEALTH_PORT', '8002')" in healthcheck
