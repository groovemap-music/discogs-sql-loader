"""Configuration for the GrooveMap Discogs SQL loader."""

from dataclasses import dataclass, field
from os import getenv
from typing import cast

from common.config import (
    _build_amqp_url,
    _build_postgres_connstr,
    get_secret,
    resolve_postgres_pool_sizes,
)


DEFAULT_HEALTH_PORT = 8002


def _resolve_health_port() -> int:
    """Read HEALTH_PORT, defaulting to the SQL-loader role port when it is unset.

    A set but invalid value (non-numeric, empty, or outside 1-65535) is an error, never
    a silent fallback to the default, so a mistyped override cannot leave the health
    endpoint on a port the deployment does not probe.
    """
    raw = getenv("HEALTH_PORT")
    if raw is None:
        return DEFAULT_HEALTH_PORT
    try:
        port = int(raw.strip())
    except ValueError:
        port = 0
    if not 1 <= port <= 65535:
        raise ValueError(f"HEALTH_PORT must be an integer between 1 and 65535, got {raw!r}")
    return port


@dataclass(frozen=True)
class TableinatorConfig:
    """Configuration for the Discogs SQL loader."""

    amqp_connection: str = field(repr=False)
    postgres_host: str
    postgres_username: str
    postgres_password: str = field(repr=False)
    postgres_database: str
    postgres_pool_min_size: int = 2
    postgres_pool_max_size: int = 12
    health_port: int = DEFAULT_HEALTH_PORT

    @classmethod
    def from_env(cls) -> TableinatorConfig:
        """Create configuration from environment variables."""
        postgres_username = get_secret("POSTGRES_USERNAME")
        postgres_password = get_secret("POSTGRES_PASSWORD")
        postgres_database = getenv("POSTGRES_DATABASE")
        missing_vars = [
            name
            for name, value in (
                ("POSTGRES_HOST", getenv("POSTGRES_HOST")),
                ("POSTGRES_USERNAME", postgres_username),
                ("POSTGRES_PASSWORD", postgres_password),
                ("POSTGRES_DATABASE", postgres_database),
            )
            if not value
        ]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        health_port = _resolve_health_port()
        pool_min, pool_max = resolve_postgres_pool_sizes(default_min=2, default_max=12)
        return cls(
            amqp_connection=_build_amqp_url(),
            postgres_host=_build_postgres_connstr(),
            postgres_username=cast("str", postgres_username),
            postgres_password=cast("str", postgres_password),
            postgres_database=cast("str", postgres_database),
            postgres_pool_min_size=pool_min,
            postgres_pool_max_size=pool_max,
            health_port=health_port,
        )
