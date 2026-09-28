"""The service's own file log sink rotates (gm-discogs-sql-loader-d44.1).

``main()`` calls ``setup_logging(SERVICE_NAME, log_file=LOG_PATH)``
(``tableinator/tableinator.py``). The shared runtime bounds that file sink to a
``RotatingFileHandler`` as of python-libraries 11cf764 ("fix(logging): bound application
log files"), replacing the previously unbounded ``logging.FileHandler``. This proves this
service's own call site receives the bounded handler, rather than relying solely on the
library's coverage of ``build_rotating_file_handler`` in isolation.
"""

from __future__ import annotations

from logging.handlers import RotatingFileHandler
from pathlib import Path
from unittest.mock import patch

from tableinator.tableinator import SERVICE_NAME, setup_logging


def test_service_log_file_sink_is_a_rotating_handler(tmp_path: Path) -> None:
    """The file handler built for this service's ``log_file`` is a ``RotatingFileHandler``."""
    log_file = tmp_path / f"{SERVICE_NAME}.log"

    # Patch basicConfig rather than let it run for real, so this test does not clobber
    # the process's global logging configuration for whatever test runs after it --
    # the same seam python-libraries' own setup_logging test patches.
    with patch("common.config.logging.basicConfig") as basic_config:
        setup_logging(SERVICE_NAME, log_file=log_file)

    handlers = basic_config.call_args.kwargs["handlers"]
    file_handlers = [handler for handler in handlers if isinstance(handler, RotatingFileHandler)]

    assert len(file_handlers) == 1
    assert Path(file_handlers[0].baseFilename) == log_file
    file_handlers[0].close()
