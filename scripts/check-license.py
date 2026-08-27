"""Validate the current first-party license and synchronized package version."""

import re
import tomllib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
with (ROOT / "pyproject.toml").open("rb") as source:
    project = tomllib.load(source)["project"]

version_match = re.search(r'^__version__ = "([^"]+)"$', (ROOT / "tableinator/__init__.py").read_text(), re.MULTILINE)
assert version_match is not None
assert project["license"] == "MIT"
assert project["version"] == version_match.group(1)
assert (ROOT / "LICENSE").read_text().startswith("MIT License\n")
