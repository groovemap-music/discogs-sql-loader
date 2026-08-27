"""Verify promoted contracts, generated binding, and immutable dependency pins."""

import json
import tomllib
from hashlib import sha256
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def digest(path: Path) -> str:
    """Return the hexadecimal SHA-256 digest for a file."""
    return sha256(path.read_bytes()).hexdigest()


catalog_source = json.loads((ROOT / "contracts/catalog-events/v1/source.json").read_text())
persistence_source = json.loads((ROOT / "contracts/persistence/v1/source.json").read_text())
compatibility = json.loads((ROOT / "contracts/persistence/v1/compatibility.json").read_text())
with (ROOT / "pyproject.toml").open("rb") as source:
    pyproject = tomllib.load(source)

assert digest(ROOT / "contracts/catalog-events/v1/contract.json") == catalog_source["contract_sha256"]
assert digest(ROOT / "tableinator/catalog_contract.py") == catalog_source["binding_sha256"]
assert digest(ROOT / "contracts/persistence/v1/compatibility.json") == persistence_source["contract_sha256"]
assert compatibility["contract"] == "groovemap.persistence"
assert compatibility["version"] == 1
assert compatibility["application_runtime"]["tested_version"] == "0.1.0"
runtime_source = pyproject["tool"]["uv"]["sources"]["groovemap-runtime"]
assert runtime_source["rev"] == compatibility["application_runtime"]["tested_commit"]
