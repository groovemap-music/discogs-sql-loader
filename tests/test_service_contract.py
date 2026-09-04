"""Import, identity, and promoted catalog-contract smoke tests."""

from pathlib import Path

import tableinator.tableinator as service
from tableinator.catalog_contract import AMQP_EXCHANGE_TYPE
from tableinator.catalog_contract import ENTITY_TYPES as DATA_TYPES
from tableinator.catalog_contract import EXCHANGE_PREFIX as DISCOGS_EXCHANGE_PREFIX


ROOT = Path(__file__).parent.parent


def test_service_import_exposes_entry_point() -> None:
    assert callable(service.main)


def test_runtime_identity_uses_repository_name() -> None:
    assert service.SERVICE_NAME == "discogs-sql-loader"
    assert Path("/logs/discogs-sql-loader.log") == service.LOG_PATH
    assert "GrooveMap" in service.STARTUP_BANNER
    assert "discogs-sql-loader" in service.STARTUP_BANNER
    assert "Tableinator" not in service.STARTUP_BANNER


def test_legacy_consumer_name_is_an_explicit_wire_compatibility_boundary() -> None:
    assert service.AMQP_CONSUMER_NAME == "tableinator"


def test_public_documentation_uses_mermaid_and_records_compatibility() -> None:
    readme = (ROOT / "README.md").read_text()
    implementation = (ROOT / "tableinator/README.md").read_text()
    compatibility = (ROOT / "docs/compatibility.md").read_text()
    assert "```mermaid" in readme
    assert "```mermaid" in implementation
    assert "ghcr.io/groovemap-music/discogs-sql-loader" in readme
    assert "AMQP consumer key `tableinator`" in compatibility
    assert "Historical `discogsography-*` issue IDs" in compatibility


def test_documented_defaults_and_schema_ownership_match_promoted_contracts() -> None:
    performance = (ROOT / "docs/performance-guide.md").read_text()
    schema = (ROOT / "docs/database-schema.md").read_text()
    assert "POSTGRES_BATCH_SIZE = 100  # Records per batch" in performance
    assert "POSTGRES_BATCH_FLUSH_INTERVAL = 5.0  # Seconds between flushes" in performance
    assert "repository owns every schema definition" in schema
    assert "`src/groovemap_schema/neo4j.py`" in schema
    assert "`src/groovemap_schema/postgres.py`" in schema
    assert "`schema-init` is only the one-shot service name" in schema
    assert "schema-init/neo4j_schema.py" not in schema
    assert "schema-init/postgres_schema.py" not in schema


def test_catalog_contract_matches_discogs_stream() -> None:
    assert DISCOGS_EXCHANGE_PREFIX == "groovemap-discogs"
    assert AMQP_EXCHANGE_TYPE == "fanout"
    assert DATA_TYPES == ["artists", "labels", "masters", "releases"]


def test_public_tree_excludes_private_planning_material() -> None:
    assert not (ROOT / "docs" / "extraction.md").exists()
    assert not any(item.is_file() for item in (ROOT / "docs" / "superpowers").rglob("*"))
    assert not any(item.is_file() for item in (ROOT / "docs" / "specs").rglob("*"))


def test_publication_docs_preserve_separate_operator_gates() -> None:
    release = (ROOT / "docs" / "release-compliance.md").read_text()
    history = (ROOT / "docs" / "history-rewrite-gate.md").read_text()
    assert "Dependabot-authored pull requests run the same required" in release
    assert "explicit operator" in release
    assert "Visibility, tags" in release
    assert "Explicit operator approval" in history
    assert "daf82a149aaa382b3cebbd4b43d3c82e53d4128e" in history
