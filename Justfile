set shell := ["bash", "-euo", "pipefail", "-c"]

default:
    @just --list

setup:
    uv sync --dev --frozen

source-check: format-check lint contract-check

format-check:
    uv run ruff format --check .

lint:
    uv run ruff check .

contract-check:
    uv run python scripts/check-contracts.py

secret-scan:
    gitleaks git --redact --no-banner
    gitleaks dir . --redact --no-banner

check: source-check typecheck coverage secret-scan build install-check license-check bump-preview

format:
    uv run ruff format .
    uv run ruff check --fix .

typecheck:
    uv run mypy

test:
    uv run pytest -m "not integration" --cov=tableinator --cov-report=term-missing --cov-report=xml

coverage: test

# Starts a pinned disposable PostgreSQL container unless TEST_DATABASE_URL points
# at an existing disposable database. This lane is intentionally outside `check`.
# It deselects `parity`, which needs a second engine.
# PostgreSQL integration suites, in a disposable database.
test-integration:
    bash scripts/test-integration.sh

# Advisory SQL/PGQ tier on the immutable PostgreSQL 19 Beta 3 image.
test-integration-pg19:
    POSTGRES_INTEGRATION_IMAGE=postgres:19beta3-alpine@sha256:b1692e50613a21e61c424859f943b9e193ae73e5a8c68abd5382dfb235bf15fc bash scripts/test-integration.sh

# Cross-store parity against discogs-graph-enricher: the same fixture events into this
# loader's PostgreSQL and the enricher's Neo4j, then a per-label comparison of the two
# graphs. Starts both pinned containers and removes them, and their anonymous volumes,
# on the way out. Opt-in and outside `check` because it is the one lane that needs Neo4j.
# Documented in docs/store-parity.md.
# Cross-store parity with discogs-graph-enricher, in disposable PostgreSQL and Neo4j.
test-parity:
    bash scripts/test-parity.sh

build:
    uv build --out-dir dist --clear

install-check: build
    bash scripts/install-check.sh

license-check:
    uv run python scripts/check-license.py
    uv run pip-licenses --fail-on "GPL-2.0-only;GPL-3.0-only;AGPL-3.0-only"

audit:
    uv run pip-audit

prepare-runtime-wheel:
    bash scripts/prepare-runtime-wheel.sh

image: prepare-runtime-wheel
    bash scripts/build-image.sh
    docker run --rm --entrypoint /app/.venv/bin/python discogs-sql-loader:local -c 'import tableinator.tableinator'
    test "$(docker run --rm --entrypoint /usr/bin/id discogs-sql-loader:local -u):$(docker run --rm --entrypoint /usr/bin/id discogs-sql-loader:local -g)" = "1000:1000"

bump-preview:
    uv run python scripts/check_bump_preview.py

# Update local version metadata and changelog only; do not commit, tag, push, or publish.
bump:
    uv run cz bump --version-files-only --changelog --yes --check-consistency
    uv lock

release-dry-run: check
    bash scripts/release-dry-run.sh
