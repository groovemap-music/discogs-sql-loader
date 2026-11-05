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
# PostgreSQL integration suites, in a disposable database, then the real-broker consumer lane.
test-integration:
    bash scripts/test-integration.sh
    bash scripts/test-consumer-recovery.sh

# Consumer loss and recovery on the homelab smoke's RabbitMQ image, in an exclusively owned
# container. Needs no PostgreSQL. CI runs it through `test-integration`.
test-consumer-recovery:
    bash scripts/test-consumer-recovery.sh

# Starts exclusively owned disposable PostgreSQL and RabbitMQ services; deprecated global QoS
# is disabled in RabbitMQ. Kept outside `check` and `test-integration` by design.
test-admission-integration:
    bash scripts/test-admission-integration.sh

# Write-path throughput on a disposable PostgreSQL: synthetic releases through the real batch
# writer, one JSON line out. Arguments pass through (--releases, --batch-size, --workers, --seed).
# Opt-in and outside `check`. Documented in docs/write-path-benchmark.md.
benchmark-write-path *args:
    bash scripts/benchmark-write-path.sh {{args}}

# First-load shaped run at scale (60k releases by default) with optional --defer-indexes and
# BENCHMARK_PG_ARGS server settings. Disposable PostgreSQL. Documented in docs/first-load-index-and-wal.md.
benchmark-first-load *args:
    bash scripts/benchmark-first-load.sh {{args}}

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
