#!/usr/bin/env bash
set -euo pipefail

# The cross-store parity lane (docs/store-parity.md). It is the only lane in this
# repository that needs a second engine: the loader writes PostgreSQL, the pinned
# `discogs-graph-enricher` writes Neo4j, and the suite compares the two graphs.
#
# Both images are pinned by digest to the same ones `database-schema`,
# `discogs-graph-enricher`, and `catalog-api` pin, so every hive proves parity against
# one pair of engines. PostgreSQL 18 is enough here: the comparison reads the `graph`
# tables and views directly and needs no property-graph catalog object.

# The reference implementation this lane compares against, pinned to a revision so a
# parity run states exactly which enricher it proved the loader equal to.
#
# It is installed here rather than declared in `pyproject.toml` because the enricher pins
# its own `groovemap-runtime` revision, which is a hard URL conflict with the one the
# persistence contract pins for this repository. `--no-deps` sidesteps the conflict and
# keeps THIS repository's runtime in place, which is the answer parity wants anyway: both
# stores have to derive media and credit roles from one `common.media` and one
# `common.credit_roles`, or the comparison would be measuring a runtime skew rather than
# the loader. Every other dependency the enricher has is already in the dev environment.
#
# `uv sync` removes the package again, so nothing here leaks into another lane.
enricher_repository="https://github.com/groovemap-music/discogs-graph-enricher.git"
enricher_revision="${PARITY_ENRICHER_REVISION:-24c4f0c37db34106c6d418c33f5484fdbfc5336d}"

suffix="$$"
postgres_container="${POSTGRES_PARITY_CONTAINER:-discogs-sql-loader-parity-postgres-${suffix}}"
neo4j_container="${NEO4J_PARITY_CONTAINER:-discogs-sql-loader-parity-neo4j-${suffix}}"
postgres_image="${POSTGRES_INTEGRATION_IMAGE:-postgres:18-alpine@sha256:d3e1620b530c944afa6e887d22eb899824da68e19c52024bf98f5220c88a65b2}"
neo4j_image="${NEO4J_INTEGRATION_IMAGE:-neo4j:2026-community@sha256:dbc377fb9cd8fe8dabc19d3041b197d5ca0ef8bae514cea175b8df265e5b7a76}"
password="${PARITY_INTEGRATION_PASSWORD:-integration-test-password}"

# `--volumes` takes the anonymous volumes the Neo4j image declares with the container;
# without it every run leaves one behind. Nothing this script did not create is touched.
cleanup() {
    docker rm --force --volumes "${postgres_container}" "${neo4j_container}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

uv sync --dev --frozen
uv pip install --quiet --no-deps "groovemap-discogs-graph-enricher @ git+${enricher_repository}@${enricher_revision}"

docker run --detach \
    --name "${postgres_container}" \
    --publish 127.0.0.1::5432 \
    --env "POSTGRES_PASSWORD=${password}" \
    "${postgres_image}" >/dev/null

docker run --detach \
    --name "${neo4j_container}" \
    --publish 127.0.0.1::7687 \
    --env "NEO4J_AUTH=neo4j/${password}" \
    --env NEO4J_server_memory_heap_initial__size=256m \
    --env NEO4J_server_memory_heap_max__size=256m \
    "${neo4j_image}" >/dev/null

postgres_ready=false
neo4j_ready=false
for _attempt in $(seq 1 60); do
    if [[ "${postgres_ready}" != true ]] && docker exec "${postgres_container}" pg_isready --quiet --username postgres; then
        postgres_ready=true
    fi
    if [[ "${neo4j_ready}" != true ]] && docker exec "${neo4j_container}" cypher-shell --username neo4j --password "${password}" "RETURN 1" >/dev/null 2>&1; then
        neo4j_ready=true
    fi
    if [[ "${postgres_ready}" == true && "${neo4j_ready}" == true ]]; then
        break
    fi
    sleep 2
done

if [[ "${postgres_ready}" != true ]]; then
    docker logs "${postgres_container}" >&2
    echo "Disposable PostgreSQL did not become ready within 120 seconds" >&2
    exit 1
fi
if [[ "${neo4j_ready}" != true ]]; then
    docker logs "${neo4j_container}" >&2
    echo "Disposable Neo4j did not become ready within 120 seconds" >&2
    exit 1
fi

postgres_published="$(docker port "${postgres_container}" 5432/tcp)"
neo4j_published="$(docker port "${neo4j_container}" 7687/tcp)"

# Direct Bolt for the random host port. The routing scheme would accept the container's
# advertised 7687 address and accidentally leave this disposable endpoint.
TEST_DATABASE_URL="postgresql://postgres:${password}@127.0.0.1:${postgres_published##*:}/postgres" \
NEO4J_URI="bolt://127.0.0.1:${neo4j_published##*:}" \
NEO4J_INTEGRATION_USER=neo4j \
NEO4J_INTEGRATION_PASSWORD="${password}" \
PARITY_ENRICHER_REVISION="${enricher_revision}" \
    uv run --no-sync pytest -m parity tests/integration/test_store_parity.py
