#!/usr/bin/env bash
set -euo pipefail

postgres_image="postgres:18-alpine@sha256:d3e1620b530c944afa6e887d22eb899824da68e19c52024bf98f5220c88a65b2"
integration_container=""

cleanup() {
    if [[ -n "${integration_container}" ]]; then
        docker stop "${integration_container}" >/dev/null
    fi
}
trap cleanup EXIT

if [[ -z "${TEST_DATABASE_URL:-}" ]]; then
    if ! command -v docker >/dev/null 2>&1; then
        echo "Docker is required when TEST_DATABASE_URL is unset" >&2
        exit 1
    fi

    integration_container="discogs-sql-loader-integration-$$"
    docker run \
        --detach \
        --env POSTGRES_PASSWORD=integration-test \
        --name "${integration_container}" \
        --publish 127.0.0.1::5432 \
        --rm \
        "${postgres_image}" >/dev/null

    ready="false"
    for _attempt in $(seq 1 30); do
        if docker exec "${integration_container}" pg_isready --quiet --username postgres; then
            ready="true"
            break
        fi
        sleep 1
    done
    if [[ "${ready}" != "true" ]]; then
        echo "Disposable PostgreSQL did not become ready" >&2
        exit 1
    fi

    published_address="$(docker port "${integration_container}" 5432/tcp)"
    published_port="${published_address##*:}"
    TEST_DATABASE_URL="postgresql://postgres:integration-test@127.0.0.1:${published_port}/postgres"
    export TEST_DATABASE_URL
fi

# `parity` needs a Neo4j this script does not start; `just test-parity` owns that lane.
uv run pytest -m "integration and not parity"
