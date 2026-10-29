#!/usr/bin/env bash
set -euo pipefail

postgres_image="${POSTGRES_INTEGRATION_IMAGE:-postgres:18-alpine@sha256:d3e1620b530c944afa6e887d22eb899824da68e19c52024bf98f5220c88a65b2}"
rabbitmq_image="${RABBITMQ_ADMISSION_IMAGE:-rabbitmq:4.1-management-alpine@sha256:54ef5ff505352c1beef05a97ee9bd6ab95b3559a76629ba0119b1d63d8d6e4b2}"
run_id="$$"
postgres_container="discogs-admission-postgres-${run_id}"
rabbitmq_container="discogs-admission-rabbitmq-${run_id}"
temp_dir="$(mktemp -d)"

cleanup() {
    docker rm -f "${rabbitmq_container}" "${postgres_container}" >/dev/null 2>&1 || true
    rm -rf -- "${temp_dir}"
}
trap cleanup EXIT

# Make the legacy channel-global basic.qos feature unavailable in this owned broker.
cat >"${temp_dir}/rabbitmq.conf" <<'EOF'
deprecated_features.permit.global_qos = false
EOF

docker run --detach --name "${postgres_container}" \
    --env POSTGRES_PASSWORD=integration-test \
    --publish 127.0.0.1::5432 "${postgres_image}" >/dev/null

docker create --name "${rabbitmq_container}" \
    --publish 127.0.0.1::5672 "${rabbitmq_image}" >/dev/null
docker cp "${temp_dir}/rabbitmq.conf" "${rabbitmq_container}:/etc/rabbitmq/rabbitmq.conf"
docker start "${rabbitmq_container}" >/dev/null

# Let the Erlang node create its private cookie before invoking its CLI in the container.
sleep 5

postgres_ready=false
rabbitmq_ready=false
for _attempt in $(seq 1 60); do
    if [[ "${postgres_ready}" != true ]] && docker exec "${postgres_container}" pg_isready --quiet --username postgres; then
        postgres_ready=true
    fi
    if [[ "${rabbitmq_ready}" != true ]] && \
        docker exec --user rabbitmq "${rabbitmq_container}" rabbitmq-diagnostics -q ping >/dev/null 2>&1 && \
        docker exec --user rabbitmq "${rabbitmq_container}" rabbitmq-diagnostics -q check_port_listener 5672 >/dev/null 2>&1; then
        rabbitmq_ready=true
    fi
    if [[ "${postgres_ready}" == true && "${rabbitmq_ready}" == true ]]; then
        break
    fi
    sleep 1
done

if [[ "${postgres_ready}" != true || "${rabbitmq_ready}" != true ]]; then
    if [[ "${rabbitmq_ready}" != true ]]; then
        docker logs "${rabbitmq_container}" >&2
    fi
    echo "Owned PostgreSQL or RabbitMQ did not become ready" >&2
    exit 1
fi

postgres_port="$(docker port "${postgres_container}" 5432/tcp)"
rabbitmq_port="$(docker port "${rabbitmq_container}" 5672/tcp)"
export TEST_DATABASE_URL="postgresql://postgres:integration-test@127.0.0.1:${postgres_port##*:}/postgres"
export TEST_AMQP_URL="amqp://guest:guest@127.0.0.1:${rabbitmq_port##*:}/"

uv run pytest tests/integration/test_shared_pool_admission.py -m integration
