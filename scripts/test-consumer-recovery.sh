#!/usr/bin/env bash
set -euo pipefail

# Same image digest as the homelab smoke that caught the v0.4.1 regression.
rabbitmq_image="${RABBITMQ_RECOVERY_IMAGE:-rabbitmq:4-management@sha256:c200fd496482cda18c412be4e6f244830e1e75949b8cf5f0e887cea9ea456109}"
postgres_image="${POSTGRES_INTEGRATION_IMAGE:-postgres:18-alpine@sha256:d3e1620b530c944afa6e887d22eb899824da68e19c52024bf98f5220c88a65b2}"
rabbitmq_container="discogs-consumer-recovery-$$"
postgres_container="discogs-consumer-recovery-postgres-$$"
temp_dir="$(mktemp -d)"

cleanup() {
    docker rm --force --volumes "${rabbitmq_container}" "${postgres_container}" >/dev/null 2>&1 || true
    rm -rf -- "${temp_dir}"
}
trap cleanup EXIT
trap "exit 130" INT
trap "exit 143" TERM

# The tests make the broker time out a delivery, so they need a short ack deadline.
cat >"${temp_dir}/rabbitmq.conf" <<'EOF'
consumer_timeout = 2000
EOF

docker create --name "${rabbitmq_container}" --rm \
    --publish 127.0.0.1::5672 "${rabbitmq_image}" >/dev/null
docker cp "${temp_dir}/rabbitmq.conf" "${rabbitmq_container}:/etc/rabbitmq/rabbitmq.conf"
docker start "${rabbitmq_container}" >/dev/null
docker run --detach --rm --name "${postgres_container}" \
    --env POSTGRES_PASSWORD=integration-test \
    --publish 127.0.0.1::5432 "${postgres_image}" >/dev/null

rabbitmq_ready=false
for _attempt in $(seq 1 90); do
    if docker exec --user rabbitmq "${rabbitmq_container}" rabbitmq-diagnostics -q check_port_listener 5672 >/dev/null 2>&1; then
        rabbitmq_ready=true
        break
    fi
    sleep 1
done
if [[ "${rabbitmq_ready}" != true ]]; then
    docker logs "${rabbitmq_container}" >&2
    echo "Owned RabbitMQ did not become ready" >&2
    exit 1
fi

postgres_ready=false
for _attempt in $(seq 1 30); do
    if docker exec "${postgres_container}" pg_isready --quiet --username postgres; then
        postgres_ready=true
        break
    fi
    sleep 1
done
if [[ "${postgres_ready}" != true ]]; then
    docker logs "${postgres_container}" >&2
    echo "Owned PostgreSQL did not become ready" >&2
    exit 1
fi

rabbitmq_port="$(docker port "${rabbitmq_container}" 5672/tcp)"
postgres_port="$(docker port "${postgres_container}" 5432/tcp)"
export TEST_AMQP_URL="amqp://guest:guest@127.0.0.1:${rabbitmq_port##*:}/"
export TEST_DATABASE_URL="postgresql://postgres:integration-test@127.0.0.1:${postgres_port##*:}/postgres"

uv run pytest tests/integration/test_consumer_recovery.py tests/integration/test_redelivery_bound.py tests/integration/test_purge_veto_durable.py -m integration --no-cov "$@"
