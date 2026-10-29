#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

set -euo pipefail

case "${1:-}" in
    acceptance|integration) suite=$1 ;;
    *) echo "Usage: $0 acceptance|integration" >&2; exit 1 ;;
esac

cd "$(dirname "$0")/../.."
python="$PWD/bin/pyactivate"

# The release-tagged image may still be publishing when qualification starts.
image="${MATERIALIZED_IMAGE:-materialize/materialized:$("$python" -c 'from materialize.ci_util import dev_docker_tag; print(dev_docker_tag())')}"
# The host Docker daemon must see fixture files created inside ci-builder.
workdir=$(mktemp -d "$PWD/.terraform-provider.XXXXXX")
export COMPOSE_FILE="$workdir/compose.json"
export COMPOSE_PROJECT_NAME="terraform-${suite}-${BUILDKITE_JOB_ID:-local}"

stop_fixtures() {
    # Terraform writes state and plugin directories into the bind-mounted checkout
    # as root. Return ownership before removing the checkout on the agent.
    docker compose exec -T provider chown -R "$(id -u):$(id -g)" /usr/src/app/integration || true
    docker compose down --volumes --remove-orphans
}

cleanup() {
    local status=$1
    if [[ -s "$COMPOSE_FILE" ]]; then
        if (( status != 0 )); then
            docker compose logs --no-color --tail=200 || true
            docker compose ps --all --quiet | xargs --no-run-if-empty docker inspect \
                --format '{{.Name}} {{json .State.Health}}' || true
        fi
        stop_fixtures || status=1
    fi
    rm -rf "$workdir"
    exit "$status"
}
trap 'cleanup $?' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

git init --quiet "$workdir/provider"
cd "$workdir/provider"
git fetch --depth=1 https://github.com/MaterializeInc/terraform-provider-materialize.git "${TERRAFORM_PROVIDER_REF:-main}"
git checkout --detach FETCH_HEAD

# Tests use service DNS names inside Compose, so host ports aren't needed.
# Allow five minutes for Materialize to become ready on a busy test agent.
docker compose -f compose.yaml config --format json | jq --arg image "$image" --arg suite "$suite" '
    .services.redpanda.container_name as $redpanda |
    .services |= with_entries(.value |= del(.ports, .container_name)) |
    .services.materialized.image = $image |
    .services.materialized2.image = $image |
    .services.materialized.healthcheck.start_period = "5m" |
    .services.materialized2.healthcheck.start_period = "5m" |
    if $suite == "acceptance" then
        # The upstream Go tests execute docker exec against this container name.
        .services.redpanda.container_name = $redpanda |
        .services.provider.volumes += ["/var/run/docker.sock:/var/run/docker.sock"]
    else . end
' > "$COMPOSE_FILE"

echo "--- Starting fixtures against $image"
docker compose up --build -d

if [[ "$suite" == acceptance ]]; then
    echo "--- Terraform Acceptance tests"
    password=$("$python" -c '
import yaml
workflow = yaml.safe_load(open(".github/workflows/acceptance.yml"))
print(next(step["env"]["MZ_PASSWORD"] for step in workflow["jobs"]["acceptance"]["steps"] if "MZ_PASSWORD" in step.get("env", {})))
')
    docker compose exec -T --workdir /usr/src/app \
        -e MZ_ENDPOINT=http://frontegg:3000 \
        -e MZ_CLOUD_ENDPOINT=http://cloud:3001 \
        -e MZ_PASSWORD="$password" \
        -e MZ_SSLMODE=disable \
        provider sh -ec '
            apk add --no-cache make docker-cli
            docker info > /dev/null
            make testacc
        '
else
    result=0
    for directory in integration integration/self_hosted; do
        echo "--- Terraform Integration tests: $directory"
        if ! docker compose exec -T --workdir "/usr/src/app/$directory" provider sh -ec '
            terraform init
            terraform apply -auto-approve -compact-warnings
            terraform plan -detailed-exitcode
            terraform destroy -auto-approve -compact-warnings
        '; then
            result=1
            docker compose logs --no-color --tail=200 || true
        fi
        if [[ "$directory" == integration ]]; then
            # Each integration variant expects a fresh database.
            stop_fixtures
            docker compose up --build -d materialized materialized_init redpanda postgres mysql provider sqlserver
        fi
    done
    exit "$result"
fi
