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

# Release qualification starts after devel-docker-tags, before the release tag
# is necessarily published. Use the same tag calculation as the publisher.
image="materialize/materialized:$(bin/pyactivate -c 'from materialize.ci_util import dev_docker_tag; print(dev_docker_tag())')"
checkout=$(mktemp -d)
compose_file=$(mktemp)

stop_fixtures() {
    # Terraform writes state and plugin directories into the bind-mounted checkout
    # as root. Return ownership before removing the checkout on the agent.
    docker compose exec -T provider chown -R "$(id -u):$(id -g)" /usr/src/app/integration || true
    docker compose down --volumes --remove-orphans
}

# shellcheck disable=SC2317,SC2329 # invoked indirectly via trap
cleanup() {
    status=$?
    if [[ -s "$compose_file" ]]; then
        if (( status != 0 )); then
            docker compose logs --no-color --tail=200 || true
            docker compose ps --all --quiet | xargs --no-run-if-empty docker inspect \
                --format '{{.Name}} {{json .State.Health}}' || true
        fi
        stop_fixtures || status=1
    fi
    rm -rf "$checkout"
    rm -f "$compose_file"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

echo "--- Checking out Terraform provider"
git -C "$checkout" init
git -C "$checkout" remote add origin https://github.com/MaterializeInc/terraform-provider-materialize.git
git -C "$checkout" fetch --depth=1 origin "${TERRAFORM_PROVIDER_REF:-main}"
git -C "$checkout" checkout --detach FETCH_HEAD
git -C "$checkout" rev-parse HEAD
cd "$checkout"
export PWD="$checkout"
export COMPOSE_PROJECT_NAME="terraform-${suite}-${BUILDKITE_JOB_ID:-local}"

# Tests use service DNS names inside Compose. Avoid the upstream fixture's
# fixed container names and published ports colliding with other local tests.
# Allow five minutes for Materialize to become ready on a busy test agent.
docker compose -f compose.yaml config --format json | jq --arg image "$image" '
    .services |= with_entries(.value |= del(.ports, .container_name)) |
    .services.materialized.image = $image |
    .services.materialized2.image = $image |
    .services.materialized.healthcheck.start_period = "5m" |
    .services.materialized2.healthcheck.start_period = "5m"
' > "$compose_file"
export COMPOSE_FILE="$compose_file"

echo "--- Starting fixtures against $image"
docker compose up --build -d

if [[ "$suite" == acceptance ]]; then
    echo "--- Terraform Acceptance tests"
    docker compose exec -T --workdir /usr/src/app \
        -e MZ_ENDPOINT=http://frontegg:3000 \
        -e MZ_CLOUD_ENDPOINT=http://cloud:3001 \
        -e MZ_PASSWORD=mzp_1b2a3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b \
        -e MZ_SSLMODE=disable \
        provider sh -c 'apk add --no-cache make && make testacc'
else
    result=0
    for directory in integration integration/self_hosted; do
        echo "--- Terraform Integration tests: $directory"
        if ! (
            docker compose exec -T --workdir "/usr/src/app/$directory" provider terraform init &&
            docker compose exec -T --workdir "/usr/src/app/$directory" provider terraform apply -auto-approve -compact-warnings &&
            docker compose exec -T --workdir "/usr/src/app/$directory" provider terraform plan -detailed-exitcode &&
            docker compose exec -T --workdir "/usr/src/app/$directory" provider terraform destroy -auto-approve -compact-warnings
        ); then
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
