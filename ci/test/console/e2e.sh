#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# e2e.sh — Run console end-to-end tests against a local kind cluster
# with the cloud stack.
#
# Required environment variables:
#   GITHUB_GHCR_TOKEN                     — GitHub token for GHCR access
#   CLOUD_DEPLOY_KEY                      — Key for cloning the cloud repo
#   CONSOLE_E2E_TEST_STAGING_PASSWORD     — Frontegg e2e test user password (staging)
#   MZ_CI_LICENSE_KEY                     — Materialize license key
#   E2E_FRONTEGG_CLIENT_ID                — Frontegg API client ID
#   E2E_FRONTEGG_SECRET_KEY               — Frontegg API secret key

set -euo pipefail

. misc/shlib/shlib.bash

dev_server_pid=
# shellcheck disable=SC2317 # invoked indirectly via trap
k8s_diagnostics() {
    ci_collapsed_heading "K8s diagnostics"
    local kc="kubectl --context kind-mzcloud"

    echo "=== All pods ==="
    $kc get pods -A -o wide 2>&1 || true

    echo "=== Materialize CRs ==="
    $kc get materialize -A -o wide 2>&1 || true

    echo "=== Events (all namespaces) ==="
    $kc get events -A --sort-by='.lastTimestamp' 2>&1 || true

    # Collect logs from each mz-system pod individually
    for pod in $($kc get pods -n mz-system -o name 2>/dev/null); do
        echo "=== Logs: mz-system/$pod ==="
        $kc logs -n mz-system "$pod" --tail=200 --timestamps 2>&1 || true
    done

    echo "=== Logs: cockroachdb ==="
    $kc logs -n cockroachdb cockroachdb-0 --tail=50 --timestamps 2>&1 || true

    # Dump environmentd pods if any exist (created when a region is enabled)
    while IFS=' ' read -r ns pod; do
        echo "=== Describe: $ns/$pod ==="
        $kc describe pod -n "$ns" "$pod" 2>&1 || true
        echo "=== Logs: $ns/$pod ==="
        $kc logs -n "$ns" "$pod" --all-containers --tail=200 --timestamps 2>&1 || true
    done < <($kc get pods -A -o wide 2>/dev/null | grep -i environmentd | awk '{print $1, $2}')
}

# shellcheck disable=SC2317 # invoked indirectly via trap
cleanup() {
    k8s_diagnostics
    ci_collapsed_heading "Cleaning up"
    if [[ -n "$dev_server_pid" ]]; then kill "$dev_server_pid" 2>/dev/null || true; fi
    kind delete cluster --name mzcloud 2>/dev/null || true
    docker rm -f proxy-docker-hub 2>/dev/null || true
    docker network rm kind 2>/dev/null || true
}
trap cleanup EXIT

# Clean up stale resources from a previous run that may not have exited cleanly.
ci_collapsed_heading "Cleaning up stale resources"
# Skip diagnostics during initial cleanup — there's no cluster to inspect yet.
kind delete cluster --name mzcloud 2>/dev/null || true
docker rm -f proxy-docker-hub 2>/dev/null || true
docker network rm kind 2>/dev/null || true

export E2E_TEST_PASSWORD="$CONSOLE_E2E_TEST_STAGING_PASSWORD"
export MATERIALIZE_LICENSE_KEY="$MZ_CI_LICENSE_KEY"

# --- Resolve cloud version ---

cloud_ref=$(<console/CLOUD_REF)

# Set up SSH for cloning the cloud repo
mkdir -p ~/.ssh
echo "$CLOUD_DEPLOY_KEY" > ~/.ssh/cloud_deploy_key
chmod 600 ~/.ssh/cloud_deploy_key
ssh-keyscan github.com >> ~/.ssh/known_hosts 2>/dev/null
export GIT_SSH_COMMAND="ssh -i ~/.ssh/cloud_deploy_key -o StrictHostKeyChecking=no"

ci_collapsed_heading "Logging into GHCR"
echo "$GITHUB_GHCR_TOKEN" | docker login ghcr.io -u "materialize-bot" --password-stdin

ci_collapsed_heading "Cloning cloud repo"
git clone --depth 100 git@github.com:MaterializeInc/cloud.git .cloud-repo

if [[ "$cloud_ref" == "__LATEST__" ]]; then
    ci_collapsed_heading "Resolving latest cloud docker tag"
    cloud_ref=$(cd .cloud-repo && node ../console/bin/latest-cloud-docker.js)
fi

echo "Cloud ref: $cloud_ref"
export MZCLOUD_DOCKER_COMPOSE_TAG="$cloud_ref"

# Re-checkout cloud at the exact ref
cd .cloud-repo
git fetch --depth 1 origin "$cloud_ref"
git checkout "$cloud_ref"
git submodule update --init --recursive
cd ..

# --- Pull cloud image ---

ci_collapsed_heading "Pulling cloud image"
docker pull "ghcr.io/materializeinc/cloud:$cloud_ref"

# --- Kind cluster ---

ci_collapsed_heading "Setting up cloud kind cluster (~10 min)"

# Extract Frontegg JWK from cloud config
frontegg_jwk=$(node -e "
    const d = require('./.cloud-repo/config/settings/local.outputs.json');
    process.stdout.write(d.frontegg_jwk);
" | perl -pe 's/\n/\\n/g')
# Extract environmentd image ref from cloud Pulumi config
environmentd_image_ref=$(grep '^  mzcloud:environmentd_image_ref:' .cloud-repo/Pulumi.staging.yaml | sed 's/.*: //')

# Create kind cluster using cloud's setup scripts
cd .cloud-repo
bin/helpers/pyactivate /dev/null
FRONTEGG_URL=https://admin.staging.cloud.materialize.com \
FRONTEGG_JWK_STRING="${frontegg_jwk}" \
ENVIRONMENTD_IMAGE_REF_DEFAULT="${environmentd_image_ref}" \
    bin/kind-create
cd ..

# --- DNS setup (needs sudo) ---
# The CI container doesn't have systemd, so we run dnsmasq on port 53 as a
# transparent forwarder that adds our custom wildcard record. We prepend
# 127.0.0.1 to /etc/resolv.conf so dnsmasq is tried first, keeping the
# original upstream nameservers as fallback for dnsmasq itself.

ci_collapsed_heading "Configuring DNS for kind cluster"
sudo bash -c 'echo "127.0.0.1  mzcloud-control-plane" >> /etc/hosts'
# Save the current upstream nameserver before we modify resolv.conf.
upstream_ns=$(grep '^nameserver' /etc/resolv.conf | head -1 | awk '{print $2}')
: "${upstream_ns:=8.8.8.8}"
# Run dnsmasq on port 53, forwarding unknown queries to the original upstream.
sudo dnsmasq \
    --address=/lb.testing.materialize.cloud/127.0.0.1 \
    --server="$upstream_ns" \
    --no-resolv \
    --listen-address=127.0.0.1 \
    --bind-interfaces
# Prepend 127.0.0.1 so the system uses dnsmasq first.
# /etc/resolv.conf may be a Docker mount, so overwrite in-place instead of mv.
sudo bash -c 'cp /etc/resolv.conf /tmp/resolv.conf.bak && { echo "nameserver 127.0.0.1"; cat /tmp/resolv.conf.bak; } > /etc/resolv.conf'

# --- Console setup ---

ci_collapsed_heading "Installing console dependencies"
cd console
export COREPACK_ENABLE_DOWNLOAD_PROMPT=0
corepack enable
yarn install --immutable --network-timeout 30000
yarn playwright install

# --- Run tests ---

ci_collapsed_heading "Starting console dev server"
DEFAULT_STACK=local yarn start &
dev_server_pid=$!

# Wait for dev server to be ready
for i in $(seq 1 60); do
    if curl -s http://localhost:3000 > /dev/null 2>&1; then
        echo "Dev server is ready"
        break
    fi
    if [[ $i -eq 60 ]]; then
        echo "Dev server failed to start within 60 seconds"
        exit 1
    fi
    sleep 1
done

ci_uncollapsed_heading "Running console e2e tests"
e2e_exit=0
set -o pipefail
CONSOLE_ADDR=http://local.dev.materialize.com:3000 yarn test:e2e:all | ../ci/test/console/embed-test-results.sh | tee run.log || e2e_exit=$?

exit "$e2e_exit"
