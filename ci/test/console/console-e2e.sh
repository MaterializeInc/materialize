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
# console-e2e.sh — Run console end-to-end tests against a local kind cluster
# with the cloud stack.
#
# This script runs directly on the Buildkite agent host (NOT inside ci-builder)
# because it needs sudo for DNS configuration and systemd. Node.js/yarn/kind
# operations are delegated to ci-builder via bin/ci-builder.
#
# Required environment variables:
#   GITHUB_TOKEN                          — GitHub token for GHCR access
#   CLOUD_DEPLOY_KEY                      — Key for cloning the cloud repo
#   E2E_TEST_PASSWORD                     — Frontegg e2e test user password
#   E2E_FRONTEGG_CLIENT_ID                — Frontegg API client ID
#   E2E_FRONTEGG_SECRET_KEY               — Frontegg API secret key

set -euo pipefail

. misc/shlib/shlib.bash

cd "$(dirname "$0")/../../.."

# --- Resolve cloud version ---

cloud_ref=$(<console/CLOUD_REF)

# Set up SSH for cloning the cloud repo
mkdir -p ~/.ssh
echo "$CLOUD_DEPLOY_KEY" > ~/.ssh/cloud_deploy_key
chmod 600 ~/.ssh/cloud_deploy_key
ssh-keyscan github.com >> ~/.ssh/known_hosts 2>/dev/null
export GIT_SSH_COMMAND="ssh -i ~/.ssh/cloud_deploy_key -o StrictHostKeyChecking=no"

ci_collapsed_heading "Cloning cloud repo"
# Clone into workspace so it's accessible inside ci-builder (which mounts CWD)
git clone --depth 100 git@github.com:MaterializeInc/cloud.git .cloud-repo

if [[ "$cloud_ref" == "__LATEST__" ]]; then
    ci_collapsed_heading "Resolving latest cloud docker tag"
    cloud_ref=$(bin/ci-builder run stable bash -c "cd .cloud-repo && node console/bin/latest-cloud-docker.js")
fi

echo "Cloud ref: $cloud_ref"
export MZCLOUD_DOCKER_COMPOSE_TAG="$cloud_ref"

# Re-checkout cloud at the exact ref
cd .cloud-repo
git fetch origin "$cloud_ref"
git checkout "$cloud_ref"
git submodule update --init --recursive
cd ..

# --- Pull cloud image (host — has Docker) ---

ci_collapsed_heading "Pulling cloud image"
echo "$GITHUB_TOKEN" | docker login ghcr.io -u "materialize-bot" --password-stdin
docker pull "ghcr.io/materializeinc/cloud:$cloud_ref"

# --- DNS setup (host — needs sudo) ---

ci_collapsed_heading "Configuring DNS for kind cluster"
sudo apt-get update -qq
sudo apt-get install -y -qq dnsmasq > /dev/null 2>&1 || true

sudo cp console/misc/dnsmasq/mzcloud-kind.conf /etc/dnsmasq.d/mzcloud-kind.conf
sudo bash -c 'echo "port=5353" >> /etc/dnsmasq.d/mzcloud-kind.conf'
sudo bash -c 'echo "no-resolv" >> /etc/dnsmasq.d/mzcloud-kind.conf'
sudo bash -c 'echo "server=8.8.8.8" >> /etc/dnsmasq.d/mzcloud-kind.conf'
sudo bash -c 'echo "127.0.0.1  mzcloud-control-plane" >> /etc/hosts'
sudo systemctl restart dnsmasq.service || true
sudo sed -i 's/#DNS=/DNS=127.0.0.1:5353/' /etc/systemd/resolved.conf
sudo systemctl restart systemd-resolved || true

# --- Kind cluster (ci-builder — has kind, kubectl, Python) ---

ci_collapsed_heading "Setting up cloud kind cluster"

# Extract Frontegg JWK from cloud config (ci-builder has node)
frontegg_jwk=$(bin/ci-builder run stable node -e "
    const d = require('./.cloud-repo/config/settings/local.outputs.json');
    process.stdout.write(d.frontegg_jwk);
")
# Extract environmentd image ref from cloud Pulumi config
environmentd_image_ref=$(grep '^  mzcloud:environmentd_image_ref:' .cloud-repo/Pulumi.staging.yaml | sed 's/.*: //')

# Create kind cluster using cloud's setup scripts
bin/ci-builder run stable bash -c "
    cd .cloud-repo
    bin/helpers/pyactivate /dev/null
    FRONTEGG_URL=https://admin.staging.cloud.materialize.com \
    FRONTEGG_JWK_STRING='${frontegg_jwk}' \
    ENVIRONMENTD_IMAGE_REF_DEFAULT='${environmentd_image_ref}' \
        bin/kind-create
"

# --- Console setup (ci-builder — has node/yarn/corepack) ---

ci_collapsed_heading "Installing console dependencies"
bin/ci-builder run stable bash -c "
    cd console
    export COREPACK_ENABLE_DOWNLOAD_PROMPT=0
    corepack enable
    yarn install --immutable --network-timeout 30000
    yarn playwright install --with-deps
"

# --- Run tests ---

ci_collapsed_heading "Starting console dev server"
bin/ci-builder run stable --detach --name console-dev-server bash -c "
    cd console
    export COREPACK_ENABLE_DOWNLOAD_PROMPT=0
    corepack enable
    DEFAULT_STACK=local yarn start
"

# Wait for dev server to be ready (host — ci-builder uses --network host)
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
bin/ci-builder run stable bash -c "
    cd console
    export COREPACK_ENABLE_DOWNLOAD_PROMPT=0
    corepack enable
    CONSOLE_ADDR=http://local.dev.materialize.com:3000 yarn test:e2e:all
" | tee run.log || e2e_exit=$?

# --- Cleanup ---

docker stop console-dev-server 2>/dev/null || true
docker rm console-dev-server 2>/dev/null || true

exit "$e2e_exit"
