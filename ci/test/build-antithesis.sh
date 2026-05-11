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
# build-antithesis.sh — antithesis-flavored build + Antithesis-registry push.
#
# 1. Write `.env` so `antithesis-config` bakes in compose refs that point
#    at the Antithesis GCP Artifact Registry (where we'll mirror to). The
#    .env content is one of antithesis-config's mzbuild inputs, so the
#    image fingerprint tracks the source it references — self-consistent.
# 2. Run the standard `ci.test.build` to compile antithesis-flavored Rust
#    binaries and build the docker images (pushed to GHCR via mzbuild).
# 3. `docker login` the Antithesis GCP Artifact Registry using
#    `ANTITHESIS_GCP_SERVICE_ACCOUNT_JSON` (a service account scoped to
#    `materialize-storage@molten-verve-216720.iam.gserviceaccount.com` —
#    kept distinct from `GCP_SERVICE_ACCOUNT_JSON` which is used elsewhere
#    for unrelated GCP integrations).
# 4. Retag + push `materialized`, `antithesis-workload`, and
#    `antithesis-config` to the Antithesis registry. Public images
#    referenced by the compose (postgres, minio, kafka stack) stay on
#    their upstream registries — Antithesis can reach those directly.

set -euo pipefail

: "${CI_ANTITHESIS:?build-antithesis.sh expects CI_ANTITHESIS=1}"

# GCP Artifact Registry path for Antithesis. Tags pushed under
# $ANTITHESIS_REGISTRY/<name>:mzbuild-<fingerprint>.
ANTITHESIS_REGISTRY="${ANTITHESIS_REGISTRY:-us-central1-docker.pkg.dev/molten-verve-216720/materialize-repository}"

echo "--- Writing test/antithesis/config/.env (registry: $ANTITHESIS_REGISTRY)"
bin/pyactivate test/antithesis/export-env.py \
    --registry "$ANTITHESIS_REGISTRY" \
    > test/antithesis/config/.env

echo "--- Building antithesis-flavored mzbuild images"
bin/pyactivate -m ci.test.build

echo "--- Authenticating to Antithesis registry"
if [[ -z "${ANTITHESIS_GCP_SERVICE_ACCOUNT_JSON:-}" ]]; then
    echo "ANTITHESIS_GCP_SERVICE_ACCOUNT_JSON is unset — pushing to the Antithesis registry will fail." >&2
    echo "Provision it as a Buildkite-agent env var (see bin/ci-builder env-forwarding)." >&2
    exit 1
fi
echo "$ANTITHESIS_GCP_SERVICE_ACCOUNT_JSON" \
    | docker login -u _json_key --password-stdin "https://${ANTITHESIS_REGISTRY%%/*}"

echo "--- Pushing Materialize-built images to the Antithesis registry"
bin/pyactivate test/antithesis/push-antithesis.py --registry "$ANTITHESIS_REGISTRY"
