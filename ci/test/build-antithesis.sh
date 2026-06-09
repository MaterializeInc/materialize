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
# 1. Write a `.env` into every `test/antithesis/configs/<group>/` so each
#    per-group `antithesis-config-<group>` image bakes in compose refs
#    that point at the Antithesis GCP Artifact Registry (where we mirror
#    to). The .env content is the same across groups (only materialized
#    + antithesis-workload refs matter), but each config image's
#    fingerprint includes its own .env copy as an input, so the per-group
#    images track the materialized fingerprint transitively.
# 2. Run the standard `ci.test.build` to compile antithesis-flavored Rust
#    binaries and build the docker images (pushed to GHCR via mzbuild).
# 3. `docker login` the Antithesis GCP Artifact Registry using
#    `ANTITHESIS_GCP_SERVICE_ACCOUNT_JSON` (a service account scoped to
#    `materialize-storage@molten-verve-216720.iam.gserviceaccount.com` —
#    kept distinct from `GCP_SERVICE_ACCOUNT_JSON` which is used elsewhere
#    for unrelated GCP integrations).
# 4. Retag + push `materialized`, `antithesis-workload`, and every
#    `antithesis-config-<group>` to the Antithesis registry. Public images
#    referenced by the composes (postgres, minio, kafka stack) stay on
#    their upstream registries — Antithesis can reach those directly.

set -euo pipefail

: "${CI_ANTITHESIS:?build-antithesis.sh expects CI_ANTITHESIS=1}"

# GCP Artifact Registry path for Antithesis. Tags pushed under
# $ANTITHESIS_REGISTRY/<name>:mzbuild-<fingerprint>.
ANTITHESIS_REGISTRY="${ANTITHESIS_REGISTRY:-us-central1-docker.pkg.dev/molten-verve-216720/materialize-repository}"

# Workload groups whose per-group docker-compose.yaml + .env must be
# written before `ci.test.build` runs (the `antithesis-config-<group>`
# mzbuild images COPY the .env into their build context).  Read from
# the manifest so adding a group to test/antithesis/groups.yaml only
# requires editing that file.
#
# Two non-obvious points about this load:
#   - The array is `workload_groups`, not `GROUPS`. `GROUPS` is a magic
#     bash variable holding the current user's supplementary group IDs;
#     `mapfile -t GROUPS < …` silently fails to overwrite it and `set
#     -e` then kills the script with no error message. Don't rename.
#   - The pyactivate call writes to a temp file rather than feeding
#     `mapfile` via process substitution. Process substitution masks
#     non-zero exits from the inner command (bash gotcha), so a
#     pyactivate failure would otherwise leave `workload_groups` empty
#     and the for-loop below would silently iterate zero times.
groups_tmp=$(mktemp)
trap 'rm -f "$groups_tmp"' EXIT
bin/pyactivate -c "
import sys
sys.path.insert(0, 'test/antithesis')
from groups import load_manifest
for name in sorted(load_manifest().groups):
    print(name)
" > "$groups_tmp"
mapfile -t workload_groups < "$groups_tmp"
echo "--- Antithesis workload groups: ${workload_groups[*]}"

for group in "${workload_groups[@]}"; do
    config_dir="test/antithesis/configs/$group"
    echo "--- Writing $config_dir/.env (registry: $ANTITHESIS_REGISTRY)"
    bin/pyactivate test/antithesis/export-env.py \
        --group="$group" \
        --registry "$ANTITHESIS_REGISTRY" \
        > "$config_dir/.env"
done

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
