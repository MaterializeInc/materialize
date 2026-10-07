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
# Build the Antithesis image flavor and push the images the test/antithesis
# harness runs to the Antithesis registry, under a version tag
# (`images.target_tag`).
# Also publishes environmentd and clusterd at the upgrade base release, which
# only needs a build the first time a release becomes the base.

set -euo pipefail

: "${CI_ANTITHESIS:?build-antithesis.sh expects CI_ANTITHESIS=1}"
: "${ANTITHESIS_GCP_SERVICE_ACCOUNT_JSON:?the Antithesis registry credential is not set}"

registry=us-central1-docker.pkg.dev/molten-verve-216720/materialize-repository
bin/pyactivate -m ci.test.build

echo "--- Pushing images to the Antithesis registry"
docker login -u _json_key --password-stdin "https://${registry%%/*}" \
    <<< "$ANTITHESIS_GCP_SERVICE_ACCOUNT_JSON"
bin/pyactivate -m materialize.antithesis.images --registry "$registry" --commit "$BUILDKITE_COMMIT" \
    --upgrade-base --output antithesis-images.json
buildkite-agent artifact upload antithesis-images.json
