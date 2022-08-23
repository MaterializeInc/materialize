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

. misc/shlib/shlib.bash

cd doc/user
hugo --gc --baseURL "/materialize/$BUILDKITE_PULL_REQUEST"

cat > config.deployment.toml <<EOF
[[deployment.targets]]
name = "preview"
url = "s3://materialize-website-previews?region=us-east-1&prefix=materialize/$BUILDKITE_PULL_REQUEST/"
EOF
hugo deploy --config config.toml,config.deployment.toml

curl -fsSL \
    -H "Authorization: Bearer $GITHUB_TOKEN" \
    -H "Accept: application/vnd.github.v3+json" \
    "https://api.github.com/repos/MaterializeInc/materialize/statuses/$BUILDKITE_COMMIT" \
    --data "{\
        \"state\": \"success\",\
        \"description\": \"Deploy preview ready.\",\
        \"target_url\": \"https://preview.materialize.com/materialize/$BUILDKITE_PULL_REQUEST/\",\
        \"context\": \"preview-docs\"\
    }"
