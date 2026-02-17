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

if [[ "$BUILDKITE_PULL_REQUEST" = false ]]; then
    echo "Skipping docs preview on non-pull request build"
    exit 0
fi

cd doc/user

# Build main docs to public/
hugo --gc --baseURL "/materialize/$BUILDKITE_PULL_REQUEST"

# Build skill docs to public/markdown-docs/
hugo --config config.toml,config.skill.toml --gc --baseURL "/materialize/$BUILDKITE_PULL_REQUEST" --disableKinds sitemap,robotsTXT,taxonomy

cat > config.deployment.toml <<EOF
[[deployment.targets]]
name = "preview"
url = "s3://materialize-website-previews?region=us-east-1&prefix=materialize/$BUILDKITE_PULL_REQUEST/"
EOF
# Single deploy: public/ contains both main site and markdown-docs/
hugo deploy --config config.toml,config.deployment.toml --force

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
