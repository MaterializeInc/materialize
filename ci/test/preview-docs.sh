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
hugo --gc --environment preview --baseURL "/materialize/$BUILDKITE_PULL_REQUEST"

# Build skill docs to public/markdown-docs/. --environment preview makes their
# absolute links point at preview.materialize.com, like the HTML build above.
hugo --config config.toml,config.skill.toml --gc --environment preview --baseURL "/materialize/$BUILDKITE_PULL_REQUEST" --disableKinds 404,sitemap,robotsTXT,taxonomy

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

# Report how agent-friendly the preview is. Neither check fails the build.
preview_url="https://preview.materialize.com/materialize/$BUILDKITE_PULL_REQUEST"

ci_uncollapsed_heading "Checking the Markdown docs' sizes and links"
if ! ../../ci/test/check-docs-markdown.py public/markdown-docs "$preview_url/markdown-docs/"; then
    echo "check-docs-markdown found problems; see above. This check does not fail the build."
fi

# afdocs (https://afdocs.dev) scores the preview against the Agent-Friendly
# Documentation Spec. It cannot map llms.txt's markdown-docs links back to
# pages, so pass it every tenth page from llms.txt, as HTML URLs.
ci_uncollapsed_heading "Scoring the preview with afdocs"
urls=$(grep -o "($preview_url/markdown-docs/[^)]*)" public/llms.txt \
    | awk 'NR % 10 == 1' \
    | sed -e 's/^(//' -e 's/)$//' -e 's#/markdown-docs/#/#' -e 's#index\.md$##' \
    | paste -sd, -)
if ! npx --yes afdocs@0.22.2 check "$preview_url" --urls "$urls" --sampling deterministic --format scorecard; then
    echo "afdocs reported failing checks; see above. This check does not fail the build."
fi
