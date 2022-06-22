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
# website.sh — deploys the website to materialize.com in CI.

set -euo pipefail

. misc/shlib/shlib.bash

# Evergreen, readable shortlinks.
#
# The materialize.com/s/ namespace is reserved for shortlinks. The idea is that
# even if we change hosting providers, or switch platforms for the marketing
# website, the /s path can always be easily reserved for shortlinks.
#
# This is not a general-purpose URL shortener. Only evergreen content should be
# added here, and only when the layer of indirection is necessary. For example,
# the "bug" shortlink allows us to embed a bug reporting URL in the
# materialized binary that can be updated if we ever switch away from GitHub
# or change how we want bugs to be filed.
declare -A shortlinks=(
    [bug]="https://github.com/MaterializeInc/materialize/issues/new?labels=C-bug&template=01-bug.yml"
    [non-materialized-error]="https://materialize.com/docs/sql/create-view/#querying-non-materialized-views"
    [chat]="https://materializecommunity.slack.com/join/shared_invite/zt-19287i0fb-zlGYWbtx31ANimPSO1jYZw"
)

cd doc/user
hugo --gc --baseURL /docs/unstable --destination public/docs/unstable
cp -R ../../ci/deploy/website/. public/
hugo deploy

touch empty

# Make the alias redirects into server-side redirects rather than client side
# redirects.
(
    cd public
    grep -lr '#HUGOALIAS' | while IFS= read -r src; do
        src=${src#./}
        dst=$(sed -nE 's/<!-- #HUGOALIAS# (.*) -->/\1/p' "$src")
        echo "Installing server-side redirect $src => $dst"
        aws s3 cp ../empty "s3://materialize-website/$src" --website-redirect "$dst"
    done
)

# NOTE(benesch): this code does not delete old shortlinks. That's fine, because
# the whole point is that the shortlinks live forever.
for slug in "${!shortlinks[@]}"; do
    aws s3 cp empty "s3://materialize-website/s/$slug" --website-redirect "${shortlinks[$slug]}"
done

# Hugo's CloudFront invalidation feature doesn't do anything smarter than
# invalidating the entire distribution, so we do it here to make it clear that
# we're invalidating the shortlinks too.
AWS_PAGER="" aws cloudfront create-invalidation --distribution-id E1F8Q2NUUC41QE --paths "/*"
