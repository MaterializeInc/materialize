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
    [chat]="https://join.slack.com/t/materializecommunity/shared_invite/zt-ljdufufo-PTwVPmgzlZtI7RIQLDrAiA"
)

cd doc/user
hugo --gc --baseURL /docs --destination public/docs
cp -R ../../ci/deploy/website/ public/
hugo deploy

# NOTE(benesch): this code does not delete old shortlinks. That's fine, because
# the whole point is that the shortlinks live forever.
touch empty
for slug in "${!shortlinks[@]}"; do
    aws s3 cp empty "s3://materialize-website/s/$slug" --website-redirect "${shortlinks[$slug]}"
done

# Hugo's CloudFront invalidation feature doesn't do anything smarter than
# invalidating the entire distribution, so we do it here to make it clear that
# we're invalidating the shortlinks too.
aws cloudfront create-invalidation --distribution-id E1F8Q2NUUC41QE --paths "/*"
