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

cd doc/user
hugo --gc --baseURL /docs/lts --destination public/docs/lts
hugo deploy --maxDeletes -1

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

# Hugo's CloudFront invalidation feature doesn't do anything smarter than
# invalidating the entire distribution, so we do it here to make it clear that
# we're invalidating the shortlinks too.
AWS_PAGER="" aws cloudfront create-invalidation --distribution-id E1F8Q2NUUC41QE --paths "/*"
