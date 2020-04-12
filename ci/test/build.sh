#!/usr/bin/env bash

# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# build.sh — packages test binaries into Docker images in CI.

set -euo pipefail

. misc/shlib/shlib.bash

# TODO(benesch): rewrite this all in Python, so it's not split between two
# languages.

ci_init

if [[ "$BUILDKITE_BRANCH" = master ]]; then
    ci_collapsed_heading "Building .deb package"
    release_version=$(bin/ci-builder run stable "cargo metadata --format-version=1 --no-deps | jq -r '.packages[] | select(.name == \"materialized\") | .version | match(\"[0-9]\\\\.[0-9]\\\\.[0-9]\") | .string'")
    commit_index=$(git rev-list HEAD | wc -l)
    commit_hash=$(git rev-parse HEAD)
    bin/ci-builder run stable cargo-deb --no-strip --deb-version "$release_version-$commit_index-$commit_hash" -p materialized -o target/debian/materialized.deb

    # Note - The below will not cause anything to become public;
    # a separate step to "publish" the files will be run in deploy.sh .
    #
    # Step 1 - create a new version.
    curl -f -X POST -H "Content-Type: application/json" -d "
        {
            \"name\": \"dev-$commit_index-$commit_hash\",
            \"desc\": \"git master\",
            \"vcs_tag\": \"$commit_hash\"
        }" -u ci@materialize:"$BINTRAY_API_KEY" \
        https://api.bintray.com/packages/materialize/apt/materialized-unstable/versions
    # Step 2 - upload the .deb for the version.
    curl -f -T target/debian/materialized.deb -u ci@materialize:"$BINTRAY_API_KEY" "https://api.bintray.com/content/materialize/apt/materialized-unstable/dev-$commit_index-$commit_hash/materialized-$commit_hash.deb;deb_distribution=generic;deb_component=main;deb_architecture=amd64"
fi

bin/ci-builder run stable misc/python/bin/activate.py ci/test/build-docker.py
